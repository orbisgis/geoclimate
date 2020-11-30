package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisdata.datamanager.api.dataset.ISpatialTable
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess

@BaseScript OSM_Utils osm_utils

/**
 * This process is used to format the OSM buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param epsg EPSG code to apply
 * @param jsonFilename Name of the json formatted file containing the filtering parameters
 * @param estimateHeight Boolean indicating if the buildings heights have to be estimated or not
 * @return outputTableName The name of the final buildings table
 * @return outputEstimatedTableName The name of the table containing the state of estimation for each building
 */
IProcess formatBuildingLayer() {
    return create {
        title "Transform OSM buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatBuildingLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, h_lev_min: 3, h_lev_max: 15, hThresholdLev2: 10, jsonFilename: "", estimateHeight: false, urbanAreasTableName : ""
        outputs outputTableName: String, outputEstimateTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, epsg, h_lev_min, h_lev_max, hThresholdLev2, jsonFilename, estimateHeight, urbanAreasTableName ->
            def outputTableName = postfix "INPUT_BUILDING"
            info 'Formating building layer'
            outputTableName = "INPUT_BUILDING_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def outputEstimateTableName = ""
            if (estimateHeight) {
                outputEstimateTableName = "EST_${outputTableName}"
                datasource """
                    DROP TABLE if exists ${outputEstimateTableName};
                    CREATE TABLE ${outputEstimateTableName} (
                        id_build INTEGER,
                        ID_SOURCE VARCHAR,
                        estimated boolean)
                """
            }
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build INTEGER, ID_SOURCE VARCHAR, 
                    HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER);
            """
            if (inputTableName) {
                def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
                def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                def mappingTypeAndUse = parametersMap.type
                def typeAndLevel = parametersMap.level
                def queryMapper = "SELECT "
                def columnToMap = parametersMap.columns
                def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        datasource."$inputZoneEnvelopeTableName".the_geom.createSpatialIndex()
                        queryMapper += " , st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName as a,  $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom and st_intersects(CASE WHEN ST_ISVALID(a.the_geom) THEN st_force2D(a.the_geom) " +
                                "else st_force2D(st_makevalid(a.the_geom)) end, b.the_geom) and st_area(a.the_geom)>1"
                    } else {
                        queryMapper += " , st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName as a where st_area(a.the_geom)>1"
                    }
                    def id_build=1;
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            String height = row.height
                            String roof_height = row.'roof:height'
                            String b_lev = row.'building:levels'
                            String roof_lev = row.'roof:levels'
                            def heightRoof = getHeightRoof(height)
                            def heightWall = getHeightWall(heightRoof, roof_height)

                            def nbLevels = getNbLevels(b_lev, roof_lev)

                            if (nbLevels >= 0) {
                                def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                                def use = typeAndUseValues[1]
                                def type = typeAndUseValues[0]
                                if (!type) {
                                    type = 'building'
                                }

                                def nbLevelsFromType = typeAndLevel[type]

                                def formatedHeight = formatHeightsAndNbLevels(heightWall, heightRoof, nbLevels, h_lev_min,
                                        h_lev_max, hThresholdLev2, nbLevelsFromType == null ? 0 : nbLevelsFromType)

                                def zIndex = getZIndex(row.'layer')

                                if (formatedHeight.nbLevels > 0 && zIndex >= 0 && type) {
                                    Geometry geom = row.the_geom
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        Geometry subGeom = geom.getGeometryN(i)
                                        if (subGeom instanceof Polygon) {
                                            stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                    $id_build, 
                                                    '${row.id}',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    '${type}',
                                                    '${use}',
                                                    ${zIndex})
                                            """.toString()
                                            if (estimateHeight) {
                                                stmt.addBatch """
                                                INSERT INTO ${outputEstimateTableName} values(
                                                    $id_build, 
                                                    '${row.id}',
                                                    ${formatedHeight.estimated})
                                                """.toString()
                                            }
                                            id_build++
                                        }
                                    }
                                }
                            }
                        }
                    }
                    //Improve building type using the urban areas table
                    if(urbanAreasTableName){
                        datasource."$outputTableName".the_geom.createSpatialIndex()
                        datasource."$outputTableName".id_build.createIndex()
                        datasource."$outputTableName".type.createIndex()
                        datasource."$urbanAreasTableName".the_geom.createSpatialIndex()
                        def buildinType= "BUILDING_TYPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                        datasource.execute"""create table $buildinType as SELECT max(b.type) as type, max(b.main_use) as main_use, a.id_build FROM $outputTableName a, $urbanAreasTableName b 
                        WHERE ST_POINTONSURFACE(a.the_geom) && b.the_geom and st_intersects(ST_POINTONSURFACE(a.the_geom), b.the_geom) AND  a.TYPE ='building' group by a.id_build""";

                        datasource.getTable(buildinType).id_build.createIndex()

                        def newBuildingWithType = "NEW_BUILDING_TYPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                        datasource.execute """DROP TABLE IF EXISTS $newBuildingWithType;
                                           CREATE TABLE $newBuildingWithType as
                                            SELECT  a.THE_GEOM, a.ID_BUILD,a.ID_SOURCE,
                                            a.HEIGHT_WALL,
                                            a.HEIGHT_ROOF,
                                               a.NB_LEV, 
                                               COALESCE(b.TYPE, a.TYPE) AS TYPE ,
                                               COALESCE(b.MAIN_USE, a.MAIN_USE) AS MAIN_USE
                                               , a.ZINDEX from $outputTableName
                                        a LEFT JOIN $buildinType b on a.id_build=b.id_build"""

                        datasource.execute """DROP TABLE IF EXISTS $buildinType, $outputTableName""";

                        datasource.execute """ALTER TABLE $newBuildingWithType RENAME TO $outputTableName""";
                    }
                }
            }
            info 'Buildings transformation finishes'
            [outputTableName: outputTableName, outputEstimateTableName: outputEstimateTableName]
        }
    }
}

/**
 * This process is used to transform the OSM roads table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw roads table
 * @param inputTableName The name of the raw roads table in the DB
 * @param epsg epsgcode to apply
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final roads table
 */
IProcess formatRoadLayer() {
    return create {
        title "Format the raw roads table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatRoadLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, jsonFilename: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName, epsg, jsonFilename ->
            info('Formating road layer')
            def outputTableName = postfix "INPUT_ROAD"
            datasource """
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(GEOMETRY, $epsg), id_road serial, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, CROSSING VARCHAR(30),
                SURFACE VARCHAR, SIDEWALK VARCHAR, ZINDEX INTEGER);
        """
            if (inputTableName) {
                //Define the mapping between the values in OSM and those used in the abstract model
                def paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
                def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                def mappingForRoadType = parametersMap.type
                def mappingForSurface = parametersMap.surface
                def typeAndWidth = parametersMap.width
                def crossingValues = parametersMap.crossing
                def queryMapper = "SELECT "
                def columnToMap = parametersMap.columns
                def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        queryMapper += ", CASE WHEN st_overlaps(st_force2D(a.the_geom), b.the_geom) " +
                                "THEN st_force2D(st_makevalid(st_intersection(st_force2D(a.the_geom), b.the_geom))) " +
                                "ELSE st_force2D(st_makevalid(a.the_geom)) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"
                    }

                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def processRow = true
                            def road_access = row.'access'
                            def road_service = row.'service'
                            if(road_service && road_service=="parking_aisle"){
                                processRow = false;
                            }
                            if(road_access && road_access=="permissive"){
                                processRow = false;
                            }
                            if(processRow){
                            def width = getWidth(row.'width')
                            String type = getTypeValue(row, columnNames, mappingForRoadType)
                            if (!type) {
                                type = 'unclassified'
                            }
                            def widthFromType = typeAndWidth[type]
                            if (width <= 0 && widthFromType) {
                                width = widthFromType
                            }
                            def crossing = row.'bridge'
                            if (crossing) {
                                crossing = crossingValues.bridge.contains(crossing) ? "'bridge'" : null
                            }

                            String surface = getTypeValue(row, columnNames, mappingForSurface)
                            String sidewalk = getSidewalk(row.'sidewalk')
                            def zIndex = getZIndex(row.'layer')
                            if (zIndex >= 0 && type) {
                                Geometry geom = row.the_geom
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    stmt.addBatch """
                                    INSERT INTO $outputTableName VALUES(ST_GEOMFROMTEXT(
                                        '${geom.getGeometryN(i)}',$epsg), 
                                        null, 
                                        '${row.id}', 
                                        ${width},
                                        '${type}',
                                        ${crossing}, 
                                        '${surface}',
                                        '${sidewalk}',
                                        ${zIndex})
                                """.toString()
                                }
                            }
                            }
                        }
                    }
                }
            }
            info('Roads transformation finishes')
            [outputTableName: outputTableName]
        }
    }
}

/**
 * This process is used to transform the raw rails table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw rails table
 * @param inputTableName The name of the raw rails table in the DB
 * @param epsg epsgcode to apply
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final rails table
 */
IProcess formatRailsLayer() {
    return create {
        title "Format the raw rails table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatRailsLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, jsonFilename: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName, epsg, jsonFilename ->
            info('Rails transformation starts')
            def outputTableName = "INPUT_RAILS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(GEOMETRY, $epsg), id_rail serial,ID_SOURCE VARCHAR, TYPE VARCHAR,CROSSING VARCHAR(30), ZINDEX INTEGER);"""

            if (inputTableName != null) {
                def paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
                def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                def mappingType = parametersMap.get("type")
                def crossingValues = parametersMap.get("crossing")
                def queryMapper = "SELECT "
                def columnToMap = parametersMap.get("columns")

                def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_makevalid(st_intersection(st_force2D(a.the_geom), b.the_geom))) " +
                                "ELSE st_force2D(st_makevalid(a.the_geom)) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"

                    }
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def type = getTypeValue(row, columnNames, mappingType)
                            def zIndex = getZIndex(row.'layer')
                            //special treatment if type is subway
                            if (type == "subway") {
                                if (!((row.tunnel != null && row.tunnel == "no" && zIndex >= 0)
                                        || (row.bridge != null && (row.bridge == "yes" || row.bridge == "viaduct")))) {
                                    type = null
                                }
                            }
                            def crossing = row.'bridge'
                            if (crossing) {
                                crossing = crossingValues.bridge.contains(crossing) ? "'bridge'" : null
                            }
                            if (zIndex >= 0 && type) {
                                Geometry geom = row.the_geom
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    stmt.addBatch """
                                    INSERT INTO $outputTableName values(ST_GEOMFROMTEXT(
                                    '${geom.getGeometryN(i)}',$epsg), 
                                    null, 
                                    '${row.id}',
                                    '${type}',
                                    ${crossing},
                                    ${zIndex})
                                """
                                }
                            }
                        }
                    }
                }
            }
            info('Rails transformation finishes')
            [outputTableName: outputTableName]
        }
    }
}

/**
 * This process is used to transform the raw vegetation table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw vegetation table
 * @param inputTableName The name of the raw vegetation table in the DB
 * @param epsg epsgcode to apply
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final vegetation table
 */
IProcess formatVegetationLayer() {
    return create {
        title "Format the raw vegetation table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatVegetationLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, jsonFilename: ""
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, epsg, jsonFilename ->
            info('Vegetation transformation starts')
            def outputTableName = postfix "INPUT_VEGET"
            datasource """ 
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_veget serial, ID_SOURCE VARCHAR, TYPE VARCHAR, HEIGHT_CLASS VARCHAR(4));"""
            if (inputTableName) {
                def paramsDefaultFile = this.class.getResourceAsStream("vegetParams.json")
                def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                def mappingType = parametersMap.get("type")
                def typeAndVegClass = parametersMap.get("class")
                def queryMapper = "SELECT "
                def columnToMap = parametersMap.get("columns")
                def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        queryMapper += ", CASE WHEN st_overlaps(st_makevalid(a.the_geom), b.the_geom) " +
                                "THEN st_force2D(st_intersection(st_makevalid(a.the_geom), st_makevalid(b.the_geom))) " +
                                "ELSE st_force2D(st_makevalid(a.the_geom)) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"
                    }
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def type = getTypeValue(row, columnNames, mappingType)
                            def height_class = typeAndVegClass[type]
                            if (type) {
                                Geometry geom = row.the_geom
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    Geometry subGeom = geom.getGeometryN(i)
                                    if (subGeom instanceof Polygon) {
                                        stmt.addBatch """
                                            INSERT INTO $outputTableName VALUES(
                                                ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                null, 
                                                '${row.id}',
                                                '${type}', 
                                                '${height_class}')
                                    """
                                    }
                                }
                            }
                        }
                    }
                }
            }
            info('Vegetation transformation finishes')
            [outputTableName: outputTableName]
        }
    }
}

/**
 * This process is used to transform the raw hydro table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw hydro table
 * @param inputTableName The name of the raw hydro table in the DB
 * @return outputTableName The name of the final hydro table
 */
IProcess formatHydroLayer() {
    return create {
        title "Format the raw hydro table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatHydroLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName, epsg ->
            info('Hydro transformation starts')
            def outputTableName = "INPUT_HYDRO_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_hydro serial, ID_SOURCE VARCHAR);"""

            if (inputTableName != null) {
                ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
                if (inputSpatialTable.rowCount > 0) {
                    inputSpatialTable.the_geom.createSpatialIndex()
                    String query
                    if (inputZoneEnvelopeTableName) {
                        query = "select id , CASE WHEN st_overlaps(st_makevalid(a.the_geom), b.the_geom) " +
                                "THEN st_force2D(st_intersection(st_makevalid(a.the_geom), st_makevalid(b.the_geom))) " +
                                "ELSE st_force2D(st_makevalid(a.the_geom)) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        query = "select id,  st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"

                    }
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(query) { row ->
                            Geometry geom = row.the_geom
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon) {
                                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), null, '${row.id}')"
                                }
                            }
                        }
                    }
                }
            }
            info('Hydro transformation finishes')
            [outputTableName: outputTableName]
        }
    }
}

/**
 * This process is used to transform the raw impervious table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw impervious table
 * @param inputTableName The name of the raw impervious table in the DB
 * @return outputTableName The name of the final impervious table
 */
IProcess formatImperviousLayer() {
    return create {
        title "Format the raw impervious table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatImperviousLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, jsonFilename: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName, epsg, jsonFilename ->
            info('Impervious transformation starts')
            def outputTableName = "INPUT_IMPERVIOUS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_impervious serial, ID_SOURCE VARCHAR);"""
            info(inputTableName)
            if (inputTableName != null) {
                def paramsDefaultFile = this.class.getResourceAsStream("imperviousParams.json")
                def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                def queryMapper = "SELECT "
                def columnToMap = parametersMap.get("columns")
                ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
                if (inputSpatialTable.rowCount > 0) {
                    inputSpatialTable.the_geom.createSpatialIndex()
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        queryMapper += ", CASE WHEN st_overlaps(st_makevalid(a.the_geom), b.the_geom) " +
                                "THEN st_force2D(st_intersection(st_makevalid(a.the_geom), st_makevalid(b.the_geom))) " +
                                "ELSE st_force2D(st_makevalid(a.the_geom)) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"
                    }
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def toAdd = true
                            if ((row.surface != null) && (row.surface == "grass")) {
                                toAdd = false
                            }
                            if ((row.parking != null) && (row.parking == "underground")) {
                                toAdd = false
                            }
                            if (toAdd) {
                                Geometry geom = row.the_geom
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    Geometry subGeom = geom.getGeometryN(i)
                                    if (subGeom instanceof Polygon) {
                                        stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), null, '${row.id}')"
                                    }
                                }
                            }
                        }
                    }
                }
            }
            info('Impervious transformation finishes')
            [outputTableName: outputTableName]
        }
    }
}

/**
 * This function defines the input values for both columns type and use to follow the constraints
 * of the geoClimate Input Model
 * @param row The row of the raw table to examine
 * @param columnNames the names of the column in the raw table
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String[] getTypeAndUse(def row,def columnNames, def myMap) {
    String strType = null
    String strUse = null
    myMap.each { finalVal ->
        def type_use = finalVal.key.split(":")
        def type
        def use
        if(type_use.size()==2){
            type =  type_use[0]
            use =type_use[1]
        }else{
            type = finalVal.key;
            use = type
        }
        finalVal.value.each { osmVals ->
            if(columnNames.contains(osmVals.key.toUpperCase())){
                def  columnValue = row.getString(osmVals.key)
            if(columnValue!=null){
            osmVals.value.each { osmVal ->
                if (osmVal.startsWith("!")) {
                    osmVal = osmVal.replace("! ","")
                    if ((columnValue != osmVal) && (columnValue != null)) {
                        if (strType == null) {
                            strType = type
                            strUse =use
                        }
                    }
                } else {
                    if (columnValue == osmVal) {
                        if (strType == null) {
                            strType = type
                            strUse =use
                        }
                    }
                }
            }
            }
            }
        }
    }
    if (strUse==null) {
            strUse = strType
    }
    return [strType,strUse]
}

/**
 * This function defines the value of the column height_wall according to the values of height and r_height
 * @param height value of the building height
 * @param r_height value of the roof height
 * @return The calculated value of height_wall (default value : 0)
 */

static float getHeightWall(height, r_height) {
    float result = 0
       if (r_height != null && r_height.isFloat())  {
           if (r_height.toFloat()<height) {
               result = height - r_height.toFloat()
           }
       } else {
           result = height
       }
    return result
}

/**
 * Rule to guarantee the height wall, height roof and number of levels values
 * @param heightWall value
 * @param heightRoof value
 * @param nbLevels value
 * @param h_lev_min value
 * @param h_lev_max value
 * @param hThresholdLev2 value
 * @param nbLevFromType value
 * @param hThresholdLev2 value
 * @return a map with the new values
 */
/*  TODO : when all the values are already available, it might happen that this control modify them. For example, a tower of 32 levels is 92 meters high.
     The control will put the value of heightRoof to 96 whereas it was 92. Do we accept it or are the real values considered as more reliable then the theoretical ones ?
 */
static Map formatHeightsAndNbLevels(def heightWall, def heightRoof, def nbLevels, def h_lev_min,
                                   def h_lev_max,def hThresholdLev2, def nbLevFromType){
    //Initialisation of heights and number of levels
    // Update height_wall
    def boolean estimated =false
    if(heightWall==0){
        if(heightRoof==0){
            if(nbLevels==0){
                heightWall = h_lev_min
                estimated = true
            }
            else {
                heightWall = h_lev_min*nbLevels
            }
        }
        else {
            heightWall = heightRoof
        }
    }
    // Update heightRoof
    if(heightRoof==0){
        heightRoof = heightWall
    }
    // Update nbLevels
    // If the nb_lev parameter (in the abstract table) is equal to 1 or 2
    // (and height_wall > 10m) then apply the rule. Else, the nb_lev is equal to 1
    if(nbLevels==0) {
        nbLevels = 1
        if (nbLevFromType == 1 || (nbLevFromType == 2 && heightWall > hThresholdLev2)) {
            nbLevels = Math.floor(heightWall / h_lev_min)
        }
    }

   // Control of heights and number of levels
   // Check if height_roof is lower than height_wall. If yes, then correct height_roof
    if(heightWall>heightRoof){
        heightRoof = heightWall
    }
    def tmpHmin=  nbLevels*h_lev_min
    // Check if there is a high difference between the "real" and "theorical (based on the level number) roof heights
    if(tmpHmin>heightRoof){
        heightRoof = tmpHmin
    }
    def tmpHmax=  nbLevels*h_lev_max
    if(nbLevFromType==1 || nbLevFromType==2 && heightWall> hThresholdLev2){
        if(tmpHmax<heightWall){
            nbLevels = Math.floor(heightWall/h_lev_max)
    }
    }
    return [heightWall:heightWall, heightRoof:heightRoof, nbLevels:nbLevels, estimated:estimated]

}


/**
 * This function defines the value of the column height_roof according to the values of height and b_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_roof (default value : 0)
 */
static float getHeightRoof(height ) {
    float result = 0
    if (height != null) {
        if (height.isFloat()) {
            result = height.toFloat()
        } else {
            // see if a pattern can be found and convert in meters
            def matcher = ("99.12 m" =~ /^(\d+|\d+\.\d+) ?(ft|foot|feet|'|m) ?(\d+)? ?("|in)?/)
            if (matcher.find()) {
                def (_, h, u, d) = matcher[0]
                if (u == "m") {
                    result = h.toFloat()
                } else {
                    result = h.toFloat() * 0.3048
                    if (d != null) {
                        result += d.toFloat() * 0.0254
                    }
                }
            }
        }
    }
    return result
}

/**
 * This function defines the value of the column nb_lev according to the values of b_lev and r_lev
 * @param row The row of the raw table to examine
 * @return The calculated value of nb_lev (default value : 0)
 */
static int getNbLevels (b_lev ,r_lev) {
    int result = 0
    if (b_lev != null && b_lev.isFloat()) {
        if (r_lev != null && r_lev.isFloat()) {
            result = b_lev.toFloat() + r_lev.toFloat()
        } else {
            result = b_lev.toFloat()
        }
    }
    return result
}

/**
 * This function defines the value of the column width according to the value of width from OSM
 * @param width The original width value
 * @return the calculated value of width (default value : null)
 */
static Float getWidth (String width){
    return (width != null && width.isFloat()) ? width.toFloat() : 0
}

/**
 * This function defines the value of the column zindex according to the value of zindex from OSM
 * @param width The original zindex value
 * @return The calculated value of zindex (default value : null)
 */
static int getZIndex (String zindex){
    return (zindex != null && zindex.isInteger()) ? zindex.toInteger() : 0
}

static String getCrossingValue( def crossingValue, def myMap) {
    return myMap[crossingValue]
}
/**
 * This function defines the input value for a column of the geoClimate Input Model according to a given mapping between the expected value and the set of key/values tag in OSM
 * @param row The row of the raw table to examine
 * @param columnNames the names of the column in the raw table
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String getTypeValue(def row, def columnNames, def myMap) {
    String strType = null
    myMap.each { finalVal ->
        def finalKey = finalVal.key
        finalVal.value.each { osmVals ->
            if (columnNames.contains(osmVals.key.toUpperCase())) {
                def columnValue = row.getString(osmVals.key)
                if (columnValue != null) {
                    osmVals.value.each { osmVal ->
                        if (osmVal.startsWith("!")) {
                            osmVal = osmVal.replace("! ", "")
                            if ((columnValue != osmVal) && (columnValue != null)) {
                                if (strType == null) {
                                    strType = finalKey
                                }
                            }
                        } else {
                            if (columnValue == osmVal) {
                                if (strType == null) {
                                    strType = finalKey
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return strType
}

/**
 * This function defines the value of the column sidewalk according to the values of sidewalk from OSM
 * @param width The original sidewalk value
 * @return The calculated value of sidewalk (default value : null)
 */
static String getSidewalk(String sidewalk) {
    String result
    switch(sidewalk){
        case 'both':
            result = 'two'
            break
        case 'right':
        case 'left':
        case 'yes':
            result = 'one'
            break
        default:
            result = 'no'
            break
    }
    return result
}

/**
 * Method to find the difference between two list
 * right - left
 *
 * @param inputColumns left list
 * @param columnsToMap right list
 * @return a flat list with escaped elements
 */
static String columnsMapper(def inputColumns, def columnsToMap){
    //def flatList = "\"${inputColumns.join("\",\"")}\""
    def flatList =  inputColumns.inject([]) { result, iter ->
        result+= "a.\"$iter\""
    }.join(",")

    columnsToMap.each {it ->
        if(!inputColumns*.toLowerCase().contains(it)){
            flatList+= ", null as \"${it}\""
        }
    }
    return flatList
}

/**
 * Get a set of parameters stored in a json file
 *
 * @param file
 * @param altResourceStream
 * @return
 */
static Map parametersMapping(def file, def altResourceStream) {
    def paramStream
    def jsonSlurper = new JsonSlurper()
    if (file) {
        if (new File(file).isFile()) {
            paramStream = new FileInputStream(file)
        } else {
            warn("No file named ${file} found. Taking default instead")
            paramStream = altResourceStream
        }
    } else {
        paramStream = altResourceStream
    }

    return jsonSlurper.parse(paramStream)

}

/**
 * This process is used to re-format the building table when the estimated height RF is used.
 * It must be used to update the nb level according the estimated height roof value
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels *
 * @param jsonFilename Name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final buildings table
 */
IProcess formatEstimatedBuilding() {
    return create {
        title "Format the estimated OSM buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatEstimatedBuilding"
        inputs datasource: JdbcDataSource, inputTableName: String, epsg: int, h_lev_min: 3, h_lev_max: 15, hThresholdLev2: 10, jsonFilename: ""
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, epsg, h_lev_min, h_lev_max, hThresholdLev2, jsonFilename ->
            def outputTableName = postfix "INPUT_BUILDING_REFORMATED_"
            info 'Re-formating building layer'
            def outputEstimateTableName = ""
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build INTEGER, ID_SOURCE VARCHAR, 
                    HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER, ID_BLOCK INTEGER, ID_RSU INTEGER);
            """
            if (inputTableName) {
                def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
                def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                def typeAndLevel = parametersMap.level
                def queryMapper = "SELECT "
                def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    def columnNames = inputSpatialTable.columns
                    queryMapper += " ${columnNames.join(",")} FROM $inputTableName"

                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def heightRoof = row.height_roof
                            def heightWall = heightRoof
                            def type = row.type
                            def nbLevelsFromType = typeAndLevel[type]

                            def formatedHeight = formatHeightsAndNbLevels(heightWall, heightRoof, 0, h_lev_min,
                                    h_lev_max, hThresholdLev2, nbLevelsFromType == null ? 0 : nbLevelsFromType)

                            stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${row.the_geom}',$epsg), 
                                                    ${row.id_build}, 
                                                    '${row.id_source}',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    '${type}',
                                                    '${row.main_use}',
                                                    ${row.zindex},
                                                    ${row.id_block},${row.id_rsu})
                                            """.toString()

                        }
                    }
                }
            }
            info 'Re-formating building finishes'
            [outputTableName: outputTableName]
        }
    }
}

    /**
     * This process is used to transform the urban areas  table into a table that matches the constraints
     * of the geoClimate Input Model
     * @param datasource A connexion to a DB containing the raw urban areas table
     * @param inputTableName The name of the raw hydro table in the DB
     * @return outputTableName The name of the final urban areas table
     */
    IProcess formatUrbanAreas() {
        return create {
            title "Format the urban areas table into a table that matches the constraints of the GeoClimate Input Model"
            id "formatUrbanAreas"
            inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, jsonFilename: ""
            outputs outputTableName: String
            run { datasource, inputTableName, inputZoneEnvelopeTableName, epsg, jsonFilename->
                info('Urban areas transformation starts')
                def outputTableName = "INPUT_URBAN_AREAS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_urban serial, ID_SOURCE VARCHAR, TYPE VARCHAR, MAIN_USE VARCHAR);"""

                if (inputTableName != null) {
                    def paramsDefaultFile = this.class.getResourceAsStream("urbanAreasParams.json")
                    def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
                    def mappingType = parametersMap.type
                    def queryMapper = "SELECT "
                    def columnToMap = parametersMap.columns
                    ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
                    if (inputSpatialTable.rowCount > 0) {
                        def columnNames = inputSpatialTable.columns
                        columnNames.remove("THE_GEOM")
                        queryMapper += columnsMapper(columnNames, columnToMap)
                        if (inputZoneEnvelopeTableName) {
                            inputSpatialTable.the_geom.createSpatialIndex()
                            queryMapper += ", CASE WHEN st_overlaps(st_makevalid(a.the_geom), b.the_geom) " +
                                    "THEN st_force2D(st_intersection(st_makevalid(a.the_geom), st_makevalid(b.the_geom))) " +
                                    "ELSE st_force2D(st_makevalid(a.the_geom)) " +
                                    "END AS the_geom " +
                                    "FROM " +
                                    "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                    "WHERE " +
                                    "a.the_geom && b.the_geom "
                        } else {
                            queryMapper += ",  st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"

                        }
                        datasource.withBatch(1000) { stmt ->
                            datasource.eachRow(queryMapper) { row ->
                                def type = getTypeValue(row, columnNames, mappingType)
                                Geometry geom = row.the_geom
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    Geometry subGeom = geom.getGeometryN(i)
                                    if (subGeom instanceof Polygon) {
                                        stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), null, '${row.id}', '${type}','${type}')"
                                    }
                                }
                            }
                        }
                    }
                }
                info('Urban areas transformation finishes')
                [outputTableName: outputTableName]
            }
        }
    }

/**
 * This process is used to build a sea-land mask layer from the coastline and zone table
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the coastlines table in the DB
 * @return outputTableName The name of the final buildings table
 */
IProcess formatSeaLandMask() {
    return create {
        title "Extract the sea/land mask"
        id "formatSeaLandMask"
        inputs datasource: JdbcDataSource, inputTableName: String,
                inputZoneEnvelopeTableName: String, epsg: int
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, epsg ->
         def outputTableName = postfix "INPUT_SEA_LAND_MASK_"
            info 'Computing sea/land mask table'
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id serial, TYPE VARCHAR);
            """
            if (inputTableName) {
               def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        def mergingDataTable = "coatline_merged${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def coastLinesIntersects = "coatline_intersect_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def islands_mark = "islands_mark_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def coastLinesIntersectsPoints = "coatline_intersect_points_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def coastLinesPoints = "coatline_points_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"

                        datasource.execute """DROP TABLE IF EXISTS $outputTableName, $coastLinesIntersects, 
                        $islands_mark, $mergingDataTable,  $coastLinesIntersectsPoints, $coastLinesPoints;

                        CREATE TABLE $coastLinesIntersects AS SELECT a.the_geom
                        from $inputTableName  AS  a,  $inputZoneEnvelopeTableName  AS b WHERE
                        a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom);     
                        
                        CREATE TABLE $islands_mark (the_geom GEOMETRY, ID SERIAL) AS 
                       SELECT the_geom, NULL FROM st_explode('(  
                       SELECT ST_LINEMERGE(st_accum(THE_GEOM)) AS the_geom, NULL FROM $coastLinesIntersects)') where  st_isclosed(the_geom)=false
                        ;                   

                        CREATE TABLE $mergingDataTable  AS
                        SELECT  THE_GEOM FROM $coastLinesIntersects 
                        UNION ALL
                        SELECT st_tomultiline(the_geom)
                        from $inputZoneEnvelopeTableName ;

                        CREATE TABLE $outputTableName (THE_GEOM GEOMETRY,ID serial, TYPE VARCHAR) AS SELECT THE_GEOM, NULL, 'land' FROM
                        st_explode('(SELECT st_polygonize(st_union(ST_NODE(st_accum(the_geom)))) AS the_geom FROM $mergingDataTable)');                
                        
                        CREATE TABLE $coastLinesPoints as  SELECT ST_LocateAlong(the_geom, 0.5, -0.01) AS the_geom FROM 
                        st_explode('(select ST_GeometryN(ST_ToMultiSegments(st_intersection(a.the_geom, b.the_geom)), 1) as the_geom from $islands_mark as a,
                        $inputZoneEnvelopeTableName as b WHERE st_isclosed(a.the_geom) = false and a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom))');
    
                        CREATE TABLE $coastLinesIntersectsPoints as  SELECT the_geom FROM st_explode('$coastLinesPoints'); 

                        CREATE INDEX IF NOT EXISTS ${outputTableName}_the_geom_idx ON $outputTableName USING RTREE(THE_GEOM);
                        CREATE INDEX IF NOT EXISTS ${coastLinesIntersectsPoints}_the_geom_idx ON $coastLinesIntersectsPoints USING RTREE(THE_GEOM);

                        UPDATE $outputTableName SET TYPE='sea' WHERE ID IN(SELECT DISTINCT(a.ID)
                                FROM $outputTableName a, $coastLinesIntersectsPoints b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_contains(a.THE_GEOM, b.THE_GEOM));                                
                                """

                        datasource.execute("drop table if exists $mergingDataTable, $coastLinesIntersects, $coastLinesIntersectsPoints, $coastLinesPoints," +
                                "$islands_mark")
                    }
                }else{
                    info "The sea/land mask table is empty"
                }
                }
            info 'The sea/land mask has been computed'
            [outputTableName: outputTableName]
            }
        }
    }


/**
 * This process is used to merge the water and the sea-land mask layers
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputSeaLandTableName The name of the sea/land table
 * @param inputWaterTableName The name of the water table
 * @return outputTableName The name of the final water table
 */
IProcess mergeWaterAndSeaLandTables() {
    return create {
        title "Extract the sea/land mask"
        id "formatSeaLandMask"
        inputs datasource: JdbcDataSource, inputSeaLandTableName: String,inputWaterTableName: String, epsg: int
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputSeaLandTableName,inputWaterTableName, epsg ->
            def outputTableName = postfix "INPUT_WATER_SEA_"
            info 'Merging sea/land mask and water table'
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_hydro serial, id_source VARCHAR);
            """
            if (inputSeaLandTableName && inputWaterTableName ) {
                if(datasource.firstRow("select count(*) as count from $inputSeaLandTableName where TYPE ='sea'").count>0){
                def tmp_water_not_in_sea =  "WATER_NOT_IN_SEA${UUID.randomUUID().toString().replaceAll("-", "_")}"
                //This method is used to merge the SEA mask with the water table
                def queryMergeWater = """DROP  TABLE IF EXISTS $outputTableName, $tmp_water_not_in_sea;
                CREATE TABLE $tmp_water_not_in_sea AS SELECT a.the_geom, a.ID_SOURCE FROM $inputWaterTableName AS a, $inputSeaLandTableName  AS b
                WHERE b."TYPE"= 'land' AND a.the_geom && b.the_geom AND st_contains(b.THE_GEOM, st_pointonsurface(a.THE_GEOM));
                CREATE TABLE $outputTableName(the_geom GEOMETRY, ID_HYDRO SERIAL, ID_SOURCE VARCHAR) AS SELECT the_geom, NULL, id_source FROM $tmp_water_not_in_sea UNION ALL 
                SELECT THE_GEOM, NULL, '-1' FROM $inputSeaLandTableName  WHERE
                "TYPE" ='sea';"""
                //Check indexes before executing the query
                datasource.getSpatialTable(inputWaterTableName).the_geom.createSpatialIndex()
                datasource.getSpatialTable(inputSeaLandTableName).the_geom.createSpatialIndex()
                datasource.execute queryMergeWater
                datasource.execute("drop table if exists $tmp_water_not_in_sea;")
                }
                else{
                    [outputTableName: inputWaterTableName]
                }
            }
            info 'The sea/land and water tables have been merged'
            [outputTableName: outputTableName]
        }
    }
}



