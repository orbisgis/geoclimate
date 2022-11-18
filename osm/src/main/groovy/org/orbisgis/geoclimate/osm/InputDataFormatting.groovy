package org.orbisgis.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.orbisgis.data.api.dataset.ISpatialTable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

import java.util.regex.Pattern

@BaseScript OSM OSM

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
 * @return outputTableName The name of the final buildings table
 * @return outputEstimatedTableName The name of the table containing the state of estimation for each building
 */
IProcess formatBuildingLayer() {
    return create {
        title "Transform OSM buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatBuildingLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, h_lev_min: 3, h_lev_max: 15, hThresholdLev2: 10, jsonFilename: "", urbanAreasTableName: ""
        outputs outputTableName: String, outputEstimateTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, epsg, h_lev_min, h_lev_max, hThresholdLev2, jsonFilename, urbanAreasTableName ->

            if (!h_lev_min) {
                h_lev_min = 3
            }
            if (!h_lev_max) {
                h_lev_max = 15
            }
            if (!hThresholdLev2) {
                hThresholdLev2 = 10
            }

            def outputTableName = postfix "INPUT_BUILDING"
            debug 'Formating building layer'
            def outputEstimateTableName = "EST_${outputTableName}"
            datasource """
                    DROP TABLE if exists ${outputEstimateTableName};
                    CREATE TABLE ${outputEstimateTableName} (
                        id_build INTEGER,
                        ID_SOURCE VARCHAR,
                        estimated boolean)
                """.toString()

            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build INTEGER, ID_SOURCE VARCHAR, 
                    HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER);
            """.toString()
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
                        queryMapper += " , st_force2D(a.the_geom) as the_geom FROM $inputTableName as a,  $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom and st_intersects( " +
                                "a.the_geom, b.the_geom) and st_area(a.the_geom)>1 and st_isempty(a.the_geom)=false "
                    } else {
                        queryMapper += " , st_force2D(a.the_geom) as the_geom FROM $inputTableName as a where st_area(a.the_geom)>1 "
                    }

                    def heightPattern = Pattern.compile("((?:\\d+\\/|(?:\\d+|^|\\s)\\.)?\\d+)\\s*([^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?(?:\\/[^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?)*)?", Pattern.CASE_INSENSITIVE)
                    def id_build = 1;
                    datasource.withBatch(100) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            String height = row.height
                            String roof_height = row.'roof:height'
                            String b_lev = row.'building:levels'
                            String roof_lev = row.'roof:levels'
                            def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                            def use = typeAndUseValues[1]
                            def type = typeAndUseValues[0]
                            if (type) {
                                def heightRoof = getHeightRoof(height, heightPattern)
                                def heightWall = getHeightWall(heightRoof, roof_height)
                                def nbLevels = getNbLevels(b_lev, roof_lev)
                                if (nbLevels >= 0) {
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

                                                stmt.addBatch """
                                                INSERT INTO ${outputEstimateTableName} values(
                                                    $id_build, 
                                                    '${row.id}',
                                                    ${formatedHeight.estimated})
                                                """.toString()

                                                id_build++
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    //Improve building type using the urban areas table
                    if (urbanAreasTableName) {
                        datasource."$outputTableName".the_geom.createSpatialIndex()
                        datasource."$outputTableName".id_build.createIndex()
                        datasource."$outputTableName".type.createIndex()
                        datasource."$urbanAreasTableName".the_geom.createSpatialIndex()
                        def buildinType = "BUILDING_TYPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                        datasource.execute """create table $buildinType as SELECT max(b.type) as type, max(b.main_use) as main_use, a.id_build FROM $outputTableName a, $urbanAreasTableName b 
                        WHERE a.the_geom && b.the_geom and st_intersects(ST_POINTONSURFACE(a.the_geom), b.the_geom) AND  a.TYPE ='building' group by a.id_build""".toString()

                        datasource.execute("CREATE INDEX ON $buildinType(id_build)".toString())

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
                                        a LEFT JOIN $buildinType b on a.id_build=b.id_build""".toString()

                        datasource.execute """DROP TABLE IF EXISTS $buildinType, $outputTableName;
                        ALTER TABLE $newBuildingWithType RENAME TO $outputTableName;""".toString()
                    }
                }
            }
            debug 'Buildings transformation finishes'
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
            debug('Formating road layer')
            def outputTableName = postfix "INPUT_ROAD"
            datasource """
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(GEOMETRY, $epsg), id_road serial, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, CROSSING VARCHAR(30),
                SURFACE VARCHAR, SIDEWALK VARCHAR, MAXSPEED INTEGER, DIRECTION INTEGER, ZINDEX INTEGER);
        """.toString()
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
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_makevalid(st_intersection(a.the_geom, b.the_geom))) " +
                                "ELSE st_force2D(a.the_geom) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom"
                    } else {
                        queryMapper += ", st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"
                    }
                    int rowcount = 1
                    def speedPattern = Pattern.compile("([0-9]+)( ([a-zA-Z]+))?", Pattern.CASE_INSENSITIVE)
                    datasource.withBatch(100) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def processRow = true
                            def road_access = row.'access'
                            def road_area = row.'area'
                            if(road_area in['yes']){
                                processRow = false
                            }
                            def road_service = row.'service'
                            if (road_service && road_service in ["parking_aisle", "alley", "slipway", "drive-through", "driveway"]) {
                                processRow = false
                            }
                            if (road_access && road_access in ["agricultural", "forestry"]) {
                                processRow = false
                            }

                            if (processRow) {
                                String type = getTypeValue(row, columnNames, mappingForRoadType)
                                def width = getWidth(row.'width')
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
                                //maxspeed value
                                int maxspeed_value = getSpeedInKmh(speedPattern, row."maxspeed")

                                String onewayValue = row."oneway"
                                int direction = 3;
                                if (onewayValue && onewayValue == "yes") {
                                    direction = 1
                                }

                                if (zIndex >= 0 && type) {
                                    Geometry geom = row.the_geom
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        stmt.addBatch """
                                    INSERT INTO $outputTableName VALUES(ST_GEOMFROMTEXT(
                                        '${geom.getGeometryN(i)}',$epsg), 
                                        ${rowcount++}, 
                                        '${row.id}', 
                                        ${width},
                                        '${type}',
                                        ${crossing}, 
                                        '${surface}',
                                        '${sidewalk}',
                                        ${maxspeed_value},
                                        ${direction},
                                        ${zIndex})
                                """.toString()

                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug('Roads transformation finishes')
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
            debug('Rails transformation starts')
            def outputTableName = "INPUT_RAILS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(GEOMETRY, $epsg), id_rail serial,ID_SOURCE VARCHAR, TYPE VARCHAR,CROSSING VARCHAR(30), ZINDEX INTEGER);""".toString()

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
                                "THEN st_force2D(st_makevalid(st_intersection(a.the_geom, b.the_geom))) " +
                                "ELSE st_force2D(a.the_geom) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", st_force2D(st_makevalid(a.the_geom)) as the_geom FROM $inputTableName  as a"

                    }
                    int rowcount = 1
                    datasource.withBatch(100) { stmt ->
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
                                    ${rowcount++}, 
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
            debug('Rails transformation finishes')
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
            debug('Vegetation transformation starts')
            def outputTableName = postfix "INPUT_VEGET"
            datasource """ 
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_veget serial, ID_SOURCE VARCHAR, TYPE VARCHAR, HEIGHT_CLASS VARCHAR(4), ZINDEX INTEGER);""".toString()
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
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_intersection(a.the_geom, b.the_geom)) " +
                                "ELSE a.the_geom " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) "
                    } else {
                        queryMapper += ", st_force2D(a.the_geom) as the_geom FROM $inputTableName  as a"
                    }
                    int rowcount = 1
                    datasource.withBatch(100) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def type = getTypeValue(row, columnNames, mappingType)
                            if (type) {
                                def height_class = typeAndVegClass[type]
                                def zindex = getZIndex(row."layer")
                                Geometry geom = row.the_geom
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    Geometry subGeom = geom.getGeometryN(i)
                                    if (subGeom instanceof Polygon) {
                                        stmt.addBatch """
                                            INSERT INTO $outputTableName VALUES(
                                                ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                ${rowcount++}, 
                                                '${row.id}',
                                                '${type}', 
                                                '${height_class}', ${zindex})
                                    """.toString()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug('Vegetation transformation finishes')
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
            debug('Hydro transformation starts')
            def outputTableName = "INPUT_HYDRO_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_hydro serial, ID_SOURCE VARCHAR, TYPE VARCHAR, ZINDEX INTEGER);""".toString()

            if (inputTableName != null) {
                ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
                if (inputSpatialTable.rowCount > 0) {
                    inputSpatialTable.the_geom.createSpatialIndex()
                    String query
                    if (inputZoneEnvelopeTableName) {
                        query = "select id , CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_intersection(a.the_geom, b.the_geom)) " +
                                "ELSE a.the_geom " +
                                "END AS the_geom , a.\"natural\", a.\"layer\"" +
                                " FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) "
                    } else {
                        query = "select id,  st_force2D(the_geom) as the_geom, \"natural\", \"layer\" FROM $inputTableName "

                    }
                    int rowcount = 1
                    datasource.withBatch(100) { stmt ->
                        datasource.eachRow(query) { row ->
                            def water_type = row.natural in ['bay', 'strait"'] ? 'sea' : 'water'
                            def zIndex = getZIndex(row.'layer')
                            Geometry geom = row.the_geom
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon) {
                                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.id}', '${water_type}', ${zIndex})".toString()
                                }
                            }
                        }
                    }
                }
            }
            debug('Hydro transformation finishes')
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
            debug('Impervious transformation starts')
            def outputTableName = "INPUT_IMPERVIOUS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_impervious serial, ID_SOURCE VARCHAR);""".toString()
            debug(inputTableName)
            if (inputTableName) {
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
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_intersection(a.the_geom, b.the_geom)) " +
                                "ELSE a.the_geom " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", st_force2D(a.the_geom) as the_geom FROM $inputTableName  as a"
                    }
                    int rowcount = 1
                    datasource.withBatch(100) { stmt ->
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
                                if(!geom.isEmpty()) {
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        Geometry subGeom = geom.getGeometryN(i)
                                        if(!subGeom.isEmpty()){
                                        if (subGeom instanceof Polygon) {
                                            stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.id}')".toString()
                                        }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug('Impervious transformation finishes')
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
static String[] getTypeAndUse(def row, def columnNames, def myMap) {
    String strType = null
    String strUse = null
    myMap.each { finalVal ->
        def type_use = finalVal.key.split(":")
        def type
        def use
        if (type_use.size() == 2) {
            type = type_use[0]
            use = type_use[1]
        } else {
            type = finalVal.key;
            use = type
        }
        finalVal.value.each { osmVals ->
            if (columnNames.contains(osmVals.key)) {
                def columnValue = row.getString(osmVals.key)
                if (columnValue != null) {
                    osmVals.value.each { osmVal ->
                        if (osmVal.startsWith("!")) {
                            osmVal = osmVal.replace("! ", "")
                            if ((columnValue != osmVal) && (columnValue != null)) {
                                if (strType == null) {
                                    strType = type
                                    strUse = use
                                }
                            }
                        } else {
                            if (columnValue == osmVal) {
                                if (strType == null) {
                                    strType = type
                                    strUse = use
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if (strUse == null) {
        strUse = strType
    }
    return [strType, strUse]
}

/**
 * This function defines the value of the column height_wall according to the values of height and r_height
 * @param height value of the building height
 * @param r_height value of the roof height
 * @return The calculated value of height_wall (default value : 0)
 */

static float getHeightWall(height, r_height) {
    float result = 0
    if (r_height != null && r_height.isFloat()) {
        if (r_height.toFloat() < height) {
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
static Map formatHeightsAndNbLevels(def heightWall, def heightRoof, def nbLevels, def h_lev_min,
                                    def h_lev_max, def hThresholdLev2, def nbLevFromType) {
    //Use the OSM values
    if (heightWall != 0 && heightRoof != 0 && nbLevels != 0) {
        return [heightWall: heightWall, heightRoof: heightRoof, nbLevels: nbLevels, estimated: false]
    }
    //Initialisation of heights and number of levels
    // Update height_wall
    boolean estimated = false
    if (heightWall == 0) {
        if (heightRoof == 0) {
            if (nbLevels == 0) {
                heightWall = h_lev_min
                estimated = true
            } else {
                heightWall = h_lev_min * nbLevels
            }
        } else {
            heightWall = heightRoof
        }
    }
    // Update heightRoof
    if (heightRoof == 0) {
        heightRoof = heightWall
    }
    // Update nbLevels
    // If the nb_lev parameter  is equal to 1 or 2
    // (and height_wall > 10m) then apply the rule. Else, the nb_lev is equal to 1
    if (nbLevels == 0) {
        nbLevels = 1
        if (nbLevFromType == 1 || (nbLevFromType == 2 && heightWall > hThresholdLev2)) {
            nbLevels = Math.floor(heightWall / h_lev_min)
        }
    }

    // Control of heights and number of levels
    // Check if height_roof is lower than height_wall. If yes, then correct height_roof
    if (heightWall > heightRoof) {
        heightRoof = heightWall
    }
    def tmpHmin = nbLevels * h_lev_min
    // Check if there is a high difference between the "real" and "theorical (based on the level number) roof heights
    if (tmpHmin > heightRoof) {
        heightRoof = tmpHmin
    }
    def tmpHmax = nbLevels * h_lev_max
    if (nbLevFromType == 1 || nbLevFromType == 2 && heightWall > hThresholdLev2) {
        if (tmpHmax < heightWall) {
            nbLevels = Math.floor(heightWall / h_lev_max)
        }
    }
    return [heightWall: heightWall, heightRoof: heightRoof, nbLevels: nbLevels, estimated: estimated]

}


/**
 * This function defines the value of the column height_roof according to the values of height and b_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_roof (default value : 0)
 */
static float getHeightRoof(height, heightPattern) {
    if (!height) return 0
    def matcher = heightPattern.matcher(height)
    if (!matcher.find()) {
        return 0
    }
    def new_h = 0
    def match1_group1 = matcher.group(1)
    def match1_group2 = matcher.group(2)
    //next match for feet, inch
    if (matcher.find()) {
        def match2_group1 = matcher.group(1)
        def match2_group2 = matcher.group(2)
        if (match1_group1) {
            new_h = Float.parseFloat(match1_group1) * 12
        }
        if (match2_group2 == "''") {
            new_h += Float.parseFloat(match2_group1)
        }
        return new_h * 0.0254

    } else {
        if (match1_group1 && match1_group2 == null) {
            return Float.parseFloat(match1_group1)
        }
        //next mach for feet, inch matcher.find();
        else {
            def type = match1_group2.toLowerCase()
            switch (type) {
                case "m":
                    return Float.parseFloat(match1_group1)
                case "foot":
                    return Float.parseFloat(match1_group1) * 0.3048
                case "'":
                    return Float.parseFloat(match1_group1) * 12 * 0.0254
                case "''":
                    return Float.parseFloat(match1_group1) * 0.0254
                default:
                    return 0
            }
        }
    }
}

/**
 * This function defines the value of the column nb_lev according to the values of b_lev and r_lev
 * @param row The row of the raw table to examine
 * @return The calculated value of nb_lev (default value : 0)
 */
static int getNbLevels(b_lev, r_lev) {
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
static Float getWidth(String width) {
    return (width != null && width.isFloat()) ? width.toFloat() : 0
}

/**
 * This function defines the value of the column zindex according to the value of zindex from OSM
 * @param width The original zindex value
 * @return The calculated value of zindex (default value : null)
 */
static int getZIndex(String zindex) {
    return (zindex != null && zindex.isInteger()) ? zindex.toInteger() : 0
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
            if (columnNames.contains(osmVals.key)) {
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
    switch (sidewalk) {
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
static String columnsMapper(def inputColumns, def columnsToMap) {
    //def flatList = "\"${inputColumns.join("\",\"")}\""
    def flatList = inputColumns.inject([]) { result, iter ->
        result += "a.\"$iter\""
    }.join(",")

    columnsToMap.each { it ->
        if (!inputColumns*.toLowerCase().contains(it)) {
            flatList += ", null as \"${it}\""
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
 * This process is used to transform the urban areas  table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw urban areas table
 * @param inputTableName The name of the urban areas table
 * @return outputTableName The name of the final urban areas table
 */
IProcess formatUrbanAreas() {
    return create {
        title "Format the urban areas table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatUrbanAreas"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, jsonFilename: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName, epsg, jsonFilename ->
            debug('Urban areas transformation starts')
            def outputTableName = "INPUT_URBAN_AREAS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY(POLYGON, $epsg), id_urban serial, ID_SOURCE VARCHAR, TYPE VARCHAR, MAIN_USE VARCHAR);""".toString()

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
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_intersection(a.the_geom, b.the_geom)) " +
                                "ELSE a.the_geom " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ",  st_force2D(a.the_geom) as the_geom FROM $inputTableName  as a"

                    }
                    int rowcount = 1
                    datasource.withBatch(100) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def typeAndUseValues = getTypeAndUse(row, columnNames, mappingType)
                            def use = typeAndUseValues[1]
                            def type = typeAndUseValues[0]
                            Geometry geom = row.the_geom
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon) {
                                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.id}', '${type}','${use}')".toString()
                                }
                            }
                        }
                    }
                }
            }
            debug('Urban areas transformation finishes')
            [outputTableName: outputTableName]
        }
    }
}

/**
 * This process is used to build a sea-land mask layer from the coastline and zone table
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the coastlines table in the DB
 * @param inputZoneEnvelopeTableName The name of the zone table to limit the area
 * @param inputWaterTableName The name of the input water table to improve sea extraction
 * @return outputTableName The name of the final buildings table
 */
IProcess formatSeaLandMask() {
    return create {
        title "Extract the sea/land mask"
        id "formatSeaLandMask"
        inputs datasource: JdbcDataSource, inputTableName: String,
                inputZoneEnvelopeTableName: String, inputWaterTableName: "", epsg: int
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, inputWaterTableName, epsg ->
            def outputTableName = postfix "INPUT_SEA_LAND_MASK_"
            debug 'Computing sea/land mask table'
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id serial, TYPE VARCHAR);
            """.toString()
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
                        def sea_land_mask = "sea_land_mask${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def sea_land_mask_in_zone = "sea_land_mask_in_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def water_to_be_filtered ="water_to_be_filtered${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        def water_filtered_exploded = "water_filtered_exploded${UUID.randomUUID().toString().replaceAll("-", "_")}"
                        datasource.execute """DROP TABLE IF EXISTS $outputTableName, $coastLinesIntersects, 
                        $islands_mark, $mergingDataTable,  $coastLinesIntersectsPoints, $coastLinesPoints,$sea_land_mask,
                        $sea_land_mask_in_zone,$water_filtered_exploded,$water_to_be_filtered;

                        CREATE TABLE $coastLinesIntersects AS SELECT a.the_geom
                        from $inputTableName  AS  a,  $inputZoneEnvelopeTableName  AS b WHERE
                        a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom);     
                        """.toString()

                        if (inputWaterTableName) {
                            datasource.getSpatialTable(inputWaterTableName).the_geom.createSpatialIndex()
                            def islands_mark_filtered = "islands_mark_filtered${UUID.randomUUID().toString().replaceAll("-", "_")}"
                            datasource.execute """ DROP TABLE IF EXISTS $islands_mark_filtered;
                        CREATE TABLE $islands_mark_filtered (the_geom GEOMETRY, ID SERIAL) AS 
                       SELECT the_geom, EXPLOD_ID  FROM st_explode('(  
                       SELECT ST_LINEMERGE(st_accum(THE_GEOM)) AS the_geom, NULL FROM $coastLinesIntersects)');
                        CREATE SPATIAL INDEX ON $islands_mark_filtered(the_geom);
                        CREATE TABLE  $islands_mark AS select * from $islands_mark_filtered where id not in( SELECT a.id from $islands_mark_filtered as a,  $inputWaterTableName as b
                        where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and b.type = 'water' and b.zindex=0)""".toString()

                            datasource.execute """  
                        CREATE TABLE $mergingDataTable  AS
                        SELECT  THE_GEOM FROM $coastLinesIntersects 
                        UNION ALL
                        SELECT st_tomultiline(the_geom)
                        from $inputZoneEnvelopeTableName ;

                        CREATE TABLE $sea_land_mask (THE_GEOM GEOMETRY,ID serial, TYPE VARCHAR, ZINDEX INTEGER) AS SELECT THE_GEOM, EXPLOD_ID, 'land', 0 AS ZINDEX FROM
                        st_explode('(SELECT st_polygonize(st_union(ST_NODE(st_accum(the_geom)))) AS the_geom FROM $mergingDataTable)');                
                        
                          CREATE SPATIAL INDEX IF NOT EXISTS ${sea_land_mask}_the_geom_idx ON $sea_land_mask (THE_GEOM);

                        CREATE TABLE $sea_land_mask_in_zone as SELECT st_intersection(a.THE_GEOM, b.the_geom) as the_geom, a.id, a.type,a.ZINDEX 
                        FROM $sea_land_mask as a, $inputZoneEnvelopeTableName  as b;                
                       
                       
                        CREATE TABLE $coastLinesPoints as  SELECT ST_LocateAlong(the_geom, 0.5, -0.01) AS the_geom FROM 
                        st_explode('(select ST_GeometryN(ST_ToMultiSegments(st_intersection(a.the_geom, b.the_geom)), 1) as the_geom from $islands_mark as a,
                        $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom))');
    
                        CREATE TABLE $coastLinesIntersectsPoints as  SELECT the_geom FROM st_explode('$coastLinesPoints'); 

                        CREATE INDEX IF NOT EXISTS ${sea_land_mask_in_zone}_id_idx ON $sea_land_mask_in_zone (id);

                        CREATE SPATIAL INDEX IF NOT EXISTS ${coastLinesIntersectsPoints}_the_geom_idx ON $coastLinesIntersectsPoints (THE_GEOM);

                        UPDATE $sea_land_mask_in_zone SET TYPE='sea' WHERE ID IN(SELECT DISTINCT(a.ID)
                                FROM $sea_land_mask_in_zone a, $coastLinesIntersectsPoints b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_contains(a.THE_GEOM, b.THE_GEOM));
                        
                        DROP TABLE IF EXISTS $water_to_be_filtered;
                        CREATE TABLE $water_to_be_filtered AS
                        SELECT st_polygonize(st_union(ST_ToMultiLine(a.the_geom), ST_ToMultiLine(st_accum(b.the_geom)))) AS the_geom, 
                        a.id AS id_sea FROM  $sea_land_mask_in_zone AS a, 
                        $inputWaterTableName AS b
                         WHERE a."TYPE" ='land'  and a.the_geom && b.the_geom and st_contains(a.the_geom, ST_PointOnSurface(b.the_geom)) GROUP BY a.id;

                        DROP TABLE IF EXISTS $water_filtered_exploded;
                        CREATE TABLE $water_filtered_exploded AS 

                        SELECT st_buffer(the_geom, -0.0001) AS the_geom, 'land' AS type ,id_sea, EXPLOD_ID AS ID  
                        FROM  st_explode('$water_to_be_filtered');

                        CREATE spatial INDEX ON $water_filtered_exploded(the_geom);
                        CREATE  INDEX ON $water_filtered_exploded(id);

                        UPDATE $water_filtered_exploded SET TYPE='sea' WHERE ID IN(SELECT DISTINCT(a.ID)
                        FROM $water_filtered_exploded a, $inputWaterTableName b WHERE a.THE_GEOM && b.THE_GEOM AND
                        st_contains(b.THE_GEOM, a.the_geom) );    
                               
                        CREATE TABLE $outputTableName as select the_geom, ROWNUM() AS id, TYPE
                        from (
                        SELECT st_buffer(the_geom, -0.0001) AS the_geom, type FROM $sea_land_mask_in_zone  
                        WHERE id NOT in(SELECT id_sea FROM $water_filtered_exploded)
                        UNION 
                        SELECT the_geom, 'land' AS TYPE FROM   $water_filtered_exploded WHERE ID NOT IN(SELECT DISTINCT(a.ID)
                                FROM $water_filtered_exploded a, $inputWaterTableName b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_contains(b.THE_GEOM, a.the_geom) )) as foo; 
                        
                         """.toString()

                        } else {
                         datasource.execute """
                        CREATE TABLE $islands_mark (the_geom GEOMETRY, ID SERIAL) AS 
                       SELECT the_geom, EXPLOD_ID  FROM st_explode('(  
                       SELECT ST_LINEMERGE(st_accum(THE_GEOM)) AS the_geom, NULL FROM $coastLinesIntersects)');""".toString()

                        datasource.execute """  
                        CREATE TABLE $mergingDataTable  AS
                        SELECT  THE_GEOM FROM $coastLinesIntersects 
                        UNION ALL
                        SELECT st_tomultiline(the_geom)
                        from $inputZoneEnvelopeTableName ;

                        CREATE TABLE $sea_land_mask (THE_GEOM GEOMETRY,ID serial, TYPE VARCHAR, ZINDEX INTEGER) AS SELECT THE_GEOM, EXPLOD_ID, 'land', 0 AS ZINDEX FROM
                        st_explode('(SELECT st_polygonize(st_union(ST_NODE(st_accum(the_geom)))) AS the_geom FROM $mergingDataTable)');                
                        
                        CREATE SPATIAL INDEX IF NOT EXISTS ${sea_land_mask}_the_geom_idx ON $sea_land_mask (THE_GEOM);

                        CREATE TABLE $outputTableName as select the_geom, id, type, ZINDEX from st_explode('(SELECT st_intersection(a.THE_GEOM, b.the_geom) as the_geom, a.id, a.type,a.ZINDEX 
                        FROM $sea_land_mask as a, $inputZoneEnvelopeTableName  as b)');                
                       
                       
                        CREATE TABLE $coastLinesPoints as  SELECT ST_LocateAlong(the_geom, 0.5, -0.01) AS the_geom FROM 
                        st_explode('(select ST_GeometryN(ST_ToMultiSegments(st_intersection(a.the_geom, b.the_geom)), 1) as the_geom from $islands_mark as a,
                        $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom))');
    
                        CREATE TABLE $coastLinesIntersectsPoints as  SELECT the_geom FROM st_explode('$coastLinesPoints'); 

                        CREATE INDEX IF NOT EXISTS ${outputTableName}_id_idx ON $outputTableName (id);

                        CREATE SPATIAL INDEX IF NOT EXISTS ${coastLinesIntersectsPoints}_the_geom_idx ON $coastLinesIntersectsPoints (THE_GEOM);

                        UPDATE $outputTableName SET TYPE='sea' WHERE ID IN(SELECT DISTINCT(a.ID)
                                FROM $outputTableName a, $coastLinesIntersectsPoints b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_contains(a.THE_GEOM, b.THE_GEOM));   
                        
                         """.toString()
                        }

                        datasource.execute("""drop table if exists $mergingDataTable, $coastLinesIntersects, $coastLinesIntersectsPoints, $coastLinesPoints,
                                $islands_mark, $sea_land_mask,$sea_land_mask_in_zone,$water_filtered_exploded,$water_to_be_filtered
                        """.toString())

                    } else {
                        debug "A zone table must be provided to compute the sea/land mask"
                    }
                } else {
                    debug "The sea/land mask table is empty"
                }
            }
            debug 'The sea/land mask has been computed'
            [outputTableName: outputTableName]
        }
    }
}


/**
 * This process is used to merge the water and the sea-land mask layers
 *
 * @param datasource A connexion to a DB  that contains the tables
 * @param inputSeaLandTableName The name of the sea/land table
 * @param inputWaterTableName The name of the water table
 * @return outputTableName The name of the final water table
 */
IProcess mergeWaterAndSeaLandTables() {
    return create {
        title "Extract the sea/land mask"
        id "formatSeaLandMask"
        inputs datasource: JdbcDataSource, inputSeaLandTableName: String, inputWaterTableName: String, epsg: int
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputSeaLandTableName, inputWaterTableName, epsg ->
            def outputTableName = postfix "INPUT_WATER_SEA_"
            debug 'Merging sea/land mask and water table'
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_hydro serial, id_source VARCHAR, ZINDEX INTEGER);
            """.toString()
            if (inputSeaLandTableName && inputWaterTableName) {
                if (datasource.firstRow("select count(*) as count from $inputSeaLandTableName where TYPE ='sea'".toString()).count > 0) {
                    def tmp_water_not_in_sea = "WATER_NOT_IN_SEA${UUID.randomUUID().toString().replaceAll("-", "_")}"
                    //This method is used to merge the SEA mask with the water table
                    def queryMergeWater = """DROP  TABLE IF EXISTS $outputTableName, $tmp_water_not_in_sea;
                CREATE TABLE $tmp_water_not_in_sea AS SELECT a.the_geom, a.ID_SOURCE, a.zindex FROM $inputWaterTableName AS a, $inputSeaLandTableName  AS b
                WHERE b."TYPE"= 'land' AND a.the_geom && b.the_geom AND st_contains(b.THE_GEOM, st_pointonsurface(a.THE_GEOM)) and a.type='water';
                CREATE TABLE $outputTableName(the_geom GEOMETRY, ID_HYDRO SERIAL, ID_SOURCE VARCHAR, ZINDEX INTEGER) AS SELECT THE_GEOM, ROWNUM() , 
                id_source, zindex 
                FROM (SELECT the_geom,  id_source, zindex FROM $tmp_water_not_in_sea UNION ALL 
                SELECT THE_GEOM,  '-1', 0 FROM $inputSeaLandTableName  WHERE
                "TYPE" ='sea') AS foo;"""
                    //Check indexes before executing the query
                    datasource.getSpatialTable(inputWaterTableName).the_geom.createSpatialIndex()
                    datasource.getSpatialTable(inputSeaLandTableName).the_geom.createSpatialIndex()
                    datasource.execute queryMergeWater.toString()
                    datasource.execute("drop table if exists $tmp_water_not_in_sea;".toString())
                } else {
                    return [outputTableName: inputWaterTableName]
                }
            }
            debug 'The sea/land and water tables have been merged'
            return [outputTableName: outputTableName]
        }
    }
}


/**
 * Return a maxspeed value expressed in kmh
 * @param maxspeedValue from OSM
 * @return
 */
static int getSpeedInKmh(def speedPattern, String maxspeedValue) {
    if (!maxspeedValue) return -1
    def matcher = speedPattern.matcher(maxspeedValue)
    if (!matcher.matches()) return -1

    def speed = Integer.parseInt(matcher.group(1))

    if (!(matcher.group(3))) return speed

    def type = matcher.group(3).toLowerCase()
    switch (type) {
        case "kmh":
            return speed
        case "mph":
            return Math.round(speed * 1.609)
        default:
            return -1
    }
}




