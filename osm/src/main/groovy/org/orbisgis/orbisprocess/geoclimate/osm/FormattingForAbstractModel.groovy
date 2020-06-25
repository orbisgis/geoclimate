package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisdata.datamanager.api.dataset.ISpatialTable
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory

@BaseScript OSM_Utils osm_utils

/**
 * This process is used to format the OSM buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param epsg epsgcode to apply
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final buildings table
 */
IProcess formatBuildingLayer() {
    return create {
        title "Transform OSM buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatBuildingLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, h_lev_min: 3, h_lev_max: 15, hThresholdLev2: 10, jsonFilename: ""
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, epsg, h_lev_min, h_lev_max, hThresholdLev2, jsonFilename ->
            def outputTableName = postfix "INPUT_BUILDING"
            info 'Formating building layer'
            outputTableName = "INPUT_BUILDING_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build serial, ID_SOURCE VARCHAR, 
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
                    inputSpatialTable.the_geom.createSpatialIndex()
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        queryMapper += " , case when st_isvalid(a.the_geom) then a.the_geom else st_makevalid(st_force2D(a.the_geom)) end  as the_geom FROM $inputTableName as a,  $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom and st_intersects(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(a.the_geom) end, b.the_geom) and st_area(a.the_geom)>1"
                    } else {
                        queryMapper += " , case when st_isvalid(a.the_geom) then a.the_geom else st_makevalid(st_force2D(a.the_geom)) end as the_geom FROM $inputTableName as a where st_area(a.the_geom)>1"
                    }
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            String height = row.height
                            String b_height = row.'building:height'
                            String roof_height = row.'roof:height'
                            String b_roof_height = row.'building:roof:height'
                            String b_lev = row.'building:levels'
                            String roof_lev = row.'roof:levels'
                            String b_roof_lev = row.'building:roof:levels'
                            def heightWall = getHeightWall(height, b_height, roof_height, b_roof_height)
                            def heightRoof = getHeightRoof(height, b_height)

                            def nbLevels = getNbLevels(b_lev, roof_lev, b_roof_lev)

                            if (nbLevels >= 0) {
                                def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                                def use = typeAndUseValues[1]
                                def type = typeAndUseValues[0]
                                if (!type) {
                                    type = 'building'
                                }

                                def nbLevelFromType = typeAndLevel[type]

                                def formatedHeight = formatHeightsAndNbLevels(heightWall, heightRoof, nbLevels, h_lev_min,
                                        h_lev_max, hThresholdLev2, nbLevelFromType == null ? 0 : nbLevelFromType)

                                def zIndex = getZIndex(row.'layer')

                                if (formatedHeight.nbLevels > 0 && zIndex >= 0 && type) {
                                    Geometry geom = row.the_geom
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        Geometry subGeom = geom.getGeometryN(i)
                                        if (subGeom instanceof Polygon) {
                                            stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                    null, 
                                                    '${row.id}',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    '${type}',
                                                    '${use}',
                                                    ${zIndex})
                                        """.toString()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            info 'Buildings transformation finishes'
            [outputTableName: outputTableName]
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
                    inputSpatialTable.the_geom.createSpatialIndex()
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        queryMapper += ", CASE WHEN st_overlaps(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(a.the_geom) end, b.the_geom) then st_intersection(st_force2D(a.the_geom), b.the_geom) else a.the_geom end as the_geom FROM $inputTableName  as a, $inputZoneEnvelopeTableName as b where a.the_geom && b.the_geom"
                    } else {
                        queryMapper += ", a.the_geom FROM $inputTableName  as a"
                    }

                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
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
                    inputSpatialTable.the_geom.createSpatialIndex()
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")

                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        queryMapper += ", CASE WHEN st_overlaps(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(a.the_geom) end, b.the_geom) then st_intersection(st_force2D(a.the_geom), b.the_geom) else a.the_geom end as the_geom FROM $inputTableName  as a, $inputZoneEnvelopeTableName as b where a.the_geom && b.the_geom"
                    } else {
                        queryMapper += ", a.the_geom FROM $inputTableName  as a"

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
                    inputSpatialTable.the_geom.createSpatialIndex()
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")

                    queryMapper += columnsMapper(columnNames, columnToMap)
                    if (inputZoneEnvelopeTableName) {
                        queryMapper += ", CASE WHEN st_overlaps(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(st_force2D(a.the_geom)) end, b.the_geom) then st_makevalid(st_intersection(st_force2D(a.the_geom), b.the_geom)) else st_makevalid(a.the_geom) end as the_geom FROM $inputTableName  as a, $inputZoneEnvelopeTableName as b where a.the_geom && b.the_geom"
                    } else {
                        queryMapper += ", st_makevalid(st_force2D(a.the_geom)) the_geom FROM $inputTableName  as a"
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
                        query = "select id, CASE WHEN st_overlaps(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(st_force2D(a.the_geom)) end, b.the_geom) then st_makevalid(st_intersection(st_force2D(a.the_geom), b.the_geom)) else st_makevalid(a.the_geom) end as the_geom FROM $inputTableName  as a, $inputZoneEnvelopeTableName as b where a.the_geom && b.the_geom"
                    } else {
                        query = "select id,  st_makevalid(st_force2D(a.the_geom)) as the_geom FROM $inputTableName  as a"

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
                        queryMapper += ", CASE WHEN st_overlaps(CASE WHEN ST_ISVALID(a.the_geom) THEN a.the_geom else st_makevalid(st_force2D(a.the_geom)) end, b.the_geom) then st_makevalid(st_intersection(st_force2D(a.the_geom), b.the_geom)) else st_makevalid(a.the_geom) end as the_geom FROM $inputTableName  as a, $inputZoneEnvelopeTableName as b where a.the_geom && b.the_geom"
                    } else {
                        queryMapper += ", st_makevalid(st_force2D(a.the_geom)) as the_geom FROM $inputTableName  as a"
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
* This function defines the value of the column height_wall according to the values of height, b_height, r_height and b_r_height
* @param row The row of the raw table to examine
* @return The calculated value of height_wall (default value : 0)
*/
static float getHeightWall(height, b_height, r_height, b_r_height) {
    float result = 0
    if ((height != null && height.isFloat()) || (b_height != null && b_height.isFloat())) {
       if ((r_height != null && r_height.isFloat()) || (b_r_height != null && b_r_height.isFloat())) {
           if (b_height != null && b_height.isFloat()) {
               if (b_r_height != null && b_r_height.isFloat()) {
                    result = b_height.toFloat() - b_r_height.toFloat()
               } else {
                   result = b_height.toFloat() - r_height.toFloat()
               }
           } else {
                if (b_r_height != null && b_r_height.isFloat()) {
                    result = height.toFloat() - b_r_height.toFloat()
               } else {
                   result = height.toFloat() - r_height.toFloat()
               }
            }
        }
   }
 return result
}

/**
 * Rule to guarantee the height wall, height roof and number of levels values
 * @param height_wall value
 * @param height_roof value
 * @param nb_lev value
 * @param h_lev_min value
 * @param h_lev_max value
 * @param hThresholdLev2 value
 * @param nbLevFromType value
 * @param hThresholdLev2 value
 * @return a map with the new values
 */
static Map formatHeightsAndNbLevels(def heightWall, def heightRoof, def nbLevels, def h_lev_min,
                                   def h_lev_max,def hThresholdLev2, def nbLevFromType){
    //Initialisation of heights and number of levels
    // Update height_wall
    if(heightWall==0){
        if(heightRoof==0){
            if(nbLevels==0){
                heightWall= h_lev_min
            }
            else {
                heightWall= h_lev_min*nbLevels
            }
        }
        else {
            heightWall= heightRoof
        }
    }
    // Update height_roof
    if(heightRoof==0){
        if(heightWall==0){
            if(nbLevels==0){
                heightRoof= h_lev_min
            }
            else {
                heightRoof= h_lev_min*nbLevels
            }
        }
        else{
            heightRoof= heightWall
        }
    }
    // Update nb_lev
    // If the nb_lev parameter (in the abstract table) is equal to 1 or 2
    // (and height_wall > 10m) then apply the rule. Else, the nb_lev is equal to 1
    if(nbLevFromType==1 || nbLevFromType==2 && heightWall> hThresholdLev2){
        if(nbLevels==0){
            if(heightWall==0){
                if(heightRoof==0){
                    nbLevels= 1
                }
                else{
                    nbLevels= heightRoof/h_lev_min
                }
            }
            else {
                nbLevels= heightWall/h_lev_min
            }
        }
    }
    else{
        nbLevels = 1
    }

   // Control of heights and number of levels
   // Check if height_roof is lower than height_wall. If yes, then correct height_roof
    if(heightWall>heightRoof){
        heightRoof = heightWall
    }
    def tmpHmin=  nbLevels*h_lev_min
    // Check if there is a high difference beetween the "real" and "theorical (based on the level number) roof heights
    if(tmpHmin>heightRoof){
        heightRoof= tmpHmin
    }
    def tmpHmax=  nbLevels*h_lev_max
    if(nbLevFromType==1 || nbLevFromType==2 && heightWall> hThresholdLev2){
    if(tmpHmax<heightWall){
        nbLevels= heightWall/h_lev_max
    }
    }
    return [heightWall:heightWall, heightRoof:heightRoof, nbLevels:nbLevels]

}


/**
 * This function defines the value of the column height_roof according to the values of height and b_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_roof (default value : 0)
 */
static float getHeightRoof(height,b_height ) {
    float result = 0
    if ((height != null && height.isFloat()) || (b_height != null && b_height.isFloat())) {
        if (height != null && height.isFloat()) {
            result = height.toFloat()
        } else {
            result = b_height.toFloat()
        }
    }
    return result
}

/**
 * This function defines the value of the column nb_lev according to the values of b_lev, r_lev and b_r_lev
 * @param row The row of the raw table to examine
 * @return The calculated value of nb_lev (default value : 0)
 */
static int getNbLevels (b_lev ,r_lev,b_r_lev) {
    int result = 0
    if (b_lev != null && b_lev.isFloat()) {
        if ((r_lev != null && r_lev.isFloat()) || (b_r_lev != null && b_r_lev.isFloat())) {
            if (r_lev != null && r_lev.isFloat()) {
                result = b_lev.toFloat() + r_lev.toFloat()
            } else {
                result = b_lev.toFloat() + b_r_lev.toFloat()
            }
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