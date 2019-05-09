package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript PrepareData prepareData

/**
 * This process is used to transform the raw buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param mappingForTypeAndUse A map between the target values for type and use in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final buildings table
 */
static IProcess transformBuildings() {
    return processFactory.create(
            "transform the raw buildings table into a table that matches the constraints of the GeoClimate Input Model",
            [datasource          : JdbcDataSource,
             inputTableName      : String,
             mappingForTypeAndUse: Map],
            [outputTableName: String],
            { datasource, inputTableName, mappingForTypeAndUse ->
                def inputTable = datasource.getSpatialTable(inputTableName)
                logger.info('Buildings transformation starts')
                datasource.execute("    drop table if exists input_building;\n" +
                        "CREATE TABLE input_building (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT,\n" +
                        "        NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER)")
                inputTable.eachRow { row ->
                    def heightWall = getHeightWall(row)
                    def heightRoof = getHeightRoof(row)
                    def nbLevels = getNbLevels(row)
                    def typeAndUseValues = getTypeAndUse(row, mappingForTypeAndUse)
                    datasource.execute("insert into input_building values('${row.the_geom}','${row.id_way}',${heightWall},${heightRoof},${nbLevels},'${typeAndUseValues[0]}','${typeAndUseValues[1]}',${row.zindex})".toString())
                }
                logger.info('Buildings transformation finishes')
                [outputTableName: "INPUT_BUILDING"]
            }
    )
}

/**
 * This process is used to transform the raw roads table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw roads table
 * @param inputTableName The name of the raw roads table in the DB
 * @param mappingForType A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @param mappingForSurface A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final roads table
 */
static IProcess transformRoads() {
    return processFactory.create(
            "transform the raw roads table into a table that matches the constraints of the GeoClimate Input Model",
            [datasource          : JdbcDataSource,
             inputTableName      : String,
             mappingForRoadType: Map,
             mappingForSurface: Map],
            [outputTableName: String],
            { datasource, inputTableName, mappingForRoadType, mappingForSurface ->
                def inputTable = datasource.getSpatialTable(inputTableName)
                logger.info('Roads transformation starts')
                datasource.execute("    drop table if exists input_road;\n" +
                        "CREATE TABLE input_road (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR,\n" +
                        "SURFACE VARCHAR, SIDEWALK VARCHAR, ZINDEX INTEGER)")
                inputTable.eachRow { row ->
                    Float width = getWidth(row.width)
                    String type = getAbstractValue(row, mappingForRoadType)
                    String surface = getAbstractValue(row, mappingForSurface)
                    String sidewalk = getSidewalk(row.sidewalk)
                    Integer zIndex = getZIndex(row.zindex)
                    datasource.execute ("insert into input_road values('${row.the_geom}','${row.id_way}',${width},'${type}','${surface}','${sidewalk}',${zIndex})".toString())
                }
                logger.info('Roads transformation finishes')
                [outputTableName: "INPUT_ROAD"]
            }
    )
}

/**
 * This process is used to transform the raw rails table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw rails table
 * @param inputTableName The name of the raw rails table in the DB
 * @param mappingForType A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final rails table
 */
static IProcess transformRails() {
    return processFactory.create(
            "transform the raw rails table into a table that matches the constraints of the GeoClimate Input Model",
            [datasource          : JdbcDataSource,
             inputTableName      : String,
             mappingForRailType: Map],
            [outputTableName: String],
            { datasource, inputTableName, mappingForRailType ->
                def inputTable = datasource.getSpatialTable(inputTableName)
                logger.info('Rails transformation starts')
                datasource.execute("    drop table if exists input_rail;\n" +
                        "CREATE TABLE input_rail (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, TYPE VARCHAR, ZINDEX INTEGER)")
                inputTable.eachRow { row ->
                    String type = getAbstractValue(row, mappingForRailType)
                    Integer zIndex = getZIndex(row.layer)

                    //special treatment if type is subway
                    if (type == "subway") {

                        if (!((row.tunnel != null && row.tunnel == "no" && row.layer != null && row.layer.toInt() >= 0)
                                || (row.bridge != null && (row.bridge == "yes" || row.bridge == "viaduct")))) {
                            type = null
                        }
                    }
                    def query = "insert into input_rail values(${row.the_geom},${row.id_way},${type},${zIndex})"
                    datasource.execute (query)
                }
                def geoFile = File.createTempFile("rail",".geojson")
                datasource.save('INPUT_RAIL', geoFile.getAbsolutePath())
                logger.info('Rails transformation finishes')
                [outputTableName: "INPUT_RAIL"]
            }
    )
}

/**
 * This process is used to transform the raw vegetation table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw vegetation table
 * @param inputTableName The name of the raw vegetation table in the DB
 * @param mappingForType A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final vegetation table
 */
static IProcess transformVeget() {
    return processFactory.create(
            "transform the raw vegetation table into a table that matches the constraints of the GeoClimate Input Model",
            [datasource    : JdbcDataSource,
             inputTableName: String,
             mappingForVegetType: Map],
            [outputTableName: String],
            { JdbcDataSource datasource, inputTableName, mappingForVegetType ->
                def inputTable = datasource.getSpatialTable(inputTableName)
                logger.info('Veget transformation starts')
                datasource.execute("    drop table if exists input_veget;\n" +
                        "CREATE TABLE input_veget (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, TYPE VARCHAR)")
                inputTable.eachRow { row ->
                    String type = getAbstractValue(row, mappingForVegetType)
                    def query = "insert into input_veget values(${row.the_geom},${row.id_source},${type})"
                    datasource.execute(query)
                }
                logger.info('Veget transformation finishes')
                [outputTableName: "INPUT_VEGET"]
            }
    )
}

/**
 * This process is used to transform the raw hydro table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw hydro table
 * @param inputTableName The name of the raw hydro table in the DB
 * @param mappingForType A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final hydro table
 */
static IProcess transformHydro() {
    return processFactory.create(
            "transform the raw hydro table into a table that matches the constraints of the GeoClimate Input Model",
            [datasource          : JdbcDataSource,
             inputTableName      : String],
            [outputTableName: String],
            { datasource, inputTableName ->
                def inputTable = datasource.getSpatialTable(inputTableName)
                logger.info('Veget transformation starts')
                datasource.execute("    drop table if exists input_hydro;\n" +
                        "CREATE TABLE input_hydro (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR)")
                inputTable.eachRow { row ->
                    def query = "insert into input_hydro values(${row.the_geom},${row.id_source})"
                    datasource.execute (query)
                }
                logger.info('Veget transformation finishes')
                [outputTableName: "INPUT_HYDRO"]
            }
    )
}

/**
 * This function defines the input values for both columns type and use to follow the constraints
 * of the geoClimate Input Model
 * @param row The row of the raw table to examine
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String[] getTypeAndUse(def row, def myMap) {
    String strType = null
    String strUse = null
    myMap.each { finalVal ->
        finalVal.value.each { osmVals ->
            osmVals.value.each { osmVal ->
                def val = osmVal.toString()
                if (val.startsWith("!")) {
                    val.replace("! ","")
                    if ((row.getString(osmVals.key) != val) && (row.getString(osmVals.key) != null)) {
                        if (strType == null) {
                            strType = finalVal.key
                        } else {
                            strUse = finalVal.key
                        }
                    }
                } else {
                    if (row.getString(osmVals.key) == val) {
                        if (strType == null) {
                            strType = finalVal.key
                        } else {
                            strUse = finalVal.key
                        }
                    }
                }
            }
        }
    }
    return [strType,strUse]
}

/**
 * This function defines the value of the column height_wall according to the values of height, b_height, r_height and b_r_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_wall (default value : 0)
 */
static float getHeightWall(def row) {
    String height = row.height
    String b_height = row.b_height
    String r_height = row.r_height
    String b_r_height = row.b_r_height
    Float result = 0
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
 * This function defines the value of the column height_roof according to the values of height and b_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_roof (default value : 0)
 */
static float getHeightRoof(def row) {
    String height = row.height
    String b_height = row.b_height
    Float result = 0
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
static int getNbLevels (def row) {
    String b_lev = row.b_lev
    String r_lev = row.r_lev
    String b_r_lev = row.b_r_lev
    int result = 0
    if (b_lev != null && b_lev.isFloat()) {
        if ((r_lev != null && r_lev.isFloat()) || (b_r_lev != null && b_r_lev.isFloat())) {
            if (r_lev.isFloat()) {
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
    return (width != null && width.isFloat()) ? width.toFloat() : null
}

/**
 * This function defines the value of the column zindex according to the value of zindex from OSM
 * @param width The original zindex value
 * @return The calculated value of zindex (default value : null)
 */
static Integer getZIndex (String zindex){
    return (zindex != null && zindex.isInteger()) ? zindex.toInteger() : null
}

/**
 * This function defines the input value for a column of the geoClimate Input Model according to a given mapping between the expected value and the set of key/values tag in OSM
 * @param row The row of the raw table to examine
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String getAbstractValue(def row, def myMap) {
    String strType = null
    myMap.each { finalVal ->
        finalVal.value.each { osmVals ->
            osmVals.value.each { osmVal ->
                def val = osmVal.toString()
                if (val.startsWith("!")) {
                    val.replace("! ","")
                    if ((row.getString(osmVals.key) != val) && (row.getString(osmVals.key) != null)) {
                        if (strType == null) {
                            strType = finalVal.key
                        }
                    }
                } else {
                    if (row.getString(osmVals.key) == val) {
                        if (strType == null) {
                            strType = finalVal.key
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


