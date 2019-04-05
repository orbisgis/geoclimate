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
                String uid = UUID.randomUUID().toString().replaceAll("-", "")
                datasource.execute("    drop table if exists tmp_" + uid + ";\n" +
                        "CREATE TABLE tmp_" + uid + " (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT,\n" +
                        "        NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER)")
                inputTable.eachRow { row ->
                    def heightWall = getHeightWall(row)
                    def heightRoof = getHeightRoof(row)
                    def nbLevels = getNbLevels(row)
                    def typeAndUseValues = getTypeAndUse(row, mappingForTypeAndUse)
                    datasource.execute("insert into tmp_${uid} values('${row.getGeometry("the_geom")}','${row.getString("id_way")}',${heightWall},${heightRoof},${nbLevels},'${typeAndUseValues[0]}','${typeAndUseValues[1]}',${row.getString("zindex")})".toString())
                }

                datasource.execute("drop table if exists input_building; alter table tmp_"+uid+" rename to input_building")
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
             mappingForType: Map,
             mappingForSurface: Map],
            [outputTableName: String],
            { datasource, inputTableName, mappingForType, mappingForSurface ->
                def inputTable = datasource.getSpatialTable(inputTableName)
                String uid = UUID.randomUUID().toString().replaceAll("-", "")
                datasource.execute("    drop table if exists tmp_" + uid + ";\n" +
                        "CREATE TABLE tmp_" + uid + " THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, \" +\n" +
                        "            \"SURFACE VARCHAR, SIDEWALK VARCHAR, ZINDEX INTEGER)")
                inputTable.eachRow { row ->
                    Float width = getWidth(row.getString("width"))
                    String type = getAbstractValue(row, mappingType)
                    String surface = getAbstractValue(row, mappingSurface)
                    String sidewalk = getSidewalk(row.getString("sidewalk"))
                    Integer zIndex = getZIndex(row.getString("zindex"))
                    datasource.execute ("insert into tmp_${uid} values('${row.getGeometry("the_geom")}','${row.getString("id_way")}',${width},'${type}','${surface}','${sidewalk}',${zIndex})".toString())
                }

                datasource.execute("drop table if exists input_road; alter table tmp_"+uid+" rename to input_road")
                [outputTableName: "INPUT_ROAD"]
            }
    )
}

// Data management for the buildings
static String[] getTypeAndUse(def row, def myMaps) {
    String strType = null
    String strUse = null
    myMaps.each { finalVal ->
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

// define the value of the column height_wall according to the values of height, b_height, r_height and b_r_height
static float getHeightWall(def row) {
    String height = row.getString("height")
    String b_height = row.getString("b_height")
    String r_height = row.getString("r_height")
    String b_r_height = row.getString("b_r_height")
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

// define the value of the column height_roof according to the values of height and b_height

static float getHeightRoof(def row) {
    String height = row.getString("height")
    String b_height = row.getString("b_height")
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
// define the value of the column nb_lev according to the values of b_lev, r_lev and b_r_lev
static int getNbLevels (def row) {
    String b_lev = row.getString("b_lev")
    String r_lev = row.getString("r_lev")
    String b_r_lev = row.getString("b_r_lev")
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

// define the value of the column width according to the value of width from OSM
static Float getWidth (String width){
    Float result
    if (width != null && width.isFloat()) {
        result=width.toFloat()
    }
    return result
}

// define the value of the column width according to the value of width from OSM
static Integer getZIndex (String zindex){
    Integer result
    if (zindex != null && zindex.isInteger()) {
        result=zindex.toInteger()
    }
    return result
}
// Get the equivalent abstract value for a set of osm key/value elements
static String getAbstractValue(def row, def myMaps) {
    String strType = null
    myMaps.each { finalVal ->
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

// define the value of the column sidewalk according to the values of sidewalk from OSM
static String getSidewalk(String sidewalk) {
    String result
    if (sidewalk != null) {
        if (sidewalk == 'both') {
            result = "two"
        } else {
            if (sidewalk == 'right' || sidewalk == 'left' || sidewalk == 'yes') {
                result = "one"
            } else {
                result = "no"
            }
        }
    }
    return result
}


