package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript PrepareData prepareData

/**
 * This process is used to format the OSM buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param mappingForTypeAndUse A map between the target values for type and use in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final buildings table
 */
IProcess formatBuildingLayer() {
    return create({
        title "Transform OSM buildings table into a table that matches the constraints of the GeoClimate input model"
        inputs datasource : JdbcDataSource , inputTableName:String
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName ->
            logger.info('Formating building layer')
            def queryMapper = "SELECT "
            def columnToMap = ['height', 'building:height', 'roof:height', 'building:roof:height',
                               'building:levels', 'roof:levels', 'building:roof:levels', 'building',
                               'amenity', 'layer', 'aeroway', 'historic', 'leisure', 'monument',
                               'place_of_worship', 'military', 'railway', 'public_transport',
                               'barrier', 'government', 'historic:building', 'grandstand',
                               'house', 'shop', 'industrial', 'man_made', 'residential',
                               'apartments', 'ruins', 'agricultural', 'barn', 'healthcare',
                               'education', 'restaurant', 'sustenance', 'office']

            def mappingTypeAndUse = [
                    "terminal"                       : ["aeroway" : ["terminal", "airport_terminal"],
                                                        "amenity" : ["terminal", "airport_terminal"],
                                                        "building": ["terminal", "airport_terminal"]
                    ],
                    "monument"                       : ["building": ["monument"],
                                                        "historic": ["monument"],
                                                        "leisure" : ["monument"],
                                                        "monument": ["yes"]
                    ],
                    "religious"                      : ["building"        : ["religious", "abbey", "cathedral", "chapel", "church", "mosque", "musalla", "temple", "synagogue", "shrine", "place_of_worship", "wayside_shrine"],
                                                        "amenity"         : ["religious", "abbey", "cathedral", "chapel", "church", "mosque", "musalla", "temple", "synagogue", "shrine", "place_of_worship", "wayside_shrine"],
                                                        "place_of_worship": ["! no", "! chapel", "! church"]
                    ],
                    "sport"                          : ["building": ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"],
                                                        "leisure" : ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"],
                                                        "amenity" : ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"]
                    ],
                    "sports_centre"                  : ["building": ["sports_centre", "sports_hall"],
                                                        "leisure" : ["sports_centre", "sports_hall"],
                                                        "amenity" : ["sports_centre", "sports_hall"]
                    ],
                    "chapel"                         : ["building"        : ["chapel"],
                                                        "amenity"         : ["chapel"],
                                                        "place_of_worship": ["chapel"],
                    ],
                    "church"                         : ["building"        : ["church"],
                                                        "amenity"         : ["church"],
                                                        "place_of_worship": ["church"],
                    ],
                    "castle"                         : ["building": ["castle", "fortress"],
                    ],
                    "military"                       : ["military": ["ammunition", "bunker", "barracks", "casemate", "office", "shelter"],
                                                        "building": ["ammunition", "bunker", "barracks", "casemate", "military", "shelter"],
                                                        "office"  : ["military"]
                    ],
                    "train_station"                  : ["building"        : ["train_station"],
                                                        "railway"         : ["station", "train_station"],
                                                        "public_transport": ["train_station"],
                                                        "amenity"         : ["train_station"]
                    ],
                    "townhall"                       : ["amenity" : ["townhall"],
                                                        "building": ["townhall"]
                    ],
                    "toll"                           : ["barrier" : ["toll_booth"],
                                                        "building": ["toll_booth"]
                    ],
                    "government"                     : ["building"  : ["government", "government_office"],
                                                        "government": ["! no"],
                                                        "office"    : ["government"]
                    ],
                    "historic"                       : ["building"         : ["historic"],
                                                        "historic"         : [],
                                                        "historic_building": ["! no"]
                    ],
                    "grandstand"                     : ["building"  : ["grandstand"],
                                                        "leisure"   : ["grandstand"],
                                                        "amenity"   : ["grandstand"],
                                                        "grandstand": ["yes"]
                    ],
                    "detached"                       : ["building": ["detached"],
                                                        "house"   : ["detached"]
                    ],
                    "farm_auxiliary"                 : ["building": ["farm_auxiliary", "barn", "stable", "sty", "cowshed", "digester", "greenhouse"]
                    ],
                    "commercial"                     : ["building": ["bank", "bureau_de_change", "boat_rental", "car_rental", "commercial", "internet_cafe", "kiosk", "money_transfer", "market", "market_place", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse"],
                                                        "amenity" : ["bank", "bureau_de_change", "boat_rental", "car_rental", "commercial", "internet_cafe", "kiosk", "money_transfer", "market", "market_place", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse"],
                                                        "shop"    : ["!= no"]
                    ],
                    "industrial"                     : ["building"  : ["industrial", "factory", "warehouse"],
                                                        "industrial": ["factory"],
                                                        "amenity"   : ["factory"]
                    ],
                    "greenhouse"                     : ["building"  : ["greenhouse"],
                                                        "amenity"   : ["greenhouse"],
                                                        "industrial": ["greenhouse"]
                    ],
                    "silo"                           : ["building": ["silo", "grain_silo"],
                                                        "man_made": ["silo", "grain_silo"]
                    ],
                    "house"                          : ["building": ["house"],
                                                        "house"   : ["! no", "! detached", "! residential", "! villa"],
                                                        "amenity" : ["house"]
                    ],
                    "residential"                    : ["building"   : ["residential", "villa", "detached", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "terrace"],
                                                        "residential": ["university", "detached", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "building"],
                                                        "house"      : ["residential"],
                                                        "amenity"    : ["residential"]
                    ],
                    "apartments"                     : ["building"   : ["apartments"],
                                                        "residential": ["apartments"],
                                                        "amenity"    : ["apartments"],
                                                        "apartments" : ["yes"]
                    ],
                    "bungalow"                       : ["building": ["bungalow"],
                                                        "house"   : ["bungalow"],
                                                        "amenity" : ["bungalow"]
                    ],
                    "ruins"                          : ["building": ["ruins"],
                                                        "ruins"   : ["ruins"]
                    ],
                    "agricultural"                   : ["building"    : ["agricultural"],
                                                        "agricultural": ["building"]
                    ],
                    "farm"                           : ["building": ["farm", "farmhouse"]
                    ],
                    "barn"                           : ["building": ["barn"],
                                                        "barn"    : ["! no"]
                    ],
                    "transportation"                 : ["building"        : ["train_station", "transportation", "station"],
                                                        "aeroway"         : ["hangar", "tower", "bunker", "control_tower", "building"],
                                                        "railway"         : ["station", "train_station", "building"],
                                                        "public_transport": ["train_station", "station"],
                                                        "amenity"         : ["train_station", "terminal"]
                    ],
                    "healthcare"                     : ["amenity"   : ["healthcare"],
                                                        "building"  : ["healthcare"],
                                                        "healthcare": ["! no"]
                    ],
                    "education"                      : ["amenity"  : ["education", "college", "kindergarten", "school", "university"],
                                                        "building" : ["education", "college", "kindergarten", "school", "university"],
                                                        "education": ["college", "kindergarten", "school", "university"]
                    ],
                    "entertainment, arts and culture": ["leisure": ["! no"]
                    ],
                    "sustenance"                     : ["amenity"   : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                        "building"  : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                        "restaurant": ["! no"],
                                                        "shop"      : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                        "sustenance": ["! no"]
                    ],
                    "office"                         : ["building": ["office"],
                                                        "amenity" : ["office"],
                                                        "office"  : ["! no"]
                    ],
                    "building"                       : ["building": ["yes"]
                    ]
            ]
            def columnNames= datasource.getTable(inputTableName).columnNames
            queryMapper += columnsMapper(columnNames, columnToMap)
            queryMapper += " FROM $inputTableName"

            outputTableName = "INPUT_BUILDING_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            datasource.execute """ DROP TABLE if exists ${outputTableName};
                        CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT,
                              NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER);"""
            datasource.withBatch(1000) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    String height = row.'height'
                    String b_height = row.'building:height'
                    String roof_height = row.'roof:height'
                    String b_roof_height = row.'building:roof:height'
                    String b_lev = row.'building:levels'
                    String roof_lev = row.'roof:levels'
                    String b_roof_lev = row.'building:roof:levels'
                    def heightWall = getHeightWall(height, b_height, roof_height, b_roof_height)
                    def heightRoof = getHeightRoof(height, b_height)
                    def nbLevels = getNbLevels(b_lev, roof_lev, b_roof_lev)
                    def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                    def use = typeAndUseValues[1]
                    def type =typeAndUseValues[0]
                    if(type == null || type.isEmpty()){
                        type =  'building'
                    }
                    stmt.addBatch"""insert into ${outputTableName} values('${row.the_geom}',
                    '${row.id}',${heightWall},${heightRoof},${nbLevels},'${type}','${use}',${row.'layer'})""".toString()
                }
            }
            logger.info('Buildings transformation finishes')
            [outputTableName: outputTableName]
        }
    }
    )
}


/**
 * This process is used to transform the OSM roads table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw roads table
 * @param inputTableName The name of the raw roads table in the DB
 * @param mappingForType A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @param mappingForSurface A map between the target values for the column type in the model
 *        and the associated key/value tags retrieved from OSM
 * @return outputTableName The name of the final roads table
 */
static IProcess formatRoadLayer() {
    return create({
            title "Format the raw roads table into a table that matches the constraints of the GeoClimate Input Model"
            inputs datasource  : JdbcDataSource,             inputTableName      : String
            outputs outputTableName: String
            run{ datasource, inputTableName ->
                logger.info('Formating road layer')
                def queryMapper = "SELECT "
                def columnToMap = ['width','highway', 'surface', 'sidewalk',
                                   'lane','layer','maxspeed','oneway',
                                   'h_ref','route','cycleway',
                                   'biclycle_road','cyclestreet','junction']
                queryMapper += columnsMapper(datasource.getTable(inputTableName).columnNames, columnToMap)
                queryMapper += " FROM $inputTableName"

                outputTableName = "INPUT_ROAD_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                datasource.execute("    drop table if exists $outputTableName;\n" +
                        "CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR,\n" +
                        "SURFACE VARCHAR, SIDEWALK VARCHAR, ZINDEX INTEGER)")
                datasource.withBatch(1000) { stmt ->
                    datasource.eachRow(queryMapper){ row ->
                        def width = getWidth(row.'width')
                        String type = getAbstractValue(row, mappingForRoadType)
                        if (null == type || type.isEmpty()) {
                            type = 'unclassified'
                        }
                        String surface = getAbstractValue(row, mappingForSurface)
                        String sidewalk = getSidewalk(row.'sidewalk')
                        def zIndex = getZIndex(row.'layer')
                        stmt.addBatch """insert into input_road values(${row.the_geom},${row.id},
                    ${width},${type},${surface},${sidewalk},${zIndex})""".toString()
                    }
                }
                logger.info('Roads transformation finishes')
                [outputTableName: "INPUT_ROAD"]
            }
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
    return create({
        title "transform the raw rails table into a table that matches the constraints of the GeoClimate Input Model"
        inputs datasource : JdbcDataSource , inputTableName: String, mappingForRailType: Map
        outputs outputTableName: String
        run { datasource, inputTableName, mappingForRailType ->
            def inputTable = datasource.getSpatialTable(inputTableName)
            logger.info('Rails transformation starts')
            datasource.execute("    drop table if exists input_rail;\n" +
                    "CREATE TABLE input_rail (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, TYPE VARCHAR, ZINDEX INTEGER)")
            inputTable.eachRow { row ->
                String type = getAbstractValue(row, mappingForRailType)
                int zIndex = getZIndex(row.'layer')

                //special treatment if type is subway
                if (type == "subway") {

                    if (!((row.tunnel != null && row.tunnel == "no" && row.layer != null && row.layer.toInt() >= 0)
                            || (row.bridge != null && (row.bridge == "yes" || row.bridge == "viaduct")))) {
                        type = null
                    }
                }
                def query = "insert into input_rail values(${row.the_geom},${row.id_way},${type},${zIndex})"
                datasource.execute(query)
            }
            logger.info('Rails transformation finishes')
            [outputTableName: "INPUT_RAIL"]
        }
    }
    )
}

///**
// * This process is used to transform the raw vegetation table into a table that matches the constraints
// * of the geoClimate Input Model
// * @param datasource A connexion to a DB containing the raw vegetation table
// * @param inputTableName The name of the raw vegetation table in the DB
// * @param mappingForType A map between the target values for the column type in the model
// *        and the associated key/value tags retrieved from OSM
// * @return outputTableName The name of the final vegetation table
// */
//static IProcess transformVeget() {
//    return processFactory.create(
//            "transform the raw vegetation table into a table that matches the constraints of the GeoClimate Input Model",
//            [datasource    : JdbcDataSource,
//             inputTableName: String,
//             mappingForVegetType: Map],
//            [outputTableName: String],
//            { JdbcDataSource datasource, inputTableName, mappingForVegetType ->
//                def inputTable = datasource.getSpatialTable(inputTableName)
//                logger.info('Veget transformation starts')
//                datasource.execute("    drop table if exists input_veget;\n" +
//                        "CREATE TABLE input_veget (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR, TYPE VARCHAR)")
//                inputTable.eachRow { row ->
//                    String type = getAbstractValue(row, mappingForVegetType)
//                    def query = "insert into input_veget values(${row.the_geom},${row.id_source},${type})"
//                    datasource.execute(query)
//                }
//                logger.info('Veget transformation finishes')
//                [outputTableName: "INPUT_VEGET"]
//            }
//    )
//}
//
///**
// * This process is used to transform the raw hydro table into a table that matches the constraints
// * of the geoClimate Input Model
// * @param datasource A connexion to a DB containing the raw hydro table
// * @param inputTableName The name of the raw hydro table in the DB
// * @param mappingForType A map between the target values for the column type in the model
// *        and the associated key/value tags retrieved from OSM
// * @return outputTableName The name of the final hydro table
// */
//static IProcess transformHydro() {
//    return processFactory.create(
//            "transform the raw hydro table into a table that matches the constraints of the GeoClimate Input Model",
//            [datasource          : JdbcDataSource,
//             inputTableName      : String],
//            [outputTableName: String],
//            { datasource, inputTableName ->
//                def inputTable = datasource.getSpatialTable(inputTableName)
//                logger.info('Veget transformation starts')
//                datasource.execute("    drop table if exists input_hydro;\n" +
//                        "CREATE TABLE input_hydro (THE_GEOM GEOMETRY, ID_SOURCE VARCHAR)")
//                inputTable.eachRow { row ->
//                    def query = "insert into input_hydro values(${row.the_geom},${row.id_source})"
//                    datasource.execute (query)
//                }
//                logger.info('Veget transformation finishes')
//                [outputTableName: "INPUT_HYDRO"]
//            }
//    )
//}
//
/**
 * This function defines the input values for both columns type and use to follow the constraints
 * of the geoClimate Input Model
 * @param row The row of the raw table to examine
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String[] getTypeAndUse(def row,def columnNames, def myMap) {
    String strType = null
    String strUse = null
    myMap.each { finalVal ->
        def finalKey = finalVal.key
        finalVal.value.each { osmVals ->
            if(columnNames.contains(osmVals.key)){
                def  columnValue = row.getString(osmVals.key)
            if(columnValue!=null){
            osmVals.value.each { osmVal ->
                if (osmVal.startsWith("!")) {
                    osmVal = osmVal.replace("! ","")
                    if ((columnValue != osmVal) && (columnValue != null)) {
                        if (strType == null) {
                            strType = finalKey
                        } else {
                            strUse = finalKey
                        }
                    }
                } else {
                    if (columnValue == osmVal) {
                        if (strType == null) {
                            strType = finalKey
                        } else {
                            strUse = finalKey
                        }
                    }
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
static float getWidth (String width){
    return (width != null && width.isFloat()) ? width.toFloat() : null
}

/**
 * This function defines the value of the column zindex according to the value of zindex from OSM
 * @param width The original zindex value
 * @return The calculated value of zindex (default value : null)
 */
static int getZIndex (String zindex){
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
    def flatList = "\"${inputColumns.join("\",\"")}\""
    columnsToMap.each {it ->
        if(!inputColumns.contains(it)){
            flatList+= ", null as \"${it}\""
        }
    }
    return flatList
}

