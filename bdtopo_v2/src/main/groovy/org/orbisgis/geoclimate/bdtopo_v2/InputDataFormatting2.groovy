package org.orbisgis.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

@BaseScript BDTopo_V2 BDTopo_V2

/**
 * This process allows to control the quality of input tables and then to format and enrich them
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param inputBuilding Table name in which the input buildings are stored
 * @param inputRoad Table name in which the input roads are stored
 * @param inputRail Table name in which the input rail ways are stored
 * @param inputHydro Table name in which the input hydrographic areas are stored
 * @param inputVeget Table name in which the input vegetation areas are stored
 * @param inputImpervious Table name in which the input impervious areas are stored
 * @param inputZone Table name in which the input zone stored
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param idZone The Zone id
 * @param expand The distance (expressed in meter) used to compute the extended area around the ZONE
 * @param buildingAbstractParameters The name of the table in which the abstract building's parameters are stored
 * @param roadAbstractParameters The name of the table in which the abstract road's parameters are stored
 * @param vegetAbstractParameters The name of the table in which the abstract vegetation area's parameters are stored
 *
 * @return outputBuilding Table name in which the (ready to be used in the GeoIndicators part) buildings are stored
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputImpervious Table name in which the (ready to be used in the GeoIndicators part) impervious areas are stored
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 */
IProcess formatData() {
    return create {
        title 'Allows to format and enrich the input data in order to feed the GeoClimate model'
        id "formatData"
        inputs datasource: JdbcDataSource, inputBuilding: String, inputRoad: String, inputRail: String,
                inputHydro: String, inputVeget: String, inputImpervious: String, inputZone: String,
                hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, expand: 1000, idZone: String
        outputs outputBuilding: String, outputRoad: String,
                outputRail: String, outputHydro: String, outputVeget: String,
                outputImpervious: String, outputZone: String
        run { JdbcDataSource datasource, inputBuilding, inputRoad, inputRail,
              inputHydro, inputVeget, inputImpervious, inputZone,
              hLevMin, hLevMax, hThresholdLev2, expand, idZone ->
            info('Formating the BDTOPO data')
            def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
            def zoneNeighbors = postfix 'ZONE_NEIGHBORS_'
            def buZone = postfix 'BU_ZONE_'
            def buildWithZone = postfix 'BUILD_WITHIN_ZONE_'
            def buildOuterZone = postfix 'BUILD_OUTER_ZONE_'
            def buildOuterZoneMatrix = postfix 'BUILD_OUTER_ZONE_MATRIX_'
            def buildZoneMatrix = postfix 'BUILD_ZONE_MATRIX_'
            def buildingFC = postfix 'BUILDING_FC_'

            //Final output table names
            def building = 'BUILDING'
            def road = 'ROAD'
            def rail = 'RAIL'
            def hydro = 'HYDRO'
            def veget = 'VEGET'
            def impervious = 'IMPERVIOUS'

            //1. CREATE ZONE_NEIGHBORS TABLE (using the EXPAND parameter and ZONE table)

            datasource.execute("""
            DROP TABLE IF EXISTS ZONE_NEIGHBORS;
            CREATE TABLE ZONE_NEIGHBORS (the_geom geometry, ID_ZONE varchar) AS SELECT  the_geom, ID_ZONE FROM $inputZone 
            UNION SELECT ST_DIFFERENCE(ST_EXPAND(the_geom, $expand), the_geom) as the_geom, 'outside' FROM $inputZone;
            CREATE SPATIAL INDEX ON ZONE_NEIGHBORS (the_geom);
            """.toString())


            info 'The BD Topo data have been formated'
            //Format population table if exists

            [outputBuilding  : building,
             outputRoad      : road,
             outputRail      : rail,
             outputHydro     : hydro,
             outputVeget     : veget,
             outputImpervious: impervious,
             outputZone      : inputZone
            ]

        }
    }
}

/**
 * This process is used to format the BDTopo buildings table into a table that matches the constraints
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
        title "Transform BDTopo buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatBuildingLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: "", epsg: int, h_lev_min: 3, h_lev_max: 15, hThresholdLev2: 10, jsonFilename: "", urbanAreasTableName: ""
        outputs outputTableName: String, outputEstimateTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, epsg, h_lev_min, h_lev_max, hThresholdLev2, jsonFilename, urbanAreasTableName ->

            datasource.execute("""
            -- Add an id (primary key, called ID_BUILD) to the input layer (INPUT_BUILDING) and create indexes

            DROP TABLE IF EXISTS BU_ZONE;
            CREATE TABLE BU_ZONE (THE_GEOM geometry, ID_BUILD serial, ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer) AS SELECT ST_FORCE2D(THE_GEOM), rownum(), 
            ID_SOURCE, HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE, MAIN_USE, ZINDEX FROM $inputTableName;
            CREATE SPATIAL INDEX ON BU_ZONE (the_geom);
            CREATE INDEX ON BU_ZONE (ID_BUILD);
            
            -- 1- Select buildings that are within a city and assign the a ID_ZONE to the building
            
            DROP TABLE IF EXISTS BUILD_WITHIN_ZONE;
            CREATE TABLE BUILD_WITHIN_ZONE AS SELECT a.ID_BUILD, b.ID_ZONE as ID_ZONE FROM BU_ZONE a, ZONE_NEIGHBORS b WHERE a.the_geom && b.the_geom AND ST_CONTAINS(b.the_geom, a.the_geom);
            CREATE INDEX ON BUILD_WITHIN_ZONE (ID_BUILD);

            -- 2- Select buildings that are on a boundary (not within a city)
            DROP TABLE IF EXISTS BUILD_OUTER_ZONE;
            CREATE TABLE BUILD_OUTER_ZONE AS SELECT * FROM BU_ZONE WHERE ID_BUILD NOT IN (SELECT ID_BUILD FROM BUILD_WITHIN_ZONE);

            -- 3- Associate building to city, depending on the maximum surface of intersection, only for buildings that are not within a city
            DROP TABLE IF EXISTS BUILD_OUTER_ZONE_MATRIX ;
            CREATE TABLE BUILD_OUTER_ZONE_MATRIX (ID_BUILD integer primary key, ID_ZONE varchar) AS SELECT a.ID_BUILD , (SELECT ID_ZONE FROM $ZONE_NEIGHBORS b WHERE a.THE_GEOM && b.THE_GEOM ORDER BY ST_AREA(ST_INTERSECTION(a.THE_GEOM, b.THE_GEOM)) DESC LIMIT 1) AS ID_ZONE 
            FROM BUILD_OUTER_ZONE a WHERE ST_NUMGEOMETRIES(a.THE_GEOM)=1;

            -- 4- Merge into one single table these information
            DROP TABLE IF EXISTS BUILD_ZONE_MATRIX ;
            CREATE TABLE BUILD_ZONE_MATRIX (ID_BUILD integer primary key, ID_ZONE varchar) AS SELECT * FROM BUILD_WITHIN_ZONE UNION SELECT * FROM BUILD_OUTER_ZONE_MATRIX;
            CREATE INDEX ON BUILD_ZONE_MATRIX (ID_BUILD);

            -- Join this "matrix" to the initial table (with all building information) (FC = First Control)
            DROP TABLE IF EXISTS BUILDING_FC ;
            CREATE TABLE BUILDING_FC AS SELECT a.*, b.ID_ZONE FROM BU_ZONE a LEFT JOIN BUILD_ZONE_MATRIX b ON a.ID_BUILD=b.ID_BUILD;

            DROP TABLE IF EXISTS ZONE_NEIGHBORS, BUILD_WITHIN_ZONE, BUILD_OUTER_ZONE, BUILD_OUTER_ZONE_MATRIX, BUILD_ZONE_MATRIX;
            
            """.toString())

            //Create the final building table

            def building_type_use =
                    ["Bâtiment agricole"        : ["farm_auxiliary": "agricultural"],
                     "Bâtiment commercial'"     : ["commercial": "commercial"],
                     "Bâtiment industriel'"     : ["light_industry": "industrial"],
                     "Serre"                    : ["greenhouse": "agricultural"],
                     "Silo"                     : ["silo": "agricultural"],
                     "Aérogare"                 : ["terminal": "transportation"],
                     "Arc de triomphe"          : ["monument": "heritage"],
                     "Arène ou théâtre antique" : ["monument": "monument"],
                     "Bâtiment religieux divers": ["religious": "religious"],
                     "Bâtiment sportif"         : ["sports_centre": "entertainment_arts_culture"],
                     "Chapelle"                 : ["chapel": "religious"],
                     "Château"                  : ["castle": "heritage"],
                     "Eglise"                   : ["church": "religious"],
                     "Fort, blockhaus, casemate": ["military": "military"],
                     "Gare"                     : ["train_station": "transportation"],
                     "Mairie"                   : ["townhall": "government"],
                     "Monument"                 : ["monument": "monument"],
                     "Péage"                    : ["toll_booth": "transportation"],
                     "Préfecture"               : ["government": "government"],
                     "Sous-préfecture"          : ["government": "government"],
                     "Tour, donjon, moulin"     : ["historic": "heritage"],
                     "Tribune"                  : ["grandstand": "entertainment_arts_culture"]]

            def building_type_level = ["building"                  : 1,
                                       "house"                     : 1,
                                       "detached"                  : 1,
                                       "residential"               : 1,
                                       "apartments"                : 1,
                                       "bungalow"                  : 0,
                                       "historic"                  : 0,
                                       "monument"                  : 0,
                                       "ruins"                     : 0,
                                       "castle"                    : 0,
                                       "agricultural"              : 0,
                                       "farm"                      : 0,
                                       "farm_auxiliary"            : 0,
                                       "barn"                      : 0,
                                       "greenhouse"                : 0,
                                       "silo"                      : 0,
                                       "commercial"                : 2,
                                       "industrial"                : 0,
                                       "sport"                     : 0,
                                       "sports_centre"             : 0,
                                       "grandstand"                : 0,
                                       "transportation"            : 0,
                                       "train_station"             : 0,
                                       "toll_booth"                : 0,
                                       "terminal"                  : 0,
                                       "healthcare"                : 1,
                                       "education"                 : 1,
                                       "entertainment_arts_culture": 0,
                                       "sustenance"                : 1,
                                       "military"                  : 0,
                                       "religious"                 : 0,
                                       "chapel"                    : 0,
                                       "church"                    : 0,
                                       "government"                : 1,
                                       "townhall"                  : 1,
                                       "office"                    : 1,
                                       "heavy_industry"            : 0,
                                       "light_industry"            : 0]

            def outputTableName = "BUILDING"

            datasource.execute("""DROP TABLE IF EXISTS $outputBuilding;
            CREATE TABLE $outputBuilding (THE_GEOM geometry, ID_BUILD integer PRIMARY KEY, ID_SOURCE varchar(24), 
            HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer, ID_ZONE varchar);""".toString())


            //Formating building table
            def id_build = 1;
            datasource.withBatch(1000) { stmt ->
                datasource.eachRow("SELECT * FROM $outputBuilding".toString()) { row ->
                    def feature_type = "building"
                    def feature_main_use = "building"
                    def id_source = row.ID_SOURCE
                    if (row.TYPE) {
                        def tmp_type_use = building_type_use.get(row.NATURE)
                        if (tmp_type_use) {
                            feature_type = tmp_type_use[0]
                            feature_main_use = tmp_type_use[1]
                        }
                    }
                    def height_wall = row.HEIGHT_WALL
                    def height_roof = row.HEIGHT_ROOF
                    def nb_lev = row.NB_LEV ? row.NB_LEV : 0
                    //Update height_wall
                    if (height_wall == null || height_wall == 0) {
                        height_wall = h_lev_min
                        height_roof = h_lev_max
                    }
                    //Update NB_LEV
                    def nbLevelsFromType = building_type_level[feature_type]
                    def formatedHeight = formatHeightsAndNbLevels(height_wall, height_roof, nb_lev, h_lev_min,
                            h_lev_max, hThresholdLev2, nbLevelsFromType == null ? 0 : nbLevelsFromType)

                    def zIndex = 0
                    if (formatedHeight.nbLevels > 0) {
                        Geometry geom = row.the_geom
                        def srid = geom.getSRID()
                        for (int i = 0; i < geom.getNumGeometries(); i++) {
                            Geometry subGeom = geom.getGeometryN(i)
                            if (subGeom instanceof Polygon) {
                                stmt.addBatch """
                                                INSERT INTO ${outputBuilding} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$srid), 
                                                    $id_build, 
                                                    '$id_source',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    '${feature_type}',
                                                    '${feature_main_use}',
                                                    ${zIndex},
                                                    '$idZone')
                                            """.toString()

                                id_build++
                            }
                        }
                    }
                }
            }
            debug 'Buildings transformation finishes'
            [outputTableName: outputTableName]

        }
    }
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