package org.orbisgis.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.orbisgis.data.api.dataset.ISpatialTable
import org.orbisgis.data.api.dataset.ITable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

import java.util.regex.Pattern

@BaseScript BDTopo_V2 BDTopo_V2

/**
 * This process is used to format the BDTopo buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @return outputTableName The name of the final buildings table
 * @return outputEstimatedTableName The name of the table containing the state of estimation for each building
 */
IProcess formatBuildingLayer() {
    return create {
        title "Transform BDTopo buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatBuildingLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: String, inputImpervious: "", h_lev_min: 3, h_lev_max: 15, hThresholdLev2: 10
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName, inputImpervious, h_lev_min, h_lev_max, hThresholdLev2 ->

            if (!h_lev_min) {
                h_lev_min = 3
            }
            if (!h_lev_max) {
                h_lev_max = 15
            }
            if (!hThresholdLev2) {
                hThresholdLev2 = 10
            }

            def outputTableName = postfix "BUILDING"
            debug 'Formating building layer'

            //Create the final building table

            datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM geometry, ID_BUILD integer, ID_SOURCE varchar(24), 
            HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX integer);""".toString())

            if (inputTableName) {
                ISpatialTable inputSpatialTable = datasource."$inputTableName"
                if (!inputSpatialTable.isEmpty()) {
                    def queryMapper = "SELECT "
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        datasource."$inputZoneEnvelopeTableName".the_geom.createSpatialIndex()
                        queryMapper += " a.*  FROM $inputTableName as a,  $inputZoneEnvelopeTableName as b WHERE a.the_geom && b.the_geom and st_intersects(a.the_geom " +
                                ",b.the_geom) and st_area(a.the_geom)>1 and st_isempty(a.the_geom)=false "
                    } else {
                        queryMapper += "* FROM $inputTableName as a where st_area(a.the_geom)>1 "
                    }

                    def building_type_use =
                            ["Bâtiment agricole"        : ["farm_auxiliary": "agricultural"],
                             "Bâtiment commercial"      : ["commercial": "commercial"],
                             "Bâtiment industriel"      : ["light_industry": "industrial"],
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
                             "Tribune"                  : ["grandstand": "entertainment_arts_culture"],
                             "Résidentiel"              : ["residential": "residential"]]

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


                    //Formating building table
                    def id_build = 1;
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper.toString()) { row ->
                            def feature_type = "building"
                            def feature_main_use = "building"
                            def id_source = row.ID_SOURCE
                            if (row.TYPE) {
                                def tmp_type_use = building_type_use.get(row.TYPE)
                                if (tmp_type_use) {
                                    def type_main = tmp_type_use.grep()[0]
                                    feature_type = type_main.key
                                    feature_main_use = type_main.value
                                }
                            }
                            def height_wall = row.HEIGHT_WALL
                            def height_roof = 0
                            def nb_lev = 0
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
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$srid), 
                                                    $id_build, 
                                                    '$id_source',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    '${feature_type}',
                                                    '${feature_main_use}',
                                                    ${zIndex})
                                            """.toString()

                                        id_build++
                                    }
                                }
                            }
                        }
                    }
                    //Let's use the impervious table to improve building qualification
                    if (inputImpervious) {
                        datasource."$outputTableName".the_geom.createSpatialIndex()
                        datasource."$outputTableName".id_build.createIndex()
                        datasource."$outputTableName".type.createIndex()
                        datasource."$inputImpervious".the_geom.createSpatialIndex()
                        def buildinType = postfix("BUILDING_TYPE")

                        datasource.execute """create table $buildinType as SELECT 
                        max(b.type) as type, 
                        max(b.type) as main_use, a.id_build FROM $outputTableName a, $inputImpervious b 
                        WHERE ST_POINTONSURFACE(a.the_geom) && b.the_geom and st_intersects(ST_POINTONSURFACE(a.the_geom), b.the_geom) 
                        AND  a.TYPE ='building' AND b.TYPE != 'unknown'
                         group by a.id_build""".toString()

                        datasource.getTable(buildinType).id_build.createIndex()

                        def newBuildingWithType = postfix("NEW_BUILDING_TYPE")

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
                        ALTER TABLE $newBuildingWithType RENAME TO $outputTableName;
                        DROP TABLE IF EXISTS $newBuildingWithType;""".toString()
                    }
                }
            }
            debug 'Buildings transformation finishes'
            [outputTableName: outputTableName]
        }
    }
}


/**
 * This process is used to transform the BDTopo roads table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw roads table
 * @param inputTableName The name of the raw roads table in the DB
 * @return outputTableName The name of the final roads table
 */
IProcess formatRoadLayer() {
    return create {
        title "Format the raw roads table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatRoadLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName ->
            debug('Formating road layer')
            def outputTableName = postfix "ROAD"
            datasource """
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_road serial, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, CROSSING VARCHAR(30),
                SURFACE VARCHAR, SIDEWALK VARCHAR, MAXSPEED INTEGER, DIRECTION INTEGER, ZINDEX INTEGER);
        """.toString()
            if (inputTableName) {
                //Define the mapping between the values in BDTopo and those used in the abstract model
                def road_types =
                        ["Autoroute"          : "motorway",
                         'Quasi-autoroute'    : 'trunk',
                         'Bretelle'           : 'highway_link',
                         'Route à 2 chaussées': 'primary',
                         'Route à 1 chaussée' : 'unclassified',
                         'Route empierrée'    : 'track',
                         'Chemin'             : 'track',
                         'Bac auto'           : 'ferry',
                         'Bac piéton'         : 'ferry',
                         'Piste cyclable'     : 'cycleway',
                         'Sentier'            : 'path',
                         'Escalier'           : 'steps',
                         'Gué ou radier'      : null,
                         'Pont'               : 'bridge', 'Tunnel': 'tunnel', 'NC': null
                        ]

                def road_types_width =
                        ["highway"     : 8,
                         "motorway"    : 24,
                         "trunk"       : 16,
                         "primary"     : 10,
                         "secondary"   : 10,
                         "tertiary"    : 8,
                         "residential" : 8,
                         "unclassified": 3,
                         "track"       : 2,
                         "path"        : 1,
                         "footway"     : 1,
                         "cycleway"    : 1,
                         "steps"       : 1,
                         "highway_link": 8,
                         "roundabout"  : 4,
                         "ferry"       : 0,
                         "pedestrian"  : 3,
                         "service"     : 3]

                def queryMapper = "SELECT "
                def inputSpatialTable = datasource."$inputTableName"
                if (!inputSpatialTable.isEmpty()) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnNames.join(",")
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_makevalid(st_intersection(a.the_geom, b.the_geom))) " +
                                "ELSE a.the_geom " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", the_geom FROM $inputTableName  as a"
                    }
                    int rowcount = 1
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def road_type = row.TYPE
                            if (road_type) {
                                road_type = road_types.get(road_type)
                            } else {
                                road_type = "unclassified"
                            }
                            def width = row.WIDTH
                            if (width == null || width <= 0) {
                                width = road_types_width.get(road_type)
                            }
                            def road_crossing = row.CROSSING
                            if (road_crossing) {
                                road_crossing = road_types.get(road_crossing)
                            }
                            def road_sens = row.SENS
                            if (road_sens) {
                                if (road_sens == "Double") {
                                    road_sens = 3
                                } else if (road_sens == "Direct") {
                                    road_sens = 1
                                } else if (road_sens == "Inverse") {
                                    road_sens = 2
                                } else {
                                    road_sens = -1
                                }
                            }
                            def road_zindex = row.ZINDEX
                            if (!road_zindex) {
                                road_zindex = 0
                            }
                            def ID_SOURCE = row.ID_SOURCE
                            def road_surface = row.SURFACE
                            def road_sidewalk = row.SIDEWALK
                            //Not yet managed
                            def road_maxspeed = null

                            if (road_zindex >= 0 && road_type) {
                                Geometry geom = row.the_geom
                                def epsg = geom.getSRID()
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    stmt.addBatch """
                                        INSERT INTO $outputTableName VALUES(ST_GEOMFROMTEXT(
                                        '${geom.getGeometryN(i)}',$epsg), 
                                        ${rowcount++}, 
                                        '${ID_SOURCE}', 
                                        ${width},
                                        '${road_type}',
                                        '${road_crossing}', 
                                        '${road_surface}',
                                        '${road_sidewalk}',
                                        ${road_maxspeed},
                                        ${road_sens},
                                        ${road_zindex})
                                        """.toString()
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
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName ->
            debug('Hydro transformation starts')
            def outputTableName = postfix("HYDRO")
            datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_hydro serial, ID_SOURCE VARCHAR, TYPE VARCHAR, ZINDEX INTEGER);""".toString()

            if (inputTableName) {
                ISpatialTable inputSpatialTable = datasource.getSpatialTable(inputTableName)
                if (!inputSpatialTable.isEmpty()) {
                    String query
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        query = "select  CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_intersection(a.the_geom, b.the_geom)) " +
                                "ELSE a.the_geom " +
                                "END AS the_geom , a.ZINDEX, a.ID_SOURCE" +
                                " FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        query = "select * FROM $inputTableName "

                    }
                    int rowcount = 1
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(query) { row ->
                            def water_type = 'water'
                            def water_zindex = 0
                            Geometry geom = row.the_geom
                            def epsg = geom.getSRID()
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon) {
                                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.ID_SOURCE}', '${water_type}', ${water_zindex})".toString()
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
 * This process is used to transform the raw rails table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw rails table
 * @param inputTableName The name of the raw rails table in the DB
 * @return outputTableName The name of the final rails table
 */
IProcess formatRailsLayer() {
    return create {
        title "Format the raw rails table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatRailsLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: ""
        outputs outputTableName: String
        run { datasource, inputTableName, inputZoneEnvelopeTableName ->
            debug('Rails transformation starts')
            def outputTableName = postfix("RAILS")
            datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_rail serial,
                ID_SOURCE VARCHAR, TYPE VARCHAR,CROSSING VARCHAR(30), ZINDEX INTEGER);""".toString()

            if (inputTableName) {
                def queryMapper = "SELECT "
                def inputSpatialTable = datasource."$inputTableName"
                if (!inputSpatialTable.isEmpty()) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnNames.join(",")
                    if (inputZoneEnvelopeTableName) {
                        inputSpatialTable.the_geom.createSpatialIndex()
                        queryMapper += ", CASE WHEN st_overlaps(a.the_geom, b.the_geom) " +
                                "THEN st_force2D(st_makevalid(st_intersection(a.the_geom, b.the_geom))) " +
                                "ELSE st_makevalid(a.the_geom) " +
                                "END AS the_geom " +
                                "FROM " +
                                "$inputTableName AS a, $inputZoneEnvelopeTableName AS b " +
                                "WHERE " +
                                "a.the_geom && b.the_geom "
                    } else {
                        queryMapper += ", the_geom FROM $inputTableName  as a"

                    }

                    def rail_types = ['LGV'                       : 'highspeed',
                                      'Principale'                : 'rail',
                                      'Voie de service'           : 'service_track',
                                      'Voie non exploitée'        : 'disused',
                                      'Transport urbain'          : 'tram',
                                      'Funiculaire ou crémaillère': 'funicular',
                                      'Metro'                     : 'subway',
                                      'Tramway'                   : 'tram',
                                      'Pont'                      : 'bridge', 'Tunnel': 'tunnel', 'NC': null]
                    int rowcount = 1
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def rail_type = row.TYPE
                            if (rail_type) {
                                rail_type = rail_types.get(rail_type)
                            } else {
                                rail_type = "unclassified"
                            }
                            def rail_zindex = row.ZINDEX
                            if (!rail_zindex) {
                                rail_zindex = 0
                            }

                            def rail_crossing = row.CROSSING
                            if (rail_crossing) {
                                rail_crossing = rail_types.get(rail_crossing)
                            }
                            if (rail_zindex >= 0 && rail_type) {
                                Geometry geom = row.the_geom
                                def epsg = geom.getSRID()
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    stmt.addBatch """
                                    INSERT INTO $outputTableName values(ST_GEOMFROMTEXT(
                                    '${geom.getGeometryN(i)}',$epsg), 
                                    ${rowcount++}, 
                                    '${row.ID_SOURCE}',
                                    '${rail_type}',
                                    '${rail_crossing}',
                                    ${rail_zindex})
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
 * @return outputTableName The name of the final vegetation table
 */
IProcess formatVegetationLayer() {
    return create {
        title "Format the raw vegetation table into a table that matches the constraints of the GeoClimate Input Model"
        id "formatVegetationLayer"
        inputs datasource: JdbcDataSource, inputTableName: String, inputZoneEnvelopeTableName: ""
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName, inputZoneEnvelopeTableName ->
            debug('Vegetation transformation starts')
            def outputTableName = postfix "VEGET"
            datasource """ 
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_veget serial, ID_SOURCE VARCHAR, TYPE VARCHAR, HEIGHT_CLASS VARCHAR(4), ZINDEX INTEGER);""".toString()
            if (inputTableName) {
                def queryMapper = "SELECT "
                def inputSpatialTable = datasource."$inputTableName"
                if (!inputSpatialTable.isEmpty()) {
                    def columnNames = inputSpatialTable.columns
                    columnNames.remove("THE_GEOM")
                    queryMapper += columnNames.join(",")
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
                        queryMapper += ", the_geom FROM $inputTableName  as a"
                    }
                    def vegetation_types = ['Zone arborée'             : 'wood',
                                            'Forêt fermée de feuillus' : 'forest',
                                            'Forêt fermée mixte'       : 'forest',
                                            'Forêt fermée de conifères': 'forest',
                                            'Forêt ouverte'            : 'forest',
                                            'Peupleraie'               : 'forest',
                                            'Haie'                     : 'hedge',
                                            'Lande ligneuse'           : 'heath',
                                            'Verger'                   : 'orchard',
                                            'Vigne'                    : 'vineyard',
                                            'Bois'                     : 'forest',
                                            'Bananeraie'               : 'banana_plants',
                                            'Mangrove'                 : 'mangrove',
                                            'Canne à sucre'            : 'sugar_cane']

                    def vegetation_classes = [
                            'tree'         : 'high',
                            'wood'         : 'high',
                            'forest'       : 'high',
                            'scrub'        : 'low',
                            'grassland'    : 'low',
                            'heath'        : 'low',
                            'tree_row'     : 'high',
                            'hedge'        : 'high',
                            'mangrove'     : 'high',
                            'orchard'      : 'high',
                            'vineyard'     : 'low',
                            'banana_plants': 'high',
                            'sugar_cane'   : 'low',
                            'unclassified' : 'low'
                    ]

                    int rowcount = 1
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def vegetation_type = row.TYPE
                            if (vegetation_type) {
                                vegetation_type = vegetation_types.get(vegetation_type)
                            } else {
                                vegetation_type = "unclassified"
                            }
                            def height_class = vegetation_classes.get(vegetation_type)
                            def vegetation_zindex = row.ZINDEX
                            if (!vegetation_zindex) {
                                vegetation_zindex = 0
                            }
                            Geometry geom = row.the_geom
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                def epsg = geom.getSRID()
                                if (subGeom instanceof Polygon) {
                                    stmt.addBatch """
                                            INSERT INTO $outputTableName VALUES(
                                                ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                ${rowcount++}, 
                                                '${row.ID_SOURCE}',
                                                '${vegetation_type}', 
                                                '${height_class}', ${vegetation_zindex})
                                    """.toString()
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