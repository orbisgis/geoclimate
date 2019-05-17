package org.orbisgis.processingchain

import org.junit.jupiter.api.Test
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue

class ProcessingChainsTest {

    private static final Logger logger = LoggerFactory.getLogger(ProcessingChainsTest.class)

    @Test
    void PrepareOSMTest() {

        IProcess prepareOSMData = org.orbisgis.processingchains.ProcessingChains.prepareOSM()

        def mappingTypeAndUse = [
                terminal: [aeroway : ["terminal", "airport_terminal"],
                           amenity : ["terminal", "airport_terminal"],
                           building: ["terminal", "airport_terminal"]
                ],
                monument: [
                        building: ["monument"],
                        historic: ["monument"],
                        leisure : ["monument"],
                        monument: ["yes"]
                ],
                religious: [
                        building: ["religious", "abbey", "cathedral", "chapel", "church", "mosque",
                                   "musalla", "temple", "synagogue", "shrine", "place_of_worship",
                                   "wayside_shrine"],
                        amenity: ["religious", "abbey", "cathedral", "chapel", "church",
                                  "mosque", "musalla", "temple", "synagogue", "shrine",
                                  "place_of_worship", "wayside_shrine"],
                        place_of_worship: ["! no", "! chapel", "! church"]
                ],
                sport                          : ["building": ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"],
                                                  "leisure" : ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"],
                                                  "amenity" : ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"]
                ],
                sports_centre                  : ["building": ["sports_centre", "sports_hall"],
                                                  "leisure" : ["sports_centre", "sports_hall"],
                                                  "amenity" : ["sports_centre", "sports_hall"]
                ],
                chapel                         : ["building"        : ["chapel"],
                                                  "amenity"         : ["chapel"],
                                                  "place_of_worship": ["chapel"],
                ],
                church                         : ["building"        : ["church"],
                                                  "amenity"         : ["church"],
                                                  "place_of_worship": ["church"],
                ],
                castle                         : ["building": ["castle", "fortress"],
                ],
                military                       : ["military": ["ammunition", "bunker", "barracks", "casemate", "office", "shelter"],
                                                  "building": ["ammunition", "bunker", "barracks", "casemate", "military", "shelter"],
                                                  "office"  : ["military"]
                ],
                train_station                  : ["building"        : ["train_station"],
                                                  "railway"         : ["station", "train_station"],
                                                  "public_transport": ["train_station"],
                                                  "amenity"         : ["train_station"]
                ],
                townhall                       : ["amenity" : ["townhall"],
                                                  "building": ["townhall"]
                ],
                toll                          : ["barrier" : ["toll_booth"],
                                                 "building": ["toll_booth"]
                ],
                government                     : ["building"  : ["government", "government_office"],
                                                  "government": ["! no"],
                                                  "office"    : ["government"]
                ],
                historic                       : ["building"         : ["historic"],
                                                  "historic"         : [],
                                                  "historic_building": ["! no"]
                ],
                grandstand                     : ["building"  : ["grandstand"],
                                                  "leisure"   : ["grandstand"],
                                                  "amenity"   : ["grandstand"],
                                                  "grandstand": ["yes"]
                ],
                detached                       : ["building": ["detached"],
                                                  "house"   : ["detached"]
                ],
                farm_auxiliary                 : ["building": ["farm_auxiliary", "barn", "stable", "sty", "cowshed", "digester", "greenhouse"]
                ],
                commercial                     : ["building": ["bank", "bureau_de_change", "boat_rental", "car_rental", "commercial", "internet_cafe", "kiosk", "money_transfer", "market", "market_place", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse"],
                                                  "amenity" : ["bank", "bureau_de_change", "boat_rental", "car_rental", "commercial", "internet_cafe", "kiosk", "money_transfer", "market", "market_place", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse"],
                                                  "shop"    : ["!= no"]
                ],
                industrial                     : ["building"  : ["industrial", "factory", "warehouse"],
                                                  "industrial": ["factory"],
                                                  "amenity"   : ["factory"]
                ],
                greenhouse                     : ["building"  : ["greenhouse"],
                                                  "amenity"   : ["greenhouse"],
                                                  "industrial": ["greenhouse"]
                ],
                silo                           : ["building": ["silo", "grain_silo"],
                                                  "man_made": ["silo", "grain_silo"]
                ],
                house                          : ["building": ["house"],
                                                  "house"   : ["! no", "! detached", "! residential", "! villa"],
                                                  "amenity" : ["house"]
                ],
                residential                    : ["building"   : ["residential", "villa", "detached", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "terrace"],
                                                  "residential": ["university", "detached", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "building"],
                                                  "house"      : ["residential"],
                                                  "amenity"    : ["residential"]
                ],
                apartments                     : ["building"   : ["apartments"],
                                                  "residential": ["apartments"],
                                                  "amenity"    : ["apartments"],
                                                  "apartments" : ["yes"]
                ],
                bungalow                       : ["building": ["bungalow"],
                                                  "house"   : ["bungalow"],
                                                  "amenity" : ["bungalow"]
                ],
                ruins                          : ["building": ["ruins"],
                                                  "ruins"   : ["ruins"]
                ],
                agricultural                   : ["building"    : ["agricultural"],
                                                  "agricultural": ["building"]
                ],
                farm                           : ["building": ["farm", "farmhouse"]
                ],
                barn                           : ["building": ["barn"],
                                                  "barn"    : ["! no"]
                ],
                transportation                 : ["building"        : ["train_station", "transportation", "station"],
                                                  "aeroway"         : ["hangar", "tower", "bunker", "control_tower", "building"],
                                                  "railway"         : ["station", "train_station", "building"],
                                                  "public_transport": ["train_station", "station"],
                                                  "amenity"         : ["train_station", "terminal"]
                ],
                healthcare                     : ["amenity"   : ["healthcare"],
                                                  "building"  : ["healthcare"],
                                                  "healthcare": ["! no"]
                ],
                education                      : ["amenity"  : ["education", "college", "kindergarten", "school", "university"],
                                                  "building" : ["education", "college", "kindergarten", "school", "university"],
                                                  "education": ["college", "kindergarten", "school", "university"]
                ],
                "entertainment, arts and culture": ["leisure": ["! no"]
                ],
                sustenance                     : ["amenity"   : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                  "building"  : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                  "restaurant": ["! no"],
                                                  "shop"      : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                  "sustenance": ["! no"]
                ],
                office                         : [building: ["office"],
                                                  amenity : ["office"],
                                                  office  : ["! no"]
                ],
                building                       : [building: ["yes"]
                ]
        ]
        def mappingRoadType = [
                "cycleway"    : [
                        "highway"      : ["cycleway"],
                        "cycleway"     : ["track"],
                        "bicycle_road": ["yes"]
                ],
                "ferry"       : [
                        "route": ["ferry"]
                ],
                "footway"     : [
                        "highway": ["footway", "pedestrian"]
                ],
                "highway"     : [
                        "highway"    : ["service", "road", "raceway", "escape"],
                        "cyclestreet": ["yes"]
                ],
                "highway_link": [
                        "highway": ["motorway_link", "motorway_junction", "trunk_link", "primary_link", "secondary_link", "tertiary_link", "junction"]
                ],
                "motorway"    : [
                        "highway": ["motorway"]
                ],
                "path"        : [
                        "highway": ["path", "bridleway"]
                ],
                "primary"     : [
                        "highway": ["primary"]
                ],
                "residential" : [
                        "highway": ["residential", "living_street"]
                ],
                "roundabout"  : [
                        "junction": ["roundabout", "circular"]
                ],
                "secondary"   : [
                        "highway": ["secondary"]
                ],
                "steps"       : [
                        "highway": ["steps"]
                ],
                "tertiary"    : [
                        "highway": ["tertiary"]
                ],
                "track"       : [
                        "highway": ["track"]
                ],
                "trunk"       : [
                        "highway": ["trunk"]
                ],
                "unclassified": [
                        "highway": ["unclassified"]
                ]
        ]

        def mappingSurface = [
                "unpaved"    : ["surface": ["unpaved", "grass_paver", "artificial_turf"]],
                "paved"      : ["surface": ["paved", "asphalt"]],
                "ground"     : ["surface": ["ground", "dirt", "earth", "clay"]],
                "gravel"     : ["surface": ["gravel", "fine_gravel", "gravel_turf"]],
                "concrete"   : ["surface": ["concrete", "concrete:lanes", "concrete:plates", "cement"]],
                "grass"      : ["surface": ["grass"]],
                "compacted"  : ["surface": ["compacted"]],
                "sand"       : ["surface": ["sand"]],
                "cobblestone": ["surface": ["cobblestone", "paving_stones", "sett", "unhewn_cobblestone"]],
                "wood"       : ["surface": ["wood", "woodchips"]],
                "pebblestone": ["surface": ["pebblestone"]],
                "mud"        : ["surface": ["mud"]],
                "metal"      : ["surface": ["metal"]],
                "water"      : ["surface": ["water"]]
        ]

        def mappingRailType = [
                "highspeed":["highspeed":["yes"]],
                "rail":["railway":["rail","light_rail","narrow_gauge"]],
                "service_track":["service":["yard","siding","spur","crossover"]],
                "disused":["railway":["disused"]],
                "funicular":["railway":["funicular"]],
                "subway":["railway":["subway"]],
                "tram":["railway":["tram"]]
        ]

        def mappingVegetType = [
                "tree":["natural":["tree"]],
                "wood":["landcover":["trees"],"natural":["wood"]],
                "forest":["landuse":["forest"]],
                "scrub":["natural":["scrub"],"landcover":["scrub"],"landuse":["scrub"]],
                "grassland":["landcover":["grass","grassland"],"natural":["grass","grassland"],"vegetation":["grassland"],"landuse":["grass","grassland"]],
                "heath":["natural":["heath"]],
                "tree_row":["natural":["tree_row"],"landcover":["tree_row"],"barrier":["tree_row"]],
                "hedge":["barrier":["hedge"],"natural":["hedge","hedge_bank"],"fence_type":["hedge"],"hedge":["hedge_bank"]],
                "mangrove":["wetland":["mangrove"]],
                "orchard":["landuse":["orchard"]],
                "vineyard":["landuse":["vineyard"],"vineyard":["! no"]],
                "banana plants":["trees":["banana_plants"],"crop":["banana"]],
                "sugar cane":["produce":["sugar_cane"],"crop":["sugar_cane"]]
        ]

        prepareOSMData.execute([
                hLevMin: 3,
                hLevMax: 15,
                hThresholdLev2: 10,
                directory : "./target/osm_processchain",
                osmTablesPrefix: "EXT",
                idZone : "56223",
                expand : 100,
                distBuffer:100,

                buildingTableColumnsNames:
                        ['height':'height','building:height':'b_height','roof:height':'r_height','building:roof:height':'b_r_height',
                         'building:levels':'b_lev','roof:levels':'r_lev','building:roof:levels':'b_r_lev','building':'building',
                         'amenity':'amenity','layer':'zindex','aeroway':'aeroway','historic':'historic','leisure':'leisure','monument':'monument',
                         'place_of_worship':'place_of_worship','military':'military','railway':'railway','public_transport':'public_transport',
                         'barrier':'barrier','government':'government','historic:building':'historic_building','grandstand':'grandstand',
                         'house':'house','shop':'shop','industrial':'industrial','man_made':'man_made', 'residential':'residential',
                         'apartments':'apartments','ruins':'ruins','agricultural':'agricultural','barn':'barn', 'healthcare':'healthcare',
                         'education':'education','restaurant':'restaurant','sustenance':'sustenance','office':'office'],
                buildingTagKeys: ['building'],
                buildingTagValues: null,
                tablesPrefix: "RAW_",
                buildingFilter: "ZONE_BUFFER",

                roadTableColumnsNames: ['width':'width','highway':'highway', 'surface':'surface', 'sidewalk':'sidewalk',
                                        'lane':'lane','layer':'zindex','maxspeed':'maxspeed','oneway':'oneway',
                                        'h_ref':'h_ref','route':'route','cycleway':'cycleway',
                                        'bicycle_road':'bicycle_road','cyclestreet':'cyclestreet','junction':'junction'],
                roadTagKeys: ['highway','cycleway','bicycle_road','cyclestreet','route','junction'],
                roadTagValues: null,
                roadFilter: "ZONE_BUFFER",

                railTableColumnsNames: ['highspeed':'highspeed','railway':'railway','service':'service',
                                        'tunnel':'tunnel','layer':'layer','bridge':'bridge'],
                railTagKeys: ['railway'],
                railTagValues: null,
                railFilter: "ZONE",

                vegetTableColumnsNames: ['natural':'natural','landuse':'landuse','landcover':'landcover',
                                         'vegetation':'vegetation','barrier':'barrier','fence_type':'fence_type',
                                         'hedge':'hedge','wetland':'wetland','vineyard':'vineyard','trees':'trees',
                                         'crop':'crop','produce':'produce'],
                vegetTagKeys: ['natural', 'landuse','landcover'],
                vegetTagValues: ['fell', 'heath', 'scrub', 'tree', 'tree_row', 'trees', 'wood','farmland',
                                 'forest','grass','grassland','greenfield','meadow','orchard','plant_nursery',
                                 'vineyard','hedge','hedge_bank','mangrove','banana_plants','banana','sugar_cane'],
                vegetFilter: "ZONE_EXTENDED",

                hydroTableColumnsNames: ['natural':'natural','water':'water','waterway':'waterway'],
                hydroTags: ['natural':['water','waterway','bay'],'water':[],'waterway':[]],
                hydroFilter: "ZONE_EXTENDED",

                mappingForTypeAndUse : mappingTypeAndUse,

                mappingForRoadType : mappingRoadType,
                mappingForSurface : mappingSurface,

                mappingForRailType : mappingRailType,

                mappingForVegetType : mappingVegetType,
                saveResults : true
        ])
    }

    @Test
    void PrepareOSMDefaultConfigTest() {
        IProcess prepareOSMData = org.orbisgis.processingchains.ProcessingChains.prepareOSMDefaultConfig()
        prepareOSMData.execute([
                directory : "./target/osm_processchain",
                idZone : "56223",
                saveResults : true])
    }

    @Test
    void CreateUnitsOfAnalysisTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchainscales")
        String sqlString = new File(getClass().getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT id_road, the_geom, zindex FROM road_test WHERE id_road < 5 OR id_road > 6;" +
                "CREATE TABLE tempo_zone AS SELECT * FROM zone_test;" +
                "CREATE TABLE tempo_veget AS SELECT id_veget, the_geom FROM veget_test WHERE id_veget < 4;" +
                "CREATE TABLE tempo_hydro AS SELECT id_hydro, the_geom FROM hydro_test WHERE id_hydro < 2;")



        IProcess pm =  org.orbisgis.processingchains.ProcessingChains.createUnitsOfAnalysis()
        pm.execute([datasource: h2GIS, zoneTable : "tempo_zone", roadTable : "tempo_road", railTable : "tempo_road",
                    vegetationTable: "tempo_veget", hydrographicTable: "tempo_hydro", surface_vegetation: null,
                    surface_hydro: null, inputTableName: "tempo_build", distance: 0.0,
                    inputLowerScaleTableName: "tempo_build",  prefixName: "test"])

        // Test the number of blocks within RSU ID 2, whether id_build 4 and 8 belongs to the same block and are both
        // within id_rsu = 2
        def row_nb = h2GIS.firstRow(("SELECT COUNT(*) AS nb_blocks FROM ${pm.results.outputTableBlockName} " +
                "WHERE id_rsu = 2").toString())
        def row_bu4 = h2GIS.firstRow(("SELECT id_block AS id_block FROM ${pm.results.outputTableBuildingName} " +
                "WHERE id_build = 4 AND id_rsu = 2").toString())
        def row_bu8 = h2GIS.firstRow(("SELECT id_block AS id_block FROM ${pm.results.outputTableBuildingName} " +
                "WHERE id_build = 8 AND id_rsu = 2").toString())
        assertTrue(4 == row_nb.nb_blocks)
        assertTrue(row_bu4.id_block ==row_bu8.id_block)
    }

    @Test
    void createLCZTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchainscales")
        String sqlString = new File(getClass().getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT id_build, the_geom, height_wall, height_roof," +
                "nb_lev FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT * FROM road_test WHERE id_road < 5 OR id_road > 6;" +
                "CREATE TABLE tempo_zone AS SELECT * FROM zone_test;" +
                "CREATE TABLE tempo_veget AS SELECT * FROM veget_test WHERE id_veget < 5;" +
                "CREATE TABLE tempo_hydro AS SELECT * FROM hydro_test WHERE id_hydro < 2;")


        // First create the scales
        IProcess pm_units =  org.orbisgis.processingchains.ProcessingChains.createUnitsOfAnalysis()
        pm_units.execute([datasource: h2GIS, zoneTable : "tempo_zone", roadTable : "tempo_road", railTable : "tempo_road",
                    vegetationTable: "tempo_veget", hydrographicTable: "tempo_hydro", surface_vegetation: null,
                    surface_hydro: null, inputTableName: "tempo_build", distance: 0.0,
                    inputLowerScaleTableName: "tempo_build",  prefixName: "test"])

        IProcess pm_lcz =  org.orbisgis.processingchains.ProcessingChains.createLCZ()
        pm_lcz.execute([datasource: h2GIS, prefixName: "test", buildingTable: pm_units.results.outputTableBuildingName,
                        rsuTable: pm_units.results.outputTableRsuName, roadTable: "tempo_road", vegetationTable: "tempo_veget",
                        hydrographicTable: "tempo_hydro", facadeDensListLayersBottom: [0, 50, 200], facadeDensNumberOfDirection: 8,
                        svfPointDensity: 0.008, svfRayLength: 100, svfNumberOfDirection: 60,
                        heightColumnName: "height_roof", fractionTypePervious: ["low_vegetation", "water"],
                        fractionTypeImpervious: ["road"], inputFields: ["id_build", "the_geom"], levelForRoads: [0]])
    }
}