package org.orbisgis.processingchain

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanager.ProcessMapper
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.assertEquals

class ProcessingChainTest {

    private final static String bdTopoDb = System.getProperty("user.home") + "/myh2gisbdtopodb.mv.db"

    @BeforeAll
    static void init(){
        System.setProperty("test.processingchain",
                Boolean.toString(ProcessingChainTest.getResource("geoclimate_bdtopo_data_test") != null))
    }

    @Test
    @EnabledIfSystemProperty(named = "test.bdtopo", matches = "true")
    void prepareBDTopoTest(){
        H2GIS h2GISDatabase = H2GIS.open(bdTopoDb-".mv.db", "sa", "")
        def process = ProcessingChain.PrepareBDTopo.prepareBDTopo()
        assertTrue process.execute([datasource: h2GISDatabase, tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    distBuffer: 500, expand: 1000, idZone: '56195',
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }
    }

    @EnabledIfSystemProperty(named = "test.processingchain", matches = "true")
    @Test
    void BDTopoProcessingChainTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchaindb")

        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/IRIS_GE.geojson").toURI()).getAbsolutePath(),"IRIS_GE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/BATI_INDIFFERENCIE.geojson").toURI()).getAbsolutePath(),"BATI_INDIFFERENCIE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/BATI_REMARQUABLE.geojson").toURI()).getAbsolutePath(),"BATI_REMARQUABLE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/ROUTE.geojson").toURI()).getAbsolutePath(),"ROUTE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/TRONCON_VOIE_FERREE.geojson").toURI()).getAbsolutePath(),"TRONCON_VOIE_FERREE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/SURFACE_EAU.geojson").toURI()).getAbsolutePath(),"SURFACE_EAU",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/ZONE_VEGETATION.geojson").toURI()).getAbsolutePath(),"ZONE_VEGETATION",true)

        ProcessMapper pm =  ProcessingChain.prepareBDTopo.createMapper()
        pm.execute([datasource: h2GIS, distBuffer : 500, expand : 1000, idZone : "56260", tableIrisName: "IRIS_GE",
                    tableBuildIndifName: "BATI_INDIFFERENCIE", tableBuildIndusName: "BATI_INDUSTRIEL",
                    tableBuildRemarqName: "BATI_REMARQUABLE", tableRoadName: "ROUTE", tableRailName: "TRONCON_VOIE_FERREE",
                    tableHydroName: "SURFACE_EAU", tableVegetName: "ZONE_VEGETATION",  hLevMin: 3,  hLevMax: 15,
                    hThresholdLev2: 10])

        pm.getResults().each {
            entry -> assertNull h2GIS.getTable(entry.getValue())
        }
    }

    @Test
    void PrepareOSMTest() {

        IProcess prepareOSMData = ProcessingChain.PrepareOSM.prepareOSM()

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

         H2GIS h2GIS = H2GIS.open("./target/osm_processchain")

         prepareOSMData.execute([
                hLevMin: 3,
                hLevMax: 15,
                hThresholdLev2: 10,
                datasource : h2GIS,
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
                mappingForVegetType : mappingVegetType
        ])
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
        IProcess pm =  ProcessingChain.PrepareOSM.createUnitsOfAnalysis()
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
        H2GIS h2GIS = H2GIS.open("./target/processinglcz")
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
        IProcess pm_units = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        pm_units.execute([datasource: h2GIS, zoneTable : "tempo_zone", buildingTable:"tempo_build", roadTable : "tempo_road", railTable : "tempo_road",
                          vegetationTable: "tempo_veget", hydrographicTable: "tempo_hydro", surface_vegetation: null,
                          surface_hydro: null, distance: 0.0, prefixName: "test"])

        IProcess pm_lcz =  ProcessingChain.BuildLCZ.createLCZ()
        pm_lcz.execute([datasource: h2GIS, prefixName: "test", buildingTable: pm_units.results.outputTableBuildingName,
                        rsuTable: pm_units.results.outputTableRsuName, roadTable: "tempo_road", vegetationTable: "tempo_veget",
                        hydrographicTable: "tempo_hydro", facadeDensListLayersBottom: [0, 50, 200], facadeDensNumberOfDirection: 8,
                        svfPointDensity: 0.008, svfRayLength: 100, svfNumberOfDirection: 60,
                        heightColumnName: "height_roof", fractionTypePervious: ["low_vegetation", "water"],
                        fractionTypeImpervious: ["road"], inputFields: ["id_build"], levelForRoads: [0]])

        h2GIS.eachRow("SELECT * FROM ${pm_lcz.results.outputTableName}".toString()){row ->
            assertTrue(row.id_rsu != null)
            assertEquals("LCZ", row.lcz1[0..2])
            assertEquals("LCZ", row.lcz2[0..2])
            assertTrue(row.min_distance != null)
            assertTrue(row.pss <= 1)
        }
        def nb_rsu = h2GIS.firstRow("SELECT COUNT(*) AS nb FROM ${pm_lcz.results.outputTableName}".toString())
        assertEquals(14, nb_rsu.nb)
    }

    @Test
    void osmGeoIndicatorsFromApi() {
        //Do not change this code
        String id_zone = "56195"
        boolean saveResults = true
        String directory ="./target/osm_processchain_full"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osmchain_indicators")

        //Extract and transform OSM data
        IProcess prepareOSMData = ProcessingChain.PrepareOSM.prepareOSMDefaultConfig()
        assertTrue prepareOSMData.execute([
                datasource  : datasource,
                idZone     : id_zone])

        String buildingTableName = prepareOSMData.getResults().outputBuilding

        String roadTableName = prepareOSMData.getResults().outputRoad

        String railTableName = prepareOSMData.getResults().outputRail

        String hydrographicTableName = prepareOSMData.getResults().outputHydro

        String vegetationTableName = prepareOSMData.getResults().outputVeget

        String zoneTableName = prepareOSMData.getResults().outputZone

        if(saveResults){
            println("Saving OSM GIS layers")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles()
            saveTables.execute( [inputTableNames: [buildingTableName,roadTableName,railTableName,hydrographicTableName,
                                                   vegetationTableName,zoneTableName]
                                 , directory: directory, datasource: datasource])
        }

        //Run tests
        osmGeoIndicators(directory, datasource, zoneTableName, buildingTableName,roadTableName,railTableName,vegetationTableName,
                hydrographicTableName,saveResults)

    }

    @Test
    void osmGeoIndicatorsFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        boolean saveResults = true
        String directory ="./target/osm_processchain_geoindicators"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osmchain_geoindicators")

        String zoneTableName="zone"
        String buildingTableName="building"
        String roadTableName="road"
        String railTableName="rails"
        String vegetationTableName="veget"
        String hydrographicTableName="hydro"

        datasource.load(urlBuilding, buildingTableName)
        datasource.load(urlRoad, roadTableName)
        datasource.load(urlRail, railTableName)
        datasource.load(urlVeget, vegetationTableName)
        datasource.load(urlHydro, hydrographicTableName)
        datasource.load(urlZone, zoneTableName)

        //Run tests
        osmGeoIndicators(directory, datasource, zoneTableName, buildingTableName,roadTableName,railTableName,vegetationTableName,
        hydrographicTableName,saveResults)

    }


    void osmGeoIndicators(String directory, JdbcDataSource datasource, String zoneTableName, String buildingTableName,
                          String roadTableName, String railTableName, String vegetationTableName,
                          String hydrographicTableName, boolean saveResults ) {
        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource : datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                                         roadTable: roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro  : 2500, distance: 0.01, prefixName: "geounits"])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationBlocks = spatialUnits.getResults().outputTableBlockName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            println("Saving spatial units")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles()
            saveTables.execute( [inputTableNames: spatialUnits.getResults().values()
                                 , directory: directory, datasource: datasource])
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        assertEquals(countBlocks.count,maxBlocks.max)

        datasource.eachRow("select count(distinct id_block) as count_block , count(id_build) as count_building from ${relationBuildings} where id_rsu = 42".toString()) {
            row ->
                assertEquals(7,row.count_block )
                assertEquals(14,row.count_building)
        }

        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count,maxRSUBlocks.max)


        //Compute building indicators
        def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
        assertTrue computeBuildingsIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: relationBuildings,
                                                       inputRoadTableName    : roadTableName])
        String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName
        if(saveResults){
            println("Saving building indicators")
            datasource.save(buildingIndicators, directory + File.separator + "${buildingIndicators}.geojson")
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count,countBuildingIndicators.count)

        //Compute block indicators
        def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
        assertTrue computeBlockIndicators.execute([datasource: datasource,
                                                   inputBuildingTableName: buildingIndicators,
                                                   inputBlockTableName: relationBlocks])
        String blockIndicators = computeBlockIndicators.getResults().outputTableName
        if(saveResults){
            println("Saving block indicators")
            datasource.save(blockIndicators, directory + File.separator + "${blockIndicators}.geojson")
        }

        //Check we have the same number of blocks
        def countRelationBlocks= datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
        assertEquals(countRelationBlocks.count,countBlocksIndicators.count)


        //Compute RSU indicators
        def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
        assertTrue computeRSUIndicators.execute([datasource             : datasource,
                                                 buildingTable          : buildingIndicators,
                                                 rsuTable               : relationRSU,
                                                 vegetationTable        : vegetationTableName,
                                                 roadTable              : roadTableName,
                                                 hydrographicTable      : hydrographicTableName])
        String rsuIndicators = computeRSUIndicators.getResults().outputTableName
        if(saveResults){
            println("Saving RSU indicators")
            datasource.save(rsuIndicators, directory + File.separator + "${rsuIndicators}.geojson")
        }

        //Check we have the same number of RSU
        def countRelationRSU= datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count,countRSUIndicators.count)


    }
}
