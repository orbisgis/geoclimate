package org.orbisgis.processingchains

import groovy.transform.BaseScript
import org.orbisgis.Geoclimate
import org.orbisgis.SpatialUnits
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanager.ProcessManager
import org.orbisgis.processmanager.ProcessMapper
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.processmanagerapi.IProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class ProcessingChains extends Script {
    public static IProcessFactory processFactory = ProcessManager.getProcessManager().factory("processingchains")

    public static Logger logger = LoggerFactory.getLogger(ProcessingChains.class)

    /**
     * This process chains a set of subprocesses to extract and transform the OSM data into
     * the geoclimate model. It uses a set of default values.
     *
     *  @param directory the path of the directory where the DB and result files should be stored
     *  @param idZone A string representing the inseeCode of the administrative level8 zone
     *  @param saveResults Set to true to save the result files in geojson and json in the @directory
     *
     * @return
     */
    static IProcess prepareOSMDefaultConfig() {
        return processFactory.create("Extract and transform OSM data to Geoclimate model",
                [directory : String,
                 idZone : String,
                 saveResults : boolean],
                [datasource: JdbcDataSource],
                { directory, idZone , saveResults -> ;
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

                    IProcess prepareOSMData = prepareOSM()
                    prepareOSMData.execute([
                            hLevMin: 3,
                            hLevMax: 15,
                            hThresholdLev2: 10,
                            directory : directory,
                            osmTablesPrefix: "EXT",
                            idZone : idZone,
                            expand : 500,
                            distBuffer:1000,
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
                            saveResults : saveResults
                    ])

                    [datasource: prepareOSMData.getResults().datasource]

                });
    }

    /**
     * This process chains a set of subprocesses to extract and transform the OSM data into
     * the geoclimate model
     *
     * @param directory the path of the directory where the DB and result files should be stored
     * @param osmTablesPrefix The prefix used for naming the 11 OSM tables build from the OSM file     *
     * @param idZone A string representing the inseeCode of the administrative level8 zone
     * @param expand The integer value of the Extended Zone
     * @param distBuffer The integer value of the Zone buffer
     *
     * @param saveResults Set to true to save the result files in geojson and json in the @directory
     * @return
     */
    static IProcess prepareOSM() {
        return processFactory.create("Extract and transform OSM data to Geoclimate model",
                [directory : String,
                 osmTablesPrefix: String,
                 idZone : String,
                 expand : int,
                 distBuffer : int,
                 hLevMin: int,
                 hLevMax: int,
                 hThresholdLev2: int,
                 buildingTableColumnsNames: Map,
                 buildingTagKeys: String[],
                 buildingTagValues: String[],
                 tablesPrefix: String,
                 buildingFilter: String,
                 roadTableColumnsNames: Map,
                 roadTagKeys: String[],
                 roadTagValues: String[],
                 roadFilter: String,
                 railTableColumnsNames: Map,
                 railTagKeys: String[],
                 railTagValues: String[],
                 railFilter: String[],
                 vegetTableColumnsNames: Map,
                 vegetTagKeys: String[],
                 vegetTagValues: String[],
                 vegetFilter: String,
                 hydroTableColumnsNames: Map ,
                 hydroTags: Map,
                 hydroFilter: String,
                 mappingForTypeAndUse : Map,
                 mappingForRoadType : Map,
                 mappingForSurface : Map,
                 mappingForRailType : Map,
                 mappingForVegetType : Map,
                 saveResults : boolean],
                [datasource: JdbcDataSource],
                { directory, osmTablesPrefix, idZone , expand, distBuffer,  hLevMin, hLevMax,hThresholdLev2, buildingTableColumnsNames,
                    buildingTagKeys,buildingTagValues,
                    tablesPrefix,
                    buildingFilter,
                    roadTableColumnsNames,
                    roadTagKeys,
                    roadTagValues,
                    roadFilter,
                    railTableColumnsNames,
                    railTagKeys,
                    railTagValues,
                    railFilter,
                    vegetTableColumnsNames,
                    vegetTagKeys,
                    vegetTagValues,
                    vegetFilter,
                    hydroTableColumnsNames,
                    hydroTags,
                    hydroFilter,
                    mappingForTypeAndUse,
                    mappingForRoadType,
                    mappingForSurface,
                    mappingForRailType,
                    mappingForVegetType,
                    saveResults -> ;

                    if(directory==null){
                        logger.info("The directory to save the data cannot be null")
                        return
                    }
                    File dirFile = new File(directory)

                    if(!dirFile.exists()){
                        dirFile.mkdir()
                        logger.info("The folder ${directory} has been created")
                    }
                    else if (!dirFile.isDirectory()){
                        logger.info("Invalid directory path")
                        return
                    }


                    String dbPath = dirFile.absolutePath+ File.separator+ "osmdb"

                    IProcess loadInitialData = org.orbisgis.osm.OSMGISLayers.loadInitialData()

                    loadInitialData.execute([
                            dbPath : dbPath,
                            osmTablesPrefix: osmTablesPrefix,
                            idZone : idZone,
                            expand : expand,
                            distBuffer:distBuffer])

                    logger.info("The OSM data has been downloaded for the zone id : ${idZone}.")

                    //The connection to the database
                    JdbcDataSource datasource = loadInitialData.getResults().outDatasource

                    if(datasource==null){
                        logger.error("Cannot create the database to store the osm data")
                        return
                    }
                    //Init model
                    IProcess initParametersAbstract = org.orbisgis.common.AbstractTablesInitialization.initParametersAbstract()
                    initParametersAbstract.execute(datasource:datasource)

                    logger.info("The geoclimate data model has been initialized.")

                    logger.info("Transform OSM data to GIS tables.")

                    IProcess prepareBuildings = org.orbisgis.osm.OSMGISLayers.prepareBuildings()

                    prepareBuildings.execute([datasource:datasource, osmTablesPrefix:osmTablesPrefix,
                                              buildingTableColumnsNames : buildingTableColumnsNames,
                                              buildingTagKeys:buildingTagKeys,
                                              buildingTagValues:buildingTagValues,
                                              buildingTagValues:buildingTagValues,
                                              tablesPrefix:tablesPrefix,
                                              buildingFilter:buildingFilter,
                    ]);
                    IProcess prepareRoads = org.orbisgis.osm.OSMGISLayers.prepareRoads()
                    prepareRoads.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          roadTableColumnsNames: roadTableColumnsNames,
                                          roadTagKeys: roadTagKeys,
                                          roadTagValues: roadTagValues,
                                          tablesPrefix: tablesPrefix,
                                          roadFilter: roadFilter])

                    IProcess prepareRails = org.orbisgis.osm.OSMGISLayers.prepareRails()
                    prepareRails.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          railTableColumnsNames: railTableColumnsNames,
                                          railTagKeys: railTagKeys,
                                          railTagValues: railTagValues,
                                          tablesPrefix: tablesPrefix,
                                          railFilter: railFilter])

                    IProcess prepareVeget = org.orbisgis.osm.OSMGISLayers.prepareVeget()
                    prepareVeget.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          vegetTableColumnsNames: vegetTableColumnsNames,
                                          vegetTagKeys: vegetTagKeys,
                                          vegetTagValues: vegetTagValues,
                                          tablesPrefix: tablesPrefix,
                                          vegetFilter: vegetFilter])

                    IProcess prepareHydro = org.orbisgis.osm.OSMGISLayers.prepareHydro()
                    prepareHydro.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          hydroTableColumnsNames: hydroTableColumnsNames,
                                          hydroTags: hydroTags,
                                          tablesPrefix: tablesPrefix,
                                          hydroFilter: hydroFilter])

                    IProcess transformBuildings = org.orbisgis.osm.FormattingForAbstractModel.transformBuildings()
                    transformBuildings.execute([datasource : datasource,
                            inputTableName      : prepareBuildings.getResults().buildingTableName,
                            mappingForTypeAndUse: mappingForTypeAndUse])
                    def inputBuilding =  transformBuildings.getResults().outputTableName

                    IProcess transformRoads = org.orbisgis.osm.FormattingForAbstractModel.transformRoads()
                    transformRoads.execute([datasource : datasource,
                            inputTableName      : prepareRoads.getResults().roadTableName,
                            mappingForRoadType: mappingForRoadType,
                            mappingForSurface: mappingForSurface])
                    def inputRoads =  transformRoads.getResults().outputTableName


                    IProcess transformRails = org.orbisgis.osm.FormattingForAbstractModel.transformRails()
                    transformRails.execute([datasource          : datasource,
                            inputTableName : prepareRails.getResults().railTableName,
                            mappingForRailType: mappingForRailType])
                    def inputRail =  transformRails.getResults().outputTableName

                    IProcess transformVeget = org.orbisgis.osm.FormattingForAbstractModel.transformVeget()
                    transformVeget.execute([datasource    : datasource,
                                            inputTableName: prepareVeget.getResults().vegetTableName,
                                            mappingForVegetType: mappingForVegetType])
                    def inputVeget =  transformVeget.getResults().outputTableName

                    IProcess transformHydro = org.orbisgis.osm.FormattingForAbstractModel.transformHydro()
                    transformHydro.execute([datasource    : datasource,
                                            inputTableName: prepareHydro.getResults().hydroTableName])
                    def inputHydro =  transformHydro.getResults().outputTableName

                    logger.info("All OSM data have been tranformed to GIS tables.")

                    logger.info("Formating GIS tables to Geoclimate model...")

                    def initResults = initParametersAbstract.getResults()

                    def inputZone = loadInitialData.getResults().outputZone
                    def inputZoneNeighbors = loadInitialData.getResults().outputZoneNeighbors

                    IProcess inputDataFormatting = org.orbisgis.common.InputDataFormatting.inputDataFormatting()

                    inputDataFormatting.execute([datasource: datasource,
                                     inputBuilding: inputBuilding, inputRoad: inputRoads, inputRail: inputRail,
                                     inputHydro: inputHydro, inputVeget: inputVeget,
                                     inputZone: inputZone, inputZoneNeighbors: inputZoneNeighbors,
                                     hLevMin: hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2, idZone: idZone,
                                     buildingAbstractUseType: initResults.outputBuildingAbstractUseType, buildingAbstractParameters: initResults.outputBuildingAbstractParameters,
                                     roadAbstractType: initResults.outputRoadAbstractType, roadAbstractParameters: initResults.outputRoadAbstractParameters,
                                     railAbstractType: initResults.outputRailAbstractType,
                                     vegetAbstractType: initResults.outputVegetAbstractType,
                                                 vegetAbstractParameters: initResults.outputVegetAbstractParameters])

                    logger.info("End of the OSM extract transform process.")

                    if(saveResults){

                        logger.info("Saving GIS layers in geojson format")
                        String finalBuildings = inputDataFormatting.getResults().outputBuilding
                        datasource.save(finalBuildings, dirFile.absolutePath+File.separator+"${finalBuildings}_${idZone}.geojson")

                        String finalRoads = inputDataFormatting.getResults().outputRoad
                        datasource.save(finalRoads, dirFile.absolutePath+File.separator+"${finalRoads}_${idZone}.geojson")

                        String finalRails= inputDataFormatting.getResults().outputRail
                        datasource.save(finalRails, dirFile.absolutePath+File.separator+"${finalRails}_${idZone}.geojson")

                        String finalHydro = inputDataFormatting.getResults().outputHydro
                        datasource.save(finalHydro, dirFile.absolutePath+File.separator+"${finalHydro}_${idZone}.geojson")

                        String finalVeget = inputDataFormatting.getResults().outputVeget
                        datasource.save(finalVeget, dirFile.absolutePath+File.separator+"${finalVeget}_${idZone}.geojson")

                        String finalZone = inputDataFormatting.getResults().outputZone
                        datasource.save(finalZone, dirFile.absolutePath+File.separator+"${finalZone}_${idZone}.geojson")

                        logger.info("Saving statistic tables in csv format")

                        String finalOutputBuildingStatZone = inputDataFormatting.getResults().outputBuildingStatZone
                        datasource.save(finalOutputBuildingStatZone, dirFile.absolutePath+File.separator+"${finalOutputBuildingStatZone}_${idZone}.csv")

                        String finalOutputBuildingStatZoneBuff = inputDataFormatting.getResults().outputBuildingStatZoneBuff
                        datasource.save(finalOutputBuildingStatZoneBuff, dirFile.absolutePath+File.separator+"${finalOutputBuildingStatZoneBuff}_${idZone}.csv")

                        String finalOutputRoadStatZone = inputDataFormatting.getResults().outputRoadStatZone
                        datasource.save(finalOutputRoadStatZone, dirFile.absolutePath+File.separator+"${finalOutputRoadStatZone}_${idZone}.csv")

                        String finalOutputRoadStatZoneBuff = inputDataFormatting.getResults().outputRoadStatZoneBuff
                        datasource.save(finalOutputRoadStatZoneBuff, dirFile.absolutePath+File.separator+"${finalOutputRoadStatZoneBuff}_${idZone}.csv")

                        String finalOutputRailStatZone = inputDataFormatting.getResults().outputRailStatZone
                        datasource.save(finalOutputRailStatZone, dirFile.absolutePath+File.separator+"${finalOutputRailStatZone}_${idZone}.csv")

                        String finalOutputHydroStatZone = inputDataFormatting.getResults().outputHydroStatZone
                        datasource.save(finalOutputHydroStatZone, dirFile.absolutePath+File.separator+"${finalOutputHydroStatZone}_${idZone}.csv")

                        String finalOutputHydroStatZoneExt= inputDataFormatting.getResults().outputHydroStatZoneExt
                        datasource.save(finalOutputHydroStatZoneExt, dirFile.absolutePath+File.separator+"${finalOutputHydroStatZoneExt}_${idZone}.csv")

                        String finalOutputVegetStatZone = inputDataFormatting.getResults().outputVegetStatZone
                        datasource.save(finalOutputVegetStatZone, dirFile.absolutePath+File.separator+"${finalOutputVegetStatZone}_${idZone}.csv")

                        String finalOutputVegetStatZoneExt = inputDataFormatting.getResults().outputVegetStatZoneExt
                        datasource.save(finalOutputVegetStatZoneExt, dirFile.absolutePath+File.separator+"${finalOutputVegetStatZoneExt}_${idZone}.csv")

                    }

                    [datasource: datasource]

                })
    }

    /** The processing chain creates the units used to describe the territory at three scales: Reference Spatial
     * Unit (RSU), block and building. The creation of the RSU needs several layers such as the hydrology,
     * the vegetation, the roads and the rail network and the boundary of the study zone. The blocks are created
     * from the buildings that are in contact.
     * Then the relationships between each scale is initialized in each unit table: the RSU ID is stored in
     * the block and in the building tables whereas the block ID is stored only in the building table.
     *
     * @param zoneTable The area of zone to be processed
     * @param roadTable The road table to be processed
     * @param railTable The rail table to be processed
     * @param vegetationTable The vegetation table to be processed
     * @param hydrographicTable The hydrographic table to be processed
     * @param surface_vegetation The minimum area of vegetation that will be considered to delineate the RSU (default 100,000 m²)
     * @param surface_hydro  The minimum area of water that will be considered to delineate the RSU (default 2,500 m²)
     * @param inputTableName The input table where are stored the geometries used to create the block (e.g. buildings...)
     * @param distance A distance to group two geometries (e.g. two buildings in a block - default 0.01 m)
     * @param inputLowerScaleTableName The input table where are stored the lowerScale objects (buildings)
     * @param prefixName A prefix used to name the output table
     * @param datasource A connection to a database
     *
     * @return outputTableBuildingName Table name where are stored the buildings and the RSU and block ID
     * @return outputTableBlockName Table name where are stored the blocks and the RSU ID
     * @return outputTableRsuName Table name where are stored the RSU
     */
    static IProcess createUnitsOfAnalysis(){
        return processFactory.create("Merge the geometries that touch each other",
                [datasource: JdbcDataSource, zoneTable : String, roadTable : String, railTable : String,
                 vegetationTable: String, hydrographicTable: String, surface_vegetation: double,
                 surface_hydro: double, inputTableName: String, distance: double,
                 inputLowerScaleTableName: String,  prefixName: String],
                [outputTableBuildingName : String, outputTableBlockName: String, outputTableRsuName: String],
                { datasource, zoneTable, roadTable, railTable, vegetationTable, hydrographicTable,
                  surface_vegetation, surface_hydro, inputTableName, distance, inputLowerScaleTableName,  prefixName ->
                    logger.info("Create the units of analysis...")

                    // Create the RSU
                    IProcess prepareRSUData = SpatialUnits.prepareRSUData()
                    IProcess createRSU = SpatialUnits.createRSU()
                    prepareRSUData.execute([datasource: datasource, zoneTable : zoneTable, roadTable : roadTable,
                                            railTable : railTable, vegetationTable: vegetationTable,
                                            hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                            surface_hydro: surface_hydro, prefixName: prefixName])
                    createRSU.execute([datasource: datasource, inputTableName : prepareRSUData.results.outputTableName,
                                       prefixName: prefixName])

                    // Create the blocks
                    IProcess createBlocks = SpatialUnits.createBlocks()
                    createBlocks.execute([datasource: datasource, inputTableName : inputTableName,
                                          prefixName: prefixName, distance: distance])


                    // Create the relations between RSU and blocks (store in the block table)
                    IProcess createScalesRelationsRsuBl = SpatialUnits.createScalesRelations()
                    createScalesRelationsRsuBl.execute([datasource: datasource,
                                                        inputLowerScaleTableName: createBlocks.results.outputTableName,
                                                        inputUpperScaleTableName: createRSU.results.outputTableName,
                                                        idColumnUp: createRSU.results.outputIdRsu,
                                                        prefixName: prefixName])


                    // Create the relations between buildings and blocks (store in the buildings table)
                    IProcess createScalesRelationsBlBu = SpatialUnits.createScalesRelations()
                    createScalesRelationsBlBu.execute([datasource: datasource,
                                                       inputLowerScaleTableName: inputLowerScaleTableName,
                                                       inputUpperScaleTableName: createBlocks.results.outputTableName,
                                                       idColumnUp: createBlocks.results.outputIdBlock,
                                                       prefixName: prefixName])


                    // Create the relations between buildings and RSU (store in the buildings table)
                    // WARNING : the building table will contain the id_block and id_rsu for each of its
                    // id_build but the relations between id_block and i_rsu should not been consider in this Table
                    // the relationships may indeed be different from the one in the block Table
                    IProcess createScalesRelationsRsuBlBu = SpatialUnits.createScalesRelations()
                    createScalesRelationsRsuBlBu.execute([datasource: datasource,
                                                          inputLowerScaleTableName:
                                                                  createScalesRelationsBlBu.results.outputTableName,
                                                          inputUpperScaleTableName: createRSU.results.outputTableName,
                                                          idColumnUp: createRSU.results.outputIdRsu,
                                                          prefixName: prefixName])


                    [outputTableBuildingName : createScalesRelationsRsuBlBu.results.outputTableName,
                     outputTableBlockName: createScalesRelationsRsuBl.results.outputTableName,
                     outputTableRsuName: createRSU.results.outputTableName]
                }
        )
    }

    /** The processing chain calculates the 7 geometric and surface cover properties used to identify local climate
     * zones (sky view factor, aspect ratio, building surface fraction, impervious surface fraction,
     * pervious surface fraction, height of roughness elements and terrain roughness class).
     *
     * @param buildingTable The table where are stored informations concerning buildings (and the id of the corresponding rsu)
     * @param rsuTable The table where are stored informations concerning RSU
     * @param roadTable The table where are stored informations concerning roads
     * @param vegetationTable The table where are stored informations concerning vegetation
     * @param hydrographicTable The table where are stored informations concerning water
     * @param facadeDensListLayersBottom the list of height corresponding to the bottom of each vertical layers used for calculation
     * of the rsu_projected_facade_area_density which is then used to calculate the height of roughness (default [0]
     * @param facadeDensNumberOfDirection The number of directions used for the calculation - according to the method used it should
     * be divisor of 360 AND a multiple of 2 (default 8)
     * @param pointDensity The density of points (nb / free m²) used to calculate the spatial average SVF (default 0.008)
     * @param rayLength The maximum distance to consider an obstacle as potential sky cover (default 100)
     * @param numberOfDirection the number of directions considered to calculate the SVF (default 60)
     * @param heightColumnName The name of the column (in the building table) used for roughness height calculation (default "height_roof")
     * @param fractionTypePervious The type of surface that should be consider to calculate the fraction of pervious soil
     * (default ["low_vegetation", "water"] but possible parameters are ["low_vegetation", "high_vegetation", "water"])
     * @param fractionTypeImpervious The type of surface that should be consider to calculate the fraction of impervious soil
     * (default ["road"] but possible parameters are ["road", "building"])
     * @param inputFields The fields of the buildingTable that should be kept in the analysis (default ["the_geom", "id_build"]
     * @param levelForRoads If the road surfaces are considered for the calculation of the impervious fraction,
     * you should give the list of road zindex to consider (default [0])
     * @param prefixName A prefix used to name the output table
     * @param datasource A connection to a database
     *
     * @return outputTableName Table name where are stored the resulting RSU
     */
    static IProcess createLCZ(){
        return processFactory.create("Create the LCZ",
                [datasource: JdbcDataSource, prefixName: String, buildingTable : String, rsuTable : String, roadTable : String,
                 vegetationTable: String, hydrographicTable: String, facadeDensListLayersBottom: double[],
                 facadeDensNumberOfDirection: int, svfPointDensity: double, svfRayLength: double,
                 svfNumberOfDirection: int, heightColumnName: String,
                 fractionTypePervious: String[], fractionTypeImpervious : String[], inputFields: String[],
                 levelForRoads: String[]],
                [outputTableName: String],
                { datasource, prefixName, buildingTable, rsuTable, roadTable, vegetationTable,
                  hydrographicTable, facadeDensListLayersBottom = [0, 50, 200], facadeDensNumberOfDirection = 8,
                  svfPointDensity = 0.008, svfRayLength = 100, svfNumberOfDirection = 60,
                  heightColumnName = "height_roof", fractionTypePervious = ["low_vegetation", "water"],
                  fractionTypeImpervious = ["road"], inputFields = ["id_build", "the_geom"], levelForRoads = [0] ->
                    logger.info("Create the LCZ...")


                    // To avoid overwriting the output files of this step, a unique identifier is created
                    def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                    // Temporary table names
                    def rsu_surf_fractions = "rsu_surf_fractions" + uid_out
                    def rsu_indic0 = "rsu_indic0" + uid_out
                    def rsu_indic1 = "rsu_indic1" + uid_out
                    def rsu_indic2 = "rsu_indic2" + uid_out
                    def rsu_indic3 = "rsu_indic3" + uid_out
                    def buAndIdRsu = "buAndIdRsu" + uid_out
                    def corr_tempo = "corr_tempo" + uid_out
                    def rsu_all_indic = "rsu_all_indic" + uid_out
                    def rsu_4_aspectratio = "rsu_4_aspectratio" + uid_out
                    def rsu_4_roughness0 = "rsu_4_roughness0" + uid_out
                    def rsu_4_roughness1 = "rsu_4_roughness1" + uid_out
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_surf_fractions; CREATE TABLE $rsu_surf_fractions " +
                            "AS SELECT * FROM $rsuTable").toString())

                    def veg_type = []
                    def perv_type = []
                    def imp_type = []
                    def columnIdRsu = "id_rsu"
                    def columnIdBu = "id_build"
                    def geometricColumn = "the_geom"

                    // I. Calculate preliminary indicators needed for the other calculations (the relations of chaining between
                    // the indicators are illustrated with the scheme IProcessA --> IProcessB)
                    // calc_building_area --> calc_build_densityNroughness
                    IProcess  calc_building_area =  Geoclimate.GenericIndicators.geometryProperties()
                    calc_building_area.execute([inputTableName: buildingTable, inputFields:inputFields,
                                                operations: ["st_area"], prefixName : prefixName, datasource:datasource])

                    // calc_veg_frac --> calc_perviousness_frac  (calculate only if vegetation considered as pervious)
                    if(fractionTypePervious.contains("low_vegetation") | fractionTypePervious.contains("high_vegetation")){
                        if(fractionTypePervious.contains("low_vegetation") & fractionTypePervious.contains("high_vegetation")){
                            veg_type.add("all")
                            perv_type.add("all_vegetation_fraction")
                        }
                        else if(fractionTypePervious.contains("low_vegetation")){
                            veg_type.add("low")
                            perv_type.add("low_vegetation_fraction")
                        }
                        else if(fractionTypePervious.contains("high_vegetation")){
                            veg_type.add("high")
                            perv_type.add("high_vegetation_fraction")
                        }
                        IProcess  calc_veg_frac =  Geoclimate.RsuIndicators.vegetationFraction()
                        calc_veg_frac.execute([rsuTable: rsuTable, vegetTable: vegetationTable, fractionType: veg_type,
                                               prefixName: prefixName, datasource: datasource])
                        // Add the vegetation field in the surface fraction tables used for pervious and impervious fractions
                        datasource.execute(("ALTER TABLE $rsu_surf_fractions ADD COLUMN ${perv_type[0]} DOUBLE; " +
                                "UPDATE $rsu_surf_fractions SET ${perv_type[0]} = (SELECT b.${perv_type[0]} FROM " +
                                "${calc_veg_frac.results.outputTableName} b WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)").toString())
                    }

                    // calc_road_frac --> calc_perviousness_frac  (calculate only if road considered as impervious)
                    if(fractionTypeImpervious.contains("road")){
                        imp_type.add("ground_road_fraction")
                        IProcess  calc_road_frac =  Geoclimate.RsuIndicators.roadFraction()
                        calc_road_frac.execute([rsuTable: rsuTable, roadTable: roadTable, levelToConsiders:
                                ["ground" : levelForRoads], prefixName: prefixName, datasource: datasource])
                        // Add the road field in the surface fraction tables used for pervious and impervious fractions
                        datasource.execute(("ALTER TABLE $rsu_surf_fractions ADD COLUMN ground_road_fraction DOUBLE; " +
                                "UPDATE $rsu_surf_fractions SET ground_road_fraction = (SELECT b.ground_road_fraction" +
                                " FROM ${calc_road_frac.results.outputTableName} b " +
                                "WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)").toString())
                    }

                    // calc_water_frac --> calc_perviousness_frac  (calculate only if water considered as pervious)
                    if(fractionTypePervious.contains("water")){
                        perv_type.add("water_fraction")
                        IProcess  calc_water_frac =  Geoclimate.RsuIndicators.waterFraction()
                        calc_water_frac.execute([rsuTable: rsuTable, waterTable: hydrographicTable, prefixName: "test",
                                                 datasource: datasource])
                        // Add the water field in the surface fraction tables used for pervious and impervious fractions
                        datasource.execute(("ALTER TABLE $rsu_surf_fractions ADD COLUMN water_fraction DOUBLE; " +
                                "UPDATE $rsu_surf_fractions SET water_fraction = (SELECT b.water_fraction" +
                                " FROM ${calc_water_frac.results.outputTableName} b " +
                                "WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)").toString())
                    }

                    // calc_build_contiguity    -->
                    //                                  calc_free_ext_density --> calc_aspect_ratio
                    // calc_build_facade_length -->
                    IProcess  calc_build_contiguity =  Geoclimate.BuildingIndicators.neighborsProperties()
                    calc_build_contiguity.execute([inputBuildingTableName: buildingTable,
                                                   operations:["building_contiguity"], prefixName : prefixName,datasource:datasource])

                    IProcess  calc_build_facade_length =  Geoclimate.BuildingIndicators.sizeProperties()
                    calc_build_facade_length.execute([inputBuildingTableName: buildingTable,
                                                      operations:["building_total_facade_length"], prefixName : prefixName,
                                                      datasource:datasource])

                                // This SQL query should not be done if the following IProcess were normally coded...
                    datasource.execute(("DROP TABLE IF EXISTS $corr_tempo; CREATE TABLE $corr_tempo AS SELECT a.*," +
                            "b.$columnIdBu FROM $rsuTable a, $buildingTable b WHERE a.$columnIdRsu = b.$columnIdRsu").toString())
                    IProcess  calc_free_ext_density =  Geoclimate.RsuIndicators.freeExternalFacadeDensity()
                    calc_free_ext_density.execute([buildingTable: buildingTable, correlationTable: corr_tempo,
                                                   buContiguityColumn: "building_contiguity", buTotalFacadeLengthColumn:
                                                           "building_total_facade_length", prefixName: prefixName, datasource: datasource])

                    // calc_proj_facade_dist --> calc_effective_roughness_height
                    IProcess  calc_proj_facade_dist =  Geoclimate.RsuIndicators.projectedFacadeAreaDistribution()
                    calc_proj_facade_dist.execute([buildingTable: buildingTable, rsuTable: rsuTable,
                                                   listLayersBottom: facadeDensListLayersBottom, numberOfDirection: facadeDensNumberOfDirection,
                                                   prefixName: "test", datasource: datasource])

                    // II. Calculate the LCZ indicators
                    // Calculate the BUILDING SURFACE FRACTION from the building area
                    // AND the HEIGHT OF ROUGHNESS ELEMENTS from the building roof height
                    // calc_build_densityNroughness --> calcSVF (which needs building_density)
                    // calc_build_densityNroughness --> calc_effective_roughness_height (which needs geometric_height)
                    IProcess  calc_build_densityNroughness =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
                    datasource.execute(("DROP TABLE IF EXISTS $buAndIdRsu; CREATE TABLE $buAndIdRsu AS SELECT a.*," +
                            "b.$columnIdRsu, b.$heightColumnName FROM ${calc_building_area.results.outputTableName} a, $buildingTable b " +
                            "WHERE a.$columnIdBu = b.$columnIdBu").toString())
                    calc_build_densityNroughness.execute([inputLowerScaleTableName: buAndIdRsu,
                                                          inputUpperScaleTableName: rsuTable, inputIdUp: columnIdRsu,
                                                          inputVarAndOperations: [(heightColumnName): ["GEOM_AVG"], "st_area":["DENS"]],
                                                          prefixName: prefixName, datasource: datasource])

                    // Calculate the SKY VIEW FACTOR from the RSU building density
                    // Merge the geometric average and the building density into the RSU table
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_indic0; CREATE TABLE $rsu_indic0 AS SELECT a.*, " +
                            "b.dens_st_area, b.geom_avg_$heightColumnName FROM $rsuTable a INNER JOIN " +
                            "${calc_build_densityNroughness.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    IProcess  calcSVF =  Geoclimate.RsuIndicators.groundSkyViewFactor()
                    calcSVF.execute([rsuTable: rsu_indic0, correlationBuildingTable: buildingTable, rsuBuildingDensityColumn:
                            "dens_st_area", pointDensity: svfPointDensity, rayLength: svfRayLength,
                             numberOfDirection: svfNumberOfDirection, prefixName: prefixName, datasource: datasource])

                    // Calculate the ASPECT RATIO from the building fraction and the free external facade density
                    // Merge the free external facade density into the RSU table containing the other indicators
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_4_aspectratio; CREATE TABLE $rsu_4_aspectratio AS SELECT a.*, " +
                            "b.rsu_free_external_facade_density FROM $rsu_indic0 a INNER JOIN " +
                            "${calc_free_ext_density.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    IProcess  calc_aspect_ratio =  Geoclimate.RsuIndicators.aspectRatio()
                    calc_aspect_ratio.execute([rsuTable: rsu_4_aspectratio, rsuFreeExternalFacadeDensityColumn:
                            "rsu_free_external_facade_density", rsuBuildingDensityColumn: "dens_st_area",
                                               prefixName: prefixName, datasource: datasource])

                    // Calculate the PERVIOUS AND IMPERVIOUS SURFACE FRACTIONS
                    // Add the building density field in the surface fraction tables used for pervious and impervious fractions
                    // if the buildings are considered in the impervious fractions
                    if(fractionTypeImpervious.contains("building")) {
                        imp_type.add("dens_st_area")
                        datasource.execute("ALTER TABLE $rsu_surf_fractions ADD COLUMN dens_st_area DOUBLE; " +
                                "UPDATE $rsu_surf_fractions SET dens_st_area = (SELECT b.dens_st_area" +
                                " FROM ${calc_build_densityNroughness.results.outputTableName} b " +
                                "WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)")
                    }
                    IProcess calc_perviousness_frac = Geoclimate.RsuIndicators.perviousnessFraction()
                    calc_perviousness_frac.execute([rsuTable: rsu_surf_fractions, operationsAndComposition: ["pervious_fraction" :
                                                                                                                     perv_type,
                                                                                                             "impervious_fraction" :
                                                                                                                     imp_type],
                                                    prefixName: prefixName, datasource: datasource])

                    datasource.eachRow("SELECT * FROM ${calc_perviousness_frac.results.outputTableName}".toString()){
                        row ->
                            println(row)
                    }

                    // Calculate the TERRAIN ROUGHNESS CLASS
                    // Merge the geometric average and the projected facade area distribution into the RSU table
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_4_roughness0; CREATE TABLE $rsu_4_roughness0 AS SELECT a.*, " +
                            "b.geom_avg_$heightColumnName FROM $rsuTable a INNER JOIN " +
                            "${calc_build_densityNroughness.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_4_roughness1; CREATE TABLE $rsu_4_roughness1 AS SELECT b.*, " +
                            "a.$geometricColumn, a.geom_avg_$heightColumnName  FROM $rsu_4_roughness0 a INNER JOIN " +
                            "${calc_proj_facade_dist.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    // calc_effective_roughness_height --> calc_roughness_class
                    IProcess  calc_effective_roughness_height =  Geoclimate.RsuIndicators.effectiveTerrainRoughnessHeight()
                    calc_effective_roughness_height.execute([rsuTable: rsu_4_roughness1, projectedFacadeAreaName:
                            "rsu_projected_facade_area_distribution", geometricMeanBuildingHeightName: "geom_avg_$heightColumnName",
                                                             prefixName: prefixName, listLayersBottom: facadeDensListLayersBottom, numberOfDirection:
                                                                     facadeDensNumberOfDirection, datasource: datasource])
                    IProcess calc_roughness_class =  Geoclimate.RsuIndicators.effectiveTerrainRoughnessClass()
                    calc_roughness_class.execute([datasource: datasource, rsuTable: calc_effective_roughness_height.results.outputTableName,
                                                  effectiveTerrainRoughnessHeight: "rsu_effective_terrain_roughness", prefixName: prefixName])


                    // III. Define the LCZ of each RSU according to their 7 geometric and surface cover properties
                    // Merge all indicator columns in one table
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_indic1; CREATE TABLE $rsu_indic1 AS SELECT a.*, " +
                            "b.rsu_aspect_ratio FROM $rsu_indic0 a INNER JOIN " +
                            "${calc_aspect_ratio.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_indic2; CREATE TABLE $rsu_indic2 AS SELECT a.*, " +
                            "b.rsu_ground_sky_view_factor FROM $rsu_indic1 a INNER JOIN " +
                            "${calcSVF.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_indic3; CREATE TABLE $rsu_indic3 AS SELECT a.*, " +
                            "b.pervious_fraction, b.impervious_fraction FROM $rsu_indic2 a INNER JOIN " +
                            "${calc_perviousness_frac.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                    datasource.execute(("DROP TABLE IF EXISTS $rsu_all_indic; CREATE TABLE $rsu_all_indic AS SELECT a.*, " +
                            "b.effective_terrain_roughness_class FROM $rsu_indic3 a INNER JOIN " +
                            "${calc_roughness_class.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())

                    datasource.execute("DROP TABLE IF EXISTS $rsu_indic0, $rsu_indic1, $rsu_indic2, $rsu_indic3".toString())

                    datasource.eachRow("SELECT * FROM ${rsu_all_indic}".toString()){
                        row ->
                            println(row)
                    }

                    [outputTableName: rsu_all_indic]
                }
        )
    }
}

