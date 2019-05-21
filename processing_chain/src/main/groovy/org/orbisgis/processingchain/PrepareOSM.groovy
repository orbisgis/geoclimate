package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.SpatialUnits
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.osm.OSMGISLayers
import org.orbisgis.processingchain.ProcessingChain
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.processmanagerapi.IProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory


@BaseScript ProcessingChain processingChain

    /**
     * This process chains a set of subprocesses to extract and transform the OSM data into
     * the geoclimate model. It uses a set of default values.
     *
     *  @param datasource a connection to the database where the result files should be stored
     *  @param idZone A string representing the inseeCode of the administrative level8 zone
     *
     * @return
     */
    public static IProcess prepareOSMDefaultConfig() {
        return processFactory.create("Extract and transform OSM data to Geoclimate model (default configuration)",
                [datasource : JdbcDataSource,
                 idZone : String],
                [outputBuilding : String, outputRoad:String, outputRail : String, outputHydro:String, outputVeget:String, outputZone:String,outputStats : String[]],
                { datasource, idZone  ->
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
                            datasource : datasource,
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
                    ])
                    [outputBuilding : prepareOSMData.getResults().outputBuilding,
                     outputRoad:prepareOSMData.getResults().outputRoad,
                     outputRail : prepareOSMData.getResults().outputRail,
                     outputHydro:prepareOSMData.getResults().outputHydro,
                     outputVeget:prepareOSMData.getResults().outputVeget,
                     outputZone:prepareOSMData.getResults().outputZone,
                     outputStats : prepareOSMData.getResults().outputStats]

                });
    }

    /**
     * This process chains a set of subprocesses to extract and transform the OSM data into
     * the geoclimate model
     *
     * @param datasource a connection to the database where the result files should be stored
     * @param osmTablesPrefix The prefix used for naming the 11 OSM tables build from the OSM file
     * @param idZone A string representing the inseeCode of the administrative level8 zone
     * @param expand The integer value of the Extended Zone
     * @param distBuffer The integer value of the Zone buffer
     * @return
     */
    public static IProcess prepareOSM() {
        return processFactory.create("Extract and transform OSM data to Geoclimate model",
                [datasource : JdbcDataSource,
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
                 mappingForVegetType : Map],
                [outputBuilding : String, outputRoad:String, outputRail : String, outputHydro:String, outputVeget:String, outputZone:String,outputStats : String[]],
                { datasource, osmTablesPrefix, idZone , expand, distBuffer,  hLevMin, hLevMax,hThresholdLev2, buildingTableColumnsNames,
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
                    mappingForVegetType ->

                    if(datasource==null){
                        logger.error("Cannot create the database to store the osm data")
                        return
                    }

                    IProcess loadInitialData = org.orbisgis.osm.OSMGISLayers.loadInitialData()

                    if(!loadInitialData.execute([
                            datasource : datasource,
                            osmTablesPrefix: osmTablesPrefix,
                            idZone : idZone,
                            expand : expand,
                            distBuffer:distBuffer])){
                        logger.info("Cannot downloaded for OSM data for the zone id : ${idZone}.")
                        return
                    }

                    logger.info("The OSM data has been downloaded for the zone id : ${idZone}.")


                    //Init model
                    IProcess initParametersAbstract = AbstractTablesInitialization.initParametersAbstract()
                    if(!initParametersAbstract.execute(datasource:datasource)){
                        logger.info("Cannot initialize the geoclimate data model.")
                        return
                    }

                    logger.info("The geoclimate data model has been initialized.")

                    logger.info("Transform OSM data to GIS tables.")

                    IProcess prepareBuildings = OSMGISLayers.prepareBuildings()

                    if(!prepareBuildings.execute([datasource:datasource, osmTablesPrefix:osmTablesPrefix,
                                              buildingTableColumnsNames : buildingTableColumnsNames,
                                              buildingTagKeys:buildingTagKeys,
                                              buildingTagValues:buildingTagValues,
                                              buildingTagValues:buildingTagValues,
                                              tablesPrefix:tablesPrefix,
                                              buildingFilter:buildingFilter,
                    ])){
                        logger.info("Cannot prepare the building table.")
                        return
                    }
                    IProcess prepareRoads = org.orbisgis.osm.OSMGISLayers.prepareRoads()
                    if(!prepareRoads.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          roadTableColumnsNames: roadTableColumnsNames,
                                          roadTagKeys: roadTagKeys,
                                          roadTagValues: roadTagValues,
                                          tablesPrefix: tablesPrefix,
                                          roadFilter: roadFilter])){
                        logger.info("Cannot prepare the road table.")
                        return
                    }

                    IProcess prepareRails = org.orbisgis.osm.OSMGISLayers.prepareRails()
                    if(!prepareRails.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          railTableColumnsNames: railTableColumnsNames,
                                          railTagKeys: railTagKeys,
                                          railTagValues: railTagValues,
                                          tablesPrefix: tablesPrefix,
                                          railFilter: railFilter])){
                        logger.info("Cannot prepare the rail table.")
                        return
                    }

                    IProcess prepareVeget = org.orbisgis.osm.OSMGISLayers.prepareVeget()
                    if(!prepareVeget.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          vegetTableColumnsNames: vegetTableColumnsNames,
                                          vegetTagKeys: vegetTagKeys,
                                          vegetTagValues: vegetTagValues,
                                          tablesPrefix: tablesPrefix,
                                          vegetFilter: vegetFilter])){
                        logger.info("Cannot prepare the vegetation table.")
                        return
                    }

                    IProcess prepareHydro = org.orbisgis.osm.OSMGISLayers.prepareHydro()
                    if(!prepareHydro.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          hydroTableColumnsNames: hydroTableColumnsNames,
                                          hydroTags: hydroTags,
                                          tablesPrefix: tablesPrefix,
                                          hydroFilter: hydroFilter])){
                        logger.info("Cannot prepare the hydrographic table.")
                        return
                    }

                    IProcess transformBuildings = org.orbisgis.osm.FormattingForAbstractModel.transformBuildings()
                    if(!transformBuildings.execute([datasource : datasource,
                            inputTableName      : prepareBuildings.getResults().buildingTableName,
                            mappingForTypeAndUse: mappingForTypeAndUse])){
                        logger.info("Cannot transform the building table to geoclimate model.")
                        return
                    }
                    def inputBuilding =  transformBuildings.getResults().outputTableName

                    IProcess transformRoads = org.orbisgis.osm.FormattingForAbstractModel.transformRoads()
                    if(!transformRoads.execute([datasource : datasource,
                            inputTableName      : prepareRoads.getResults().roadTableName,
                            mappingForRoadType: mappingForRoadType,
                            mappingForSurface: mappingForSurface])){
                        logger.info("Cannot transform the road table to geoclimate model.")
                        return
                    }
                    def inputRoads =  transformRoads.getResults().outputTableName


                    IProcess transformRails = org.orbisgis.osm.FormattingForAbstractModel.transformRails()
                    if(!transformRails.execute([datasource          : datasource,
                            inputTableName : prepareRails.getResults().railTableName,
                            mappingForRailType: mappingForRailType])){
                        logger.info("Cannot transform the rail table to geoclimate model.")
                        return
                    }
                    def inputRail =  transformRails.getResults().outputTableName

                    IProcess transformVeget = org.orbisgis.osm.FormattingForAbstractModel.transformVeget()
                    if(!transformVeget.execute([datasource    : datasource,
                                            inputTableName: prepareVeget.getResults().vegetTableName,
                                            mappingForVegetType: mappingForVegetType])){
                        logger.info("Cannot transform the vegetation table to geoclimate model.")
                        return
                    }
                    def inputVeget =  transformVeget.getResults().outputTableName

                    IProcess transformHydro = org.orbisgis.osm.FormattingForAbstractModel.transformHydro()
                    if(!transformHydro.execute([datasource    : datasource,
                                            inputTableName: prepareHydro.getResults().hydroTableName])){
                        logger.info("Cannot transform the hydrographic table to geoclimate model.")
                        return
                    }
                    def inputHydro =  transformHydro.getResults().outputTableName

                    logger.info("All OSM data have been tranformed to GIS tables.")

                    logger.info("Formating GIS tables to Geoclimate model...")

                    def initResults = initParametersAbstract.getResults()

                    def inputZone = loadInitialData.getResults().outputZone
                    def inputZoneNeighbors = loadInitialData.getResults().outputZoneNeighbors

                    IProcess inputDataFormatting = org.orbisgis.common.InputDataFormatting.inputDataFormatting()

                    if(!inputDataFormatting.execute([datasource: datasource,
                                     inputBuilding: inputBuilding, inputRoad: inputRoads, inputRail: inputRail,
                                     inputHydro: inputHydro, inputVeget: inputVeget,
                                     inputZone: inputZone, inputZoneNeighbors: inputZoneNeighbors,
                                     hLevMin: hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2, idZone: idZone,
                                     buildingAbstractUseType: initResults.outputBuildingAbstractUseType, buildingAbstractParameters: initResults.outputBuildingAbstractParameters,
                                     roadAbstractType: initResults.outputRoadAbstractType, roadAbstractParameters: initResults.outputRoadAbstractParameters,
                                     railAbstractType: initResults.outputRailAbstractType,
                                     vegetAbstractType: initResults.outputVegetAbstractType,
                                                 vegetAbstractParameters: initResults.outputVegetAbstractParameters])){
                        logger.info("Cannot format the tables to geoclimate model.")
                        return
                    }

                    logger.info("End of the OSM extract transform process.")

                    String finalBuildings = inputDataFormatting.getResults().outputBuilding
                    String finalRoads = inputDataFormatting.getResults().outputRoad
                    String finalRails= inputDataFormatting.getResults().outputRail
                    String finalHydro = inputDataFormatting.getResults().outputHydro
                    String finalVeget = inputDataFormatting.getResults().outputVeget
                    String finalZone = inputDataFormatting.getResults().outputZone

                    String finalOutputBuildingStatZone = inputDataFormatting.getResults().outputBuildingStatZone
                    String finalOutputBuildingStatZoneBuff = inputDataFormatting.getResults().outputBuildingStatZoneBuff
                    String finalOutputRoadStatZone = inputDataFormatting.getResults().outputRoadStatZone
                    String finalOutputRoadStatZoneBuff = inputDataFormatting.getResults().outputRoadStatZoneBuff
                    String finalOutputRailStatZone = inputDataFormatting.getResults().outputRailStatZone
                    String finalOutputHydroStatZone = inputDataFormatting.getResults().outputHydroStatZone
                    String finalOutputHydroStatZoneExt= inputDataFormatting.getResults().outputHydroStatZoneExt
                    String finalOutputVegetStatZone = inputDataFormatting.getResults().outputVegetStatZone
                    String finalOutputVegetStatZoneExt = inputDataFormatting.getResults().outputVegetStatZoneExt

                    [outputBuilding : finalBuildings, outputRoad:finalRoads,
                     outputRail : finalRails, outputHydro:finalHydro, outputVeget:finalVeget, outputZone:finalZone,
                     outputStats :[finalOutputBuildingStatZone,finalOutputBuildingStatZoneBuff,finalOutputRoadStatZone,
                                   finalOutputRoadStatZoneBuff,finalOutputRailStatZone,finalOutputHydroStatZone,finalOutputHydroStatZoneExt,
                                   finalOutputVegetStatZone,finalOutputVegetStatZoneExt]]

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
    public static IProcess createUnitsOfAnalysis(){
        return processFactory.create("Merge the geometries that touch each other",
                [datasource: JdbcDataSource, zoneTable : String, roadTable : String, railTable : String,
                 vegetationTable: String, hydrographicTable: String, surface_vegetation: Double,
                 surface_hydro: Double, inputTableName: String, distance: Double,
                 inputLowerScaleTableName: String,  prefixName: String],
                [outputTableBuildingName : String, outputTableBlockName: String, outputTableRsuName: String],
                { datasource, zoneTable, roadTable, railTable, vegetationTable, hydrographicTable,
                  surface_vegetation, surface_hydro, inputTableName, distance, inputLowerScaleTableName,  prefixName ->
                    logger.info("Create the units of analysis...")

                    // Create the RSU
                    IProcess prepareRSUData = SpatialUnits.prepareRSUData()
                    IProcess createRSU = SpatialUnits.createRSU()
                    if(!prepareRSUData.execute([datasource: datasource, zoneTable : zoneTable, roadTable : roadTable,
                                            railTable : railTable, vegetationTable: vegetationTable,
                                            hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                            surface_hydro: surface_hydro, prefixName: prefixName])){
                        logger.info("Cannot prepare the data to compute the RSU")
                        return
                    }

                    if(!createRSU.execute([datasource: datasource, inputTableName : prepareRSUData.results.outputTableName,
                                       prefixName: prefixName])){
                        logger.info("Cannot compute the RSU")
                        return
                    }

                    // Create the blocks
                    IProcess createBlocks = SpatialUnits.createBlocks()
                    if(!createBlocks.execute([datasource: datasource, inputTableName : inputTableName,
                                          prefixName: prefixName, distance: distance])){
                        logger.info("Cannot compute the block")
                        return
                    }


                    // Create the relations between RSU and blocks (store in the block table)
                    IProcess createScalesRelationsRsuBl = SpatialUnits.createScalesRelations()
                    if(!createScalesRelationsRsuBl.execute([datasource: datasource,
                                                        inputLowerScaleTableName: createBlocks.results.outputTableName,
                                                        inputUpperScaleTableName: createRSU.results.outputTableName,
                                                        idColumnUp: createRSU.results.outputIdRsu,
                                                        prefixName: prefixName])){
                        logger.info("Cannot create the relations between RSU and blocks")
                        return
                    }


                    // Create the relations between buildings and blocks (store in the buildings table)
                    IProcess createScalesRelationsBlBu = SpatialUnits.createScalesRelations()
                    if(!createScalesRelationsBlBu.execute([datasource: datasource,
                                                       inputLowerScaleTableName: inputLowerScaleTableName,
                                                       inputUpperScaleTableName: createBlocks.results.outputTableName,
                                                       idColumnUp: createBlocks.results.outputIdBlock,
                                                       prefixName: prefixName])){
                        logger.info("Cannot create the relations between buildings and blocks")
                        return
                    }


                    // Create the relations between buildings and RSU (store in the buildings table)
                    // WARNING : the building table will contain the id_block and id_rsu for each of its
                    // id_build but the relations between id_block and i_rsu should not been consider in this Table
                    // the relationships may indeed be different from the one in the block Table
                    IProcess createScalesRelationsRsuBlBu = SpatialUnits.createScalesRelations()
                    if(!createScalesRelationsRsuBlBu.execute([datasource: datasource,
                                                          inputLowerScaleTableName:
                                                                  createScalesRelationsBlBu.results.outputTableName,
                                                          inputUpperScaleTableName: createRSU.results.outputTableName,
                                                          idColumnUp: createRSU.results.outputIdRsu,
                                                          prefixName: prefixName])){
                        logger.info("Cannot create the relations between buildings and RSU")
                        return
                    }


                    [outputTableBuildingName : createScalesRelationsRsuBlBu.results.outputTableName,
                     outputTableBlockName: createScalesRelationsRsuBl.results.outputTableName,
                     outputTableRsuName: createRSU.results.outputTableName]
                }
        )
    }


