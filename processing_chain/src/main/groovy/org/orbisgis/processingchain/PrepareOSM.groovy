package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.common.InputDataFormatting
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.osm.FormattingForAbstractModel
import org.orbisgis.osm.OSMGISLayers

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
def prepareOSMDefaultConfig() {
    return create({
        title "Extract and transform OSM data to Geoclimate model (default configuration)"
        inputs datasource : JdbcDataSource, idZone : String
        outputs outputBuilding : String, outputRoad:String, outputRail : String, outputHydro:String, outputVeget:String, outputZone:String,outputStats : String[]
        { datasource, idZone ->
            def mappingTypeAndUse = [
                    terminal                         : [aeroway : ["terminal", "airport_terminal"],
                                                        amenity : ["terminal", "airport_terminal"],
                                                        building: ["terminal", "airport_terminal"]
                    ],
                    monument                         : [
                            building: ["monument"],
                            historic: ["monument"],
                            leisure : ["monument"],
                            monument: ["yes"]
                    ],
                    religious                        : [
                            building        : ["religious", "abbey", "cathedral", "chapel", "church", "mosque",
                                               "musalla", "temple", "synagogue", "shrine", "place_of_worship",
                                               "wayside_shrine"],
                            amenity         : ["religious", "abbey", "cathedral", "chapel", "church",
                                               "mosque", "musalla", "temple", "synagogue", "shrine",
                                               "place_of_worship", "wayside_shrine"],
                            place_of_worship: ["! no", "! chapel", "! church"]
                    ],
                    sport                            : ["building": ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"],
                                                        "leisure" : ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"],
                                                        "amenity" : ["swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track"]
                    ],
                    sports_centre                    : ["building": ["sports_centre", "sports_hall"],
                                                        "leisure" : ["sports_centre", "sports_hall"],
                                                        "amenity" : ["sports_centre", "sports_hall"]
                    ],
                    chapel                           : ["building"        : ["chapel"],
                                                        "amenity"         : ["chapel"],
                                                        "place_of_worship": ["chapel"],
                    ],
                    church                           : ["building"        : ["church"],
                                                        "amenity"         : ["church"],
                                                        "place_of_worship": ["church"],
                    ],
                    castle                           : ["building": ["castle", "fortress"],
                    ],
                    military                         : ["military": ["ammunition", "bunker", "barracks", "casemate", "office", "shelter"],
                                                        "building": ["ammunition", "bunker", "barracks", "casemate", "military", "shelter"],
                                                        "office"  : ["military"]
                    ],
                    train_station                    : ["building"        : ["train_station"],
                                                        "railway"         : ["station", "train_station"],
                                                        "public_transport": ["train_station"],
                                                        "amenity"         : ["train_station"]
                    ],
                    townhall                         : ["amenity" : ["townhall"],
                                                        "building": ["townhall"]
                    ],
                    toll                             : ["barrier" : ["toll_booth"],
                                                        "building": ["toll_booth"]
                    ],
                    government                       : ["building"  : ["government", "government_office"],
                                                        "government": ["! no"],
                                                        "office"    : ["government"]
                    ],
                    historic                         : ["building"         : ["historic"],
                                                        "historic"         : [],
                                                        "historic_building": ["! no"]
                    ],
                    grandstand                       : ["building"  : ["grandstand"],
                                                        "leisure"   : ["grandstand"],
                                                        "amenity"   : ["grandstand"],
                                                        "grandstand": ["yes"]
                    ],
                    detached                         : ["building": ["detached"],
                                                        "house"   : ["detached"]
                    ],
                    farm_auxiliary                   : ["building": ["farm_auxiliary", "barn", "stable", "sty", "cowshed", "digester", "greenhouse"]
                    ],
                    commercial                       : ["building": ["bank", "bureau_de_change", "boat_rental", "car_rental", "commercial", "internet_cafe", "kiosk", "money_transfer", "market", "market_place", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse"],
                                                        "amenity" : ["bank", "bureau_de_change", "boat_rental", "car_rental", "commercial", "internet_cafe", "kiosk", "money_transfer", "market", "market_place", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse"],
                                                        "shop"    : ["!= no"]
                    ],
                    industrial                       : ["building"  : ["industrial", "factory", "warehouse"],
                                                        "industrial": ["factory"],
                                                        "amenity"   : ["factory"]
                    ],
                    greenhouse                       : ["building"  : ["greenhouse"],
                                                        "amenity"   : ["greenhouse"],
                                                        "industrial": ["greenhouse"]
                    ],
                    silo                             : ["building": ["silo", "grain_silo"],
                                                        "man_made": ["silo", "grain_silo"]
                    ],
                    house                            : ["building": ["house"],
                                                        "house"   : ["! no", "! detached", "! residential", "! villa"],
                                                        "amenity" : ["house"]
                    ],
                    residential                      : ["building"   : ["residential", "villa", "detached", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "terrace"],
                                                        "residential": ["university", "detached", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "building"],
                                                        "house"      : ["residential"],
                                                        "amenity"    : ["residential"]
                    ],
                    apartments                       : ["building"   : ["apartments"],
                                                        "residential": ["apartments"],
                                                        "amenity"    : ["apartments"],
                                                        "apartments" : ["yes"]
                    ],
                    bungalow                         : ["building": ["bungalow"],
                                                        "house"   : ["bungalow"],
                                                        "amenity" : ["bungalow"]
                    ],
                    ruins                            : ["building": ["ruins"],
                                                        "ruins"   : ["ruins"]
                    ],
                    agricultural                     : ["building"    : ["agricultural"],
                                                        "agricultural": ["building"]
                    ],
                    farm                             : ["building": ["farm", "farmhouse"]
                    ],
                    barn                             : ["building": ["barn"],
                                                        "barn"    : ["! no"]
                    ],
                    transportation                   : ["building"        : ["train_station", "transportation", "station"],
                                                        "aeroway"         : ["hangar", "tower", "bunker", "control_tower", "building"],
                                                        "railway"         : ["station", "train_station", "building"],
                                                        "public_transport": ["train_station", "station"],
                                                        "amenity"         : ["train_station", "terminal"]
                    ],
                    healthcare                       : ["amenity"   : ["healthcare"],
                                                        "building"  : ["healthcare"],
                                                        "healthcare": ["! no"]
                    ],
                    education                        : ["amenity"  : ["education", "college", "kindergarten", "school", "university"],
                                                        "building" : ["education", "college", "kindergarten", "school", "university"],
                                                        "education": ["college", "kindergarten", "school", "university"]
                    ],
                    "entertainment, arts and culture": ["leisure": ["! no"]
                    ],
                    sustenance                       : ["amenity"   : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                        "building"  : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                        "restaurant": ["! no"],
                                                        "shop"      : ["restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub"],
                                                        "sustenance": ["! no"]
                    ],
                    office                           : [building: ["office"],
                                                        amenity : ["office"],
                                                        office  : ["! no"]
                    ],
                    building                         : [building: ["yes"]
                    ]
            ]
            def mappingRoadType = [
                    "cycleway"    : [
                            "highway"     : ["cycleway"],
                            "cycleway"    : ["track"],
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
                    "highspeed"    : ["highspeed": ["yes"]],
                    "rail"         : ["railway": ["rail", "light_rail", "narrow_gauge"]],
                    "service_track": ["service": ["yard", "siding", "spur", "crossover"]],
                    "disused"      : ["railway": ["disused"]],
                    "funicular"    : ["railway": ["funicular"]],
                    "subway"       : ["railway": ["subway"]],
                    "tram"         : ["railway": ["tram"]]
            ]

            def mappingVegetType = [
                    "tree"         : ["natural": ["tree"]],
                    "wood"         : ["landcover": ["trees"], "natural": ["wood"]],
                    "forest"       : ["landuse": ["forest"]],
                    "scrub"        : ["natural": ["scrub"], "landcover": ["scrub"], "landuse": ["scrub"]],
                    "grassland"    : ["landcover": ["grass", "grassland"], "natural": ["grass", "grassland"], "vegetation": ["grassland"], "landuse": ["grass", "grassland"]],
                    "heath"        : ["natural": ["heath"]],
                    "tree_row"     : ["natural": ["tree_row"], "landcover": ["tree_row"], "barrier": ["tree_row"]],
                    "hedge"        : ["barrier": ["hedge"], "natural": ["hedge", "hedge_bank"], "fence_type": ["hedge"], "hedge": ["hedge_bank"]],
                    "mangrove"     : ["wetland": ["mangrove"]],
                    "orchard"      : ["landuse": ["orchard"]],
                    "vineyard"     : ["landuse": ["vineyard"], "vineyard": ["! no"]],
                    "banana plants": ["trees": ["banana_plants"], "crop": ["banana"]],
                    "sugar cane"   : ["produce": ["sugar_cane"], "crop": ["sugar_cane"]]
            ]

            def prepareOSMData = prepareOSM()
            prepareOSMData([
                    hLevMin                  : 3,
                    hLevMax                  : 15,
                    hThresholdLev2           : 10,
                    datasource               : datasource,
                    osmTablesPrefix          : "EXT",
                    idZone                   : idZone,
                    expand                   : 500,
                    distBuffer               : 1000,
                    buildingTableColumnsNames:
                            ['height'          : 'height', 'building:height': 'b_height', 'roof:height': 'r_height', 'building:roof:height': 'b_r_height',
                             'building:levels' : 'b_lev', 'roof:levels': 'r_lev', 'building:roof:levels': 'b_r_lev', 'building': 'building',
                             'amenity'         : 'amenity', 'layer': 'zindex', 'aeroway': 'aeroway', 'historic': 'historic', 'leisure': 'leisure', 'monument': 'monument',
                             'place_of_worship': 'place_of_worship', 'military': 'military', 'railway': 'railway', 'public_transport': 'public_transport',
                             'barrier'         : 'barrier', 'government': 'government', 'historic:building': 'historic_building', 'grandstand': 'grandstand',
                             'house'           : 'house', 'shop': 'shop', 'industrial': 'industrial', 'man_made': 'man_made', 'residential': 'residential',
                             'apartments'      : 'apartments', 'ruins': 'ruins', 'agricultural': 'agricultural', 'barn': 'barn', 'healthcare': 'healthcare',
                             'education'       : 'education', 'restaurant': 'restaurant', 'sustenance': 'sustenance', 'office': 'office'],
                    buildingTagKeys          : ['building'],
                    buildingTagValues        : null,
                    tablesPrefix             : "RAW_",
                    buildingFilter           : "ZONE_BUFFER",

                    roadTableColumnsNames    : ['width'       : 'width', 'highway': 'highway', 'surface': 'surface', 'sidewalk': 'sidewalk',
                                                'lane'        : 'lane', 'layer': 'zindex', 'maxspeed': 'maxspeed', 'oneway': 'oneway',
                                                'h_ref'       : 'h_ref', 'route': 'route', 'cycleway': 'cycleway',
                                                'bicycle_road': 'bicycle_road', 'cyclestreet': 'cyclestreet', 'junction': 'junction'],
                    roadTagKeys              : ['highway', 'cycleway', 'bicycle_road', 'cyclestreet', 'route', 'junction'],
                    roadTagValues            : null,
                    roadFilter               : "ZONE_BUFFER",

                    railTableColumnsNames    : ['highspeed': 'highspeed', 'railway': 'railway', 'service': 'service',
                                                'tunnel'   : 'tunnel', 'layer': 'layer', 'bridge': 'bridge'],
                    railTagKeys              : ['railway'],
                    railTagValues            : null,
                    railFilter               : "ZONE",

                    vegetTableColumnsNames   : ['natural'   : 'natural', 'landuse': 'landuse', 'landcover': 'landcover',
                                                'vegetation': 'vegetation', 'barrier': 'barrier', 'fence_type': 'fence_type',
                                                'hedge'     : 'hedge', 'wetland': 'wetland', 'vineyard': 'vineyard', 'trees': 'trees',
                                                'crop'      : 'crop', 'produce': 'produce'],
                    vegetTagKeys             : ['natural', 'landuse', 'landcover'],
                    vegetTagValues           : ['fell', 'heath', 'scrub', 'tree', 'tree_row', 'trees', 'wood', 'farmland',
                                                'forest', 'grass', 'grassland', 'greenfield', 'meadow', 'orchard', 'plant_nursery',
                                                'vineyard', 'hedge', 'hedge_bank', 'mangrove', 'banana_plants', 'banana', 'sugar_cane'],
                    vegetFilter              : "ZONE_EXTENDED",

                    hydroTableColumnsNames   : ['natural': 'natural', 'water': 'water', 'waterway': 'waterway'],
                    hydroTags                : ['natural': ['water', 'waterway', 'bay'], 'water': [], 'waterway': []],
                    hydroFilter              : "ZONE_EXTENDED",

                    mappingForTypeAndUse     : mappingTypeAndUse,

                    mappingForRoadType       : mappingRoadType,
                    mappingForSurface        : mappingSurface,

                    mappingForRailType       : mappingRailType,

                    mappingForVegetType      : mappingVegetType,
            ])
            [outputBuilding: prepareOSMData.getResults().outputBuilding,
             outputRoad    : prepareOSMData.getResults().outputRoad,
             outputRail    : prepareOSMData.getResults().outputRail,
             outputHydro   : prepareOSMData.getResults().outputHydro,
             outputVeget   : prepareOSMData.getResults().outputVeget,
             outputZone    : prepareOSMData.getResults().outputZone,
             outputStats   : prepareOSMData.getResults().outputStats]

        }
    })
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
def prepareOSM() {
    return create({
        title "Extract and transform OSM data to Geoclimate model"
        inputs datasource : JdbcDataSource,
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
             mappingForVegetType : Map
        outputs outputBuilding : String, outputRoad:String, outputRail : String, outputHydro:String, outputVeget:String, outputZone:String,outputStats : String[]
        run { datasource, osmTablesPrefix, idZone, expand, distBuffer, hLevMin, hLevMax, hThresholdLev2, buildingTableColumnsNames,
              buildingTagKeys, buildingTagValues,
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

            if (datasource == null) {
                error "Cannot create the database to store the osm data"
                return
            }

            def loadInitialData = OSMGISLayers.loadInitialData()

            if (!loadInitialData([
                    datasource     : datasource,
                    osmTablesPrefix: osmTablesPrefix,
                    idZone         : idZone,
                    expand         : expand,
                    distBuffer     : distBuffer])) {
                info "Cannot downloaded for OSM data for the zone id : ${idZone}."
                return
            }

            info "The OSM data has been downloaded for the zone id : ${idZone}."


            //Init model
            def initParametersAbstract = AbstractTablesInitialization.initParametersAbstract()
            if (!initParametersAbstract(datasource: datasource)) {
                info "Cannot initialize the geoclimate data model."
                return
            }

            info "The geoclimate data model has been initialized."

            info "Transform OSM data to GIS tables."

            def prepareBuildings = OSMGISLayers.prepareBuildings()

            if (!prepareBuildings([datasource               : datasource, osmTablesPrefix: osmTablesPrefix,
                                           buildingTableColumnsNames: buildingTableColumnsNames,
                                           buildingTagKeys          : buildingTagKeys,
                                           buildingTagValues        : buildingTagValues,
                                           buildingTagValues        : buildingTagValues,
                                           tablesPrefix             : tablesPrefix,
                                           buildingFilter           : buildingFilter,
            ])) {
                info "Cannot prepare the building table."
                return
            }
            def prepareRoads = OSMGISLayers.prepareRoads()
            if (!prepareRoads([datasource           : datasource,
                                       osmTablesPrefix      : osmTablesPrefix,
                                       roadTableColumnsNames: roadTableColumnsNames,
                                       roadTagKeys          : roadTagKeys,
                                       roadTagValues        : roadTagValues,
                                       tablesPrefix         : tablesPrefix,
                                       roadFilter           : roadFilter])) {
                info "Cannot prepare the road table."
                return
            }

            def prepareRails = OSMGISLayers.prepareRails()
            if (!prepareRails([datasource           : datasource,
                                       osmTablesPrefix      : osmTablesPrefix,
                                       railTableColumnsNames: railTableColumnsNames,
                                       railTagKeys          : railTagKeys,
                                       railTagValues        : railTagValues,
                                       tablesPrefix         : tablesPrefix,
                                       railFilter           : railFilter])) {
                info "Cannot prepare the rail table."
                return
            }

            def prepareVeget = OSMGISLayers.prepareVeget()
            if (!prepareVeget([datasource            : datasource,
                                       osmTablesPrefix       : osmTablesPrefix,
                                       vegetTableColumnsNames: vegetTableColumnsNames,
                                       vegetTagKeys          : vegetTagKeys,
                                       vegetTagValues        : vegetTagValues,
                                       tablesPrefix          : tablesPrefix,
                                       vegetFilter           : vegetFilter])) {
                info"Cannot prepare the vegetation table."
                return
            }

            def prepareHydro = OSMGISLayers.prepareHydro()
            if (!prepareHydro([datasource            : datasource,
                                       osmTablesPrefix       : osmTablesPrefix,
                                       hydroTableColumnsNames: hydroTableColumnsNames,
                                       hydroTags             : hydroTags,
                                       tablesPrefix          : tablesPrefix,
                                       hydroFilter           : hydroFilter])) {
                info"Cannot prepare the hydrographic table."
                return
            }

            def transformBuildings = FormattingForAbstractModel.transformBuildings()
            if (!transformBuildings([datasource          : datasource,
                                             inputTableName      : prepareBuildings.getResults().buildingTableName,
                                             mappingForTypeAndUse: mappingForTypeAndUse])) {
                info "Cannot transform the building table to geoclimate model."
                return
            }
            def inputBuilding = transformBuildings.getResults().outputTableName

            def transformRoads = FormattingForAbstractModel.transformRoads()
            if (!transformRoads([datasource        : datasource,
                                         inputTableName    : prepareRoads.getResults().roadTableName,
                                         mappingForRoadType: mappingForRoadType,
                                         mappingForSurface : mappingForSurface])) {
                info "Cannot transform the road table to geoclimate model."
                return
            }
            def inputRoads = transformRoads.getResults().outputTableName


            def transformRails = FormattingForAbstractModel.transformRails()
            if (!transformRails([datasource        : datasource,
                                         inputTableName    : prepareRails.getResults().railTableName,
                                         mappingForRailType: mappingForRailType])) {
                info "Cannot transform the rail table to geoclimate model."
                return
            }
            def inputRail = transformRails.getResults().outputTableName

            def transformVeget = FormattingForAbstractModel.transformVeget()
            if (!transformVeget([datasource         : datasource,
                                         inputTableName     : prepareVeget.getResults().vegetTableName,
                                         mappingForVegetType: mappingForVegetType])) {
                info "Cannot transform the vegetation table to geoclimate model."
                return
            }
            def inputVeget = transformVeget.getResults().outputTableName

            def transformHydro = FormattingForAbstractModel.transformHydro()
            if (!transformHydro([datasource    : datasource,
                                         inputTableName: prepareHydro.getResults().hydroTableName])) {
                info "Cannot transform the hydrographic table to geoclimate model."
                return
            }
            def inputHydro = transformHydro.getResults().outputTableName

            info "All OSM data have been tranformed to GIS tables."

            info "Formating GIS tables to Geoclimate model..."

            def initResults = initParametersAbstract.getResults()

            def inputZone = loadInitialData.getResults().outputZone
            def inputZoneNeighbors = loadInitialData.getResults().outputZoneNeighbors

            def inputDataFormatting = InputDataFormatting.inputDataFormatting()

            if (!inputDataFormatting([datasource             : datasource,
                                              inputBuilding          : inputBuilding, inputRoad: inputRoads, inputRail: inputRail,
                                              inputHydro             : inputHydro, inputVeget: inputVeget,
                                              inputZone              : inputZone, inputZoneNeighbors: inputZoneNeighbors,
                                              hLevMin                : hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2, idZone: idZone,
                                              buildingAbstractUseType: initResults.outputBuildingAbstractUseType, buildingAbstractParameters: initResults.outputBuildingAbstractParameters,
                                              roadAbstractType       : initResults.outputRoadAbstractType, roadAbstractParameters: initResults.outputRoadAbstractParameters,
                                              railAbstractType       : initResults.outputRailAbstractType,
                                              vegetAbstractType      : initResults.outputVegetAbstractType,
                                              vegetAbstractParameters: initResults.outputVegetAbstractParameters])) {
                info "Cannot format the tables to geoclimate model."
                return
            }

            info "End of the OSM extract transform process."

            def finalBuildings = inputDataFormatting.getResults().outputBuilding
            def finalRoads = inputDataFormatting.getResults().outputRoad
            def finalRails = inputDataFormatting.getResults().outputRail
            def finalHydro = inputDataFormatting.getResults().outputHydro
            def finalVeget = inputDataFormatting.getResults().outputVeget
            def finalZone = inputDataFormatting.getResults().outputZone

            def finalOutputBuildingStatZone = inputDataFormatting.getResults().outputBuildingStatZone
            def finalOutputBuildingStatZoneBuff = inputDataFormatting.getResults().outputBuildingStatZoneBuff
            def finalOutputRoadStatZone = inputDataFormatting.getResults().outputRoadStatZone
            def finalOutputRoadStatZoneBuff = inputDataFormatting.getResults().outputRoadStatZoneBuff
            def finalOutputRailStatZone = inputDataFormatting.getResults().outputRailStatZone
            def finalOutputHydroStatZone = inputDataFormatting.getResults().outputHydroStatZone
            def finalOutputHydroStatZoneExt = inputDataFormatting.getResults().outputHydroStatZoneExt
            def finalOutputVegetStatZone = inputDataFormatting.getResults().outputVegetStatZone
            def finalOutputVegetStatZoneExt = inputDataFormatting.getResults().outputVegetStatZoneExt

            [outputBuilding: finalBuildings, outputRoad: finalRoads,
             outputRail    : finalRails, outputHydro: finalHydro, outputVeget: finalVeget, outputZone: finalZone,
             outputStats   : [finalOutputBuildingStatZone, finalOutputBuildingStatZoneBuff, finalOutputRoadStatZone,
                              finalOutputRoadStatZoneBuff, finalOutputRailStatZone, finalOutputHydroStatZone, finalOutputHydroStatZoneExt,
                              finalOutputVegetStatZone, finalOutputVegetStatZoneExt]]

        }
    })
}

