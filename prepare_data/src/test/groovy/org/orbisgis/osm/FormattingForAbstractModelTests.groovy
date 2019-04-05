package org.orbisgis.osm

import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.*

class FormattingForAbstractModelTests {

    @Test
    void transformBuildingsTest() {
        def h2GIS = H2GIS.open('D:\\Users\\le_sauxe\\AppData\\Local\\Temp\\osm_gis_final\\osmGisDb')
        h2GIS.execute "drop table if exists INPUT_BUILDING;"
        assertNotNull(h2GIS.getTable("RAW_INPUT_BUILDING"))
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

        def process = PrepareData.FormattingForAbstractModel.transformBuildings()
        process.execute([
                datasource          : h2GIS,
                inputTableName      : "RAW_INPUT_BUILDING",
                mappingForTypeAndUse: mappingTypeAndUse])
        assertNotNull(h2GIS.getTable("INPUT_BUILDING"))
        assertEquals(h2GIS.getTable("RAW_INPUT_BUILDING").getRowCount(), h2GIS.getTable("INPUT_BUILDING").getRowCount())
        assertTrue(h2GIS.getTable("INPUT_BUILDING").getColumnNames().contains("TYPE"))
        assertTrue(h2GIS.getTable("INPUT_BUILDING").getColumnNames().contains("MAIN_USE"))
        assertTrue(h2GIS.getTable("INPUT_BUILDING").getColumnNames().contains("HEIGHT_WALL"))
    }

    @Test
    void transformRoadsTest() {
        def h2GIS = H2GIS.open('D:\\Users\\le_sauxe\\AppData\\Local\\Temp\\osm_gis_final\\osmGisDb')
        h2GIS.execute "drop table if exists INPUT_ROAD;"
        assertNotNull(h2GIS.getTable("RAW_INPUT_ROAD"))
        //Define the mapping between the values in OSM and those used in the abstract model
        def mappingType = [
                "cycleway":[
                        "highway":["cycleway"],
                        "cycleway":["track"],
                        "biclycle_road":["yes"]
                ],
                "ferry":[
                        "route":["ferry"]
                ],
                "footway":[
                        "highway":["footway","pedestrian"]
                ],
                "highway":[
                        "highway":["service","road","raceway","escape"],
                        "cyclestreet":["yes"]
                ],
                "highway_link":[
                        "highway":["motorway_link","motorway_junction","trunk_link","primary_link","secondary_link","tertiary_link","junction"]
                ],
                "motorway":[
                        "highway":["motorway"]
                ],
                "path":[
                        "highway":["path","bridleway"]
                ],
                "primary":[
                        "highway":["primary"]
                ],
                "residential":[
                        "highway":["residential","living_street"]
                ],
                "roundabout":[
                        "junction":["roundabout","circular"]
                ],
                "secondary":[
                        "highway":["secondary"]
                ],
                "steps":[
                        "highway":["steps"]
                ],
                "tertiary":[
                        "highway":["tertiary"]
                ],
                "track":[
                        "highway":["track"]
                ],
                "trunk":[
                        "highway":["trunk"]
                ],
                "unclassified":[
                        "highway":["unclassified"]
                ]
        ]

        def mappingSurface = [
                "unpaved":["surface":["unpaved","grass_paver","artificial_turf"]],
                "paved":["surface":["paved","asphalt"]],
                "ground":["surface":["ground","dirt","earth","clay"]],
                "gravel":["surface":["gravel","fine_gravel","gravel_turf"]],
                "concrete":["surface":["concrete","concrete:lanes","concrete:plates","cement"]],
                "grass":["surface":["grass"]],
                "compacted":["surface":["compacted"]],
                "sand":["surface":["sand"]],
                "cobblestone":["surface":["cobblestone","paving_stones","sett","unhewn_cobblestone"]],
                "wood":["surface":["wood","woodchips"]],
                "pebblestone":["surface":["pebblestone"]],
                "mud":["surface":["mud"]],
                "metal":["surface":["metal"]],
                "water":["surface":["water"]]
        ]

        def process = PrepareData.FormattingForAbstractModel.transformRoads()
        process.execute([datasource          : h2GIS,
                         inputTableName      : "RAW_INPUT_ROAD",
                         mappingForType: mappingType,
                         mappingForSurface: mappingSurface])
        assertNotNull(h2GIS.getTable("INPUT_ROAD"))
        assertEquals(h2GIS.getTable("RAW_INPUT_ROAD").getRowCount(), h2GIS.getTable("INPUT_ROAD").getRowCount())
        assertTrue(h2GIS.getTable("INPUT_ROAD").getColumnNames().contains("TYPE"))
        assertTrue(h2GIS.getTable("INPUT_ROAD").getColumnNames().contains("SURFACE"))
        assertTrue(h2GIS.getTable("INPUT_ROAD").getColumnNames().contains("ZINDEX"))
    }
}
