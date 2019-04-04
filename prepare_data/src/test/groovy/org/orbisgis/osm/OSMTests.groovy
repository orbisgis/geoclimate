package org.orbisgis.osm

import org.orbisgis.PrepareData
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class OSMTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMTests.class)
    /**
     * A basic test just for demo... must be removed
     */
    @Test
    void prepareBuildingsTest() {
        def h2GIS = H2GIS.open('./target/h2db')
        h2GIS.load(new File(this.class.getResource("zoneExtended.osm").toURI()).getAbsolutePath(),"ext",false)
        h2GIS.execute "drop table if exists RAW_INPUT_BUILDING;"
        assertNotNull(h2GIS.getTable("EXT_NODE"))
        logger.info('Load OSM tables OK')
        h2GIS.execute OSMGISLayers.createIndexesOnOSMTables("ext")
        logger.info('Index OSM tables OK')
        h2GIS.execute OSMGISLayers.zoneSQLScript('ext',"35236",1000, 500)
        def process = PrepareData.OSMGISLayers.prepareBuildings()
        process.execute([
                datasource   : h2GIS,
                osmTablesPrefix : "ext",
                outputColumnNames: ['height':'height','building:height':'b_height','roof:height':'r_height','building:roof:height':'b_r_height',
                               'building:levels':'b_lev','roof:levels':'r_lev','building:roof:levels':'b_r_lev','building':'building',
                               'amenity':'amenity','layer':'zindex','aeroway':'aeroway','historic':'historic','leisure':'leisure','monument':'monument',
                               'place_of_worship':'place_of_worship','military':'military','railway':'railway','public_transport':'public_transport',
                               'barrier':'barrier','government':'government','historic:building':'historic_building','grandstand':'grandstand',
                               'house':'house','shop':'shop','industrial':'industrial','man_made':'man_made', 'residential':'residential',
                               'apartments':'apartments','ruins':'ruins','agricultural':'agricultural','barn':'barn', 'healthcare':'healthcare',
                               'education':'education','restaurant':'restaurant','sustenance':'sustenance','office':'office'],
                tagKeys: ['building'],
                tagValues: null,
                buildingTablePrefix: "RAW_",
                filteringZoneTableName: "ZONE_BUFFER"])
        assertNotNull(h2GIS.getTable("RAW_INPUT_BUILDING"))
        assertTrue(h2GIS.getTable("RAW_INPUT_BUILDING").getRowCount()==15083)
        assertTrue(h2GIS.getTable("RAW_INPUT_BUILDING").getColumnCount()==38)
    }

    @Test
    void prepareRoadsTest() {
        def h2GIS = H2GIS.open('./target/h2db')
        h2GIS.load(new File(this.class.getResource("zoneExtended.osm").toURI()).getAbsolutePath(),"ext",false)
        h2GIS.execute "drop table if exists RAW_INPUT_ROAD;"
        assertNotNull(h2GIS.getTable("EXT_NODE"))
        logger.info('Load OSM tables OK')
        h2GIS.execute OSMGISLayers.createIndexesOnOSMTables("ext")
        logger.info('Index OSM tables OK')
        h2GIS.execute OSMGISLayers.zoneSQLScript('ext',"35236",1000, 500)
        def process = PrepareData.OSMGISLayers.prepareRoads()
        process.execute([
                datasource   : h2GIS,
                osmTablesPrefix : "ext",
                outputColumnNames: ['width':'width','highway':'highway', 'surface':'surface', 'sidewalk':'sidewalk',
                                   'lane':'lane','layer':'zindex','maxspeed':'maxspeed','oneway':'oneway',
                                   'h_ref':'h_ref','route':'route','cycleway':'cycleway',
                                   'biclycle_road':'biclycle_road','cyclestreet':'cyclestreet','junction':'junction'],
                tagKeys: ['highway','cycleway','biclycle_road','cyclestreet','route','junction'],
                tagValues: null,
                roadTablePrefix: "RAW_",
                filteringZoneTableName: "ZONE_BUFFER"])
        assertNotNull(h2GIS.getTable("RAW_INPUT_ROAD"))
        assertTrue(h2GIS.getTable("RAW_INPUT_ROAD").getRowCount()==6545)
        assertTrue(h2GIS.getTable("RAW_INPUT_ROAD").getColumnCount()==16)
    }

    @Test
    void prepareRailsTest() {
        def h2GIS = H2GIS.open('./target/h2db')
        h2GIS.load(new File(this.class.getResource("zoneExtended.osm").toURI()).getAbsolutePath(),"ext",false)
        h2GIS.execute "drop table if exists RAW_INPUT_RAIL;"
        assertNotNull(h2GIS.getTable("EXT_NODE"))
        logger.info('Load OSM tables OK')
        h2GIS.execute OSMGISLayers.createIndexesOnOSMTables("ext")
        logger.info('Index OSM tables OK')
        h2GIS.execute OSMGISLayers.zoneSQLScript('ext',"35236",1000, 500)
        def process = PrepareData.OSMGISLayers.prepareRails()
        process.execute([
                datasource   : h2GIS,
                osmTablesPrefix : "ext",
                outputColumnNames: ['highspeed':'highspeed','railway':'railway','service':'service',
                                   'tunnel':'tunnel','layer':'layer','bridge':'bridge'],
                tagKeys: ['railway'],
                tagValues: null,
                railTablePrefix: "RAW_",
                filteringZoneTableName: "ZONE_BUFFER"])
        assertTrue(h2GIS.getTable("RAW_INPUT_RAIL").getColumnNames().contains("highspeed"))
        assertTrue(h2GIS.getTable("RAW_INPUT_RAIL").getRowCount()==380)
        assertTrue(h2GIS.getTable("RAW_INPUT_RAIL").getColumnCount()==8)
    }

    @Test
    void prepareVegetTest() {
        def h2GIS = H2GIS.open('./target/h2db')
        h2GIS.load(new File(this.class.getResource("zoneExtended.osm").toURI()).getAbsolutePath(),"ext",false)
        h2GIS.execute "drop table if exists RAW_INPUT_VEGET;"
        assertNotNull(h2GIS.getTable("EXT_NODE"))
        logger.info('Load OSM tables OK')
        h2GIS.execute OSMGISLayers.createIndexesOnOSMTables("ext")
        logger.info('Index OSM tables OK')
        h2GIS.execute OSMGISLayers.zoneSQLScript('ext',"35236",1000, 500)
        def process = PrepareData.OSMGISLayers.prepareVeget()
        process.execute([
                datasource   : h2GIS,
                osmTablesPrefix : "ext",
                outputColumnNames: ['natural':'natural','landuse':'landuse','landcover':'landcover',
                                    'vegetation':'vegetation','barrier':'barrier','fence_type':'fence_type',
                                    'hedge':'hedge','wetland':'wetland','vineyard':'vineyard',
                                    'trees':'trees','crop':'crop','produce':'produce'],
                tagKeys: ['natural', 'landuse','landcover'],
                tagValues: ['fell', 'heath', 'scrub', 'tree', 'tree_row', 'trees', 'wood','farmland',
                            'forest','grass','grassland','greenfield','meadow','orchard','plant_nursery',
                            'vineyard','hedge','hedge_bank','mangrove','banana_plants','banana','sugar_cane'],
                vegetTablePrefix: "RAW_",
                filteringZoneTableName: "ZONE_EXTENDED"])
        assertTrue h2GIS.getTable("RAW_INPUT_VEGET").getColumnNames().contains("produce")
        assertTrue h2GIS.getTable("RAW_INPUT_VEGET").getRowCount()==862
        assertTrue h2GIS.getTable("RAW_INPUT_VEGET").getColumnCount()==14
    }
}