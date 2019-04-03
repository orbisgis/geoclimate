package org.orbisgis.osm

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class OSMTests {

    /**
     * A basic test just for demo... must be removed
     */
    @Test
    void prepareBuildingsTest() {
        def h2GIS = H2GIS.open('./target/buildingdb')
        h2GIS.load(new File(this.class.getResource("zoneExtended.osm").toURI()).getAbsolutePath(),"ext",true)
        assertNotNull(h2GIS.getTable("EXT_NODE"))
        println('Load OSM tables OK')
        h2GIS.execute OSMGISLayers.createIndexesOnOSMTables("ext")
        println('Index OSM tables OK')
        h2GIS.execute OSMGISLayers.zoneSQLScript('ext',"56243",1000, 500)
        def process = PrepareData.OSMGISLayers.prepareBuildings()
        process.execute([
                datasource   : h2GIS,
                prefix : "ext",
                inputOptions: ['height':'height','building:height':'b_height','roof:height':'r_height','building:roof:height':'b_r_height',
                               'building:levels':'b_lev','roof:levels':'r_lev','building:roof:levels':'b_r_lev','building':'building',
                               'amenity':'amenity','layer':'zindex','aeroway':'aeroway','historic':'historic','leisure':'leisure','monument':'monument',
                               'place_of_worship':'place_of_worship','military':'military','railway':'railway','public_transport':'public_transport',
                               'barrier':'barrier','government':'government','historic:building':'historic_building','grandstand':'grandstand',
                               'house':'house','shop':'shop','industrial':'industrial','man_made':'man_made', 'residential':'residential',
                               'apartments':'apartments','ruins':'ruins','agricultural':'agricultural','barn':'barn', 'healthcare':'healthcare',
                               'education':'education','restaurant':'restaurant','sustenance':'sustenance','office':'office'],
                inputTagKeys: ['building'],
                inputTagValues: null,
                buildingTableName: "RAW_INPUT_BUILDING",
                zoneBufferTableName: "ZONE_BUFFER"])
        assertNotNull(h2GIS.getTable("RAW_INPUT_BUILDING"))
    }

}