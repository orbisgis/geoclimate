
package org.orbisgis.osm


import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.*

class FormattingForAbstractModelTests {

    @Test
   void transformBuildingsTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("saint_jean.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 661, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        h2GIS.getTable(extractData.results.buildingTableName).save("./target/osm_building.shp")

        IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName])

        assertEquals 661, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp")
    }

    @Test
    void transformRoadsTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("saint_jean.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 56, h2GIS.getTable(extractData.results.roadTableName).rowCount
        h2GIS.getTable(extractData.results.roadTableName).save("./target/osm_road.shp")

        IProcess format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.roadTableName])

        assertEquals 22, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_formated.shp")
    }
}


//
//    @Test
//    void transformRoadsTest() {
//        new OSMGISLayersTests().prepareRoadsTest()
//        def h2GIS = H2GIS.open('./target/osmdb')
//        assertNotNull(h2GIS.getTable("RAW_INPUT_ROAD"))
//
//        logger.info('Process starts')
//        def process = PrepareData.FormattingForAbstractModel.transformRoads()
//        process.execute([datasource       : h2GIS,
//                         inputTableName   : "RAW_INPUT_ROAD",
//                         mappingForRoadType   : mappingType,
//                         mappingForSurface: mappingSurface])
//        assertNotNull(h2GIS.getTable("INPUT_ROAD"))
//        assertEquals(h2GIS.getTable("RAW_INPUT_ROAD").getRowCount(), h2GIS.getTable("INPUT_ROAD").getRowCount())
//        assertTrue(h2GIS.getTable("INPUT_ROAD").getColumnNames().contains("TYPE"))
//        assertTrue(h2GIS.getTable("INPUT_ROAD").getColumnNames().contains("SURFACE"))
//        assertTrue(h2GIS.getTable("INPUT_ROAD").getColumnNames().contains("ZINDEX"))
//
//        h2GIS.save("INPUT_ROAD", '/home/ebocher/Autres/codes/geoclimate/processing_chain/target/road.geojson')
//    }
//
//    @Test
//    void transformRailsTest() {
//        new OSMGISLayersTests().prepareRailsTest()
//        def h2GIS = H2GIS.open('./target/osmdb')
//        assertNotNull(h2GIS.getTable("RAW_INPUT_RAIL"))
//        logger.info(h2GIS.getTable("RAW_INPUT_RAIL").getRowCount().toString())
//        //Define the mapping between the values in OSM and those used in the abstract model
//        def mappingType = [
//                "highspeed":["highspeed":["yes"]],
//                "rail":["railway":["rail","light_rail","narrow_gauge"]],
//                "service_track":["service":["yard","siding","spur","crossover"]],
//                "disused":["railway":["disused"]],
//                "funicular":["railway":["funicular"]],
//                "subway":["railway":["subway"]],
//                "tram":["railway":["tram"]]
//        ]
//        logger.info('Process starts')
//        def process = PrepareData.FormattingForAbstractModel.transformRails()
//        process.execute([datasource       : h2GIS,
//                         inputTableName   : "RAW_INPUT_RAIL",
//                         mappingForRailType   : mappingType])
//        assertNotNull(h2GIS.getTable("INPUT_RAIL"))
//        assertEquals(h2GIS.getTable("RAW_INPUT_RAIL").getRowCount(), h2GIS.getTable("INPUT_RAIL").getRowCount())
//        assertTrue(h2GIS.getTable("INPUT_RAIL").getColumnNames().contains("TYPE"))
//        assertTrue(h2GIS.getTable("INPUT_RAIL").getColumnNames().contains("ZINDEX"))
//    }
//
//    @Test
//    void transformVegetTest() {
//        new OSMGISLayersTests().prepareVegetTest()
//        def h2GIS = H2GIS.open('./target/osmdb')
//        assertNotNull(h2GIS.getTable("RAW_INPUT_VEGET"))
//        //Define the mapping between the values in OSM and those used in the abstract model
//        def mappingType = [
//                "tree":["natural":["tree"]],
//                "wood":["landcover":["trees"],"natural":["wood"]],
//                "forest":["landuse":["forest"]],
//                "scrub":["natural":["scrub"],"landcover":["scrub"],"landuse":["scrub"]],
//                "grassland":["landcover":["grass","grassland"],"natural":["grass","grassland"],"vegetation":["grassland"],"landuse":["grass","grassland"]],
//                "heath":["natural":["heath"]],
//                "tree_row":["natural":["tree_row"],"landcover":["tree_row"],"barrier":["tree_row"]],
//                "hedge":["barrier":["hedge"],"natural":["hedge","hedge_bank"],"fence_type":["hedge"],"hedge":["hedge_bank"]],
//                "mangrove":["wetland":["mangrove"]],
//                "orchard":["landuse":["orchard"]],
//                "vineyard":["landuse":["vineyard"],"vineyard":["! no"]],
//                "banana plants":["trees":["banana_plants"],"crop":["banana"]],
//                "sugar cane":["produce":["sugar_cane"],"crop":["sugar_cane"]]
//        ]
//        logger.info('Process starts')
//        def process = PrepareData.FormattingForAbstractModel.transformVeget()
//        process.execute([datasource       : h2GIS,
//                         inputTableName   : "RAW_INPUT_VEGET",
//                         mappingForVegetType   : mappingType])
//        assertNotNull(h2GIS.getTable("INPUT_VEGET"))
//        assertEquals(h2GIS.getTable("RAW_INPUT_VEGET").getRowCount(), h2GIS.getTable("INPUT_VEGET").getRowCount())
//        assertTrue(h2GIS.getTable("INPUT_VEGET").getColumnNames().contains("TYPE"))
//
//    }
//
//    @Test
//    void transformHydroTest() {
//        new OSMGISLayersTests().prepareHydroTest()
//        def h2GIS = H2GIS.open('./target/osmdb')
//        assertNotNull(h2GIS.getTable("RAW_INPUT_HYDRO"))
//        logger.info('Process starts')
//        def process = PrepareData.FormattingForAbstractModel.transformHydro()
//        process.execute([datasource       : h2GIS,
//                         inputTableName   : "RAW_INPUT_HYDRO"])
//        assertNotNull(h2GIS.getTable("INPUT_HYDRO"))
//        assertEquals(h2GIS.getTable("RAW_INPUT_HYDRO").getRowCount(), h2GIS.getTable("INPUT_HYDRO").getRowCount())
//        assertEquals(2, h2GIS.getTable("INPUT_HYDRO").getColumnCount())
//    }
//
//}