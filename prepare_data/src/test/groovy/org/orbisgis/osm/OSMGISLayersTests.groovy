package org.orbisgis.osm

import org.junit.jupiter.api.Disabled
import org.orbisgis.PrepareData
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class OSMGISLayersTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMGISLayersTests.class)

    //@Test
    void extractAndCreateGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb')

        IProcess process = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
        process.execute([
                datasource : h2GIS,
                placeName: "Cliscouët, Vannes"])
        process.getResults().each {it ->
            println it
        }
        //assertNotNull(h2GIS.getTable("RAW_INPUT_BUILDING"))
        //assertTrue(h2GIS.getTable("RAW_INPUT_BUILDING").getColumnNames().contains("HEIGHT"))
        //assertTrue(h2GIS.getTable("RAW_INPUT_BUILDING").getColumnNames().contains("OFFICE"))
    }

    @Test
    void createGISLayersTest() {
        def h2GIS = H2GIS.open('./target/osmdb')

        IProcess process = PrepareData.OSMGISLayers.createGISLayers()
        process.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("saint_jean.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertnul process.results.railTableName
        assertnul process.results.waterTableName
        assertEquals 3, h2GIS.getTable(process.results.buildingTableName).rowCount
        assertEquals 3, h2GIS.getTable(process.results.vegetationTableName).rowCount
        assertEquals 3, h2GIS.getTable(process.results.roadTableName).rowCount
    }
}