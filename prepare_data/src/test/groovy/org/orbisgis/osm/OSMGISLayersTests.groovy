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

    //@Test disable due to potential API blocking
    void extractAndCreateGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb')

        IProcess process = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
        process.execute([
                datasource : h2GIS,
                placeName: "Cliscouët, Vannes"])
        process.getResults().each {it ->
            println it
        }
    }

    @Test
    void createGISLayersTest() {
        def h2GIS = H2GIS.open('./target/osmdb')
        IProcess process = PrepareData.OSMGISLayers.createGISLayers()
        process.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("saint_jean.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertNull process.results.railTableName
        assertNull process.results.waterTableName
        assertEquals 448, h2GIS.getTable(process.results.buildingTableName).rowCount
        h2GIS.getTable(process.results.buildingTableName).save("./target/osm_building.shp")

        h2GIS.getTable(process.results.vegetationTableName).save("./target/osm_vegetation.shp")
        assertEquals 3, h2GIS.getTable(process.results.vegetationTableName).rowCount
        h2GIS.getTable(process.results.roadTableName).save("./target/osm_road.shp")
        assertEquals 22, h2GIS.getTable(process.results.roadTableName).rowCount
    }
}