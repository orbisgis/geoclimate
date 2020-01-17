package org.orbisgis.orbisprocess.geoclimate.preparedata.osm

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals

class OSMGISLayersTests {

    private static final Logger logger = LoggerFactory.getLogger(OSMGISLayersTests.class)

    @Disabled
    @Test //enable it to test data extraction from the overpass api
    void extractAndCreateGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess process = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
        process.execute([
                datasource : h2GIS,
                zoneToExtract: "Cliscouet, Vannes"])
        process.getResults().each {it ->
            if(it.value!=null){
                h2GIS.getTable(it.value).save("./target/${it.value}.shp")
            }
        }
    }

    @Test
    void createGISLayersTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess process = PrepareData.OSMGISLayers.createGISLayers()
        def osmfile = new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath()
        process.execute([
                datasource : h2GIS,
                osmFilePath: osmfile,
                epsg :2154])
        //h2GIS.getTable(process.results.buildingTableName).save("./target/osm_building.shp")
        assertEquals 1038, h2GIS.getTable(process.results.buildingTableName).rowCount

        //h2GIS.getTable(process.results.vegetationTableName).save("./target/osm_vegetation.shp")
        assertEquals 135, h2GIS.getTable(process.results.vegetationTableName).rowCount

        //h2GIS.getTable(process.results.roadTableName).save("./target/osm_road.shp")
        assertEquals 198, h2GIS.getTable(process.results.roadTableName).rowCount

        //h2GIS.getTable(process.results.railTableName).save("./target/osm_rails.shp")
        assertEquals 44, h2GIS.getTable(process.results.railTableName).rowCount

        //h2GIS.getTable(process.results.hydroTableName).save("./target/osm_hydro.shp")
        assertEquals 10, h2GIS.getTable(process.results.hydroTableName).rowCount

        //h2GIS.getTable(process.results.imperviousTableName).save("./target/osm_hydro.shp")
        assertEquals 43, h2GIS.getTable(process.results.imperviousTableName).rowCount
    }
}
