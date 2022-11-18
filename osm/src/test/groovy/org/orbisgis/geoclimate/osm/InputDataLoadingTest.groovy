package org.orbisgis.geoclimate.osm

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.data.H2GIS
import org.orbisgis.process.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals

class InputDataLoadingTest {

    private static final Logger logger = LoggerFactory.getLogger(InputDataLoadingTest.class)

    static  H2GIS h2GIS

    @BeforeAll
    static  void loadDb(){
        h2GIS = H2GIS.open('./target/osm_gislayers_test;AUTO_SERVER=TRUE')
    }

    @Disabled //enable it to test data extraction from the overpass api
    @Test
    void extractAndCreateGISLayers() {
        IProcess process = OSM.InputDataLoading.extractAndCreateGISLayers()
        process.execute([
                datasource : h2GIS,
                zoneToExtract: "Île de la Nouvelle-Amsterdam"])
        process.getResults().each {it ->
            if(it.value!=null){
                h2GIS.getTable(it.value).save("./target/${it.value}.shp", true)
            }
        }
    }

    @Test
    void createGISLayersTest() {
        IProcess process = OSM.InputDataLoading.createGISLayers()
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
        assertEquals 211, h2GIS.getTable(process.results.roadTableName).rowCount

        //h2GIS.getTable(process.results.railTableName).save("./target/osm_rails.shp")
        assertEquals 44, h2GIS.getTable(process.results.railTableName).rowCount

        //h2GIS.getTable(process.results.hydroTableName).save("./target/osm_hydro.shp")
        assertEquals 10, h2GIS.getTable(process.results.hydroTableName).rowCount

        //h2GIS.getTable(process.results.imperviousTableName).save("./target/osm_hydro.shp")
        assertEquals 47, h2GIS.getTable(process.results.imperviousTableName).rowCount

        //h2GIS.getTable(process.results.imperviousTableName).save("./target/osm_hydro.shp")
        assertEquals 6, h2GIS.getTable(process.results.urbanAreasTableName).rowCount
    }

    //This test is used for debug purpose
    @Test
    @Disabled
    void createGISLayersFromFileTestIntegration() {
        IProcess process = OSM.InputDataLoading.createGISLayers()
        def osmfile = "/tmp/map.osm"
        process.execute([
                datasource : h2GIS,
                osmFilePath: osmfile,
                epsg :2154])
        //h2GIS.getTable(process.results.buildingTableName).save("./target/osm_building.shp")

        //h2GIS.getTable(process.results.vegetationTableName).save("./target/osm_vegetation.shp")

        h2GIS.getTable(process.results.roadTableName).save("/tmp/osm_road.shp")

        //h2GIS.getTable(process.results.railTableName).save("./target/osm_rails.shp")

        //h2GIS.getTable(process.results.hydroTableName).save("./target/osm_hydro.shp")

        //h2GIS.getTable(process.results.imperviousTableName).save("./target/osm_hydro.shp")

        //h2GIS.getTable(process.results.imperviousTableName).save("./target/osm_hydro.shp")
    }
}
