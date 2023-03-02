package org.orbisgis.geoclimate.osm

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.process.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals

class InputDataLoadingTest {

    @TempDir
    static File folder

    static  H2GIS h2GIS

    @BeforeAll
    static  void loadDb(){
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "osm_inputDataLoadingTest;AUTO_SERVER=TRUE;")
    }

    @Disabled //enable it to test data extraction from the overpass api
    @Test
    void extractAndCreateGISLayers() {
        Map extract = OSM.InputDataLoading.extractAndCreateGISLayers(h2GIS, "Île de la Nouvelle-Amsterdam")
        extract.each {it ->
            if(it.value!=null){
                h2GIS.getTable(it.value).save(new File(folder, "${it.value}.shp").absolutePath, true)
            }
        }
    }

    @Test
    void createGISLayersTest() {
        def osmfile = new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath()
        Map extract = OSM.InputDataLoading.createGISLayers(h2GIS, osmfile, 2154)
        assertEquals 1038, h2GIS.getTable(extract.building).rowCount

        assertEquals 135, h2GIS.getTable(extract.vegetation).rowCount
        assertEquals 211, h2GIS.getTable(extract.road).rowCount

        assertEquals 44, h2GIS.getTable(extract.rail).rowCount

        assertEquals 10, h2GIS.getTable(extract.water).rowCount

        assertEquals 47, h2GIS.getTable(extract.impervious).rowCount

        assertEquals 6, h2GIS.getTable(extract.urban_areas).rowCount
    }

    //This test is used for debug purpose
    @Test
    @Disabled
    void createGISLayersFromFileTestIntegration() {
        Map extract = OSM.InputDataLoading.createGISLayers(h2GIS, "/tmp/map.osm", 2154)

        //h2GIS.getTable(extract.vegetation).save("./target/osm_vegetation.shp")

        h2GIS.getTable(extract.road).save("/tmp/osm_road.shp")
    }
}
