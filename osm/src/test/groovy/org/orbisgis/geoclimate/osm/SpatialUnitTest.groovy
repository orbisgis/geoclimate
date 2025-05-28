package org.orbisgis.geoclimate.osm

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

class SpatialUnitTest {

    @TempDir
    static File folder

    static H2GIS h2GIS

    @BeforeAll
    static void loadDb() {
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "osm_spatialUnitTest;")
    }

    @Disabled
    @Test
    void testRSUOutput() {
        def location = "Angers"
        //Extract the data from OSM
        Map extract = OSM.InputDataLoading.extractAndCreateGISLayers(h2GIS, location)

        if(!extract){
            println("Cannot create the GIS layers")
            return
        }

        //Format the GIS layers
        String road= OSM.InputDataFormatting.formatRoadLayer(h2GIS, extract.road)
        String rail= OSM.InputDataFormatting.formatRailsLayer(h2GIS, extract.rail)
        String vegetation= OSM.InputDataFormatting.formatVegetationLayer(h2GIS, extract.vegetation)
        String water= OSM.InputDataFormatting.formatWaterLayer(h2GIS, extract.water)

        h2GIS.save(extract.zone, "/tmp/zone.fgb", true)
        h2GIS.save(vegetation, "/tmp/vegetation.fgb", true)
        h2GIS.save(water, "/tmp/water.fgb", true)
        //Create the TSU
        def createRSU = Geoindicators.SpatialUnits.createTSU(h2GIS, extract.zone,
                road, rail,
                vegetation, water,
                "", "", 10000, 2500,
                10000, "tsu")
        h2GIS.save(createRSU, "/tmp/rsu.fgb", true)
    }
}
