package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.process.api.IProcess

import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class NoiseIndicatorsTests {

    @TempDir
    static File folder

    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.absolutePath+File.separator+"NoiseIndicatorsTests;AUTO_SERVER=TRUE")
    }

    @Test
    void groundAcousticLayer() {
        h2GIS.load(SpatialUnitsTests.class.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("building_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("hydro_test.geojson"), true)
        def zone = h2GIS.load(SpatialUnitsTests.class.getResource("zone_test.geojson"), true)

        def env = h2GIS.getSpatialTable(zone).getExtent()
        if (env) {
            def gridP = Geoindicators.SpatialUnits.createGrid()
            assert gridP.execute([geometry: env, deltaX: 100, deltaY: 100, datasource: h2GIS])
            def outputTable = gridP.results.outputTableName

            IProcess process = Geoindicators.NoiseIndicators.groundAcousticAbsorption()
            assertTrue process.execute(["zone"  : outputTable, "id_zone": "id_grid",
                             building: "building_test", road: "road_test", vegetation: "veget_test", water: "hydro_test", datasource: h2GIS])


            def ground_acoustic = process.results.ground_acoustic

            h2GIS.save(ground_acoustic, "/tmp/ground_acoustics.shp", true)

        }
    }
}
