/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class NoiseIndicatorsTests {

    @TempDir
    static File folder

    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.absolutePath + File.separator + "NoiseIndicatorsTests;AUTO_SERVER=TRUE")
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
            def gridP = Geoindicators.SpatialUnits.createGrid(h2GIS, env, 100, 100)
            assertNotNull(gridP)
            String ground_acoustic = Geoindicators.NoiseIndicators.groundAcousticAbsorption(h2GIS, gridP, "id_grid",
                    "building_test", "road_test", "hydro_test", "veget_test", "")
            assertTrue(h2GIS.hasTable(ground_acoustic))
        }
    }
}
