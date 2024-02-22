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
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

import static org.orbisgis.data.H2GIS.open

class SprawlIndicatorsTests {

    @TempDir
    static File folder
    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        folder = new File("/tmp")
        h2GIS = open(folder.getAbsolutePath() + File.separator + "sprawlindicators")
    }

    @Disabled
    @Test
    void debug_tests() {

        /*String rsu_lcz = h2GIS.load("/home/ebocher/Autres/data/geoclimate/uhi_lcz/Angers/rsu_lcz.geojson", true)
        def buffering_cool_areas = Geoindicators.SprawlIndicators.buffering_cool_areas(h2GIS, rsu_lcz, "")

        h2GIS.save(buffering_cool_areas, "/tmp/buffering_cool_areas.geojson", true)*/


        String grid_indicators = h2GIS.load("/home/ebocher/Autres/data/geoclimate/uhi_lcz/Dijon/grid_indicators.geojson", true)


        def sprawlLayer = Geoindicators.LCZIndicators.compute_sprawl_areas(h2GIS, grid_indicators)

        h2GIS.save(sprawlLayer, "/tmp/sprawl_areas_fractions.geojson", true)

        /*
        String grid_distances = Geoindicators.SprawlIndicators.grid_distances(h2GIS, sprawlLayer, grid_indicators)

        h2GIS.save(grid_distances, "/tmp/sprawl_areas_distances.geojson", true)


        String sprawlLayerInverse = Geoindicators.SprawlIndicators.inverse_geometries(h2GIS, sprawlLayer)

        h2GIS.save(sprawlLayerInverse, "/tmp/spraw_areas_inverse.geojson", true)


        String grid_distances_out = Geoindicators.SprawlIndicators.grid_distances(h2GIS, sprawlLayerInverse, grid_indicators)

        h2GIS.save(grid_distances_out, "/tmp/sprawl_areas_distances_out.geojson", true)

        def cool_areasLayer = Geoindicators.SprawlIndicators.sprawl_cool_areas(h2GIS, sprawlLayer, grid_indicators)

        h2GIS.save(cool_areasLayer, "/tmp/cool_areas.geojson", true)

        String cool_areas_inverse = Geoindicators.SprawlIndicators.inverse_cool_areas(h2GIS,sprawlLayerInverse, cool_areasLayer)

        h2GIS.save(cool_areas_inverse, "/tmp/cool_areas_inverse.geojson", true)


        String cool_areas_distances = Geoindicators.SprawlIndicators.grid_distances(h2GIS, cool_areas_inverse, grid_indicators)

        h2GIS.save(cool_areas_distances, "/tmp/cool_areas_distances.geojson", true)
        */


    }


}
