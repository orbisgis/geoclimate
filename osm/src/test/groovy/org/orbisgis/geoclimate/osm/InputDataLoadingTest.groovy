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
package org.orbisgis.geoclimate.osm

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals

class InputDataLoadingTest {

    @TempDir
    static File folder

    static H2GIS h2GIS

    @BeforeAll
    static void loadDb() {
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "osm_inputDataLoadingTest;AUTO_SERVER=TRUE;")
    }

    @Disabled
    //enable it to test data extraction from the overpass api
    @Test
    void extractAndCreateGISLayers() {
        Map extract = OSM.InputDataLoading.extractAndCreateGISLayers(h2GIS, "Île de la Nouvelle-Amsterdam")
        extract.each { it ->
            if (it.value != null) {
                h2GIS.getTable(it.value).save(new File(folder, "${it.value}.shp").absolutePath, true)
            }
        }
    }

    @Test
    void createGISLayersTest() {
        def osmfile = new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath()
        Map extract = OSM.InputDataLoading.createGISLayers(h2GIS, osmfile, 2154)
        assertEquals 1034, h2GIS.getTable(extract.building).rowCount
        assertEquals 135, h2GIS.getTable(extract.vegetation).rowCount
        assertEquals 211, h2GIS.getTable(extract.road).rowCount

        assertEquals 44, h2GIS.getTable(extract.rail).rowCount

        assertEquals 10, h2GIS.getTable(extract.water).rowCount

        assertEquals 47, h2GIS.getTable(extract.impervious).rowCount

        assertEquals 10, h2GIS.getTable(extract.urban_areas).rowCount
    }

    //This test is used for debug purpose
    @Test
    @Disabled
    void createGISLayersFromFileTestIntegration() {
        Map extract = OSM.InputDataLoading.createGISLayers(h2GIS, "/tmp/3be46f5e1060bd53fe7c5b97a04102b3f36fa6f6257c87664dbaec3e954879ab.osm", 2154)

        //h2GIS.getTable(extract.vegetation).save("./target/osm_vegetation.shp")

        h2GIS.getTable(extract.road).save("/tmp/osm_road.shp")
    }
}
