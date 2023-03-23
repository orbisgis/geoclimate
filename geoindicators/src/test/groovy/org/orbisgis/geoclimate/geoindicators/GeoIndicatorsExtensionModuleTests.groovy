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
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.orbisgis.data.api.dataset.ISpatialTable

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class GeoIndicatorsExtensionModuleTests {

    @TempDir
    static File folder
    private static def h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "geoIndicatorsExtensionModuleTests;AUTO_SERVER=TRUE")
    }

    @Test
    void saveGeometryToFile() {
        Geometry geom = new GeometryFactory().createPoint(new Coordinate(10, 10))
        File outputFile = new File(folder, "geometry.geojson")
        geom.save(h2GIS, outputFile.getAbsolutePath())
        h2GIS.load(outputFile.getAbsolutePath(), "mygeom", true)
        ISpatialTable table = h2GIS.getSpatialTable("mygeom")
        assertTrue(table.getRowCount() == 1)
        table.next()
        assertEquals(geom, table.getGeometry(1))
    }

    @Test
    void saveGeometrySridToFile() {
        Geometry geom = new GeometryFactory().createPoint(new Coordinate(10, 10))
        geom.setSRID(4326)
        File outputFile = new File(folder, "geometry.geojson")
        geom.save(h2GIS, outputFile.getAbsolutePath())
        h2GIS.load(outputFile.getAbsolutePath(), "mygeom", true)
        ISpatialTable table = h2GIS.getSpatialTable("mygeom")
        assertTrue(table.getRowCount() == 1)
        table.next()
        assertEquals(4326, table.getGeometry(1).getSRID())
    }


}
