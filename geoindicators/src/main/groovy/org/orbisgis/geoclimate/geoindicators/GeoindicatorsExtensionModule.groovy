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

import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS

/**
 * A module to extend on the fly some classes
 */

    /**
     * Utilities to save a geometry to a file
     * @param geometry input geometry
     * @param h2GIS input h2GIS database
     * @param filePath path for the file
     * @return
     */
    static String save(Geometry geometry, H2GIS h2GIS, String filePath) {
        return h2GIS.save("(SELECT ST_GEOMFROMTEXT('${geometry}',${geometry.getSRID()}) as the_geom, CAST(1 as integer) as id)", filePath, true)
    }
