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
package org.orbisgis.geoclimate.bdtopo


import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.geoclimate.utils.AbstractScript

/**
 * BDTopo utils
 */
abstract class BDTopoUtils extends AbstractScript {

    BDTopoUtils() {
    }

    /**
     * Utility method to create a bbox from a point (X, Y) and a distance (in meters).
     * The bbox is represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * @param x
     * @param y
     * @param distance
     * @return
     */
    static List bbox(def x, def y, float distance) {
        if (distance >= 0) {
            return [y - distance, x - distance, y + distance, x + distance]
        } else {
            throw new IllegalArgumentException("Bbox operation does not accept negative value")
        }
    }

    /**
     * Utility method to create bbox from a GEOMETRY.
     * The BBOX is represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * @param geometry JTS geometry
     * @return
     */
    static List bbox(Geometry geometry) {
        if (geometry) {
            Envelope env = geometry.getEnvelopeInternal()
            return [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        }
        return
    }

    /**
     * Utility method to create a bbox from a Envelope
     * The bbox is represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * @param env JTS envelope
     * @return
     */
    static List bbox(Envelope env) {
        if (env) {
            return [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        }
        return
    }

}
