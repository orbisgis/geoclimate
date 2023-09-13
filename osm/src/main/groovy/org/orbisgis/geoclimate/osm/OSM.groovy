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

import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.geoclimate.utils.AbstractScript
import org.orbisgis.geoclimate.utils.LoggerUtils

/**
 * Main class to access to the OSM processes
 *
 */
abstract class OSM extends AbstractScript {

    public static WorkflowOSM = new WorkflowOSM()
    public static InputDataLoading = new InputDataLoading()
    public static InputDataFormatting = new InputDataFormatting()

    OSM() {
        super(OSM.class)
    }


    /**
     * Run the OSM workflow
     * @param input
     * @return
     */
    static Map workflow(def input) {
        WorkflowOSM workflowOSM = new WorkflowOSM()
        return workflowOSM.workflow(input)
    }

    /**
     * Utility method to create bbox represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * from a point and a distance
     * @param lat
     * @param lon
     * @param distance
     * @return
     */
    static List bbox(float lat, float lon, float distance) {
        return Utilities.createBBox(lat, lon, distance)
    }

    /**
     * Utility method to create bbox represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * from a geometry
     * @param geometry JTS geometry
     * @return
     */
    static List bbox(Geometry geometry) {
        if (geometry) {
            Envelope env = geometry.getEnvelopeInternal()
            return [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        }
    }

    /**
     * Utility method to create bbox represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * from an envelope
     * @param env JTS envelope
     * @return
     */
    static List bbox(Envelope env) {
        if (env) {
            return [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        }
    }

}