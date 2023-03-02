package org.orbisgis.geoclimate.osm

import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.geoclimate.geoindicators.WorkflowUtilities
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.geoclimate.utils.AbstractScript
import org.orbisgis.process.GroovyProcessFactory
import org.slf4j.LoggerFactory

/**
 * Main class to access to the OSM processes
 *
 */
abstract class OSM extends AbstractScript {

    public static WorkflowOSM = new WorkflowOSM()
    public static InputDataLoading = new InputDataLoading()
    public static InputDataFormatting = new InputDataFormatting()

    OSM() {
        super(LoggerFactory.getLogger(OSM.class))
        WorkflowUtilities.setLoggerLevel("INFO")
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