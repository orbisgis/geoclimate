package org.orbisgis.geoclimate.bdtopo

import ch.qos.logback.classic.Logger
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.geoclimate.geoindicators.WorkflowUtilities
import org.orbisgis.geoclimate.utils.AbstractScript
import org.orbisgis.geoclimate.utils.LoggerUtils
import org.slf4j.LoggerFactory

/**
 * BDTopo utils
 */
abstract class BDTopoUtils extends AbstractScript{

    BDTopoUtils() {
        super(LoggerFactory.getLogger(BDTopoUtils.class))
        LoggerUtils.setLoggerLevel("INFO")
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
