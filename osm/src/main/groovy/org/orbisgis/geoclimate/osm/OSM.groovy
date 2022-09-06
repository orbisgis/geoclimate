package org.orbisgis.geoclimate.osm

import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.process.GroovyProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Main class to access to the OSM processes
 *
 */
abstract class OSM extends GroovyProcessFactory {

    public static Logger logger = LoggerFactory.getLogger(OSM.class)

    public static WorkflowOSM = new WorkflowOSM()
    public static InputDataLoading = new InputDataLoading()
    public static InputDataFormatting = new InputDataFormatting()


    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }

    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static debug = { obj -> logger.debug(obj.toString()) }

    /**
     * Utility method to generate a name
     * @param prefixName
     * @param baseName
     * @return
     */
    static def getOutputTableName(prefixName, baseName) {
        if (!prefixName) {
            return baseName
        } else {
            return prefixName + "_" + baseName
        }
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
        if(geometry){
            Envelope env = geometry.getEnvelopeInternal()
            return [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        }
    }

    /**
     * Utility method to create bbox represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * from an envelope
     * @param env  JTS envelope
     * @return
     */
    static List bbox(Envelope env) {
        if(env){
            return [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        }
    }

}