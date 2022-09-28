package org.orbisgis.geoclimate.bdtopo_v2

import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.geoclimate.geoindicators.WorkflowUtilities
import org.orbisgis.process.GroovyProcessFactory
import org.slf4j.LoggerFactory


/**
 * Class to manage and access to the BDTOPO processes
 *
 */
abstract class BDTopo_V2 extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(BDTopo_V2.class)

    public static WorkflowBDTopo_V2 = new WorkflowBDTopo_V2()
    public static InputDataLoading = new InputDataLoading()
    public static InputDataFormatting = new InputDataFormatting()

    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }

    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def debug = { obj -> logger.debug(obj.toString()) }

    /**
     * Utility method to create bbox represented by a list of  values  :
     * [minY, minX, maxY, maxX]
     * from a point and a distance
     * @param x
     * @param y
     * @param distance
     * @return
     */
    static List bbox(def x, def y, float distance) {
        if(distance>=0) {
            return [y - distance, x - distance, y + distance, x + distance]
        }else{
            throw new IllegalArgumentException("Bbox operation does not accept negative value");
        }
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