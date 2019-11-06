package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.orbisgis.orbisprocess.geoclimate.geoindicators.DataUtils
import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.LoggerFactory

/**
 * This class contains all references to the group of chains used by GeoClimate
 */
abstract class ProcessingChain extends GroovyProcessFactory {
    public static def logger = LoggerFactory.getLogger(ProcessingChain.class)

    public static PrepareBDTopo = new PrepareBDTopo()
    public static PrepareOSM  = new PrepareOSM()
    public static BuildGeoIndicators  = new BuildGeoIndicators()
    public static BuildSpatialUnits  = new BuildSpatialUnits()
    public static DataUtils  = new DataUtils()
    public static Workflow  = new Workflow()

    //Utility methods
    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def getOutputTableName(prefixName, baseName){
        if (!prefixName){
            return baseName
        }
        else{
            return prefixName + "_" + baseName
        }
    }
}
