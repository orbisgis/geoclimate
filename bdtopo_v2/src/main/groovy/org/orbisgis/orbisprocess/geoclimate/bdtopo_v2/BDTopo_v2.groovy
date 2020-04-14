package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.slf4j.LoggerFactory

/**
 * Class to manage and access to the BDTOPO processes
 *
 */
abstract class BDTopo_v2 extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(BDTopo_v2.class)


    public static AbstractTablesInitialization = new AbstractTablesInitialization()
    public static BDTopoGISLayers = new BDTopoGISLayers()
    public static InputDataFormatting = new InputDataFormatting()
    public static PrepareBDTopo = new PrepareBDTopo()
    public static Workflow  = new Workflow()

    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }

    /**
     * Utility method to generate a name
     * @param prefixName
     * @param baseName
     * @return
     */
    static def getOutputTableName(prefixName, baseName){
        if (!prefixName){
            return baseName
        }
        else{
            return prefixName + "_" + baseName
        }
    }

}
