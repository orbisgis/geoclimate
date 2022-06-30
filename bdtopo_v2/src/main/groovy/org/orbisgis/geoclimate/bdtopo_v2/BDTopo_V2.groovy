package org.orbisgis.geoclimate.bdtopo_v2

import org.orbisgis.process.GroovyProcessFactory
import org.slf4j.LoggerFactory


/**
 * Class to manage and access to the BDTOPO processes
 *
 */
abstract class BDTopo_V2 extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(BDTopo_V2.class)

    public static WorkflowBDTopo_V2 = new WorkflowBDTopo_V2()
    public static InputDataLoading = new InputDataLoading2()
    public static InputDataFormatting = new InputDataFormatting2()


    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }

    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def debug = { obj -> logger.debug(obj.toString()) }

}