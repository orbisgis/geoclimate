package org.orbisgis.processingchain

import org.orbisgis.processmanager.ProcessManager
import org.orbisgis.processmanagerapi.IProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory


abstract class ProcessingChain extends Script {
    public static Logger logger = LoggerFactory.getLogger(ProcessingChain.class)

    public static IProcessFactory processFactory = ProcessManager.getProcessManager().factory("processing_chain")

    public  static PrepareBDTopo prepareBDTopo= new PrepareBDTopo()

    public static  PrepareOSM prepareOSM = new PrepareOSM()

    public static  BuildGeoIndicators buildGeoIndicators = new BuildGeoIndicators()

    public BuildSpatialUnits buildSpatialUnits = new BuildSpatialUnits()

    public BuildLCZ buildLCZ = new BuildLCZ()
}
