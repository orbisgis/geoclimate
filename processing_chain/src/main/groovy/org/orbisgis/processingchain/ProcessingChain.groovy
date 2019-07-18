package org.orbisgis.processingchain

import org.orbisgis.geoindicators.DataUtils
import org.orbisgis.processmanager.GroovyProcessFactory
import org.orbisgis.processmanager.ProcessManager
import org.orbisgis.processmanagerapi.IProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This class contains all references to the group of chains used by GeoClimate
 */
abstract class ProcessingChain extends GroovyProcessFactory {
    public static Logger logger = LoggerFactory.getLogger(ProcessingChain.class)

    public  static PrepareBDTopo = new PrepareBDTopo()

    public static  PrepareOSM  = new PrepareOSM()

    public static BuildGeoIndicators  = new BuildGeoIndicators()

    public static BuildSpatialUnits  = new BuildSpatialUnits()

    public static BuildLCZ  = new BuildLCZ()

    public static DataUtils  = new DataUtils()
}
