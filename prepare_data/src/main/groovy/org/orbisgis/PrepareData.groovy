package org.orbisgis

import org.orbisgis.bdtopo.BDTopoGISLayers
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.osm.FormattingForAbstractModel
import org.orbisgis.common.InputDataFormatting
import org.orbisgis.osm.OSMGISLayers
import org.orbisgis.processmanager.ProcessManager
import org.orbisgis.processmanagerapi.IProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class PrepareData extends Script {

    public static IProcessFactory processFactory = ProcessManager.getProcessManager().factory("prepareData")

    public static Logger logger = LoggerFactory.getLogger(PrepareData.class)


    public static AbstractTablesInitialization = new AbstractTablesInitialization()
    public static BDTopoGISLayers = new BDTopoGISLayers()
    public static OSMGISLayers = new OSMGISLayers()
    public static FormattingForAbstractModel = new FormattingForAbstractModel()
    public static InputDataFormatting = new InputDataFormatting()

}
