package org.orbisgis

import org.orbisgis.bdtopo.BDTopoGISLayers
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.osm.FormattingForAbstractModel
import org.orbisgis.common.InputDataFormatting
import org.orbisgis.osm.OSMGISLayers
import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.LoggerFactory

abstract class PrepareData extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(PrepareData.class)


    public static AbstractTablesInitialization = new AbstractTablesInitialization()
    public static BDTopoGISLayers = new BDTopoGISLayers()
    public static OSMGISLayers = new OSMGISLayers()
    public static FormattingForAbstractModel = new FormattingForAbstractModel()
    public static InputDataFormatting = new InputDataFormatting()

}
