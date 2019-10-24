package org.orbisgis.orbisprocess.preparedata

import org.orbisgis.orbisprocess.preparedata.bdtopo.BDTopoGISLayers
import org.orbisgis.orbisprocess.preparedata.common.AbstractTablesInitialization
import org.orbisgis.orbisprocess.preparedata.osm.FormattingForAbstractModel
import org.orbisgis.orbisprocess.preparedata.common.InputDataFormatting
import org.orbisgis.orbisprocess.preparedata.osm.OSMGISLayers
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
