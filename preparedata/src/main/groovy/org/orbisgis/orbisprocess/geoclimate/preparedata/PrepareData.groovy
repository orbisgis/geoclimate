package org.orbisgis.orbisprocess.geoclimate.preparedata

import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.orbisgis.orbisprocess.geoclimate.preparedata.bdtopo.BDTopoGISLayers
import org.orbisgis.orbisprocess.geoclimate.preparedata.common.AbstractTablesInitialization
import org.orbisgis.orbisprocess.geoclimate.preparedata.common.InputDataFormatting
import org.orbisgis.orbisprocess.geoclimate.preparedata.osm.FormattingForAbstractModel
import org.orbisgis.orbisprocess.geoclimate.preparedata.osm.OSMGISLayers
import org.slf4j.LoggerFactory

abstract class PrepareData extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(PrepareData.class)


    public static AbstractTablesInitialization = new AbstractTablesInitialization()
    public static BDTopoGISLayers = new BDTopoGISLayers()
    public static OSMGISLayers = new OSMGISLayers()
    public static FormattingForAbstractModel = new FormattingForAbstractModel()
    public static InputDataFormatting = new InputDataFormatting()

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
