package org.orbisgis.orbisprocess.geoclimate.osm


import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.slf4j.LoggerFactory

abstract class OSM extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(OSM.class)
    public static OSMGISLayers = new OSMGISLayers()
    public static FormattingForAbstractModel = new FormattingForAbstractModel()
    public static PrepareOSM  = new PrepareOSM()

    public static Workflow  = new Workflow()

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
