package org.orbisgis.orbisprocess.geoclimate.osm


import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.slf4j.LoggerFactory

/**
 * Main class to access to the OSM processes
 *
 */
abstract class OSM extends GroovyProcessFactory {

    public static def logger = LoggerFactory.getLogger(OSM.class)
    public static Workflow  = WorkflowOSM.OSM();

    public static  formatBuildingLayer= FormattingForAbstractModel.formatBuildingLayer()
    public static  formatVegetationLayer= FormattingForAbstractModel.formatVegetationLayer()
    public static  formatRoadLayer= FormattingForAbstractModel.formatRoadLayer()
    public static  formatRailsLayer= FormattingForAbstractModel.formatRailsLayer()
    public static  formatHydroLayer= FormattingForAbstractModel.formatHydroLayer()
    public static  formatImperviousLayer= FormattingForAbstractModel.formatImperviousLayer()

    public static createGISLayers  = OSMGISLayers.createGISLayers()
    public static extractAndCreateGISLayers  = OSMGISLayers.extractAndCreateGISLayers()
    public static buildGeoclimateLayers  = PrepareOSM.buildGeoclimateLayers()

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
