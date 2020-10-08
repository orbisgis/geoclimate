package org.orbisgis.orbisprocess.geoclimate.osm

/**
 * Main class to access to the OSM processes
 *
 */
class OSM  {

    public static def workflow
    public static def formatBuildingLayer;
    public static def formatVegetationLayer
    public static def formatRoadLayer
    public static def formatRailsLayer
    public static def formatHydroLayer
    public static def formatImperviousLayer
    public static def createGISLayers
    public static def extractAndCreateGISLayers
    public static def buildGeoclimateLayers
    public static def formatEstimatedBuilding
    static {
        def formattingForAbstractModel = new FormattingForAbstractModel()
        def osmGISLayers = new OSMGISLayers()
        def prepareOSM = new PrepareOSM()
        def workflowOSM  = new WorkflowOSM()
        formatBuildingLayer= formattingForAbstractModel.formatBuildingLayer()
        formatVegetationLayer= formattingForAbstractModel.formatVegetationLayer()
        formatRoadLayer= formattingForAbstractModel.formatRoadLayer()
        formatRailsLayer= formattingForAbstractModel.formatRailsLayer()
        formatHydroLayer= formattingForAbstractModel.formatHydroLayer()
        formatImperviousLayer= formattingForAbstractModel.formatImperviousLayer()
        formatEstimatedBuilding = formattingForAbstractModel.formatEstimatedBuilding()
        createGISLayers  = osmGISLayers.createGISLayers()
        extractAndCreateGISLayers  = osmGISLayers.extractAndCreateGISLayers()
        buildGeoclimateLayers  = prepareOSM.buildGeoclimateLayers()
        workflow  = workflowOSM.workflow()
    }

}