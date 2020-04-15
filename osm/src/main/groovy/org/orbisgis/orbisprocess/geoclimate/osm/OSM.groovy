package org.orbisgis.orbisprocess.geoclimate.osm


/**
 * Main class to access to the OSM processes
 *
 */
class OSM  {

    public static def Workflow  = new WorkflowOSM().OSM();
    public static def formatBuildingLayer= new FormattingForAbstractModel().formatBuildingLayer()
    public static def formatVegetationLayer= new FormattingForAbstractModel().formatVegetationLayer()
    public static def formatRoadLayer= new FormattingForAbstractModel().formatRoadLayer()
    public static def formatRailsLayer= new FormattingForAbstractModel().formatRailsLayer()
    public static def formatHydroLayer= new FormattingForAbstractModel().formatHydroLayer()
    public static def formatImperviousLayer= new FormattingForAbstractModel().formatImperviousLayer()
    public static def createGISLayers  = new OSMGISLayers().createGISLayers()
    public static def extractAndCreateGISLayers  = new OSMGISLayers().extractAndCreateGISLayers()
    public static def buildGeoclimateLayers  = new PrepareOSM().buildGeoclimateLayers()
}
