package org.orbisgis.orbisprocess.geoclimate.osm


/**
 * Main class to access to the OSM processes
 *
 */
class OSM  {

    public static def Workflow  = WorkflowOSM.OSM();
    public static  def formatBuildingLayer= FormattingForAbstractModel.formatBuildingLayer()
    public static  def formatVegetationLayer= FormattingForAbstractModel.formatVegetationLayer()
    public static  def formatRoadLayer= FormattingForAbstractModel.formatRoadLayer()
    public static  def formatRailsLayer= FormattingForAbstractModel.formatRailsLayer()
    public static  def formatHydroLayer= FormattingForAbstractModel.formatHydroLayer()
    public static  def formatImperviousLayer= FormattingForAbstractModel.formatImperviousLayer()
    public static def createGISLayers  = OSMGISLayers.createGISLayers()
    public static def extractAndCreateGISLayers  = OSMGISLayers.extractAndCreateGISLayers()
    public static def buildGeoclimateLayers  = PrepareOSM.buildGeoclimateLayers()
}
