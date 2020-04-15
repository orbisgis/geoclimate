package org.orbisgis.orbisprocess.geoclimate

import org.orbisgis.orbisprocess.geoclimate.geoindicators.*
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.orbisprocess.geoclimate.processingchain.*

/**
 * Root access point to the Geoindicators processes.
 */
class Geoclimate {

    public static def BuildGeoIndicators  = new BuildGeoIndicators()
    public static def BuildSpatialUnits  = new BuildSpatialUnits()
    public static def DataUtils  = new DataUtils()
    public static def Workflow  = new Workflow()
    public static def BuildingIndicators = new BuildingIndicators()
    public static def RsuIndicators = new RsuIndicators()
    public static def BlockIndicators = new BlockIndicators()
    public static def GenericIndicators = new GenericIndicators()
    public static def SpatialUnits = new SpatialUnits()
    public static def TypologyClassification = new TypologyClassification()
    public static def OSM = new OSM()
    public static def BDTOPO_V2 = new BDTopo_V2()

    /**
     * Set the logger for all the processes.
     *
     * @param logger Logger to use in the processes.
     */
    static void setLogger(def logger){
        PrepareData.logger = logger
        ProcessingChain.logger = logger
        Geoindicators.logger = logger
    }
}
