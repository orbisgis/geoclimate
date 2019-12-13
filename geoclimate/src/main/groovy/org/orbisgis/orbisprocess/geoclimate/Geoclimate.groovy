package org.orbisgis.orbisprocess.geoclimate

import org.orbisgis.orbisprocess.geoclimate.geoindicators.*
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.orbisprocess.geoclimate.preparedata.bdtopo.BDTopoGISLayers
import org.orbisgis.orbisprocess.geoclimate.preparedata.common.AbstractTablesInitialization
import org.orbisgis.orbisprocess.geoclimate.preparedata.common.InputDataFormatting
import org.orbisgis.orbisprocess.geoclimate.preparedata.osm.FormattingForAbstractModel
import org.orbisgis.orbisprocess.geoclimate.preparedata.osm.OSMGISLayers
import org.orbisgis.orbisprocess.geoclimate.processingchain.*

/**
 * Root access point to the Geoindicators processes.
 */
class Geoclimate {

    public static def PrepareBDTopo = new PrepareBDTopo()
    public static def PrepareOSM  = new PrepareOSM()
    public static def BuildGeoIndicators  = new BuildGeoIndicators()
    public static def BuildSpatialUnits  = new BuildSpatialUnits()
    public static def DataUtils  = new DataUtils()
    public static def Workflow  = new Workflow()
    public static def AbstractTablesInitialization = new AbstractTablesInitialization()
    public static def BDTopoGISLayers = new BDTopoGISLayers()
    public static def OSMGISLayers = new OSMGISLayers()
    public static def FormattingForAbstractModel = new FormattingForAbstractModel()
    public static def InputDataFormatting = new InputDataFormatting()
    public static def BuildingIndicators = new BuildingIndicators()
    public static def RsuIndicators = new RsuIndicators()
    public static def BlockIndicators = new BlockIndicators()
    public static def GenericIndicators = new GenericIndicators()
    public static def SpatialUnits = new SpatialUnits()
    public static def TypologyClassification = new TypologyClassification()

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
