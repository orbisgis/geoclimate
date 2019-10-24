package org.orbisgis.orbisprocess.geoclimate

import org.orbisgis.orbisprocess.geoindicators.BlockIndicators
import org.orbisgis.orbisprocess.geoindicators.BuildingIndicators
import org.orbisgis.orbisprocess.geoindicators.DataUtils
import org.orbisgis.orbisprocess.geoindicators.GenericIndicators
import org.orbisgis.orbisprocess.geoindicators.Geoindicators
import org.orbisgis.orbisprocess.geoindicators.RsuIndicators
import org.orbisgis.orbisprocess.geoindicators.SpatialUnits
import org.orbisgis.orbisprocess.geoindicators.TypologyClassification
import org.orbisgis.orbisprocess.preparedata.PrepareData
import org.orbisgis.orbisprocess.preparedata.bdtopo.BDTopoGISLayers
import org.orbisgis.orbisprocess.preparedata.common.AbstractTablesInitialization
import org.orbisgis.orbisprocess.preparedata.common.InputDataFormatting
import org.orbisgis.orbisprocess.preparedata.osm.FormattingForAbstractModel
import org.orbisgis.orbisprocess.preparedata.osm.OSMGISLayers
import org.orbisgis.orbisprocess.processingchain.BuildGeoIndicators
import org.orbisgis.orbisprocess.processingchain.BuildLCZ
import org.orbisgis.orbisprocess.processingchain.BuildSpatialUnits
import org.orbisgis.orbisprocess.processingchain.GeoclimateChain
import org.orbisgis.orbisprocess.processingchain.PrepareBDTopo
import org.orbisgis.orbisprocess.processingchain.PrepareOSM
import org.orbisgis.orbisprocess.processingchain.ProcessingChain
import org.slf4j.LoggerFactory

class Geoclimate {

    public static PrepareBDTopo = new PrepareBDTopo()
    public static PrepareOSM  = new PrepareOSM()
    public static BuildGeoIndicators  = new BuildGeoIndicators()
    public static BuildSpatialUnits  = new BuildSpatialUnits()
    public static BuildLCZ  = new BuildLCZ()
    public static DataUtils  = new DataUtils()
    public static GeoclimateChain  = new GeoclimateChain()
    public static AbstractTablesInitialization = new AbstractTablesInitialization()
    public static BDTopoGISLayers = new BDTopoGISLayers()
    public static OSMGISLayers = new OSMGISLayers()
    public static FormattingForAbstractModel = new FormattingForAbstractModel()
    public static InputDataFormatting = new InputDataFormatting()
    public static BuildingIndicators = new BuildingIndicators()
    public static RsuIndicators = new RsuIndicators()
    public static BlockIndicators = new BlockIndicators()
    public static GenericIndicators = new GenericIndicators()
    public static SpatialUnits = new SpatialUnits()
    public static TypologyClassification = new TypologyClassification()

    static void setLogger(def logger){
        PrepareData.logger = logger
        ProcessingChain.logger = logger
        Geoindicators.logger = logger
    }
}
