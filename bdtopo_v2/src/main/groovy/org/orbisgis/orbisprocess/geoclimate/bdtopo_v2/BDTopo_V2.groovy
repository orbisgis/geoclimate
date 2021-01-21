package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2



/**
 * Class to manage and access to the BDTOPO processes
 *
 */
class BDTopo_V2  {

    public static workflow
    public static initParametersAbstract
    public static importPreprocess
    public static initTypes
    public static formatInputData
    public static prepareData

    static {
        def workflowBDTopo_V2 = new WorkflowBDTopo_V2()
        def abstractTablesInitialization = new AbstractTablesInitialization()
        def bDTopoGISLayers = new BDTopoGISLayers()
        def inputDataFormatting = new InputDataFormatting()
        def prepareBDTopo = new PrepareBDTopo()
        workflow =  workflowBDTopo_V2.workflow()
        initParametersAbstract = abstractTablesInitialization.initParametersAbstract()
        importPreprocess = bDTopoGISLayers.importPreprocess()
        initTypes = bDTopoGISLayers.initTypes();
        formatInputData =  inputDataFormatting.formatData()
        prepareData = prepareBDTopo.prepareData()
    }

}