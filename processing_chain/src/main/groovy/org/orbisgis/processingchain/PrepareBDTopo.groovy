package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.bdtopo.BDTopoGISLayers
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.processmanager.ProcessMapper
import org.orbisgis.processmanagerapi.IProcess

@BaseScript ProcessingChain processingChain

/**
 *
 * @return
 */
public static ProcessMapper createMapper(){
    def abstractTablesInit = AbstractTablesInitialization.initParametersAbstract()
    def bdTopoInitTypes = BDTopoGISLayers.initTypes()
    def importPreProcess = BDTopoGISLayers.importPreprocess()

    ProcessMapper mapper = new ProcessMapper()
    mapper.link(outputBuildingAbstractUseType : abstractTablesInit, buildingAbstractUseType : bdTopoInitTypes)
    mapper.link(outputRoadAbstractType : abstractTablesInit, roadAbstractType : bdTopoInitTypes)
    mapper.link(outputRailAbstractType : abstractTablesInit, railAbstractType : bdTopoInitTypes)
    mapper.link(outputVegetAbstractType : abstractTablesInit, vegetAbstractType : bdTopoInitTypes)

    mapper.link(outputBuildingAbstractUseType : abstractTablesInit, building_abstract_use_type : importPreProcess)
    mapper.link(outputRoadAbstractType : abstractTablesInit, road_abstract_type : importPreProcess)
    mapper.link(outputRailAbstractType : abstractTablesInit, rail_abstract_type : importPreProcess)
    mapper.link(outputVegetAbstractType : abstractTablesInit, veget_abstract_type : importPreProcess)
    mapper.link(outputBuildingBDTopoUseType : bdTopoInitTypes, building_bd_topo_use_type : importPreProcess)
    mapper.link(outputroadBDTopoType : bdTopoInitTypes, road_bd_topo_type : importPreProcess)
    mapper.link(outputrailBDTopoType : bdTopoInitTypes, rail_bd_topo_type : importPreProcess)
    mapper.link(outputvegetBDTopoType : bdTopoInitTypes, veget_bd_topo_type : importPreProcess)


    return mapper
}