package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.bdtopo.BDTopoGISLayers
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.common.InputDataFormatting
import org.orbisgis.processmanager.ProcessMapper
import org.orbisgis.processmanagerapi.IProcess

@BaseScript ProcessingChain processingChain

/** The processing chain creates the Table from the Abstract model and feed them with the BDTopo data according to some
 * defined rules (i.e. certain types of buildings or vegetation are transformed into a generic type in the abstract
 * model). Then default values are set (or values are corrected) and the quality of the input data is assessed and
 * returned into a statistic table for each layer. For further information, cf. each of the four processes used in the
 * mapper.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param distBuffer The distance (exprimed in meter) used to compute the buffer area around the ZONE
 * @param expand The distance (exprimed in meter) used to compute the extended area around the ZONE
 * @param idZone The Zone id
 * @param tableIrisName The table name in which the IRIS are stored
 * @param tableBuildIndifName The table name in which the undifferentiated ("Indifférencié" in french) buildings are stored
 * @param tableBuildIndusName The table name in which the industrial buildings are stored
 * @param tableBuildRemarqName The table name in which the remarkable ("Remarquable" in french) buildings are stored
 * @param tableRoadName The table name in which the roads are stored
 * @param tableRailName The table name in which the rail ways are stored
 * @param tableHydroName The table name in which the hydrographic areas are stored
 * @param tableVegetName The table name in which the vegetation areas are stored
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 *
 * @return outputBuilding Table name in which the (ready to be used in the GeoIndicators part) buildings are stored
 * @return outputBuildingStatZone Table that store the building statistics at the zone level
 * @return outputBuildingStatZoneBuff Table that store the building statistics at the zone_buffer level
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRoadStatZone Table that store the road statistics at the zone level
 * @return outputRoadStatZoneBuff Table that store the road statistics at the zone_buffer level
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputRailStatZone Table that store the rail ways statistics at the zone level
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputHydroStatZone Table that store the hydrographic areas statistics at the zone level
 * @return outputHydroStatZoneExt Table that store the hydrographic areas statistics at the zone_extended level
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputVegetStatZone Table that store the vegetation areas statistics at the zone level
 * @return outputVegetStatZoneExt Table that store the vegetation areas statistics at the zone_extended level
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 */
public static ProcessMapper createMapper(){
    def abstractTablesInit = AbstractTablesInitialization.initParametersAbstract()
    def bdTopoInitTypes = BDTopoGISLayers.initTypes()
    def tableFeeding = BDTopoGISLayers.importPreprocess()
    def dataFormatting = InputDataFormatting.inputDataFormatting()

    ProcessMapper mapper = new ProcessMapper()
    // FROM abstractTablesInit...
    // ...to bdTopoInitTypes
    mapper.link(outputBuildingAbstractUseType : abstractTablesInit, buildingAbstractUseType : bdTopoInitTypes)
    mapper.link(outputRoadAbstractType : abstractTablesInit, roadAbstractType : bdTopoInitTypes)
    mapper.link(outputRailAbstractType : abstractTablesInit, railAbstractType : bdTopoInitTypes)
    mapper.link(outputVegetAbstractType : abstractTablesInit, vegetAbstractType : bdTopoInitTypes)

    // ...to tableFeeding
    mapper.link(outputBuildingAbstractUseType : abstractTablesInit, buildingAbstractUseType : tableFeeding)
    mapper.link(outputRoadAbstractType : abstractTablesInit, roadAbstractType : tableFeeding)
    mapper.link(outputRailAbstractType : abstractTablesInit, railAbstractType : tableFeeding)
    mapper.link(outputVegetAbstractType : abstractTablesInit, vegetAbstractType : tableFeeding)

    // ...to dataFormatting
    mapper.link(outputBuildingAbstractParameters : abstractTablesInit, buildingAbstractParameters : dataFormatting)
    mapper.link(outputRoadAbstractParameters : abstractTablesInit, roadAbstractParameters : dataFormatting)
    mapper.link(outputVegetAbstractParameters : abstractTablesInit, vegetAbstractParameters : dataFormatting)

    // FROM bdTopoInitTypes...
    // ...to tableFeeding
    mapper.link(outputBuildingBDTopoUseType : bdTopoInitTypes, building_bd_topo_use_type : tableFeeding)
    mapper.link(outputroadBDTopoType : bdTopoInitTypes, road_bd_topo_type : tableFeeding)
    mapper.link(outputrailBDTopoType : bdTopoInitTypes, rail_bd_topo_type : tableFeeding)
    mapper.link(outputvegetBDTopoType : bdTopoInitTypes, veget_bd_topo_type : tableFeeding)

    // FROM tableFeeding...
    // ...to dataFormatting
    mapper.link(outputBuildingName : tableFeeding, inputBuilding : dataFormatting)
    mapper.link(outputRoadName : tableFeeding, inputRoad : dataFormatting)
    mapper.link(outputRailName : tableFeeding, inputRail : dataFormatting)
    mapper.link(outputHydroName : tableFeeding, inputHydro : dataFormatting)
    mapper.link(outputVegetName : tableFeeding, inputVeget : dataFormatting)
    mapper.link(outputZoneName : tableFeeding, inputZone : dataFormatting)
    mapper.link(outputZoneNeighborsName : tableFeeding, inputZoneNeighbors : dataFormatting)

    return mapper
}