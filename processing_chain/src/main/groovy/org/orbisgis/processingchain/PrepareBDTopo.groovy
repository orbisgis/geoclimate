package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.bdtopo.BDTopoGISLayers
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.common.InputDataFormatting
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanager.ProcessMapper


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
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 * @return outputStats List that stores the name of the statistic tables for each layer at different scales
 *
 */
def prepareBDTopo() {
    return create({
        title "Extract and transform BD Topo data to Geoclimate model"
        inputs datasource : JdbcDataSource,
             distBuffer : 500,
             expand : 1000,
             idZone : String,
             tableIrisName : String,
             tableBuildIndifName : String,
             tableBuildIndusName : String,
             tableBuildRemarqName : String,
             tableRoadName : String,
             tableRailName : String,
             tableHydroName : String,
             tableVegetName : String,
             hLevMin : 3,
             hLevMax : 15,
             hThresholdLev2 : 10
        outputs outputBuilding : String, outputRoad:String, outputRail : String, outputHydro:String, outputVeget:String, outputZone:String,outputStats : String[]
        run { datasource, distBuffer, expand, idZone, tableIrisName, tableBuildIndifName, tableBuildIndusName, tableBuildRemarqName, tableRoadName, tableRailName,
              tableHydroName, tableVegetName, hLevMin, hLevMax, hThresholdLev2 ->

            if (datasource == null) {
                error "Cannot create the database to store the BD Topo data"
                return
            }

            //Init model
            def initParametersAbstract = PrepareData.AbstractTablesInitialization.initParametersAbstract()
            if (!initParametersAbstract(datasource: datasource)) {
                info "Cannot initialize the geoclimate data model."
                return
            }
            def abstractTables = initParametersAbstract.getResults()

            //Init BD Topo parameters
            def initTypes = PrepareData.BDTopoGISLayers.initTypes()
            if (!initTypes([datasource       : datasource, buildingAbstractUseType: abstractTables.outputBuildingAbstractUseType,
                            roadAbstractType : abstractTables.outputRoadAbstractType, railAbstractType: abstractTables.outputRailAbstractType,
                            vegetAbstractType: abstractTables.outputVegetAbstractType])) {
                info "Cannot initialize the BD Topo parameters."
                return
            }
            def initTables = initTypes.getResults()

            //Import preprocess
            def importPreprocess = PrepareData.BDTopoGISLayers.importPreprocess()
            if (!importPreprocess([datasource                : datasource, tableIrisName: tableIrisName,
                                   tableBuildIndifName       : tableBuildIndifName,
                                   tableBuildIndusName       : tableBuildIndusName, tableBuildRemarqName: tableBuildRemarqName,
                                   tableRoadName             : tableRoadName, tableRailName: tableRailName,
                                   tableHydroName            : tableHydroName, tableVegetName: tableVegetName,
                                   distBuffer                : distBuffer, expand: expand, idZone: idZone,
                                   building_bd_topo_use_type : initTables.outputBuildingBDTopoUseType,
                                   building_abstract_use_type: abstractTables.outputBuildingAbstractUseType,
                                   road_bd_topo_type         : initTables.outputroadBDTopoType,
                                   road_abstract_type        : abstractTables.outputRoadAbstractType,
                                   rail_bd_topo_type         : initTables.outputrailBDTopoType,
                                   rail_abstract_type        : abstractTables.outputRailAbstractType,
                                   veget_bd_topo_type        : initTables.outputvegetBDTopoType,
                                   veget_abstract_type       : abstractTables.outputVegetAbstractType])) {
                info "Cannot import preprocess."
                return
            }
            def preprocessTables = importPreprocess.getResults()

            // Input data formatting and statistics
            def inputDataFormatting = PrepareData.InputDataFormatting.inputDataFormatting()
            if (!inputDataFormatting([datasource             : datasource,
                                      inputBuilding          : preprocessTables.outputBuildingName, inputRoad: preprocessTables.outputRoadName, inputRail: preprocessTables.outputRailName,
                                      inputHydro             : preprocessTables.outputHydroName, inputVeget: preprocessTables.outputVegetName,
                                      inputZone              : preprocessTables.outputZoneName, inputZoneNeighbors: preprocessTables.outputZoneNeighborsName,
                                      hLevMin                : hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2, idZone: idZone,
                                      buildingAbstractUseType: abstractTables.outputBuildingAbstractUseType, buildingAbstractParameters: abstractTables.outputBuildingAbstractParameters,
                                      roadAbstractType       : abstractTables.outputRoadAbstractType, roadAbstractParameters: abstractTables.outputRoadAbstractParameters,
                                      railAbstractType       : abstractTables.outputRailAbstractType,
                                      vegetAbstractType      : abstractTables.outputVegetAbstractType,
                                      vegetAbstractParameters: abstractTables.outputVegetAbstractParameters])) {
                info "Cannot format data and compute statistics."
                return
            }

            info "End of the BD Topo extract transform process."

            def finalBuildings = inputDataFormatting.getResults().outputBuilding
            def finalRoads = inputDataFormatting.getResults().outputRoad
            def finalRails = inputDataFormatting.getResults().outputRail
            def finalHydro = inputDataFormatting.getResults().outputHydro
            def finalVeget = inputDataFormatting.getResults().outputVeget
            def finalZone = inputDataFormatting.getResults().outputZone

            def finalOutputBuildingStatZone = inputDataFormatting.getResults().outputBuildingStatZone
            def finalOutputBuildingStatZoneBuff = inputDataFormatting.getResults().outputBuildingStatZoneBuff
            def finalOutputRoadStatZone = inputDataFormatting.getResults().outputRoadStatZone
            def finalOutputRoadStatZoneBuff = inputDataFormatting.getResults().outputRoadStatZoneBuff
            def finalOutputRailStatZone = inputDataFormatting.getResults().outputRailStatZone
            def finalOutputHydroStatZone = inputDataFormatting.getResults().outputHydroStatZone
            def finalOutputHydroStatZoneExt = inputDataFormatting.getResults().outputHydroStatZoneExt
            def finalOutputVegetStatZone = inputDataFormatting.getResults().outputVegetStatZone
            def finalOutputVegetStatZoneExt = inputDataFormatting.getResults().outputVegetStatZoneExt

            [outputBuilding: finalBuildings, outputRoad: finalRoads,
             outputRail    : finalRails, outputHydro: finalHydro, outputVeget: finalVeget, outputZone: finalZone,
             outputStats   : [finalOutputBuildingStatZone, finalOutputBuildingStatZoneBuff, finalOutputRoadStatZone,
                              finalOutputRoadStatZoneBuff, finalOutputRailStatZone, finalOutputHydroStatZone, finalOutputHydroStatZoneExt,
                              finalOutputVegetStatZone, finalOutputVegetStatZoneExt]]

        }})
}





























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
    mapper.link(outputBuildingAbstractUseType : abstractTablesInit, buildingAbstractUseType : dataFormatting)
    mapper.link(outputRoadAbstractType : abstractTablesInit, roadAbstractType : dataFormatting)
    mapper.link(outputRailAbstractType : abstractTablesInit, railAbstractType : dataFormatting)
    mapper.link(outputVegetAbstractType : abstractTablesInit, vegetAbstractType : dataFormatting)


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