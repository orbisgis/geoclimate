package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.Geoclimate
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript ProcessingChain processingChain

/**
 * Compute the geoindicators at building scale
 *
 * @return
 */
public static IProcess computeBuildingsIndicators() {
    return processFactory.create("Compute the geoindicators at building scale", [datasource            : JdbcDataSource,
                                                                                 inputBuildingTableName: String,
                                                                                 inputRoadTableName    : String,
                                                                                 saveResults           : boolean],
            [outputTableName: String], { datasource, inputBuildingTableName, inputRoadTableName, saveResults ->

        def idColumnBu = "id_build"


        String buildingPrefixName = "building_indicators"

        IProcess computeGeometryProperties = org.orbisgis.GenericIndicators.geometryProperties()
        computeGeometryProperties.execute([inputTableName: inputBuildingTableName, inputFields: ["id_build"], operations: ["st_length", "st_perimeter", "st_area"]
                                           , prefixName  : buildingPrefixName, datasource: datasource])

        def buildTableGeometryProperties = computeGeometryProperties.results.outputTableName

        IProcess computeSizeProperties = org.orbisgis.BuildingIndicators.sizeProperties()
        computeSizeProperties.execute([inputBuildingTableName: inputBuildingTableName,
                                       operations            : ["building_volume", "building_floor_area", "building_total_facade_length"]
                                       , prefixName          : buildingPrefixName, datasource: datasource])

        def buildTableSizeProperties = computeSizeProperties.results.outputTableName


        IProcess computeFormProperties = org.orbisgis.BuildingIndicators.formProperties()
        computeFormProperties.execute([inputBuildingTableName: inputBuildingTableName, operations: ["building_concavity", "building_form_factor",
                                                                                                    "building_raw_compacity", "building_convexhull_perimeter_density"]
                                       , prefixName          : buildingPrefixName, datasource: datasource])

        def buildTableFormProperties = computeFormProperties.results.outputTableName


        IProcess computeNeighborsProperties = org.orbisgis.BuildingIndicators.neighborsProperties()
        computeNeighborsProperties.execute([inputBuildingTableName: inputBuildingTableName, operations: ["building_contiguity", "building_common_wall_fraction",
                                                                                                         "building_number_building_neighbor"]
                                            , prefixName          : buildingPrefixName, datasource: datasource])
        def buildTableComputeNeighborsProperties = computeNeighborsProperties.results.outputTableName

        IProcess computeMinimumBuildingSpacing = org.orbisgis.BuildingIndicators.minimumBuildingSpacing()
        computeMinimumBuildingSpacing.execute([inputBuildingTableName: inputBuildingTableName, bufferDist: 100
                                               , prefixName          : buildingPrefixName, datasource: datasource])

        def buildTableComputeMinimumBuildingSpacing = computeMinimumBuildingSpacing.results.outputTableName


        IProcess computeRoadDistance = org.orbisgis.BuildingIndicators.roadDistance()
        computeRoadDistance.execute([inputBuildingTableName: inputBuildingTableName, inputRoadTableName: inputRoadTableName, bufferDist: 100
                                     , prefixName          : buildingPrefixName, datasource: datasource])

        def buildTableComputeRoadDistance = computeRoadDistance.results.outputTableName


        // Need the number of neighbors in the input Table in order to calculate the following indicator
        IProcess computeJoinNeighbors = org.orbisgis.DataUtils.joinTables()
        computeJoinNeighbors.execute([inputTableNamesWithId: [(buildTableComputeNeighborsProperties)    : idColumnBu,
                                                              (inputBuildingTableName)                  : idColumnBu],
                                            prefixName           : buildingPrefixName+"_neighbors",
                                            datasource           : datasource])

        def buildTableJoinNeighbors = computeJoinNeighbors.results.outputTableName

        IProcess computeLikelihoodLargeBuilding = org.orbisgis.BuildingIndicators.likelihoodLargeBuilding()
        computeLikelihoodLargeBuilding.execute([inputBuildingTableName: buildTableJoinNeighbors,
                                                nbOfBuildNeighbors    : "building_number_building_neighbor",
                                                prefixName            : buildingPrefixName,
                                                datasource            : datasource])

        def buildTableComputeLikelihoodLargeBuilding = computeLikelihoodLargeBuilding.results.outputTableName


        IProcess buildingTableJoin = org.orbisgis.DataUtils.joinTables()
        buildingTableJoin.execute([inputTableNamesWithId: [(buildTableGeometryProperties)            : idColumnBu,
                                                           (buildTableSizeProperties)                : idColumnBu,
                                                           (buildTableFormProperties)                : idColumnBu,
                                                           (buildTableJoinNeighbors)                 : idColumnBu,
                                                           (buildTableComputeMinimumBuildingSpacing) : idColumnBu,
                                                           (buildTableComputeRoadDistance)           : idColumnBu,
                                                           (buildTableComputeLikelihoodLargeBuilding): idColumnBu],
                                   prefixName           : buildingPrefixName,
                                   datasource           : datasource])

        [outputTableName: buildingTableJoin.results.outputTableName]

    })
}


    /**
     * Compute the geoindicators at block scale
     *
     * @return
     */
    public static IProcess computeBlockIndicators(){
        return processFactory.create("Compute the geoindicators at block scale", [datasource: JdbcDataSource,
                                                                              inputBuildingTableName: String,
                                                                              inputBlockTableName: String,
                                                                              saveResults : boolean],
                [outputTableName: String], { datasource, inputBuildingTableName, inputBlockTableName, saveResults ->

            String blockPrefixName = "block_indicators"            
            String id_block = "id_block"

            //Compute :
            //Sum of the building area
            //Sum of the building volume composing the block
            //Sum of block floor area
            def  computeSimpleStats =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
            computeSimpleStats.execute([inputLowerScaleTableName: inputBuildingTableName,inputUpperScaleTableName: inputBlockTableName,
                          inputIdUp: id_block, inputVarAndOperations: ["area":["SUM"],"building_floor_area":["SUM"],
                                                                         "building_volume" :["SUM"]],
                          prefixName: blockPrefixName, datasource: datasource])

            //Ratio between the holes area and the blocks area
            // block_hole_area_density
            def computeHoleAreaDensity = Geoclimate.BlockIndicators.holeAreaDensity()
            computeHoleAreaDensity.execute(blockTable: inputBlockTableName, prefixName: blockPrefixName, datasource: datasource)

            //Perkins SKill Score of the distribution of building direction within a block
            // block_perkins_skill_score_building_direction
            def computePerkinsSkillScoreBuildingDirection = Geoclimate.BlockIndicators.perkinsSkillScoreBuildingDirection()
            computePerkinsSkillScoreBuildingDirection.execute([buildingTableName: inputBuildingTableName, angleRangeSize: 15, prefixName: blockPrefixName, datasource: datasource])


            //Block closingness
            IProcess computeClosingness = Geoclimate.BlockIndicators.closingness()
            computeClosingness.execute(correlationTableName: inputBuildingTableName, blockTable: inputBlockTableName, prefixName: blockPrefixName, datasource: datasource)

            //Block net compacity
            IProcess computeNetCompacity = Geoclimate.BlockIndicators.netCompacity()
            computeNetCompacity.execute([buildTable: inputBuildingTableName, buildingVolumeField: "building_volume", buildingContiguityField: "building_contiguity",
                                         prefixName: blockPrefixName, datasource: datasource])

            //Block mean building height
            //Block standard deviation building height
            IProcess computeWeightedAggregatedStatistics = Geoclimate.GenericIndicators.weightedAggregatedStatistics()
            computeWeightedAggregatedStatistics.execute([inputLowerScaleTableName: inputBuildingTableName,inputUpperScaleTableName: inputBlockTableName, inputIdUp: id_block,
                                                         inputVarWeightsOperations: ["height_roof": ["area": ["AVG", "STD"]]]
                                                                                     , prefixName: blockPrefixName, datasource: datasource])

            //Merge all in one table
            IProcess blockTableJoin = org.orbisgis.DataUtils.joinTables()
            blockTableJoin.execute([inputTableNamesWithId: [computeSimpleStats: id_block,
                        computeHoleAreaDensity:id_block ,
                        computePerkinsSkillScoreBuildingDirection: id_block,
                        computeClosingness:id_block,
                        computeNetCompacity:id_block,
                        computeWeightedAggregatedStatistics:id_block]
                         , prefixName: blockPrefixName, datasource: datasource])
            [outputTableName: blockTableJoin]
        })

    }

