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
    return processFactory.create("Compute the geoindicators at building scale",
            [datasource : JdbcDataSource,inputBuildingTableName: String,inputRoadTableName : String],
            [outputTableName: String], { datasource, inputBuildingTableName, inputRoadTableName ->

        logger.info("Start computing building indicators...")

        def idColumnBu = "id_build"


        String buildingPrefixName = "building_indicators"

        IProcess computeGeometryProperties = org.orbisgis.GenericIndicators.geometryProperties()
        if(!computeGeometryProperties.execute([inputTableName: inputBuildingTableName, inputFields: ["id_build"], operations: ["st_length", "st_perimeter", "st_area"]
                                           , prefixName  : buildingPrefixName, datasource: datasource])){
            logger.info("Cannot compute the length,perimeter,area properties of the buildings")
            return
        }

        def buildTableGeometryProperties = computeGeometryProperties.results.outputTableName

        IProcess computeSizeProperties = org.orbisgis.BuildingIndicators.sizeProperties()
        if(!computeSizeProperties.execute([inputBuildingTableName: inputBuildingTableName,
                                       operations            : ["building_volume", "building_floor_area", "building_total_facade_length"]
                                       , prefixName          : buildingPrefixName, datasource: datasource])){
            logger.info("Cannot compute the building_volume, building_floor_area, building_total_facade_length indicators for the buildings")
            return
        }

        def buildTableSizeProperties = computeSizeProperties.results.outputTableName


        IProcess computeFormProperties = org.orbisgis.BuildingIndicators.formProperties()
        if(!computeFormProperties.execute([inputBuildingTableName: inputBuildingTableName, operations: ["building_concavity", "building_form_factor",
                                                                                                    "building_raw_compacity", "building_convexhull_perimeter_density"]
                                       , prefixName          : buildingPrefixName, datasource: datasource])){
            logger.info("Cannot compute the building_concavity, building_form_factor, building_raw_compacity, building_convexhull_perimeter_density indicators for the buildings")
            return
        }

        def buildTableFormProperties = computeFormProperties.results.outputTableName


        IProcess computeNeighborsProperties = org.orbisgis.BuildingIndicators.neighborsProperties()
        if(!computeNeighborsProperties.execute([inputBuildingTableName: inputBuildingTableName, operations: ["building_contiguity", "building_common_wall_fraction",
                                                                                                         "building_number_building_neighbor"]
                                            , prefixName          : buildingPrefixName, datasource: datasource])){
            logger.info("Cannot compute the building_contiguity, building_common_wall_fraction,building_number_building_neighbor indicators for the buildings")
            return
        }
        def buildTableComputeNeighborsProperties = computeNeighborsProperties.results.outputTableName

        IProcess computeMinimumBuildingSpacing = org.orbisgis.BuildingIndicators.minimumBuildingSpacing()
        if(!computeMinimumBuildingSpacing.execute([inputBuildingTableName: inputBuildingTableName, bufferDist: 100
                                               , prefixName          : buildingPrefixName, datasource: datasource])){
            logger.info("Cannot compute the minimum building spacing indicator")
            return
        }

        def buildTableComputeMinimumBuildingSpacing = computeMinimumBuildingSpacing.results.outputTableName


        IProcess computeRoadDistance = org.orbisgis.BuildingIndicators.roadDistance()
        if(!computeRoadDistance.execute([inputBuildingTableName: inputBuildingTableName, inputRoadTableName: inputRoadTableName, bufferDist: 100
                                     , prefixName          : buildingPrefixName, datasource: datasource])){
            logger.info("Cannot compute the closest minimum distance to a road at 100 meters. ")
            return
        }

        def buildTableComputeRoadDistance = computeRoadDistance.results.outputTableName


        // Need the number of neighbors in the input Table in order to calculate the following indicator
        IProcess computeJoinNeighbors = org.orbisgis.DataUtils.joinTables()
        if(!computeJoinNeighbors.execute([inputTableNamesWithId: [(buildTableComputeNeighborsProperties)    : idColumnBu,
                                                              (inputBuildingTableName)                  : idColumnBu],
                                            outputTableName           : buildingPrefixName+"_neighbors",
                                            datasource           : datasource])){
            logger.info("Cannot compute number of neighbors of a building. ")
            return
        }

        def buildTableJoinNeighbors = computeJoinNeighbors.results.outputTableName

        IProcess computeLikelihoodLargeBuilding = org.orbisgis.BuildingIndicators.likelihoodLargeBuilding()
        if(!computeLikelihoodLargeBuilding.execute([inputBuildingTableName: buildTableJoinNeighbors,
                                                nbOfBuildNeighbors    : "building_number_building_neighbor",
                                                prefixName            : buildingPrefixName,
                                                datasource            : datasource])){
            logger.info("Cannot compute the like lihood large building indicator. ")
            return
        }

        def buildTableComputeLikelihoodLargeBuilding = computeLikelihoodLargeBuilding.results.outputTableName


        IProcess buildingTableJoin = org.orbisgis.DataUtils.joinTables()
        if(!buildingTableJoin.execute([inputTableNamesWithId: [(buildTableGeometryProperties)            : idColumnBu,
                                                           (buildTableSizeProperties)                : idColumnBu,
                                                           (buildTableFormProperties)                : idColumnBu,
                                                           (buildTableJoinNeighbors)                 : idColumnBu,
                                                           (buildTableComputeMinimumBuildingSpacing) : idColumnBu,
                                                           (buildTableComputeRoadDistance)           : idColumnBu,
                                                           (buildTableComputeLikelihoodLargeBuilding): idColumnBu],
                                   outputTableName           : buildingPrefixName,
                                   datasource           : datasource])){
            logger.info("Cannot merge all indicator in the table $buildingPrefixName.")
            return
        }

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
                                                                              inputBlockTableName: String],
                [outputTableName: String], { datasource, inputBuildingTableName, inputBlockTableName ->

            logger.info("Start computing block indicators...")
            String blockPrefixName = "block_indicators"            
            String id_block = "id_block"

            //Compute :
            //Sum of the building area
            //Sum of the building volume composing the block
            //Sum of block floor area
            IProcess  computeSimpleStats =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
            if(!computeSimpleStats.execute([inputLowerScaleTableName: inputBuildingTableName,inputUpperScaleTableName: inputBlockTableName,
                          inputIdUp: id_block, inputVarAndOperations: ["area":["SUM"],"building_floor_area":["SUM"],
                                                                         "building_volume" :["SUM"]],
                          prefixName: blockPrefixName, datasource: datasource])){
                logger.info("Cannot compute the sum of of the building area, building volume and block floor area.")
                return
            }

            //Ratio between the holes area and the blocks area
            // block_hole_area_density
            IProcess computeHoleAreaDensity = Geoclimate.BlockIndicators.holeAreaDensity()
            if(!computeHoleAreaDensity.execute(blockTable: inputBlockTableName, prefixName: blockPrefixName, datasource: datasource)){
                logger.info("Cannot compute the hole area density.")
                return
            }

            //Perkins SKill Score of the distribution of building direction within a block
            // block_perkins_skill_score_building_direction
            IProcess computePerkinsSkillScoreBuildingDirection = Geoclimate.BlockIndicators.perkinsSkillScoreBuildingDirection()
            if(!computePerkinsSkillScoreBuildingDirection.execute([buildingTableName: inputBuildingTableName, angleRangeSize: 15, prefixName: blockPrefixName, datasource: datasource])){
                logger.info("Cannot compute perkins skill indicator. ")
                return
            }


            //Block closingness
            IProcess computeClosingness = Geoclimate.BlockIndicators.closingness()
            if(!computeClosingness.execute(correlationTableName: inputBuildingTableName, blockTable: inputBlockTableName, prefixName: blockPrefixName, datasource: datasource)){
                logger.info("Cannot compute closingness indicator. ")
                return
            }

            //Block net compacity
            IProcess computeNetCompacity = Geoclimate.BlockIndicators.netCompacity()
            if(!computeNetCompacity.execute([buildTable: inputBuildingTableName, buildingVolumeField: "building_volume", buildingContiguityField: "building_contiguity",
                                         prefixName: blockPrefixName, datasource: datasource])){
                logger.info("Cannot compute the net compacity indicator. ")
                return
            }

            //Block mean building height
            //Block standard deviation building height
            IProcess computeWeightedAggregatedStatistics = Geoclimate.GenericIndicators.weightedAggregatedStatistics()
            if(!computeWeightedAggregatedStatistics.execute([inputLowerScaleTableName: inputBuildingTableName,inputUpperScaleTableName: inputBlockTableName, inputIdUp: id_block,
                                                         inputVarWeightsOperations: ["height_roof": ["area": ["AVG", "STD"]]]
                                                                                     , prefixName: blockPrefixName, datasource: datasource])){
                logger.info("Cannot compute the block mean building height and standard deviation building height indicators. ")
                return
            }

            //Merge all in one table
            IProcess blockTableJoin = org.orbisgis.DataUtils.joinTables()
            if(!blockTableJoin.execute([inputTableNamesWithId: [(inputBlockTableName): id_block,
                                                            (computeSimpleStats.results.outputTableName): id_block,
                                                            (computeHoleAreaDensity.results.outputTableName):id_block ,
                                                            (computePerkinsSkillScoreBuildingDirection.results.outputTableName): id_block,
                                                            (computeClosingness.results.outputTableName):id_block,
                                                            (computeNetCompacity.results.outputTableName):id_block,
                                                            (computeWeightedAggregatedStatistics.results.outputTableName):id_block]
                         , outputTableName: blockPrefixName, datasource: datasource])){
                logger.info("Cannot merge all tables in $blockPrefixName. ")
                return
            }
            [outputTableName: blockTableJoin.results.outputTableName]
        })

    }
    


