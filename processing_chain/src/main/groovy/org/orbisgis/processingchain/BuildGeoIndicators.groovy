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

/**
 * Compute the geoindicators at RSU scale
 *
 * @return
 */
public static IProcess computeRSUIndicators() {
    return processFactory.create("Compute the geoindicators at block scale", [datasource            : JdbcDataSource,
                                                                              inputBuildingTableName: String,
                                                                              inputBlockTableName   : String,
                                                                              inputRSUTableName   : String,
                                                                              inputVegetTableName :String,
                                                                              inputRoadTableName:String,
                                                                              inputWaterTableName:String],
            [outputTableName: String], { datasource, inputBuildingTableName, inputBlockTableName, inputRSUTableName,
                                         inputVegetTableName,inputRoadTableName,inputWaterTableName ->


        logger.info("Start computing RSU indicators...")
        def id_rsu = "id_rsu"

        String rsuPrefixName = "rsu_indicators"

        //rsu_area
        IProcess computeGeometryProperties = Geoclimate.GenericIndicators.geometryProperties()
        if(!computeGeometryProperties.execute([inputTableName: inputRSUTableName, inputFields: ["id_rsu"], operations: ["st_area"]
                                               , prefixName  : rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the area of the RSU")
            return
        }
        def rsuTableGeometryProperties = computeGeometryProperties.results.outputTableName

        //Building free external facade density
        IProcess computeFreeExtDensity = Geoclimate.RsuIndicators.freeExternalFacadeDensity()
        if(!computeFreeExtDensity.execute([buildingTable: inputBuildingTableName,rsuTable: inputRSUTableName,
                                       buContiguityColumn: "building_contiguity", buTotalFacadeLengthColumn: "building_total_facade_length",
                                       prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the free external facade density for the RSU")
            return
        }

        def rsu_free_ext_density = computeFreeExtDensity.results.outputTableName
        
        //rsu_building_density
        //rsu_building_volume_density
        IProcess computeRSUStatistics = Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
        if(!computeRSUStatistics.execute([inputLowerScaleTableName: inputBuildingTableName,inputUpperScaleTableName: inputRSUTableName, inputIdUp: id_rsu,
                               inputVarAndOperations: ["building_volume":["DENS"],
                                                       "area":["DENS"]],
                                   prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the statistics : building density for the RSU")
            return
        }
        def rsuStatistics = computeRSUStatistics.results.outputTableName

        //rsu_road_fraction
        IProcess computeRoadFraction = Geoclimate.RsuIndicators.roadFraction()
        if(!computeRoadFraction.execute([rsuTable: inputRSUTableName, roadTable: inputRoadTableName,
                                     levelToConsiders: ["underground":[-4, -3, -2, -1],
                                      "ground":[0]],
                                     prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of road for the RSU")
            return
        }

        def roadFraction = computeRoadFraction.results.outputTableName

        //rsu_water_fraction
        IProcess computeWaterFraction= Geoclimate.RsuIndicators.waterFraction()
        if(!computeWaterFraction.execute([rsuTable: inputRSUTableName, waterTable: inputWaterTableName,
                                               prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of water for the RSU")
            return
        }

        def waterFraction = computeWaterFraction.results.outputTableName


        //rsu_vegetation_fraction
        IProcess computeVegetationFraction = Geoclimate.RsuIndicators.vegetationFraction()
        if(!computeVegetationFraction.execute([rsuTable: inputRSUTableName, vegetTable: inputVegetTableName, fractionType: ["all"],
                                           prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of all vegetation for the RSU")
            return
        }

        def vegetationFraction = computeVegetationFraction.results.outputTableName


        //rsu_pervious_fraction
        /*IProcess computePerviousnessFraction = Geoclimate.RsuIndicators.perviousnessFraction()
        if(!computePerviousnessFraction.execute([rsuTable: inputRSUTableName, operationsAndComposition: String[],
                                               prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of all vegetation for the RSU")
            return
        }

        def perviousnessFraction = computePerviousnessFraction.getResults().results.outputTableName*/


        //rsu_aspect_ratio

        //IProcess computeAspectRatio = Geoclimate.RsuIndicators.aspectRatio()
        //computeAspectRatio.execute([rsuTable: inputRSUTableName, rsuFreeExternalFacadeDensityColumn: String,
        //                           rsuBuildingDensityColumn: String, prefixName: rsuPrefixName, datasource: datasource])

        //rsu_free_vertical_roof_density The sum of all vertical facades areas located above the building gutter height (wall height) divided by the RSU area.

        //rsu_free_non_vertical_roof_density Building non vertical roof density considering that all buildings have either flat or gable roof


        //rsu_mean_building_neighbor_number

        //rsu_mean_building_height The mean height of the buildings included within a RSU, weighted by their area.

        //rsu_std_building_height -pondéré par la surface

        //rsu_building_number RSU number of buildings

        //rsu_mean_building_volume RSU mean building volume

        //rsu_high_vegetation_fraction

        //rsu_low_vegetation_fraction

        //rsu_linear_road_density


        //rsu_house_floor_area

        //rsu_ground_sky_view_factor

        //rsu_impervious_fraction


        //rsu_roughness_height

        //rsu_terrain_roughness_class

        //rsu_road_direction_distribution

        //rsu_free_vertical_roof_area_distribution

        //rsu_free_non_vertical_roof_area_distribution

        //rsu_effective_terrain_roughness

        //rsu_perkins_skill_score_building_direction_variability

        //rsu_building_floor_density

        //rsu_mean_minimum_building_spacing

        //rsu_projected_facade_area_distribution

        //Merge all in one table
        IProcess rsuTableJoin = org.orbisgis.DataUtils.joinTables()
        if(!rsuTableJoin.execute([inputTableNamesWithId: [(inputRSUTableName): id_rsu,
                                                          (vegetationFraction):id_rsu,
                                                        (rsuTableGeometryProperties): id_rsu,
                                                        (crsu_free_ext_density): id_rsu,
                                                        (rsuStatistics):id_rsu,
                                                          (roadFraction):id_rsu,
                                                          (waterFraction):id_rsu]
                                    , outputTableName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot merge all tables in $rsuPrefixName. ")
            return
        }
        [outputTableName: rsuTableJoin.results.outputTableName]

    })
}
    


