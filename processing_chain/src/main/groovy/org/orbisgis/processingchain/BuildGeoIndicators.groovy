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
 * @param buildingTable The table where are stored informations concerning buildings (and the id of the corresponding rsu)
 * @param rsuTable The table where are stored informations concerning RSU
 * @param roadTable The table where are stored informations concerning roads
 * @param vegetationTable The table where are stored informations concerning vegetation
 * @param hydrographicTable The table where are stored informations concerning water
 * @param facadeDensListLayersBottom the list of height corresponding to the bottom of each vertical layers used for calculation
 * of the rsu_projected_facade_area_density which is then used to calculate the height of roughness (default [0, 10, 20, 30, 40, 50])
 * @param facadeDensNumberOfDirection The number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 12)
 * @param pointDensity The density of points (nb / free m²) used to calculate the spatial average SVF (default 0.008)
 * @param rayLength The maximum distance to consider an obstacle as potential sky cover (default 100)
 * @param numberOfDirection the number of directions considered to calculate the SVF (default 60)
 * @param heightColumnName The name of the column (in the building table) used for roughness height calculation (default "height_roof")
 * @param fractionTypePervious The type of surface that should be consider to calculate the fraction of pervious soil
 * (default ["low_vegetation", "water"] but possible parameters are ["low_vegetation", "high_vegetation", "water"])
 * @param fractionTypeImpervious The type of surface that should be consider to calculate the fraction of impervious soil
 * (default ["road"] but possible parameters are ["road", "building"])
 * @param inputFields The fields of the buildingTable that should be kept in the analysis (default ["the_geom", "id_build"]
 * @param levelForRoads If the road surfaces are considered for the calculation of the impervious fraction,
 * you should give the list of road zindex to consider (default [0])
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @return
 */
public static IProcess computeRSUIndicators() {
    return processFactory.create("Compute the geoindicators at block scale",
            [datasource                 : JdbcDataSource, buildingTable              : String,
             inputBlockTableName        : String,         rsuTable                   : String,
             vegetationTable            : String,         roadTable                  : String,
             hydrographicTable          : String,         facadeDensListLayersBottom : [0, 10, 20, 30, 40, 50],
             facadeDensNumberOfDirection: 12,             svfPointDensity            : 0.008,
             svfRayLength               : 100,            svfNumberOfDirection       : 60,
             heightColumnName           : "height_roof",  fractionTypePervious       : ["low_vegetation", "water"],
             fractionTypeImpervious     : ["road"],       inputFields                : ["id_build", "the_geom"],
             levelForRoads              : [0]],
            [outputTableName: String],
            { datasource, prefixName, buildingTable, rsuTable, roadTable, vegetationTable,
              hydrographicTable, facadeDensListLayersBottom, facadeDensNumberOfDirection,
              svfPointDensity, svfRayLength, svfNumberOfDirection,
              heightColumnName, fractionTypePervious,
              fractionTypeImpervious, inputFields, levelForRoads ->

        logger.info("Start computing RSU indicators...")
        def id_rsu = "id_rsu"

        String rsuPrefixName = "rsu_indicators"

        def finalTablesToJoin = [:]

        finalTablesToJoin.put(rsuTable, id_rsu)

        // rsu_area
        IProcess computeGeometryProperties = Geoclimate.GenericIndicators.geometryProperties()
        if(!computeGeometryProperties.execute([inputTableName: rsuTable, inputFields: ["id_rsu"], operations: ["st_area"]
                                               , prefixName  : rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the area of the RSU")
            return
        }
        def rsuTableGeometryProperties = computeGeometryProperties.results.outputTableName

        finalTablesToJoin.put(rsuTableGeometryProperties, id_rsu)

        // Building free external facade density
        IProcess computeFreeExtDensity = Geoclimate.RsuIndicators.freeExternalFacadeDensity()
        if(!computeFreeExtDensity.execute([buildingTable: buildingTable,rsuTable: rsuTable,
                                       buContiguityColumn: "building_contiguity", buTotalFacadeLengthColumn: "building_total_facade_length",
                                       prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the free external facade density for the RSU")
            return
        }

        def rsu_free_ext_density = computeFreeExtDensity.results.outputTableName

        finalTablesToJoin.put(rsu_free_ext_density, id_rsu)
        
        // rsu_building_density + rsu_building_volume_density + rsu_mean_building_neighbor_number + rsu_building_floor_density
        IProcess computeRSUStatisticsUnweighted = Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
        if(!computeRSUStatisticsUnweighted.execute([inputLowerScaleTableName: buildingTable,inputUpperScaleTableName: rsuTable, inputIdUp: id_rsu,
                               inputVarAndOperations: ["building_volume":["DENS"],
                                                       "area":["DENS"],"building_number_building_neighbor":["AVG"], "building_floor_area":["DENS"]],
                                   prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the statistics : building, building volume densities and mean building neighbor number for the RSU")
            return
        }
        def rsuStatisticsUnweighted = computeRSUStatisticsUnweighted.results.outputTableName

        finalTablesToJoin.put(rsuStatisticsUnweighted, id_rsu)

        // rsu_road_fraction
        IProcess computeRoadFraction = Geoclimate.RsuIndicators.roadFraction()
        if(!computeRoadFraction.execute([rsuTable: rsuTable, roadTable: roadTable,
                                     levelToConsiders: ["ground":[0]],
                                     prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of road for the RSU")
            return
        }

        def roadFraction = computeRoadFraction.results.outputTableName

        finalTablesToJoin.put(roadFraction, id_rsu)

        // rsu_water_fraction
        IProcess computeWaterFraction= Geoclimate.RsuIndicators.waterFraction()
        if(!computeWaterFraction.execute([rsuTable: rsuTable, waterTable: hydrographicTable,
                                               prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of water for the RSU")
            return
        }

        def waterFraction = computeWaterFraction.results.outputTableName

        finalTablesToJoin.put(waterFraction, id_rsu)

        // rsu_vegetation_fraction + rsu_high_vegetation_fraction + rsu_low_vegetation_fraction
        IProcess computeVegetationFraction = Geoclimate.RsuIndicators.vegetationFraction()
        if(!computeVegetationFraction.execute([rsuTable: rsuTable, vegetTable: vegetationTable,
                                               fractionType: ["low", "high","all"],
                                               prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of all vegetation for the RSU")
            return
        }

        def vegetationFraction = computeVegetationFraction.results.outputTableName

        finalTablesToJoin.put(vegetationFraction, id_rsu)


        // rsu_mean_building_height weighted by their area + rsu_std_building_height weighted by their area.
        // + rsu_building_number_density RSU number of buildings weighted by RSU area
        // + rsu_mean_building_volume RSU mean building volume weighted.
        IProcess computeRSUStatisticsWeighted  = Geoclimate.GenericIndicators.weightedAggregatedStatistics()
        if(!computeRSUStatisticsWeighted.execute([inputLowerScaleTableName: buildingTable,inputUpperScaleTableName: rsuTable,
                                                  inputIdUp: id_rsu, inputVarWeightsOperations: ["height_roof" : ["area": ["AVG", "STD"]]
                                                      ,"building_number_building_neighbor" : ["area": ["AVG"]],
                                                  "building_volume" : ["area": ["AVG"]]],
                                                  prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the weighted indicators mean, std height building, building number density and \n\
            mean volume building.")
            return
        }

        def rsuStatisticsWeighted  = computeRSUStatisticsWeighted.results.outputTableName

        finalTablesToJoin.put(rsuStatisticsWeighted, id_rsu)


        // rsu_linear_road_density + rsu_road_direction_distribution
        IProcess computeLinearRoadOperations = Geoclimate.RsuIndicators.linearRoadOperations()
        if(!computeLinearRoadOperations.execute([rsuTable: rsuTable, roadTable: roadTable, operations: ["rsu_road_direction_distribution",
                                              "rsu_linear_road_density"],
                                             prefixName: rsuPrefixName, angleRangeSize: 30,
                                             levelConsiderated: [0], datasource: datasource])){
            logger.info("Cannot compute the linear road density and road direction distribution")
            return
        }
        def linearRoadOperations  = computeLinearRoadOperations.results.outputTableName

        finalTablesToJoin.put(linearRoadOperations, id_rsu)


        // rsu_free_vertical_roof_density + rsu_free_non_vertical_roof_density
        IProcess computeRoofAreaDistribution= Geoclimate.RsuIndicators.roofAreaDistribution()
        if(!computeRoofAreaDistribution.execute([rsuTable: rsuTable, buildingTable: buildingTable, listLayersBottom: [0, 10, 20, 30, 40, 50],
                                             prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the free and non free vertical roof density")
        }

        def roofAreaDistribution  = computeRoofAreaDistribution.results.outputTableName

        finalTablesToJoin.put(roofAreaDistribution, id_rsu)


        // rsu_pervious_fraction
        /*IProcess computePerviousnessFraction = Geoclimate.RsuIndicators.perviousnessFraction()
        if(!computePerviousnessFraction.execute([rsuTable: rsuTable, operationsAndComposition: String[],
                                               prefixName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot compute the fraction of all vegetation for the RSU")
            return
        }

        def perviousnessFraction = computePerviousnessFraction.getResults().results.outputTableName*/


        // rsu_aspect_ratio

        //IProcess computeAspectRatio = Geoclimate.RsuIndicators.aspectRatio()
        //computeAspectRatio.execute([rsuTable: rsuTable, rsuFreeExternalFacadeDensityColumn: String,
        //                           rsuBuildingDensityColumn: String, prefixName: rsuPrefixName, datasource: datasource])




        // rsu_ground_sky_view_factor

        // rsu_impervious_fraction


        // rsu_roughness_height

        // rsu_terrain_roughness_class

        // rsu_free_vertical_roof_area_distribution

        // rsu_free_non_vertical_roof_area_distribution

        // rsu_effective_terrain_roughness

        // rsu_perkins_skill_score_building_direction_variability

        // rsu_mean_minimum_building_spacing

        // rsu_projected_facade_area_distribution

        // Merge all in one table
        IProcess rsuTableJoin = org.orbisgis.DataUtils.joinTables()
        if(!rsuTableJoin.execute([inputTableNamesWithId: finalTablesToJoin
                                    , outputTableName: rsuPrefixName, datasource: datasource])){
            logger.info("Cannot merge all tables in $rsuPrefixName. ")
            return
        }
        [outputTableName: rsuTableJoin.results.outputTableName]

    })
}
    


