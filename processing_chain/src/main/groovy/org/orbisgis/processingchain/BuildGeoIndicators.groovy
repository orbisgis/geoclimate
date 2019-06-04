package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.BuildingIndicators
import org.orbisgis.DataUtils
import org.orbisgis.GenericIndicators
import org.orbisgis.Geoclimate
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript ProcessingChain processingChain

/**
 * Compute the geoindicators at building scale
 *
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 * @return
 */
public static IProcess computeBuildingsIndicators() {
    return processFactory.create("Compute the geoindicators at building scale",
            [datasource : JdbcDataSource,inputBuildingTableName: String,inputRoadTableName : String,
             indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"]],
            [outputTableName: String], { datasource, inputBuildingTableName, inputRoadTableName, indicatorUse ->

        logger.info("Start computing building indicators...")

        def idColumnBu = "id_build"

        // Maps for intermediate or final joins
        def finalTablesToJoin = [:]
        finalTablesToJoin.put(inputBuildingTableName, idColumnBu)

        String buildingPrefixName = "building_indicators"

        // building_area + building_perimeter
        def geometryOperations = ["st_perimeter", "st_area"]
        if (indicatorUse.contains("LCZ") | indicatorUse.contains("TEB")) {geometryOperations = ["st_area"]}
        IProcess computeGeometryProperties = GenericIndicators.geometryProperties()
        if (!computeGeometryProperties.execute([inputTableName: inputBuildingTableName, inputFields: ["id_build"],
                                                operations    : geometryOperations, prefixName: buildingPrefixName,
                                                datasource    : datasource])) {
            logger.info("Cannot compute the length,perimeter,area properties of the buildings")
            return
        }
        def buildTableGeometryProperties = computeGeometryProperties.results.outputTableName
        finalTablesToJoin.put(buildTableGeometryProperties, idColumnBu)

        // For indicators that are useful for urban_typology OR for LCZ classification
        if(indicatorUse.contains("LCZ") | indicatorUse.contains("URBAN_TYPOLOGY")) {
            // building_volume + building_floor_area + building_total_facade_length
            def sizeOperations = ["building_volume", "building_floor_area", "building_total_facade_length"]
            if (!indicatorUse.contains("URBAN_TYPOLOGY")) {
                sizeOperations = ["building_total_facade_length"]
            }
            IProcess computeSizeProperties = BuildingIndicators.sizeProperties()
            if (!computeSizeProperties.execute([inputBuildingTableName: inputBuildingTableName,
                                                operations            : sizeOperations,
                                                prefixName            : buildingPrefixName,
                                                datasource            : datasource])) {
                logger.info("Cannot compute the building_volume, building_floor_area, building_total_facade_length indicators for the buildings")
                return
            }
            def buildTableSizeProperties = computeSizeProperties.results.outputTableName
            finalTablesToJoin.put(buildTableSizeProperties, idColumnBu)

            // building_contiguity + building_common_wall_fraction + building_number_building_neighbor
            def neighborOperations = ["building_contiguity", "building_common_wall_fraction", "building_number_building_neighbor"]
            if (indicatorUse.contains("LCZ") & !indicatorUse.contains("URBAN_TYPOLOGY")) {
                neighborOperations = ["building_contiguity"]
            }
            IProcess computeNeighborsProperties = BuildingIndicators.neighborsProperties()
            if (!computeNeighborsProperties.execute([inputBuildingTableName: inputBuildingTableName,
                                                     operations            : neighborOperations,
                                                     prefixName            : buildingPrefixName,
                                                     datasource            : datasource])) {
                logger.info("Cannot compute the building_contiguity, building_common_wall_fraction,building_number_building_neighbor indicators for the buildings")
                return
            }
            def buildTableComputeNeighborsProperties = computeNeighborsProperties.results.outputTableName
            finalTablesToJoin.put(buildTableComputeNeighborsProperties, idColumnBu)

            if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                // building_concavity + building_form_factor + building_raw_compacity + building_convex_hull_perimeter_density
                IProcess computeFormProperties = BuildingIndicators.formProperties()
                if (!computeFormProperties.execute([inputBuildingTableName: inputBuildingTableName, operations: ["building_concavity", "building_form_factor",
                                                                                                                 "building_raw_compacity", "building_convexhull_perimeter_density"]
                                                    , prefixName          : buildingPrefixName, datasource: datasource])) {
                    logger.info("Cannot compute the building_concavity, building_form_factor, building_raw_compacity, building_convexhull_perimeter_density indicators for the buildings")
                    return
                }
                def buildTableFormProperties = computeFormProperties.results.outputTableName
                finalTablesToJoin.put(buildTableFormProperties, idColumnBu)

                // building_minimum_building_spacing
                IProcess computeMinimumBuildingSpacing = BuildingIndicators.minimumBuildingSpacing()
                if (!computeMinimumBuildingSpacing.execute([inputBuildingTableName: inputBuildingTableName, bufferDist: 100
                                                            , prefixName          : buildingPrefixName, datasource: datasource])) {
                    logger.info("Cannot compute the minimum building spacing indicator")
                    return
                }
                def buildTableComputeMinimumBuildingSpacing = computeMinimumBuildingSpacing.results.outputTableName
                finalTablesToJoin.put(buildTableComputeMinimumBuildingSpacing, idColumnBu)

                // building_road_distance
                IProcess computeRoadDistance = BuildingIndicators.roadDistance()
                if (!computeRoadDistance.execute([inputBuildingTableName: inputBuildingTableName, inputRoadTableName: inputRoadTableName, bufferDist: 100
                                                  , prefixName          : buildingPrefixName, datasource: datasource])) {
                    logger.info("Cannot compute the closest minimum distance to a road at 100 meters. ")
                    return
                }
                def buildTableComputeRoadDistance = computeRoadDistance.results.outputTableName
                finalTablesToJoin.put(buildTableComputeRoadDistance, idColumnBu)

                // Join for building_likelihood
                IProcess computeJoinNeighbors = DataUtils.joinTables()
                if (!computeJoinNeighbors.execute([inputTableNamesWithId: [(buildTableComputeNeighborsProperties): idColumnBu,
                                                                           (inputBuildingTableName)              : idColumnBu],
                                                   outputTableName      : buildingPrefixName + "_neighbors",
                                                   datasource           : datasource])) {
                    logger.info("Cannot join the number of neighbors of a building. ")
                    return
                }
                def buildTableJoinNeighbors = computeJoinNeighbors.results.outputTableName

                // building_likelihood_large_building
                IProcess computeLikelihoodLargeBuilding = BuildingIndicators.likelihoodLargeBuilding()
                if (!computeLikelihoodLargeBuilding.execute([inputBuildingTableName: buildTableJoinNeighbors,
                                                             nbOfBuildNeighbors    : "building_number_building_neighbor",
                                                             prefixName            : buildingPrefixName,
                                                             datasource            : datasource])) {
                    logger.info("Cannot compute the like lihood large building indicator. ")
                    return
                }
                def buildTableComputeLikelihoodLargeBuilding = computeLikelihoodLargeBuilding.results.outputTableName
                finalTablesToJoin.put(buildTableComputeLikelihoodLargeBuilding, idColumnBu)
            }
        }

        IProcess buildingTableJoin = DataUtils.joinTables()
        if(!buildingTableJoin.execute([inputTableNamesWithId: finalTablesToJoin,
                                       outputTableName      : buildingPrefixName,
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
            IProcess computePerkinsSkillScoreBuildingDirection = Geoclimate.GenericIndicators.perkinsSkillScoreBuildingDirection()
            if(!computePerkinsSkillScoreBuildingDirection.execute([buildingTableName: inputBuildingTableName,
                                                                   inputIdUp   : id_block,
                                                                   angleRangeSize: 15,
                                                                   prefixName: blockPrefixName,
                                                                   datasource: datasource])){
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
 * @param angleRangeSizeBuDirection The range size (in °) of each interval angle used to calculate the distribution
 * of building direction (used in the Perkins Skill Score direction - should be a divisor of 180 - default 15°)
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @return
 */
public static IProcess computeRSUIndicators() {
    return processFactory.create("Compute the geoindicators at block scale",
            [datasource                 : JdbcDataSource,   buildingTable               : String,
             rsuTable                   : String,           prefixName                  : "rsu_indicators",
             vegetationTable            : String,           roadTable                   : String,
             hydrographicTable          : String,           facadeDensListLayersBottom  : [0, 10, 20, 30, 40, 50],
             facadeDensNumberOfDirection: 12,               svfPointDensity             : 0.008,
             svfRayLength               : 100,              svfNumberOfDirection        : 60,
             heightColumnName           : "height_roof",    fractionTypePervious        : ["low_vegetation", "water"],
             fractionTypeImpervious     : ["road"],         inputFields                 : ["id_build", "the_geom"],
             levelForRoads              : [0],              angleRangeSizeBuDirection   : 30,
             indicatorUse               : ["LCZ", "URBAN_TYPOLOGY", "TEB"]],
            [outputTableName: String],
            { datasource, buildingTable, rsuTable,
              prefixName, vegetationTable, roadTable,
              hydrographicTable, facadeDensListLayersBottom, facadeDensNumberOfDirection,
              svfPointDensity, svfRayLength, svfNumberOfDirection,
              heightColumnName, fractionTypePervious, fractionTypeImpervious,
              inputFields, levelForRoads, angleRangeSizeBuDirection, indicatorUse ->

                logger.info("Start computing RSU indicators...")
                def to_start = System.currentTimeMillis()

                def columnIdRsu = "id_rsu"

                // Maps for intermediate or final joins
                def finalTablesToJoin = [:]
                def intermediateJoin = [:]
                finalTablesToJoin.put(rsuTable, columnIdRsu)
                intermediateJoin.put(rsuTable, columnIdRsu)

                // rsu_area
                if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                    IProcess computeGeometryProperties = Geoclimate.GenericIndicators.geometryProperties()
                    if (!computeGeometryProperties.execute([inputTableName: rsuTable, inputFields: [columnIdRsu],
                                                            operations    : ["st_area"], prefixName: prefixName,
                                                            datasource    : datasource])) {
                        logger.info("Cannot compute the area of the RSU")
                        return
                    }
                    def rsuTableGeometryProperties = computeGeometryProperties.results.outputTableName
                    finalTablesToJoin.put(rsuTableGeometryProperties, columnIdRsu)
                }


                // Building free external facade density
                if (indicatorUse.contains("URBAN_TYPOLOGY") | indicatorUse.contains("LCZ")) {
                    IProcess computeFreeExtDensity = Geoclimate.RsuIndicators.freeExternalFacadeDensity()
                    if (!computeFreeExtDensity.execute([buildingTable               : buildingTable, rsuTable: rsuTable,
                                                        buContiguityColumn          : "building_contiguity",
                                                        buTotalFacadeLengthColumn   : "building_total_facade_length",
                                                        prefixName                  : prefixName,
                                                        datasource                  : datasource])) {
                        logger.info("Cannot compute the free external facade density for the RSU")
                        return
                    }
                    def rsu_free_ext_density = computeFreeExtDensity.results.outputTableName
                    intermediateJoin.put(rsu_free_ext_density, columnIdRsu)
                }

                // rsu_building_density + rsu_building_volume_density + rsu_mean_building_neighbor_number
                // + rsu_building_floor_density + rsu_roughness_height
                def inputVarAndOperations = [:]
                if (indicatorUse.contains("LCZ")) {
                    inputVarAndOperations = inputVarAndOperations << [(heightColumnName): ["GEOM_AVG"], "area": ["DENS"]]
                }
                if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                    inputVarAndOperations = inputVarAndOperations << ["building_volume"                  : ["DENS"],
                                                                      (heightColumnName)                 : ["GEOM_AVG"],
                                                                      "area"                             : ["DENS"],
                                                                      "building_number_building_neighbor": ["AVG"],
                                                                      "building_floor_area"              : ["DENS"],
                                                                      "building_minimum_building_spacing": ["AVG"]]
                }
                if (indicatorUse.contains("TEB")) {
                    inputVarAndOperations = inputVarAndOperations << ["area": ["DENS"]]
                }
                IProcess computeRSUStatisticsUnweighted = Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
                if (!computeRSUStatisticsUnweighted.execute([inputLowerScaleTableName: buildingTable,
                                                             inputUpperScaleTableName: rsuTable,
                                                             inputIdUp               : columnIdRsu,
                                                             inputVarAndOperations   : inputVarAndOperations,
                                                             prefixName              : prefixName,
                                                             datasource              : datasource])) {
                    logger.info("Cannot compute the statistics : building, building volume densities and mean building neighbor number for the RSU")
                    return
                }
                def rsuStatisticsUnweighted = computeRSUStatisticsUnweighted.results.outputTableName
                // Join in an intermediate table (for perviousness fraction)
                intermediateJoin.put(rsuStatisticsUnweighted, columnIdRsu)


                // rsu_road_fraction
                if (indicatorUse.contains("URBAN_TYPOLOGY") | indicatorUse.contains("LCZ")) {
                    IProcess computeRoadFraction = Geoclimate.RsuIndicators.roadFraction()
                    if (!computeRoadFraction.execute([rsuTable        : rsuTable, roadTable: roadTable,
                                                      levelToConsiders: ["ground": [0]],
                                                      prefixName      : prefixName, datasource: datasource])) {
                        logger.info("Cannot compute the fraction of road for the RSU")
                        return
                    }
                    def roadFraction = computeRoadFraction.results.outputTableName
                    intermediateJoin.put(roadFraction, columnIdRsu)
                }

                // rsu_water_fraction
                if (indicatorUse.contains("URBAN_TYPOLOGY") | indicatorUse.contains("LCZ")) {
                    IProcess computeWaterFraction = Geoclimate.RsuIndicators.waterFraction()
                    if (!computeWaterFraction.execute([rsuTable  : rsuTable, waterTable: hydrographicTable,
                                                       prefixName: prefixName, datasource: datasource])) {
                        logger.info("Cannot compute the fraction of water for the RSU")
                        return
                    }
                    def waterFraction = computeWaterFraction.results.outputTableName
                    // Join in an intermediate table (for perviousness fraction)
                    intermediateJoin.put(waterFraction, columnIdRsu)
                }

                // rsu_vegetation_fraction + rsu_high_vegetation_fraction + rsu_low_vegetation_fraction
                def fractionTypeVeg = ["low", "high", "all"]
                if (!indicatorUse.contains("LCZ") & !indicatorUse.contains("TEB")) {
                    fractionTypeVeg = ["all"]
                }
                IProcess computeVegetationFraction = Geoclimate.RsuIndicators.vegetationFraction()
                if (!computeVegetationFraction.execute([rsuTable    : rsuTable, vegetTable: vegetationTable,
                                                        fractionType: fractionTypeVeg, prefixName: prefixName,
                                                        datasource  : datasource])) {
                    logger.info("Cannot compute the fraction of all vegetation for the RSU")
                    return
                }
                def vegetationFraction = computeVegetationFraction.results.outputTableName
                // Join in an intermediate table
                intermediateJoin.put(vegetationFraction, columnIdRsu)


                // rsu_mean_building_height weighted by their area + rsu_std_building_height weighted by their area.
                // + rsu_building_number_density RSU number of buildings weighted by RSU area
                // + rsu_mean_building_volume RSU mean building volume weighted.
                if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                    IProcess computeRSUStatisticsWeighted = Geoclimate.GenericIndicators.weightedAggregatedStatistics()
                    if (!computeRSUStatisticsWeighted.execute([inputLowerScaleTableName: buildingTable, inputUpperScaleTableName: rsuTable,
                                                               inputIdUp               : columnIdRsu, inputVarWeightsOperations: ["height_roof"                        : ["area": ["AVG", "STD"]]
                                                                                                                                  , "building_number_building_neighbor": ["area": ["AVG"]],
                                                                                                                                  "building_volume"                    : ["area": ["AVG"]]],
                                                               prefixName              : prefixName, datasource: datasource])) {
                        logger.info("Cannot compute the weighted indicators mean, std height building, building number density and \n\
                    mean volume building.")
                        return
                    }
                    def rsuStatisticsWeighted = computeRSUStatisticsWeighted.results.outputTableName
                    finalTablesToJoin.put(rsuStatisticsWeighted, columnIdRsu)
                }

                // rsu_linear_road_density + rsu_road_direction_distribution
                if (indicatorUse.contains("URBAN_TYPOLOGY") | indicatorUse.contains("TEB")){
                    def roadOperations = ["rsu_road_direction_distribution", "rsu_linear_road_density"]
                    if (indicatorUse.contains("URBAN_TYPOLOGY")) {roadOperations=["rsu_road_direction_distribution"]}
                    IProcess computeLinearRoadOperations = Geoclimate.RsuIndicators.linearRoadOperations()
                    if (!computeLinearRoadOperations.execute([rsuTable  : rsuTable, roadTable: roadTable,
                                                              operations: roadOperations, levelConsiderated: [0],
                                                              datasource: datasource, prefixName: prefixName])) {
                        logger.info("Cannot compute the linear road density and road direction distribution")
                        return
                    }
                    def linearRoadOperations = computeLinearRoadOperations.results.outputTableName
                    finalTablesToJoin.put(linearRoadOperations, columnIdRsu)
                }

                // rsu_free_vertical_roof_area_distribution + rsu_free_non_vertical_roof_area_distribution
                if (indicatorUse.contains("TEB")) {
                    IProcess computeRoofAreaDist = Geoclimate.RsuIndicators.roofAreaDistribution()
                    if (!computeRoofAreaDist.execute([rsuTable        : rsuTable, buildingTable: buildingTable,
                                                      listLayersBottom: facadeDensListLayersBottom, prefixName: prefixName,
                                                      datasource      : datasource])) {
                        logger.info("Cannot compute the roof area distribution in $prefixName. ")
                        return
                    }
                    def roofAreaDist = computeRoofAreaDist.results.outputTableName
                    finalTablesToJoin.put(roofAreaDist, columnIdRsu)
                }

                // rsu_projected_facade_area_distribution
                if (indicatorUse.contains("LCZ") | indicatorUse.contains("TEB")) {
                    if(indicatorUse.contains("LCZ") & !indicatorUse.contains("TEB")){
                        facadeDensListLayersBottom: [0, 50, 200]
                        facadeDensNumberOfDirection: 8
                    }
                    IProcess computeProjFacadeDist = Geoclimate.RsuIndicators.projectedFacadeAreaDistribution()
                    if (!computeProjFacadeDist.execute([buildingTable   : buildingTable, rsuTable: rsuTable,
                                                        listLayersBottom: facadeDensListLayersBottom, numberOfDirection: facadeDensNumberOfDirection,
                                                        prefixName      : "test", datasource: datasource])) {
                        logger.info("Cannot compute the projected facade distribution in $prefixName. ")
                        return
                    }
                    def projFacadeDist = computeProjFacadeDist.results.outputTableName
                    intermediateJoin.put(projFacadeDist, columnIdRsu)
                }

                // Create an intermediate join tables to have all needed input fields for future indicator calculation
                IProcess computeIntermediateJoin = Geoclimate.DataUtils.joinTables()
                if(!computeIntermediateJoin.execute([inputTableNamesWithId: intermediateJoin,
                                                     outputTableName      : "tab4aspratio",
                                                     datasource           : datasource])){
                    logger.info("Cannot merge the tables used for aspect ratio calculation in $prefixName. ")
                    return
                }
                def intermediateJoinTable = computeIntermediateJoin.results.outputTableName
                finalTablesToJoin.put(intermediateJoinTable, columnIdRsu)


                // rsu_aspect_ratio
                if (indicatorUse.contains("LCZ")) {
                    IProcess computeAspectRatio = Geoclimate.RsuIndicators.aspectRatio()
                    if (!computeAspectRatio.execute([rsuTable                          : intermediateJoinTable,
                                                     rsuFreeExternalFacadeDensityColumn: "rsu_free_external_facade_density",
                                                     rsuBuildingDensityColumn          : "dens_area",
                                                     prefixName                        : prefixName,
                                                     datasource                        : datasource])) {
                        logger.info("Cannot compute the aspect ratio calculation in $prefixName. ")
                        return
                    }
                    def aspectRatio = computeAspectRatio.results.outputTableName
                    finalTablesToJoin.put(aspectRatio, columnIdRsu)
                }

                // rsu_ground_sky_view_factor
                if (indicatorUse.contains("LCZ")) {
                    IProcess computeSVF = Geoclimate.RsuIndicators.groundSkyViewFactor()
                    if (!computeSVF.execute([rsuTable                : intermediateJoinTable, correlationBuildingTable: buildingTable,
                                             rsuBuildingDensityColumn: "dens_area", pointDensity: svfPointDensity,
                                             rayLength               : svfRayLength, numberOfDirection: svfNumberOfDirection,
                                             prefixName              : prefixName, datasource: datasource])) {
                        logger.info("Cannot compute the SVF calculation in $prefixName. ")
                        return
                    }
                    def SVF = computeSVF.results.outputTableName
                    finalTablesToJoin.put(SVF, columnIdRsu)
                }

                // rsu_pervious_fraction + rsu_impervious_fraction
                if (indicatorUse.contains("LCZ")) {
                    List perv_type = fractionTypePervious.collect { "${it}_fraction" }
                    List imp_type = fractionTypeImpervious.collect {
                        if (it == "building") {
                            "area_dens"
                        } else if (it == "road") {
                            "ground_${it}_fraction"
                        } else {
                            "${it}_fraction"
                        }
                    }
                    IProcess computePerviousnessFraction = Geoclimate.RsuIndicators.perviousnessFraction()
                    if (!computePerviousnessFraction.execute([rsuTable                : intermediateJoinTable,
                                                              operationsAndComposition: ["pervious_fraction"  : perv_type,
                                                                                         "impervious_fraction": imp_type],
                                                              prefixName              : prefixName,
                                                              datasource              : datasource])) {
                        logger.info("Cannot compute the perviousness fraction for the RSU")
                        return
                    }
                    def perviousnessFraction = computePerviousnessFraction.results.outputTableName
                    finalTablesToJoin.put(perviousnessFraction, columnIdRsu)
                }

                // rsu_effective_terrain_roughness
                if (indicatorUse.contains("LCZ")) {
                    // Create the join tables to have all needed input fields for aspect ratio computation
                    IProcess computeEffRoughHeight = Geoclimate.RsuIndicators.effectiveTerrainRoughnessHeight()
                    if (!computeEffRoughHeight.execute([rsuTable                       : intermediateJoinTable,
                                                        projectedFacadeAreaName        : "rsu_projected_facade_area_distribution",
                                                        geometricMeanBuildingHeightName: "geom_avg_$heightColumnName",
                                                        prefixName                     : prefixName,
                                                        listLayersBottom               : facadeDensListLayersBottom,
                                                        numberOfDirection              : facadeDensNumberOfDirection,
                                                        datasource                     : datasource])) {
                        logger.info("Cannot compute the SVF calculation in $prefixName. ")
                        return
                    }
                    def effRoughHeight = computeEffRoughHeight.results.outputTableName
                    finalTablesToJoin.put(effRoughHeight, columnIdRsu)

                     // rsu_terrain_roughness_class
                    IProcess computeRoughClass = Geoclimate.RsuIndicators.effectiveTerrainRoughnessClass()
                    if (!computeRoughClass.execute([datasource                     : datasource,
                                                    rsuTable                       : effRoughHeight,
                                                    effectiveTerrainRoughnessHeight: "rsu_effective_terrain_roughness",
                                                    prefixName                     : prefixName])) {
                        logger.info("Cannot compute the SVF calculation in $prefixName. ")
                        return
                    }
                    def roughClass = computeRoughClass.results.outputTableName
                    finalTablesToJoin.put(roughClass, columnIdRsu)
                }

                // rsu_perkins_skill_score_building_direction_variability
                if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                    IProcess computePerkinsDirection = Geoclimate.GenericIndicators.perkinsSkillScoreBuildingDirection()
                    if (!computePerkinsDirection.execute([buildingTableName: buildingTable, inputIdUp: columnIdRsu,
                                                          angleRangeSize   : angleRangeSizeBuDirection, prefixName: prefixName,
                                                          datasource       : datasource])) {
                        logger.info("Cannot compute the perkins Skill Score building direction distribution in $prefixName. ")
                        return
                    }
                    def perkinsDirection = computePerkinsDirection.results.outputTableName
                    finalTablesToJoin.put(perkinsDirection, columnIdRsu)
                }

                // Merge all in one table
                // To avoid duplicate the_geom in the join table, remove it from the intermediate table
                datasource.execute("ALTER TABLE $intermediateJoinTable DROP COLUMN the_geom;")
                IProcess rsuTableJoin = DataUtils.joinTables()
                if(!rsuTableJoin.execute([inputTableNamesWithId: finalTablesToJoin,
                                          outputTableName      : prefixName,
                                          datasource           : datasource])){
                    logger.info("Cannot merge all tables in $prefixName. ")
                    return
                }
                def tObis = System.currentTimeMillis()-to_start

                logger.info("Geoindicators calculation time: ${tObis/1000} s")
                [outputTableName: rsuTableJoin.results.outputTableName]

    })
}
    


