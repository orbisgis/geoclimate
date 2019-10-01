package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.geoindicators.DataUtils
import org.orbisgis.geoindicators.Geoindicators
import org.orbisgis.datamanager.JdbcDataSource

@BaseScript ProcessingChain processingChain

/**
 * Compute the geoindicators at building scale
 *
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 * 
 * @return
 */
def computeBuildingsIndicators() {
    return create({
        title "Compute the geoindicators at building scale"
        inputs datasource: JdbcDataSource, inputBuildingTableName: String, inputRoadTableName: String,
                indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"]
        outputs outputTableName: String
        run { datasource, inputBuildingTableName, inputRoadTableName, indicatorUse ->

            info "Start computing building indicators..."

            def idColumnBu = "id_build"

            // Maps for intermediate or final joins
            def finalTablesToJoin = [:]
            finalTablesToJoin.put(inputBuildingTableName, idColumnBu)

            def buildingPrefixName = "building_indicators"

            // building_area + building_perimeter
            def geometryOperations = ["st_perimeter", "st_area"]
            if (indicatorUse.contains("LCZ") || indicatorUse.contains("TEB")) {
                geometryOperations = ["st_area"]
            }
            def computeGeometryProperties = Geoindicators.GenericIndicators.geometryProperties()
            if (!computeGeometryProperties([inputTableName: inputBuildingTableName, inputFields: ["id_build"],
                                            operations    : geometryOperations, prefixName: buildingPrefixName,
                                            datasource    : datasource])) {
                info "Cannot compute the length,perimeter,area properties of the buildings"
                return
            }
            def buildTableGeometryProperties = computeGeometryProperties.results.outputTableName
            finalTablesToJoin.put(buildTableGeometryProperties, idColumnBu)

            // For indicators that are useful for urban_typology OR for LCZ classification
            if (indicatorUse.contains("LCZ") || indicatorUse.contains("URBAN_TYPOLOGY")) {
                // building_volume + building_floor_area + building_total_facade_length
                def sizeOperations = ["building_volume", "building_floor_area", "building_total_facade_length"]
                if (!indicatorUse.contains("URBAN_TYPOLOGY")) {
                    sizeOperations = ["building_total_facade_length"]
                }
                def computeSizeProperties = Geoindicators.BuildingIndicators.sizeProperties()
                if (!computeSizeProperties([inputBuildingTableName: inputBuildingTableName,
                                            operations            : sizeOperations,
                                            prefixName            : buildingPrefixName,
                                            datasource            : datasource])) {
                    info "Cannot compute the building_volume, building_floor_area, building_total_facade_length " +
                            "indicators for the buildings"
                    return
                }
                def buildTableSizeProperties = computeSizeProperties.results.outputTableName
                finalTablesToJoin.put(buildTableSizeProperties, idColumnBu)

                // building_contiguity + building_common_wall_fraction + building_number_building_neighbor
                def neighborOperations = ["building_contiguity", "building_common_wall_fraction", "building_number_building_neighbor"]
                if (indicatorUse.contains("LCZ") && !indicatorUse.contains("URBAN_TYPOLOGY")) {
                    neighborOperations = ["building_contiguity"]
                }
                def computeNeighborsProperties = Geoindicators.BuildingIndicators.neighborsProperties()
                if (!computeNeighborsProperties([inputBuildingTableName: inputBuildingTableName,
                                                 operations            : neighborOperations,
                                                 prefixName            : buildingPrefixName,
                                                 datasource            : datasource])) {
                    info "Cannot compute the building_contiguity, building_common_wall_fraction, " +
                            "building_number_building_neighbor indicators for the buildings"
                    return
                }
                def buildTableComputeNeighborsProperties = computeNeighborsProperties.results.outputTableName
                finalTablesToJoin.put(buildTableComputeNeighborsProperties, idColumnBu)

                if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                    // building_concavity + building_form_factor + building_raw_compacity + building_convex_hull_perimeter_density
                    def computeFormProperties = Geoindicators.BuildingIndicators.formProperties()
                    if (!computeFormProperties([inputBuildingTableName: inputBuildingTableName,
                                                operations            : ["building_concavity", "building_form_factor",
                                                                         "building_raw_compacity",
                                                                         "building_convexhull_perimeter_density"],
                                                prefixName            : buildingPrefixName,
                                                datasource            : datasource])) {
                        info "Cannot compute the building_concavity, building_form_factor, building_raw_compacity, " +
                                "building_convexhull_perimeter_density indicators for the buildings"
                        return
                    }
                    def buildTableFormProperties = computeFormProperties.results.outputTableName
                    finalTablesToJoin.put(buildTableFormProperties, idColumnBu)

                    // building_minimum_building_spacing
                    def computeMinimumBuildingSpacing = Geoindicators.BuildingIndicators.minimumBuildingSpacing()
                    if (!computeMinimumBuildingSpacing([inputBuildingTableName: inputBuildingTableName,
                                                        bufferDist            : 100,
                                                        prefixName            : buildingPrefixName, datasource: datasource])) {
                        info "Cannot compute the minimum building spacing indicator"
                        return
                    }
                    def buildTableComputeMinimumBuildingSpacing = computeMinimumBuildingSpacing.results.outputTableName
                    finalTablesToJoin.put(buildTableComputeMinimumBuildingSpacing, idColumnBu)

                    // building_road_distance
                    def computeRoadDistance = Geoindicators.BuildingIndicators.roadDistance()
                    if (!computeRoadDistance([inputBuildingTableName: inputBuildingTableName,
                                              inputRoadTableName    : inputRoadTableName,
                                              bufferDist            : 100,
                                              prefixName            : buildingPrefixName,
                                              datasource            : datasource])) {
                        info "Cannot compute the closest minimum distance to a road at 100 meters."
                        return
                    }
                    def buildTableComputeRoadDistance = computeRoadDistance.results.outputTableName
                    finalTablesToJoin.put(buildTableComputeRoadDistance, idColumnBu)

                    // Join for building_likelihood
                    def computeJoinNeighbors = Geoindicators.DataUtils.joinTables()
                    if (!computeJoinNeighbors([inputTableNamesWithId: [(buildTableComputeNeighborsProperties): idColumnBu,
                                                                       (inputBuildingTableName)              : idColumnBu],
                                               outputTableName      : buildingPrefixName + "_neighbors",
                                               datasource           : datasource])) {
                        info "Cannot join the number of neighbors of a building."
                        return
                    }
                    def buildTableJoinNeighbors = computeJoinNeighbors.results.outputTableName

                    // building_likelihood_large_building
                    def computeLikelihoodLargeBuilding = Geoindicators.BuildingIndicators.likelihoodLargeBuilding()
                    if (!computeLikelihoodLargeBuilding([inputBuildingTableName: buildTableJoinNeighbors,
                                                         nbOfBuildNeighbors    : "building_number_building_neighbor",
                                                         prefixName            : buildingPrefixName,
                                                         datasource            : datasource])) {
                        info "Cannot compute the like lihood large building indicator."
                        return
                    }
                    def buildTableComputeLikelihoodLargeBuilding = computeLikelihoodLargeBuilding.results.outputTableName
                    finalTablesToJoin.put(buildTableComputeLikelihoodLargeBuilding, idColumnBu)
                }
            }

            def buildingTableJoin = Geoindicators.DataUtils.joinTables()
            if (!buildingTableJoin([inputTableNamesWithId: finalTablesToJoin,
                                    outputTableName      : buildingPrefixName,
                                    datasource           : datasource])) {
                info "Cannot merge all indicator in the table $buildingPrefixName."
                return
            }

            [outputTableName: buildingTableJoin.results.outputTableName]

        }
    })
}


/**
 * Compute the geoindicators at block scale
 *
 * @return
 */
def computeBlockIndicators(){
    return create({
        title "Compute the geoindicators at block scale"
        inputs datasource: JdbcDataSource, inputBuildingTableName: String, inputBlockTableName: String
        outputs outputTableName: String
        run { datasource, inputBuildingTableName, inputBlockTableName ->

            info "Start computing block indicators..."
            def blockPrefixName = "block_indicators"
            def id_block = "id_block"

            // Maps for intermediate or final joins
            def finalTablesToJoin = [:]
            finalTablesToJoin.put(inputBlockTableName, id_block)

            //Compute :
            //Sum of the building area
            //Sum of the building volume composing the block
            //Sum of block floor area
            def computeSimpleStats = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
            if(!computeSimpleStats([inputLowerScaleTableName: inputBuildingTableName,
                                    inputUpperScaleTableName: inputBlockTableName,
                                    inputIdUp               : id_block,
                                    inputVarAndOperations   : ["area"               :["SUM"],
                                                               "building_floor_area":["SUM"],
                                                               "building_volume" :["SUM"]],
                                    prefixName: blockPrefixName,
                                    datasource: datasource])){
                info "Cannot compute the sum of of the building area, building volume and block floor area."
                return
            }

            finalTablesToJoin.put(computeSimpleStats.results.outputTableName, id_block)

            //Ratio between the holes area and the blocks area
            // block_hole_area_density
            def computeHoleAreaDensity = Geoindicators.BlockIndicators.holeAreaDensity()
            if(!computeHoleAreaDensity(blockTable: inputBlockTableName,
                                       prefixName: blockPrefixName,
                                       datasource: datasource)){
                info "Cannot compute the hole area density."
                return
            }
            finalTablesToJoin.put(computeHoleAreaDensity.results.outputTableName, id_block)

            //Perkins SKill Score of the distribution of building direction within a block
            // block_perkins_skill_score_building_direction
            def computePerkinsSkillScoreBuildingDirection = Geoindicators.GenericIndicators.perkinsSkillScoreBuildingDirection()
            if(!computePerkinsSkillScoreBuildingDirection([buildingTableName: inputBuildingTableName,
                                                           inputIdUp        : id_block,
                                                           angleRangeSize   : 15,
                                                           prefixName       : blockPrefixName,
                                                           datasource       : datasource])) {
                info "Cannot compute perkins skill indicator. "
                return
            }
            finalTablesToJoin.put(computePerkinsSkillScoreBuildingDirection.results.outputTableName, id_block)


            //Block closingness
            def computeClosingness = Geoindicators.BlockIndicators.closingness()
            if(!computeClosingness(correlationTableName: inputBuildingTableName,
                                   blockTable          : inputBlockTableName,
                                   prefixName          : blockPrefixName,
                                   datasource          : datasource)){
                info "Cannot compute closingness indicator. "
                return
            }
            finalTablesToJoin.put(computeClosingness.results.outputTableName, id_block)

            //Block net compacity
            def computeNetCompacity = Geoindicators.BlockIndicators.netCompacity()
            if(!computeNetCompacity([buildTable             : inputBuildingTableName,
                                     buildingVolumeField    : "building_volume",
                                     buildingContiguityField: "building_contiguity",
                                     prefixName             : blockPrefixName,
                                     datasource             : datasource])){
                info "Cannot compute the net compacity indicator. "
                return
            }
            finalTablesToJoin.put(computeNetCompacity.results.outputTableName, id_block)

            //Block mean building height
            //Block standard deviation building height
            def computeWeightedAggregatedStatistics = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
            if(!computeWeightedAggregatedStatistics([inputLowerScaleTableName : inputBuildingTableName,
                                                     inputUpperScaleTableName : inputBlockTableName,
                                                     inputIdUp                : id_block,
                                                     inputVarWeightsOperations: ["height_roof": ["area": ["AVG", "STD"]]],
                                                     prefixName               : blockPrefixName,
                                                     datasource               : datasource])){
                info "Cannot compute the block mean building height and standard deviation building height indicators. "
                return
            }
            finalTablesToJoin.put(computeWeightedAggregatedStatistics.results.outputTableName, id_block)

            //Merge all in one table
            def blockTableJoin = Geoindicators.DataUtils.joinTables()
            if(!blockTableJoin([inputTableNamesWithId: finalTablesToJoin,
                                outputTableName: blockPrefixName,
                                datasource: datasource])){
                info "Cannot merge all tables in $blockPrefixName. "
                return
            }
            [outputTableName: blockTableJoin.results.outputTableName]
        }
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
 * @param svfSimplified A boolean to use a simplified version of the SVF calculation (default false)
 * @param datasource A connection to a database
 *
 * @return
 */
def computeRSUIndicators() {
    return create({
        title "Compute the geoindicators at RSU scale"
        inputs  datasource                 : JdbcDataSource,   buildingTable               : String,
                rsuTable                   : String,           prefixName                  : "rsu_indicators",
                vegetationTable            : String,           roadTable                   : String,
                hydrographicTable          : String,           facadeDensListLayersBottom  : [0, 10, 20, 30, 40, 50],
                facadeDensNumberOfDirection: 12,               svfPointDensity             : 0.008,
                svfRayLength               : 100,              svfNumberOfDirection        : 60,
                heightColumnName           : "height_roof",    fractionTypePervious        : ["low_vegetation", "water"],
                fractionTypeImpervious     : ["road"],         inputFields                 : ["id_build", "the_geom"],
                levelForRoads              : [0],              angleRangeSizeBuDirection   : 30,
                svfSimplified              : false,            indicatorUse                : ["LCZ", "URBAN_TYPOLOGY", "TEB"]
        outputs outputTableName: String
        run { datasource            , buildingTable                     , rsuTable,
              prefixName            , vegetationTable                   , roadTable,
              hydrographicTable     , facadeDensListLayersBottom        , facadeDensNumberOfDirection,
              svfPointDensity       , svfRayLength                      , svfNumberOfDirection,
              heightColumnName      , fractionTypePervious              , fractionTypeImpervious,
              inputFields           , levelForRoads                     , angleRangeSizeBuDirection,
              svfSimplified         , indicatorUse ->

            info "Start computing RSU indicators..."
            def to_start = System.currentTimeMillis()

            def columnIdRsu = "id_rsu"

            // Maps for intermediate or final joins
            def finalTablesToJoin = [:]
            def intermediateJoin = [:]
            finalTablesToJoin.put(rsuTable, columnIdRsu)
            intermediateJoin.put(rsuTable, columnIdRsu)

            // rsu_area
            if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                def computeGeometryProperties = Geoindicators.GenericIndicators.geometryProperties()
                if (!computeGeometryProperties([inputTableName: rsuTable, inputFields: [columnIdRsu],
                                                operations    : ["st_area"], prefixName: prefixName,
                                                datasource    : datasource])) {
                    info "Cannot compute the area of the RSU"
                    return
                }
                def rsuTableGeometryProperties = computeGeometryProperties.results.outputTableName
                finalTablesToJoin.put(rsuTableGeometryProperties, columnIdRsu)
            }


            // Building free external facade density
            if (indicatorUse.contains("URBAN_TYPOLOGY") || indicatorUse.contains("LCZ")) {
                def computeFreeExtDensity = Geoindicators.RsuIndicators.freeExternalFacadeDensity()
                if (!computeFreeExtDensity([buildingTable            : buildingTable, rsuTable: rsuTable,
                                            buContiguityColumn       : "building_contiguity",
                                            buTotalFacadeLengthColumn: "building_total_facade_length",
                                            prefixName               : prefixName,
                                            datasource               : datasource])) {
                    info "Cannot compute the free external facade density for the RSU"
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
            def computeRSUStatisticsUnweighted = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
            if (!computeRSUStatisticsUnweighted([inputLowerScaleTableName: buildingTable,
                                                 inputUpperScaleTableName: rsuTable,
                                                 inputIdUp               : columnIdRsu,
                                                 inputVarAndOperations   : inputVarAndOperations,
                                                 prefixName              : prefixName,
                                                 datasource              : datasource])) {
                info "Cannot compute the statistics : building, building volume densities and mean building neighbor number for the RSU"
                return
            }
            def rsuStatisticsUnweighted = computeRSUStatisticsUnweighted.results.outputTableName
            // Join in an intermediate table (for perviousness fraction)
            intermediateJoin.put(rsuStatisticsUnweighted, columnIdRsu)


            // rsu_road_fraction
            if (indicatorUse.contains("URBAN_TYPOLOGY") || indicatorUse.contains("LCZ")) {
                def computeRoadFraction = Geoindicators.RsuIndicators.roadFraction()
                if (!computeRoadFraction([rsuTable        : rsuTable,
                                          roadTable       : roadTable,
                                          levelToConsiders: ["ground": [0]],
                                          prefixName      : prefixName,
                                          datasource      : datasource])) {
                    info "Cannot compute the fraction of road for the RSU"
                    return
                }
                def roadFraction = computeRoadFraction.results.outputTableName
                intermediateJoin.put(roadFraction, columnIdRsu)
            }

            // rsu_water_fraction
            if (indicatorUse.contains("URBAN_TYPOLOGY") || indicatorUse.contains("LCZ")) {
                def computeWaterFraction = Geoindicators.RsuIndicators.waterFraction()
                if (!computeWaterFraction([rsuTable  : rsuTable,
                                           waterTable: hydrographicTable,
                                           prefixName: prefixName,
                                           datasource: datasource])) {
                    info "Cannot compute the fraction of water for the RSU"
                    return
                }
                def waterFraction = computeWaterFraction.results.outputTableName
                // Join in an intermediate table (for perviousness fraction)
                intermediateJoin.put(waterFraction, columnIdRsu)
            }

            // rsu_vegetation_fraction + rsu_high_vegetation_fraction + rsu_low_vegetation_fraction
            def fractionTypeVeg = ["low", "high", "all"]
            if (!indicatorUse.contains("LCZ") && !indicatorUse.contains("TEB")) {
                fractionTypeVeg = ["all"]
            }
            def computeVegetationFraction = Geoindicators.RsuIndicators.vegetationFraction()
            if (!computeVegetationFraction([rsuTable    : rsuTable,
                                            vegetTable  : vegetationTable,
                                            fractionType: fractionTypeVeg,
                                            prefixName  : prefixName,
                                            datasource  : datasource])) {
                info "Cannot compute the fraction of all vegetation for the RSU"
                return
            }
            def vegetationFraction = computeVegetationFraction.results.outputTableName
            // Join in an intermediate table
            intermediateJoin.put(vegetationFraction, columnIdRsu)


            // rsu_mean_building_height weighted by their area + rsu_std_building_height weighted by their area.
            // + rsu_building_number_density RSU number of buildings weighted by RSU area
            // + rsu_mean_building_volume RSU mean building volume weighted.
            if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                def computeRSUStatisticsWeighted = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
                if (!computeRSUStatisticsWeighted([inputLowerScaleTableName : buildingTable,
                                                   inputUpperScaleTableName : rsuTable,
                                                   inputIdUp                : columnIdRsu,
                                                   inputVarWeightsOperations: ["height_roof"                      : ["area": ["AVG", "STD"]],
                                                                               "building_number_building_neighbor": ["area": ["AVG"]],
                                                                               "building_volume"                  : ["area": ["AVG"]]],
                                                   prefixName               : prefixName,
                                                   datasource               : datasource])) {
                    info "Cannot compute the weighted indicators mean, std height building, building number density and \n\
                    mean volume building."
                    return
                }
                def rsuStatisticsWeighted = computeRSUStatisticsWeighted.results.outputTableName
                finalTablesToJoin.put(rsuStatisticsWeighted, columnIdRsu)
            }

            // rsu_linear_road_density + rsu_road_direction_distribution
            if (indicatorUse.contains("URBAN_TYPOLOGY") || indicatorUse.contains("TEB")) {
                def roadOperations = ["rsu_road_direction_distribution", "rsu_linear_road_density"]
                if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                    roadOperations = ["rsu_road_direction_distribution"]
                }
                def computeLinearRoadOperations = Geoindicators.RsuIndicators.linearRoadOperations()
                if (!computeLinearRoadOperations([rsuTable         : rsuTable,
                                                  roadTable        : roadTable,
                                                  operations       : roadOperations,
                                                  levelConsiderated: [0],
                                                  datasource       : datasource,
                                                  prefixName       : prefixName])) {
                    info "Cannot compute the linear road density and road direction distribution"
                    return
                }
                def linearRoadOperations = computeLinearRoadOperations.results.outputTableName
                finalTablesToJoin.put(linearRoadOperations, columnIdRsu)
            }

            // rsu_free_vertical_roof_area_distribution + rsu_free_non_vertical_roof_area_distribution
            if (indicatorUse.contains("TEB")) {
                def computeRoofAreaDist = Geoindicators.RsuIndicators.roofAreaDistribution()
                if (!computeRoofAreaDist([rsuTable        : rsuTable,
                                          buildingTable   : buildingTable,
                                          listLayersBottom: facadeDensListLayersBottom,
                                          prefixName      : prefixName,
                                          datasource      : datasource])) {
                    info "Cannot compute the roof area distribution in $prefixName. "
                    return
                }
                def roofAreaDist = computeRoofAreaDist.results.outputTableName
                finalTablesToJoin.put(roofAreaDist, columnIdRsu)
            }

            // rsu_projected_facade_area_distribution
            if (indicatorUse.contains("LCZ") || indicatorUse.contains("TEB")) {
                if (!indicatorUse.contains("TEB")) {
                    facadeDensListLayersBottom:
                    [0, 50, 200]
                    facadeDensNumberOfDirection: 8
                }
                def computeProjFacadeDist = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
                if (!computeProjFacadeDist([buildingTable    : buildingTable,
                                            rsuTable         : rsuTable,
                                            listLayersBottom : facadeDensListLayersBottom,
                                            numberOfDirection: facadeDensNumberOfDirection,
                                            prefixName       : "test",
                                            datasource       : datasource])) {
                    info "Cannot compute the projected facade distribution in $prefixName. "
                    return
                }
                def projFacadeDist = computeProjFacadeDist.results.outputTableName
                intermediateJoin.put(projFacadeDist, columnIdRsu)
            }

            // Create an intermediate join tables to have all needed input fields for future indicator calculation
            def computeIntermediateJoin = Geoindicators.DataUtils.joinTables()
            if (!computeIntermediateJoin([inputTableNamesWithId: intermediateJoin,
                                          outputTableName      : "tab4aspratio",
                                          datasource           : datasource])) {
                info "Cannot merge the tables used for aspect ratio calculation in $prefixName. "
                return
            }
            def intermediateJoinTable = computeIntermediateJoin.results.outputTableName
            finalTablesToJoin.put(intermediateJoinTable, columnIdRsu)


            // rsu_aspect_ratio
            if (indicatorUse.contains("LCZ")) {
                def computeAspectRatio = Geoindicators.RsuIndicators.aspectRatio()
                if (!computeAspectRatio([rsuTable                          : intermediateJoinTable,
                                         rsuFreeExternalFacadeDensityColumn: "rsu_free_external_facade_density",
                                         rsuBuildingDensityColumn          : "dens_area",
                                         prefixName                        : prefixName,
                                         datasource                        : datasource])) {
                    info "Cannot compute the aspect ratio calculation in $prefixName. "
                    return
                }
                def aspectRatio = computeAspectRatio.results.outputTableName
                finalTablesToJoin.put(aspectRatio, columnIdRsu)
            }

            // rsu_ground_sky_view_factor
            if (indicatorUse.contains("LCZ")) {
                def SVF = "SVF"
                // If the fast version is chosen (SVF derived from extended RSU free facade fraction
                if (svfSimplified == true) {
                    def computeExtFF =  Geoindicators.RsuIndicators.extendedFreeFacadeFraction()
                    if (!computeExtFF([buildingTable: buildingTable,
                                          rsuTable: intermediateJoinTable,
                                          buTotalFacadeLengthColumn: "building_total_facade_length",
                                          prefixName: prefixName, buffDist : 10, datasource: datasource])){
                        info "Cannot compute the SVF calculation in $prefixName. "
                        return
                        }
                    datasource.execute "DROP TABLE IF EXISTS $SVF; CREATE TABLE SVF " +
                            "AS SELECT 1-rsu_extended_free_facade_fraction AS RSU_GROUND_SKY_VIEW_FACTOR, $columnIdRsu " +
                            "FROM ${computeExtFF.results.outputTableName}"
                    }
                else {
                    def computeSVF = Geoindicators.RsuIndicators.groundSkyViewFactor()
                    if (!computeSVF([rsuTable                : intermediateJoinTable,
                                     correlationBuildingTable: buildingTable,
                                     pointDensity            : svfPointDensity,
                                     rayLength               : svfRayLength,
                                     numberOfDirection       : svfNumberOfDirection,
                                     prefixName              : prefixName,
                                     datasource              : datasource])) {
                        info "Cannot compute the SVF calculation in $prefixName. "
                        return
                        }
                    SVF = computeSVF.results.outputTableName
                    }
                finalTablesToJoin.put(SVF, columnIdRsu)
            }

            // rsu_pervious_fraction + rsu_impervious_fraction
            if (indicatorUse.contains("LCZ")) {
                def perv_type = fractionTypePervious.collect { "${it}_fraction" }
                def imp_type = fractionTypeImpervious.collect {
                    if (it == "building") {
                        "area_dens"
                    } else if (it == "road") {
                        "ground_${it}_fraction"
                    } else {
                        "${it}_fraction"
                    }
                }
                def computePerviousnessFraction = Geoindicators.RsuIndicators.perviousnessFraction()
                if (!computePerviousnessFraction([rsuTable                : intermediateJoinTable,
                                                  operationsAndComposition: ["pervious_fraction"  : perv_type,
                                                                             "impervious_fraction": imp_type],
                                                  prefixName              : prefixName,
                                                  datasource              : datasource])) {
                    info "Cannot compute the perviousness fraction for the RSU"
                    return
                }
                def perviousnessFraction = computePerviousnessFraction.results.outputTableName
                finalTablesToJoin.put(perviousnessFraction, columnIdRsu)
            }

            // rsu_effective_terrain_roughness
            if (indicatorUse.contains("LCZ")) {
                // Create the join tables to have all needed input fields for aspect ratio computation
                def computeEffRoughHeight = Geoindicators.RsuIndicators.effectiveTerrainRoughnessHeight()
                if (!computeEffRoughHeight([rsuTable                       : intermediateJoinTable,
                                            projectedFacadeAreaName        : "rsu_projected_facade_area_distribution",
                                            geometricMeanBuildingHeightName: "geom_avg_$heightColumnName",
                                            prefixName                     : prefixName,
                                            listLayersBottom               : facadeDensListLayersBottom,
                                            numberOfDirection              : facadeDensNumberOfDirection,
                                            datasource                     : datasource])) {
                    info "Cannot compute the SVF calculation in $prefixName."
                    return
                }
                def effRoughHeight = computeEffRoughHeight.results.outputTableName
                finalTablesToJoin.put(effRoughHeight, columnIdRsu)

                // rsu_terrain_roughness_class
                def computeRoughClass = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass()
                if (!computeRoughClass([datasource                     : datasource,
                                        rsuTable                       : effRoughHeight,
                                        effectiveTerrainRoughnessHeight: "rsu_effective_terrain_roughness",
                                        prefixName                     : prefixName])) {
                    info "Cannot compute the SVF calculation in $prefixName."
                    return
                }
                def roughClass = computeRoughClass.results.outputTableName
                finalTablesToJoin.put(roughClass, columnIdRsu)
            }

            // rsu_perkins_skill_score_building_direction_variability
            if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                 def computePerkinsDirection = Geoindicators.GenericIndicators.perkinsSkillScoreBuildingDirection()
                if (!computePerkinsDirection([buildingTableName: buildingTable, inputIdUp: columnIdRsu,
                                              angleRangeSize   : angleRangeSizeBuDirection, prefixName: prefixName,
                                              datasource       : datasource])) {
                    info "Cannot compute the perkins Skill Score building direction distribution in $prefixName."
                    return
                }
                def perkinsDirection = computePerkinsDirection.results.outputTableName
                finalTablesToJoin.put(perkinsDirection, columnIdRsu)
            }

            // Merge all in one table
            // To avoid duplicate the_geom in the join table, remove it from the intermediate table
            datasource.execute("ALTER TABLE $intermediateJoinTable DROP COLUMN the_geom;")
            def rsuTableJoin = Geoindicators.DataUtils.joinTables()
            if (!rsuTableJoin([inputTableNamesWithId: finalTablesToJoin,
                               outputTableName      : prefixName,
                               datasource           : datasource])) {
                info "Cannot merge all tables in $prefixName. "
                return
            }
            def tObis = System.currentTimeMillis() - to_start

            info "Geoindicators calculation time: ${tObis / 1000} s"
            [outputTableName: rsuTableJoin.results.outputTableName]

        }
    })
}