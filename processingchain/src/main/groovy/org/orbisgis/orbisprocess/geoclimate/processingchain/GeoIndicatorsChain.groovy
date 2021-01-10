package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.transform.BaseScript
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators

@BaseScript ProcessingChain pf

/**
 * Compute the geoindicators at building scale
 *
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 *
 * @return
 */
IProcess computeBuildingsIndicators() {
    return create {
        title "Compute the geoindicators at building scale"
        id "computeBuildingsIndicators"
        inputs datasource: JdbcDataSource, inputBuildingTableName: String, inputRoadTableName: String,
                indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], prefixName: ""
        outputs outputTableName: String
        run { datasource, inputBuildingTableName, inputRoadTableName, indicatorUse, prefixName ->

            info "Start computing building indicators..."

            def idColumnBu = "id_build"
            def BASE_NAME = "building_indicators"

            // Maps for intermediate or final joins
            def finalTablesToJoin = [:]
            finalTablesToJoin.put(inputBuildingTableName, idColumnBu)

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASE_NAME
            def buildingPrefixName = "building_indicator_"
            def buildTableJoinNeighbors = postfix "A"

            // building_area + building_perimeter
            def geometryOperations = ["st_area"]
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                geometryOperations = ["st_perimeter", "st_area"]
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
            if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                // building_volume + building_floor_area + building_total_facade_length
                def sizeOperations = ["volume", "floor_area", "total_facade_length"]
                if (!indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                    sizeOperations = ["total_facade_length"]
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
                def neighborOperations = ["contiguity", "common_wall_fraction", "number_building_neighbor"]
                if (indicatorUse*.toUpperCase().contains("LCZ") && !indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                    neighborOperations = ["contiguity"]
                }
                def computeNeighborsProperties = Geoindicators.BuildingIndicators.neighborsProperties()
                if (!computeNeighborsProperties([inputBuildingTableName: inputBuildingTableName,
                                                 operations            : neighborOperations,
                                                 prefixName            : buildingPrefixName,
                                                 datasource            : datasource])) {
                    info "Cannot compute the building_contiguity, building_common_wall_fraction, " +
                            "number_building_neighbor indicators for the buildings"
                    return
                }
                def buildTableComputeNeighborsProperties = computeNeighborsProperties.results.outputTableName
                finalTablesToJoin.put(buildTableComputeNeighborsProperties, idColumnBu)

                if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                    // area_concavity + building_form_factor + building_raw_compactness + perimeter_convexity
                    def computeFormProperties = Geoindicators.BuildingIndicators.formProperties()
                    if (!computeFormProperties([inputBuildingTableName: inputBuildingTableName,
                                                operations            : ["area_concavity", "form_factor",
                                                                         "raw_compactness",
                                                                         "perimeter_convexity"],
                                                prefixName            : buildingPrefixName,
                                                datasource            : datasource])) {
                        info "Cannot compute the area_concavity, form_factor, raw_compactness, " +
                                "perimeter_convexity indicators for the buildings"
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
                    buildTableJoinNeighbors = computeJoinNeighbors.results.outputTableName

                    // building_likelihood_large_building
                    def computeLikelihoodLargeBuilding = Geoindicators.BuildingIndicators.likelihoodLargeBuilding()
                    if (!computeLikelihoodLargeBuilding([inputBuildingTableName: buildTableJoinNeighbors,
                                                         nbOfBuildNeighbors    : "number_building_neighbor",
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

            // Rename the last table to the right output table name
            datasource.execute "DROP TABLE IF EXISTS $outputTableName;" +
                    "ALTER TABLE ${buildingTableJoin.results.outputTableName} RENAME TO $outputTableName"

            // Remove all intermediate tables (indicators alone in one table)
            // Recover all created tables in an array
            def finTabNames = finalTablesToJoin.keySet().toArray()
            // Remove the block table from the list of "tables to remove" (since it needs to be conserved)
            finTabNames = finTabNames - inputBuildingTableName
            datasource.execute """DROP TABLE IF EXISTS ${finTabNames.join(",")}, $buildTableJoinNeighbors"""

            [outputTableName: outputTableName]

        }
    }
}

/**
 * Compute the geoindicators at block scale
 *
 * @return
 */
IProcess computeBlockIndicators() {
    return create {
        title "Compute the geoindicators at block scale"
        id "computeBlockIndicators"
        inputs datasource: JdbcDataSource, inputBuildingTableName: String, inputBlockTableName: String, prefixName: ""
        outputs outputTableName: String
        run { datasource, inputBuildingTableName, inputBlockTableName, prefixName ->

            def BASE_NAME = "block_indicators"

            info "Start computing block indicators..."
            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASE_NAME
            def blockPrefixName = "block_indicator_"
            def id_block = "id_block"
            def id_build = "id_build"

            // Maps for intermediate or final joins
            def finalTablesToJoin = [:]
            finalTablesToJoin.put(inputBlockTableName, id_block)

            //Compute :
            //Sum of the building area
            //Sum of the building volume composing the block
            //Sum of block floor area
            def computeSimpleStats = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
            if (!computeSimpleStats([inputLowerScaleTableName: inputBuildingTableName,
                                     inputUpperScaleTableName: inputBlockTableName,
                                     inputIdUp               : id_block,
                                     inputIdLow              : id_build,
                                     inputVarAndOperations   : ["area"      : ["SUM"],
                                                                "floor_area": ["SUM"],
                                                                "volume"    : ["SUM"]],
                                     prefixName              : blockPrefixName,
                                     datasource              : datasource])) {
                info "Cannot compute the sum of of the building area, building volume and block floor area."
                return
            }

            finalTablesToJoin.put(computeSimpleStats.results.outputTableName, id_block)

            //Ratio between the holes area and the blocks area
            // block_hole_area_density
            def computeHoleAreaDensity = Geoindicators.BlockIndicators.holeAreaDensity()
            if (!computeHoleAreaDensity(blockTable: inputBlockTableName,
                    prefixName: blockPrefixName,
                    datasource: datasource)) {
                info "Cannot compute the hole area density."
                return
            }
            finalTablesToJoin.put(computeHoleAreaDensity.results.outputTableName, id_block)

            //Perkins SKill Score of the distribution of building direction within a block
            // block_perkins_skill_score_building_direction
            def computePerkinsSkillScoreBuildingDirection = Geoindicators.GenericIndicators.buildingDirectionDistribution()
            if (!computePerkinsSkillScoreBuildingDirection([buildingTableName: inputBuildingTableName,
                                                            tableUp          : inputBlockTableName,
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
            if (!computeClosingness(correlationTableName: inputBuildingTableName,
                    blockTable: inputBlockTableName,
                    prefixName: blockPrefixName,
                    datasource: datasource)) {
                info "Cannot compute closingness indicator. "
                return
            }
            finalTablesToJoin.put(computeClosingness.results.outputTableName, id_block)

            //Block net compactness
            def computeNetCompactness = Geoindicators.BlockIndicators.netCompactness()
            if (!computeNetCompactness([buildTable             : inputBuildingTableName,
                                        buildingVolumeField    : "volume",
                                        buildingContiguityField: "contiguity",
                                        prefixName             : blockPrefixName,
                                        datasource             : datasource])) {
                info "Cannot compute the net compactness indicator. "
                return
            }
            finalTablesToJoin.put(computeNetCompactness.results.outputTableName, id_block)

            //Block mean building height
            //Block standard deviation building height
            def computeWeightedAggregatedStatistics = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
            if (!computeWeightedAggregatedStatistics([inputLowerScaleTableName : inputBuildingTableName,
                                                      inputUpperScaleTableName : inputBlockTableName,
                                                      inputIdUp                : id_block,
                                                      inputVarWeightsOperations: ["height_roof": ["area": ["AVG", "STD"]]],
                                                      prefixName               : blockPrefixName,
                                                      datasource               : datasource])) {
                info "Cannot compute the block mean building height and standard deviation building height indicators. "
                return
            }
            finalTablesToJoin.put(computeWeightedAggregatedStatistics.results.outputTableName, id_block)

            //Merge all in one table
            def blockTableJoin = Geoindicators.DataUtils.joinTables()
            if (!blockTableJoin([inputTableNamesWithId: finalTablesToJoin,
                                 outputTableName      : blockPrefixName,
                                 datasource           : datasource])) {
                info "Cannot merge all tables in $blockPrefixName. "
                return
            }

            // Rename the last table to the right output table name
            datasource.execute "DROP TABLE IF EXISTS $outputTableName;" +
                    "ALTER TABLE ${blockTableJoin.results.outputTableName} RENAME TO $outputTableName"

            // Modify all indicators which do not have the expected name
            def listColumnNames = datasource.getTable(outputTableName).columns
            def mapIndic2Change = ["SUM_AREA"  : "AREA", "SUM_FLOOR_AREA": "FLOOR_AREA",
                                   "SUM_VOLUME": "VOLUME"]
            def query2ModifyNames = ""
            for (ind in mapIndic2Change.keySet()) {
                if (listColumnNames.contains(ind)) {
                    query2ModifyNames += "ALTER TABLE $outputTableName RENAME COLUMN $ind TO ${mapIndic2Change[ind]};"
                }
            }
            if (query2ModifyNames != "") {
                datasource.execute query2ModifyNames
            }

            // Remove all intermediate tables (indicators alone in one table)
            // Recover all created tables in an array
            def finTabNames = finalTablesToJoin.keySet().toArray()
            // Remove the block table from the list of "tables to remove" (since it needs to be conserved)
            finTabNames = finTabNames - inputBlockTableName
            datasource.execute """DROP TABLE IF EXISTS ${finTabNames.join(",")}"""

            [outputTableName: outputTableName]
        }
    }
}

/**
 * Compute the geoindicators at RSU scale
 *
 * @param buildingTable The table where are stored informations concerning buildings (and the id of the corresponding rsu)
 * @param rsuTable The table where are stored informations concerning RSU
 * @param roadTable The table where are stored informations concerning roads
 * @param vegetationTable The table where are stored informations concerning vegetation
 * @param hydrographicTable The table where are stored informations concerning water
 * @param imperviousTable The table where are stored the impervious areas
 * @param facadeDensListLayersBottom the list of height corresponding to the bottom of each vertical layers used for calculation
 * of the rsu_projected_facade_area_density which is then used to calculate the height of roughness (default [0, 10, 20, 30, 40, 50])
 * @param facadeDensNumberOfDirection The number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 12)
 * @param pointDensity The density of points (nb / free m²) used to calculate the spatial average SVF (default 0.008)
 * @param rayLength The maximum distance to consider an obstacle as potential sky cover (default 100)
 * @param numberOfDirection the number of directions considered to calculate the SVF (default 60)
 * @param heightColumnName The name of the column (in the building table) used for roughness length calculation (default "height_roof")
 * @param fractionTypePervious The type of surface that should be consider to calculate the fraction of pervious soil
 * (default ["low_vegetation", "water"] but possible parameters are ["low_vegetation", "high_vegetation", "water"])
 * @param fractionTypeImpervious The type of surface that should be consider to calculate the fraction of impervious soil
 * (default ["road"] but possible parameters are ["road", "building"])
 * @param inputFields The fields of the buildingTable that should be kept in the analysis (default ["the_geom", "id_build"]
 * @param levelForRoads If the road surfaces are considered for the calculation of the impervious fraction,
 * you should give the list of road zindex to consider (default [0])
 * @param angleRangeSizeBuDirection The range size (in °) of each interval angle used to calculate the distribution
 * of building direction (used in the Perkins Skill Score direction - should be a divisor of 180 - default 15°)
 * @param prefixName A prefix used to name the output table
 * @param svfSimplified A boolean to use a simplified version of the SVF calculation (default false)
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 * @param surfSuperpositions Map where are stored the overlaying layers as keys and the overlapped
 * layers as values. Note that the priority order for the overlapped layers is taken according to the priority variable
 * name and (default ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]])
 * @param surfPriorities List indicating the priority order to set between layers in order to remove potential double count
 * of overlapped layers (for example a geometry containing water and low_vegetation must be either water
 * or either low_vegetation, not both (default ["water", "building", "high_vegetation", "low_vegetation",
 * "road", "impervious"]
 * @param buildingAreaTypeAndComposition Building type proportion that should be calculated (default: ["light_industry": ["light_industry"],
 *                                                                                                      "heavy_industry": ["heavy_industry"],
*                                                                                                       "commercial":["commercial"],
 *                                                                                                      "residential": ["residential"]])
 * @param floorAreaTypeAndComposition Building floor area type proportion that should be calculated (default: ["residential": ["residential"]])
 * @param urbanTypoSurfFraction Map containing as key the name of the fraction indicators useful for the urban typology classification
 * and as value a list of the fractions that have to be summed up to calculate the indicator. No need to modify
 * these values if not interested by the urban typology
 * @param lczSurfFraction Map containing as key the name of the fraction indicators useful for the LCZ classification
 * and as value a list of the fractions that have to be summed up to calculate the indicator. No need to modify
 * these values if not interested by the lcz classification.
 * @param buildingFractions List of fractions to sum to calculate the total building fraction which is useful as input of the aspect ratio
 * @param datasource A connection to a database
 *
 * @return
 */
IProcess computeRSUIndicators() {
    return create {
        title "Compute the geoindicators at RSU scale"
        id "computeRSUIndicators"
        inputs  datasource                      : JdbcDataSource,   buildingTable               : "",
                rsuTable                        : "",               prefixName                  : "",
                vegetationTable                 : "",               roadTable                   : "",
                hydrographicTable               : "",               imperviousTable             : "",
                facadeDensListLayersBottom      : [0, 10, 20, 30, 40, 50],
                facadeDensNumberOfDirection     : 12,               svfPointDensity             : 0.008,
                svfRayLength                    : 100,              svfNumberOfDirection        : 60,
                heightColumnName                : "height_roof",
                inputFields                     : ["id_build", "the_geom"],
                levelForRoads                   : [0],              angleRangeSizeBuDirection   : 30,
                svfSimplified                   : false,
                indicatorUse                    : ["LCZ", "URBAN_TYPOLOGY", "TEB"],
                surfSuperpositions              : ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]],
                surfPriorities                  : ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"],
                buildingAreaTypeAndComposition  : ["light_industry": ["light_industry"],
                                                   "heavy_industry": ["heavy_industry"],
                                                   "commercial":["commercial"],
                                                   "residential": ["residential"]],
                floorAreaTypeAndComposition     : ["residential": ["residential"]],
                urbanTypoSurfFraction           : ["vegetation_fraction_urb"                 : ["high_vegetation_fraction",
                                                                                                "low_vegetation_fraction",
                                                                                                "high_vegetation_low_vegetation_fraction",
                                                                                                "high_vegetation_road_fraction",
                                                                                                "high_vegetation_impervious_fraction",
                                                                                                "high_vegetation_water_fraction",
                                                                                                "high_vegetation_building_fraction"],
                                                   "low_vegetation_fraction_urb"                  : ["low_vegetation_fraction"],
                                                   "high_vegetation_impervious_fraction_urb"  : ["high_vegetation_road_fraction",
                                                                                                 "high_vegetation_impervious_fraction"],
                                                   "high_vegetation_pervious_fraction_urb"    : ["high_vegetation_fraction",
                                                                                                 "high_vegetation_low_vegetation_fraction",
                                                                                                 "high_vegetation_water_fraction"],
                                                   "road_fraction_urb"                        : ["road_fraction",
                                                                                                 "high_vegetation_road_fraction"],
                                                   "impervious_fraction_urb"                  : ["road_fraction",
                                                                                                 "high_vegetation_road_fraction",
                                                                                                 "impervious_fraction",
                                                                                                 "high_vegetation_impervious_fraction"]],
                lczSurfFraction             : ["building_fraction_lcz"                  : ["building_fraction",
                                                                                           "high_vegetation_building_fraction"],
                                               "pervious_fraction_lcz"                   : ["high_vegetation_fraction",
                                                                                            "low_vegetation_fraction",
                                                                                            "water_fraction",
                                                                                            "high_vegetation_low_vegetation_fraction",
                                                                                            "high_vegetation_water_fraction"],
                                               "high_vegetation_fraction_lcz"            : ["high_vegetation_fraction",
                                                                                            "high_vegetation_low_vegetation_fraction",
                                                                                            "high_vegetation_road_fraction",
                                                                                            "high_vegetation_impervious_fraction",
                                                                                            "high_vegetation_water_fraction",
                                                                                            "high_vegetation_building_fraction"],
                                               "low_vegetation_fraction_lcz"             : ["low_vegetation_fraction"],
                                               "impervious_fraction_lcz"                 : ["impervious_fraction",
                                                                                            "road_fraction",
                                                                                            "high_vegetation_impervious_fraction",
                                                                                            "high_vegetation_road_fraction"],
                                               "water_fraction_lcz"                      : ["water_fraction",
                                                                                            "high_vegetation_water_fraction"]],
                buildingFractions          : ["high_vegetation_building_fraction","building_fraction"]
        outputs outputTableName: String
        run { datasource                , buildingTable                     , rsuTable,
              prefixName                , vegetationTable                   , roadTable,
              hydrographicTable         , imperviousTable,
              facadeDensListLayersBottom, facadeDensNumberOfDirection,
              svfPointDensity           , svfRayLength                      , svfNumberOfDirection,
              heightColumnName          , inputFields                       , levelForRoads,
              angleRangeSizeBuDirection , svfSimplified                     , indicatorUse,
              surfSuperpositions        , surfPriorities                    , buildingAreaTypeAndComposition,
              floorAreaTypeAndComposition,
              urbanTypoSurfFraction     , lczSurfFraction                   , buildingFractions ->

            info "Start computing RSU indicators..."
            def to_start = System.currentTimeMillis()

            def columnIdRsu = "id_rsu"
            def columnIdBuild = "id_build"
            def BASE_NAME = "rsu_indicators"

            // Maps for intermediate or final joins
            def finalTablesToJoin = [:]
            def intermediateJoin = [:]
            finalTablesToJoin.put(rsuTable, columnIdRsu)
            intermediateJoin.put(rsuTable, columnIdRsu)

            // Name of the output table
            def outputTableName = prefix prefixName, BASE_NAME

            // PrefixName for intermediate table (start with a letter to avoid table name issue if start with a number)
            def temporaryPrefName = "rsu_indicator"

            // Other temporary tables that have to be deleted at the end of the process
            def urbanTypoFractionIndic = "urban_typo_fraction_indic"
            def lczFractionIndic = "lcz_fraction_indic"
            def preAspectRatioTable = "pre_HW_table"

            // Intermediate table that needs to be delete at the end
            def SVF = "SVF"
            def computeExtFF

            //Re-use the surface fraction table if it has been already calculated
            //Useful when the building height is estimated.
            def surfaceFractions = getCachedTableName("GEOCLIMATE_TABLE_RSU_SURFACE_FRACTIONS")
            if(!surfaceFractions){
            // Calculate all surface fractions indicators
            // Need to create the smallest geometries used as input of the surface fraction process
            def  computeSmallestGeom =  Geoindicators.RsuIndicators.smallestCommunGeometry()
            if (!computeSmallestGeom.execute([
                    rsuTable: rsuTable,buildingTable: buildingTable,roadTable : roadTable, vegetationTable: vegetationTable,waterTable: hydrographicTable,
                    imperviousTable:imperviousTable,
                    prefixName: temporaryPrefName, datasource: datasource])){
                info "Cannot compute the smallest commun geometries"
                return
            }
            def superpositionsTable = computeSmallestGeom.results.outputTableName
            // Calculate the surface fractions from the commun geom
            def  computeSurfaceFractions =  Geoindicators.RsuIndicators.surfaceFractions()
            if (!computeSurfaceFractions.execute([
                    rsuTable: rsuTable, spatialRelationsTable: superpositionsTable,
                    superpositions: surfSuperpositions,
                    priorities: surfPriorities,
                    prefixName: temporaryPrefName, datasource: datasource])){
                info "Cannot compute the surface fractions"
                return
            }
                surfaceFractions = computeSurfaceFractions.results.outputTableName
            }
            finalTablesToJoin.put(surfaceFractions, columnIdRsu)

            // Get all column names from the surfaceFraction IProcess to make verifications
            def surfFracList = datasource.getTable(surfaceFractions).getColumns()

            // Calculate the surface fractions needed for the urban typology classification
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                info """Processing urban typology surface fraction calculation"""
                // Get all columns needed for the calculations and verify that they exist
                def neededSurfUrb = urbanTypoSurfFraction.findResults { k, v -> true ? v : null }.flatten()
                def missingElementsUrb = neededSurfUrb - neededSurfUrb.findAll { indUrb -> surfFracList.contains(indUrb.toUpperCase()) }
                if (missingElementsUrb.size() == 0) {
                    def queryUrbSurfFrac = """DROP TABLE IF EXISTS $urbanTypoFractionIndic;
                                        CREATE TABLE $urbanTypoFractionIndic AS SELECT $columnIdRsu, """
                    urbanTypoSurfFraction.each { urbIndicator, indicatorList ->
                        queryUrbSurfFrac += "${indicatorList.join("+")} AS $urbIndicator, "
                    }
                    queryUrbSurfFrac += " FROM $surfaceFractions"
                    datasource.execute queryUrbSurfFrac
                    finalTablesToJoin.put(urbanTypoFractionIndic, columnIdRsu)
                } else {
                    error """'urbanTypoSurfFraction' and 'surfSuperpositions' parameters given by the user are not consistent.
                                Impossible to find the following indicators in the surface fractions table: ${missingElementsUrb.join(", ")}"""
                }
            }


            // Calculate the surface fractions needed for the LCZ classification
            if (indicatorUse*.toUpperCase().contains("LCZ")) {
                info """Processing LCZ surface fraction indicators calculation"""
                // Get all columns needed for the calculations and verify that they exist
                def neededSurfLcz = urbanTypoSurfFraction.findResults { k, v -> true ? v : null }.flatten()
                def missingElementsLcz = neededSurfLcz - neededSurfLcz.findAll { indLcz -> surfFracList.contains(indLcz.toUpperCase()) }
                if (missingElementsLcz.size() == 0) {
                    def querylczSurfFrac = """DROP TABLE IF EXISTS $lczFractionIndic;
                                            CREATE TABLE $lczFractionIndic AS SELECT $columnIdRsu, """
                    lczSurfFraction.each { urbIndicator, indicatorList ->
                        querylczSurfFrac += "${indicatorList.join("+")} AS $urbIndicator, "
                    }
                    querylczSurfFrac += " FROM $surfaceFractions"
                    datasource.execute querylczSurfFrac
                    finalTablesToJoin.put(lczFractionIndic, columnIdRsu)
                } else {
                    error """'lczSurfFraction' and 'surfSuperpositions' parameters given by the user are not consistent.
                                Impossible to find the following indicators in the surface fractions table: ${missingElementsLcz.join(", ")}"""
                }
            }

            // building type fractions
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY") || indicatorUse*.toUpperCase().contains("LCZ")) {
                def computeTypeProportion = Geoindicators.GenericIndicators.typeProportion()
                if (!computeTypeProportion([
                        inputTableName                  : buildingTable,
                        idField                     : columnIdRsu,
                        inputUpperTableName         : rsuTable,
                        typeFieldName               : "type",
                        areaTypeAndComposition      : buildingAreaTypeAndComposition,
                        floorAreaTypeAndComposition : floorAreaTypeAndComposition,
                        prefixName                  : temporaryPrefName,
                        datasource                  : datasource])) {
                    info "Cannot compute the building type proportion of the RSU"
                    return
                }
                def rsuTableTypeProportion = computeTypeProportion.results.outputTableName
                finalTablesToJoin.put(rsuTableTypeProportion, columnIdRsu)
            }

            // rsu_area (note that the uuid is used as prefix for intermediate tables - indicator alone in a table)
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                def computeGeometryProperties = Geoindicators.GenericIndicators.geometryProperties()
                if (!computeGeometryProperties([inputTableName: rsuTable, inputFields: [columnIdRsu],
                                                operations    : ["st_area"], prefixName: temporaryPrefName,
                                                datasource    : datasource])) {
                    info "Cannot compute the area of the RSU"
                    return
                }
                def rsuTableGeometryProperties = computeGeometryProperties.results.outputTableName
                finalTablesToJoin.put(rsuTableGeometryProperties, columnIdRsu)
            }


            // Building free external facade density
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY") || indicatorUse*.toUpperCase().contains("LCZ")) {
                def computeFreeExtDensity = Geoindicators.RsuIndicators.freeExternalFacadeDensity()
                if (!computeFreeExtDensity([buildingTable            : buildingTable, rsuTable: rsuTable,
                                            buContiguityColumn       : "contiguity",
                                            buTotalFacadeLengthColumn: "total_facade_length",
                                            prefixName               : temporaryPrefName,
                                            datasource               : datasource])) {
                    info "Cannot compute the free external facade density for the RSU"
                    return
                }
                def rsu_free_ext_density = computeFreeExtDensity.results.outputTableName
                intermediateJoin.put(rsu_free_ext_density, columnIdRsu)
            }

            // rsu_building_density + rsu_building_volume_density + rsu_mean_building_volume
            // + rsu_mean_building_neighbor_number + rsu_building_floor_density + rsu_roughness_length
            // + rsu_building_number_density (RSU number of buildings divided RSU area)
            def inputVarAndOperations = [:]
            if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("TEB")) {
                inputVarAndOperations = inputVarAndOperations << [(heightColumnName): ["GEOM_AVG"]]
            }
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                inputVarAndOperations = inputVarAndOperations << ["volume"                  : ["DENS", "AVG"],
                                                                  (heightColumnName)        : ["GEOM_AVG"],
                                                                  "number_building_neighbor": ["AVG"],
                                                                  "floor_area"              : ["DENS"],
                                                                  "minimum_building_spacing": ["AVG"],
                                                                  "building"                : ["NB_DENS"]]
            }
            def computeRSUStatisticsUnweighted = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
            if (!computeRSUStatisticsUnweighted([inputLowerScaleTableName: buildingTable,
                                                 inputUpperScaleTableName: rsuTable,
                                                 inputIdUp               : columnIdRsu,
                                                 inputIdLow              : columnIdBuild,
                                                 inputVarAndOperations   : inputVarAndOperations,
                                                 prefixName              : temporaryPrefName,
                                                 datasource              : datasource])) {
                info "Cannot compute the statistics : building, building volume densities, building number density" +
                        " and mean building neighbor number for the RSU"
                return
            }
            def rsuStatisticsUnweighted = computeRSUStatisticsUnweighted.results.outputTableName
            // Join in an intermediate table (for perviousness fraction)
            intermediateJoin.put(rsuStatisticsUnweighted, columnIdRsu)


            // rsu_mean_building_height weighted by their area + rsu_std_building_height weighted by their area.
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY") || indicatorUse*.toUpperCase().contains("LCZ")) {
                def computeRSUStatisticsWeighted = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
                if (!computeRSUStatisticsWeighted([inputLowerScaleTableName : buildingTable,
                                                   inputUpperScaleTableName : rsuTable,
                                                   inputIdUp                : columnIdRsu,
                                                   inputVarWeightsOperations: ["height_roof": ["area": ["AVG", "STD"]],
                                                                                "nb_lev": ["area": ["AVG"]]],
                                                   prefixName               : temporaryPrefName,
                                                   datasource               : datasource])) {
                    info "Cannot compute the weighted indicators mean, std height building and \n\
                mean volume building."
                    return
                }
                def rsuStatisticsWeighted = computeRSUStatisticsWeighted.results.outputTableName
                finalTablesToJoin.put(rsuStatisticsWeighted, columnIdRsu)
            }

            // rsu_linear_road_density + rsu_road_direction_distribution
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY") || indicatorUse*.toUpperCase().contains("TEB")) {
                def roadOperations = ["linear_road_density"]
                if (indicatorUse*.toUpperCase().contains("TEB")) {
                    roadOperations = ["road_direction_distribution", "linear_road_density"]
                }
                def computeLinearRoadOperations = Geoindicators.RsuIndicators.linearRoadOperations()
                if (!computeLinearRoadOperations([rsuTable         : rsuTable,
                                                  roadTable        : roadTable,
                                                  operations       : roadOperations,
                                                  levelConsiderated: [0],
                                                  datasource       : datasource,
                                                  prefixName       : temporaryPrefName])) {
                    info "Cannot compute the linear road density and road direction distribution"
                    return
                }
                def linearRoadOperations = computeLinearRoadOperations.results.outputTableName
                finalTablesToJoin.put(linearRoadOperations, columnIdRsu)
            }

            // rsu_free_vertical_roof_area_distribution + rsu_free_non_vertical_roof_area_distribution
            if (indicatorUse*.toUpperCase().contains("TEB")) {
                def computeRoofAreaDist = Geoindicators.RsuIndicators.roofAreaDistribution()
                if (!computeRoofAreaDist([rsuTable        : rsuTable,
                                          buildingTable   : buildingTable,
                                          listLayersBottom: facadeDensListLayersBottom,
                                          prefixName      : temporaryPrefName,
                                          datasource      : datasource])) {
                    info "Cannot compute the roof area distribution. "
                    return
                }
                def roofAreaDist = computeRoofAreaDist.results.outputTableName
                finalTablesToJoin.put(roofAreaDist, columnIdRsu)
            }

            // rsu_projected_facade_area_distribution
            if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("TEB")) {
                if (!indicatorUse*.toUpperCase().contains("TEB")) {
                    facadeDensListLayersBottom:
                    [0, 50, 200]
                    facadeDensNumberOfDirection: 8
                }
                def computeProjFacadeDist = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
                if (!computeProjFacadeDist([buildingTable    : buildingTable,
                                            rsuTable         : rsuTable,
                                            listLayersBottom : facadeDensListLayersBottom,
                                            numberOfDirection: facadeDensNumberOfDirection,
                                            prefixName       : temporaryPrefName,
                                            datasource       : datasource])) {
                    info "Cannot compute the projected facade distribution. "
                    return
                }
                def projFacadeDist = computeProjFacadeDist.results.outputTableName
                intermediateJoin.put(projFacadeDist, columnIdRsu)
            }

            // // Need to have the total building fraction in one indicator (by default building alone fraction and building high vegetation fractions are separated)
            if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                datasource.execute """DROP TABLE IF EXISTS $preAspectRatioTable;
                               CREATE TABLE $preAspectRatioTable 
                                    AS SELECT $columnIdRsu, ${buildingFractions.join("+")} AS BUILDING_TOTAL_FRACTION 
                                    FROM $surfaceFractions"""
                intermediateJoin.put(preAspectRatioTable, columnIdRsu)
            }

            // Create an intermediate join tables to have all needed input fields for future indicator calculation
            def computeIntermediateJoin = Geoindicators.DataUtils.joinTables()
            if (!computeIntermediateJoin([inputTableNamesWithId: intermediateJoin,
                                          outputTableName      : "tab4aspratio",
                                          datasource           : datasource])) {
                info "Cannot merge the tables used for aspect ratio calculation. "
                return
            }
            def intermediateJoinTable = computeIntermediateJoin.results.outputTableName
            finalTablesToJoin.put(intermediateJoinTable, columnIdRsu)


            // rsu_aspect_ratio
            if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                def computeAspectRatio = Geoindicators.RsuIndicators.aspectRatio()
                if (!computeAspectRatio([rsuTable                          : intermediateJoinTable,
                                         rsuFreeExternalFacadeDensityColumn: "free_external_facade_density",
                                         rsuBuildingDensityColumn          : "BUILDING_TOTAL_FRACTION",
                                         prefixName                        : temporaryPrefName,
                                         datasource                        : datasource])) {
                    info "Cannot compute the aspect ratio calculation "
                    return
                }
                def aspectRatio = computeAspectRatio.results.outputTableName
                finalTablesToJoin.put(aspectRatio, columnIdRsu)
            }

            // rsu_ground_sky_view_factor
            if (indicatorUse*.toUpperCase().contains("LCZ")) {
                // If the fast version is chosen (SVF derived from extended RSU free facade fraction
                if (svfSimplified == true) {
                    computeExtFF = Geoindicators.RsuIndicators.extendedFreeFacadeFraction()
                    if (!computeExtFF([buildingTable            : buildingTable,
                                       rsuTable                 : intermediateJoinTable,
                                       buContiguityColumn       : "contiguity",
                                       buTotalFacadeLengthColumn: "total_facade_length",
                                       prefixName               : temporaryPrefName, buffDist: 10, datasource: datasource])) {
                        info "Cannot compute the SVF calculation. "
                        return
                    }
                    datasource.execute "DROP TABLE IF EXISTS $SVF; CREATE TABLE SVF " +
                            "AS SELECT 1-extended_free_facade_fraction AS GROUND_SKY_VIEW_FACTOR, $columnIdRsu " +
                            "FROM ${computeExtFF.results.outputTableName}"
                } else {
                    def computeSVF = Geoindicators.RsuIndicators.groundSkyViewFactor()
                    if (!computeSVF([rsuTable                : intermediateJoinTable,
                                     correlationBuildingTable: buildingTable,
                                     pointDensity            : svfPointDensity,
                                     rayLength               : svfRayLength,
                                     numberOfDirection       : svfNumberOfDirection,
                                     prefixName              : temporaryPrefName,
                                     datasource              : datasource])) {
                        info "Cannot compute the SVF calculation. "
                        return
                    }
                    SVF = computeSVF.results.outputTableName
                }
                finalTablesToJoin.put(SVF, columnIdRsu)
            }

            // rsu_effective_terrain_roughness
            if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("TEB")) {
                // Create the join tables to have all needed input fields for aspect ratio computation
                def computeEffRoughHeight = Geoindicators.RsuIndicators.effectiveTerrainRoughnessLength()
                if (!computeEffRoughHeight([rsuTable                       : intermediateJoinTable,
                                            projectedFacadeAreaName        : "projected_facade_area_distribution",
                                            geometricMeanBuildingHeightName: "geom_avg_$heightColumnName",
                                            prefixName                     : temporaryPrefName,
                                            listLayersBottom               : facadeDensListLayersBottom,
                                            numberOfDirection              : facadeDensNumberOfDirection,
                                            datasource                     : datasource])) {
                    info "Cannot compute the projected_facade_area_distribution."
                    return
                }
                def effRoughHeight = computeEffRoughHeight.results.outputTableName
                finalTablesToJoin.put(effRoughHeight, columnIdRsu)

                // rsu_terrain_roughness_class
                if (indicatorUse*.toUpperCase().contains("LCZ")) {
                    def computeRoughClass = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass()
                    if (!computeRoughClass([datasource                     : datasource,
                                            rsuTable                       : effRoughHeight,
                                            effectiveTerrainRoughnessLength: "effective_terrain_roughness_length",
                                            prefixName                     : temporaryPrefName])) {
                        info "Cannot compute the SVF calculation."
                        return
                    }
                    def roughClass = computeRoughClass.results.outputTableName
                    finalTablesToJoin.put(roughClass, columnIdRsu)
                }
            }

            // rsu_perkins_skill_score_building_direction_variability
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                def computePerkinsDirection = Geoindicators.GenericIndicators.buildingDirectionDistribution()
                if (!computePerkinsDirection([buildingTableName: buildingTable,
                                              tableUp          : rsuTable,
                                              inputIdUp        : columnIdRsu,
                                              angleRangeSize   : angleRangeSizeBuDirection, prefixName: temporaryPrefName,
                                              datasource       : datasource])) {
                    info "Cannot compute the perkins Skill Score building direction distribution."
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
                               outputTableName      : outputTableName,
                               datasource           : datasource])) {
                info "Cannot merge all tables. "
                return
            }

            // Modify all indicators which do not have the expected name
            def listColumnNames = datasource.getTable(outputTableName).columns
            def mapIndic2Change = ["FLOOR_AREA_DENSITY"    : "BUILDING_FLOOR_AREA_DENSITY",
                                   "VOLUME_DENSITY"        : "BUILDING_VOLUME_DENSITY",
                                   "LINEAR_ROAD_DENSITY_H0": "GROUND_LINEAR_ROAD_DENSITY"]
            def query2ModifyNames = ""
            for (ind in mapIndic2Change.keySet()) {
                if (listColumnNames.contains(ind)) {
                    query2ModifyNames += "ALTER TABLE $outputTableName RENAME COLUMN $ind TO ${mapIndic2Change[ind]};"
                }
            }
            if (query2ModifyNames != "") {
                datasource.execute query2ModifyNames
            }

            // Remove all intermediate tables (indicators alone in one table)
            // Do not drop cached tables. The drop must be done at the end of the chain
            def interTabNames = removeAllCachedTableNames(intermediateJoin.keySet())
            def finTabNames = removeAllCachedTableNames(finalTablesToJoin.keySet())
            // Remove the RSU table from the list of "tables to remove" (since it needs to be conserved)
            interTabNames = interTabNames - rsuTable
            finTabNames = finTabNames - rsuTable
            datasource.execute """DROP TABLE IF EXISTS ${interTabNames.join(",")}, 
                                                ${finTabNames.join(",")}, $SVF"""

            def tObis = System.currentTimeMillis() - to_start

            info "Geoindicators calculation time: ${tObis / 1000} s"
            [outputTableName: outputTableName]
        }
    }
}
/** The processing chain creates the units used to describe the territory at three scales: Reference Spatial
 * Unit (RSU), block and building. The creation of the RSU needs several layers such as the hydrology,
 * the vegetation, the roads and the rail network and the boundary of the study zone. The blocks are created
 * from the buildings that are in contact.
 * Then the relationships between each scale is initialized in each unit table: the RSU ID is stored in
 * the block and in the building tables whereas the block ID is stored only in the building table.
 *
 * @param zoneTable The area of zone to be processed *
 * @param buildingTable The building table to be processed
 * @param roadTable The road table to be processed
 * @param railTable The rail table to be processed
 * @param vegetationTable The vegetation table to be processed
 * @param hydrographicTable The hydrographic table to be processed
 * @param surface_vegetation The minimum area of vegetation that will be considered to delineate the RSU (default 100,000 m²)
 * @param surface_hydro  The minimum area of water that will be considered to delineate the RSU (default 2,500 m²)
 * @param snappingTolerance A distance to group the geometries (e.g. two buildings in a block - default 0.01 m)
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 *
 * @return outputTableBuildingName Table name where are stored the buildings and the RSU and block ID
 * @return outputTableBlockName Table name where are stored the blocks and the RSU ID
 * @return outputTableRsuName Table name where are stored the RSU
 */
IProcess createUnitsOfAnalysis() {
    return create {
        title "Create all new spatial units and their relations : building, block and RSU"
        id "createUnitsOfAnalysis"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 10000, surface_hydro: 2500,
                snappingTolerance: 0.01d, prefixName: "", indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"]
        outputs outputTableBuildingName: String, outputTableBlockName: String, outputTableRsuName: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, snappingTolerance, prefixName, indicatorUse ->
            info "Create the units of analysis..."
            // Create the RSU
            def prepareRSUData = Geoindicators.SpatialUnits.prepareRSUData()
            if (!prepareRSUData([datasource        : datasource,
                                 zoneTable         : zoneTable,
                                 roadTable         : roadTable,
                                 railTable         : railTable,
                                 vegetationTable   : vegetationTable,
                                 hydrographicTable : hydrographicTable,
                                 surface_vegetation: surface_vegetation,
                                 surface_hydro     : surface_hydro,
                                 prefixName        : prefixName])) {
                info "Cannot prepare the data for RSU calculation."
                return
            }
            def rsuDataPrepared = prepareRSUData.results.outputTableName

            def createRSU = Geoindicators.SpatialUnits.createRSU()
            if (!createRSU([datasource        : datasource,
                            inputTableName    : rsuDataPrepared,
                            prefixName        : prefixName,
                            inputZoneTableName: zoneTable])) {
                info "Cannot compute the RSU."
                return
            }

            // By default, the building table is used to calculate the relations between buildings and RSU
            def inputLowerScaleBuRsu = buildingTable
            // And the block / RSU table does not exists
            def tableRsuBlocks = null
            // If the urban typology is needed
            if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                // Create the blocks
                def createBlocks = Geoindicators.SpatialUnits.createBlocks()
                if (!createBlocks([datasource    : datasource,
                                   inputTableName: buildingTable,
                                   prefixName    : prefixName,
                                   snappingTolerance      : snappingTolerance])) {
                    info "Cannot create the blocks."
                    return
                }

                // Create the relations between RSU and blocks (store in the block table)
                def createScalesRelationsRsuBl = Geoindicators.SpatialUnits.spatialJoin()
                if (!createScalesRelationsRsuBl([datasource              : datasource,
                                                 sourceTable             : createBlocks.results.outputTableName,
                                                 targetTable             : createRSU.results.outputTableName,
                                                 idColumnTarget          : createRSU.results.outputIdRsu,
                                                 prefixName              : prefixName,
                                                 nbRelations             : 1])) {
                    info "Cannot compute the scales relations between blocks and RSU."
                    return
                }

                // Create the relations between buildings and blocks (store in the buildings table)
                def createScalesRelationsBlBu = Geoindicators.SpatialUnits.spatialJoin()
                if (!createScalesRelationsBlBu([datasource              : datasource,
                                                sourceTable             : buildingTable,
                                                targetTable             : createBlocks.results.outputTableName,
                                                idColumnTarget          : createBlocks.results.outputIdBlock,
                                                prefixName              : prefixName,
                                                nbRelations             : 1])) {
                    info "Cannot compute the scales relations between blocks and buildings."
                    return
                }
                inputLowerScaleBuRsu = createScalesRelationsBlBu.results.outputTableName
                tableRsuBlocks = createScalesRelationsRsuBl.results.outputTableName
            }


            // Create the relations between buildings and RSU (store in the buildings table)
            // WARNING : if the blocks are used, the building table will contain the id_block and id_rsu for each of its
            // id_build but the relations between id_block and i_rsu should not been consider in this Table
            // the relationships may indeed be different from the one in the block Table
            def createScalesRelationsRsuBlBu = Geoindicators.SpatialUnits.spatialJoin()
            if (!createScalesRelationsRsuBlBu([datasource              : datasource,
                                               sourceTable             : inputLowerScaleBuRsu,
                                               targetTable             : createRSU.results.outputTableName,
                                               idColumnTarget          : createRSU.results.outputIdRsu,
                                               prefixName              : prefixName,
                                               nbRelations             : 1])) {
                info "Cannot compute the scales relations between buildings and RSU."
                return
            }


            [outputTableBuildingName: createScalesRelationsRsuBlBu.results.outputTableName,
             outputTableBlockName   : tableRsuBlocks,
             outputTableRsuName     : createRSU.results.outputTableName]
        }
    }
}


/**
 * Compute all geoindicators at the 3 scales :
 * building, block and RSU
 * Compute also the LCZ classification and the urban typology
 *
 * @return 4 tables outputTableBuildingIndicators, outputTableBlockIndicators, outputTableRsuIndicators,
 * outputTableRsuLcz . The first three tables contains the geoindicators and the last table the LCZ classification.
 * This table can be empty if the user decides not to calculate it.
 *
 */
IProcess computeAllGeoIndicators() {
    return create {
        title "Compute all geoindicators"
        id "computeAllGeoIndicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: "", railTable: "", vegetationTable: "",
                hydrographicTable: "", imperviousTable: "",
                buildingEstimateTableName :"",
                surface_vegetation: 10000, surface_hydro: 2500,
                snappingTolerance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified: false, prefixName: "",
                mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
                urbanTypoModelName: "",
                buildingHeightModelName: ""
        outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String,
                outputTableRsuIndicators: String, outputTableRsuLcz: String, outputTableZone: String,
                outputTableRsuUrbanTypoArea: String, outputTableRsuUrbanTypoFloorArea: String,
                outputTableBuildingUrbanTypo: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              imperviousTable,buildingEstimateTableName,
              surface_vegetation, surface_hydro, snappingTolerance, indicatorUse, svfSimplified, prefixName, mapOfWeights,
              urbanTypoModelName, buildingHeightModelName ->
            //Estimate height
            if (buildingHeightModelName) {
                def start = System.currentTimeMillis()
                enableTableCache()
                if (!buildingEstimateTableName) {
                    error "To estimate the building height a table that contains the list of building to estimate must be provided"
                    return
                }
                info "Geoclimate will try to estimate the building heights with the model $buildingHeightModelName."
                //Let's check if the model exists
                File inputModelFile = new File(buildingHeightModelName)
                def modelName = FilenameUtils.getBaseName(buildingHeightModelName)
                if (!inputModelFile.exists()) {
                    //We try to find this model in geoclimate
                    def modelURL = "https://github.com/orbisgis/geoclimate/raw/master/models/${modelName}.model"
                    def localInputModelFile = new File(System.getProperty("user.home") + File.separator + ".geoclimate" + File.separator + modelName + ".model")
                    // The model doesn't exist on the local folder we download it
                    if (!localInputModelFile.exists()) {
                        FileUtils.copyURLToFile(new URL(modelURL), localInputModelFile)
                        if (!localInputModelFile.exists()) {
                            error "Cannot find any model file to estimate the building heights"
                            return
                        }
                    }
                } else {
                    if (!FilenameUtils.isExtension(pathAndFileName, "model")) {
                        error "The extension of the model file must be .model"
                        return
                    }
                }
                IProcess geoIndicators = computeGeoclimateIndicators()
                if (!geoIndicators.execute(datasource: datasource, zoneTable: zoneTable,
                        buildingTable: buildingTable,
                        roadTable: roadTable,
                        railTable: railTable, vegetationTable: vegetationTable,
                        hydrographicTable: hydrographicTable, imperviousTable: imperviousTable,
                        surface_vegetation: surface_vegetation, surface_hydro: surface_hydro,
                        indicatorUse: ["URBAN_TYPOLOGY"],
                        svfSimplified: true, prefixName: prefixName,
                        mapOfWeights: mapOfWeights,
                        urbanTypoModelName: "")) {
                    error "Cannot build the geoindicators to estimate the building height"
                    return
                }
                //Let's go to select the building's id that must be processed to fix the height
                info "Extracting the building having no height information and estimate it"
                def results = geoIndicators.results;
                //Select indicators we need at building scales
                def buildingIndicatorsTableName = results.outputTableBuildingIndicators;
                datasource.getTable(buildingEstimateTableName).id_build.createIndex()
                datasource.getTable(buildingIndicatorsTableName).id_build.createIndex()
                datasource.getTable(buildingIndicatorsTableName).id_rsu.createIndex()

                def estimated_building_with_indicators = "ESTIMATED_BUILDING_INDICATORS_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                datasource.execute """DROP TABLE IF EXISTS $estimated_building_with_indicators;
                                           CREATE TABLE $estimated_building_with_indicators 
                                                    AS SELECT a.*
                                                    FROM $buildingIndicatorsTableName a 
                                                        RIGHT JOIN $buildingEstimateTableName b 
                                                        ON a.id_build=b.id_build
                                                    WHERE b.ESTIMATED = true AND a.ID_RSU IS NOT NULL;"""

                info "Collect building indicators to estimate the height"

                def applygatherScales = Geoindicators.GenericIndicators.gatherScales()
                applygatherScales.execute([
                        buildingTable    : estimated_building_with_indicators,
                        blockTable       : results.outputTableBlockIndicators,
                        rsuTable         : results.outputTableRsuIndicators,
                        targetedScale    : "BUILDING",
                        operationsToApply: ["AVG", "STD"],
                        prefixName       : prefixName,
                        datasource       : datasource])
                def gatheredScales = applygatherScales.results.outputTableName

                info "Start estimating the building height"

                //Apply RF model
                def applyRF = Geoindicators.TypologyClassification.applyRandomForestModel()
                if (!applyRF.execute([
                        explicativeVariablesTableName: gatheredScales,
                        pathAndFileName              : buildingHeightModelName,
                        idName                       : "id_build",
                        prefixName                   : prefixName,
                        datasource                   : datasource])) {
                    error "Cannot apply the building height model $buildingHeightModelName"
                    return
                }

                //Update the abstract building table
                info "Replace the input building table by the estimated height"
                def buildEstimatedHeight = applyRF.results.outputTableName

                def nbBuildingEstimated = datasource.firstRow("select count(*) as count from $buildEstimatedHeight").count

                datasource.getTable(buildEstimatedHeight).id_build.createIndex()

                def newEstimatedHeigthWithIndicators = "NEW_BUILDING_INDICATORS_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                //Use build table indicators
                datasource.execute """DROP TABLE IF EXISTS $newEstimatedHeigthWithIndicators;
                                           CREATE TABLE $newEstimatedHeigthWithIndicators as 
                                            SELECT  a.THE_GEOM, a.ID_BUILD,a.ID_SOURCE,
                                        CASE WHEN b.HEIGHT_ROOF IS NULL THEN a.HEIGHT_WALL ELSE 0 END AS HEIGHT_WALL ,
                                                COALESCE(b.HEIGHT_ROOF, a.HEIGHT_ROOF) AS HEIGHT_ROOF,
                                                CASE WHEN b.HEIGHT_ROOF IS NULL THEN a.NB_LEV ELSE 0 END AS NB_LEV, a.TYPE,a.MAIN_USE, a.ZINDEX, a.ID_BLOCK, a.ID_RSU from $buildingIndicatorsTableName
                                        a LEFT JOIN $buildEstimatedHeight b on a.id_build=b.id_build"""

                //We must format only estimated buildings
                //Apply format on the new abstract table
                def epsg = datasource."$newEstimatedHeigthWithIndicators".srid;
                IProcess formatEstimatedBuilding = ProcessingChain.FormatingDataChain.formatEstimatedBuilding()
                formatEstimatedBuilding.execute([
                        datasource    : datasource,
                        inputTableName: newEstimatedHeigthWithIndicators,
                        epsg          : epsg])

                def newbuildingTableName = formatEstimatedBuilding.results.outputTableName

                //Drop tables
                datasource.execute """DROP TABLE IF EXISTS $estimated_building_with_indicators,
                                        $newEstimatedHeigthWithIndicators, $buildEstimatedHeight,
                                        $gatheredScales"""

                //We use the existing spatial units
                def relationBlocks = results.outputTableBlockIndicators
                def relationRSU = results.outputTableRsuIndicators

                //The spatial relation tables RSU and BLOCK  must be filtered to keep only necessary columns
                def rsuRelationFiltered = prefix prefixName, "RSU_RELATION_"
                datasource.execute """DROP TABLE IF EXISTS $rsuRelationFiltered;
            CREATE TABLE $rsuRelationFiltered AS SELECT ID_RSU, THE_GEOM FROM $relationRSU;
            DROP TABLE $relationRSU;"""

                def relationBlocksFiltered = prefix prefixName, "BLOCK_RELATION_"
                datasource.execute """DROP TABLE IF EXISTS $relationBlocksFiltered;
            CREATE TABLE $relationBlocksFiltered AS SELECT ID_BLOCK,  THE_GEOM,ID_RSU FROM $relationBlocks;
            DROP TABLE $relationBlocks;"""

                // Temporary (and output tables) are created
                def lczIndicTable = postfix "LCZ_INDIC_TABLE"
                def baseNameUrbanTypoRsu = prefix prefixName, "URBAN_TYPO_RSU_"
                def urbanTypoBuilding
                def distribNotPercent = "DISTRIB_NOT_PERCENT"
                def COLUMN_ID_RSU = "id_rsu"
                def COLUMN_ID_BUILD = "id_build"
                def GEOMETRIC_COLUMN = "the_geom"
                def CORRESPONDENCE_TAB_URB_TYPO = ["ba"  : 1, "bgh": 2, "icif": 3, "icio": 4, "id": 5, "local": 6, "pcif": 7,
                                                   "pcio": 8, "pd": 9, "psc": 10]
                def nameColTypoMaj = "TYPO_MAJ"

                // Output Lcz (and urbanTypo) table names are set to null in case LCZ indicators (and urban typo) are not calculated
                def rsuLcz = null
                def urbanTypoArea = baseNameUrbanTypoRsu + "AREA"
                def urbanTypoFloorArea = baseNameUrbanTypoRsu + "FLOOR_AREA"
                def rsuLczWithoutGeom = "rsu_lcz_without_geom"

                //Compute building indicators
                def computeBuildingsIndicators = ProcessingChain.GeoIndicatorsChain.computeBuildingsIndicators()
                if (!computeBuildingsIndicators.execute([datasource            : datasource,
                                                         inputBuildingTableName: newbuildingTableName,
                                                         inputRoadTableName    : roadTable,
                                                         indicatorUse          : indicatorUse,
                                                         prefixName            : prefixName])) {
                    error "Cannot compute the building indicators"
                    return null
                }

                def buildingIndicators = computeBuildingsIndicators.results.outputTableName

                //Compute block indicators
                def blockIndicators = null
                if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                    def computeBlockIndicators = ProcessingChain.GeoIndicatorsChain.computeBlockIndicators()
                    if (!computeBlockIndicators.execute([datasource            : datasource,
                                                         inputBuildingTableName: buildingIndicators,
                                                         inputBlockTableName   : relationBlocksFiltered,
                                                         prefixName            : prefixName])) {
                        error "Cannot compute the block indicators"
                        return null
                    }
                    blockIndicators = computeBlockIndicators.results.outputTableName
                }

                //Compute RSU indicators
                def rsuIndicators = null
                def computeRSUIndicators = ProcessingChain.GeoIndicatorsChain.computeRSUIndicators()
                if (!computeRSUIndicators.execute([datasource       : datasource,
                                                   buildingTable    : buildingIndicators,
                                                   rsuTable         : rsuRelationFiltered,
                                                   vegetationTable  : vegetationTable,
                                                   roadTable        : roadTable,
                                                   hydrographicTable: hydrographicTable,
                                                   imperviousTable  : imperviousTable,
                                                   indicatorUse     : indicatorUse,
                                                   svfSimplified    : svfSimplified,
                                                   prefixName       : prefixName])) {
                    error "Cannot compute the RSU indicators"
                    return null
                }
                rsuIndicators = computeRSUIndicators.results.outputTableName
                info "All geoindicators have been computed"

                // If the LCZ indicators should be calculated, we only affect a LCZ class to each RSU
                if (indicatorUse.contains("LCZ")) {
                    info """ The LCZ classification is performed """

                    def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"              : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                         "BUILDING_FRACTION_LCZ"             : "BUILDING_SURFACE_FRACTION",
                                         "ASPECT_RATIO"                      : "ASPECT_RATIO",
                                         "GROUND_SKY_VIEW_FACTOR"            : "SKY_VIEW_FACTOR",
                                         "PERVIOUS_FRACTION_LCZ"             : "PERVIOUS_SURFACE_FRACTION",
                                         "IMPERVIOUS_FRACTION_LCZ"           : "IMPERVIOUS_SURFACE_FRACTION",
                                         "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH": "TERRAIN_ROUGHNESS_LENGTH"]

                    // Get into a new table the ID, geometry column and the 7 indicators defined by Stewart and Oke (2012)
                    // for LCZ classification (rename the indicators with the real names)
                    def queryReplaceNames = ""
                    lczIndicNames.each { oldIndic, newIndic ->
                        queryReplaceNames += "ALTER TABLE $lczIndicTable ALTER COLUMN $oldIndic RENAME TO $newIndic;"
                    }
                    datasource.execute """DROP TABLE IF EXISTS $lczIndicTable;
                                CREATE TABLE $lczIndicTable 
                                        AS SELECT $COLUMN_ID_RSU, $GEOMETRIC_COLUMN, ${lczIndicNames.keySet().join(",")} 
                                        FROM ${computeRSUIndicators.results.outputTableName};
                                $queryReplaceNames"""

                    datasource."$lczIndicTable".reload()

                    // The classification algorithm is called
                    def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
                    if (!classifyLCZ([rsuLczIndicators : lczIndicTable,
                                      rsuAllIndicators : computeRSUIndicators.results.outputTableName,
                                      normalisationType: "AVG",
                                      mapOfWeights     : mapOfWeights,
                                      prefixName       : prefixName,
                                      datasource       : datasource,
                                      prefixName       : prefixName])) {
                        info "Cannot compute the LCZ classification."
                        return
                    }
                    rsuLcz = classifyLCZ.results.outputTableName
                    datasource.execute "DROP TABLE IF EXISTS $lczIndicTable"

                }
                // If the URBAN_TYPOLOGY indicators should be calculated, we only affect a URBAN typo class
                // to each building and then to each RSU
                if (indicatorUse.contains("URBAN_TYPOLOGY") && urbanTypoModelName) {
                    info """ The URBAN TYPOLOGY classification is performed """
                    applygatherScales = Geoindicators.GenericIndicators.gatherScales()
                    applygatherScales.execute([
                            buildingTable    : buildingIndicators,
                            blockTable       : blockIndicators,
                            rsuTable         : rsuIndicators,
                            targetedScale    : "BUILDING",
                            operationsToApply: ["AVG", "STD"],
                            prefixName       : prefixName,
                            datasource       : datasource])
                    gatheredScales = applygatherScales.results.outputTableName

                    applyRF = Geoindicators.TypologyClassification.applyRandomForestModel()
                    if (!applyRF.execute([
                            explicativeVariablesTableName: gatheredScales,
                            pathAndFileName              : urbanTypoModelName,
                            idName                       : COLUMN_ID_BUILD,
                            prefixName                   : prefixName,
                            datasource                   : datasource])) {
                        error "Cannot apply the urban typology model $urbanTypoModelName"
                        return
                    }
                    def urbanTypoBuild = applyRF.results.outputTableName

                    // Creation of a list which contains all types of the urban typology (in their string version)
                    def urbTypoCorrespondenceTabInverted = [:]
                    CORRESPONDENCE_TAB_URB_TYPO.each { fin, ini ->
                        urbTypoCorrespondenceTabInverted[ini] = fin
                    }
                    datasource."$urbanTypoBuild".I_TYPO.createIndex()
                    def queryDistinct = """SELECT DISTINCT I_TYPO AS I_TYPO FROM $urbanTypoBuild"""
                    def mapTypos = datasource.rows(queryDistinct)
                    def listTypos = []
                    mapTypos.each {
                        listTypos.add(urbTypoCorrespondenceTabInverted[it.I_TYPO])
                    }

                    // Join the geometry field to the building typology table and replace integer by string values
                    def queryCaseWhenReplace = ""
                    def endCaseWhen = ""
                    urbTypoCorrespondenceTabInverted.each { ini, fin ->
                        queryCaseWhenReplace += "CASE WHEN b.I_TYPO=$ini THEN '$fin' ELSE "
                        endCaseWhen += " END"
                    }
                    queryCaseWhenReplace = queryCaseWhenReplace + " 'unknown' " + endCaseWhen
                    urbanTypoBuilding = prefix prefixName, "URBAN_TYPO_BUILDING"
                    datasource."$urbanTypoBuild"."$COLUMN_ID_BUILD".createIndex()
                    datasource."$buildingIndicators"."$COLUMN_ID_BUILD".createIndex()
                    datasource """  DROP TABLE IF EXISTS $urbanTypoBuilding;
                                CREATE TABLE $urbanTypoBuilding
                                    AS SELECT   a.$COLUMN_ID_BUILD, a.$COLUMN_ID_RSU, a.THE_GEOM,
                                                $queryCaseWhenReplace AS I_TYPO
                                    FROM $buildingIndicators a LEFT JOIN $urbanTypoBuild b
                                    ON a.$COLUMN_ID_BUILD = b.$COLUMN_ID_BUILD
                                    WHERE a.$COLUMN_ID_RSU IS NOT NULL"""

                    // Create a distribution table (for each RSU, contains the % area OR floor area of each urban typo)
                    def queryCasewhen = [:]
                    queryCasewhen["AREA"] = ""
                    queryCasewhen["FLOOR_AREA"] = ""
                    queryCasewhen.keySet().each { ind ->
                        def querySum = ""
                        listTypos.each { typoCol ->
                            queryCasewhen[ind] += """ SUM(CASE WHEN a.I_TYPO='$typoCol' THEN b.$ind ELSE 0 END) AS TYPO_$typoCol,"""
                            querySum = querySum + " COALESCE(b.TYPO_${typoCol}/(b.TYPO_${listTypos.join("+b.TYPO_")}), 0) AS TYPO_$typoCol,"
                        }
                        // Calculates the distribution per RSU
                        datasource."$buildingIndicators"."$COLUMN_ID_RSU".createIndex()
                        datasource."$urbanTypoBuilding"."$COLUMN_ID_BUILD".createIndex()
                        datasource.execute """  DROP TABLE IF EXISTS $distribNotPercent;
                                            CREATE TABLE $distribNotPercent
                                                AS SELECT   b.$COLUMN_ID_RSU,
                                                            ${queryCasewhen[ind][0..-2]} 
                                                FROM $urbanTypoBuilding a RIGHT JOIN $buildingIndicators b
                                                ON a.$COLUMN_ID_BUILD = b.$COLUMN_ID_BUILD
                                                WHERE b.$COLUMN_ID_RSU IS NOT NULL 
                                                GROUP BY b.$COLUMN_ID_RSU
                                                """
                        // Calculates the frequency by RSU
                        datasource."$distribNotPercent"."$COLUMN_ID_RSU".createIndex()
                        datasource."$rsuIndicators"."$COLUMN_ID_RSU".createIndex()
                        datasource.execute """  DROP TABLE IF EXISTS TEMPO_DISTRIB;
                                            CREATE TABLE TEMPO_DISTRIB
                                                AS SELECT   a.$COLUMN_ID_RSU, a.the_geom,
                                                            ${querySum[0..-2]} 
                                                FROM $rsuIndicators a LEFT JOIN $distribNotPercent b
                                                ON a.$COLUMN_ID_RSU = b.$COLUMN_ID_RSU"""

                        // Characterize the distribution to identify the most frequent type within a RSU
                        def computeDistribChar = Geoindicators.GenericIndicators.distributionCharacterization()
                        computeDistribChar([distribTableName: "TEMPO_DISTRIB",
                                            inputId         : COLUMN_ID_RSU,
                                            initialTable    : "TEMPO_DISTRIB",
                                            distribIndicator: ["uniqueness"],
                                            extremum        : "GREATEST",
                                            keep2ndCol      : false,
                                            keepColVal      : false,
                                            prefixName      : "${prefixName}$ind",
                                            datasource      : datasource])
                        def resultsDistrib = computeDistribChar.results.outputTableName

                        // Join main typo table with distribution table and replace typo by null when it has been set
                        // while there is no building in the RSU
                        datasource."$resultsDistrib"."$COLUMN_ID_RSU".createIndex()
                        datasource.tempo_distrib."$COLUMN_ID_RSU".createIndex()
                        datasource """  DROP TABLE IF EXISTS $baseNameUrbanTypoRsu$ind;
                                    CREATE TABLE $baseNameUrbanTypoRsu$ind
                                        AS SELECT   a.*, 
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        b.UNIQUENESS_VALUE END AS UNIQUENESS_VALUE,
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        LOWER(SUBSTRING(b.EXTREMUM_COL FROM 6)) END AS $nameColTypoMaj
                                        FROM    TEMPO_DISTRIB a LEFT JOIN $resultsDistrib b
                                        ON a.$COLUMN_ID_RSU=b.$COLUMN_ID_RSU"""
                    }
                    // Drop temporary tables
                    datasource """DROP TABLE IF EXISTS $urbanTypoBuild, $gatheredScales, $distribNotPercent, TEMPO_DISTRIB"""
                } else {
                    urbanTypoArea = null
                    urbanTypoFloorArea = null
                    urbanTypoBuilding = null
                }

                datasource.execute "DROP TABLE IF EXISTS $rsuLczWithoutGeom;"
                //Drop all cached tables
                def cachedTableNames = getCachedTableNames()
                if (cachedTableNames) {
                    datasource.execute "DROP TABLE IF EXISTS ${cachedTableNames.join(",")}"
                }

                //Populate reporting

                def nbBuilding = datasource.firstRow("select count(*) as count from ${computeBuildingsIndicators.getResults().outputTableName} WHERE ID_RSU IS NOT NULL").count

                def nbBlock = 0
                if (blockIndicators) {
                 nbBlock = datasource.firstRow("select count(*) as count from ${blockIndicators}").count
                }
                def nbRSU = datasource.firstRow("select count(*) as count from ${computeRSUIndicators.getResults().outputTableName}").count

                //Update reporting to the zone table
                datasource.execute"""update ${zoneTable} 
                set nb_estimated_building = ${nbBuildingEstimated}, 
                nb_building = ${nbBuilding}, 
                nb_block =  ${nbBlock},
                nb_rsu = ${nbRSU},
                computation_time = ${(System.currentTimeMillis()-start)/1000},
                last_update = CAST(now() AS VARCHAR)"""

                //Clean the System properties that stores intermediate table names
                clearTablesCache()
                return [outputTableBuildingIndicators   : computeBuildingsIndicators.getResults().outputTableName,
                        outputTableBlockIndicators      : blockIndicators,
                        outputTableRsuIndicators        : computeRSUIndicators.getResults().outputTableName,
                        outputTableRsuLcz               : rsuLcz,
                        outputTableZone                 : zoneTable,
                        outputTableRsuUrbanTypoArea     : urbanTypoArea,
                        outputTableRsuUrbanTypoFloorArea: urbanTypoFloorArea,
                        outputTableBuildingUrbanTypo    : urbanTypoBuilding]

            }
            else {
                clearTablesCache()
                IProcess geoIndicators = computeGeoclimateIndicators()
                if (!geoIndicators.execute(datasource: datasource, zoneTable: zoneTable,
                        buildingTable: buildingTable,
                        roadTable: roadTable,
                        railTable: railTable, vegetationTable: vegetationTable,
                        hydrographicTable: hydrographicTable, imperviousTable: imperviousTable,
                        surface_vegetation: surface_vegetation, surface_hydro: surface_hydro,
                        indicatorUse: indicatorUse,
                        svfSimplified: svfSimplified, prefixName: prefixName,
                        mapOfWeights: mapOfWeights,
                        urbanTypoModelName: urbanTypoModelName)) {
                    error "Cannot build the geoindicators"
                    return
                } else {
                    return geoIndicators.getResults()
                }
            }
        }
    }
}

/**
 * Compute all geoclimate indicators at the 3 scales :
 * building, block and RSU
 * The LCZ classification  and the urban typology
 *
 * @return 8 tables outputTableBuildingIndicators, outputTableBlockIndicators, outputTableRsuIndicators,
 * outputTableRsuLcz, outputTableZone ,
 * outputTableRsuUrbanTypoArea, outputTableRsuUrbanTypoFloorArea,
 * outputTableBuildingUrbanTypo.
 * The first three tables contains the geoindicators and the last tables the LCZ and urban typology classifications.
 * This table can be empty if the user decides not to calculate it.
 *
 */
IProcess computeGeoclimateIndicators() {
    return create {
        title "Compute all geoindicators"
        id "computeAllGeoIndicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: "", railTable: "", vegetationTable: "",
                hydrographicTable: "", imperviousTable: "", surface_vegetation: 10000, surface_hydro: 2500,
                snappingTolerance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified: false, prefixName: "",
                mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
                urbanTypoModelName: ""
        outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String,
                outputTableRsuIndicators: String, outputTableRsuLcz: String, outputTableZone: String,
                outputTableRsuUrbanTypoArea: String, outputTableRsuUrbanTypoFloorArea: String,
                outputTableBuildingUrbanTypo: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              imperviousTable,
              surface_vegetation, surface_hydro, snappingTolerance, indicatorUse, svfSimplified, prefixName, mapOfWeights,
               urbanTypoModelName ->
            info "Start computing the geoindicators..."
            def start =  System.currentTimeMillis()
            // Temporary (and output tables) are created
            def lczIndicTable = postfix "LCZ_INDIC_TABLE"
            def baseNameUrbanTypoRsu = prefix prefixName, "URBAN_TYPO_RSU_"
            def urbanTypoBuilding
            def distribNotPercent = "DISTRIB_NOT_PERCENT"

            def COLUMN_ID_RSU = "id_rsu"
            def COLUMN_ID_BUILD = "id_build"
            def GEOMETRIC_COLUMN = "the_geom"
            def CORRESPONDENCE_TAB_URB_TYPO = ["ba"  : 1, "bgh": 2, "icif": 3, "icio": 4, "id": 5, "local": 6, "pcif": 7,
                                               "pcio": 8, "pd": 9, "psc": 10]
            def nameColTypoMaj = "TYPO_MAJ"
            //Check data before computing indicators
            if (!zoneTable && !buildingTable && !roadTable) {
                error "To compute the geoindicators the zone, building and road tables must not be null or empty"
                return null
            }

            // Output Lcz (and urbanTypo) table names are set to null in case LCZ indicators (and urban typo) are not calculated
            def rsuLcz = null
            def urbanTypoArea = baseNameUrbanTypoRsu + "AREA"
            def urbanTypoFloorArea = baseNameUrbanTypoRsu + "FLOOR_AREA"
            def rsuLczWithoutGeom = "rsu_lcz_without_geom"

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = createUnitsOfAnalysis()
            if (!spatialUnits.execute([datasource       : datasource, zoneTable: zoneTable,
                                       buildingTable    : buildingTable, roadTable: roadTable,
                                       railTable        : railTable, vegetationTable: vegetationTable,
                                       hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                       surface_hydro    : surface_hydro, snappingTolerance: snappingTolerance,
                                       prefixName       : prefixName,
                                       indicatorUse     : indicatorUse])) {
                error "Cannot create the spatial units"
                return null
            }
            def relationBuildings = spatialUnits.getResults().outputTableBuildingName
            def relationBlocks = spatialUnits.getResults().outputTableBlockName
            def relationRSU = spatialUnits.getResults().outputTableRsuName


            //Compute building indicators
            def computeBuildingsIndicators = computeBuildingsIndicators()
            if (!computeBuildingsIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: relationBuildings,
                                                     inputRoadTableName    : roadTable,
                                                     indicatorUse          : indicatorUse,
                                                     prefixName            : prefixName])) {
                error "Cannot compute the building indicators"
                return null
            }

            def buildingIndicators = computeBuildingsIndicators.results.outputTableName

            //Compute block indicators
            def blockIndicators = null
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                def computeBlockIndicators = computeBlockIndicators()
                if (!computeBlockIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: buildingIndicators,
                                                     inputBlockTableName   : relationBlocks,
                                                     prefixName            : prefixName])) {
                    error "Cannot compute the block indicators"
                    return null
                }
                blockIndicators = computeBlockIndicators.results.outputTableName
            }

            //Compute RSU indicators
            def rsuIndicators = null
            def computeRSUIndicators = computeRSUIndicators()
            if (!computeRSUIndicators.execute([datasource       : datasource,
                                               buildingTable    : buildingIndicators,
                                               rsuTable         : relationRSU,
                                               vegetationTable  : vegetationTable,
                                               roadTable        : roadTable,
                                               hydrographicTable: hydrographicTable,
                                               imperviousTable  : imperviousTable,
                                               indicatorUse     : indicatorUse,
                                               svfSimplified    : svfSimplified,
                                               prefixName       : prefixName])) {
                error "Cannot compute the RSU indicators"
                return null
            }
            rsuIndicators = computeRSUIndicators.results.outputTableName
            info "All geoindicators have been computed"

            // If the LCZ indicators should be calculated, we only affect a LCZ class to each RSU
            if (indicatorUse.contains("LCZ")) {
                info """ The LCZ classification is performed """
                def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"              : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                         "BUILDING_FRACTION_LCZ"             : "BUILDING_SURFACE_FRACTION",
                                         "ASPECT_RATIO"                      : "ASPECT_RATIO",
                                         "GROUND_SKY_VIEW_FACTOR"            : "SKY_VIEW_FACTOR",
                                         "PERVIOUS_FRACTION_LCZ"             : "PERVIOUS_SURFACE_FRACTION",
                                         "IMPERVIOUS_FRACTION_LCZ"           : "IMPERVIOUS_SURFACE_FRACTION",
                                         "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH": "TERRAIN_ROUGHNESS_LENGTH"]

                    // Get into a new table the ID, geometry column and the 7 indicators defined by Stewart and Oke (2012)
                    // for LCZ classification (rename the indicators with the real names)
                    def queryReplaceNames = ""
                    lczIndicNames.each { oldIndic, newIndic ->
                        queryReplaceNames += "ALTER TABLE $lczIndicTable ALTER COLUMN $oldIndic RENAME TO $newIndic;"
                    }
                    datasource.execute """DROP TABLE IF EXISTS $lczIndicTable;
                                CREATE TABLE $lczIndicTable 
                                        AS SELECT $COLUMN_ID_RSU, $GEOMETRIC_COLUMN, ${lczIndicNames.keySet().join(",")} 
                                        FROM ${rsuIndicators};
                                $queryReplaceNames"""

                    datasource."$lczIndicTable".reload()

                    // The classification algorithm is called
                    def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
                    if (!classifyLCZ([rsuLczIndicators : lczIndicTable,
                                      rsuAllIndicators : rsuIndicators,
                                      normalisationType: "AVG",
                                      mapOfWeights     : mapOfWeights,
                                      prefixName       : prefixName,
                                      datasource       : datasource,
                                      prefixName       : prefixName])) {
                        info "Cannot compute the LCZ classification."
                        return
                    }
                    rsuLcz = classifyLCZ.results.outputTableName
                    datasource.execute "DROP TABLE IF EXISTS $lczIndicTable"

            }
            // If the URBAN_TYPOLOGY indicators should be calculated, we only affect a URBAN typo class
            // to each building and then to each RSU
            if (indicatorUse.contains("URBAN_TYPOLOGY") && urbanTypoModelName) {
                info """ The URBAN TYPOLOGY classification is performed """
                def applygatherScales = Geoindicators.GenericIndicators.gatherScales()
                applygatherScales.execute([
                        buildingTable    : buildingIndicators,
                        blockTable       : blockIndicators,
                        rsuTable         : rsuIndicators,
                        targetedScale    : "BUILDING",
                        operationsToApply: ["AVG", "STD"],
                        prefixName       : prefixName,
                        datasource       : datasource])
                def gatheredScales = applygatherScales.results.outputTableName

                def applyRF = Geoindicators.TypologyClassification.applyRandomForestModel()
                if(!applyRF.execute([
                        explicativeVariablesTableName: gatheredScales,
                        pathAndFileName              : urbanTypoModelName,
                        idName                       : COLUMN_ID_BUILD,
                        prefixName                   : prefixName,
                        datasource                   : datasource])){
                    error "Cannot apply the urban typology model $urbanTypoModelName"
                    return
                }
                def urbanTypoBuild = applyRF.results.outputTableName

                // Creation of a list which contains all types of the urban typology (in their string version)
                def urbTypoCorrespondenceTabInverted = [:]
                CORRESPONDENCE_TAB_URB_TYPO.each { fin, ini ->
                    urbTypoCorrespondenceTabInverted[ini] = fin
                }
                datasource."$urbanTypoBuild".I_TYPO.createIndex()
                def queryDistinct = """SELECT DISTINCT I_TYPO AS I_TYPO FROM $urbanTypoBuild"""
                def mapTypos = datasource.rows(queryDistinct)
                def listTypos = []
                mapTypos.each {
                    listTypos.add(urbTypoCorrespondenceTabInverted[it.I_TYPO])
                }

                // Join the geometry field to the building typology table and replace integer by string values
                def queryCaseWhenReplace = ""
                def endCaseWhen = ""
                urbTypoCorrespondenceTabInverted.each { ini, fin ->
                    queryCaseWhenReplace += "CASE WHEN b.I_TYPO=$ini THEN '$fin' ELSE "
                    endCaseWhen += " END"
                }
                queryCaseWhenReplace = queryCaseWhenReplace + " 'unknown' " + endCaseWhen
                urbanTypoBuilding = prefix prefixName, "URBAN_TYPO_BUILDING"
                datasource."$urbanTypoBuild"."$COLUMN_ID_BUILD".createIndex()
                datasource."$buildingIndicators"."$COLUMN_ID_BUILD".createIndex()
                datasource """  DROP TABLE IF EXISTS $urbanTypoBuilding;
                                CREATE TABLE $urbanTypoBuilding
                                    AS SELECT   a.$COLUMN_ID_BUILD, a.$COLUMN_ID_RSU, a.THE_GEOM,
                                                $queryCaseWhenReplace AS I_TYPO
                                    FROM $buildingIndicators a LEFT JOIN $urbanTypoBuild b
                                    ON a.$COLUMN_ID_BUILD = b.$COLUMN_ID_BUILD
                                    WHERE a.$COLUMN_ID_RSU IS NOT NULL"""

                // Create a distribution table (for each RSU, contains the % area OR floor area of each urban typo)
                def queryCasewhen = [:]
                queryCasewhen["AREA"] = ""
                queryCasewhen["FLOOR_AREA"] = ""
                queryCasewhen.keySet().each { ind ->
                    def querySum = ""
                    listTypos.each { typoCol ->
                        queryCasewhen[ind] += """ SUM(CASE WHEN a.I_TYPO='$typoCol' THEN b.$ind ELSE 0 END) AS TYPO_$typoCol,"""
                        querySum = querySum + " COALESCE(b.TYPO_${typoCol}/(b.TYPO_${listTypos.join("+b.TYPO_")}), 0) AS TYPO_$typoCol,"
                    }
                    // Calculates the distribution per RSU
                    datasource."$buildingIndicators"."$COLUMN_ID_RSU".createIndex()
                    datasource."$urbanTypoBuilding"."$COLUMN_ID_BUILD".createIndex()
                    datasource.execute """  DROP TABLE IF EXISTS $distribNotPercent;
                                            CREATE TABLE $distribNotPercent
                                                AS SELECT   b.$COLUMN_ID_RSU,
                                                            ${queryCasewhen[ind][0..-2]} 
                                                FROM $urbanTypoBuilding a RIGHT JOIN $buildingIndicators b
                                                ON a.$COLUMN_ID_BUILD = b.$COLUMN_ID_BUILD
                                                WHERE b.$COLUMN_ID_RSU IS NOT NULL 
                                                GROUP BY b.$COLUMN_ID_RSU
                                                """
                    // Calculates the frequency by RSU
                    datasource."$distribNotPercent"."$COLUMN_ID_RSU".createIndex()
                    datasource."$rsuIndicators"."$COLUMN_ID_RSU".createIndex()
                    datasource.execute """  DROP TABLE IF EXISTS TEMPO_DISTRIB;
                                            CREATE TABLE TEMPO_DISTRIB
                                                AS SELECT   a.$COLUMN_ID_RSU, a.the_geom,
                                                            ${querySum[0..-2]} 
                                                FROM $rsuIndicators a LEFT JOIN $distribNotPercent b
                                                ON a.$COLUMN_ID_RSU = b.$COLUMN_ID_RSU"""

                    // Characterize the distribution to identify the most frequent type within a RSU
                    def computeDistribChar = Geoindicators.GenericIndicators.distributionCharacterization()
                    computeDistribChar([distribTableName: "TEMPO_DISTRIB",
                                        inputId         : COLUMN_ID_RSU,
                                        initialTable    : "TEMPO_DISTRIB",
                                        distribIndicator: ["uniqueness"],
                                        extremum        : "GREATEST",
                                        keep2ndCol      : false,
                                        keepColVal      : false,
                                        prefixName      : "${prefixName}$ind",
                                        datasource      : datasource])
                    def resultsDistrib = computeDistribChar.results.outputTableName

                    // Join main typo table with distribution table and replace typo by null when it has been set
                    // while there is no building in the RSU
                    datasource."$resultsDistrib"."$COLUMN_ID_RSU".createIndex()
                    datasource.tempo_distrib."$COLUMN_ID_RSU".createIndex()
                    datasource """  DROP TABLE IF EXISTS $baseNameUrbanTypoRsu$ind;
                                    CREATE TABLE $baseNameUrbanTypoRsu$ind
                                        AS SELECT   a.*, 
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        b.UNIQUENESS_VALUE END AS UNIQUENESS_VALUE,
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        LOWER(SUBSTRING(b.EXTREMUM_COL FROM 6)) END AS $nameColTypoMaj
                                        FROM    TEMPO_DISTRIB a LEFT JOIN $resultsDistrib b
                                        ON a.$COLUMN_ID_RSU=b.$COLUMN_ID_RSU"""
                }

                // Drop temporary tables
                datasource """DROP TABLE IF EXISTS $urbanTypoBuild, $gatheredScales, $distribNotPercent, TEMPO_DISTRIB"""
            } else {
                urbanTypoArea = null
                urbanTypoFloorArea = null
                urbanTypoBuilding = null
            }

            datasource.execute "DROP TABLE IF EXISTS $rsuLczWithoutGeom;"

            //Populate reporting
            def nbBuilding = datasource.firstRow("select count(*) as count from ${buildingIndicators} WHERE ID_RSU IS NOT NULL").count
            def nbBlock = 0
            if(blockIndicators){
             nbBlock = datasource.firstRow("select count(*) as count from ${blockIndicators}").count
            }
            def nbRSU = datasource.firstRow("select count(*) as count from ${rsuIndicators}").count
            //Alter the zone table to add statics
            datasource.execute"""ALTER TABLE ${zoneTable} ADD COLUMN (
                NB_BUILDING INTEGER,
                NB_ESTIMATED_BUILDING INTEGER,
                NB_BLOCK INTEGER,
                NB_RSU INTEGER,
                COMPUTATION_TIME INTEGER,
                LAST_UPDATE VARCHAR
                )"""
            //Update reporting to the zone table
            datasource.execute"""update ${zoneTable} 
            set nb_estimated_building = 0, 
            nb_building = ${nbBuilding}, 
            nb_block =  ${nbBlock},
            nb_rsu = ${nbRSU},
            computation_time = ${(System.currentTimeMillis()-start)/1000},
            last_update = CAST(now() AS VARCHAR)"""

            return [outputTableBuildingIndicators   : buildingIndicators,
                    outputTableBlockIndicators      : blockIndicators,
                    outputTableRsuIndicators        : rsuIndicators,
                    outputTableRsuLcz               : rsuLcz,
                    outputTableZone                 : zoneTable,
                    outputTableRsuUrbanTypoArea     : urbanTypoArea,
                    outputTableRsuUrbanTypoFloorArea: urbanTypoFloorArea,
                    outputTableBuildingUrbanTypo    : urbanTypoBuilding]
        }
    }
}

/**
 * This process is used to aggregate geoclimate output indicators on a grid
 *
 * @param h2gis_datasource the local H2GIS database
 * @param processing_parameters the geoclimate chain parameters
 * @param zoneEnvelopeTableName
 * @param x_size
 * @param y_size
 * @param outputTableName the name of the table in the output_datasource to save the result
 * @return
 */
IProcess rasterizeIndicators() {
    return create {
        title "Aggregate indicators on a grid"
        id "rasterizeIndicators"
        inputs datasource: JdbcDataSource,
                zoneEnvelopeTableName: String,
                x_size : Integer, y_size : Integer,list_indicators :[],
                buildingTable: String, roadTable: "", vegetationTable: "",
                hydrographicTable: "", imperviousTable: "", rsu_lcz:"", rsu_urban_typo:"",
                prefixName: String
        outputs outputTableName: String
        run { datasource, zoneEnvelopeTableName, x_size, y_size,list_indicators,buildingTable, roadTable, vegetationTable,
            hydrographicTable, imperviousTable, rsu_lcz,rsu_urban_typo, prefixName ->
            if(x_size<=0 || y_size<= 0){
                info "Invalid grid size padding. Must be greater that 0"
                return
            }
            if(!list_indicators){
                info "The list of indicator names cannot be null or empty"
                return
            }
            def grid_indicators_table = "grid_indicators"
            def grid_column_identifier ="id"
            //Start to compute the grid
            def gridProcess = Geoindicators.SpatialUnits.createGrid()
            def box = datasource.getSpatialTable(zoneEnvelopeTableName).getExtent()
            if(gridProcess.execute([geometry: box, deltaX: x_size, deltaY: y_size,  datasource: datasource])) {
                def grid_table_name = gridProcess.results.outputTableName
                def indicatorTablesToJoin = [:]
                indicatorTablesToJoin.put(grid_table_name, grid_column_identifier)
                /*
                * Make aggregation process with previous grid and current rsu lcz
                */
                if (list_indicators*.toUpperCase().contains("RSU_LCZ") && rsu_lcz) {
                    def indicatorName = "LCZ1"
                    def upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics()
                    if (upperScaleAreaStatistics.execute(
                            [upperTableName : grid_table_name,
                             upperColumnId  : grid_column_identifier,
                             lowerTableName : rsu_lcz,
                             lowerColumnName: indicatorName,
                             keepGeometry   : false,
                             prefixName     : prefixName,
                             datasource     : datasource])) {
                        indicatorTablesToJoin.put(upperScaleAreaStatistics.results.outputTableName, grid_column_identifier)
                    } else {
                        info "Cannot compute the LCZ at grid scale"
                    }
                }
                // Calculate all surface fractions indicators on the GRID cell
                // Need to create the smallest geometries used as input of the surface fraction process
                def columnFractionsList = []
                if (!list_indicators*.toUpperCase().contains("BUILDING_AREA")) {
                    buildingTable = ""
                } else {
                    columnFractionsList << "BUILDING"
                }
                if (!list_indicators*.toUpperCase().contains("WATER_AREA")) {
                    hydrographicTable = ""
                } else {
                    columnFractionsList << "WATER"
                }
                if (!list_indicators*.toUpperCase().contains("VEGETATION_AREA")) {
                    vegetationTable = ""
                } else {
                    columnFractionsList << "LOW_VEGETATION"
                    columnFractionsList << "HIGH_VEGETATION"
                }
                if (!list_indicators*.toUpperCase().contains("ROAD_AREA")) {
                    roadTable = ""
                } else {
                    columnFractionsList << "ROAD"
                }
                if (!list_indicators*.toUpperCase().contains("IMPERVIOUS_AREA")) {
                    imperviousTable = ""
                } else {
                    columnFractionsList << "IMPERVIOUS"
                }
                if(columnFractionsList){
                    def computeSmallestGeom = Geoindicators.RsuIndicators.smallestCommunGeometry()
                    if (computeSmallestGeom.execute([
                            rsuTable       : grid_table_name, id_rsu: grid_column_identifier,
                            buildingTable  : buildingTable, roadTable: roadTable,
                            vegetationTable: vegetationTable, waterTable: hydrographicTable,
                            imperviousTable: imperviousTable,
                            prefixName     : prefixName, datasource: datasource])) {
                        def superpositionsTableGrid = computeSmallestGeom.results.outputTableName
                        // Calculate the surface fractions from the commun geom
                        def outputGridFractions = postfix("grid_fractions")
                        def query = """DROP TABLE IF EXISTS $outputGridFractions; CREATE TABLE $outputGridFractions AS SELECT b.${grid_column_identifier} """
                        datasource."$grid_table_name"."$grid_column_identifier".createIndex()
                        datasource."$superpositionsTableGrid"."$grid_column_identifier".createIndex()
                        def columnsSup = datasource.getTable(superpositionsTableGrid).columns
                        columnsSup.remove("AREA")
                        columnsSup.remove("ID")
                        if (columnsSup.size() > 0) {
                            columnsSup.each { name ->
                                if (name in columnFractionsList) {
                                    query += ", COALESCE(SUM(CASE WHEN a.${name} =1  THEN a.area ELSE 0 END),0)/st_area(b.the_geom) AS ${name}_fraction "
                                }
                            }
                            query += """ FROM $superpositionsTableGrid AS a RIGHT JOIN $grid_table_name b 
                            ON a.${grid_column_identifier}=b.${grid_column_identifier} GROUP BY b.${grid_column_identifier};"""
                            datasource.execute query
                            indicatorTablesToJoin.put(outputGridFractions, grid_column_identifier)
                        }
                    } else {
                        info "Cannot compute the surface fractions at grid scale"
                    }
                }
                //Join all indicators at grid scale
                def joinGrids = Geoindicators.DataUtils.joinTables()
                if (!joinGrids([inputTableNamesWithId: indicatorTablesToJoin,
                                outputTableName      : grid_indicators_table,
                                datasource           : datasource])) {
                    info "Cannot merge all indicators in grid table $grid_indicators_table."
                    return
                }
            }
            [outputTableName: grid_indicators_table]
        }
    }
}

