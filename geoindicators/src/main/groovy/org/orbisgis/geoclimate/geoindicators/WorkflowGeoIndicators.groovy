/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.SQLException

@BaseScript Geoindicators geoindicators

/**
 * Compute the geoindicators at building scale
 *
 * @param building table name
 * @param road table name
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "UTRF", "TEB"])
 *
 * @return
 */
String computeBuildingsIndicators(JdbcDataSource datasource, String building, String road,
                                  List indicatorUse = ["LCZ", "UTRF", "TEB"], String prefixName = "") {

    info "Start computing building indicators..."

    def idColumnBu = "id_build"
    def BASE_NAME = "building_indicators"

    // Maps for intermediate or final joins
    def finalTablesToJoin = [:]
    finalTablesToJoin.put(building, idColumnBu)

    // The name of the outputTableName is constructed
    def outputTableName = postfix BASE_NAME
    def buildingPrefixName = "building_indicator_"
    def buildTableJoinNeighbors = postfix "A"

    // building_area + building_perimeter
    def geometryOperations = ["st_area"]
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        geometryOperations = ["st_perimeter", "st_area"]
    }
    def buildTableGeometryProperties = Geoindicators.GenericIndicators.geometryProperties(datasource, building, ["id_build"],
            geometryOperations, buildingPrefixName)
    if (!buildTableGeometryProperties) {
        info "Cannot compute the length,perimeter,area properties of the buildings"
        return
    }
    finalTablesToJoin.put(buildTableGeometryProperties, idColumnBu)

    // For indicators that are useful for UTRF OR for LCZ classification
    if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("UTRF")) {
        // building_volume + building_floor_area + building_total_facade_length
        def sizeOperations = ["volume", "floor_area", "total_facade_length"]
        if (!indicatorUse*.toUpperCase().contains("UTRF")) {
            sizeOperations = ["total_facade_length"]
        }
        def buildTableSizeProperties = Geoindicators.BuildingIndicators.sizeProperties(datasource, building,
                sizeOperations, buildingPrefixName)
        if (!buildTableSizeProperties) {
            info "Cannot compute the building_volume, building_floor_area, building_total_facade_length " +
                    "indicators for the buildings"
            return
        }
        finalTablesToJoin.put(buildTableSizeProperties, idColumnBu)

        // building_contiguity + building_common_wall_fraction + building_number_building_neighbor
        def neighborOperations = ["contiguity", "common_wall_fraction", "number_building_neighbor"]
        if (indicatorUse*.toUpperCase().contains("LCZ") && !indicatorUse*.toUpperCase().contains("UTRF")) {
            neighborOperations = ["contiguity"]
        }
        def buildTableComputeNeighborsProperties = Geoindicators.BuildingIndicators.neighborsProperties(datasource, building,
                neighborOperations, buildingPrefixName)
        if (!buildTableComputeNeighborsProperties) {
            info "Cannot compute the building_contiguity, building_common_wall_fraction, " +
                    "number_building_neighbor indicators for the buildings"
            return
        }
        finalTablesToJoin.put(buildTableComputeNeighborsProperties, idColumnBu)

        if (indicatorUse*.toUpperCase().contains("UTRF")) {
            // area_concavity + building_form_factor + building_raw_compactness + perimeter_convexity
            def buildTableFormProperties = Geoindicators.BuildingIndicators.formProperties(datasource, building,
                    ["area_concavity", "form_factor",
                     "raw_compactness",
                     "perimeter_convexity"],
                    buildingPrefixName)
            if (!buildTableFormProperties) {
                info "Cannot compute the area_concavity, form_factor, raw_compactness, " +
                        "perimeter_convexity indicators for the buildings"
                return
            }
            finalTablesToJoin.put(buildTableFormProperties, idColumnBu)

            // building_minimum_building_spacing
            def buildTableComputeMinimumBuildingSpacing = Geoindicators.BuildingIndicators.minimumBuildingSpacing(datasource, building,
                    100, buildingPrefixName)
            if (!buildTableComputeMinimumBuildingSpacing) {
                info "Cannot compute the minimum building spacing indicator"
                return
            }
            finalTablesToJoin.put(buildTableComputeMinimumBuildingSpacing, idColumnBu)

            // building_road_distance
            def buildTableComputeRoadDistance = Geoindicators.BuildingIndicators.roadDistance(datasource, building,
                    road, 100,
                    buildingPrefixName)
            if (!buildTableComputeRoadDistance) {
                info "Cannot compute the closest minimum distance to a road at 100 meters."
                return
            }
            finalTablesToJoin.put(buildTableComputeRoadDistance, idColumnBu)

            // Join for building_likelihood
            def computeJoinNeighbors = Geoindicators.DataUtils.joinTables(datasource, [(buildTableComputeNeighborsProperties): idColumnBu,
                                                                                       (building)                            : idColumnBu],
                    buildingPrefixName + "_neighbors")
            if (!computeJoinNeighbors) {
                info "Cannot join the number of neighbors of a building."
                return
            }
            buildTableJoinNeighbors = computeJoinNeighbors

            // building_likelihood_large_building
            def computeLikelihoodLargeBuilding = Geoindicators.BuildingIndicators.likelihoodLargeBuilding(datasource, buildTableJoinNeighbors,
                    "number_building_neighbor", buildingPrefixName)
            if (!computeLikelihoodLargeBuilding) {
                info "Cannot compute the like likelihood large building indicator."
                return
            }
            def buildTableComputeLikelihoodLargeBuilding = computeLikelihoodLargeBuilding
            finalTablesToJoin.put(buildTableComputeLikelihoodLargeBuilding, idColumnBu)
        }
    }

    def buildingTableJoin = Geoindicators.DataUtils.joinTables(datasource, finalTablesToJoin, buildingPrefixName)
    if (!buildingTableJoin) {
        info "Cannot merge all indicator in the table $buildingPrefixName."
        return
    }

    // Rename the last table to the right output table name
    datasource.execute """DROP TABLE IF EXISTS $outputTableName;
                    ALTER TABLE ${buildingTableJoin} RENAME TO $outputTableName""".toString()

    // Remove all intermediate tables (indicators alone in one table)
    // Recover all created tables in an array
    def finTabNames = finalTablesToJoin.keySet().toArray()
    // Remove the block table from the list of "tables to remove" (since it needs to be conserved)
    finTabNames = finTabNames - building
    datasource.execute """DROP TABLE IF EXISTS ${finTabNames.join(",")}, $buildTableJoinNeighbors""".toString()

    return outputTableName

}

/**
 * Compute the geoindicators at block scale
 *
 * @return
 */
String computeBlockIndicators(JdbcDataSource datasource, String inputBuildingTableName, String inputBlockTableName, String prefixName = "") {
    def BASE_NAME = "block_indicators"

    info "Start computing block indicators..."
    // The name of the outputTableName is constructed
    def outputTableName = postfix(BASE_NAME)
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
    def computeSimpleStats = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(datasource, inputBuildingTableName,
            inputBlockTableName, id_block,
            id_build, ["area"      : ["SUM"],
                       "floor_area": ["SUM"],
                       "volume"    : ["SUM"]],
            blockPrefixName)
    if (!computeSimpleStats) {
        info "Cannot compute the sum of of the building area, building volume and block floor area."
        return
    }

    finalTablesToJoin.put(computeSimpleStats, id_block)

    //Ratio between the holes area and the blocks area
    // block_hole_area_density
    def computeHoleAreaDensity = Geoindicators.BlockIndicators.holeAreaDensity(datasource, inputBlockTableName, blockPrefixName)
    if (!computeHoleAreaDensity) {
        info "Cannot compute the hole area density."
        return
    }
    finalTablesToJoin.put(computeHoleAreaDensity, id_block)

    //Perkins SKill Score of the distribution of building direction within a block
    // block_perkins_skill_score_building_direction
    def computePerkinsSkillScoreBuildingDirection = Geoindicators.GenericIndicators.buildingDirectionDistribution(datasource, inputBuildingTableName,
            inputBlockTableName,
            id_block,
            15,
            blockPrefixName)
    if (!computePerkinsSkillScoreBuildingDirection) {
        info "Cannot compute perkins skill indicator. "
        return
    }
    finalTablesToJoin.put(computePerkinsSkillScoreBuildingDirection, id_block)


    //Block closingness
    String computeClosingness = Geoindicators.BlockIndicators.closingness(datasource, inputBuildingTableName,
            inputBlockTableName, blockPrefixName)
    if (!computeClosingness) {
        info "Cannot compute closingness indicator. "
        return
    }
    finalTablesToJoin.put(computeClosingness, id_block)

    //Block net compactness
    def computeNetCompactness = Geoindicators.BlockIndicators.netCompactness(datasource, inputBuildingTableName, "volume",
            "contiguity", blockPrefixName)
    if (!computeNetCompactness) {
        info "Cannot compute the net compactness indicator. "
        return
    }
    finalTablesToJoin.put(computeNetCompactness, id_block)

    //Block mean building height
    //Block standard deviation building height
    def computeWeightedAggregatedStatistics = Geoindicators.GenericIndicators.weightedAggregatedStatistics(datasource, inputBuildingTableName,
            inputBlockTableName, id_block,
            ["height_roof": ["area": ["AVG", "STD"]]], blockPrefixName)
    if (!computeWeightedAggregatedStatistics) {
        info "Cannot compute the block mean building height and standard deviation building height indicators. "
        return
    }
    finalTablesToJoin.put(computeWeightedAggregatedStatistics, id_block)

    //Merge all in one table
    def blockTableJoin = Geoindicators.DataUtils.joinTables(datasource, finalTablesToJoin, blockPrefixName)
    if (!blockTableJoin) {
        info "Cannot merge all tables in $blockPrefixName. "
        return
    }

    // Rename the last table to the right output table name
    datasource.execute """DROP TABLE IF EXISTS $outputTableName;
                    ALTER TABLE ${blockTableJoin} RENAME TO $outputTableName""".toString()

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
        datasource.execute query2ModifyNames.toString()
    }

    // Remove all intermediate tables (indicators alone in one table)
    // Recover all created tables in an array
    def finTabNames = finalTablesToJoin.keySet().toArray()
    // Remove the block table from the list of "tables to remove" (since it needs to be conserved)
    finTabNames = finTabNames - inputBlockTableName
    datasource.execute """DROP TABLE IF EXISTS ${finTabNames.join(",")}""".toString()

    return outputTableName
}


/**
 * Compute the geoindicators at RSU scale
 *
 * @param buildingTable The table where are stored informations concerning buildings (and the id of the corresponding rsu)
 * @param rsu The table where are stored informations concerning RSU
 * @param road The table where are stored informations concerning roads
 * @param vegetation The table where are stored informations concerning vegetation
 * @param water The table where are stored informations concerning water
 * @param impervious The table where are stored the impervious areas
 * @param rail The table where are stored the rail ways
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
 * be calculated (default is all indicators : ["LCZ", "UTRF", "TEB"])
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
 * @param utrfSurfFraction Map containing as key the name of the fraction indicators useful for the urban typology classification
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
String computeRSUIndicators(JdbcDataSource datasource, String buildingTable,
                            String rsu, String vegetation, String road, String water, String impervious, String rail, Map parameters =
                                    [facadeDensListLayersBottom    : [0, 10, 20, 30, 40, 50],
                                     facadeDensNumberOfDirection   : 12,
                                     svfPointDensity               : 0.008,
                                     svfRayLength                  : 100,
                                     svfNumberOfDirection          : 60,
                                     heightColumnName              : "height_roof",
                                     inputFields                   : ["id_build", "the_geom"],
                                     levelForRoads                 : [0],
                                     angleRangeSizeBuDirection     : 30,
                                     svfSimplified                 : true,
                                     indicatorUse                  : ["LCZ", "UTRF", "TEB"],
                                     surfSuperpositions            : ["high_vegetation": ["water", "building", "low_vegetation", "rail", "road", "impervious"]],
                                     surfPriorities                : ["water", "building", "high_vegetation", "low_vegetation", "rail", "road", "impervious"],
                                     buildingAreaTypeAndComposition: ["light_industry": ["light_industry"],
                                                                      "heavy_industry": ["heavy_industry"],
                                                                      "commercial"    : ["commercial"],
                                                                      "residential"   : ["residential"]],
                                     floorAreaTypeAndComposition   : ["residential": ["residential"]],
                                     utrfSurfFraction              : ["vegetation_fraction_utrf"                : ["high_vegetation_fraction",
                                                                                                                   "low_vegetation_fraction",
                                                                                                                   "high_vegetation_low_vegetation_fraction",
                                                                                                                   "high_vegetation_road_fraction",
                                                                                                                   "high_vegetation_impervious_fraction",
                                                                                                                   "high_vegetation_water_fraction",
                                                                                                                   "high_vegetation_building_fraction"],
                                                                      "low_vegetation_fraction_utrf"            : ["low_vegetation_fraction"],
                                                                      "high_vegetation_impervious_fraction_utrf": ["high_vegetation_road_fraction",
                                                                                                                   "high_vegetation_impervious_fraction"],
                                                                      "high_vegetation_pervious_fraction_utrf"  : ["high_vegetation_fraction",
                                                                                                                   "high_vegetation_low_vegetation_fraction",
                                                                                                                   "high_vegetation_water_fraction"],
                                                                      "road_fraction_utrf"                      : ["road_fraction",
                                                                                                                   "high_vegetation_road_fraction"],
                                                                      "impervious_fraction_utrf"                : ["road_fraction",
                                                                                                                   "high_vegetation_road_fraction",
                                                                                                                   "impervious_fraction",
                                                                                                                   "high_vegetation_impervious_fraction"]],
                                     lczSurfFraction               : ["building_fraction_lcz"       : ["building_fraction",
                                                                                                       "high_vegetation_building_fraction"],
                                                                      "pervious_fraction_lcz"       : ["high_vegetation_fraction",
                                                                                                       "low_vegetation_fraction",
                                                                                                       "water_fraction",
                                                                                                       "high_vegetation_low_vegetation_fraction",
                                                                                                       "high_vegetation_water_fraction"],
                                                                      "high_vegetation_fraction_lcz": ["high_vegetation_fraction",
                                                                                                       "high_vegetation_low_vegetation_fraction",
                                                                                                       "high_vegetation_road_fraction",
                                                                                                       "high_vegetation_impervious_fraction",
                                                                                                       "high_vegetation_water_fraction",
                                                                                                       "high_vegetation_building_fraction"],
                                                                      "low_vegetation_fraction_lcz" : ["low_vegetation_fraction"],
                                                                      "impervious_fraction_lcz"     : ["impervious_fraction",
                                                                                                       "road_fraction",
                                                                                                       "rail_fraction",
                                                                                                       "high_vegetation_impervious_fraction",
                                                                                                       "high_vegetation_road_fraction",
                                                                                                       "high_vegetation_rail_fraction"],
                                                                      "water_fraction_lcz"          : ["water_fraction",
                                                                                                       "high_vegetation_water_fraction"]],
                                     buildingFractions             : ["high_vegetation_building_fraction", "building_fraction"]], String prefixName = "") {

    info "Start computing RSU indicators..."
    def to_start = System.currentTimeMillis()
    def columnIdRsu = "id_rsu"
    def columnIdBuild = "id_build"
    def BASE_NAME = "rsu_indicators"
    def tablesToDrop = []
    // Maps for intermediate or final joins
    def finalTablesToJoin = [:]
    def intermediateJoin = [:]
    finalTablesToJoin.put(rsu, columnIdRsu)
    intermediateJoin.put(rsu, columnIdRsu)

    // Name of the output table
    def outputTableName = postfix(BASE_NAME)

    // PrefixName for intermediate table (start with a letter to avoid table name issue if start with a number)
    def temporaryPrefName = "rsu_indicator"

    // Other temporary tables that have to be deleted at the end of the process
    def utrfFractionIndic = "utrf_fraction_indic"
    def lczFractionIndic = "lcz_fraction_indic"
    def preAspectRatioTable = "pre_HW_table"

    // Intermediate table that needs to be delete at the end
    def SVF = "SVF"
    def computeExtFF

    //Re-use the surface fraction table if it has been already calculated
    //Useful when the building height is estimated.
    def surfaceFractions = getCachedTableName("GEOCLIMATE_TABLE_RSU_SURFACE_FRACTIONS")
    if (!surfaceFractions) {
        // Calculate all surface fractions indicators
        // Need to create the smallest geometries used as input of the surface fraction process
        def superpositionsTable = Geoindicators.RsuIndicators.smallestCommunGeometry(datasource,
                rsu, "id_rsu", buildingTable, road, water, vegetation,
                impervious, rail, temporaryPrefName)
        if (!superpositionsTable) {
            info "Cannot compute the smallest commun geometries"
            return
        }
        // Calculate the surface fractions from the commun geom
        def computeSurfaceFractions = Geoindicators.RsuIndicators.surfaceFractions(
                datasource, rsu, "id_rsu", superpositionsTable,
                parameters.surfSuperpositions,
                parameters.surfPriorities, temporaryPrefName)
        if (!computeSurfaceFractions) {
            info "Cannot compute the surface fractions"
            return
        }

        tablesToDrop << superpositionsTable

        surfaceFractions = computeSurfaceFractions
    }
    finalTablesToJoin.put(surfaceFractions, columnIdRsu)

    // Get all column names from the surfaceFraction method to make verifications
    def surfFracList = datasource.getColumnNames(surfaceFractions)
    def indicatorUse = parameters.indicatorUse
    def utrfSurfFraction = parameters.utrfSurfFraction
    def lczSurfFraction = parameters.lczSurfFraction

    // Calculate the surface fractions needed for the urban typology classification
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        info """Processing urban typology surface fraction calculation"""
        // Get all columns needed for the calculations and verify that they exist

        def neededSurfUrb = utrfSurfFraction.findResults { k, v -> true ? v : null }.flatten()
        def missingElementsUrb = neededSurfUrb - neededSurfUrb.findAll { indUrb -> surfFracList.contains(indUrb.toUpperCase()) }
        if (missingElementsUrb.size() == 0) {
            def queryUrbSurfFrac = """DROP TABLE IF EXISTS $utrfFractionIndic;
                                        CREATE TABLE $utrfFractionIndic AS SELECT $columnIdRsu, """
            utrfSurfFraction.each { urbIndicator, indicatorList ->
                queryUrbSurfFrac += "${indicatorList.join("+")} AS $urbIndicator, "
            }
            queryUrbSurfFrac += " FROM $surfaceFractions"
            datasource.execute queryUrbSurfFrac.toString()
            finalTablesToJoin.put(utrfFractionIndic, columnIdRsu)
        } else {
            error """'utrfSurfFraction' and 'surfSuperpositions' parameters given by the user are not consistent.
                                Impossible to find the following indicators in the surface fractions table: ${missingElementsUrb.join(", ")}"""
        }
    }

    // Calculate the surface fractions needed for the LCZ classification
    if (indicatorUse*.toUpperCase().contains("LCZ")) {
        info """Processing LCZ surface fraction indicators calculation"""
        // Get all columns needed for the calculations and verify that they exist
        def neededSurfLcz = utrfSurfFraction.findResults { k, v -> true ? v : null }.flatten()
        def missingElementsLcz = neededSurfLcz - neededSurfLcz.findAll { indLcz -> surfFracList.contains(indLcz.toUpperCase()) }
        if (missingElementsLcz.size() == 0) {
            def querylczSurfFrac = """DROP TABLE IF EXISTS $lczFractionIndic;
                                            CREATE TABLE $lczFractionIndic AS SELECT $columnIdRsu, """
            lczSurfFraction.each { urbIndicator, indicatorList ->
                querylczSurfFrac += "${indicatorList.join("+")} AS $urbIndicator, "
            }
            querylczSurfFrac += " FROM $surfaceFractions"
            datasource.execute querylczSurfFrac.toString()
            finalTablesToJoin.put(lczFractionIndic, columnIdRsu)
        } else {
            error """'lczSurfFraction' and 'surfSuperpositions' parameters given by the user are not consistent.
                                Impossible to find the following indicators in the surface fractions table: ${missingElementsLcz.join(", ")}"""
        }
    }

    // building type fractions
    if (indicatorUse*.toUpperCase().contains("UTRF") || indicatorUse*.toUpperCase().contains("LCZ")) {
        def rsuTableTypeProportion = Geoindicators.GenericIndicators.typeProportion(datasource, buildingTable,
                columnIdRsu, "type", rsu, parameters.buildingAreaTypeAndComposition,
                parameters.floorAreaTypeAndComposition, temporaryPrefName)
        if (!rsuTableTypeProportion) {
            info "Cannot compute the building type proportion of the RSU"
            return
        }
        finalTablesToJoin.put(rsuTableTypeProportion, columnIdRsu)
    }

    // rsu_area (note that the uuid is used as prefix for intermediate tables - indicator alone in a table)
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        def computeGeometryProperties = Geoindicators.GenericIndicators.geometryProperties(datasource, rsu, [columnIdRsu],
                ["st_area"], temporaryPrefName)
        if (!computeGeometryProperties) {
            info "Cannot compute the area of the RSU"
            return
        }
        finalTablesToJoin.put(computeGeometryProperties, columnIdRsu)
    }


    // Building free external facade density
    if (indicatorUse*.toUpperCase().contains("UTRF") || indicatorUse*.toUpperCase().contains("LCZ")) {
        def rsu_free_ext_density = Geoindicators.RsuIndicators.freeExternalFacadeDensity(datasource, buildingTable, rsu,
                "contiguity", "total_facade_length",
                temporaryPrefName)
        if (!rsu_free_ext_density) {
            info "Cannot compute the free external facade density for the RSU"
            return
        }
        intermediateJoin.put(rsu_free_ext_density, columnIdRsu)
    }

    // rsu_building_density + rsu_building_volume_density + rsu_mean_building_volume
    // + rsu_mean_building_neighbor_number + rsu_building_floor_density + rsu_roughness_length
    // + rsu_building_number_density (RSU number of buildings divided RSU area)
    def inputVarAndOperations = [:]
    def heightColumnName = parameters.heightColumnName

    if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("TEB")) {
        inputVarAndOperations = inputVarAndOperations << [(heightColumnName): ["GEOM_AVG"]]
    }
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        inputVarAndOperations = inputVarAndOperations << ["volume"                  : ["DENS", "AVG"],
                                                          (heightColumnName)        : ["GEOM_AVG"],
                                                          "number_building_neighbor": ["AVG"],
                                                          "floor_area"              : ["DENS"],
                                                          "minimum_building_spacing": ["AVG"],
                                                          "building"                : ["NB_DENS"],
                                                          "pop"                     : ["SUM", "DENS"]]
    }
    def rsuStatisticsUnweighted = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(datasource, buildingTable,
            rsu, columnIdRsu, columnIdBuild,
            inputVarAndOperations, temporaryPrefName)
    if (!rsuStatisticsUnweighted) {
        info "Cannot compute the statistics : building, building volume densities, building number density" +
                " and mean building neighbor number for the RSU"
        return
    }
    // Join in an intermediate table (for perviousness fraction)
    intermediateJoin.put(rsuStatisticsUnweighted, columnIdRsu)


    // rsu_mean_building_height weighted by their area + rsu_std_building_height weighted by their area.
    if (indicatorUse*.toUpperCase().contains("UTRF") || indicatorUse*.toUpperCase().contains("LCZ")) {
        def rsuStatisticsWeighted = Geoindicators.GenericIndicators.weightedAggregatedStatistics(datasource, buildingTable,
                rsu, columnIdRsu,
                ["height_roof": ["area": ["AVG", "STD"]],
                 "nb_lev"     : ["area": ["AVG"]]], temporaryPrefName)
        if (!rsuStatisticsWeighted) {
            info "Cannot compute the weighted indicators mean, std height building and \n\
                mean volume building."
            return
        }
        finalTablesToJoin.put(rsuStatisticsWeighted, columnIdRsu)
    }

    // rsu_linear_road_density + rsu_road_direction_distribution
    if (indicatorUse*.toUpperCase().contains("UTRF") || indicatorUse*.toUpperCase().contains("TEB")) {
        def roadOperations = ["linear_road_density"]
        if (indicatorUse*.toUpperCase().contains("TEB")) {
            roadOperations = ["road_direction_distribution", "linear_road_density"]
        }
        def linearRoadOperations = Geoindicators.RsuIndicators.linearRoadOperations(datasource, rsu,
                road, roadOperations, 30, [0], temporaryPrefName)
        if (!linearRoadOperations) {
            info "Cannot compute the linear road density and road direction distribution"
            return
        }
        finalTablesToJoin.put(linearRoadOperations, columnIdRsu)
    }

    def facadeDensListLayersBottom = parameters.facadeDensListLayersBottom
    def facadeDensNumberOfDirection = parameters.facadeDensNumberOfDirection

    // rsu_free_vertical_roof_area_distribution + rsu_free_non_vertical_roof_area_distribution
    if (indicatorUse*.toUpperCase().contains("TEB")) {
        def roofAreaDist = Geoindicators.RsuIndicators.roofAreaDistribution(datasource, rsu,
                buildingTable, facadeDensListLayersBottom,
                temporaryPrefName)
        if (!roofAreaDist) {
            info "Cannot compute the roof area distribution. "
            return
        }
        finalTablesToJoin.put(roofAreaDist, columnIdRsu)
    }

    // rsu_projected_facade_area_distribution
    if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("TEB")) {
        if (!indicatorUse*.toUpperCase().contains("TEB")) {
            facadeDensListLayersBottom:
            [0, 50, 200]
            facadeDensNumberOfDirection: 8
        }
        def projFacadeDist = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution(datasource, buildingTable,
                rsu, "id_rsu", facadeDensListLayersBottom,
                facadeDensNumberOfDirection, temporaryPrefName)
        if (!projFacadeDist) {
            info "Cannot compute the projected facade distribution. "
            return
        }
        intermediateJoin.put(projFacadeDist, columnIdRsu)
    }

    // // Need to have the total building fraction in one indicator (by default building alone fraction and building high vegetation fractions are separated)
    if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("UTRF")) {
        datasource.execute """DROP TABLE IF EXISTS $preAspectRatioTable;
                               CREATE TABLE $preAspectRatioTable 
                                    AS SELECT $columnIdRsu, ${parameters.buildingFractions.join("+")} AS BUILDING_TOTAL_FRACTION 
                                    FROM $surfaceFractions""".toString()
        intermediateJoin.put(preAspectRatioTable, columnIdRsu)
    }

    // Create an intermediate join tables to have all needed input fields for future indicator calculation
    def intermediateJoinTable = Geoindicators.DataUtils.joinTables(datasource, intermediateJoin, "tab4aspratio")
    if (!intermediateJoinTable) {
        info "Cannot merge the tables used for aspect ratio calculation. "
        return
    }
    finalTablesToJoin.put(intermediateJoinTable, columnIdRsu)


    // rsu_aspect_ratio
    if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("UTRF")) {
        def aspectRatio = Geoindicators.RsuIndicators.aspectRatio(datasource, intermediateJoinTable,
                "free_external_facade_density",
                "BUILDING_TOTAL_FRACTION", temporaryPrefName)
        if (!aspectRatio) {
            info "Cannot compute the aspect ratio calculation "
            return
        }
        finalTablesToJoin.put(aspectRatio, columnIdRsu)
    }

    // rsu_ground_sky_view_factor
    if (indicatorUse*.toUpperCase().contains("LCZ")) {
        // If the fast version is chosen (SVF derived from extended RSU free facade fraction
        if (parameters.svfSimplified == true) {
            computeExtFF = Geoindicators.RsuIndicators.extendedFreeFacadeFraction(datasource, buildingTable, intermediateJoinTable,
                    "contiguity", "total_facade_length",
                    10, temporaryPrefName)
            if (!computeExtFF) {
                info "Cannot compute the SVF calculation. "
                return
            }
            datasource.execute """DROP TABLE IF EXISTS $SVF; CREATE TABLE SVF 
                            AS SELECT 1-extended_free_facade_fraction AS GROUND_SKY_VIEW_FACTOR, $columnIdRsu 
                            FROM ${computeExtFF}; DROP TABLE ${computeExtFF}""".toString()
        } else {
            def computeSVF = Geoindicators.RsuIndicators.groundSkyViewFactor(datasource, intermediateJoinTable, "id_rsu",
                    buildingTable, parameters.svfPointDensity,
                    parameters.svfRayLength, parameters.svfNumberOfDirection,
                    temporaryPrefName)
            if (!computeSVF) {
                info "Cannot compute the SVF calculation. "
                return
            }
            SVF = computeSVF
        }
        finalTablesToJoin.put(SVF, columnIdRsu)
    }

    // rsu_effective_terrain_roughness
    if (indicatorUse*.toUpperCase().contains("LCZ") || indicatorUse*.toUpperCase().contains("TEB")) {
        // Create the join tables to have all needed input fields for aspect ratio computation
        def effRoughHeight = Geoindicators.RsuIndicators.effectiveTerrainRoughnessLength(datasource, intermediateJoinTable, "id_rsu",
                "projected_facade_area_distribution",
                "geom_avg_$heightColumnName",
                facadeDensListLayersBottom, facadeDensNumberOfDirection, temporaryPrefName)
        if (!effRoughHeight) {
            info "Cannot compute the projected_facade_area_distribution."
            return
        }
        finalTablesToJoin.put(effRoughHeight, columnIdRsu)

        // rsu_terrain_roughness_class
        if (indicatorUse*.toUpperCase().contains("LCZ")) {
            def roughClass = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass(datasource, effRoughHeight, "id_rsu", "effective_terrain_roughness_length", temporaryPrefName)
            if (!roughClass) {
                info "Cannot compute the roughness class."
                return
            }
            finalTablesToJoin.put(roughClass, columnIdRsu)
        }
    }

    // rsu_perkins_skill_score_building_direction_variability
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        def perkinsDirection = Geoindicators.GenericIndicators.buildingDirectionDistribution(datasource, buildingTable,
                rsu, columnIdRsu,
                parameters.angleRangeSizeBuDirection, temporaryPrefName)
        if (!perkinsDirection) {
            info "Cannot compute the perkins Skill Score building direction distribution."
            return
        }
        finalTablesToJoin.put(perkinsDirection, columnIdRsu)
    }

    // Merge all in one table
    // To avoid duplicate the_geom in the join table, remove it from the intermediate table
    datasource.execute("ALTER TABLE $intermediateJoinTable DROP COLUMN the_geom;".toString())
    def rsuTableJoin = Geoindicators.DataUtils.joinTables(datasource, finalTablesToJoin, outputTableName)
    if (!rsuTableJoin) {
        info "Cannot merge all tables. "
        return
    }

    // Modify all indicators which do not have the expected name
    def listColumnNames = datasource.getColumnNames(outputTableName)
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
        datasource.execute query2ModifyNames.toString()
    }

    // Remove all intermediate tables (indicators alone in one table)
    // Do not drop cached tables. The drop must be done at the end of the chain
    def interTabNames = removeAllCachedTableNames(intermediateJoin.keySet())
    def finTabNames = removeAllCachedTableNames(finalTablesToJoin.keySet())
    // Remove the RSU table from the list of "tables to remove" (since it needs to be conserved)
    interTabNames = interTabNames - rsu
    finTabNames = finTabNames - rsu
    datasource.execute """DROP TABLE IF EXISTS ${interTabNames.join(",")}, 
                                                ${finTabNames.join(",")}, $SVF""".toString()

    datasource.dropTable(tablesToDrop)
    def tObis = System.currentTimeMillis() - to_start

    info "Geoindicators calculation time: ${tObis / 1000} s"
    return outputTableName
}


/**
 * Compute the typology indicators
 *
 * @param building_indicators The table where are stored indicators values at building scale
 * @param block_indicators The table where are stored indicators values at block scale
 * @param rsu_indicators The table where are stored indicators values at rsu scale
 * @param prefixName A prefix used to name the output table
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "UTRF", "TEB"])
 * @param mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
 * @param utrfModelName Name of the Random Forest model used to calculate the urban typology
 *
 * @return 4 tables: rsu_lcz, rsu_utrf_area, rsu_utrf_floor_area, building_utrf
 */
Map computeTypologyIndicators(JdbcDataSource datasource, String building_indicators, String block_indicators,
                              String rsu_indicators, Map parameters, String prefixName) {
    info "Start computing Typology indicators..."

    def tablesToDrop = []
    def indicatorUse = parameters.indicatorUse
    def utrfModelName = parameters.utrfModelName
    def mapOfWeights = parameters.mapOfWeights
    //Sanity check for URTF model
    def runUTRFTypology = true
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        if (!utrfModelName) {
            runUTRFTypology = false
        } else if (!modelCheck(utrfModelName)) {
            return
        }
    }
    // Temporary (and output tables) are created
    def lczIndicTable = postfix "LCZ_INDIC_TABLE"
    def baseNameUtrfRsu = postfix ""
    def utrfBuilding
    def distribNotPercent = "DISTRIB_NOT_PERCENT"

    def COLUMN_ID_RSU = "id_rsu"
    def COLUMN_ID_BUILD = "id_build"
    def GEOMETRIC_COLUMN = "the_geom"
    def CORRESPONDENCE_TAB_UTRF = ["ba"  : 1, "bgh": 2, "icif": 3, "icio": 4, "id": 5, "local": 6, "pcif": 7,
                                   "pcio": 8, "pd": 9, "psc": 10]
    def nameColTypoMaj = "TYPO_MAJ"
    def nameColTypoSecond = "TYPO_SECOND"

    // Output Lcz (and urbanTypo) table names are set to null in case LCZ indicators (and urban typo) are not calculated
    def rsuLcz = null
    def utrfArea = "UTRF_RSU_AREA" + baseNameUtrfRsu
    def utrfFloorArea = "UTRF_RSU_FLOOR_AREA" + baseNameUtrfRsu

    if (indicatorUse.contains("LCZ")) {
        info """ The LCZ classification will be performed """
        def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"              : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                             "BUILDING_FRACTION_LCZ"             : "BUILDING_SURFACE_FRACTION",
                             "ASPECT_RATIO"                      : "ASPECT_RATIO",
                             "GROUND_SKY_VIEW_FACTOR"            : "SKY_VIEW_FACTOR",
                             "PERVIOUS_FRACTION_LCZ"             : "PERVIOUS_SURFACE_FRACTION",
                             "IMPERVIOUS_FRACTION_LCZ"           : "IMPERVIOUS_SURFACE_FRACTION",
                             "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH": "TERRAIN_ROUGHNESS_LENGTH"]

        // Get into a new table the ID, geometry column and the 7 indicators defined by Stewart and Oke (2012)
        // for LCZ classification (rename the indicators with the real names)
        def queryReplaceNames = []
        lczIndicNames.each { oldIndic, newIndic ->
            queryReplaceNames << "$oldIndic as $newIndic"
        }
        datasource.execute """DROP TABLE IF EXISTS $lczIndicTable;
                                CREATE TABLE $lczIndicTable 
                                        AS SELECT $COLUMN_ID_RSU, $GEOMETRIC_COLUMN, ${queryReplaceNames.join(",")} 
                                        FROM ${rsu_indicators};""".toString()
        tablesToDrop << lczIndicTable

        // The classification algorithm is called
        def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType(datasource, lczIndicTable,
                rsu_indicators, "AVG",
                mapOfWeights, prefixName)

        if (!classifyLCZ) {
            datasource.execute "DROP TABLE IF EXISTS $lczIndicTable".toString()
            info "Cannot compute the LCZ classification."
            return
        }
        rsuLcz = classifyLCZ
        datasource.execute "DROP TABLE IF EXISTS $lczIndicTable".toString()

    }
    // If the UTRF indicators should be calculated, we only affect a URBAN typo class
    // to each building and then to each RSU
    if (indicatorUse.contains("UTRF") && runUTRFTypology) {
        info """ The URBAN TYPOLOGY classification is performed """
        def gatheredScales = Geoindicators.GenericIndicators.gatherScales(datasource,
                building_indicators, block_indicators,
                rsu_indicators, "BUILDING",
                ["AVG", "STD"], prefixName)
        tablesToDrop << gatheredScales
        if (!datasource.isEmpty(gatheredScales)) {
            def utrfBuild = Geoindicators.TypologyClassification.applyRandomForestModel(datasource,
                    gatheredScales, utrfModelName, COLUMN_ID_BUILD, prefixName)
            if (!utrfBuild) {
                error "Cannot apply the urban typology model $utrfModelName"
                return
            }

            tablesToDrop << utrfBuild

            // Creation of a list which contains all types of the urban typology (in their string version)
            def urbTypoCorrespondenceTabInverted = [:]
            CORRESPONDENCE_TAB_UTRF.each { fin, ini ->
                urbTypoCorrespondenceTabInverted[ini] = fin
            }
            datasource.createIndex(utrfBuild, "I_TYPO")
            def queryDistinct = """SELECT DISTINCT I_TYPO AS I_TYPO FROM $utrfBuild"""
            def mapTypos = datasource.rows(queryDistinct.toString())
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
            utrfBuilding = postfix "UTRF_BUILDING"
            datasource.createIndex(utrfBuild, COLUMN_ID_BUILD)
            datasource.createIndex(building_indicators, COLUMN_ID_BUILD)
            datasource """  DROP TABLE IF EXISTS $utrfBuilding;
                                CREATE TABLE $utrfBuilding
                                    AS SELECT   a.$COLUMN_ID_BUILD, a.$COLUMN_ID_RSU, a.THE_GEOM,
                                                $queryCaseWhenReplace AS I_TYPO
                                    FROM $building_indicators a LEFT JOIN $utrfBuild b
                                    ON a.$COLUMN_ID_BUILD = b.$COLUMN_ID_BUILD
                                    WHERE a.$COLUMN_ID_RSU IS NOT NULL""".toString()

            // Create a distribution table (for each RSU, contains the % area OR floor area of each urban typo)
            def queryCasewhen = [:]
            queryCasewhen["AREA"] = ""
            queryCasewhen["FLOOR_AREA"] = ""
            datasource.createIndex(rsu_indicators, COLUMN_ID_RSU)
            datasource.createIndex(building_indicators, COLUMN_ID_RSU)
            datasource.createIndex(utrfBuilding, COLUMN_ID_BUILD)

            queryCasewhen.keySet().each { ind ->
                def querySum = ""
                listTypos.each { typoCol ->
                    queryCasewhen[ind] += """ SUM(CASE WHEN a.I_TYPO='$typoCol' THEN b.$ind ELSE 0 END) AS TYPO_$typoCol,"""
                    querySum = querySum + " COALESCE(b.TYPO_${typoCol}/(b.TYPO_${listTypos.join("+b.TYPO_")}), 0) AS TYPO_$typoCol,"
                }
                // Calculates the distribution per RSU
                datasource.execute """  DROP TABLE IF EXISTS $distribNotPercent;
                                            CREATE TABLE $distribNotPercent
                                                AS SELECT   b.$COLUMN_ID_RSU,
                                                            ${queryCasewhen[ind][0..-2]} 
                                                FROM $utrfBuilding a RIGHT JOIN $building_indicators b
                                                ON a.$COLUMN_ID_BUILD = b.$COLUMN_ID_BUILD
                                                WHERE b.$COLUMN_ID_RSU IS NOT NULL 
                                                GROUP BY b.$COLUMN_ID_RSU
                                                """.toString()
                tablesToDrop << distribNotPercent
                // Calculates the frequency by RSU
                datasource.createIndex(distribNotPercent, COLUMN_ID_RSU)
                datasource.execute """  DROP TABLE IF EXISTS TEMPO_DISTRIB;
                                            CREATE TABLE TEMPO_DISTRIB
                                                AS SELECT   a.$COLUMN_ID_RSU, a.the_geom,
                                                            ${querySum[0..-2]} 
                                                FROM $rsu_indicators a LEFT JOIN $distribNotPercent b
                                                ON a.$COLUMN_ID_RSU = b.$COLUMN_ID_RSU""".toString()
                tablesToDrop << "TEMPO_DISTRIB"
                // Characterize the distribution to identify the 2 most frequent type within a RSU
                def resultsDistrib = Geoindicators.GenericIndicators.distributionCharacterization(
                        datasource, "TEMPO_DISTRIB",
                        "TEMPO_DISTRIB", COLUMN_ID_RSU, ["uniqueness"],
                        "GREATEST", true, false, "${prefixName}$ind")
                tablesToDrop << resultsDistrib
                // Join main typo table with distribution table and replace typo by null when it has been set
                // while there is no building in the RSU
                datasource.createIndex(resultsDistrib, COLUMN_ID_RSU)
                datasource.createIndex("tempo_distrib", COLUMN_ID_RSU)
                datasource """  DROP TABLE IF EXISTS UTRF_RSU_$ind$baseNameUtrfRsu;
                                    CREATE TABLE UTRF_RSU_$ind$baseNameUtrfRsu
                                        AS SELECT   a.*, 
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        b.UNIQUENESS_VALUE END AS UNIQUENESS_VALUE,
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        LOWER(SUBSTRING(b.EXTREMUM_COL FROM 6)) END AS $nameColTypoMaj,
                                                    CASE WHEN   b.UNIQUENESS_VALUE=-1
                                                    THEN        NULL
                                                    ELSE        LOWER(SUBSTRING(b.EXTREMUM_COL2 FROM 6)) END AS $nameColTypoSecond
                                        FROM    TEMPO_DISTRIB a LEFT JOIN $resultsDistrib b
                                        ON a.$COLUMN_ID_RSU=b.$COLUMN_ID_RSU""".toString()
            }
            // Drop temporary tables
            datasource.dropTable(tablesToDrop)
        }
    } else {
        utrfArea = null
        utrfFloorArea = null
        utrfBuilding = null
    }

    return [rsu_lcz            : rsuLcz,
            rsu_utrf_area      : utrfArea,
            rsu_utrf_floor_area: utrfFloorArea,
            building_utrf      : utrfBuilding]
}

/** The processing chain creates the units used to describe the territory at three scales: Reference Spatial
 * Unit (RSU), block and building. Then the relationships between each scale is initialized in each unit table: the RSU ID is stored in
 * the block and in the building tables whereas the block ID is stored only in the building table.
 * If the user wants to provide its own RSU he can provide it. Otherwise, the creation of the RSU needs several
 * layers such as the hydrology, the vegetation, the roads and the rail network and the boundary of the study zone.
 * The blocks are created from the buildings that are in contact.
 *
 * @param zone The area of zone to be processed *
 * @param building The building table to be processed
 * @param road The road table to be processed
 * @param rail The rail table to be processed
 * @param vegetation The vegetation table to be processed
 * @param water The hydrographic table to be processed
 * @param sea_land_mask The sea areas to be processed
 * @param urban_areas The urban areas table to be processed
 * @param rsu Only if the RSU table is provided by the user (otherwise the default RSU is calculated)
 * @param surface_vegetation The minimum area of vegetation that will be considered to delineate the RSU (default 100,000 m²)
 * @param surface_hydro The minimum area of water that will be considered to delineate the RSU (default 2,500 m²)
 * @param snappingTolerance A distance to group the geometries (e.g. two buildings in a block - default 0.01 m)
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "UTRF", "TEB"])
 *
 * @return building Table name where are stored the buildings and the RSU and block ID
 * @return block Table name where are stored the blocks and the RSU ID
 * @return rsu Table name where are stored the RSU
 */
Map createUnitsOfAnalysis(JdbcDataSource datasource, String zone, String building,
                          String road, String rail, String vegetation,
                          String water, String  sea_land_mask, String urban_areas,
                          String rsu, double surface_vegetation,
                          double surface_hydro, double surface_urban_areas,
                          double snappingTolerance, List indicatorUse = ["LCZ", "UTRF", "TEB"], String prefixName = "") {
    info "Create the units of analysis..."
    def idRsu = "id_rsu"
    def tablesToDrop = []
    if (!rsu) {
        // Create the RSU
        rsu = Geoindicators.SpatialUnits.createTSU(datasource, zone, road, rail,
                vegetation, water,
                sea_land_mask, urban_areas, surface_vegetation,
                surface_hydro,surface_urban_areas, prefixName)
        if (!rsu) {
            info "Cannot compute the RSU."
            return
        }
    }


    // By default, the building table is used to calculate the relations between buildings and RSU
    def inputLowerScaleBuRsu = building
    // And the block / RSU table does not exists
    def tableRsuBlocks = null
    // If the urban typology is needed
    if (indicatorUse.contains("UTRF")) {
        // Create the blocks
        String createBlocks = Geoindicators.SpatialUnits.createBlocks(datasource, building, snappingTolerance, prefixName)
        if (!createBlocks) {
            info "Cannot create the blocks."
            return
        }
        // Create the relations between RSU and blocks (store in the block table)
        String createScalesRelationsRsuBl = Geoindicators.SpatialUnits.spatialJoin(datasource, createBlocks, rsu,
                idRsu, 1, prefixName)
        if (!createScalesRelationsRsuBl) {
            info "Cannot compute the scales relations between blocks and RSU."
            return
        }
        tableRsuBlocks = createScalesRelationsRsuBl

        // Create the relations between buildings and blocks (store in the buildings table)
        String createScalesRelationsBlBu = Geoindicators.SpatialUnits.spatialJoin(datasource,
                building, createBlocks, "id_block", 1, prefixName)
        if (!createScalesRelationsBlBu) {
            info "Cannot compute the scales relations between blocks and buildings."
            datasource.dropTable(createBlocks)
            return
        }
        datasource.dropTable(createBlocks)
        inputLowerScaleBuRsu = createScalesRelationsBlBu
        tablesToDrop << inputLowerScaleBuRsu
    }


    // Create the relations between buildings and RSU (store in the buildings table)
    // WARNING : if the blocks are used, the building table will contain the id_block and id_rsu for each of its
    // id_build but the relations between id_block and i_rsu should not been consider in this Table
    // the relationships may indeed be different from the one in the block Table
    String createScalesRelationsRsuBlBu = Geoindicators.SpatialUnits.spatialJoin(datasource,
            inputLowerScaleBuRsu,
            rsu, idRsu, 1, prefixName)
    if (!createScalesRelationsRsuBlBu) {
        info "Cannot compute the scales relations between buildings and RSU."
        return
    }

    //Replace the building table with a new one that contains the relations between block and RSU
    datasource.execute("""DROP TABLE IF EXISTS $building;
            ALTER TABLE $createScalesRelationsRsuBlBu RENAME TO $building; """.toString())
    tablesToDrop << createScalesRelationsRsuBlBu
    datasource.dropTable(tablesToDrop)

    return ["building": building,
            "block"   : tableRsuBlocks,
            "rsu"     : rsu]
}


/**
 * Get all default parameters used by the workflow
 * @return
 */
Map getParameters() {
    return [
            "surface_vegetation"            : 10000d, "surface_hydro": 2500d, "surface_urban_areas":10000d,
            "snappingTolerance"             : 0.01d, "indicatorUse": ["LCZ", "UTRF", "TEB"],
            "mapOfWeights"                  : ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                                               "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
            "utrfModelName"                 : "",
            "buildingHeightModelName"       : "",
            "nbEstimatedBuildHeight"        : 0,
            "svfSimplified"                 : true,
            "facadeDensListLayersBottom"    : [0, 10, 20, 30, 40, 50],
            "facadeDensNumberOfDirection"   : 12,
            "svfPointDensity"               : 0.008,
            "svfRayLength"                  : 100,
            "svfNumberOfDirection"          : 60,
            "heightColumnName"              : "height_roof",
            "inputFields"                   : ["id_build", "the_geom"],
            "levelForRoads"                 : [0],
            "angleRangeSizeBuDirection"     : 30,
            "surfSuperpositions"            : ["high_vegetation": ["water", "building", "low_vegetation", "rail", "road", "impervious"]],
            "surfPriorities"                : ["water", "building", "high_vegetation", "low_vegetation", "rail", "road", "impervious"],
            "buildingAreaTypeAndComposition": ["light_industry": ["light_industry"],
                                               "heavy_industry": ["heavy_industry"],
                                               "commercial"    : ["commercial"],
                                               "residential"   : ["residential"]],
            "floorAreaTypeAndComposition"   : ["residential": ["residential"]],
            "utrfSurfFraction"              : ["vegetation_fraction_utrf"                : ["high_vegetation_fraction",
                                                                                            "low_vegetation_fraction",
                                                                                            "high_vegetation_low_vegetation_fraction",
                                                                                            "high_vegetation_road_fraction",
                                                                                            "high_vegetation_impervious_fraction",
                                                                                            "high_vegetation_water_fraction",
                                                                                            "high_vegetation_building_fraction"],
                                               "low_vegetation_fraction_utrf"            : ["low_vegetation_fraction"],
                                               "high_vegetation_impervious_fraction_utrf": ["high_vegetation_road_fraction",
                                                                                            "high_vegetation_impervious_fraction"],
                                               "high_vegetation_pervious_fraction_utrf"  : ["high_vegetation_fraction",
                                                                                            "high_vegetation_low_vegetation_fraction",
                                                                                            "high_vegetation_water_fraction"],
                                               "road_fraction_utrf"                      : ["road_fraction",
                                                                                            "high_vegetation_road_fraction"],
                                               "impervious_fraction_utrf"                : ["road_fraction",
                                                                                            "high_vegetation_road_fraction",
                                                                                            "impervious_fraction",
                                                                                            "high_vegetation_impervious_fraction"]],
            "lczSurfFraction"               : ["building_fraction_lcz"       : ["building_fraction",
                                                                                "high_vegetation_building_fraction"],
                                               "pervious_fraction_lcz"       : ["high_vegetation_fraction",
                                                                                "low_vegetation_fraction",
                                                                                "water_fraction",
                                                                                "high_vegetation_low_vegetation_fraction",
                                                                                "high_vegetation_water_fraction"],
                                               "high_vegetation_fraction_lcz": ["high_vegetation_fraction",
                                                                                "high_vegetation_low_vegetation_fraction",
                                                                                "high_vegetation_road_fraction",
                                                                                "high_vegetation_impervious_fraction",
                                                                                "high_vegetation_water_fraction",
                                                                                "high_vegetation_building_fraction"],
                                               "low_vegetation_fraction_lcz" : ["low_vegetation_fraction"],
                                               "impervious_fraction_lcz"     : ["impervious_fraction",
                                                                                "rail_fraction",
                                                                                "road_fraction",
                                                                                "high_vegetation_impervious_fraction",
                                                                                "high_vegetation_road_fraction",
                                                                                "high_vegetation_rail_fraction"],
                                               "water_fraction_lcz"          : ["water_fraction",
                                                                                "high_vegetation_water_fraction"]],
            "buildingFractions"             : ["high_vegetation_building_fraction", "building_fraction"]]

}

/**
 * Merge the input parameters with default supported parameters
 * @param parameters
 * @return
 */
Map getParameters(Map parameters) {
    Map parm = getParameters()
    parameters.each { it ->
        if (parm.containsKey(it.key)) {
            parm.put(it.key, it.value)
        }
    }
    return parm
}
/**
 * Compute all geoindicators at the 3 scales :
 * building, block and RSU
 * Compute also the LCZ classification and the urban typology
 *
 *
 * @return 5 tables building_indicators, block_indicators, rsu_indicators,
 * rsu_lcz, buildingTableName. The first three tables contains the geoindicators and the last table the LCZ classification.
 * The last table returns the new building table if the height model is set to true
 * This table can be empty if the user decides not to calculate it.
 *
 */
Map computeAllGeoIndicators(JdbcDataSource datasource, String zone, String building, String road, String rail, String vegetation,
                            String water, String impervious, String buildingEstimateTableName,
                            String sea_land_mask,String urban_areas, String rsuTable,
                            Map parameters = [:], String prefixName) {
    Map inputParameters = getParameters()
    if (parameters) {
        inputParameters = getParameters(parameters)
    }

    def surface_vegetation = inputParameters.surface_vegetation
    def surface_hydro = inputParameters.surface_hydro
    def surface_urban_areas = inputParameters.surface_urban_areas
    def snappingTolerance = inputParameters.snappingTolerance
    def buildingHeightModelName = inputParameters.buildingHeightModelName
    def indicatorUse = inputParameters.indicatorUse

    //Estimate height
    if (inputParameters.buildingHeightModelName && datasource.getRowCount(building) > 0) {
        def start = System.currentTimeMillis()
        enableTableCache()
        def buildingTableName
        def rsuTableForHeightEst
        def buildingIndicatorsForHeightEst
        def blockIndicatorsForHeightEst
        def rsuIndicatorsForHeightEst
        def nbBuildingEstimated

        // Estimate building height
        Map estimHeight = estimateBuildingHeight(datasource, zone, building,
                road, rail, vegetation,
                water, impervious,
                buildingEstimateTableName,
                sea_land_mask, urban_areas, rsuTable,
                surface_vegetation, surface_hydro,surface_urban_areas,
                snappingTolerance,
                buildingHeightModelName, prefixName)
        if (!estimHeight) {
            error "Cannot estimate building height"
            return
        } else {
            buildingTableName = estimHeight.building
            rsuTableForHeightEst = estimHeight.rsu
            buildingIndicatorsForHeightEst = estimHeight.building_indicators_without_height
            blockIndicatorsForHeightEst = estimHeight.block_indicators_without_height
            rsuIndicatorsForHeightEst = estimHeight.rsu_indicators_without_height
            nbBuildingEstimated = estimHeight.nb_building_estimated
        }

        indicatorUse = inputParameters.indicatorUse
        //This is a shortcut to extract building with estimated height
        if (indicatorUse.isEmpty()) {
            //Clean the System properties that stores intermediate table names
            datasource.dropTable(getCachedTableNames())
            clearTablesCache()
            return ["building_indicators": buildingIndicatorsForHeightEst,
                    "block_indicators"   : blockIndicatorsForHeightEst,
                    "rsu_indicators"     : rsuIndicatorsForHeightEst,
                    "rsu_lcz"            : null,
                    "zone"               : zone,
                    "rsu_utrf_area"      : null,
                    "rsu_utrf_floor_area": null,
                    "building_utrf"      : null,
                    "building"           : buildingTableName]
        }
        def buildingForGeoCalc
        def blocksForGeoCalc
        def rsuForGeoCalc
        // If the RSU is provided by the user, new relations between units should be performed
        if (rsuTable) {
            Map spatialUnitsForCalc = createUnitsOfAnalysis(datasource, zone,
                    building, road, rail,
                    vegetation,
                    water, sea_land_mask, "", rsuTable,
                    surface_vegetation,
                    surface_hydro, surface_urban_areas, snappingTolerance, indicatorUse,
                    prefixName)
            if (!spatialUnitsForCalc) {
                error "Cannot create the spatial units"
                return null
            }
            buildingForGeoCalc = spatialUnitsForCalc.building
            blocksForGeoCalc = spatialUnitsForCalc.block
            rsuForGeoCalc = spatialUnitsForCalc.rsu
        } else {
            buildingForGeoCalc = buildingTableName
            //The spatial relation tables RSU and BLOCK  must be filtered to keep only necessary columns
            rsuForGeoCalc = prefix prefixName, "RSU_RELATION_"
            datasource.execute """DROP TABLE IF EXISTS $rsuForGeoCalc;
                    CREATE TABLE $rsuForGeoCalc AS SELECT ID_RSU, THE_GEOM FROM $rsuTableForHeightEst;
                    DROP TABLE $rsuTableForHeightEst;""".toString()

            blocksForGeoCalc = prefix prefixName, "BLOCK_RELATION_"
            datasource.execute """DROP TABLE IF EXISTS $blocksForGeoCalc;
                    CREATE TABLE $blocksForGeoCalc AS SELECT ID_BLOCK,  THE_GEOM,ID_RSU FROM $blockIndicatorsForHeightEst;
                    DROP TABLE $blockIndicatorsForHeightEst;""".toString()
        }

        //Compute Geoindicators (at all scales + typologies)
        parameters.nbBuildingEstimated = nbBuildingEstimated
        Map geoIndicators = computeGeoclimateIndicators(datasource, zone,
                buildingForGeoCalc, blocksForGeoCalc,
                rsuForGeoCalc, road, vegetation,
                water, impervious, rail, inputParameters, prefixName)
        if (!geoIndicators) {
            error "Cannot build the geoindicators"
            datasource.dropTable(blocksForGeoCalc, rsuForGeoCalc, buildingIndicatorsForHeightEst, rsuIndicatorsForHeightEst)
            return
        } else {
            //Clean the System properties that stores intermediate table names
            datasource.dropTable(getCachedTableNames())
            clearTablesCache()
            geoIndicators.put("building", buildingTableName)
            datasource.dropTable(blocksForGeoCalc, rsuForGeoCalc, buildingIndicatorsForHeightEst, rsuIndicatorsForHeightEst)
            return geoIndicators
        }
    } else {
        datasource.dropTable(getCachedTableNames())
        clearTablesCache()
        //Create spatial units and relations : building, block, rsu
        Map spatialUnits = createUnitsOfAnalysis(datasource, zone,
                building, road,
                rail, vegetation,
                water, sea_land_mask, "","",
                surface_vegetation,
                surface_hydro,surface_urban_areas, snappingTolerance, indicatorUse,
                prefixName)
        if (!spatialUnits) {
            error "Cannot create the spatial units"
            return null
        }
        def relationBuildings = spatialUnits.building
        def relationBlocks = spatialUnits.block
        rsuTable = spatialUnits.rsu

        Map geoIndicators = computeGeoclimateIndicators(datasource, zone,
                relationBuildings, relationBlocks,
                rsuTable, road, vegetation, water, impervious, rail, inputParameters, prefixName)
        if (!geoIndicators) {
            error "Cannot build the geoindicators"
            return
        } else {
            geoIndicators.put("building", building)
            return geoIndicators
        }
    }
}


/**
 * Compute all geoindicators having no height are computed and then used for building height estimation.
 *
 * @return 5 tables building_table, rsu_table, building_indicators_without_height, block_indicators_without_height,
 * rsu_indicators_without_height
 *          1 integer being the number of buildings having their height estimated
 *
 */
Map estimateBuildingHeight(JdbcDataSource datasource, String zone, String building,
                           String road, String rail, String vegetation,
                           String water, String impervious,
                           String building_estimate, String sea_land_mask, String urban_areas, String rsu,
                           double surface_vegetation, double surface_hydro,double surface_urban_areas,
                           double snappingTolerance, String buildingHeightModelName, String prefixName = "") {
    if (!building_estimate) {
        error "To estimate the building height a table that contains the list of building to estimate must be provided"
        return
    }

    if (!modelCheck(buildingHeightModelName)) {
        return
    }
    info "Geoclimate will try to estimate the building heights with the model $buildingHeightModelName."

    //Create spatial units and relations : building, block, rsu
    Map spatialUnits = createUnitsOfAnalysis(datasource, zone,
            building, road, rail, vegetation,
            water, sea_land_mask, urban_areas, rsu,
            surface_vegetation, surface_hydro,surface_urban_areas, snappingTolerance, ["UTRF"],
            prefixName)
    if (!spatialUnits) {
        error "Cannot create the spatial units"
        return null
    }
    def relationBuildings = spatialUnits.building
    def relationBlocks = spatialUnits.block
    def rsuTable = spatialUnits.rsu

    // Calculates indicators needed for the building height estimation algorithm
    Map geoIndicatorsEstH = computeGeoclimateIndicators(datasource, zone,
            relationBuildings, relationBlocks, rsuTable, road,
            vegetation, water, impervious, rail,
            getParameters([indicatorUse: ["UTRF"]]), prefixName)
    if (!geoIndicatorsEstH) {
        error "Cannot build the geoindicators to estimate the building height"
        return
    }

    datasource.dropTable(relationBlocks)

    //Let's go to select the building's id that must be processed to fix the height
    info "Extracting the building having no height information and estimate it"
    //Select indicators we need at building scales
    def buildingIndicatorsForHeightEst = geoIndicatorsEstH.building_indicators
    def blockIndicatorsForHeightEst = geoIndicatorsEstH.block_indicators
    def rsuIndicatorsForHeightEst = geoIndicatorsEstH.rsu_indicators
    datasource.createIndex(building_estimate, "id_build")
    datasource.createIndex(buildingIndicatorsForHeightEst, "id_build")
    datasource.createIndex(buildingIndicatorsForHeightEst, "id_rsu")

    def estimated_building_with_indicators = "ESTIMATED_BUILDING_INDICATORS_${UUID.randomUUID().toString().replaceAll("-", "_")}"

    datasource.execute """DROP TABLE IF EXISTS $estimated_building_with_indicators;
                                               CREATE TABLE $estimated_building_with_indicators 
                                                        AS SELECT a.*
                                                        FROM $buildingIndicatorsForHeightEst a 
                                                            RIGHT JOIN $building_estimate b 
                                                            ON a.id_build=b.id_build
                                                        WHERE b.ESTIMATED = true AND a.ID_RSU IS NOT NULL;""".toString()

    info "Collect building indicators to estimate the height"

    def gatheredScales = Geoindicators.GenericIndicators.gatherScales(datasource,
            estimated_building_with_indicators,
            blockIndicatorsForHeightEst, rsuIndicatorsForHeightEst,
            "BUILDING", ["AVG", "STD"],
            prefixName)

    def buildingTableName = "BUILDING_TABLE_WITH_RSU_AND_BLOCK_ID"
    int nbBuildingEstimated =0
    def buildEstimatedHeight
    if (datasource.getTable(gatheredScales).isEmpty()) {
        info "No building height to estimate"
        nbBuildingEstimated = 0
        datasource.execute """DROP TABLE IF EXISTS $buildingTableName;
                                           CREATE TABLE $buildingTableName 
                                                AS SELECT  THE_GEOM, ID_BUILD, ID_SOURCE, HEIGHT_WALL ,
                                                    HEIGHT_ROOF, NB_LEV, TYPE, MAIN_USE, ZINDEX, ID_BLOCK, ID_RSU from $estimated_building_with_indicators""".toString()

    } else {
        info "Start estimating the building height"
        //Apply RF model
        buildEstimatedHeight = Geoindicators.TypologyClassification.applyRandomForestModel(datasource,
                gatheredScales, buildingHeightModelName,"id_build", prefixName)
        if (!buildEstimatedHeight) {
            error "Cannot apply the building height model $buildingHeightModelName"
            return
        }

        //Update the abstract building table
        info "Replace the input building table by the estimated height"

        nbBuildingEstimated = datasource.firstRow("select count(*) as count from $buildEstimatedHeight".toString()).count

        //We must format only estimated buildings
        //Apply format on the new abstract table
        def epsg = datasource."$buildEstimatedHeight".srid;
        def formatedBuildEstimatedHeight = formatEstimatedBuilding(datasource, buildEstimatedHeight, epsg)

        datasource.createIndex(formatedBuildEstimatedHeight, "id_build")

        buildingTableName = "INPUT_BUILDING_ESTIMATED_${UUID.randomUUID().toString().replaceAll("-", "_")}"

        //Use build table indicators
        datasource.execute """DROP TABLE IF EXISTS $buildingTableName;
                                               CREATE TABLE $buildingTableName as 
                                                SELECT  a.THE_GEOM, a.ID_BUILD,a.ID_SOURCE,
                                            CASE WHEN b.HEIGHT_ROOF IS NULL THEN a.HEIGHT_WALL ELSE 0 END AS HEIGHT_WALL ,
                                                    COALESCE(b.HEIGHT_ROOF, a.HEIGHT_ROOF) AS HEIGHT_ROOF,
                                                    CASE WHEN b.HEIGHT_ROOF IS NULL THEN a.NB_LEV ELSE 0 END AS NB_LEV, a.TYPE,a.MAIN_USE, a.ZINDEX, a.ID_BLOCK, a.ID_RSU 
                                                from $buildingIndicatorsForHeightEst
                                            a LEFT JOIN $formatedBuildEstimatedHeight b on a.id_build=b.id_build""".toString()


        //Drop intermediate tables
        datasource.execute """DROP TABLE IF EXISTS $estimated_building_with_indicators,
                                            $formatedBuildEstimatedHeight, $buildEstimatedHeight,
                                            $gatheredScales""".toString()
    }

    return ["building"                          : buildingTableName,
            "rsu"                               : rsuTable,
            "building_indicators_without_height": buildingIndicatorsForHeightEst,
            "block_indicators_without_height"   : blockIndicatorsForHeightEst,
            "rsu_indicators_without_height"     : rsuIndicatorsForHeightEst,
            "nb_building_estimated"             : nbBuildingEstimated]
}


/**
 * Compute all geoclimate indicators at the 3 scales (building, block and RSU) plus the typologies (LCZ and UTRF)
 * The LCZ classification  and the urban typology
 *
 * @return 8 tables building_indicators, block_indicators, rsu_indicators,
 * rsu_lcz, zone ,
 * rsu_utrf_area, rsu_utrf_floor_area,
 * building_utrf.
 * The first three tables contains the geoindicators and the last tables the LCZ and urban typology classifications.
 * This table can be empty if the user decides not to calculate it.
 *
 */
Map computeGeoclimateIndicators(JdbcDataSource datasource, String zone, String buildingsWithRelations, String blocksWithRelations, String rsu,
                                String road, String vegetation,
                                String water, String impervious, String rail, Map parameters = [
        "indicatorUse" : ["LCZ", "UTRF", "TEB"], "svfSimplified": false,
        "mapOfWeights" : ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                          "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                          "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
        "utrfModelName": "", "nbEstimatedBuildHeight": 0], String prefixName = "") {
    info "Start computing the geoindicators..."
    def start = System.currentTimeMillis()

    def indicatorUse = parameters.indicatorUse
    //Compute building indicators
    String buildingIndicators = computeBuildingsIndicators(datasource, buildingsWithRelations,
            road, indicatorUse, prefixName)
    if (!buildingIndicators) {
        error "Cannot compute the building indicators"
        return null
    }

    //Compute block indicators
    String blockIndicators = null
    if (indicatorUse*.toUpperCase().contains("UTRF")) {
        blockIndicators = computeBlockIndicators(datasource, buildingIndicators, blocksWithRelations, prefixName)
        if (!blockIndicators) {
            error "Cannot compute the block indicators"
            return null
        }
    }

    //Compute RSU indicators
    def rsuIndicators = computeRSUIndicators(datasource, buildingIndicators,
            rsu, vegetation,
            road, water,
            impervious, rail, parameters, prefixName)
    if (!rsuIndicators) {
        error "Cannot compute the RSU indicators"
        return null
    }

    // Compute the typology indicators (LCZ and UTRF)
    Map computeTypologyIndicators = Geoindicators.WorkflowGeoIndicators.computeTypologyIndicators(datasource,
            buildingIndicators, blockIndicators,
            rsuIndicators, parameters, prefixName)
    if (!computeTypologyIndicators) {
        info "Cannot compute the Typology indicators."
        return
    }
    info "All geoindicators have been computed"
    def rsuLcz = computeTypologyIndicators.rsu_lcz
    def utrfArea = computeTypologyIndicators.rsu_utrf_area
    def utrfFloorArea = computeTypologyIndicators.rsu_utrf_floor_area
    def utrfBuilding = computeTypologyIndicators.building_utrf


    //Populate reporting
    def nbBuilding = datasource.firstRow("select count(*) as count from ${buildingIndicators} WHERE ID_RSU IS NOT NULL".toString()).count
    def nbBlock = 0
    if (blockIndicators) {
        nbBlock = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString()).count
    }
    def nbRSU = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString()).count
    //Alter the zone table to add statics
    if (!datasource.getColumnNames(zone).contains("NB_ESTIMATED_BUILDING")) {
        datasource.execute """ALTER TABLE ${zone} ADD COLUMN (
                    NB_BUILDING INTEGER,
                    NB_ESTIMATED_BUILDING INTEGER,
                    NB_BLOCK INTEGER,
                    NB_RSU INTEGER,
                    COMPUTATION_TIME INTEGER,
                    LAST_UPDATE VARCHAR, VERSION VARCHAR, BUILD_NUMBER VARCHAR
                    )""".toString()
    }

    def nbEstimatedBuildHeight = parameters.nbEstimatedBuildHeight
    //Update reporting to the zone table
    datasource.execute """update ${zone} 
                set nb_estimated_building = ${nbEstimatedBuildHeight}, 
                nb_building = ${nbBuilding}, 
                nb_block =  ${nbBlock},
                nb_rsu = ${nbRSU},
                computation_time = ${(System.currentTimeMillis() - start) / 1000},
                last_update = CAST(now() AS VARCHAR),
                version = '${Geoindicators.version()}',
                build_number = '${Geoindicators.buildNumber()}'""".toString()


    return ["building_indicators": buildingIndicators,
            "block_indicators"   : blockIndicators,
            "rsu_indicators"     : rsuIndicators,
            "rsu_lcz"            : rsuLcz,
            "zone"               : zone,
            "rsu_utrf_area"      : utrfArea,
            "rsu_utrf_floor_area": utrfFloorArea,
            "building_utrf"      : utrfBuilding]
}
/**
 * This process is used to commpute aggregate geoclimate indicators on a grid set as input
 *
 * @param h2gis_datasource the local H2GIS database
 * @param gridTableName the name of the grid table to aggregate the data
 * @param list_indicators indicators names to compute
 * @param buildingTable name
 * @param roadTable name
 * @param vegetationTable name
 * @param hydrographicTable name
 * @param imperviousTable name
 * @param rsu_lcz name
 * @param rsu_utrf_area name
 * @param rsu_utrf_floor_area name
 * @param prefixName for the output table
 * @param outputTableName the name of grid  table in the output_datasource to save the result
 * @return
 */
String rasterizeIndicators(JdbcDataSource datasource,
                           String grid, List list_indicators,
                           String building, String road, String vegetation,
                           String water, String impervious, String rsu_lcz,
                           String rsu_utrf_area, String rsu_utrf_floor_area, String sea_land_mask,
                           String prefixName = "") {
    if (!list_indicators) {
        info "The list of indicator names cannot be null or empty"
        return
    }

    //Concert the list of indicators to upper case

    def list_indicators_upper = list_indicators.collect { it.toUpperCase() }

    def tablesToDrop = []
    // Temporary (and output tables) are created
    def tesselatedSeaLandTab = postfix "TESSELATED_SEA_LAND"
    def seaLandFractionTab = postfix "SEA_LAND_FRACTION"

    def seaLandTypeField = "TYPE"
    def grid_indicators_table = postfix "grid_indicators"
    def grid_column_identifier = "id_grid"
    def indicatorTablesToJoin = [:]
    indicatorTablesToJoin.put(grid, grid_column_identifier)
    /*
    * Make aggregation process with previous grid and current rsu lcz
    */
    if (list_indicators_upper.intersect(["LCZ_FRACTION", "LCZ_PRIMARY"]) && rsu_lcz) {
        def indicatorName = "LCZ_PRIMARY"
        String upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics(
                datasource, grid, grid_column_identifier,
                rsu_lcz, indicatorName, indicatorName,
                false, "lcz")
        if (upperScaleAreaStatistics) {
            indicatorTablesToJoin.put(upperScaleAreaStatistics, grid_column_identifier)

            if (list_indicators_upper.contains("LCZ_PRIMARY")) {
                def resultsDistrib = Geoindicators.GenericIndicators.distributionCharacterization(datasource,
                        upperScaleAreaStatistics, upperScaleAreaStatistics, grid_column_identifier,
                        ["equality", "uniqueness"],
                        "GREATEST", true, true,
                        "lcz")
                // Rename the standard indicators into names consistent with the current method (LCZ type...)
                datasource """  ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_COL TO LCZ_PRIMARY;
                                ALTER TABLE $resultsDistrib RENAME COLUMN UNIQUENESS_VALUE TO LCZ_UNIQUENESS_VALUE;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EQUALITY_VALUE TO LCZ_EQUALITY_VALUE;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_COL2 TO LCZ_SECONDARY;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_VAL TO MIN_DISTANCE;""".toString()

                // Need to replace the string LCZ values by an integer
                datasource.createIndex(resultsDistrib, "lcz_primary")
                datasource.createIndex(resultsDistrib, "lcz_secondary")
                def casewhenQuery1 = ""
                def casewhenQuery2 = ""
                def parenthesis = ""
                // LCZ types need to be String when using the method 'distributionCHaracterization',
                // thus need to define a correspondence table
                def correspondenceMap = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 101, 102, 103, 104, 105, 106, 107]

                correspondenceMap.each { lczInt ->
                    casewhenQuery1 += "CASEWHEN(LCZ_PRIMARY = 'LCZ_PRIMARY_$lczInt', $lczInt, "
                    casewhenQuery2 += "CASEWHEN(LCZ_SECONDARY = 'LCZ_SECONDARY_$lczInt', $lczInt, "
                    parenthesis += ")"
                }
                def distribLczTableInt = postfix "distribLczTableInt"

                datasource """  DROP TABLE IF EXISTS $distribLczTableInt;
                                CREATE TABLE $distribLczTableInt
                                        AS SELECT   $grid_column_identifier, $casewhenQuery1 null$parenthesis AS LCZ_PRIMARY,
                                                    $casewhenQuery2 null$parenthesis AS LCZ_SECONDARY, 
                                                    MIN_DISTANCE, LCZ_UNIQUENESS_VALUE, LCZ_EQUALITY_VALUE 
                                        FROM $resultsDistrib;
                                DROP TABLE IF EXISTS $resultsDistrib""".toString()
                tablesToDrop << resultsDistrib
                indicatorTablesToJoin.put(distribLczTableInt, grid_column_identifier)
            }

        } else {
            info "Cannot aggregate the LCZ at grid scale"
        }
    }

    /*
    * Make aggregation process with previous grid and current rsu urban typo area
    */
    if (list_indicators_upper.contains("UTRF_AREA_FRACTION") && rsu_utrf_area) {
        String indicatorName = "TYPO_MAJ"
        String upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics(datasource,
                grid, grid_column_identifier, rsu_utrf_area,
                indicatorName, "AREA_TYPO_MAJ", false, "utrf_area")
        if (upperScaleAreaStatistics) {
            indicatorTablesToJoin.put(upperScaleAreaStatistics, grid_column_identifier)
        } else {
            info "Cannot aggregate the Urban Typology at grid scale"
        }
    }

    if (list_indicators_upper.contains("UTRF_FLOOR_AREA_FRACTION") && rsu_utrf_floor_area) {
        def indicatorName = "TYPO_MAJ"
        def upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics(datasource,
                grid, grid_column_identifier, rsu_utrf_floor_area, indicatorName, "FLOOR_AREA_TYPO_MAJ", false,
                "utrf_floor_area")
        if (upperScaleAreaStatistics) {
            indicatorTablesToJoin.put(upperScaleAreaStatistics, grid_column_identifier)
        } else {
            info "Cannot aggregate the Urban Typology at grid scale"
        }
    }

    // If any surface fraction calculation is needed, create the priority list containing only needed fractions
    // and also set which type of statistics is needed if "BUILDING_HEIGHT" is activated
    def surfaceFractionsProcess
    def columnFractionsList = [:]
    def priorities = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]

    def unweightedBuildingIndicators = [:]
    def weightedBuildingIndicators = [:]
    def height_roof_unweighted_list = []
    list_indicators_upper.each {
        if (it == "BUILDING_FRACTION"
                || it == "BUILDING_SURFACE_DENSITY" ||
                it == "ASPECT_RATIO") {
            columnFractionsList.put(priorities.indexOf("building"), "building")
        } else if (it == "WATER_FRACTION") {
            columnFractionsList.put(priorities.indexOf("water"), "water")
        } else if (it == "VEGETATION_FRACTION") {
            columnFractionsList.put(priorities.indexOf("high_vegetation"), "high_vegetation")
            columnFractionsList.put(priorities.indexOf("low_vegetation"), "low_vegetation")
        } else if (it == "ROAD_FRACTION") {
            columnFractionsList.put(priorities.indexOf("road"), "road")
        } else if (it == "IMPERVIOUS_FRACTION") {
            columnFractionsList.put(priorities.indexOf("impervious"), "impervious")
        } else if (it == "BUILDING_HEIGHT" && building) {
            height_roof_unweighted_list.addAll(["AVG", "STD"])
        } else if (it == "BUILDING_HEIGHT_WEIGHTED" && building) {
            weightedBuildingIndicators["height_roof"] = ["area": ["AVG", "STD"]]
        } else if (it == "BUILDING_POP" && building) {
            unweightedBuildingIndicators.put("pop", ["SUM"])
        } else if (it == "HEIGHT_OF_ROUGHNESS_ELEMENTS" && building) {
            height_roof_unweighted_list.add("GEOM_AVG")
        }
    }
    if (height_roof_unweighted_list) {
        unweightedBuildingIndicators.put("height_roof", height_roof_unweighted_list)
    }

    // Calculate all surface fractions indicators on the GRID cell
    if (columnFractionsList) {
        def priorities_tmp = columnFractionsList.values().sort()
        // Need to create the smallest geometries used as input of the surface fraction process
        String superpositionsTableGrid = Geoindicators.RsuIndicators.smallestCommunGeometry(datasource,
                grid, grid_column_identifier,
                building, road, water,
                vegetation, impervious, null, prefixName)
        if (superpositionsTableGrid) {
            surfaceFractionsProcess = Geoindicators.RsuIndicators.surfaceFractions(
                    datasource, grid, grid_column_identifier, superpositionsTableGrid,
                    [:], priorities_tmp, prefixName)
            if (surfaceFractionsProcess) {
                indicatorTablesToJoin.put(surfaceFractionsProcess, grid_column_identifier)
            }
            tablesToDrop << superpositionsTableGrid
        } else {
            info "Cannot compute the surface fractions at grid scale"
        }
    }
    String createScalesRelationsGridBl
    String computeBuildingStats
    if (unweightedBuildingIndicators) {
        // Create the relations between grid cells and buildings
        createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(datasource, building, grid,
                grid_column_identifier, null,
                prefixName)
        if (!createScalesRelationsGridBl) {
            info "Cannot compute the scales relations between buildings and grid cells."
            return
        }
        computeBuildingStats = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(datasource, createScalesRelationsGridBl,
                grid, grid_column_identifier,
                grid_column_identifier, unweightedBuildingIndicators,
                prefixName)
        if (!computeBuildingStats) {
            info "Cannot compute the building statistics on grid cells."
            return
        }
        indicatorTablesToJoin.put(computeBuildingStats, grid_column_identifier)

    }

    String buildingCutted
    if (weightedBuildingIndicators) {
        //Cut the building to compute exact fractions
        buildingCutted = cutBuilding(datasource, grid, building)
        if (!buildingCutted) {
            info "Cannot split the building with the grid to compute the weighted statistics"
            return
        }
        def computeWeightedAggregStat = Geoindicators.GenericIndicators.weightedAggregatedStatistics(datasource, buildingCutted,
                grid, grid_column_identifier,
                weightedBuildingIndicators, prefixName)
        if (!computeWeightedAggregStat) {
            info "Cannot compute the weighted aggregated statistics on grid cells."
            return
        }
        indicatorTablesToJoin.put(computeWeightedAggregStat, grid_column_identifier)

    }

    if (list_indicators_upper.contains("BUILDING_TYPE_FRACTION") && building) {
        if (!buildingCutted) {
            buildingCutted = cutBuilding(datasource, grid, building)
            if (!buildingCutted) {
                info "Cannot split the building with the grid to compute the weighted statistics"
                return
            }
        }
        def indicatorName = "TYPE"
        def upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics(datasource, grid,
                grid_column_identifier,
                buildingCutted,
                indicatorName, indicatorName, false,
                "building_type_fraction")
        if (upperScaleAreaStatistics) {
            indicatorTablesToJoin.put(upperScaleAreaStatistics, grid_column_identifier)
            tablesToDrop<<upperScaleAreaStatistics
        } else {
            info "Cannot aggregate the building type at grid scale"
        }
    }


    if ((list_indicators_upper.intersect(["FREE_EXTERNAL_FACADE_DENSITY", "ASPECT_RATIO", "BUILDING_SURFACE_DENSITY"]) && building)) {
        if (!createScalesRelationsGridBl) {
            // Create the relations between grid cells and buildings
            createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(datasource,
                    building, grid, grid_column_identifier, null,
                    prefixName)
            if (!createScalesRelationsGridBl) {
                info "Cannot compute the scales relations between buildings and grid cells."
                return
            }
        }
        def freeFacadeDensityExact = Geoindicators.RsuIndicators.freeExternalFacadeDensityExact(datasource, createScalesRelationsGridBl,
                grid, grid_column_identifier, prefixName)
        if (freeFacadeDensityExact) {
            if (list_indicators_upper.intersect(["FREE_EXTERNAL_FACADE_DENSITY", "ASPECT_RATIO"])) {
                indicatorTablesToJoin.put(freeFacadeDensityExact, grid_column_identifier)
                tablesToDrop<<freeFacadeDensityExact
            }
            if (list_indicators_upper.contains("FREE_EXTERNAL_FACADE_DENSITY")) {
                def buildingSurfDensity = Geoindicators.RsuIndicators.buildingSurfaceDensity(
                        datasource, freeFacadeDensityExact,
                        surfaceFractionsProcess,
                        "FREE_EXTERNAL_FACADE_DENSITY",
                        "building_fraction", grid_column_identifier, prefixName)
                if (buildingSurfDensity) {
                    if (list_indicators_upper.contains("BUILDING_SURFACE_DENSITY")) {
                        indicatorTablesToJoin.put(buildingSurfDensity, grid_column_identifier)
                    }
                }
                tablesToDrop<<freeFacadeDensityExact
                tablesToDrop<<buildingSurfDensity
            }
        } else {
            info "Cannot calculate the exact free external facade density"
        }
    }


    if (list_indicators_upper.contains("BUILDING_HEIGHT_DIST") && building) {
        if (!buildingCutted) {
            buildingCutted = cutBuilding(datasource, grid, building)
            if (!buildingCutted) {
                info "Cannot split the building with the grid to compute the building height distance"
                return
            }
        }
        def roofFractionDistributionExact = Geoindicators.RsuIndicators.roofFractionDistributionExact(datasource,
                grid, buildingCutted, grid_column_identifier,
                [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50], false, prefixName)
        if (roofFractionDistributionExact) {
            indicatorTablesToJoin.put(roofFractionDistributionExact, grid_column_identifier)
        } else {
            info "Cannot compute the roof fraction distribution."
        }
    }

    if (list_indicators_upper.contains("FRONTAL_AREA_INDEX") && building) {
        if (!datasource.getTable(building).isEmpty()) {
            if (!createScalesRelationsGridBl) {
                // Create the relations between grid cells and buildings
                createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(datasource, building,
                        grid, grid_column_identifier, null,
                        prefixName)
                if (!createScalesRelationsGridBl) {
                    info "Cannot compute the scales relations between buildings and grid cells."
                    return
                }
            }
            def frontalAreaIndexDistribution = Geoindicators.RsuIndicators.frontalAreaIndexDistribution(datasource,
                    createScalesRelationsGridBl, grid, grid_column_identifier,
                    [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50], 12, true, prefixName)
            if (frontalAreaIndexDistribution) {
                indicatorTablesToJoin.put(frontalAreaIndexDistribution, grid_column_identifier)
            } else {
                info "Cannot compute the frontal area index."
            }
        }
    }

    if (list_indicators_upper.contains("SEA_LAND_FRACTION") && sea_land_mask) {
        // If only one type of surface (land or sea) is in the zone, no need for computational fraction calculation
        def sea_land_type_rows = datasource.rows("""SELECT $seaLandTypeField, COUNT(*) AS NB_TYPES
                                                                    FROM $sea_land_mask
                                                                    GROUP BY $seaLandTypeField""".toString())
        if (sea_land_type_rows[seaLandTypeField].count("sea") == 0) {
            datasource """ 
                            DROP TABLE IF EXISTS $seaLandFractionTab;
                            CREATE TABLE $seaLandFractionTab
                                AS SELECT $grid_column_identifier, 1 AS LAND_FRACTION
                                FROM $grid"""
            indicatorTablesToJoin.put(seaLandFractionTab, grid_column_identifier)
        } else {
            // Split the potentially big complex seaLand geometries into smaller triangles in order to makes calculation faster
            datasource """
                            DROP TABLE IF EXISTS $tesselatedSeaLandTab;
                            CREATE TABLE $tesselatedSeaLandTab(id_tesselate serial, the_geom geometry, $seaLandTypeField VARCHAR)
                                AS SELECT explod_id, the_geom, $seaLandTypeField
                                FROM ST_EXPLODE('(SELECT st_tessellate(the_geom) AS the_geom, $seaLandTypeField 
                                                    FROM $sea_land_mask
                                                    WHERE ST_DIMENSION(the_geom) = 2 AND ST_ISEMPTY(the_geom) IS NOT TRUE
                                                            AND ST_AREA(the_geom)>0)')"""

            def upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics(datasource,
                    grid, grid_column_identifier,
                    tesselatedSeaLandTab, seaLandTypeField, seaLandTypeField,
                    prefixName)
            tablesToDrop << tesselatedSeaLandTab
            if (upperScaleAreaStatistics) {
                // Modify columns name to postfix with "_FRACTION"
                datasource """ 
                            ALTER TABLE ${upperScaleAreaStatistics} RENAME COLUMN TYPE_LAND TO LAND_FRACTION;
                            ALTER TABLE ${upperScaleAreaStatistics} RENAME COLUMN TYPE_SEA TO SEA_FRACTION;
                            ALTER TABLE ${upperScaleAreaStatistics} DROP COLUMN THE_GEOM;"""
                indicatorTablesToJoin.put(upperScaleAreaStatistics, grid_column_identifier)
            } else {
                info "Cannot compute the frontal area index."
            }
        }
    }


    if (list_indicators_upper.contains("SVF") && building) {
        if (!createScalesRelationsGridBl) {
            // Create the relations between grid cells and buildings
            createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(datasource, building,
                    grid, grid_column_identifier, null,
                    prefixName)
            if (!createScalesRelationsGridBl) {
                info "Cannot compute the scales relations between buildings and grid cells."
                return
            }
        }
        def svf_fraction = Geoindicators.RsuIndicators.groundSkyViewFactor(datasource, grid, grid_column_identifier, createScalesRelationsGridBl, 0.008, 100, 60, "grid")

        if (svf_fraction) {
            datasource """  ALTER TABLE ${svf_fraction} RENAME COLUMN GROUND_SKY_VIEW_FACTOR TO SVF""".toString()
            indicatorTablesToJoin.put(svf_fraction, grid_column_identifier)
        } else {
            info "Cannot compute the sky view factor."
        }
        tablesToDrop << svf_fraction
    }

    if (list_indicators_upper.intersect(["HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS_CLASS"]) && building) {
        def heightColumnName = "height_roof"
        def facadeDensListLayersBottom = [0, 10, 20, 30, 40, 50]
        def facadeDensNumberOfDirection = 12
        if (!createScalesRelationsGridBl) {
            // Create the relations between grid cells and buildings
            createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(datasource,
                    building, grid, grid_column_identifier, null,
                    prefixName)
            if (!createScalesRelationsGridBl) {
                info "Cannot compute the scales relations between buildings and grid cells."
                return
            }
        }
        def frontalAreaIndexDistribution = Geoindicators.RsuIndicators.frontalAreaIndexDistribution(datasource,
                createScalesRelationsGridBl, grid, grid_column_identifier,
                facadeDensListLayersBottom, facadeDensNumberOfDirection, false, prefixName,)
        if (!frontalAreaIndexDistribution) {
            info "Cannot compute the frontal area index at grid scale."
        }

        def tablesToJoin = [:]
        tablesToJoin.put(frontalAreaIndexDistribution, grid_column_identifier)
        tablesToJoin.put(computeBuildingStats, grid_column_identifier)
        tablesToJoin.put(grid, grid_column_identifier)

        tablesToDrop << frontalAreaIndexDistribution

        def grid_for_roughness = Geoindicators.DataUtils.joinTables(datasource, tablesToJoin, "grid_for_roughness")
        if (!grid_for_roughness) {
            info "Cannot prepare the grid roughness indicators."
            return
        }

        tablesToDrop << grid_for_roughness

        def effRoughHeight = Geoindicators.RsuIndicators.effectiveTerrainRoughnessLength(datasource, grid_for_roughness,
                grid_column_identifier,
                "frontal_area_index",
                "geom_avg_$heightColumnName",
                facadeDensListLayersBottom, facadeDensNumberOfDirection, prefixName)
        if (!effRoughHeight) {
            info "Cannot compute the the effective terrain roughness length at grid scale."
            return
        }

        indicatorTablesToJoin.put(effRoughHeight, grid_column_identifier)
        tablesToDrop << effRoughHeight

        if (list_indicators_upper.contains("TERRAIN_ROUGHNESS_CLASS")) {
            def roughClass = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass(datasource, effRoughHeight,
                    grid_column_identifier, "effective_terrain_roughness_length", prefixName)
            if (!roughClass) {
                info "Cannot compute the roughness class."
                return
            }
            indicatorTablesToJoin.put(roughClass, grid_column_identifier)
            tablesToDrop << roughClass
        }
    }


    //Join all indicators at grid scale
    def joinGrids = Geoindicators.DataUtils.joinTables(datasource, indicatorTablesToJoin, grid_indicators_table)
    if (!joinGrids) {
        info "Cannot merge all indicators in grid table $grid_indicators_table."
        return
    }

    //Compute the aspect_ratio
    if (list_indicators_upper.contains("ASPECT_RATIO") && building) {
        //Because all other indicators have been yet computed we just alter the table with the aspect_ratio column
        datasource.execute("""
        ALTER TABLE $grid_indicators_table ADD COLUMN ASPECT_RATIO DOUBLE PRECISION;
        UPDATE $grid_indicators_table SET ASPECT_RATIO = case when building_fraction = 1 then null else  free_external_facade_density / (1 - building_fraction) end
        """.toString())
    }
    tablesToDrop << createScalesRelationsGridBl
    tablesToDrop << buildingCutted

    datasource.dropTable(indicatorTablesToJoin.keySet().toArray(new String[0]))

    // Remove temporary tables
    datasource.dropTable(tablesToDrop)
    return grid_indicators_table
}

/**
 * Method to cut the building with a grid or any other table
 * @param datasource
 * @param grid
 * @param building
 * @return
 */
String cutBuilding(JdbcDataSource datasource, String grid, String building) {
    String buildingCutted = postfix("building_cutted")
    datasource.createSpatialIndex(grid)
    datasource.createSpatialIndex(building)
    try {
        datasource.execute("""
        DROP TABLE IF EXISTS $buildingCutted;
        CREATE TABLE $buildingCutted as 
        SELECT *, ST_AREA(THE_GEOM) AS area from 
        (SELECT a.* EXCEPT(the_geom), st_intersection(a.the_geom, b.the_geom) as the_geom, b.* EXCEPT(the_geom),
        (b.HEIGHT_WALL + b.HEIGHT_ROOF) / 2 AS BUILD_HEIGHT
        FROM $grid as a, $building as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom)) 
        as foo
        """.toString())
    }
    catch (SQLException ex) {
        buildingCutted = null
    }
    return buildingCutted
}
/**
 * This process is used to create a grid to aggregate indicators
 *
 * @param h2gis_datasource the local H2GIS database
 * @param zoneEnvelopeTableName
 * @param x_size x size of the grid
 * @param y_size y size of the grid
 * @param srid used to reproject the grid
 * @param outputTableName the name of grid  table
 * @return
 */
String createGrid(JdbcDataSource datasource,
                  Geometry envelope,
                  int x_size, int y_size,
                  int srid, boolean rowCol = false) {
    //Start to compute the grid
    def grid_table_name = Geoindicators.SpatialUnits.createGrid(datasource, envelope, x_size, y_size, rowCol)
    if (grid_table_name) {
        //Reproject the grid in the local UTM
        if (envelope.getSRID() == 4326) {
            def reprojectedGrid = postfix("grid")
            datasource.execute """drop table if exists $reprojectedGrid; 
                    create table $reprojectedGrid as  select st_transform(the_geom, $srid) as the_geom, id_grid, id_col, id_row from $grid_table_name""".toString()
            grid_table_name = reprojectedGrid
        }
        return grid_table_name
    }
}

/**
 * This process is used to re-format the building table when the estimated height RF is used.
 * It must be used to update the nb level according the estimated height roof value
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @param epsg srid code of the output table
 * @return The name of the final buildings table
 */
String formatEstimatedBuilding(JdbcDataSource datasource, String inputTableName, int epsg, float h_lev_min = 3) {
    def outputTableName = postfix "INPUT_BUILDING_REFORMATED_"
    info 'Re-formating building layer'
    def outputEstimateTableName = ""
    datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build INTEGER, ID_SOURCE VARCHAR, 
                    HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER, ID_BLOCK INTEGER, ID_RSU INTEGER);
            """
    if (inputTableName) {
        def queryMapper = "SELECT "
        def inputSpatialTable = datasource."$inputTableName"
        if (inputSpatialTable.rowCount > 0) {
            def columnNames = inputSpatialTable.columns
            queryMapper += " ${columnNames.join(",")} FROM $inputTableName"
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    def heightRoof = row.height_roof
                    def type = row.type
                    def formatedData = formatHeightsAndNbLevels(null, heightRoof, 0, h_lev_min,
                            type, null)
                    def nbLevels = formatedData.nbLevels
                    def heightWall = formatedData.heightWall
                    stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${row.the_geom}',$epsg), 
                                                    ${row.id_build}, 
                                                    '${row.id_source}',
                                                    ${heightWall},
                                                    ${heightRoof},
                                                    ${nbLevels},
                                                    '${type}',
                                                    '${row.main_use}',
                                                    ${row.zindex},
                                                    ${row.id_block},${row.id_rsu})
                                            """.toString()

                }
            }
        }
    }
    info 'Re-formating building finishes'
    return outputTableName
}


/**
 * Rule to guarantee the height wall, height roof and number of levels values
 * @param heightWall value
 * @param heightRoof value
 * @param nbLevels value
 * @param h_lev_min value
 * @return a map with the new values
 */
static Map formatHeightsAndNbLevels(def heightWall, def heightRoof, def nbLevels, def h_lev_min,
                                    def buildingType, def levelBuildingTypeMap) {
    //Use the BDTopo values
    if (heightWall != 0 && heightRoof != 0 && nbLevels != 0) {
        return [heightWall: heightWall, heightRoof: heightRoof, nbLevels: nbLevels, estimated: false]
    }
    //Initialisation of heights and number of levels
    // Update height_wall
    boolean estimated = false
    if (heightWall == 0) {
        if (!heightRoof || heightRoof == 0) {
            if (nbLevels == 0) {
                nbLevels = levelBuildingTypeMap[buildingType]
                if (!nbLevels) {
                    nbLevels = 1
                }
                heightWall = h_lev_min * nbLevels
                heightRoof = heightWall
                estimated = true
            } else {
                heightWall = h_lev_min * nbLevels
                heightRoof = heightWall
            }
        } else {
            heightWall = heightRoof
            nbLevels = Math.max(Math.floor(heightWall / h_lev_min), 1)
        }
    } else if (heightWall == heightRoof) {
        if (nbLevels == 0) {
            nbLevels = Math.max(Math.floor(heightWall / h_lev_min), 1)
        }
    }
    // Control of heights and number of levels
    // Check if height_roof is lower than height_wall. If yes, then correct height_roof
    else if (heightWall > heightRoof) {
        heightRoof = heightWall
        if (nbLevels == 0) {
            nbLevels = Math.max(Math.floor(heightWall / h_lev_min), 1)
        }
    } else if (heightRoof > heightWall) {
        if (nbLevels == 0) {
            nbLevels = Math.max(Math.floor(heightRoof / h_lev_min), 1)
        }
    }
    return [heightWall: heightWall, heightRoof: heightRoof, nbLevels: nbLevels, estimated: estimated]

}


/**
 * This utility method checks if a model exists
 *  - the model name must have the extension .model
 *  - the file must exists
 *  - if the file doesn't exist we try to download it on the geoclimate/model repository
 * @param modelName
 * @return true if the model exists or if we can download it on the repository
 */
static boolean modelCheck(String modelName) {
    if (!modelName) {
        error "Cannot find any model file"
        return
    }
    File inputModelFile = new File(modelName)
    def baseNameModel = FilenameUtils.getBaseName(modelName)
    if (!inputModelFile.exists()) {
        //We try to find this model in geoclimate
        def modelURL = "https://github.com/orbisgis/geoclimate/raw/master/models/${baseNameModel}.model"
        def localInputModelFile = new File(System.getProperty("user.home") + File.separator + ".geoclimate" + File.separator + baseNameModel + ".model")
        // The model doesn't exist on the local folder we download it
        if (!localInputModelFile.exists()) {
            FileUtils.copyURLToFile(new URL(modelURL), localInputModelFile)
            if (!localInputModelFile.exists()) {
                error "Cannot find any model file"
                return
            }
        }
    } else {
        if (!FilenameUtils.isExtension(modelName, "model")) {
            error "The extension of the model file must be .model"
            return
        }
    }
    return true
}