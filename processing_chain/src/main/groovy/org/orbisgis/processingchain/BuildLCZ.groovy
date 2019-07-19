package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.geoindicators.Geoindicators
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript ProcessingChain processingChain

/** The processing chain calculates the 7 geometric and surface cover properties used to identify local climate
 * zones (sky view factor, aspect ratio, building surface fraction, impervious surface fraction,
 * pervious surface fraction, height of roughness elements and terrain roughness class).
 *
 * @param buildingTable The table where are stored informations concerning buildings (and the id of the corresponding rsu)
 * @param rsuTable The table where are stored informations concerning RSU
 * @param roadTable The table where are stored informations concerning roads
 * @param vegetationTable The table where are stored informations concerning vegetation
 * @param hydrographicTable The table where are stored informations concerning water
 * @param facadeDensListLayersBottom the list of height corresponding to the bottom of each vertical layers used for calculation
 * of the rsu_projected_facade_area_density which is then used to calculate the height of roughness (default [0]
 * @param facadeDensNumberOfDirection The number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 8)
 * @param pointDensity The density of points (nb / free m²) used to calculate the spatial average SVF (default 0.008)
 * @param rayLength The maximum distance to consider an obstacle as potential sky cover (default 100)
 * @param numberOfDirection the number of directions considered to calculate the SVF (default 60)
 * @param heightColumnName The name of the column (in the building table) used for roughness height calculation (default "height_roof")
 * @param mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
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
 * @return outputTableName Table name where are stored the resulting RSU
 */
IProcess createLCZ() {
    return create({
        title "Create the LCZ"
        inputs datasource: JdbcDataSource, prefixName: String, buildingTable: String, rsuTable: String, roadTable: String,
                vegetationTable: String, hydrographicTable: String, facadeDensListLayersBottom: [0, 50, 200],
                facadeDensNumberOfDirection: 8, svfPointDensity: 0.008, svfRayLength: 100,
                svfNumberOfDirection: 60, heightColumnName: "height_roof",
                mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_class": 1],
                fractionTypePervious: ["low_vegetation", "water"], fractionTypeImpervious: ["road"], inputFields: ["id_build", "the_geom"],
                levelForRoads: [0]
        outputs outputTableName: String
        run { JdbcDataSource datasource, prefixName, buildingTable, rsuTable, roadTable, vegetationTable,
              hydrographicTable, facadeDensListLayersBottom, facadeDensNumberOfDirection,
              svfPointDensity, svfRayLength, svfNumberOfDirection,
              heightColumnName,
              mapOfWeights,
              fractionTypePervious,
              fractionTypeImpervious, inputFields, levelForRoads ->
            logger.info("Create the LCZ...")

            // To avoid overwriting the output files of this step, a unique identifier is created
            def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

            // Temporary table names
            def lczIndicTable = "lczIndicTable" + uid_out
            def rsu_indic0 = "rsu_indic0" + uid_out
            def rsu_indic1 = "rsu_indic1" + uid_out
            def rsu_indic2 = "rsu_indic2" + uid_out
            def rsu_indic3 = "rsu_indic3" + uid_out

            // Output table name
            def outputTableName = "lczTable"

            def veg_type = []
            def perv_type = []
            def imp_type = []
            def surf_fractions = [:]
            def columnIdRsu = "id_rsu"
            def columnIdBu = "id_build"
            def geometricColumn = "the_geom"
            def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"             : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                 "DENS_AREA"                        : "BUILDING_SURFACE_FRACTION",
                                 "RSU_ASPECT_RATIO"                 : "ASPECT_RATIO",
                                 "RSU_GROUND_SKY_VIEW_FACTOR"       : "SKY_VIEW_FACTOR",
                                 "PERVIOUS_FRACTION"                : "PERVIOUS_SURFACE_FRACTION",
                                 "IMPERVIOUS_FRACTION"              : "IMPERVIOUS_SURFACE_FRACTION",
                                 "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS": "TERRAIN_ROUGHNESS_CLASS"]


            //Compute building indicators
            def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
            if (!computeBuildingsIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: buildingTable,
                                                     inputRoadTableName    : roadTable,
                                                     indicatorUse          : ["LCZ"]])) {
                logger.info("Cannot compute building indicators.")
                return
            }
            String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName

            //Compute RSU indicators
            def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
            if (!computeRSUIndicators.execute([datasource       : datasource,
                                               buildingTable    : buildingIndicators,
                                               rsuTable         : rsuTable,
                                               vegetationTable  : vegetationTable,
                                               roadTable        : roadTable,
                                               hydrographicTable: hydrographicTable,
                                               indicatorUse     : ["LCZ"]])) {
                logger.info("Cannot compute the RSU indicators.")
                return
            }
            String rsuIndicators = computeRSUIndicators.getResults().outputTableName
            /*
                // I. Calculate preliminary indicators needed for the other calculations (the relations of chaining between
                // the indicators are illustrated with the scheme IProcessA --> IProcessB)
                // calc_building_area --> calc_build_densityNroughness
                IProcess calc_building_area = Geoindicators.GenericIndicators.geometryProperties()
                calc_building_area.execute([inputTableName: buildingTable, inputFields: inputFields,
                                            operations    : ["st_area"], prefixName: prefixName, datasource: datasource])

                // calc_veg_frac --> calc_perviousness_frac  (calculate only if vegetation considered as pervious)
                if (fractionTypePervious.contains("low_vegetation") | fractionTypePervious.contains("high_vegetation")) {
                    if (fractionTypePervious.contains("low_vegetation") & fractionTypePervious.contains("high_vegetation")) {
                        veg_type.add("all")
                        perv_type.add("all_vegetation_fraction")
                    } else if (fractionTypePervious.contains("low_vegetation")) {
                        veg_type.add("low")
                        perv_type.add("low_vegetation_fraction")
                    } else if (fractionTypePervious.contains("high_vegetation")) {
                        veg_type.add("high")
                        perv_type.add("high_vegetation_fraction")
                    }
                    IProcess calc_veg_frac = Geoindicators.RsuIndicators.vegetationFraction()
                    calc_veg_frac.execute([rsuTable  : rsuTable, vegetTable: vegetationTable, fractionType: veg_type,
                                           prefixName: prefixName, datasource: datasource])
                    // Add the table in the map that will be used to join the vegetation field with the RSU table
                    // in order to be used to calculate the pervious and impervious surface fractions
                    surf_fractions[calc_veg_frac.results.outputTableName]=columnIdRsu
                }

                // calc_road_frac --> calc_perviousness_frac  (calculate only if road considered as impervious)
                if (fractionTypeImpervious.contains("road")) {
                    imp_type.add("ground_road_fraction")
                    IProcess calc_road_frac = Geoindicators.RsuIndicators.roadFraction()
                    calc_road_frac.execute([rsuTable: rsuTable, roadTable: roadTable, levelToConsiders:
                            ["ground": levelForRoads], prefixName: prefixName, datasource: datasource])
                    // Add the table in the map that will be used to join the vegetation field with the RSU table
                    // in order to be used to calculate the pervious and impervious surface fractions
                    surf_fractions[calc_road_frac.results.outputTableName]=columnIdRsu
                }

                // calc_water_frac --> calc_perviousness_frac  (calculate only if water considered as pervious)
                if (fractionTypePervious.contains("water")) {
                    perv_type.add("water_fraction")
                    IProcess calc_water_frac = Geoindicators.RsuIndicators.waterFraction()
                    calc_water_frac.execute([rsuTable  : rsuTable, waterTable: hydrographicTable, prefixName: "test",
                                             datasource: datasource])
                    // Add the table in the map that will be used to join the vegetation field with the RSU table
                    // in order to be used to calculate the pervious and impervious surface fractions
                    surf_fractions[calc_water_frac.results.outputTableName]=columnIdRsu
                }

                // calc_build_contiguity    -->
                //                                  calc_free_ext_density --> calc_aspect_ratio
                // calc_build_facade_length -->
                IProcess calc_build_contiguity = Geoindicators.BuildingIndicators.neighborsProperties()
                calc_build_contiguity.execute([inputBuildingTableName: buildingTable,
                                               operations            : ["building_contiguity"], prefixName: prefixName, datasource: datasource])
                IProcess calc_build_facade_length = Geoindicators.BuildingIndicators.sizeProperties()
                calc_build_facade_length.execute([inputBuildingTableName: buildingTable,
                                                  operations            : ["building_total_facade_length"], prefixName: prefixName,
                                                  datasource            : datasource])
                IProcess join_for_input_facade_dens = Geoindicators.DataUtils.joinTables()
                join_for_input_facade_dens.execute([inputTableNamesWithId: [(buildingTable)                                 : columnIdBu,
                                                                          (calc_build_facade_length.results.outputTableName): columnIdBu,
                                                                          (calc_build_contiguity.results.outputTableName)   : columnIdBu],
                                                  outputTableName        : "tab4facdens", datasource: datasource])
                IProcess calc_free_ext_density = Geoindicators.RsuIndicators.freeExternalFacadeDensity()
                calc_free_ext_density.execute([buildingTable            : join_for_input_facade_dens.results.outputTableName,
                                               rsuTable                 : rsuTable, buContiguityColumn : "building_contiguity",
                                               buTotalFacadeLengthColumn: "building_total_facade_length",
                                               prefixName               : prefixName, datasource: datasource])

                // calc_proj_facade_dist --> calc_effective_roughness_height
                IProcess calc_proj_facade_dist = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
                calc_proj_facade_dist.execute([buildingTable   : buildingTable, rsuTable: rsuTable,
                                               listLayersBottom: facadeDensListLayersBottom, numberOfDirection: facadeDensNumberOfDirection,
                                               prefixName      : "test", datasource: datasource])

                // II. Calculate the LCZ indicators
                // Calculate the BUILDING SURFACE FRACTION from the building area
                // AND the HEIGHT OF ROUGHNESS ELEMENTS from the building roof height
                // calc_build_densityNroughness --> calcSVF (which needs building_density)
                // calc_build_densityNroughness --> calc_effective_roughness_height (which needs geometric_height)
                IProcess join_for_densityNroughness = Geoindicators.DataUtils.joinTables()
                join_for_densityNroughness.execute([inputTableNamesWithId: [(calc_building_area.results.outputTableName): columnIdBu,
                                                                            (buildingTable): columnIdBu],
                                                    outputTableName      : "tab4roughness", datasource: datasource])
                IProcess calc_build_densityNroughness = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
                calc_build_densityNroughness.execute([inputLowerScaleTableName: join_for_densityNroughness.results.outputTableName,
                                                      inputUpperScaleTableName: rsuTable, inputIdUp: columnIdRsu,
                                                      inputVarAndOperations   : [(heightColumnName): ["GEOM_AVG"], "AREA": ["DENS"]],
                                                      prefixName              : prefixName, datasource: datasource])

                // Calculate the SKY VIEW FACTOR from the RSU building density
                // Merge the geometric average and the building density into the RSU table
                IProcess join_for_SVF = Geoindicators.DataUtils.joinTables()
                join_for_SVF.execute([inputTableNamesWithId: [(calc_build_densityNroughness.results.outputTableName): columnIdRsu,
                                                                            (rsuTable): columnIdRsu],
                                      outputTableName      : "tab4svf", datasource: datasource])
                IProcess calcSVF = Geoindicators.RsuIndicators.groundSkyViewFactor()
                calcSVF.execute([rsuTable           : join_for_SVF.results.outputTableName, correlationBuildingTable: buildingTable, rsuBuildingDensityColumn:
                        "dens_area", pointDensity: svfPointDensity, rayLength: svfRayLength,
                                 numberOfDirection  : svfNumberOfDirection, prefixName: prefixName, datasource: datasource])

                // Calculate the ASPECT RATIO from the building fraction and the free external facade density
                // Merge the free external facade density into the RSU table containing the other indicators
                IProcess join_for_aspect_ratio = Geoindicators.DataUtils.joinTables()
                join_for_aspect_ratio.execute([inputTableNamesWithId: [(join_for_SVF.results.outputTableName): columnIdRsu,
                                                              (calc_free_ext_density.results.outputTableName): columnIdRsu],
                                               outputTableName: "tab4aspratio", datasource: datasource])
                IProcess calc_aspect_ratio = Geoindicators.RsuIndicators.aspectRatio()
                calc_aspect_ratio.execute([rsuTable                                 : join_for_aspect_ratio.results.outputTableName,
                                           rsuFreeExternalFacadeDensityColumn       : "rsu_free_external_facade_density",
                                           rsuBuildingDensityColumn                 : "dens_area",
                                           prefixName                               : prefixName, datasource: datasource])

                // Calculate the PERVIOUS AND IMPERVIOUS SURFACE FRACTIONS
                // Add the building density field in the surface fraction tables used for pervious and impervious fractions
                // if the buildings are considered in the impervious fractions
                if (fractionTypeImpervious.contains("building")) {
                    // Add the table in the map that will be used to join the vegetation field with the RSU table
                    // in order to be used to calculate the pervious and impervious surface fractions
                    surf_fractions[calc_build_densityNroughness.results.outputTableName]=columnIdRsu
                }
                IProcess joinFracSurf = Geoindicators.DataUtils.joinTables()
                joinFracSurf.execute([inputTableNamesWithId: surf_fractions,
                                      outputTableName      : "tab4perv", datasource: datasource])
                IProcess calc_perviousness_frac = Geoindicators.RsuIndicators.perviousnessFraction()
                calc_perviousness_frac.execute([rsuTable  : joinFracSurf.results.outputTableName,
                                                operationsAndComposition: ["pervious_fraction": perv_type,
                                                                           "impervious_fraction": imp_type],
                                                prefixName: prefixName, datasource: datasource])

                // Calculate the TERRAIN ROUGHNESS CLASS
                // Merge the geometric average and the projected facade area distribution into the RSU table
                IProcess join4Roughness = Geoindicators.DataUtils.joinTables()
                join4Roughness.execute([inputTableNamesWithId   : [(rsuTable)                                              :columnIdRsu,
                                                                   (calc_build_densityNroughness.results.outputTableName)  :columnIdRsu,
                                                                   (calc_proj_facade_dist.results.outputTableName)         :columnIdRsu],
                                        outputTableName         : "tab4perv",
                                        datasource              : datasource])
                // calc_effective_roughness_height --> calc_roughness_class
                IProcess calc_effective_roughness_height = Geoindicators.RsuIndicators.effectiveTerrainRoughnessHeight()
                calc_effective_roughness_height.execute([rsuTable                           : join4Roughness.results.outputTableName,
                                                         projectedFacadeAreaName            : "rsu_projected_facade_area_distribution",
                                                         geometricMeanBuildingHeightName    : "geom_avg_$heightColumnName",
                                                         prefixName                         : prefixName,
                                                         listLayersBottom                   : facadeDensListLayersBottom,
                                                         numberOfDirection                  : facadeDensNumberOfDirection,
                                                         datasource                         : datasource])
                IProcess calc_roughness_class = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass()
                calc_roughness_class.execute([datasource                        : datasource,
                                              rsuTable                          : calc_effective_roughness_height.results.outputTableName,
                                              effectiveTerrainRoughnessHeight   : "rsu_effective_terrain_roughness",
                                              prefixName                        : prefixName])

                // III. Define the LCZ of each RSU according to their 7 geometric and surface cover properties
                // Merge all indicator columns in one table
                IProcess join_final_table = Geoindicators.DataUtils.joinTables()
                join_final_table.execute([inputTableNamesWithId:
                        [(join_for_SVF.results.outputTableName)          : columnIdRsu,
                         (calc_aspect_ratio.results.outputTableName)     : columnIdRsu,
                         (calcSVF.results.outputTableName)               : columnIdRsu,
                         (calc_perviousness_frac.results.outputTableName): columnIdRsu,
                         (calc_roughness_class.results.outputTableName)  : columnIdRsu],
                                          outputTableName      : "_allindic", datasource: datasource])*/

            // Rename the indicators in order to be consistent with the LCZ ones
            String queryReplaceNames = ""

            lczIndicNames.each { oldIndic, newIndic ->
                queryReplaceNames += "ALTER TABLE $rsuIndicators ALTER COLUMN $oldIndic RENAME TO $newIndic;"
            }
            // Keep only the ID, geometry column and the 7 indicators useful for LCZ classification
            datasource.execute "$queryReplaceNames"
            datasource.execute "CREATE TABLE $lczIndicTable AS SELECT $columnIdRsu, $geometricColumn, " +
                    "${lczIndicNames.values().join(",")} FROM $rsuIndicators"

                // The classification algorithm is called
                IProcess classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
                if(!classifyLCZ.execute([rsuLczIndicators   : lczIndicTable,
                                     normalisationType  : "AVG",
                                     mapOfWeights       : mapOfWeights,
                                     prefixName         : prefixName,
                                     datasource         : datasource])){
                    logger.info("Cannot compute the LCZ classification.")
                    return
                }


            datasource.execute("DROP TABLE IF EXISTS $rsu_indic0, $rsu_indic1, $rsu_indic2, $rsu_indic3".toString())


            [outputTableName: classifyLCZ.results.outputTableName]
        }
    })
}
