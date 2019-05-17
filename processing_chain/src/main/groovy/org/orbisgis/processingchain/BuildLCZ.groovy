package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.Geoclimate
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
public static IProcess createLCZ(){
    return processFactory.create("Create the LCZ",
            [datasource                 : JdbcDataSource, prefixName: String, buildingTable: String, rsuTable: String, roadTable: String,
             vegetationTable            : String, hydrographicTable: String, facadeDensListLayersBottom: double[],
             facadeDensNumberOfDirection: int, svfPointDensity: double, svfRayLength: double,
             svfNumberOfDirection       : int, heightColumnName: String,
             fractionTypePervious       : String[], fractionTypeImpervious: String[], inputFields: String[],
             levelForRoads              : String[]],
            [outputTableName: String],
            { datasource, prefixName, buildingTable, rsuTable, roadTable, vegetationTable,
              hydrographicTable, facadeDensListLayersBottom = [0, 50, 200], facadeDensNumberOfDirection = 8,
              svfPointDensity = 0.008, svfRayLength = 100, svfNumberOfDirection = 60,
              heightColumnName = "height_roof", fractionTypePervious = ["low_vegetation", "water"],
              fractionTypeImpervious = ["road"], inputFields = ["id_build", "the_geom"], levelForRoads = [0] ->
                logger.info("Create the LCZ...")


                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def rsu_surf_fractions = "rsu_surf_fractions" + uid_out
                def rsu_indic0 = "rsu_indic0" + uid_out
                def rsu_indic1 = "rsu_indic1" + uid_out
                def rsu_indic2 = "rsu_indic2" + uid_out
                def rsu_indic3 = "rsu_indic3" + uid_out
                def buAndIdRsu = "buAndIdRsu" + uid_out
                def bu_tempo0 = "bu_tempo0" + uid_out
                def bu_tempo1 = "bu_tempo1" + uid_out
                def rsu_all_indic = "rsu_all_indic" + uid_out
                def rsu_4_aspectratio = "rsu_4_aspectratio" + uid_out
                def rsu_4_roughness0 = "rsu_4_roughness0" + uid_out
                def rsu_4_roughness1 = "rsu_4_roughness1" + uid_out
                datasource.execute(("DROP TABLE IF EXISTS $rsu_surf_fractions; CREATE TABLE $rsu_surf_fractions " +
                        "AS SELECT * FROM $rsuTable").toString())

                def veg_type = []
                def perv_type = []
                def imp_type = []
                def columnIdRsu = "id_rsu"
                def columnIdBu = "id_build"
                def geometricColumn = "the_geom"

                // I. Calculate preliminary indicators needed for the other calculations (the relations of chaining between
                // the indicators are illustrated with the scheme IProcessA --> IProcessB)
                // calc_building_area --> calc_build_densityNroughness
                IProcess  calc_building_area =  Geoclimate.GenericIndicators.geometryProperties()
                calc_building_area.execute([inputTableName: buildingTable, inputFields:inputFields,
                                            operations: ["st_area"], prefixName : prefixName, datasource:datasource])

                // calc_veg_frac --> calc_perviousness_frac  (calculate only if vegetation considered as pervious)
                if(fractionTypePervious.contains("low_vegetation") | fractionTypePervious.contains("high_vegetation")){
                    if(fractionTypePervious.contains("low_vegetation") & fractionTypePervious.contains("high_vegetation")){
                        veg_type.add("all")
                        perv_type.add("all_vegetation_fraction")
                    }
                    else if(fractionTypePervious.contains("low_vegetation")){
                        veg_type.add("low")
                        perv_type.add("low_vegetation_fraction")
                    }
                    else if(fractionTypePervious.contains("high_vegetation")){
                        veg_type.add("high")
                        perv_type.add("high_vegetation_fraction")
                    }
                    IProcess  calc_veg_frac =  Geoclimate.RsuIndicators.vegetationFraction()
                    calc_veg_frac.execute([rsuTable: rsuTable, vegetTable: vegetationTable, fractionType: veg_type,
                                           prefixName: prefixName, datasource: datasource])
                    // Add the vegetation field in the surface fraction tables used for pervious and impervious fractions
                    datasource.execute(("ALTER TABLE $rsu_surf_fractions ADD COLUMN ${perv_type[0]} DOUBLE; " +
                            "UPDATE $rsu_surf_fractions SET ${perv_type[0]} = (SELECT b.${perv_type[0]} FROM " +
                            "${calc_veg_frac.results.outputTableName} b WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)").toString())
                }

                // calc_road_frac --> calc_perviousness_frac  (calculate only if road considered as impervious)
                if(fractionTypeImpervious.contains("road")){
                    imp_type.add("ground_road_fraction")
                    IProcess  calc_road_frac =  Geoclimate.RsuIndicators.roadFraction()
                    calc_road_frac.execute([rsuTable: rsuTable, roadTable: roadTable, levelToConsiders:
                            ["ground" : levelForRoads], prefixName: prefixName, datasource: datasource])
                    // Add the road field in the surface fraction tables used for pervious and impervious fractions
                    datasource.execute(("ALTER TABLE $rsu_surf_fractions ADD COLUMN ground_road_fraction DOUBLE; " +
                            "UPDATE $rsu_surf_fractions SET ground_road_fraction = (SELECT b.ground_road_fraction" +
                            " FROM ${calc_road_frac.results.outputTableName} b " +
                            "WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)").toString())
                }

                // calc_water_frac --> calc_perviousness_frac  (calculate only if water considered as pervious)
                if(fractionTypePervious.contains("water")){
                    perv_type.add("water_fraction")
                    IProcess  calc_water_frac =  Geoclimate.RsuIndicators.waterFraction()
                    calc_water_frac.execute([rsuTable: rsuTable, waterTable: hydrographicTable, prefixName: "test",
                                             datasource: datasource])
                    // Add the water field in the surface fraction tables used for pervious and impervious fractions
                    datasource.execute(("ALTER TABLE $rsu_surf_fractions ADD COLUMN water_fraction DOUBLE; " +
                            "UPDATE $rsu_surf_fractions SET water_fraction = (SELECT b.water_fraction" +
                            " FROM ${calc_water_frac.results.outputTableName} b " +
                            "WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)").toString())
                }

                // calc_build_contiguity    -->
                //                                  calc_free_ext_density --> calc_aspect_ratio
                // calc_build_facade_length -->
                IProcess  calc_build_contiguity =  Geoclimate.BuildingIndicators.neighborsProperties()
                calc_build_contiguity.execute([inputBuildingTableName: buildingTable,
                                               operations:["building_contiguity"], prefixName : prefixName,datasource:datasource])

                IProcess  calc_build_facade_length =  Geoclimate.BuildingIndicators.sizeProperties()
                calc_build_facade_length.execute([inputBuildingTableName: buildingTable,
                                                  operations:["building_total_facade_length"], prefixName : prefixName,
                                                  datasource:datasource])

                // This SQL query should not be done if the following IProcess were normally coded...
                datasource.execute(("DROP TABLE IF EXISTS $bu_tempo0; CREATE TABLE $bu_tempo0 AS SELECT a.*," +
                        "b.building_contiguity FROM $buildingTable a, ${calc_build_contiguity.results.outputTableName} b " +
                        "WHERE a.$columnIdBu = b.$columnIdBu; DROP TABLE IF EXISTS $bu_tempo1; " +
                        "CREATE TABLE $bu_tempo1 AS SELECT a.*," +
                        "b.building_total_facade_length FROM $bu_tempo0 a, ${calc_build_facade_length.results.outputTableName} b " +
                        "WHERE a.$columnIdBu = b.$columnIdBu;").toString())
                IProcess  calc_free_ext_density =  Geoclimate.RsuIndicators.freeExternalFacadeDensity()
                calc_free_ext_density.execute([buildingTable: bu_tempo1, rsuTable: rsuTable,
                                               buContiguityColumn: "building_contiguity", buTotalFacadeLengthColumn:
                                                       "building_total_facade_length", prefixName: prefixName, datasource: datasource])

                // calc_proj_facade_dist --> calc_effective_roughness_height
                IProcess  calc_proj_facade_dist =  Geoclimate.RsuIndicators.projectedFacadeAreaDistribution()
                calc_proj_facade_dist.execute([buildingTable: buildingTable, rsuTable: rsuTable,
                                               listLayersBottom: facadeDensListLayersBottom, numberOfDirection: facadeDensNumberOfDirection,
                                               prefixName: "test", datasource: datasource])

                // II. Calculate the LCZ indicators
                // Calculate the BUILDING SURFACE FRACTION from the building area
                // AND the HEIGHT OF ROUGHNESS ELEMENTS from the building roof height
                // calc_build_densityNroughness --> calcSVF (which needs building_density)
                // calc_build_densityNroughness --> calc_effective_roughness_height (which needs geometric_height)
                IProcess  calc_build_densityNroughness =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
                datasource.execute(("DROP TABLE IF EXISTS $buAndIdRsu; CREATE TABLE $buAndIdRsu AS SELECT a.*," +
                        "b.$columnIdRsu, b.$heightColumnName FROM ${calc_building_area.results.outputTableName} a, $buildingTable b " +
                        "WHERE a.$columnIdBu = b.$columnIdBu").toString())
                calc_build_densityNroughness.execute([inputLowerScaleTableName: buAndIdRsu,
                                                      inputUpperScaleTableName: rsuTable, inputIdUp: columnIdRsu,
                                                      inputVarAndOperations: [(heightColumnName): ["GEOM_AVG"], "st_area":["DENS"]],
                                                      prefixName: prefixName, datasource: datasource])

                // Calculate the SKY VIEW FACTOR from the RSU building density
                // Merge the geometric average and the building density into the RSU table
                datasource.execute(("DROP TABLE IF EXISTS $rsu_indic0; CREATE TABLE $rsu_indic0 AS SELECT a.*, " +
                        "b.dens_st_area, b.geom_avg_$heightColumnName FROM $rsuTable a INNER JOIN " +
                        "${calc_build_densityNroughness.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                IProcess  calcSVF =  Geoclimate.RsuIndicators.groundSkyViewFactor()
                calcSVF.execute([rsuTable: rsu_indic0, correlationBuildingTable: buildingTable, rsuBuildingDensityColumn:
                        "dens_st_area", pointDensity: svfPointDensity, rayLength: svfRayLength,
                                 numberOfDirection: svfNumberOfDirection, prefixName: prefixName, datasource: datasource])

                // Calculate the ASPECT RATIO from the building fraction and the free external facade density
                // Merge the free external facade density into the RSU table containing the other indicators
                datasource.execute(("DROP TABLE IF EXISTS $rsu_4_aspectratio; CREATE TABLE $rsu_4_aspectratio AS SELECT a.*, " +
                        "b.rsu_free_external_facade_density FROM $rsu_indic0 a INNER JOIN " +
                        "${calc_free_ext_density.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                IProcess  calc_aspect_ratio =  Geoclimate.RsuIndicators.aspectRatio()
                calc_aspect_ratio.execute([rsuTable: rsu_4_aspectratio, rsuFreeExternalFacadeDensityColumn:
                        "rsu_free_external_facade_density", rsuBuildingDensityColumn: "dens_st_area",
                                           prefixName: prefixName, datasource: datasource])

                // Calculate the PERVIOUS AND IMPERVIOUS SURFACE FRACTIONS
                // Add the building density field in the surface fraction tables used for pervious and impervious fractions
                // if the buildings are considered in the impervious fractions
                if(fractionTypeImpervious.contains("building")) {
                    imp_type.add("dens_st_area")
                    datasource.execute("ALTER TABLE $rsu_surf_fractions ADD COLUMN dens_st_area DOUBLE; " +
                            "UPDATE $rsu_surf_fractions SET dens_st_area = (SELECT b.dens_st_area" +
                            " FROM ${calc_build_densityNroughness.results.outputTableName} b " +
                            "WHERE $rsu_surf_fractions.$columnIdRsu = b.$columnIdRsu)")
                }
                IProcess calc_perviousness_frac = Geoclimate.RsuIndicators.perviousnessFraction()
                calc_perviousness_frac.execute([rsuTable: rsu_surf_fractions, operationsAndComposition: ["pervious_fraction" :
                                                                                                                 perv_type,
                                                                                                         "impervious_fraction" :
                                                                                                                 imp_type],
                                                prefixName: prefixName, datasource: datasource])

                // Calculate the TERRAIN ROUGHNESS CLASS
                // Merge the geometric average and the projected facade area distribution into the RSU table
                datasource.execute(("DROP TABLE IF EXISTS $rsu_4_roughness0; CREATE TABLE $rsu_4_roughness0 AS SELECT a.*, " +
                        "b.geom_avg_$heightColumnName FROM $rsuTable a INNER JOIN " +
                        "${calc_build_densityNroughness.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                datasource.execute(("DROP TABLE IF EXISTS $rsu_4_roughness1; CREATE TABLE $rsu_4_roughness1 AS SELECT b.*, " +
                        "a.$geometricColumn, a.geom_avg_$heightColumnName  FROM $rsu_4_roughness0 a INNER JOIN " +
                        "${calc_proj_facade_dist.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                // calc_effective_roughness_height --> calc_roughness_class
                IProcess  calc_effective_roughness_height =  Geoclimate.RsuIndicators.effectiveTerrainRoughnessHeight()
                calc_effective_roughness_height.execute([rsuTable: rsu_4_roughness1, projectedFacadeAreaName:
                        "rsu_projected_facade_area_distribution", geometricMeanBuildingHeightName: "geom_avg_$heightColumnName",
                                                         prefixName: prefixName, listLayersBottom: facadeDensListLayersBottom, numberOfDirection:
                                                                 facadeDensNumberOfDirection, datasource: datasource])
                IProcess calc_roughness_class =  Geoclimate.RsuIndicators.effectiveTerrainRoughnessClass()
                calc_roughness_class.execute([datasource: datasource, rsuTable: calc_effective_roughness_height.results.outputTableName,
                                              effectiveTerrainRoughnessHeight: "rsu_effective_terrain_roughness", prefixName: prefixName])


                // III. Define the LCZ of each RSU according to their 7 geometric and surface cover properties
                // Merge all indicator columns in one table
                datasource.execute(("DROP TABLE IF EXISTS $rsu_indic1; CREATE TABLE $rsu_indic1 AS SELECT a.*, " +
                        "b.rsu_aspect_ratio FROM $rsu_indic0 a INNER JOIN " +
                        "${calc_aspect_ratio.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                datasource.execute(("DROP TABLE IF EXISTS $rsu_indic2; CREATE TABLE $rsu_indic2 AS SELECT a.*, " +
                        "b.rsu_ground_sky_view_factor FROM $rsu_indic1 a INNER JOIN " +
                        "${calcSVF.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                datasource.execute(("DROP TABLE IF EXISTS $rsu_indic3; CREATE TABLE $rsu_indic3 AS SELECT a.*, " +
                        "b.pervious_fraction, b.impervious_fraction FROM $rsu_indic2 a INNER JOIN " +
                        "${calc_perviousness_frac.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())
                datasource.execute(("DROP TABLE IF EXISTS $rsu_all_indic; CREATE TABLE $rsu_all_indic AS SELECT a.*, " +
                        "b.effective_terrain_roughness_class FROM $rsu_indic3 a INNER JOIN " +
                        "${calc_roughness_class.results.outputTableName} b ON a.$columnIdRsu = b.$columnIdRsu").toString())

                datasource.execute("DROP TABLE IF EXISTS $rsu_indic0, $rsu_indic1, $rsu_indic2, $rsu_indic3".toString())

                datasource.eachRow("SELECT * FROM ${rsu_all_indic}".toString()){
                    row ->
                        println(row)
                }

                [outputTableName: rsu_all_indic]
            }
    )
}