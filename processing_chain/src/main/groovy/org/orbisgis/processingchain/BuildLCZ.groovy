package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.geoindicators.Geoindicators
import org.orbisgis.datamanager.JdbcDataSource


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
def createLCZ() {
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
                levelForRoads: [0], svfSimplified: false
        outputs outputTableName: String
        run { JdbcDataSource datasource, prefixName, buildingTable, rsuTable, roadTable, vegetationTable,
              hydrographicTable, facadeDensListLayersBottom, facadeDensNumberOfDirection,
              svfPointDensity, svfRayLength, svfNumberOfDirection,
              heightColumnName,
              mapOfWeights,
              fractionTypePervious,
              fractionTypeImpervious, inputFields, levelForRoads, svfSimplified ->
            info "Create the LCZ..."

            // Name of the table where are stored the indicators needed for the LCZ classification
            def lczIndicTable = prefixName+"_lczIndicTable"

            def columnIdRsu = "id_rsu"
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
            if (!computeBuildingsIndicators([datasource            : datasource,
                                             inputBuildingTableName: buildingTable,
                                             inputRoadTableName    : roadTable,
                                             indicatorUse          : ["LCZ"]])) {
                info "Cannot compute building indicators."
                return
            }
            def buildingIndicators = computeBuildingsIndicators.getResults().outputTableName

            //Compute RSU indicators
            def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
            if (!computeRSUIndicators([datasource           : datasource,
                                       buildingTable        : buildingIndicators,
                                       rsuTable             : rsuTable,
                                       vegetationTable      : vegetationTable,
                                       roadTable            : roadTable,
                                       hydrographicTable    : hydrographicTable,
                                       indicatorUse         : ["LCZ"],
                                       svfPointDensity      : svfPointDensity,
                                       svfRayLength         : svfRayLength,
                                       svfNumberOfDirection : svfNumberOfDirection,
                                       svfSimplified        : svfSimplified])) {
                info "Cannot compute the RSU indicators."
                return
            }
            def rsuIndicators = computeRSUIndicators.getResults().outputTableName

            // Rename the indicators in order to be consistent with the LCZ ones
            def queryReplaceNames = ""

            lczIndicNames.each { oldIndic, newIndic ->
                queryReplaceNames += "ALTER TABLE $rsuIndicators ALTER COLUMN $oldIndic RENAME TO $newIndic;"
            }
            // Keep only the ID, geometry column and the 7 indicators useful for LCZ classification
            datasource.execute "$queryReplaceNames"
            datasource.execute "DROP TABLE IF EXISTS $lczIndicTable;" +
                    "CREATE TABLE $lczIndicTable AS SELECT $columnIdRsu, $geometricColumn, " +
                    "${lczIndicNames.values().join(",")} FROM $rsuIndicators"

                // The classification algorithm is called
                def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
                if(!classifyLCZ([rsuLczIndicators   : lczIndicTable,
                                 normalisationType  : "AVG",
                                 mapOfWeights       : mapOfWeights,
                                 prefixName         : prefixName,
                                 datasource         : datasource])){
                    info "Cannot compute the LCZ classification."
                    return
                }

            [outputTableName: classifyLCZ.results.outputTableName]
        }
    })
}
