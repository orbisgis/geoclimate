package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators

import static org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager.load

@BaseScript GroovyProcessFactory pf

def Geoindicators = load(Geoindicators)

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
create {
    title "Compute all geoindicators"
    id "GeoIndicators"
    inputs datasource: JdbcDataSource, zoneTable: "", buildingTable: "",
            roadTable: "", railTable: "", vegetationTable: "",
            hydrographicTable: "", imperviousTable :"", surface_vegetation: 100000, surface_hydro: 2500,
            distance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified:false, prefixName: "",
            mapOfWeights: ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                           "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                           "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
            industrialFractionThreshold: 0.3
    outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String,
            outputTableRsuIndicators: String, outputTableRsuLcz:String, outputTableZone:String
    run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,imperviousTable,
          surface_vegetation, surface_hydro, distance,indicatorUse,svfSimplified, prefixName, mapOfWeights,
          industrialFractionThreshold ->
        info "Start computing the geoindicators..."
        // Temporary tables are created
        def lczIndicTable = postfix "LCZ_INDIC_TABLE"
        def COLUMN_ID_RSU = "id_rsu"
        def GEOMETRIC_COLUMN = "the_geom"

        //Check data before computing indicators

        if(!zoneTable && !buildingTable && !roadTable){
            error "To compute the geoindicators the zone, building and road tables must not be null or empty"
            return null
        }

        // Output Lcz table name is set to null in case LCZ indicators are not calculated
        def rsuLcz = null

        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = processManager.BuildSpatialUnits.createUnitsOfAnalysis
        if (!spatialUnits.execute([datasource       : datasource,           zoneTable           : zoneTable,
                                   buildingTable    : buildingTable,        roadTable           : roadTable,
                                   railTable        : railTable,            vegetationTable     : vegetationTable,
                                   hydrographicTable: hydrographicTable,    surface_vegetation  : surface_vegetation,
                                   surface_hydro    : surface_hydro,        distance            : distance,
                                   prefixName       : prefixName,
                                   indicatorUse:indicatorUse])) {
            error "Cannot create the spatial units"
            return null
        }
        def relationBuildings = spatialUnits.getResults().outputTableBuildingName
        def relationBlocks = spatialUnits.getResults().outputTableBlockName
        def relationRSU = spatialUnits.getResults().outputTableRsuName


        //Compute building indicators
        def computeBuildingsIndicators = processManager.BuildGeoIndicators.computeBuildingsIndicators
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
        if(indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")){
            def computeBlockIndicators = processManager.BuildGeoIndicators.computeBlockIndicators
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
        def computeRSUIndicators = processManager.BuildGeoIndicators.computeRSUIndicators
        if (!computeRSUIndicators.execute([datasource       : datasource,
                                           buildingTable    : buildingIndicators,
                                           rsuTable         : relationRSU,
                                           vegetationTable  : vegetationTable,
                                           roadTable        : roadTable,
                                           hydrographicTable: hydrographicTable,
                                           imperviousTable:   imperviousTable,
                                           indicatorUse     : indicatorUse,
                                           svfSimplified    : svfSimplified,
                                           prefixName       : prefixName])) {
            error "Cannot compute the RSU indicators"
            return null
        }
        info "All geoindicators have been computed"

        // If the LCZ indicators should be calculated, we only affect a LCZ class to each RSU
        if(indicatorUse.contains("LCZ")){
            def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"             : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                 "BUILDING_FRACTION_LCZ"            : "BUILDING_SURFACE_FRACTION",
                                 "ASPECT_RATIO"                     : "ASPECT_RATIO",
                                 "GROUND_SKY_VIEW_FACTOR"           : "SKY_VIEW_FACTOR",
                                 "PERVIOUS_FRACTION_LCZ"            : "PERVIOUS_SURFACE_FRACTION",
                                 "IMPERVIOUS_FRACTION_LCZ"          : "IMPERVIOUS_SURFACE_FRACTION",
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

            // The classification algorithm is called
            def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType
            if(!classifyLCZ([rsuLczIndicators               : lczIndicTable,
                             rsuAllIndicators               : computeRSUIndicators.results.outputTableName,
                             normalisationType              : "AVG",
                             mapOfWeights                   : mapOfWeights,
                             prefixName                     : prefixName,
                             datasource                     : datasource,
                             prefixName                     : prefixName,
                             industrialFractionThreshold    : industrialFractionThreshold])){
                info "Cannot compute the LCZ classification."
                return
            }
            rsuLcz = classifyLCZ.results.outputTableName
        }

        datasource.execute "DROP TABLE IF EXISTS $lczIndicTable;"


        return [outputTableBuildingIndicators: computeBuildingsIndicators.getResults().outputTableName,
                outputTableBlockIndicators   : blockIndicators,
                outputTableRsuIndicators     : computeRSUIndicators.getResults().outputTableName,
                outputTableRsuLcz   : rsuLcz,
                outputTableZone:zoneTable]
    }
}