package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.datamanager.JdbcDataSource

@BaseScript ProcessingChain processingChain

/**
 * Extract OSM data from a place name and compute all geoindicators
 *
 * @return 10 tables : zoneTable , zoneEnvelopeTableName, buildingTable,
 * roadTable, railTable, vegetationTable,
 * hydrographicTable, buildingIndicators,
 * blockIndicators, rsuIndicators
 *
 */
def OSMGeoIndicators() {
    return create({
        title "Create all geoindicators from OSM data"
        inputs datasource: JdbcDataSource, placeName: String, distance: 0
        outputs zoneTable: String, zoneEnvelopeTableName: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, buildingIndicators: String,
                blockIndicators: String,
                rsuIndicators: String
        run { datasource, placeName, distance ->

            IProcess prepareOSMData = ProcessingChain.PrepareOSM.buildGeoclimateLayers()

            if (!prepareOSMData.execute([datasource: datasource, placeName: placeName, distance: distance])) {
                error "Cannot extract the GIS layers from the place name $placeName"
                return null
            }

            String buildingTableName = prepareOSMData.getResults().outputBuilding

            String roadTableName = prepareOSMData.getResults().outputRoad

            String railTableName = prepareOSMData.getResults().outputRail

            String hydrographicTableName = prepareOSMData.getResults().outputHydro

            String vegetationTableName = prepareOSMData.getResults().outputVeget

            String zoneTableName = prepareOSMData.getResults().outputZone

            String zoneEnvelopeTableName = prepareOSMData.getResults().outputZoneEnvelope

            def prefixName  = placeName.trim().split("\\s*(,|\\s)\\s*").join("_");

            IProcess geoIndicators = buildGeoIndicators()
            if (!geoIndicators.execute(datasource: JdbcDataSource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                    roadTable: roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                    hydrographicTable: hydrographicTableName, prefixName: prefixName)) {
                error "Cannot build the geoindicators"
                return null
            }
            return [zoneTable         : zoneTableName, zoneEnvelopeTableName: zoneEnvelopeTableName, buildingTable: buildingTableName,
                    roadTable         : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                    hydrographicTable : hydrographicTableName,
                    buildingIndicators: geoIndicators.results.outputTableBuildingIndicators,
                    blockIndicators   : geoIndicators.results.outputTableBlockIndicators,
                    rsuIndicators     : geoIndicators.results.outputTableRsuIndicators]
        }
    })
}

/**
 * Compute all geoindicators at the 3 scales :
 * building, block and RSU
 *
 * @return
 *
 */
def buildGeoIndicators() {
    return create({
        title "Create all geoindicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: 0.01, prefixName: String, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"]
        outputs outputTableBuildingName: String, outputTableBlockName: String, outputTableRsuName: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance, prefixName,indicatorUse ->
            info "Start computing the geoindicators..."

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
            if (!spatialUnits.execute([datasource       : datasource, zoneTable: zoneTable, buildingTable: buildingTable,
                                       roadTable        : roadTable, railTable: railTable, vegetationTable: vegetationTable,
                                       hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                       surface_hydro    : surface_hydro, distance: distance, prefixName: prefixName])) {
                error "Cannot create the spatial units"
                return null
            }
            String relationBuildings = spatialUnits.getResults().outputTableBuildingName
            String relationBlocks = spatialUnits.getResults().outputTableBlockName
            String relationRSU = spatialUnits.getResults().outputTableRsuName

            //Compute building indicators
            def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
            if (!computeBuildingsIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: relationBuildings,
                                                     inputRoadTableName    : roadTableName,
                                                     indicatorUse          : indicatorUse,
                                                     prefixName            : prefixName])) {
                error "Cannot compute the building indicators"
                return null
            }

            def buildingIndicators = computeBuildingsIndicators.results.outputTableName

            //Compute block indicators
            def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
            if (!computeBlockIndicators.execute([datasource            : datasource,
                                                 inputBuildingTableName: buildingIndicators,
                                                 inputBlockTableName   : relationBlocks,
                                                 prefixName            : prefixName])) {
                error "Cannot compute the block indicators"
                return null
            }

            //Compute RSU indicators
            def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
            if (!computeRSUIndicators.execute([datasource       : datasource,
                                               buildingTable    : buildingIndicators,
                                               rsuTable         : relationRSU,
                                               vegetationTable  : vegetationTable,
                                               roadTable        : roadTable,
                                               hydrographicTable: hydrographicTable,
                                               indicatorUse     : indicatorUse,
                                               prefixName       : prefixName,
                                               svfSimplified    : svfSimplified])) {
                error "Cannot compute the RSU indicators"
                return null
            }
            info "All geoindicators have been computed"
            return [outputTableBuildingIndicators: computeBuildingsIndicators.getResults().outputTableName,
                    outputTableBlockIndicators   : computeBlockIndicators.getResults().outputTableName,
                    outputTableRsuIndicators     : computeRSUIndicators.getResults().outputTableName]
        }
    })
}


