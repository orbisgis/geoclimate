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
        inputs datasource: JdbcDataSource, placeName: String, distance: 0,indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified:false
        outputs zoneTable: String, zoneEnvelopeTableName: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, buildingIndicators: String,
                blockIndicators: String,
                rsuIndicators: String
        run { datasource, placeName, distance,indicatorUse, svfSimplified ->

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

            IProcess geoIndicators = buildGeoIndicators()
            if (!geoIndicators.execute(datasource: datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                    roadTable: roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                    hydrographicTable: hydrographicTableName,
            indicatorUse: indicatorUse, svfSimplified: svfSimplified)) {
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
 * Extract OSM data from a place name and compute the Local Climate Zone areas
 *
 * @return 10 tables : zoneTable , zoneEnvelopeTableName, buildingTable,
 * roadTable, railTable, vegetationTable,
 * hydrographicTable, lczTable
 *
 */
def OSMLCZ() {
    return create({
        title "Compute the Local Climate Zone areas from OSM data"
        inputs datasource: JdbcDataSource, placeName: String, distance: 0,indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified:false
        outputs zoneTable: String, zoneEnvelopeTableName: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, lczTable: String
        run { datasource, placeName, distance,indicatorUse, svfSimplified ->

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

            IProcess lcz = buildLCZ()
            if (!lcz.execute(datasource: datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                    roadTable: roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                    hydrographicTable: hydrographicTableName, svfSimplified: svfSimplified)) {
                error "Cannot build the LCZ"
                return null
            }
            return [zoneTable         : zoneTableName, zoneEnvelopeTableName: zoneEnvelopeTableName, buildingTable: buildingTableName,
                    roadTable         : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                    hydrographicTable : hydrographicTableName,
                    lczTable: lcz.results.outputTableLCZ]
        }
    })
}

/**
 * Compute all geoindicators at the 3 scales :
 * building, block and RSU
 *
 * @return 3 tables outputTableBuildingIndicators, outputTableBlockIndicators, outputTableRsuIndicators
 * that contain the indicators at the 3 scales
 *
 */
def buildGeoIndicators() {
    return create({
        title "Compute all geoindicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified:false
        outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String, outputTableRsuIndicators: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance,indicatorUse,svfSimplified ->
            info "Start computing the geoindicators..."

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
            if (!spatialUnits.execute([datasource       : datasource, zoneTable: zoneTable, buildingTable: buildingTable,
                                       roadTable        : roadTable, railTable: railTable, vegetationTable: vegetationTable,
                                       hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                       surface_hydro    : surface_hydro, distance: distance])) {
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
                                                     inputRoadTableName    : roadTable,
                                                     indicatorUse          : indicatorUse])) {
                error "Cannot compute the building indicators"
                return null
            }

            def buildingIndicators = computeBuildingsIndicators.results.outputTableName

            //Compute block indicators
            def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
            if (!computeBlockIndicators.execute([datasource            : datasource,
                                                 inputBuildingTableName: buildingIndicators,
                                                 inputBlockTableName   : relationBlocks])) {
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


/**
 * Compute the Local Climate Zone areas
 *
 * @return the outputTableLCZ name
 *
 */
def buildLCZ() {
    return create({
        title "Compute the Local Climate Zones"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: 0.01, svfSimplified:false, prefixName: "lcz",
                mapOfWeights : ["sky_view_factor": 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                 "height_of_roughness_elements": 1, "terrain_roughness_class": 1]
        outputs outputTableLCZ: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance,svfSimplified,prefixName,mapOfWeights ->
            info "Start computing the Local Climate Zones..."

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
            if(!spatialUnits.execute([datasource : datasource, zoneTable: zoneTable,
                                             buildingTable: buildingTable,
                                             roadTable: roadTable, railTable: railTable,
                                             vegetationTable: vegetationTable,
                                             hydrographicTable: hydrographicTable,
                                             surface_vegetation: 100000,
                                             surface_hydro  : 2500, distance: distance,
                                             prefixName: prefixName, indicatorUse: ["LCZ"]])){
                error "Cannot compute the spatial units"
                return null
            }

            String relationBuildings = spatialUnits.getResults().outputTableBuildingName
            String relationRSU = spatialUnits.getResults().outputTableRsuName


            // Calculate the LCZ indicators and the corresponding LCZ class of each RSU
            IProcess pm_lcz =  ProcessingChain.BuildLCZ.createLCZ()
            if(!pm_lcz.execute([datasource: datasource, prefixName: prefixName, buildingTable: relationBuildings,
                                rsuTable: relationRSU, roadTable: roadTable, vegetationTable: vegetationTable,
                                hydrographicTable: hydrographicTable,
                                facadeDensListLayersBottom: [0, 50, 200], facadeDensNumberOfDirection: 8,
                                svfPointDensity: 0.008, svfRayLength: 100, svfNumberOfDirection: 60,
                                heightColumnName: "height_roof", fractionTypePervious: ["low_vegetation", "water"],
                                fractionTypeImpervious: ["road"], inputFields: ["id_build"], levelForRoads: [0],
                                svfSimplified : svfSimplified, mapOfWeights : mapOfWeights])){
                logger.info("Cannot create the LCZ.")
                return null
            }
            String lczResults = pm_lcz.results.outputTableName

            info "The Local Climate Zones have been computed"
            return [outputTableLCZ: pm_lcz.results.outputTableName]
        }
    })
}
