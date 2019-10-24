package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue


class ChainProcessMainTest {

    public static Logger logger = LoggerFactory.getLogger(ChainProcessMainTest.class)

    /**
     * A method to compute geomorphological indicators
     * @param directory
     * @param datasource
     * @param zoneTableName
     * @param buildingTableName
     * @param roadTableName
     * @param railTableName
     * @param vegetationTableName
     * @param hydrographicTableName
     * @param saveResults
     * @param indicatorUse
     */
    void osmGeoIndicators(String directory, JdbcDataSource datasource, String zoneTableName, String buildingTableName,
                          String roadTableName, String railTableName, String vegetationTableName,
                          String hydrographicTableName, boolean saveResults, boolean svfSimplified = false, indicatorUse,
                          String prefixName = "" ) {
        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource : datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                                         roadTable: roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro  : 2500, distance: 0.01, prefixName: prefixName])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationBlocks = spatialUnits.getResults().outputTableBlockName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            logger.info("Saving spatial units")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles()
            saveTables.execute( [inputTableNames: spatialUnits.getResults().values()
                                 , directory: directory, datasource: datasource])
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        assertEquals(countBlocks.count,maxBlocks.max)


        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count,maxRSUBlocks.max)


        //Compute building indicators
        def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
        assertTrue computeBuildingsIndicators.execute([datasource               : datasource,
                                                       inputBuildingTableName   : relationBuildings,
                                                       inputRoadTableName       : roadTableName,
                                                       indicatorUse             : indicatorUse,
                                                       prefixName               : prefixName])
        String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName
        if(saveResults){
            logger.info("Saving building indicators")
            datasource.save(buildingIndicators, directory + File.separator + "${buildingIndicators}.geojson")
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count,countBuildingIndicators.count)

        //Compute block indicators
        if (indicatorUse.contains("URBAN_TYPOLOGY")) {
            def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
            assertTrue computeBlockIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: buildingIndicators,
                                                       inputBlockTableName   : relationBlocks,
                                                       prefixName            : prefixName])
            String blockIndicators = computeBlockIndicators.getResults().outputTableName
            if (saveResults) {
                logger.info("Saving block indicators")
                datasource.save(blockIndicators, directory + File.separator + "${blockIndicators}.geojson")
            }
            //Check if we have the same number of blocks
            def countRelationBlocks= datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count,countBlocksIndicators.count)
        }

        //Compute RSU indicators
        def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
        assertTrue computeRSUIndicators.execute([datasource             : datasource,
                                                 buildingTable          : buildingIndicators,
                                                 rsuTable               : relationRSU,
                                                 vegetationTable        : vegetationTableName,
                                                 roadTable              : roadTableName,
                                                 hydrographicTable      : hydrographicTableName,
                                                 indicatorUse           : indicatorUse,
                                                 prefixName             : prefixName,
                                                 svfSimplified          : svfSimplified])
        String rsuIndicators = computeRSUIndicators.getResults().outputTableName
        if(saveResults){
            logger.info("Saving RSU indicators")
            datasource.save(rsuIndicators, directory + File.separator + "${rsuIndicators}.geojson")
        }

        //Check if we have the same number of RSU
        def countRelationRSU= datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count,countRSUIndicators.count)
    }

    /**
     * A method to compute the LCZ
     *
     * @param directory
     * @param datasource
     * @param zoneTableName
     * @param buildingTableName
     * @param roadTableName
     * @param railTableName
     * @param vegetationTableName
     * @param hydrographicTableName
     * @param saveResults
     */
    void calcLcz(String directory, JdbcDataSource datasource, String zoneTableName, String buildingTableName,
                 String roadTableName, String railTableName, String vegetationTableName,
                 String hydrographicTableName, boolean saveResults, boolean svfSimplified = false, String prefixName = "",
                 def mapOfWeights = ["sky_view_factor": 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                 "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                 "height_of_roughness_elements": 1, "terrain_roughness_class": 1]) {


        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource : datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                                         roadTable: roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro  : 2500, distance: 0.01, prefixName: "geounits", indicatorUse: ["LCZ"]])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            logger.info("Saving spatial units")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles()
            saveTables.execute( [inputTableNames: spatialUnits.getResults().values()
                                 , directory: directory, datasource: datasource])
        }

        // Calculate the LCZ indicators and the corresponding LCZ class of each RSU
        IProcess pm_lcz =  ProcessingChain.BuildLCZ.createLCZ()
        if(!pm_lcz.execute([datasource: datasource, prefixName: prefixName, buildingTable: relationBuildings,
                            rsuTable: relationRSU, roadTable: roadTableName, vegetationTable: vegetationTableName,
                            hydrographicTable: hydrographicTableName, facadeDensListLayersBottom: [0, 50, 200], facadeDensNumberOfDirection: 8,
                            svfPointDensity: 0.008, svfRayLength: 100, svfNumberOfDirection: 60,
                            heightColumnName: "height_roof", fractionTypePervious: ["low_vegetation", "water"],
                            fractionTypeImpervious: ["road"], inputFields: ["id_build"], levelForRoads: [0],
                            svfSimplified : svfSimplified, mapOfWeights : mapOfWeights])){
            logger.info("Cannot create the LCZ.")
            return
        }
        String lczResults = pm_lcz.results.outputTableName

        // Check if we have the same number of RSU
        def countRelationRSU= datasource.firstRow("select count(*) as count from $relationRSU")
        def countRSULcz = datasource.firstRow("select count(*) as count from $lczResults")
        assertEquals(countRelationRSU.count,countRSULcz.count)

        if (saveResults) {
            logger.info("Saving LCZ classes")
            datasource.save(lczResults, directory + File.separator + "${lczResults}.geojson")
        }
    }
}
