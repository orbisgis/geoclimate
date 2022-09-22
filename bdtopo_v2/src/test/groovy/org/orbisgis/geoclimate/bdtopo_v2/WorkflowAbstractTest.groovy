package org.orbisgis.geoclimate.bdtopo_v2


import org.orbisgis.process.api.IProcess
import org.orbisgis.geoclimate.Geoindicators
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class WorkflowAbstractTest {

    public static Logger logger = LoggerFactory.getLogger(WorkflowAbstractTest.class)

    /**
     * A method to compute geomorphological indicators
     * @param directory
     * @param datasource
     * @param zone
     * @param buildingTableName
     * @param roadTableName
     * @param railTableName
     * @param vegetationTableName
     * @param hydrographicTableName
     * @param saveResults
     * @param indicatorUse
     */
    void geoIndicatorsCalc(String directory, def datasource, String zone, String buildingTableName,
                           String roadTableName, String railTableName, String vegetationTableName,
                           String hydrographicTableName, boolean saveResults, boolean svfSimplified = false, def indicatorUse,
                           String prefixName = "") {
        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = Geoindicators.WorkflowGeoIndicators.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource       : datasource, zoneTable: zone, buildingTable: buildingTableName,
                                         roadTable        : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro    : 2500, distance: 0.01, prefixName: prefixName])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationBlocks = spatialUnits.getResults().outputTableBlockName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            logger.debug("Saving spatial units")
            IProcess saveTables = Geoindicators.DataUtils.saveTablesAsFiles()
            saveTables.execute([inputTableNames: spatialUnits.getResults().values(), delete:true
                                , directory    : directory, datasource: datasource])
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        assertEquals(countBlocks.count, maxBlocks.max)


        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count, maxRSUBlocks.max)

        //Compute building indicators
        def computeBuildingsIndicators = Geoindicators.WorkflowGeoIndicators.computeBuildingsIndicators()
        assertTrue computeBuildingsIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: relationBuildings,
                                                       inputRoadTableName    : roadTableName,
                                                       indicatorUse          : indicatorUse,
                                                       prefixName            : prefixName])
        String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName
        if (saveResults) {
            logger.debug("Saving building indicators")
            datasource.save(buildingIndicators, directory + File.separator + "${buildingIndicators}.geojson", true)
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count, countBuildingIndicators.count)

        //Compute block indicators
        if (indicatorUse.contains("UTRF")) {
            def computeBlockIndicators = Geoindicators.WorkflowGeoIndicators.computeBlockIndicators()
            assertTrue computeBlockIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: buildingIndicators,
                                                       inputBlockTableName   : relationBlocks,
                                                       prefixName            : prefixName])
            String blockIndicators = computeBlockIndicators.getResults().outputTableName
            if (saveResults) {
                logger.debug("Saving block indicators")
                datasource.save(blockIndicators, directory + File.separator + "${blockIndicators}.geojson",true)
            }
            //Check if we have the same number of blocks
            def countRelationBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count, countBlocksIndicators.count)
        }

        //Compute RSU indicators
        def computeRSUIndicators = Geoindicators.WorkflowGeoIndicators.computeRSUIndicators()
        assertTrue computeRSUIndicators.execute([datasource       : datasource,
                                                 buildingTable    : buildingIndicators,
                                                 rsuTable         : relationRSU,
                                                 vegetationTable  : vegetationTableName,
                                                 roadTable        : roadTableName,
                                                 hydrographicTable: hydrographicTableName,
                                                 indicatorUse     : indicatorUse,
                                                 prefixName       : prefixName,
                                                 svfSimplified    : svfSimplified])
        String rsuIndicators = computeRSUIndicators.getResults().outputTableName
        if (saveResults) {
            logger.debug("Saving RSU indicators")
            datasource.save(rsuIndicators, directory + File.separator + "${rsuIndicators}.geojson",true)
        }

        //Check if we have the same number of RSU
        def countRelationRSU = datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count, countRSUIndicators.count)
    }
}