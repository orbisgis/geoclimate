package org.orbisgis.geoclimate.osm

import org.orbisgis.geoclimate.Geoindicators
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

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
        Map spatialUnits = Geoindicators.WorkflowGeoIndicators.createUnitsOfAnalysis(datasource, zone, buildingTableName,
                roadTableName, railTableName, vegetationTableName,
                hydrographicTableName, "", "", 10000,
                2500, 0.01, indicatorUse, prefixName)

        String relationBuildings = spatialUnits.building
        String relationBlocks = spatialUnits.block
        String relationRSU = spatialUnits.rsu

        if (saveResults) {
            logger.debug("Saving spatial units")
            Geoindicators.DataUtils.saveTablesAsFiles(datasource, spatialUnits.values(),
                    true, directory)
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        assertEquals(countBlocks.count, maxBlocks.max)


        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count, maxRSUBlocks.max)

        //Compute building indicators
        String buildingIndicators = Geoindicators.WorkflowGeoIndicators.computeBuildingsIndicators(datasource, relationBuildings,
                                                                                                         roadTableName, indicatorUse,
                                                                                                         prefixName)
        assert buildingIndicators
        assertTrue(datasource.getSpatialTable(buildingIndicators).srid > 0)
        if (saveResults) {
            logger.debug("Saving building indicators")
            datasource.getSpatialTable(buildingIndicators).save(directory + File.separator + "${buildingIndicators}.geojson", true)
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count, countBuildingIndicators.count)

        //Compute block indicators
        if (indicatorUse.contains("UTRF")) {
            def blockIndicators = Geoindicators.WorkflowGeoIndicators.computeBlockIndicators(datasource,
                    buildingIndicators,
                    relationBlocks,
                    prefixName)
            assertNotNull(blockIndicators)
            if (saveResults) {
                logger.debug("Saving block indicators")
                datasource.getSpatialTable(blockIndicators).save(directory + File.separator + "${blockIndicators}.geojson", true)
            }
            //Check if we have the same number of blocks
            def countRelationBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count, countBlocksIndicators.count)
            assertTrue(datasource.getSpatialTable(blockIndicators).srid > 0)
        }

        Map parameters = Geoindicators.WorkflowGeoIndicators.getParameters(["indicatorUse":indicatorUse, "svfSimplified":svfSimplified])
        //Compute RSU indicators
        def rsuIndicators = Geoindicators.WorkflowGeoIndicators.computeRSUIndicators( datasource, buildingIndicators,
                                                                                             relationRSU,
                                                                                              vegetationTableName,
                                                                                              roadTableName,
                                                                                             hydrographicTableName,null,
                                                                                              parameters,
                                                                                              prefixName)
        assert rsuIndicators
        if (saveResults) {
            logger.debug("Saving RSU indicators")
            datasource.getSpatialTable(rsuIndicators).save(directory + File.separator + "${rsuIndicators}.geojson", true)
        }

        //Check if we have the same number of RSU
        def countRelationRSU = datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count, countRSUIndicators.count)
        assertTrue(datasource.getSpatialTable(rsuIndicators).srid > 0)
    }
}