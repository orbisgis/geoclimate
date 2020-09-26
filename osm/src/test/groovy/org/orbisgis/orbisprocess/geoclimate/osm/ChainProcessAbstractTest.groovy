package org.orbisgis.orbisprocess.geoclimate.osm

import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager
import org.orbisgis.orbisdata.processmanager.process.ProcessManager
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators as GI
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain as PC
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class ChainProcessAbstractTest {

    public static Logger logger = LoggerFactory.getLogger(ChainProcessAbstractTest.class)

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
    void geoIndicatorsCalc(String directory, def datasource, String zoneTableName, String buildingTableName,
                           String roadTableName, String railTableName, String vegetationTableName,
                           String hydrographicTableName, boolean saveResults, boolean svfSimplified = false, def indicatorUse,
                           String prefixName = "") {
        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = ProcessingChain.GeoIndicatorsChain.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource       : datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                                         roadTable        : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro    : 2500, distance: 0.01, prefixName: prefixName])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationBlocks = spatialUnits.getResults().outputTableBlockName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            logger.info("Saving spatial units")
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
        def computeBuildingsIndicators = ProcessingChain.GeoIndicatorsChain.computeBuildingsIndicators()
        assertTrue computeBuildingsIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: relationBuildings,
                                                       inputRoadTableName    : roadTableName,
                                                       indicatorUse          : indicatorUse,
                                                       prefixName            : prefixName])
        String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName
        assertTrue(datasource.getSpatialTable(buildingIndicators).srid>0)
        if (saveResults) {
            logger.info("Saving building indicators")
            datasource.getSpatialTable(buildingIndicators).save(directory + File.separator + "${buildingIndicators}.geojson", true)
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count, countBuildingIndicators.count)

        //Compute block indicators
        if (indicatorUse.contains("URBAN_TYPOLOGY")) {
            def computeBlockIndicators = ProcessingChain.GeoIndicatorsChain.computeBlockIndicators()
            assertTrue computeBlockIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: buildingIndicators,
                                                       inputBlockTableName   : relationBlocks,
                                                       prefixName            : prefixName])
            String blockIndicators = computeBlockIndicators.getResults().outputTableName
            if (saveResults) {
                logger.info("Saving block indicators")
                datasource.getSpatialTable(blockIndicators).save(directory + File.separator + "${blockIndicators}.geojson", true)
            }
            //Check if we have the same number of blocks
            def countRelationBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count, countBlocksIndicators.count)
            assertTrue(datasource.getSpatialTable(blockIndicators).srid>0)
        }

        //Compute RSU indicators
        def computeRSUIndicators = ProcessingChain.GeoIndicatorsChain.computeRSUIndicators()
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
            logger.info("Saving RSU indicators")
            datasource.getSpatialTable(rsuIndicators).save(directory + File.separator + "${rsuIndicators}.geojson", true)
        }

        //Check if we have the same number of RSU
        def countRelationRSU = datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count, countRSUIndicators.count)
        assertTrue(datasource.getSpatialTable(rsuIndicators).srid>0)
    }
}