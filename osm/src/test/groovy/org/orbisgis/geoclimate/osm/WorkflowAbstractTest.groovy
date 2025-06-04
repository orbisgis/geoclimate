/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
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
                           String hydrographicTableName, String imperviousTableName, String sealandmaskTableName,
                           String urban_areas,
                           boolean saveResults, boolean svfSimplified = false, def indicatorUse,
                           String prefixName = "", boolean onlySea = false) {
        //Create spatial units and relations : building, block, rsu
        Map spatialUnits = Geoindicators.WorkflowGeoIndicators.createUnitsOfAnalysis(datasource, zone, zone,buildingTableName,
                roadTableName, railTableName, vegetationTableName,
                hydrographicTableName, sealandmaskTableName, urban_areas, "", 10000,
                2500, 10000, 0.01,5000, indicatorUse,prefixName)

        String relationBuildings = spatialUnits.building
        String relationBlocks = spatialUnits.block
        String relationRSU = spatialUnits.rsu

        if (saveResults) {
            logger.debug("Saving spatial units")
            Geoindicators.DataUtils.saveTablesAsFiles(datasource, new ArrayList(spatialUnits.values()),
                    true, directory)
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        if (!onlySea) {
            assertEquals(countBlocks.count, maxBlocks.max)
        }

        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count, maxRSUBlocks.max)

        //Compute building indicators
        String buildingIndicators = Geoindicators.WorkflowGeoIndicators.computeBuildingsIndicators(datasource, relationBuildings,
                roadTableName, indicatorUse,
                prefixName)
        assert buildingIndicators
        if (!onlySea) {
            assertTrue(datasource.getSpatialTable(buildingIndicators).srid > 0)
        }
        if (saveResults) {
            logger.debug("Saving building indicators")
            datasource.getSpatialTable(buildingIndicators).save(directory + File.separator + "${buildingIndicators}.fgb", true)
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
                datasource.getSpatialTable(blockIndicators).save(directory + File.separator + "${blockIndicators}.fgb", true)
            }
            //Check if we have the same number of blocks
            def countRelationBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count, countBlocksIndicators.count)
            if (!onlySea) {
                assertTrue(datasource.getSpatialTable(blockIndicators).srid > 0)
            }
        }

        Map parameters = Geoindicators.WorkflowGeoIndicators.getParameters(["indicatorUse": indicatorUse, "svfSimplified": svfSimplified])
        //Compute RSU indicators
        def rsuIndicators = Geoindicators.WorkflowGeoIndicators.computeRSUIndicators(datasource, buildingIndicators,
                relationRSU,
                vegetationTableName,
                roadTableName,
                hydrographicTableName,
                imperviousTableName, railTableName,
                parameters,
                prefixName)
        assert rsuIndicators
        if (saveResults) {
            logger.debug("Saving RSU indicators")
            datasource.getSpatialTable(rsuIndicators).save(directory + File.separator + "${rsuIndicators}.fgb", true)
        }

        //Check if we have the same number of RSU
        def countRelationRSU = datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count, countRSUIndicators.count)
        assertTrue(datasource.getSpatialTable(rsuIndicators).srid > 0)
    }
}