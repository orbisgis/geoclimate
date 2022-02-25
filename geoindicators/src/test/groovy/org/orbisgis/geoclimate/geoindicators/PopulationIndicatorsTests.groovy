package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.orbisdata.processmanager.api.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open

/**
 * Unit tests for PopulationIndicators process
 *
 * @author Erwan Bocher, CNRS
 */
class PopulationIndicatorsTests {

    private static H2GIS h2GIS

    private static def randomDbName() {
        "${PopulationIndicatorsTests.simpleName}_${UUID.randomUUID().toString().replaceAll "-", "_"}"
    }

    @BeforeAll
    static void beforeAll() {
        h2GIS = open "./target/${randomDbName()};AUTO_SERVER=TRUE"
    }

    @Test
    void buildingPopulationTest1() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,1, 'residential', 'POLYGON ((3 6, 6 6, 6 3, 3 3, 3 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        assertEquals(10f, (float)h2GIS.firstRow("select pop from ${process.results.buildingTableName}").pop)
    }

    @Test
    void buildingPopulationTestBorderBuilding() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,1, 'residential', 'POLYGON ((12 6, 8 6, 8 3, 12 3, 12 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        assertEquals(10f, (float)h2GIS.firstRow("select pop from ${process.results.buildingTableName}").pop)
    }

    @Test
    void buildingPopulationTestSeveralBuilding() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,1, 'residential', 'POLYGON ((3 6, 1 6, 1 3, 3 3, 3 6))'::GEOMETRY);
        INSERT INTO building VALUES(2,1, 'residential', 'POLYGON ((8 6, 6 6, 6 3, 8 3, 8 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        def rows = h2GIS.rows("select pop from ${process.results.buildingTableName} order by id_build")
        assertEquals(5f, (float)rows[0].pop)
        assertEquals(5f, (float)rows[1].pop)
    }
    @Test
    void buildingPopulationTestSeveralBuildingLevel() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,1, 'residential', 'POLYGON ((3 6, 1 6, 1 3, 3 3, 3 6))'::GEOMETRY);
        INSERT INTO building VALUES(2,2, 'residential', 'POLYGON ((8 6, 6 6, 6 3, 8 3, 8 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        def rows = h2GIS.rows("select pop from ${process.results.buildingTableName} order by id_build")
        assertEquals(3.33f, (float)rows[0].pop, 0.01)
        assertEquals(6.666f, (float)rows[1].pop, 0.01)
    }

    @Test
    void buildingPopulationTestLevel() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,2, 'residential', 'POLYGON ((3 6, 6 6, 6 3, 3 3, 3 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        def rows = h2GIS.rows("select pop from ${process.results.buildingTableName} order by id_build")
        assertEquals(3.33f, (float)rows[0].pop, 0.01)
        assertEquals(6.666f, (float)rows[1].pop, 0.01)
    }

    @Test
    void buildingPopulationTestBorderBuildingSeveralPopUnits() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        INSERT INTO population_grid VALUES(2, 10, 'POLYGON ((10 10, 20 10, 20 0, 10 0, 10 10))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,1, 'residential', 'POLYGON ((12 6, 8 6, 8 3, 12 3, 12 6))'::GEOMETRY);
        INSERT INTO building VALUES(2,1, 'residential', 'POLYGON ((5 6, 1 6, 1 3, 5 3, 5 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        def rows = h2GIS.rows("select pop, id_build from ${process.results.buildingTableName} order by id_build")
        assertEquals(13.33f, (float)rows[0].pop, 0.01)
        assertEquals(6.666f, (float)rows[1].pop, 0.01)
    }

    @Test
    void buildingPopulationTestBorderBuildingSeveralPopUnitsLevels() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        INSERT INTO population_grid VALUES(2, 10, 'POLYGON ((10 10, 20 10, 20 0, 10 0, 10 10))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,2, 'residential', 'POLYGON ((12 6, 8 6, 8 3, 12 3, 12 6))'::GEOMETRY);
        INSERT INTO building VALUES(2,1, 'residential', 'POLYGON ((5 6, 1 6, 1 3, 5 3, 5 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        def rows = h2GIS.rows("select pop, id_build from ${process.results.buildingTableName} order by id_build")
        assertEquals(15f, (float)rows[0].pop, 0.01)
        assertEquals(5f, (float)rows[1].pop, 0.01)
    }

    @Test
    void buildingPopulationTestBorderBuildingSeveralPopUnitsLevels2() {
        h2GIS.execute("""DROP TABLE if exists population_grid, building;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        INSERT INTO population_grid VALUES(2, 10, 'POLYGON ((10 10, 20 10, 20 0, 10 0, 10 10))'::GEOMETRY);
        CREATE TABLE building (ID_BUILD integer, NB_LEV integer,MAIN_USE varchar, THE_GEOM GEOMETRY);
        INSERT INTO building VALUES(1,1, 'residential', 'POLYGON ((12 6, 8 6, 8 3, 12 3, 12 6))'::GEOMETRY);
        INSERT INTO building VALUES(2,2, 'residential', 'POLYGON ((5 6, 1 6, 1 3, 5 3, 5 6))'::GEOMETRY);
        """.toString())
        IProcess process = Geoindicators.PopulationIndicators.buildingPopulation()
        assertTrue process.execute([inputBuildingTableName: "building", inputPopulationTableName: "population_grid",  datasource: h2GIS])
        def rows = h2GIS.rows("select pop, id_build from ${process.results.buildingTableName} order by id_build")
        assertEquals(12f, (float)rows[0].pop, 0.01)
        assertEquals(8f, (float)rows[1].pop, 0.01)
    }
}
