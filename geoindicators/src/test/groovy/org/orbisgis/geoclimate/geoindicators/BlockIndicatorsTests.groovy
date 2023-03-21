package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.orbisgis.data.H2GIS.open

class BlockIndicatorsTests {

    @TempDir
    static File folder

    private static def h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "blockIndicatorsTests;AUTO_SERVER=TRUE")
    }

    @BeforeEach
    void beforeEach() {
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void holeAreaDensityTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_block; 
                CREATE TABLE tempo_block AS SELECT * FROM block_test WHERE id_block = 6
        """

        def p = Geoindicators.BlockIndicators.holeAreaDensity(h2GIS, "tempo_block", "test")

        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_hole_area_density") { sum += it.hole_area_density }
        assertEquals 3.0 / 47, sum, 0.00001
    }

    @Test
    void netCompactnessTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, building_size_properties, building_contiguity; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8
        """

        def p = Geoindicators.BuildingIndicators.sizeProperties(h2GIS,
                "tempo_build", ["volume"], "test")
        assert p

        // The indicators are gathered in a same table
        h2GIS """
                DROP TABLE IF EXISTS tempo_build2; 
                CREATE TABLE tempo_build2 AS 
                    SELECT a.*, b.volume 
                    FROM tempo_build a 
                    LEFT JOIN test_building_size_properties b 
                    ON a.id_build = b.id_build
        """

        Geoindicators.BlockIndicators.netCompactness(h2GIS, "tempo_build2", "volume", "contiguity", "test")
        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_net_compactness WHERE id_block = 4") { sum += it.net_compactness }
        assertEquals 0.51195, sum, 0.00001
    }

    @Test
    void closingnessTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_block, tempo_build; 
                CREATE TABLE tempo_block AS SELECT * FROM block_test WHERE id_block = 8; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_block = 8
        """

        Geoindicators.BlockIndicators.closingness(h2GIS, "tempo_build", "tempo_block", "test")
        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_closingness") { sum += it.closingness }
        assert 450 == sum
    }
}