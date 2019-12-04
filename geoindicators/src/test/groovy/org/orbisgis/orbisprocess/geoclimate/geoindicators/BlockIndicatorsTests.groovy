package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class BlockIndicatorsTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open("./target/${BlockIndicatorsTests.getName()};AUTO_SERVER=TRUE")
    }

    @BeforeEach
    void initData(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void holeAreaDensityTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute "DROP TABLE IF EXISTS tempo_block; " +
                "CREATE TABLE tempo_block AS SELECT * FROM block_test WHERE id_block = 6"

        def  p =  Geoindicators.BlockIndicators.holeAreaDensity()
        assertTrue p.execute([blockTable: "tempo_block", prefixName: "test", datasource: h2GIS])

        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_hole_area_density"){
            row -> sum += row.hole_area_density
        }
        assertEquals(3.0/47, sum, 0.00001)
    }

    @Test
    void netCompactnessTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, building_size_properties, building_contiguity; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8"

        def  p_size =  Geoindicators.BuildingIndicators.sizeProperties()
        assertTrue p_size.execute([inputBuildingTableName: "tempo_build",
                        operations:["volume"], prefixName : "test", datasource:h2GIS])

        // The indicators are gathered in a same table
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build2; " +
                "CREATE TABLE tempo_build2 AS SELECT a.*, b.volume FROM tempo_build a" +
                " LEFT JOIN test_building_size_properties b ON a.id_build = b.id_build"

        def  p =  Geoindicators.BlockIndicators.netCompactness()
        assertTrue p.execute([buildTable: "tempo_build2", buildingVolumeField: "volume",
                   buildingContiguityField: "contiguity", prefixName: "test", datasource: h2GIS])
        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_net_compactness WHERE id_block = 4"){
            row -> sum += row.net_compactness
        }
        assertEquals(0.51195, sum, 0.00001)
    }

    @Test
    void closingnessTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_block, tempo_build; " +
                "CREATE TABLE tempo_block AS SELECT * FROM block_test WHERE id_block = 8; CREATE TABLE tempo_build AS" +
                " SELECT * FROM building_test WHERE id_block = 8")

        def p = Geoindicators.BlockIndicators.closingness()
        assertTrue p.execute([correlationTableName: "tempo_build", blockTable: "tempo_block", prefixName: "test",
                              datasource: h2GIS])
        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_closingness") {
            row -> sum += row.closingness
        }
        assertEquals(450, sum)
    }
}