package org.orbisgis

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.geoindicators.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class BlockIndicatorsTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
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
            row -> sum += row.block_hole_area_density
        }
        assertEquals(3.0/47, sum, 0.00001)
    }

    @Test
    void netCompacityTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, building_size_properties, building_contiguity; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8"

        def  p_size =  Geoindicators.BuildingIndicators.sizeProperties()
        assertTrue p_size.execute([inputBuildingTableName: "tempo_build",
                        operations:["building_volume"], prefixName : "test", datasource:h2GIS])

        // The indicators are gathered in a same table
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build2; " +
                "CREATE TABLE tempo_build2 AS SELECT a.*, b.building_volume FROM tempo_build a" +
                " LEFT JOIN test_building_size_properties b ON a.id_build = b.id_build"

        def  p =  Geoindicators.BlockIndicators.netCompacity()
        assertTrue p.execute([buildTable: "tempo_build2", buildingVolumeField: "building_volume",
                   buildingContiguityField: "building_contiguity", prefixName: "test", datasource: h2GIS])
        def sum = 0
        h2GIS.eachRow("SELECT * FROM test_block_net_compacity WHERE id_block = 4"){
            row -> sum += row.block_net_compacity
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
            row -> sum += row.block_closingness
        }
        assertEquals(450, sum)
    }
}