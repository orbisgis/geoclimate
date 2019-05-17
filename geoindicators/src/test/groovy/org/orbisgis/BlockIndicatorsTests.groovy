package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals

class BlockIndicatorsTests {

    @Test
    void perkinsSkillScoreBuildingDirectionTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, block_perkins_skill_score_building_direction; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9")

        def  p =  Geoclimate.BlockIndicators.perkinsSkillScoreBuildingDirection()
        p.execute([buildingTableName: "tempo_build", angleRangeSize: 15, prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_block_perkins_skill_score_building_direction WHERE id_block = 4"){
            row ->
                concat+= row.block_perkins_skill_score_building_direction
        }
        assertEquals(4.0/12, concat, 0.0001)
    }

    @Test
    void holeAreaDensityTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_block; " +
                "CREATE TABLE tempo_block AS SELECT * FROM block_test WHERE id_block = 6")

        def  p =  Geoclimate.BlockIndicators.holeAreaDensity()
        p.execute([blockTable: "tempo_block", prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_block_hole_area_density"){
            row ->
                concat+= row.block_hole_area_density
        }
        assertEquals(3.0/47, concat, 0.00001)
    }

    @Test
    void netCompacityTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, building_size_properties, building_contiguity; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8")

        def  p_size =  Geoclimate.BuildingIndicators.sizeProperties()
        p_size.execute([inputBuildingTableName: "tempo_build",
                        operations:["building_volume"], prefixName : "test", datasource:h2GIS])

        // The indicators are gathered in a same table
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build2; " +
                "CREATE TABLE tempo_build2 AS SELECT a.*, b.building_volume FROM tempo_build a" +
                " LEFT JOIN test_building_size_properties b ON a.id_build = b.id_build")

        def  p =  Geoclimate.BlockIndicators.netCompacity()
        p.execute([buildTable: "tempo_build2", buildingVolumeField: "building_volume",
                   buildingContiguityField: "building_contiguity", prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_block_net_compacity WHERE id_block = 4"){
            row ->
                concat+= row.block_net_compacity
        }
        assertEquals(0.51195, concat, 0.00001)
    }

    @Test
    void closingnessTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_block, tempo_build; " +
                "CREATE TABLE tempo_block AS SELECT * FROM block_test WHERE id_block = 8; CREATE TABLE tempo_build AS" +
                " SELECT * FROM building_test WHERE id_block = 8")

        def p = Geoclimate.BlockIndicators.closingness()
        p.execute([correlationTableName: "tempo_build", blockTable: "tempo_block", prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_block_closingness") {
            row ->
                concat += row.block_closingness
        }
        assertEquals(450, concat)
    }
}