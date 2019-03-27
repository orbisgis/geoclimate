package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals

class BlockIndicatorsTests {

    @Test
    void unweightedOperationFromLowerScale() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, unweighted_operation_from_lower_scale1, " +
                "unweighted_operation_from_lower_scale2, unweighted_operation_from_lower_scale3; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8")

        def  psum =  Geoclimate.BlockIndicators.unweightedOperationFromLowerScale()
        psum.execute([inputLowerScaleTableName: "tempo_build",inputCorrelationTableName: "block_build_corr",inputIdLow: "id_build",
                   inputIdUp: "id_block", inputToTransfo: "building_area",operations: ["SUM"],
                   outputTableName: "unweighted_operation_from_lower_scale1", datasource: h2GIS])
        def  pavg =  Geoclimate.BlockIndicators.unweightedOperationFromLowerScale()
        pavg.execute([inputLowerScaleTableName: "tempo_build",inputCorrelationTableName: "rsu_build_corr",inputIdLow: "id_build",
                      inputIdUp: "id_rsu", inputToTransfo: "building_number_building_neighbor",operations: ["AVG"],
                      outputTableName: "unweighted_operation_from_lower_scale2", datasource: h2GIS])
        def  pgeom_avg =  Geoclimate.BlockIndicators.unweightedOperationFromLowerScale()
        pgeom_avg.execute([inputLowerScaleTableName: "tempo_build",inputCorrelationTableName: "rsu_build_corr",inputIdLow: "id_build",
                      inputIdUp: "id_rsu", inputToTransfo: "height_roof",operations: ["GEOM_AVG"],
                      outputTableName: "unweighted_operation_from_lower_scale3", datasource: h2GIS])
        def concat = ["", "", 0]
        h2GIS.eachRow("SELECT * FROM unweighted_operation_from_lower_scale1 WHERE id_block = 1 OR id_block = 4 ORDER BY id_block ASC"){
            row ->
                concat[0]+= "${row.sum_building_area}\n"
        }
        h2GIS.eachRow("SELECT * FROM unweighted_operation_from_lower_scale2 WHERE id_rsu = 1 OR id_rsu = 2 ORDER BY id_rsu ASC"){
            row ->
                concat[1]+= "${row.avg_building_number_building_neighbor}\n"
        }
        h2GIS.eachRow("SELECT * FROM unweighted_operation_from_lower_scale3 WHERE id_rsu = 1"){
            row ->
                concat[2]+= row.geom_avg_height_roof
        }
        assertEquals("156.0\n310.0\n", concat[0])
        assertEquals("0.4\n0.0\n", concat[1])
        assertEquals(10.69, concat[2], 0.01)
    }

}