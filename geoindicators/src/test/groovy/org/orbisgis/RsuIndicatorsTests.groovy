package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals

class RsuIndicatorsTests {

    @Test
    void testRsuFreeExternalFacadeDensity() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 8")
        // The geometry of the RSU is useful for the calculation, then it is inserted inside the build/rsu correlation table
        h2GIS.execute("DROP TABLE IF EXISTS corr_tempo; CREATE TABLE corr_tempo AS SELECT a.*, b.the_geom " +
                "FROM rsu_build_corr a, rsu_test b WHERE a.id_rsu = b.id_rsu")

        def  p =  Geoclimate.RsuIndicators.rsuFreeExternalFacadeDensity()
        p.execute([inputBuildingTableName: "tempo_build",inputFields:[],inputCorrelationTableName: "corr_tempo",
                   buContiguityFieldName: "building_contiguity", buTotalFacadeLength: "building_total_facade_length",
                   outputTableName: "rsu_free_external_facade_density", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM rsu_free_external_facade_density WHERE id_rsu = 1"){
            row ->
                concat+= row.rsu_free_external_facade_density
        }
        assertEquals(0.947, concat)
    }
}