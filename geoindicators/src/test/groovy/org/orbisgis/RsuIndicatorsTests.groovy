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
        p.execute([buildingTable: "tempo_build",inputColumns:[],correlationTable: "corr_tempo",
                   buContiguityColumn: "building_contiguity", buTotalFacadeLengthColumn: "building_total_facade_length",
                   outputTableName: "rsu_free_external_facade_density", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM rsu_free_external_facade_density WHERE id_rsu = 1"){
            row ->
                concat+= row.rsu_free_external_facade_density
        }
        assertEquals(0.947, concat)
    }

    @Test
    void testRsuGroundSkyViewFactor() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build > 8 AND id_build < 27")
        // The geometry of the buildings are useful for the calculation, then they are inserted inside
        // the build/rsu correlation table
        h2GIS.execute("DROP TABLE IF EXISTS corr_tempo; CREATE TABLE corr_tempo AS SELECT a.*, b.the_geom, b.height_wall " +
                "FROM rsu_build_corr a, tempo_build b WHERE a.id_build = b.id_build")

        def  p =  Geoclimate.RsuIndicators.rsuGroundSkyViewFactor()
        p.execute([rsuTable: "rsu_test",inputColumns:[],correlationBuildingTable: "corr_tempo",
                   rsuAreaColumn: "rsu_area", rsuBuildingDensityColumn: "rsu_building_density", pointDensity: 0.008,
                   rayLength: 100, numberOfDirection: 60, outputTableName: "rsu_ground_sky_view_factor",
                   datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM rsu_ground_sky_view_factor WHERE id_rsu = 8"){
            row ->
                concat+= row.rsu_ground_sky_view_factor
        }
        assertEquals(0.54, concat, 0.05)
    }

    @Test
    void testRsuAspectRatio() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        def  p =  Geoclimate.RsuIndicators.rsuAspectRatio()
        p.execute([rsuTable: "rsu_test",inputColumns:["id_rsu", "the_geom"], rsuFreeExternalFacadeDensityColumn:
                "rsu_free_external_facade_density", rsuBuildingDensityColumn: "rsu_building_density",
                   outputTableName: "rsu_aspect_ratio", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM rsu_aspect_ratio WHERE id_rsu = 1"){
            row ->
                concat+= row.rsu_aspect_ratio
        }
        assertEquals(1.344, concat, 0.001)
    }
}