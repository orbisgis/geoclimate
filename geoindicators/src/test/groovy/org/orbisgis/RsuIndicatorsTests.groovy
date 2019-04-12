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

    @Test
    void testRsuProjectedFacadeAreaDistribution() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6")

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def dirMedDeg = 180/numberOfDirection
        def  p =  Geoclimate.RsuIndicators.rsuProjectedFacadeAreaDistribution()
        p.execute([buildingTable: "tempo_build", inputColumns: ["id_rsu", "the_geom"], rsuTable: "rsu_test", listLayersBottom: listLayersBottom,
                   numberOfDirection: numberOfDirection, outputTableName: "rsu_projected_facade_area_distribution",
                   datasource: h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM rsu_projected_facade_area_distribution WHERE id_rsu = 1"){
            row ->
                // Iterate over columns
                def names = []
                for (i in 1..listLayersBottom.size()){
                    names[i-1]="rsu_projected_facade_area_distribution${listLayersBottom[i-1]}"+
                            "_${listLayersBottom[i]}"
                    if (i == listLayersBottom.size()){
                        names[listLayersBottom.size()-1]="rsu_projected_facade_area_distribution"+
                                "${listLayersBottom[listLayersBottom.size()-1]}_"
                    }
                    for (int d=0; d<numberOfDirection/2; d++){
                        int dirDeg = d*360/numberOfDirection
                        concat+= row["${names[i-1]}D${dirDeg+dirMedDeg}".toString()].round(2).toString()+"\n"
                    }
                }

        }
        assertEquals("637.1\n637.1\n32.53\n32.53\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n", concat)
    }

    @Test
    void testRsuRoofAreaDistribution() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, rsu_roof_area_distribution; " +
                "CREATE TABLE tempo_build AS SELECT a.*, b.id_rsu " +
                "FROM building_test a, rsu_build_corr b WHERE a.id_build = b.id_build AND a.id_build < 6 OR " +
                "a.id_build = b.id_build AND a.id_build < 29 AND a.id_build > 26")

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def  p =  Geoclimate.RsuIndicators.rsuRoofAreaDistribution()
        p.execute([rsuTable: "rsu_test", correlationBuildingTable: "tempo_build",
                   listLayersBottom: listLayersBottom, outputTableName: "rsu_roof_area_distribution",
                   datasource: h2GIS])
        def concat1 = ""
        def concat2 = ""
        h2GIS.eachRow("SELECT * FROM rsu_roof_area_distribution WHERE id_rsu = 1"){
            row ->
                // Iterate over columns
                for (i in 1..listLayersBottom.size()){
                    if (i == listLayersBottom.size()) {
                        concat1 += row["rsu_non_vert_roof_area${listLayersBottom[listLayersBottom.size() - 1]}_"].round(2) + "\n"
                        concat1 += row["rsu_vert_roof_area${listLayersBottom[listLayersBottom.size() - 1]}_"].round(2) + "\n"
                    }
                    else {
                        concat1+=row["rsu_non_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                        concat1+=row["rsu_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                    }
                }
        }
        h2GIS.eachRow("SELECT * FROM rsu_roof_area_distribution WHERE id_rsu = 13"){
            row ->
                // Iterate over columns
                for (i in 1..listLayersBottom.size()){
                    if (i == listLayersBottom.size()) {
                        concat2 += row["rsu_non_vert_roof_area${listLayersBottom[listLayersBottom.size() - 1]}_"].round(2) + "\n"
                        concat2 += row["rsu_vert_roof_area${listLayersBottom[listLayersBottom.size() - 1]}_"].round(2) + "\n"
                    }
                    else {
                        concat2+=row["rsu_non_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                        concat2+=row["rsu_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                    }
                }
        }
        assertEquals("405.25\n56.48\n289.27\n45.64\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n",
                concat1)
        assertEquals("355.02\n163.23\n404.01\n141.88\n244.92\n235.5\n48.98\n6.73\n0.0\n0.0\n0.0\n0.0\n",
                concat2)
    }

    @Test
    void testRsuEffectiveTerrainRoughnessHeight() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, rsu_table; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6")

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def pFacadeDistrib =  Geoclimate.RsuIndicators.rsuProjectedFacadeAreaDistribution()
        pFacadeDistrib.execute([buildingTable: "tempo_build", inputColumns: ["id_rsu", "the_geom"],
                                rsuTable: "rsu_test", listLayersBottom: listLayersBottom, numberOfDirection:
                                numberOfDirection, outputTableName: "rsu_projected_facade_area_distribution",
                                datasource: h2GIS])
        def  pGeomAvg =  Geoclimate.BlockIndicators.unweightedOperationFromLowerScale()
        pGeomAvg.execute([inputLowerScaleTableName: "tempo_build",inputCorrelationTableName: "rsu_build_corr",
                          inputIdLow: "id_build", inputIdUp: "id_rsu", inputToTransfo: "height_roof",
                          operations: ["GEOM_AVG"], outputTableName: "unweighted_operation_from_lower_scale",
                          datasource: h2GIS])

        h2GIS.execute("CREATE TABLE rsu_table AS SELECT a.*, b.geom_avg_height_roof " +
                "FROM rsu_projected_facade_area_distribution a, unweighted_operation_from_lower_scale b " +
                "WHERE a.id_rsu = b.id_rsu")
        def  p =  Geoclimate.RsuIndicators.rsuEffectiveTerrainRoughnessHeight()
        p.execute([rsuTable: "rsu_table", projectedFacadeAreaName: "rsu_projected_facade_area_distribution",
                   geometricMeanBuildingHeightName: "geom_avg_height_roof", prefixName: "test", listLayersBottom: listLayersBottom,
                   numberOfDirection: numberOfDirection, datasource: h2GIS])

        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness WHERE id_rsu = 1"){
            row ->
                concat += row["rsu_effective_terrain_roughness"].round(2)
                }
        assertEquals(1.6, concat)
    }

    @Test
    void testRsuLinearRoadOperations() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS road_tempo; CREATE TABLE road_tempo AS SELECT * " +
                "FROM road_test WHERE id_road < 7")

        def p1 =  Geoclimate.RsuIndicators.rsuLinearRoadOperations()
        p1.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["rsu_road_direction_distribution",
                "rsu_linear_road_density"], prefixName: "test", angleRangeSize: 30, levelConsiderated: null,
                datasource: h2GIS])
        def t0 = h2GIS.firstRow("SELECT rsu_road_direction_distribution_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        def t1 = h2GIS.firstRow("SELECT rsu_road_direction_distribution_d90_120 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        def t2 = h2GIS.firstRow("SELECT rsu_linear_road_density " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(25.59, t0.rsu_road_direction_distribution_d0_30.round(2))
        assertEquals(10.0, t1.rsu_road_direction_distribution_d90_120)
        assertEquals(0.0142, t2.rsu_linear_road_density.round(4))

        def p2 =  Geoclimate.RsuIndicators.rsuLinearRoadOperations()
        p2.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["rsu_road_direction_distribution"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [0], datasource: h2GIS])
        def t01 = h2GIS.firstRow("SELECT rsu_road_direction_distribution_h0_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(20, t01.rsu_road_direction_distribution_h0_d0_30)

        def p3 =  Geoclimate.RsuIndicators.rsuLinearRoadOperations()
        p3.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["rsu_linear_road_density"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [-1], datasource: h2GIS])
        def t001 = h2GIS.firstRow("SELECT rsu_linear_road_density_hminus1 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(0.00224, t001.rsu_linear_road_density_hminus1.round(5))
    }
}