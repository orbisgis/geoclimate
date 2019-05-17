package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals

class GenericIndicatorsTests {

    @Test
    void unweightedOperationFromLowerScaleTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build0, tempo_build, unweighted_operation_from_lower_scale1, " +
                "unweighted_operation_from_lower_scale2, unweighted_operation_from_lower_scale3; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE " +
                "id_build < 8;")

        def  psum =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
        psum.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "block_test",
                   inputIdUp: "id_block", inputVarAndOperations: ["building_area":["SUM"]],
                   prefixName: "first", datasource: h2GIS])
        def  pavg =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
        pavg.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "rsu_test",
                      inputIdUp: "id_rsu", inputVarAndOperations: ["building_number_building_neighbor":["AVG"]],
                      prefixName: "second", datasource: h2GIS])
        def  pgeom_avg =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
        pgeom_avg.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "rsu_test",
                      inputIdUp: "id_rsu", inputVarAndOperations: ["height_roof": ["GEOM_AVG"]],
                      prefixName: "third", datasource: h2GIS])
        def  pdens =  Geoclimate.GenericIndicators.unweightedOperationFromLowerScale()
        pdens.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "rsu_test",
                      inputIdUp: "id_rsu", inputVarAndOperations: ["building_number_building_neighbor":["AVG"],
                                                                   "building_area":["SUM", "DENS", "NB_DENS"]],
                      prefixName: "fourth", datasource: h2GIS])
        def concat = ["", "", 0, ""]
        h2GIS.eachRow("SELECT * FROM first_unweighted_operation_from_lower_scale WHERE id_block = 1 OR id_block = 4 ORDER BY id_block ASC"){
            row ->
                concat[0]+= "${row.sum_building_area}\n"
        }
        h2GIS.eachRow("SELECT * FROM second_unweighted_operation_from_lower_scale WHERE id_rsu = 1 OR id_rsu = 2 ORDER BY id_rsu ASC"){
            row ->
                concat[1]+= "${row.avg_building_number_building_neighbor}\n"
        }
        h2GIS.eachRow("SELECT * FROM third_unweighted_operation_from_lower_scale WHERE id_rsu = 1"){
            row ->
                concat[2]+= row.geom_avg_height_roof
        }
        h2GIS.eachRow("SELECT * FROM fourth_unweighted_operation_from_lower_scale WHERE id_rsu = 1"){
            row ->
                concat[3]+= "${row.avg_building_number_building_neighbor}\n"
                concat[3]+= "${row.sum_building_area}\n"
                concat[3]+= "${row.dens_building_area}\n"
                concat[3]+= "${row.bui_nb_dens}\n"
        }
        assertEquals("156.0\n310.0\n", concat[0])
        assertEquals("0.4\n0.0\n", concat[1])
        assertEquals(10.69, concat[2], 0.01)
        assertEquals("0.4\n606.0\n0.303\n0.0025\n", concat[3])
    }

    @Test
    void weightedAggregatedStatisticsTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, one_weighted_aggregated_statistics, " +
                "two_weighted_aggregated_statistics, three_weighted_aggregated_statistics; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test " +
                "WHERE id_build < 8")

        def  pavg =  Geoclimate.GenericIndicators.weightedAggregatedStatistics()
        pavg.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "rsu_test",
                      inputIdUp: "id_rsu", inputVarWeightsOperations: ["height_roof" : ["building_area": ["AVG"]]],
                      prefixName: "one", datasource: h2GIS])
        def  pstd =  Geoclimate.GenericIndicators.weightedAggregatedStatistics()
        pstd.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "rsu_test",
                      inputIdUp: "id_rsu", inputVarWeightsOperations: ["height_roof": ["building_area": ["STD"]]],
                      prefixName: "two", datasource: h2GIS])
        def  pall =  Geoclimate.GenericIndicators.weightedAggregatedStatistics()
        pall.execute([inputLowerScaleTableName: "tempo_build",inputUpperScaleTableName: "rsu_test",
                      inputIdUp: "id_rsu", inputVarWeightsOperations: ["height_wall": ["building_area": ["STD"]],
                                                                       "height_roof": ["building_area": ["AVG", "STD"]]],
                      prefixName: "three", datasource: h2GIS])
        def concat = [0, 0, ""]
        h2GIS.eachRow("SELECT * FROM one_weighted_aggregated_statistics WHERE id_rsu = 1"){
            row ->
                concat[0]+= row.weighted_avg_height_roof_building_area
        }
        h2GIS.eachRow("SELECT * FROM two_weighted_aggregated_statistics WHERE id_rsu = 1"){
            row ->
                concat[1]+= row.weighted_std_height_roof_building_area
        }
        h2GIS.eachRow("SELECT * FROM three_weighted_aggregated_statistics WHERE id_rsu = 1"){
            row ->
                concat[2]+= "${row.weighted_avg_height_roof_building_area.round(3)}\n" +
                        "${row.weighted_std_height_roof_building_area.round(1)}\n" +
                        "${row.weighted_std_height_wall_building_area.round(2)}\n"
        }
        assertEquals(10.178, concat[0], 0.001)
        assertEquals(2.5, concat[1], 0.1)
        assertEquals("10.178\n2.5\n2.52\n", concat[2])
    }

    @Test
    void geometryPropertiesTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        h2GIS.execute("""
                DROP TABLE IF EXISTS spatial_table, test_geometry_properties;
                CREATE TABLE spatial_table (id int, the_geom LINESTRING);
                INSERT INTO spatial_table VALUES (1, 'LINESTRING(0 0, 0 10)'::GEOMETRY);
        """)
        def  p =  Geoclimate.GenericIndicators.geometryProperties()
        p.execute([inputTableName: "spatial_table", inputFields:["id", "the_geom"], operations:["st_issimple","st_area", "area", "st_dimension"], prefixName : "test",datasource:h2GIS])
        assert p.results.outputTableName == "test_geometry_properties"
        h2GIS.getTable("test_geometry_properties").eachRow {
            row -> assert row.the_geom!=null
                assert row.st_issimple==true
                assertEquals(0,row.st_area)
                assertEquals(1, row.st_dimension)
                assertEquals(1,  row.id)
        }
    }
}