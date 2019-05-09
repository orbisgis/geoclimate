package org.orbisgis

import org.orbisgis.datamanager.h2gis.H2GIS
import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class BuildingIndicatorsTests {

    @Test
    void geometryPropertiesTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        h2GIS.execute("""
                DROP TABLE IF EXISTS spatial_table, test_geometry_properties;
                CREATE TABLE spatial_table (id int, the_geom LINESTRING);
                INSERT INTO spatial_table VALUES (1, 'LINESTRING(0 0, 0 10)'::GEOMETRY);
        """)
       def  p =  Geoclimate.BuildingIndicators.geometryProperties()
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

    @Test
    void sizePropertiesTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, test_building_size_properties; CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build = 7;")

        def  p =  Geoclimate.BuildingIndicators.sizeProperties()
        p.execute([inputBuildingTableName: "tempo_build", operations:["building_volume", "building_floor_area", "building_total_facade_length",
                               "building_passive_volume_ratio"],
                   prefixName : "test",datasource:h2GIS])
        h2GIS.getTable("test_building_size_properties").eachRow {
            row ->
                assertEquals(141,row.building_volume)
                assertEquals(47, row.building_floor_area)
                assertEquals(38,  row.building_total_facade_length)
                assertEquals(0,  row.building_passive_volume_ratio)
        }
    }

    @Test
    void neighborsPropertiesTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, test_building_neighbors_properties; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 7")

        def  p =  Geoclimate.BuildingIndicators.neighborsProperties()
        p.execute([inputBuildingTableName: "tempo_build",
                   operations:["building_contiguity","building_common_wall_fraction",
                               "building_number_building_neighbor"],
                   prefixName : "test",datasource:h2GIS])
        def concat = ["", "", ""]
        h2GIS.eachRow("SELECT * FROM test_building_neighbors_properties WHERE id_build = 1 OR id_build = 5 ORDER BY id_build ASC"){
            row ->
                concat[0]+= "${row.building_contiguity.round(5)}\n"
                concat[1]+= "${row.building_common_wall_fraction.round(5)}\n"
                concat[2]+= "${row.building_number_building_neighbor}\n"

        }
        assertEquals("0.0\n${(50/552).round(5)}\n".toString(),concat[0].toString())
        assertEquals("0.0\n${(10/46).round(5)}\n".toString(), concat[1].toString())
        assertEquals("0\n1\n".toString(),  concat[2].toString())
    }

    @Test
    void formPropertiesTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, test_building_form_properties; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 8 OR id_build = 30")

        def  p =  Geoclimate.BuildingIndicators.formProperties()
        p.execute([inputBuildingTableName: "tempo_build",
                   operations:["building_concavity","building_form_factor",
                               "building_raw_compacity", "building_convexhull_perimeter_density"],
                   prefixName : "test",datasource:h2GIS])
        def concat = ["", "", "", ""]
        h2GIS.eachRow("SELECT * FROM test_building_form_properties WHERE id_build = 1 OR id_build = 7 ORDER BY id_build ASC"){
            row ->
                concat[0]+= "${row.building_concavity}\n"
                concat[1]+= "${row.building_form_factor.round(5)}\n"
        }
        h2GIS.eachRow("SELECT * FROM test_building_form_properties WHERE id_build = 2 ORDER BY id_build ASC"){
            row ->
                concat[2]+= "${row.building_raw_compacity.round(3)}\n"
        }
        h2GIS.eachRow("SELECT * FROM test_building_form_properties WHERE id_build = 1 OR id_build = 7 OR " +
                "id_build = 30 ORDER BY id_build ASC"){
            row ->
                concat[3]+= "${row.building_convexhull_perimeter_density.round(5)}\n"
        }
        assertEquals("1.0\n0.94\n".toString(),concat[0].toString())
        assertEquals("${(0.0380859375).round(5)}\n${(0.0522222222222222).round(5)}\n".toString(), concat[1].toString())
        assertEquals("5.607\n".toString(),  concat[2].toString())
        assertEquals("1.0\n0.78947\n0.85714\n".toString(), concat[3].toString())
    }

    @Test
    void minimumBuildingSpacingTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, test_building_form_properties; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 7")

        def  p =  Geoclimate.BuildingIndicators.minimumBuildingSpacing()
        p.execute([inputBuildingTableName: "tempo_build",
                   bufferDist:100,
                   prefixName : "test",datasource:h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_building_minimum_building_spacing WHERE id_build = 2 OR id_build = 4 OR id_build = 6 ORDER BY id_build ASC"){
            row ->
                concat+= "${row.building_minimum_building_spacing}\n"
        }
        assertEquals("2.0\n0.0\n7.0\n", concat)
    }

    @Test
    void roadDistanceTest() {
        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_road, test_building_road_distance; CREATE TABLE tempo_road AS SELECT * " +
                "FROM road_test WHERE id_road = 1")

        def  p =  Geoclimate.BuildingIndicators.roadDistance()
        p.execute([inputBuildingTableName: "building_test", inputRoadTableName: "tempo_road", bufferDist:100,
                   prefixName : "test",datasource:h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_building_road_distance WHERE id_build = 1 OR id_build = 6 ORDER BY id_build ASC"){
            row ->
                concat+= "${row.building_road_distance}\n"
        }
        assertEquals("100.0\n61.0\n", concat)
    }

}