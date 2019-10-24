package org.orbisgis.orbisprocess.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.orbisprocess.geoindicators.Geoindicators

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class BuildingIndicatorsTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open('./target/buildingdb;AUTO_SERVER=TRUE')
    }

    @BeforeEach
    void initData(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void sizePropertiesTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, test_building_size_properties; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build = 7;"

        def  p =  Geoindicators.BuildingIndicators.sizeProperties()
        assertTrue p.execute([inputBuildingTableName: "tempo_build",
                              operations:["building_volume", "building_floor_area", "building_total_facade_length",
                                          "building_passive_volume_ratio"],
                              prefixName : "test",datasource:h2GIS])
        h2GIS.getTable("test_building_size_properties").eachRow {
            row ->
                assertEquals(141, row.building_volume)
                assertEquals(47, row.building_floor_area)
                assertEquals(38, row.building_total_facade_length)
                assertEquals(0, row.building_passive_volume_ratio)
        }
    }

    @Test
    void neighborsPropertiesTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, test_building_neighbors_properties; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 7"

        def  p =  Geoindicators.BuildingIndicators.neighborsProperties()
        assertTrue p.execute([inputBuildingTableName: "tempo_build",
                              operations:["building_contiguity","building_common_wall_fraction",
                                          "building_number_building_neighbor"],
                              prefixName : "test",datasource:h2GIS])
        def concat = ["", "", ""]
        h2GIS.eachRow("SELECT * FROM test_building_neighbors_properties WHERE id_build = 1 OR id_build = 5 " +
                "ORDER BY id_build ASC"){
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
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, test_building_form_properties; CREATE TABLE tempo_build " +
                "AS SELECT * FROM building_test WHERE id_build < 8 OR id_build = 30"

        def  p =  Geoindicators.BuildingIndicators.formProperties()
        assertTrue p.execute([inputBuildingTableName: "tempo_build",
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
            row -> concat[2]+= "${row.building_raw_compacity.round(3)}\n"
        }
        h2GIS.eachRow("SELECT * FROM test_building_form_properties WHERE id_build = 1 OR id_build = 7 OR " +
                "id_build = 30 ORDER BY id_build ASC"){
            row -> concat[3]+= "${row.building_convexhull_perimeter_density.round(5)}\n"
        }
        assertEquals("1.0\n0.94\n".toString(),concat[0].toString())
        assertEquals("${(0.0380859375).round(5)}\n${(0.0522222222222222).round(5)}\n".toString(), concat[1].toString())
        assertEquals("5.607\n".toString(),  concat[2].toString())
        assertEquals("1.0\n0.78947\n0.85714\n".toString(), concat[3].toString())
    }

    @Test
    void minimumBuildingSpacingTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, test_building_form_properties; CREATE TABLE tempo_build AS " +
                "SELECT * FROM building_test WHERE id_build < 7"

        def  p =  Geoindicators.BuildingIndicators.minimumBuildingSpacing()
        assertTrue p.execute([inputBuildingTableName: "tempo_build", bufferDist:100, prefixName : "test",
                              datasource:h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_building_minimum_building_spacing WHERE id_build = 2 OR id_build = 4 " +
                "OR id_build = 6 ORDER BY id_build ASC"){
            row -> concat+= "${row.building_minimum_building_spacing}\n"
        }
        assertEquals("2.0\n0.0\n7.0\n", concat)
    }

    @Test
    void roadDistanceTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_road, test_building_road_distance; CREATE TABLE tempo_road " +
                "AS SELECT * FROM road_test WHERE id_road < 5"

        def  p =  Geoindicators.BuildingIndicators.roadDistance()
        assertTrue p.execute([inputBuildingTableName: "building_test", inputRoadTableName: "tempo_road", bufferDist:100,
                   prefixName : "test",datasource:h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_building_road_distance WHERE id_build = 6 OR id_build = 33 ORDER BY id_build ASC"){
            row -> concat+= "${row.building_road_distance.round(4)}\n"
        }
        assertEquals("23.9556\n100.0\n", concat)
    }

    @Test
    void likelihoodLargeBuildingTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_build2, test_building_neighbors_properties; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 29")

        def  pneighb =  Geoindicators.BuildingIndicators.neighborsProperties()
        assertTrue pneighb.execute([inputBuildingTableName: "tempo_build",
                   operations:["building_number_building_neighbor"],
                   prefixName : "test", datasource:h2GIS])

        // The number of neighbors are added to the tempo_build table
        h2GIS.execute "CREATE TABLE tempo_build2 AS SELECT a.id_build, a.the_geom, b.building_number_building_neighbor" +
                " FROM tempo_build a, test_building_neighbors_properties b WHERE a.id_build = b.id_build"

        def  p =  Geoindicators.BuildingIndicators.likelihoodLargeBuilding()
        assertTrue p.execute([inputBuildingTableName: "tempo_build2", nbOfBuildNeighbors: "building_number_building_neighbor",
                  prefixName : "test", datasource:h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_building_likelihood_large_building WHERE id_build = 4 OR id_build = 7 OR " +
                "id_build = 28 ORDER BY id_build ASC"){
            row -> concat+= "${row.building_likelihood_large_building.round(2)}\n"
        }
        assertEquals("0.0\n0.02\n1.0\n", concat)
    }

}