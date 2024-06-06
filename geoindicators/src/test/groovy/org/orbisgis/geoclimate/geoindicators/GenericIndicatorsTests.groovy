/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.api.dataset.ISpatialTable
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.*
import static org.orbisgis.data.H2GIS.open

class GenericIndicatorsTests {

    @TempDir
    static File folder
    private static def h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "genericIndicatorsTests;AUTO_SERVER=TRUE")
    }

    @BeforeEach
    void beforeEach() {
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void unweightedOperationFromLowerScaleTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build0, tempo_build, tempo_rsu, unweighted_operation_from_lower_scale1, 
                unweighted_operation_from_lower_scale2, unweighted_operation_from_lower_scale3; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8; 
                CREATE TABLE tempo_rsu AS SELECT * FROM rsu_test WHERE id_rsu < 17"""

        def psum = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(h2GIS,
                "tempo_build", "block_test",
                "id_block", "id_build",
                ["area": ["SUM"]], "first")
        assert psum
        def pavg = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu",
                "id_build", ["number_building_neighbor": ["AVG"]],
                "second")
        assert pavg
        def pgeom_avg = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu",
                "id_build", ["height_roof": ["GEOM_AVG"]],
                "third")
        assert pgeom_avg
        def pdens = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu",
                "id_build", ["number_building_neighbor": ["AVG"],
                             "area"                    : ["SUM", "DENS"],
                             "building"                : ["NB_DENS"]],
                "fourth")
        assert pdens
        def pstd = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(h2GIS,
                "tempo_build", "tempo_rsu",
                "id_rsu", "id_build", ["number_building_neighbor": ["STD"]], "fifth")
        assert pstd

        def values = h2GIS.rows("SELECT sum_area FROM first_unweighted_operation_from_lower_scale WHERE id_block = 1 OR id_block = 4 ORDER BY id_block ASC")
        assert [156, 310] == values.collect { it.values() }.flatten()
        values = h2GIS.rows("SELECT avg_number_building_neighbor FROM second_unweighted_operation_from_lower_scale WHERE id_rsu = 1 OR id_rsu = 2 ORDER BY id_rsu ASC")
        assert [0.4, 0.0] == values.collect { it.values() }.flatten()

        def value = h2GIS.firstRow("SELECT geom_avg_height_roof FROM third_unweighted_operation_from_lower_scale WHERE id_rsu = 1")

        assertEquals 10.69, value.geom_avg_height_roof, 0.01

        values = h2GIS.rows("SELECT avg_number_building_neighbor, sum_area, area_density, area_density FROM fourth_unweighted_operation_from_lower_scale WHERE id_rsu = 1")

        assert [0.4, 606, 0.303] == values.collect { it.values() }.flatten()

        value = h2GIS.firstRow("SELECT std_number_building_neighbor FROM fifth_unweighted_operation_from_lower_scale WHERE id_rsu = 1")
        assertEquals 0.490, value.std_number_building_neighbor, 0.001

        def nb_rsu = h2GIS.firstRow "SELECT COUNT(*) AS NB FROM ${pgeom_avg}"
        def val_zero = h2GIS.firstRow "SELECT area_density AS val FROM ${pdens} WHERE id_rsu = 14"

        assert 16 == nb_rsu.nb
        assert 0 == val_zero.val
        // Test the fix concerning nb_dens_building (initially >0 while no building in RSU...)
        def nb_dens = h2GIS.firstRow("SELECT building_number_density FROM fourth_unweighted_operation_from_lower_scale WHERE id_rsu = 14")
        assert 0 == nb_dens["BUILDING_NUMBER_DENSITY"]
        def geom_ave = h2GIS.firstRow("SELECT geom_avg_height_roof FROM third_unweighted_operation_from_lower_scale WHERE id_rsu = 14")
        assert 0 == geom_ave["geom_avg_height_roof"]
    }

    @Test
    void weightedAggregatedStatisticsTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, tempo_rsu, one_weighted_aggregated_statistics, 
                two_weighted_aggregated_statistics, three_weighted_aggregated_statistics; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8; 
                CREATE TABLE tempo_rsu AS SELECT * FROM rsu_test WHERE id_rsu < 17;
        """

        def pavg = Geoindicators.GenericIndicators.weightedAggregatedStatistics(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu",
                ["height_roof": ["area": ["AVG"]]], "one")
        assert pavg
        def pstd = Geoindicators.GenericIndicators.weightedAggregatedStatistics(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu",
                ["height_roof": ["area": ["STD"]]],
                "two")
        assert pstd
        def pall = Geoindicators.GenericIndicators.weightedAggregatedStatistics(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu",
                ["height_wall": ["area": ["STD"]],
                 "height_roof": ["area": ["AVG", "STD"]]],
                "three")
        assert pall
        def concat = [0, 0, ""]
        h2GIS.eachRow("SELECT * FROM one_weighted_aggregated_statistics WHERE id_rsu = 1") {
            row -> concat[0] += row.avg_height_roof_area_weighted
        }
        h2GIS.eachRow("SELECT * FROM two_weighted_aggregated_statistics WHERE id_rsu = 1") {
            row -> concat[1] += row.std_height_roof_area_weighted
        }
        h2GIS.eachRow("SELECT * FROM three_weighted_aggregated_statistics WHERE id_rsu = 1") {
            row ->
                concat[2] += "${row.avg_height_roof_area_weighted.round(3)}\n" +
                        "${row.std_height_roof_area_weighted.round(1)}\n" +
                        "${row.std_height_wall_area_weighted.round(2)}\n"
        }
        def nb_rsu = h2GIS.firstRow("SELECT COUNT(*) AS NB FROM ${pavg}".toString())
        assertEquals 10.178, concat[0], 0.001
        assertEquals 2.5, concat[1], 0.1
        assert "10.178\n2.5\n2.52\n" == concat[2]
        assert 16 == nb_rsu.nb
    }

    @Test
    void geometryPropertiesTest() {
        h2GIS """
                DROP TABLE IF EXISTS spatial_table, test_geometry_properties;
                CREATE TABLE spatial_table (id int, the_geom GEOMETRY(LINESTRING));
                INSERT INTO spatial_table VALUES (1, 'LINESTRING(0 0, 0 10)'::GEOMETRY);
        """
        def p = Geoindicators.GenericIndicators.geometryProperties(h2GIS,
                "spatial_table",
                ["id", "the_geom"],
                ["st_issimple", "st_area", "area", "st_dimension"],
                "test")
        assert p
        assert p == "test_geometry_properties"
        h2GIS.getTable(p).eachRow {
            row ->
                assert row.the_geom
                assert row.issimple
                assert 0 == row.area
                assert 1 == row.dimension
                assert 1 == row.id
        }
    }

    @Test
    void buildingDirectionDistributionTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, test_MAIN_BUILDING_DIRECTION, test_DISTRIBUTION_REPARTITION; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9
        """

        def p = Geoindicators.GenericIndicators.buildingDirectionDistribution(h2GIS,
                "tempo_build", "block_test", "id_block",
                15f, ["equality", "uniqueness"], "test")
        assert p

        assertEquals 4.0 / 12, h2GIS.firstRow("SELECT * FROM test_MAIN_BUILDING_DIRECTION " +
                "WHERE id_block = 4").BUILDING_DIRECTION_EQUALITY, 0.0001
        assert "ANG97_5" == h2GIS.firstRow("SELECT * FROM test_MAIN_BUILDING_DIRECTION " +
                "WHERE id_block = 4")."main_building_direction"
        assertEquals((28.0 - 22.0) / (22 + 28.0), h2GIS.firstRow("SELECT * FROM test_MAIN_BUILDING_DIRECTION " +
                "WHERE id_block = 4").BUILDING_DIRECTION_UNIQUENESS, 0.0001)
    }

    @Test
    void buildingDirectionDistributionTest2() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, test_MAIN_BUILDING_DIRECTION, test_DISTRIBUTION_REPARTITION; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9
        """

        def p = Geoindicators.GenericIndicators.buildingDirectionDistribution(h2GIS,
                "tempo_build", "rsu_test",
                "id_rsu", 15,
                "test")
        assert p

        assertEquals(-1, h2GIS.firstRow("SELECT * FROM test_MAIN_BUILDING_DIRECTION " +
                "WHERE id_rsu = 14").BUILDING_DIRECTION_EQUALITY, 0.0001)
        assert "unknown" == h2GIS.firstRow("SELECT * FROM test_MAIN_BUILDING_DIRECTION " +
                "WHERE id_rsu = 14")."main_building_direction"
        assertEquals(-1, h2GIS.firstRow("SELECT * FROM test_MAIN_BUILDING_DIRECTION " +
                "WHERE id_rsu = 14").BUILDING_DIRECTION_UNIQUENESS, 0.0001)
    }

    @Test
    void distributionCharacterizationTest1() {
        // Tests with extremum = "GREATEST" and all distribIndicators

        // Create a table containing a distribution between columns
        h2GIS """
                DROP TABLE IF EXISTS distrib_test,test_DISTRIBUTION_REPARTITION;
                CREATE TABLE distrib_test(id integer, col1 double, col2 double, col3 double, col4 double);
                INSERT INTO distrib_test VALUES (1, 25, 25, 25, 25), (2, 10, 20, 40, 20), 
                                                (3, 0, 0, 60, 40), (4, 0, 0, 0, 100),
                                                (5, null, 0, 0, 100), (6, 0, 0, 0, 0);
        """


        def resultTab = Geoindicators.GenericIndicators.distributionCharacterization(h2GIS,
                "distrib_test", "distrib_test", "id",
                ["equality", "uniqueness"], "GREATEST",
                true, true, "test")
        assert resultTab

        assert 1 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 1").EQUALITY_VALUE
        assert 0.25 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 4").EQUALITY_VALUE
        assert 0 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 1").UNIQUENESS_VALUE
        assert 1 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 4").UNIQUENESS_VALUE
        assert "COL3" == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 2").EXTREMUM_COL
        assert "COL4" == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 4").EXTREMUM_COL
        assert "COL4" == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 3").EXTREMUM_COL2
        assert 60 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 3").EXTREMUM_VAL
        assert "unknown" == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 5").EXTREMUM_COL
        assert -1 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 5").EQUALITY_VALUE
        assert -1 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 5").UNIQUENESS_VALUE
        assert -1 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 6").UNIQUENESS_VALUE
        assert -1 == h2GIS.firstRow("SELECT * FROM $resultTab WHERE id = 6").EQUALITY_VALUE
    }

    @Test
    void distributionCharacterizationTest2() {
        // Tests with extremum = "LOWEST" and only "uniqueness" indicator

        // Create a table containing a distribution between columns
        h2GIS """
                DROP TABLE IF EXISTS distrib_test,test_DISTRIBUTION_REPARTITION;
                CREATE TABLE distrib_test(id integer, col1 double, col2 double, col3 double, col4 double);
                INSERT INTO distrib_test VALUES (1, 25, 25, 25, 25), (2, 10, 20, 40, 20), 
                                                (3, 0, 0, 60, 40), (4, 0, 0, 0, 100), 
                                                (5, 0, 0, 0, 0);
        """

        def p1 = Geoindicators.GenericIndicators.distributionCharacterization(h2GIS,
                "distrib_test", "distrib_test",
                "id", ["uniqueness"],
                "LEAST", "test")
        assertNotNull(p1)

        assert 0 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 1")["UNIQUENESS_VALUE"]
        assertEquals 1.0 / 3, h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION " +
                "WHERE id = 2")["UNIQUENESS_VALUE"], 0.0001
        assert -1 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 4")["UNIQUENESS_VALUE"]
        assert "COL1" == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 2")["EXTREMUM_COL"]
        assert -1 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 5").UNIQUENESS_VALUE
    }

    @Test
    void distributionCharacterizationTest3() {
        // Tests with only "inequality" indicator

        // Create a table containing a distribution between columns
        h2GIS """
                DROP TABLE IF EXISTS distrib_test,test_DISTRIBUTION_REPARTITION;
                CREATE TABLE distrib_test(id integer, col1 double, col2 double, col3 double, col4 double);
                INSERT INTO distrib_test VALUES (1, 25, 25, 25, 25), (2, 10, 20, 40, 20),
                                                (3, 0, 0, 60, 40), (4, 0, 0, 0, 100);
        """


        def p1 = Geoindicators.GenericIndicators.distributionCharacterization(h2GIS, "distrib_test",
                "distrib_test", "id",
                ["equality"], "LEAST", "test")

        assert 1 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 1")["EQUALITY_VALUE"]
        assert 0.25 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 4")["EQUALITY_VALUE"]
        assert "COL1" == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 2")["EXTREMUM_COL"]
        assert "COL1" == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 4")["EXTREMUM_COL"]
    }

    @Test
    void typeProportionTest() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build; 
                CREATE TABLE tempo_build AS SELECT a.*
                        FROM building_test a
                        WHERE a.id_build < 4;"""

        // Test 1
        def typeProportion = Geoindicators.GenericIndicators.typeProportion(h2GIS,
                "tempo_build", "id_rsu", "type", "rsu_test",
                ["industrial": ["industrial"], "residential": ["residential", "detached"]], null,
                "")

        assert typeProportion

        def result1 = h2GIS.firstRow("SELECT * FROM ${typeProportion}".toString())
        assert (156.0 / 296).trunc(3) == result1.area_fraction_industrial.trunc(3)
        assert (140.0 / 296).trunc(3) == result1.area_fraction_residential.trunc(3)
        def resultNull = h2GIS.firstRow("SELECT * FROM ${typeProportion} WHERE id_rsu = 14")
        assert 0 == resultNull.area_fraction_industrial
        assert 0 == resultNull.area_fraction_residential

        // Test 2
        def p2 = Geoindicators.GenericIndicators.typeProportion(h2GIS,
                "tempo_build", "id_rsu", "type", "rsu_test", null,
                ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                "")

        def result2 = h2GIS.firstRow("SELECT * FROM ${p2}")
        assert (312.0 / 832).trunc(3) == result2.floor_area_fraction_industrial.trunc(3)
        assert (520.0 / 832).trunc(3) == result2.floor_area_fraction_residential.trunc(3)

        // Test 3
        def p3 = Geoindicators.GenericIndicators.typeProportion(h2GIS,
                "tempo_build", "id_rsu", "type", "rsu_test",
                ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                "")

        def result3 = h2GIS.firstRow("SELECT * FROM ${p3}")
        assert (156.0 / 296).trunc(3) == result3.area_fraction_industrial.trunc(3)
        assert (140.0 / 296).trunc(3) == result3.area_fraction_residential.trunc(3)
        assert (312.0 / 832).trunc(3) == result3.floor_area_fraction_industrial.trunc(3)
        assert (520.0 / 832).trunc(3) == result3.floor_area_fraction_residential.trunc(3)
    }

    @Test
    void typeProportionTest2() {
        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS """
                DROP TABLE IF EXISTS tempo_build0, tempo_build, tempo_rsu, unweighted_operation_from_lower_scale1, 
                unweighted_operation_from_lower_scale2, unweighted_operation_from_lower_scale3; 
                CREATE TABLE tempo_build AS SELECT a.*
                        FROM building_test a, rsu_test b
                        WHERE id_build < 4;"""
        // Test 1
        assertThrows(Exception.class, ()-> Geoindicators.GenericIndicators.typeProportion(h2GIS,
                "tempo_build", "id_rsu", "type",
                "rsu_test",
                null, null, ""))
    }

    @Test
    void gatherScalesTest() {
        h2GIS """
                DROP TABLE IF EXISTS tempo_block, tempo_build, tempo_rsu; 
                CREATE TABLE tempo_block AS SELECT a.*, b.id_rsu
                        FROM block_test a, rsu_test b
                        WHERE a.the_geom && b.the_geom AND ST_COVERS(b.the_geom, a.the_geom);
                CREATE TABLE tempo_build 
                        AS SELECT   id_build, id_block, id_rsu, zindex, the_geom, height_wall, height_roof, area, 
                                    perimeter, nb_lev, total_facade_length, number_building_neighbor, contiguity, type
                        FROM building_test;
                CREATE TABLE tempo_rsu 
                        AS SELECT   id_rsu, the_geom, rsu_area, rsu_building_density, rsu_free_external_facade_density
                        FROM rsu_test;"""

        def colRsu = ["id_rsu", "the_geom", "build_std_height_wall", "build_std_height_roof", "build_std_area", "build_std_perimeter",
                      "build_std_total_facade_length", "build_std_number_building_neighbor", "build_std_contiguity",
                      "build_avg_height_wall", "build_avg_height_roof", "build_avg_area", "build_avg_perimeter",
                      "build_avg_total_facade_length", "build_avg_number_building_neighbor", "build_avg_contiguity",
                      "rsu_area", "rsu_building_density", "rsu_free_external_facade_density"]

        // Test 1
        def gatheredScales1 = Geoindicators.GenericIndicators.gatherScales(h2GIS,
                "tempo_build", "tempo_block", "tempo_rsu",
                "RSU", ["AVG", "STD"],
                "test")
        assert gatheredScales1
        def finalColRsu = h2GIS.getColumnNames(gatheredScales1).collect { it.toLowerCase() }
        assertEquals colRsu.sort(), finalColRsu.sort()
    }

    @Test
    void gatherScalesTest2() {
        h2GIS """
                DROP TABLE IF EXISTS tempo_block, tempo_build, tempo_rsu; 
                CREATE TABLE tempo_block AS SELECT a.*, b.id_rsu
                        FROM block_test a, rsu_test b
                        WHERE a.the_geom && b.the_geom AND ST_COVERS(b.the_geom, a.the_geom);
                CREATE TABLE tempo_build 
                        AS SELECT   id_build, id_block, id_rsu, zindex, the_geom, height_wall, height_roof, area, 
                                    perimeter, nb_lev, total_facade_length, number_building_neighbor, contiguity, type
                        FROM building_test;
                CREATE TABLE tempo_rsu 
                        AS SELECT   id_rsu, the_geom, rsu_area, rsu_building_density, rsu_free_external_facade_density
                        FROM rsu_test;"""

        def colBuild = ["build_height_wall", "build_height_roof", "build_area", "build_perimeter", "build_nb_lev",
                        "build_total_facade_length", "build_number_building_neighbor", "build_contiguity", "build_type",
                        "build_zindex", "id_block", "id_build", "id_rsu", "the_geom",
                        "build_std_height_wall", "build_std_height_roof", "build_std_area", "build_std_perimeter",
                        "build_std_total_facade_length", "build_std_number_building_neighbor", "build_std_contiguity",
                        "build_avg_height_wall", "build_avg_height_roof", "build_avg_area", "build_avg_perimeter",
                        "build_avg_total_facade_length", "build_avg_number_building_neighbor", "build_avg_contiguity",
                        "rsu_rsu_area", "rsu_rsu_building_density", "rsu_rsu_free_external_facade_density"]

        // Test 2
        def gatheredScales2 = Geoindicators.GenericIndicators.gatherScales(h2GIS,
                "tempo_build", "tempo_block",
                "tempo_rsu", "BUILDING",
                ["AVG", "STD"], "test")
        assert gatheredScales2
        def finalColBuild = h2GIS.getColumnNames(gatheredScales2).collect { it.toLowerCase() }
        assertEquals colBuild.sort(), finalColBuild.sort()

    }

        @Test
    void gatherScalesTest3() {
        h2GIS """
                DROP TABLE IF EXISTS tempo_block, tempo_build, tempo_rsu; 
                CREATE TABLE tempo_build (id_build int, id_block int, id_rsu int, the_geom geometry, height_roof float);
                CREATE TABLE tempo_block (id_block int, id_rsu int, the_geom geometry, area float);
                CREATE TABLE tempo_rsu (id_rsu int, the_geom geometry, area double);

                INSERT INTO tempo_build VALUES  (1, 1, 1, 'POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'::GEOMETRY, 3),
                                                (2, 2, null, 'POLYGON((-10 -10, -10 -8, -8 -8, -8 -10, -10 -10))'::GEOMETRY, 5);
                INSERT INTO tempo_block VALUES  (1, 1, 'POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'::GEOMETRY, 1),
                                                (2, null, 'POLYGON((-10 -10, -10 -8, -8 -8, -8 -10, -10 -10))'::GEOMETRY, 2);  
                INSERT INTO tempo_rsu VALUES  (1, 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY, 100),
                                                (2, 'POLYGON((0 10, 0 20, 10 20, 10 10, 0 10))'::GEOMETRY, 100);  
                                                   """

        // Test 1
        def gatheredScales1 = Geoindicators.GenericIndicators.gatherScales(h2GIS,
                "tempo_build", "tempo_block", "tempo_rsu",
                "BUILDING", ["AVG", "STD"], "test")

        // The building eing in no rsu should not be in the resulting table
        assertEquals h2GIS.firstRow("""SELECT COUNT(*) AS nb FROM $gatheredScales1""").nb, 1
    }

    @Test
    void upperScaleAreaStatisticsTest() {
        h2GIS.execute """DROP TABLE IF EXISTS rsu_test_limited;
                        CREATE TABLE rsu_test_limited
                            AS SELECT * FROM rsu_test
                            WHERE ID_RSU < 18;"""

        def indicatorTableName = "rsu_test_limited"
        def indicatorName = "rsu_area"
        def geometryColumnName = "the_geom"

        def geometry = h2GIS.getSpatialTable(indicatorTableName).getExtent(geometryColumnName)
        geometry.setSRID(h2GIS.getSpatialTable(indicatorTableName).srid)
        def gridProcess = Geoindicators.SpatialUnits.createGrid(h2GIS, geometry,
                1000D, 1000D)

        assertNotNull(gridProcess)
        assertEquals(4, h2GIS.getSpatialTable(gridProcess).getRowCount())

        def upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics(h2GIS,
                gridProcess, "id_grid", indicatorTableName,indicatorName, indicatorName, "agg")

        assertNotNull(upperScaleAreaStatistics)

        ISpatialTable upperStats = h2GIS.getSpatialTable(upperScaleAreaStatistics)
        assertNotNull(upperStats)

        def nb_indicators = h2GIS.rows "SELECT distinct ${indicatorName} AS nb FROM $indicatorTableName"
        def columns = upperStats.getColumnNames()
        columns.remove("ID_GRID")
        columns.remove("THE_GEOM")
        assertEquals(nb_indicators.size(), columns.size())

        def query = "drop table if exists babeth_zone; create table babeth_zone as select ID_GRID,"
        columns.each {
            query += "SUM($it) + "
        }
        query = query[0..-4]
        query += " as sum_indic from ${upperScaleAreaStatistics} group by ID_GRID"
        h2GIS.execute query

        def values = h2GIS.firstRow "select count(*) as nb from babeth_zone where sum_indic=0"
        assertEquals(1, values.NB)

        values = h2GIS.firstRow "select sum_indic as nb from babeth_zone where ID_GRID=3"
        assertEquals(0.11, values.NB)

        values = h2GIS.firstRow "select sum_indic as nb from babeth_zone where ID_GRID=0"
        assertEquals(0.007575, values.NB)

    }
}