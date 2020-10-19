package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.api.dataset.ISpatialTable

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open

import org.orbisgis.commons.printer.Ascii

class GenericIndicatorsTests {

    private static def h2GIS

    private static def randomDbName() {
        "${GenericIndicatorsTests.simpleName}_${UUID.randomUUID().toString().replaceAll "-", "_"}"
    }

    @BeforeAll
    static void beforeAll() {
        h2GIS = open "./target/${randomDbName()};AUTO_SERVER=TRUE"
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

        def psum = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assert psum([
                inputLowerScaleTableName: "tempo_build",
                inputUpperScaleTableName: "block_test",
                inputIdUp               : "id_block",
                inputIdLow              : "id_build",
                inputVarAndOperations   : ["area": ["SUM"]],
                prefixName              : "first",
                datasource              : h2GIS])
        def pavg = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assert pavg([
                inputLowerScaleTableName: "tempo_build",
                inputUpperScaleTableName: "tempo_rsu",
                inputIdUp               : "id_rsu",
                inputIdLow              : "id_build",
                inputVarAndOperations   : ["number_building_neighbor": ["AVG"]],
                prefixName              : "second",
                datasource              : h2GIS])
        def pgeom_avg = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assert pgeom_avg([
                inputLowerScaleTableName: "tempo_build",
                inputUpperScaleTableName: "tempo_rsu",
                inputIdUp               : "id_rsu",
                inputIdLow              : "id_build",
                inputVarAndOperations   : ["height_roof": ["GEOM_AVG"]],
                prefixName              : "third",
                datasource              : h2GIS])
        def pdens = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assert pdens([
                inputLowerScaleTableName: "tempo_build",
                inputUpperScaleTableName: "tempo_rsu",
                inputIdUp               : "id_rsu",
                inputIdLow              : "id_build",
                inputVarAndOperations   : ["number_building_neighbor": ["AVG"],
                                           "area"                    : ["SUM", "DENS", "NB_DENS"]],
                prefixName              : "fourth",
                datasource              : h2GIS])
        def pstd = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assert pstd([
                inputLowerScaleTableName: "tempo_build",
                inputUpperScaleTableName: "tempo_rsu",
                inputIdUp               : "id_rsu",
                inputIdLow              : "id_build",
                inputVarAndOperations   : ["number_building_neighbor": ["STD"]],
                prefixName              : "fifth",
                datasource              : h2GIS])
        def concat = ["", "", 0, ""]

        h2GIS.eachRow("SELECT * FROM first_unweighted_operation_from_lower_scale WHERE id_block = 1 OR id_block = 4 ORDER BY id_block ASC") {
            row -> concat[0] += "${row.sum_area}\n"
        }
        h2GIS.eachRow("SELECT * FROM second_unweighted_operation_from_lower_scale WHERE id_rsu = 1 OR id_rsu = 2 ORDER BY id_rsu ASC") {
            row -> concat[1] += "${row.avg_number_building_neighbor}\n"
        }
        h2GIS.eachRow("SELECT * FROM third_unweighted_operation_from_lower_scale WHERE id_rsu = 1") {
            row -> concat[2] += row.geom_avg_height_roof
        }
        h2GIS.eachRow("SELECT * FROM fourth_unweighted_operation_from_lower_scale WHERE id_rsu = 1") {
            row ->
                concat[3] += "${row.avg_number_building_neighbor}\n"
                concat[3] += "${row.sum_area}\n"
                concat[3] += "${row.area_density}\n"
                concat[3] += "${row.area_number_density}\n"
        }
        concat[4] = h2GIS.firstRow("SELECT std_number_building_neighbor FROM fifth_unweighted_operation_from_lower_scale WHERE id_rsu = 1")

        def nb_rsu = h2GIS.firstRow "SELECT COUNT(*) AS NB FROM ${pgeom_avg.results.outputTableName}"
        def val_zero = h2GIS.firstRow "SELECT area_density AS val FROM ${pdens.results.outputTableName} WHERE id_rsu = 14"
        assert "156.0\n310.0\n" == concat[0]
        assert "0.4\n0.0\n" == concat[1]
        assertEquals 10.69, concat[2], 0.01
        assert "0.4\n606.0\n0.303\n0.0025\n" == concat[3]
        assertEquals 0.490, concat[4]["STD_NUMBER_BUILDING_NEIGHBOR"], 0.001
        assert 16 == nb_rsu.nb
        assert 0 == val_zero.val
        // Test the fix concerning nb_dens_building (initially >0 while no building in RSU...)
        def nb_dens = h2GIS.firstRow("SELECT area_number_density FROM fourth_unweighted_operation_from_lower_scale WHERE id_rsu = 14")
        assert 0 == nb_dens["AREA_NUMBER_DENSITY"]
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

        def pavg = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
        assert pavg([
                inputLowerScaleTableName : "tempo_build",
                inputUpperScaleTableName : "tempo_rsu",
                inputIdUp                : "id_rsu",
                inputVarWeightsOperations: ["height_roof": ["area": ["AVG"]]],
                prefixName               : "one",
                datasource               : h2GIS])
        def pstd = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
        assert pstd([
                inputLowerScaleTableName : "tempo_build",
                inputUpperScaleTableName : "tempo_rsu",
                inputIdUp                : "id_rsu",
                inputVarWeightsOperations: ["height_roof": ["area": ["STD"]]],
                prefixName               : "two",
                datasource               : h2GIS])
        def pall = Geoindicators.GenericIndicators.weightedAggregatedStatistics()
        assert pall([
                inputLowerScaleTableName : "tempo_build",
                inputUpperScaleTableName : "tempo_rsu",
                inputIdUp                : "id_rsu",
                inputVarWeightsOperations: ["height_wall": ["area": ["STD"]],
                                            "height_roof": ["area": ["AVG", "STD"]]],
                prefixName               : "three",
                datasource               : h2GIS])
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
        def nb_rsu = h2GIS.firstRow("SELECT COUNT(*) AS NB FROM ${pavg.results.outputTableName}".toString())
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
        def p = Geoindicators.GenericIndicators.geometryProperties()
        assert p([
                inputTableName: "spatial_table",
                inputFields   : ["id", "the_geom"],
                operations    : ["st_issimple", "st_area", "area", "st_dimension"],
                prefixName    : "test",
                datasource    : h2GIS])
        assert p.results.outputTableName == "test_geometry_properties"
        h2GIS.test_geometry_properties.eachRow {
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

        def p = Geoindicators.GenericIndicators.buildingDirectionDistribution()
        assert p([
                buildingTableName: "tempo_build",
                inputIdUp        : "id_block",
                tableUp          : "block_test",
                angleRangeSize   : 15,
                prefixName       : "test",
                datasource       : h2GIS,
                distribIndicator : ["equality", "uniqueness"]])

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

        def p = Geoindicators.GenericIndicators.buildingDirectionDistribution()
        assert p([
                buildingTableName: "tempo_build",
                tableUp          : "rsu_test",
                inputIdUp        : "id_rsu",
                angleRangeSize   : 15,
                prefixName       : "test",
                datasource       : h2GIS,
                distribIndicator : ["equality", "uniqueness"]])

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
                                                (5, null, 0, 0, 100);
        """


        def p1 = Geoindicators.GenericIndicators.distributionCharacterization()
        assert p1([
                distribTableName: "distrib_test",
                initialTable    : "distrib_test",
                inputId         : "id",
                distribIndicator: ["equality", "uniqueness"],
                extremum        : "GREATEST",
                keep2ndCol      : true,
                keepColVal      : true,
                prefixName      : "test",
                datasource      : h2GIS])
        def resultTab = p1.results.outputTableName

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
    }

    @Test
    void distributionCharacterizationTest2() {
        // Tests with extremum = "LOWEST" and only "uniqueness" indicator

        // Create a table containing a distribution between columns
        h2GIS """
                DROP TABLE IF EXISTS distrib_test,test_DISTRIBUTION_REPARTITION;
                CREATE TABLE distrib_test(id integer, col1 double, col2 double, col3 double, col4 double);
                INSERT INTO distrib_test VALUES (1, 25, 25, 25, 25), (2, 10, 20, 40, 20), 
                                                (3, 0, 0, 60, 40), (4, 0, 0, 0, 100);
        """


        def p1 = Geoindicators.GenericIndicators.distributionCharacterization()
        assert p1([
                distribTableName: "distrib_test",
                initialTable    : "distrib_test",
                inputId         : "id",
                distribIndicator: ["uniqueness"],
                extremum        : "LEAST",
                prefixName      : "test",
                datasource      : h2GIS])

        assert 0 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 1")["UNIQUENESS_VALUE"]
        assertEquals 1.0 / 3, h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION " +
                "WHERE id = 2")["UNIQUENESS_VALUE"], 0.0001
        assert 0 == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 4")["UNIQUENESS_VALUE"]
        assert "COL1" == h2GIS.firstRow("SELECT * FROM test_DISTRIBUTION_REPARTITION WHERE id = 2")["EXTREMUM_COL"]
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


        def p1 = Geoindicators.GenericIndicators.distributionCharacterization()
        assert p1([
                distribTableName: "distrib_test",
                initialTable    : "distrib_test",
                inputId         : "id",
                distribIndicator: ["equality"],
                extremum        : "LEAST",
                prefixName      : "test",
                datasource      : h2GIS])

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
        def p1 = Geoindicators.GenericIndicators.typeProportion()
        assert p1([
                inputTableName        : "tempo_build",
                idField               : "id_rsu",
                inputUpperTableName   : "rsu_test",
                typeFieldName         : "type",
                areaTypeAndComposition: ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                prefixName            : "",
                datasource            : h2GIS])

        def result1 = h2GIS.firstRow("SELECT * FROM ${p1.results.outputTableName}")
        assert (156.0 / 296).trunc(3) == result1.area_fraction_industrial.trunc(3)
        assert (140.0 / 296).trunc(3) == result1.area_fraction_residential.trunc(3)
        def resultNull = h2GIS.firstRow("SELECT * FROM ${p1.results.outputTableName} WHERE id_rsu = 14")
        assert 0 == resultNull.area_fraction_industrial
        assert 0 == resultNull.area_fraction_residential

        // Test 2
        def p2 = Geoindicators.GenericIndicators.typeProportion()
        assert p2([
                inputTableName             : "tempo_build",
                idField                    : "id_rsu",
                inputUpperTableName        : "rsu_test",
                typeFieldName              : "type",
                floorAreaTypeAndComposition: ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                prefixName                 : "",
                datasource                 : h2GIS])

        def result2 = h2GIS.firstRow("SELECT * FROM ${p2.results.outputTableName}")
        assert (312.0 / 832).trunc(3) == result2.floor_area_fraction_industrial.trunc(3)
        assert (520.0 / 832).trunc(3) == result2.floor_area_fraction_residential.trunc(3)

        // Test 3
        def p3 = Geoindicators.GenericIndicators.typeProportion()
        assert p3([
                inputTableName             : "tempo_build",
                idField                    : "id_rsu",
                inputUpperTableName        : "rsu_test",
                typeFieldName              : "type",
                areaTypeAndComposition     : ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                floorAreaTypeAndComposition: ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                prefixName                 : "",
                datasource                 : h2GIS])

        def result3 = h2GIS.firstRow("SELECT * FROM ${p3.results.outputTableName}")
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
        def p = Geoindicators.GenericIndicators.typeProportion()
        assertFalse(p([
                inputTableName             : "tempo_build",
                idField                    : "id_rsu",
                inputUpperTableName        : "rsu_test",
                typeFieldName              : "type",
                areaTypeAndComposition     : null,
                floorAreaTypeAndComposition: null,
                prefixName                 : "",
                datasource                 : h2GIS]))
        assertTrue(p([
                inputTableName             : "tempo_build",
                idField                    : "id_rsu",
                inputUpperTableName        : "rsu_test",
                typeFieldName              : "type",
                areaTypeAndComposition     : null,
                floorAreaTypeAndComposition: ["industrial": ["industrial"], "residential": ["residential", "detached"]],
                prefixName                 : "",
                datasource                 : h2GIS]))
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

        def colBuild = ["build_height_wall", "build_height_roof", "build_area", "build_perimeter", "build_nb_lev",
                        "build_total_facade_length", "build_number_building_neighbor", "build_contiguity", "build_type",
                        "build_zindex", "id_block", "id_build", "id_rsu", "the_geom",
                        "build_std_height_wall", "build_std_height_roof", "build_std_area", "build_std_perimeter",
                        "build_std_total_facade_length", "build_std_number_building_neighbor", "build_std_contiguity",
                        "build_avg_height_wall", "build_avg_height_roof", "build_avg_area", "build_avg_perimeter",
                        "build_avg_total_facade_length", "build_avg_number_building_neighbor", "build_avg_contiguity",
                        "rsu_rsu_area", "rsu_rsu_building_density", "rsu_rsu_free_external_facade_density"]
        def colRsu = ["id_rsu", "the_geom", "build_std_height_wall", "build_std_height_roof", "build_std_area", "build_std_perimeter",
                      "build_std_total_facade_length", "build_std_number_building_neighbor", "build_std_contiguity",
                      "build_avg_height_wall", "build_avg_height_roof", "build_avg_area", "build_avg_perimeter",
                      "build_avg_total_facade_length", "build_avg_number_building_neighbor", "build_avg_contiguity",
                      "rsu_area", "rsu_building_density", "rsu_free_external_facade_density"]

        // Test 1
        def applyGatherScales1 = Geoindicators.GenericIndicators.gatherScales()
        applyGatherScales1.execute([
                buildingTable    : "tempo_build",
                blockTable       : "tempo_block",
                rsuTable         : "tempo_rsu",
                targetedScale    : "RSU",
                operationsToApply: ["AVG", "STD"],
                prefixName       : "test",
                datasource       : h2GIS])
        def gatheredScales1 = applyGatherScales1.results.outputTableName
        def finalColRsu = h2GIS."$gatheredScales1".columns.collect { it.toLowerCase() }
        assertEquals colRsu.sort(), finalColRsu.sort()

        // Test 2
        def applyGatherScales2 = Geoindicators.GenericIndicators.gatherScales()
        applyGatherScales2.execute([
                buildingTable    : "tempo_build",
                blockTable       : "tempo_block",
                rsuTable         : "tempo_rsu",
                targetedScale    : "BUILDING",
                operationsToApply: ["AVG", "STD"],
                prefixName       : "test",
                datasource       : h2GIS])
        def gatheredScales2 = applyGatherScales2.results.outputTableName
        def finalColBuild = h2GIS."$gatheredScales2".columns.collect { it.toLowerCase() }
        assertEquals colBuild.sort(), finalColBuild.sort()
    }

    @Test
    void gatherScalesTest2() {
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
        def applyGatherScales1 = Geoindicators.GenericIndicators.gatherScales()
        applyGatherScales1.execute([
                buildingTable    : "tempo_build",
                blockTable       : "tempo_block",
                rsuTable         : "tempo_rsu",
                targetedScale    : "BUILDING",
                operationsToApply: ["AVG", "STD"],
                prefixName       : "test",
                datasource       : h2GIS])
        def gatheredScales1 = applyGatherScales1.results.outputTableName
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
        def gridProcess = Geoindicators.SpatialUnits.createGrid()
        gridProcess.execute([geometry  : geometry,
                             deltaX    : 1000D,
                             deltaY    : 1000D,
                             datasource: h2GIS])

        def targetTableName = gridProcess.results.outputTableName
        assertEquals(4, h2GIS.getSpatialTable(targetTableName).getRowCount())

        def upperScaleAreaStatistics = Geoindicators.GenericIndicators.upperScaleAreaStatistics()
        upperScaleAreaStatistics.execute(
                [upperTableName: targetTableName,
                 upperColumnId : "id",
                 lowerTableName: indicatorTableName,
                 lowerColumName: indicatorName,
                 prefixName    : "agg",
                 datasource    : h2GIS])

        def upperScaleTableResult = upperScaleAreaStatistics.results.outputTableName
        ISpatialTable upperStats = h2GIS.getSpatialTable(upperScaleTableResult)
        assertNotNull(upperStats)

        def nb_indicators = h2GIS.rows "SELECT distinct ${indicatorName} AS nb FROM $indicatorTableName"
        def columns = upperStats.getColumns()
        columns.remove("ID")
        columns.remove("THE_GEOM")
        assertEquals(nb_indicators.size(), columns.size())

        def query = "drop table if exists babeth_zone; create table babeth_zone as select id,"
        columns.each {
            query += "SUM($it) + "
        }
        query = query[0..-4]
        query += " as sum_indic from ${upperScaleTableResult} group by ID"
        h2GIS.execute query

        def values = h2GIS.firstRow "select count(*) as nb from babeth_zone where sum_indic=0"
        assertEquals(2, values.NB)

        values = h2GIS.firstRow "select sum_indic as nb from babeth_zone where id=3"
        assertEquals(110000, values.NB)

        values = h2GIS.firstRow "select sum_indic as nb from babeth_zone where id=0"
        assertEquals(7575, values.NB)
    }

    void zonalAreaTest() {
            def indicatorTableName = "zonal_area_building_test"
            def indicatorName = "height_wall"
            def query = """DROP TABLE IF EXISTS $indicatorTableName;
                       CREATE TABLE $indicatorTableName 
                       AS SELECT $indicatorName, the_geom 
                       FROM building_test;"""
            h2GIS.execute(query)

            def value1 = h2GIS.firstRow("SELECT height_wall FROM building_test")[indicatorName]
            def value2 = h2GIS.firstRow("SELECT $indicatorName FROM $indicatorTableName")[indicatorName]
            def value3 = h2GIS.firstRow("SELECT the_geom FROM $indicatorTableName")['the_geom'].toString()
            assert value1 == 8
            assert value2 == 8
            assertEquals('POLYGON ((4 4, 10 4, 10 30, 4 30, 4 4))', value3)

            def zonalAreaProcess = Geoindicators.GenericIndicators.zonalArea()
            zonalAreaProcess.execute(
                    [indicatorTableName: indicatorTableName,
                     indicatorName     : indicatorName,
                     prefixName        : "agg",
                     datasource        : h2GIS])
            assert h2GIS."$indicatorTableName"
    }
}

