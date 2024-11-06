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
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.*
import static org.orbisgis.data.H2GIS.open

class RsuIndicatorsTests {


    @TempDir
    static File folder
    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "rsuIndicatorsTests;AUTO_SERVER=TRUE")
    }

    @BeforeEach
    void beforeEach() {
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void freeExternalFacadeDensityTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; 
                CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 8
        """
        // The geometry of the RSU is useful for the calculation, then it is inserted inside the build/rsu correlation table
        h2GIS """
                DROP TABLE IF EXISTS rsu_tempo; 
                CREATE TABLE rsu_tempo AS SELECT * FROM rsu_test
        """

        def p = Geoindicators.RsuIndicators.freeExternalFacadeDensity(h2GIS,
                "tempo_build",
                "rsu_tempo",
                "contiguity",
                "total_facade_length",
                "test")
        assertNotNull(p)
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_free_external_facade_density WHERE id_rsu = 1") {
            row -> concat += row.free_external_facade_density
        }
        assert 0.947 == concat
    }

    @Test
    void freeExternalFacadeDensityExactTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, tempo_rsu; 
                CREATE TABLE tempo_build(id_build int, the_geom geometry, height_wall double);
                INSERT INTO tempo_build VALUES (1, 'POLYGON((50 50, 150 50, 150 150, 140 150, 140 60, 60 60, 60 150,
                                                            50 150, 50 50))'::GEOMETRY, 20),
                                               (2, 'POLYGON((60 60, 140 60, 140 110, 60 110, 60 60))'::GEOMETRY, 10);
                CREATE TABLE tempo_rsu(id_rsu int, the_geom geometry);
                INSERT INTO tempo_rsu VALUES    (1, 'POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))'::GEOMETRY),
                                                (2, 'POLYGON((100 0, 200 0, 200 100, 100 100, 100 0))'::GEOMETRY),
                                                (3, 'POLYGON((0 100, 100 100, 100 200, 0 200, 0 100))'::GEOMETRY),
                                                (4, 'POLYGON((100 100, 200 100, 200 200, 100 200, 100 100))'::GEOMETRY),
                                                (5, 'POLYGON((200 200, 300 200, 300 300, 200 300, 200 200))'::GEOMETRY);
        """
        // First calculate the correlation table between buildings and rsu
        def buildingTableRelation = Geoindicators.SpatialUnits.spatialJoin(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu", null, "test")

        assertNotNull(buildingTableRelation)
        def p = Geoindicators.RsuIndicators.freeExternalFacadeDensityExact(h2GIS,
                buildingTableRelation, "tempo_rsu",
                "id_rsu", "test")
        assertNotNull(p)
        assertEquals 0.28, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").FREE_EXTERNAL_FACADE_DENSITY
        assertEquals 0.28, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 2").FREE_EXTERNAL_FACADE_DENSITY
        assertEquals 0.25, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 3").FREE_EXTERNAL_FACADE_DENSITY
        assertEquals 0.25, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 4").FREE_EXTERNAL_FACADE_DENSITY
        assertEquals 0d, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 5").FREE_EXTERNAL_FACADE_DENSITY
    }

    @Test
    void freeExternalFacadeDensityExactTest2() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, tempo_rsu; 
                CREATE TABLE tempo_build(id_build int, the_geom geometry, height_wall double);
                INSERT INTO tempo_build VALUES (1, 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY, 10),
                                               (2, 'POLYGON ((10 0, 20 0, 20 20, 10 20, 10 0))'::GEOMETRY, 10),
                                               (3, 'POLYGON ((30 30, 50 30, 50 50, 30 50, 30 30))'::GEOMETRY, 10),
                                               (4, 'POLYGON ((120 60, 130 60, 130 50, 120 50, 120 60))'::GEOMETRY, 10);
                CREATE TABLE tempo_rsu(id_rsu int, the_geom geometry);
                INSERT INTO tempo_rsu VALUES    (1, 'POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))'::GEOMETRY),
               (2, 'POLYGON((100 100, 200 100, 200 0, 100 0, 100 100))'::GEOMETRY) ;
        """
        // First calculate the correlation table between buildings and rsu
        def buildingTableRelation = Geoindicators.SpatialUnits.spatialJoin(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu", null, "test")

        assertNotNull(buildingTableRelation)
        def p = Geoindicators.RsuIndicators.freeExternalFacadeDensityExact(h2GIS,
                buildingTableRelation, "tempo_rsu",
                "id_rsu", "test")
        assertNotNull(p)
        assertEquals 0.16, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").FREE_EXTERNAL_FACADE_DENSITY
        assertEquals(0.04, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 2").FREE_EXTERNAL_FACADE_DENSITY)
    }

    @Test
    void groundSkyViewFactorTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build > 8 AND id_build < 27 OR id_build = 37"
        // The geometry of the buildings are useful for the calculation, then they are inserted inside
        // the build/rsu correlation table
        h2GIS "DROP TABLE IF EXISTS corr_tempo; CREATE TABLE corr_tempo AS SELECT a.*, b.the_geom, b.height_wall " +
                "FROM rsu_build_corr a, tempo_build b WHERE a.id_build = b.id_build"

        def p = Geoindicators.RsuIndicators.groundSkyViewFactor(h2GIS, "rsu_test", "id_rsu", "corr_tempo",
                0.008, 100, 60, "test")
        assertNotNull(p)
        assertEquals 0.54, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor " +
                "WHERE id_rsu = 8").ground_sky_view_factor, 0.05
        // For RSU having no buildings in and around them
        assertEquals 1, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 1").ground_sky_view_factor
        // For buildings that are RSU...
        assertEquals 0.5, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 18").ground_sky_view_factor, 0.03
    }

    @Test
    void aspectRatioTest() {
        def p = Geoindicators.RsuIndicators.aspectRatio(h2GIS, "rsu_test",
                "rsu_free_external_facade_density", "rsu_building_density", "test")
        assertNotNull(p)
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_aspect_ratio WHERE id_rsu = 1") {
            row -> concat += row.aspect_ratio
        }
        assertEquals(0.672, concat, 0.001)
    }

    @Test
    void aspectRatioTest2() {
        def p = Geoindicators.RsuIndicators.aspectRatio(h2GIS, "rsu_test",
                "rsu_free_external_facade_density", "rsu_building_density", "test")
        assert (p)
        def result = h2GIS.firstRow("SELECT aspect_ratio FROM test_rsu_aspect_ratio WHERE id_rsu = 17")
        assertEquals(null, result["aspect_ratio"])
    }

    @Test
    void streetWidthTest() {
        def p = Geoindicators.RsuIndicators.streetWidth(h2GIS, "rsu_test_all_indics_for_lcz",
                "GEOM_AVG_HEIGHT_ROOF", "aspect_ratio", "test")
        assertNotNull(p)
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_street_width WHERE id_rsu = 1") {
            row -> concat += row.street_width
        }
        assertEquals(7.5, concat, 0.001)
    }

    @Test
    void streetWidthTest2() {
        def p = Geoindicators.RsuIndicators.streetWidth(h2GIS, "rsu_test_all_indics_for_lcz",
                "GEOM_AVG_HEIGHT_ROOF", "aspect_ratio", "test")
        assert (p)
        def result = h2GIS.firstRow("SELECT street_width FROM test_rsu_street_width WHERE id_rsu = 18")
        assertEquals(null, result["street_width"])
    }

    @Test
    void projectedFacadeAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, test_rsu_projected_facade_area_distribution;" +
                " CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 6")

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def rangeDeg = 360 / numberOfDirection
        def p = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution(h2GIS, "tempo_build", "rsu_test", "id_rsu", listLayersBottom,
                numberOfDirection, "test")
        assertNotNull(p)
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_projected_facade_area_distribution WHERE id_rsu = 1") {
            row ->
                // Iterate over columns
                def names = []
                for (i in 1..listLayersBottom.size()) {
                    names[i - 1] = "projected_facade_area_distribution_H${listLayersBottom[i - 1]}" +
                            "_${listLayersBottom[i]}"
                    if (i == listLayersBottom.size()) {
                        names[listLayersBottom.size() - 1] = "projected_facade_area_distribution" +
                                "_H${listLayersBottom[listLayersBottom.size() - 1]}"
                    }
                    for (int d = 0; d < numberOfDirection / 2; d++) {
                        int dirDeg = d * 360 / numberOfDirection
                        concat += row["${names[i - 1]}_D${dirDeg}_${dirDeg + rangeDeg}".toString()].round(2).toString() + "\n"
                    }
                }

        }
        assertEquals("637.10\n637.10\n32.53\n32.53\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n", concat)
    }

    @Test
    void projectedFacadeAreaDistributionTest2() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, test_rsu_projected_facade_area_distribution;" +
                " CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 1"

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def rangeDeg = 360 / numberOfDirection
        def p = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution(h2GIS, "tempo_build", "rsu_test", "id_rsu", listLayersBottom,
                numberOfDirection, "test")
        assertNotNull(p)
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_projected_facade_area_distribution WHERE id_rsu = 1") {
            row ->
                // Iterate over columns
                def names = []
                for (i in 1..listLayersBottom.size()) {
                    names[i - 1] = "projected_facade_area_distribution_H${listLayersBottom[i - 1]}" +
                            "_${listLayersBottom[i]}"
                    if (i == listLayersBottom.size()) {
                        names[listLayersBottom.size() - 1] = "projected_facade_area_distribution" +
                                "_H${listLayersBottom[listLayersBottom.size() - 1]}"
                    }
                    for (int d = 0; d < numberOfDirection / 2; d++) {
                        int dirDeg = d * 360 / numberOfDirection
                        concat += row["${names[i - 1]}_D${dirDeg}_${dirDeg + rangeDeg}".toString()].round(2).toString() + "\n"
                    }
                }
        }
        assertEquals("0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n", concat)
    }

    @Test
    void roofAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, test_rsu_roof_area_distribution; " +
                "CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6 OR " +
                "id_build < 29 AND id_build > 26"

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def p = Geoindicators.RsuIndicators.roofAreaDistribution(h2GIS, "rsu_test", "tempo_build", listLayersBottom, "test")
        assertNotNull(p)
        def concat1 = ""
        def concat2 = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1") {
            row ->
                // Iterate over columns
                for (i in 1..listLayersBottom.size()) {
                    if (i == listLayersBottom.size()) {
                        concat1 += row["non_vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                        concat1 += row["vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                    } else {
                        concat1 += row["non_vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]}"].round(2) + "\n"
                        concat1 += row["vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]}"].round(2) + "\n"
                    }
                }
        }
        h2GIS.eachRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13") {
            row ->
                // Iterate over columns
                for (i in 1..listLayersBottom.size()) {
                    if (i == listLayersBottom.size()) {
                        concat2 += row["non_vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                        concat2 += row["vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                    } else {
                        concat2 += row["non_vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]}"].round(2) + "\n"
                        concat2 += row["vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]}"].round(2) + "\n"
                    }
                }
        }
        assertEquals("405.25\n56.48\n289.27\n45.64\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n",
                concat1)
        assertEquals("355.02\n163.23\n404.01\n141.88\n244.92\n235.50\n48.98\n6.73\n0.00\n0.00\n0.00\n0.00\n",
                concat2)

        // Test the optionally calculated roof densities
        def NV1 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1").NON_VERT_ROOF_DENSITY
        def V1 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1").VERT_ROOF_DENSITY
        def NV2 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13").NON_VERT_ROOF_DENSITY
        def V2 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13").VERT_ROOF_DENSITY
        assertEquals(796.64 / 2000, NV1 + V1, 0.001)
        assertEquals(1600.27 / 10000, NV2 + V2, 0.001)
    }

    @Test
    void effectiveTerrainRoughnesslengthTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS("DROP TABLE IF EXISTS tempo_build, rsu_table, BUILDING_INTERSECTION, BUILDING_INTERSECTION_EXPL, BUILDINGFREE, BUILDINGLAYER; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6")

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def pFacadeDistrib = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution(h2GIS, "tempo_build",
                "rsu_test", "id_rsu", listLayersBottom,
                numberOfDirection, "test")
        assertNotNull(pFacadeDistrib)
        def pGeomAvg = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(h2GIS, "tempo_build",
                "rsu_build_corr", "id_rsu",
                "id_build", ["height_roof": ["GEOM_AVG"]],
                "test")
        assertNotNull(pGeomAvg)

        // Add the geometry field in the previous resulting Tables
        h2GIS "ALTER TABLE test_unweighted_operation_from_lower_scale add column the_geom GEOMETRY;" +
                "UPDATE test_unweighted_operation_from_lower_scale set the_geom = (SELECT a.the_geom FROM " +
                "rsu_test a WHERE a.id_rsu = test_unweighted_operation_from_lower_scale.id_rsu);"

        h2GIS "CREATE TABLE rsu_table AS SELECT a.*, b.geom_avg_height_roof, b.the_geom " +
                "FROM test_rsu_projected_facade_area_distribution a, test_unweighted_operation_from_lower_scale b " +
                "WHERE a.id_rsu = b.id_rsu"
        def p = Geoindicators.RsuIndicators.effectiveTerrainRoughnessLength(h2GIS, "rsu_table", "id_rsu",
                "projected_facade_area_distribution", "geom_avg_height_roof",
                listLayersBottom, numberOfDirection, "test")
        assertNotNull(p)
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness_length WHERE id_rsu = 1") {
            row -> concat += row["effective_terrain_roughness_length"].round(2)
        }
        assertEquals(1.60, concat)
    }

    @Test
    void linearRoadOperationsTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS road_tempo; CREATE TABLE road_tempo AS SELECT * " +
                "FROM road_test WHERE id_road < 7"

        def p1 = Geoindicators.RsuIndicators.linearRoadOperations(h2GIS, "rsu_test",
                "road_test", ["road_direction_distribution", "linear_road_density"],
                30, null, "test")
        assertNotNull(p1)
        def t0 = h2GIS.firstRow("SELECT road_direction_distribution_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        def t1 = h2GIS.firstRow("SELECT road_direction_distribution_d90_120 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        def t2 = h2GIS.firstRow("SELECT linear_road_density " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(25.59, t0.road_direction_distribution_d0_30.round(2))
        assertEquals(10.0, t1.road_direction_distribution_d90_120)
        assertEquals(0.0142, t2.linear_road_density.round(4))

        def p2 = Geoindicators.RsuIndicators.linearRoadOperations(h2GIS, "rsu_test", "road_test", ["road_direction_distribution"],
                30, [0], "test")
        assertNotNull(p2)
        def t01 = h2GIS.firstRow("SELECT road_direction_distribution_h0_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(20, t01.road_direction_distribution_h0_d0_30)

        def p3 = Geoindicators.RsuIndicators.linearRoadOperations(h2GIS, "rsu_test", "road_test", ["linear_road_density"],
                30, [-1], "test")
        assertNotNull(p3)
        def t001 = h2GIS.firstRow("SELECT linear_road_density_hminus1 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(0.00224, t001.linear_road_density_hminus1.round(5))
    }

    @Test
    void effectiveTerrainRoughnessClassTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT *, CASEWHEN(id_rsu = 1, 2.3," +
                "CASEWHEN(id_rsu = 2, 0.1, null)) AS effective_terrain_roughness_length FROM rsu_test"

        def p = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass(h2GIS, "rsu_tempo", "id_rsu", "effective_terrain_roughness_length",
                "test")
        assertNotNull(p)
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness_class WHERE id_rsu < 4 ORDER BY id_rsu ASC") {
            row -> concat += "${row["effective_terrain_roughness_class"]}\n".toString()
        }
        assertEquals("8\n4\nnull\n", concat)
    }


    @Test
    void extendedFreeFacadeFractionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6 OR id_build = 35"
        // The geometry of the RSU is useful for the calculation, then it is inserted inside the build/rsu correlation table
        h2GIS "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test WHERE id_rsu = 1"

        def p = Geoindicators.RsuIndicators.extendedFreeFacadeFraction(h2GIS, "tempo_build",
                "rsu_tempo", "contiguity", "total_facade_length",
                30, "test")
        assertNotNull(p)
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_extended_free_facade_fraction WHERE id_rsu = 1") {
            row -> concat += row.extended_free_facade_fraction.round(3)
        }
        assertEquals(0.177, concat)
    }

    @Test
    void smallestCommunGeometryTest() {
        h2GIS.load(SpatialUnitsTests.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("building_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("zone_test.geojson"), true)

        def outputTableGeoms = Geoindicators.SpatialUnits.prepareTSUData(h2GIS,
                'zone_test', 'road_test', '',
                'veget_test', 'hydro_test', "", "",
                10000, 2500, 10000, "prepare_rsu")

        assertNotNull h2GIS.getTable(outputTableGeoms)

        def outputTable = Geoindicators.SpatialUnits.createTSU(h2GIS, outputTableGeoms, "", "rsu")

        def outputTableStats = Geoindicators.RsuIndicators.smallestCommunGeometry(h2GIS,
                outputTable, "id_rsu", "building_test", "road_test", "hydro_test", "veget_test", "", "",
                "test")
        assertNotNull(outputTableStats)

        h2GIS """DROP TABLE IF EXISTS stats_rsu;
                    CREATE INDEX ON $outputTableStats (ID_RSU);
                   CREATE TABLE stats_rsu AS SELECT b.the_geom,
                round(sum(CASE WHEN a.low_vegetation=1 THEN a.area ELSE 0 END),1) AS low_VEGETATION_sum,
                round(sum(CASE WHEN a.high_vegetation=1 THEN a.area ELSE 0 END),1) AS high_VEGETATION_sum,
                round(sum(CASE WHEN a.water=1 THEN a.area ELSE 0 END),1) AS water_sum,
                round(sum(CASE WHEN a.road=1 THEN a.area ELSE 0 END),1) AS road_sum,
                round(sum(CASE WHEN a.building=1 THEN a.area ELSE 0 END),1) AS building_sum, a.id_rsu
                FROM $outputTableStats AS a, $outputTable b WHERE a.id_rsu=b.id_rsu GROUP BY b.id_rsu;
            CREATE INDEX ON stats_rsu(id_rsu);"""


        //Check the sum of building areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_building;
                          CREATE TABLE stats_building as select sum(st_area(the_geom)) as building_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from building_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.zindex=0 ) group by id_rsu;
                           CREATE INDEX ON stats_building(id_rsu);                           
                           DROP TABLE IF EXISTS building_compare_stats;
                           CREATE TABLE building_compare_stats as select (a.building_sum- b.building_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_building as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from building_compare_stats where diff > 1").count == 0


        //Check the sum of low vegetation areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_vegetation_low;
                          CREATE TABLE stats_vegetation_low as select sum(st_area(the_geom)) as vegetation_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from veget_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.type='low' ) group by id_rsu;
                           CREATE INDEX ON stats_vegetation_low(id_rsu);                           
                           DROP TABLE IF EXISTS vegetation_low_compare_stats;
                           CREATE TABLE vegetation_low_compare_stats as select (a.low_VEGETATION_sum- b.vegetation_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_vegetation_low as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from vegetation_low_compare_stats where diff > 1").count == 0

        //Check the sum of high vegetation areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_vegetation_high;
                          CREATE TABLE stats_vegetation_high as select sum(st_area(the_geom)) as vegetation_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from veget_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.type='high' ) group by id_rsu;
                           CREATE INDEX ON stats_vegetation_high(id_rsu);                           
                           DROP TABLE IF EXISTS vegetation_high_compare_stats;
                           CREATE TABLE vegetation_high_compare_stats as select (a.high_VEGETATION_sum- b.vegetation_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_vegetation_high as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from vegetation_high_compare_stats where diff > 1").count == 0

        //Check the sum of water areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_water;
                          CREATE TABLE stats_water as select sum(st_area(the_geom)) as water_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from hydro_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.zindex=0 ) group by id_rsu;
                           CREATE INDEX ON stats_water(id_rsu);                           
                           DROP TABLE IF EXISTS water_compare_stats;
                           CREATE TABLE water_compare_stats as select (a.water_sum- b.water_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_water as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from water_compare_stats where diff > 1").count == 0

    }

    @Test
    void surfaceFractionTest() {
        // Only the RSU 4 is conserved for the test
        h2GIS "DROP TABLE IF EXISTS rsu_tempo;" +
                "CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test WHERE id_rsu = 4"

        // Need to create the smallest geometries used as input of the surface fraction process
        def tempoTable = Geoindicators.RsuIndicators.smallestCommunGeometry(h2GIS,
                "rsu_tempo", "id_rsu", "building_test", "", "hydro_test", "veget_test", "", "",
                "test")
        assertNotNull(tempoTable)

        // Apply the surface fractions for different combinations
        // combination 1
        def superpositions0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        String p0 = Geoindicators.RsuIndicators.surfaceFractions(h2GIS,
                "rsu_tempo", "id_rsu", tempoTable,
                superpositions0, priorities0, "test")
        assertNotNull(p0)
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0}")
        assertEquals(1.0 / 5, result0["high_vegetation_building_fraction"])
        assertEquals(3.0 / 20, result0["high_vegetation_low_vegetation_fraction"])
        assertEquals(3.0 / 20, result0["high_vegetation_fraction"])
        assertEquals(3.0 / 20, result0["low_vegetation_fraction"])
        assertEquals(1.0 / 4, result0["water_fraction"])
        assertEquals(1.0 / 10, result0["building_fraction"])
        assertEquals(0d, result0["undefined_fraction"])

        // combination 2

        def superpositions1 = ["high_vegetation": ["building", "water", "low_vegetation", "road", "impervious"]]
        def priorities1 = ["building", "water", "high_vegetation", "low_vegetation", "road", "impervious"]
        def p1 = Geoindicators.RsuIndicators.surfaceFractions(h2GIS,
                "rsu_tempo", "id_rsu", tempoTable,
                superpositions1, priorities1,
                "test")
        assertNotNull(p1)
        def result1 = h2GIS.firstRow("SELECT * FROM ${p1}")
        assertEquals(1.0 / 5, result1["high_vegetation_building_fraction"])
        assertEquals(3.0 / 20, result1["high_vegetation_low_vegetation_fraction"])
        assertEquals(3.0 / 20, result1["high_vegetation_fraction"])
        assertEquals(3.0 / 20, result1["low_vegetation_fraction"])
        assertEquals(3.0 / 20, result1["water_fraction"])
        assertEquals(1.0 / 5, result1["building_fraction"])
        assertEquals(0d, result0["undefined_fraction"])

        // combination 3

        def superpositions2 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"],
                               "building"       : ["low_vegetation"]]
        def p2 = Geoindicators.RsuIndicators.surfaceFractions(h2GIS,
                "rsu_tempo", "id_rsu", tempoTable,
                superpositions2, priorities0,
                "test")
        assertNotNull(p2)
        def result2 = h2GIS.firstRow("SELECT * FROM ${p2}")
        assertEquals(1.0 / 5, result2["high_vegetation_building_fraction"])
        assertEquals(3.0 / 20, result2["high_vegetation_low_vegetation_fraction"])
        assertEquals(3.0 / 20, result2["high_vegetation_fraction"])
        assertEquals(1.0 / 10, result2["building_low_vegetation_fraction"])
        assertEquals(3.0 / 20, result2["low_vegetation_fraction"])
        assertEquals(1.0 / 4, result2["water_fraction"])
        assertEquals(0d, result2["building_fraction"])
        assertEquals(0d, result0["undefined_fraction"])
    }

    @Test
    void surfaceFractionTest3() {
        // Test whether the road fraction is taken into account...
        h2GIS "DROP TABLE IF EXISTS rsu_tempo, road_tempo;" +
                "CREATE TABLE rsu_tempo(id_rsu int, the_geom geometry, rsu_area float, rsu_building_density float, rsu_free_external_facade_density float);" +
                "INSERT INTO rsu_tempo VALUES  (1, 'POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 10000, 0.4, null);" +
                "CREATE TABLE road_tempo(id_road int, the_geom geometry, width float, zindex int, crossing varchar(30));" +
                "INSERT INTO road_tempo VALUES (1, 'LINESTRING (1000 1000, 1000 1100)'::GEOMETRY, 10, 0, null);"

        // Need to create the smallest geometries used as input of the surface fraction process
        def tempoTable = Geoindicators.RsuIndicators.smallestCommunGeometry(h2GIS,
                "rsu_tempo", "id_rsu", null, "road_tempo", null, null, null, null,
                "test")
        assertNotNull(tempoTable)

        // Apply the surface fractions for different combinations
        // combination 1

        def superpositions0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        def p0 = Geoindicators.RsuIndicators.surfaceFractions(h2GIS,
                "rsu_tempo", "id_rsu", tempoTable,
                superpositions0, priorities0, "test")
        assertNotNull(p0)
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0}")
        assertEquals(5.0 / 100, result0["road_fraction"])
        assertEquals(0.95d, result0["undefined_fraction"])
    }

    @Test
    void surfaceFractionTest4() {
        // Test whether the rsu having no surface fraction is not set to null
        h2GIS """DROP TABLE IF EXISTS rsu_tempo, smallest_geom;
                            CREATE TABLE rsu_tempo(id_rsu int, the_geom geometry);
                            INSERT INTO rsu_tempo VALUES  (1, 'POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY);
                            CREATE TABLE smallest_geom(area double, low_vegetation integer, high_vegetation integer,
                                water integer, impervious integer, road integer, building integer, rail integer, id_rsu integer);
                                INSERT INTO smallest_geom VALUES (923, 0, 1, 0, 0, 0, 0, 0, 2)"""


        // Apply the surface fractions for different combinations
        // combination 1
        def superpositions0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        def p0 = Geoindicators.RsuIndicators.surfaceFractions(h2GIS,
                "rsu_tempo", "id_rsu", "smallest_geom",
                superpositions0, priorities0, "test")
        assertNotNull(p0)
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0} WHERE ID_RSU=1")
        assertEquals(0d, result0["building_fraction"])
        assertEquals(1d, result0["undefined_fraction"])
    }

    @Test
    void surfaceFractionTest5() {
        // Only the RSU 4 is conserved for the test
        h2GIS "DROP TABLE IF EXISTS rsu_tempo;" +
                "CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test WHERE id_rsu = 4"
        // Need to create the smallest geometries used as input of the surface fraction process
        String tempoTable = Geoindicators.RsuIndicators.smallestCommunGeometry(h2GIS,
                "rsu_tempo", "id_rsu", "building_test", null, "hydro_test", "veget_test", null, null,
                "test")
        assertNotNull(tempoTable)


        def superpositions0 = [:]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        def p0 = Geoindicators.RsuIndicators.surfaceFractions(h2GIS,
                "rsu_tempo", "id_rsu", tempoTable,
                superpositions0, priorities0, "test")
        assertNotNull(p0)
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0}")
        assertEquals(3.0 / 10, result0["high_vegetation_fraction"])
        assertEquals(3.0 / 20, result0["low_vegetation_fraction"])
        assertEquals(1.0 / 4, result0["water_fraction"])
        assertEquals(3.0 / 10, result0["building_fraction"])
        assertEquals(0d, result0["undefined_fraction"])
    }

    @Test
    void buildingSurfaceDensityTest() {
        h2GIS """
                DROP TABLE IF EXISTS facade_density_tab, building_fraction_tab; 
                CREATE TABLE facade_density_tab(id_rsu int, facade_density double);
                INSERT INTO facade_density_tab VALUES (1, 1.2),
                                               (2, 0);
                CREATE TABLE building_fraction_tab(id_rsu int, building_fraction double);
                INSERT INTO building_fraction_tab VALUES    (1, 0.5),
                                                (2, 1);
        """

        def p0 = Geoindicators.RsuIndicators.buildingSurfaceDensity(h2GIS,
                "facade_density_tab", "building_fraction_tab",
                "facade_density", "building_fraction",
                "id_rsu", "test")
        assertNotNull(p0)
        def result1 = h2GIS.firstRow("SELECT * FROM ${p0} WHERE id_rsu=1")
        assertEquals(1.7, result1["building_surface_density"])
        def result2 = h2GIS.firstRow("SELECT * FROM ${p0} WHERE id_rsu=2")
        assertEquals(1.0, result2["building_surface_density"])
    }

    @Test
    void roofFractionDistributionExactTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, tempo_rsu; 
                CREATE TABLE tempo_build(id_build int, the_geom geometry, height_wall double, height_roof double);
                INSERT INTO tempo_build VALUES (1, 'POLYGON((-50 -50, 50 -50, 50 50, -50 50, -50 -50))'::GEOMETRY, 2, 4),
                                               (2, 'POLYGON((50 -50, 150 -50, 150 50, 50 50, 50 -50))'::GEOMETRY, 18, 24),
                                               (3, 'POLYGON((50 50, 100 50, 100 150, 50 150, 50 50))'::GEOMETRY, 60, 60);
                CREATE TABLE tempo_rsu(id_rsu int, the_geom geometry);
                INSERT INTO tempo_rsu VALUES    (1, 'POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))'::GEOMETRY),
                                                (2, 'POLYGON((100 0, 200 0, 200 100, 100 100, 100 0))'::GEOMETRY),
                                                (3, 'POLYGON((0 100, 100 100, 100 200, 0 200, 0 100))'::GEOMETRY),
                                                (4, 'POLYGON((100 100, 200 100, 200 200, 100 200, 100 100))'::GEOMETRY);
            """
        // First calculate the correlation table between buildings and rsu
        String createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu", null, "test")
        assertNotNull(createScalesRelationsGridBl)
        def p = Geoindicators.RsuIndicators.roofFractionDistributionExact(h2GIS,
                "tempo_rsu", createScalesRelationsGridBl,
                "id_rsu",
                [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50], "test")

        assertNotNull(p)
        assertEquals 1.0 / 3, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").ROOF_FRACTION_DISTRIBUTION_H0_5, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").ROOF_FRACTION_DISTRIBUTION_H15_20, 0.00001
        assertEquals 1.0 / 3, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").ROOF_FRACTION_DISTRIBUTION_H20_25, 0.00001
        assertEquals 1.0 / 3, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").ROOF_FRACTION_DISTRIBUTION_H50, 0.00001
        assertEquals 1.0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 2").ROOF_FRACTION_DISTRIBUTION_H20_25, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 2").ROOF_FRACTION_DISTRIBUTION_H0_5, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 2").ROOF_FRACTION_DISTRIBUTION_H50, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 3").ROOF_FRACTION_DISTRIBUTION_H20_25, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 3").ROOF_FRACTION_DISTRIBUTION_H0_5, 0.00001
        assertEquals 1.0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 3").ROOF_FRACTION_DISTRIBUTION_H50, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 4").ROOF_FRACTION_DISTRIBUTION_H20_25, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 4").ROOF_FRACTION_DISTRIBUTION_H0_5, 0.00001
        assertEquals 0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 4").ROOF_FRACTION_DISTRIBUTION_H50, 0.00001
    }

    @Test
    void frontalAreaIndexDistributionTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS """
                DROP TABLE IF EXISTS tempo_build, tempo_rsu; 
                CREATE TABLE tempo_build(id_build int, the_geom geometry, height_wall double);
                INSERT INTO tempo_build VALUES (1, 'POLYGON((-50 -50, 50 -50, 50 50, -50 50, -50 -50))'::GEOMETRY, 3),
                                               (2, 'POLYGON((50 -50, 150 -50, 150 50, 50 50, 50 -50))'::GEOMETRY, 21),
                                               (3, 'POLYGON((50 50, 100 50, 100 150, 50 150, 50 50))'::GEOMETRY, 60);
                CREATE TABLE tempo_rsu(id_rsu int, the_geom geometry);
                INSERT INTO tempo_rsu VALUES    (1, 'POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))'::GEOMETRY),
                                                (2, 'POLYGON((100 0, 200 0, 200 100, 100 100, 100 0))'::GEOMETRY),
                                                (3, 'POLYGON((0 100, 100 100, 100 200, 0 200, 0 100))'::GEOMETRY),
                                                (4, 'POLYGON((100 100, 200 100, 200 200, 100 200, 100 100))'::GEOMETRY),
                                                (5, 'POLYGON((200 200, 300 200, 300 300, 200 300, 200 200))'::GEOMETRY);
            """
        // First calculate the correlation table between buildings and rsu
        def createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin(h2GIS,
                "tempo_build", "tempo_rsu", "id_rsu", null, "test")

        assertNotNull(createScalesRelationsGridBl)

        def p = Geoindicators.RsuIndicators.frontalAreaIndexDistribution(h2GIS,
                createScalesRelationsGridBl,
                "tempo_rsu",
                "id_rsu",
                [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
                12, true,
                "test")
        assertNotNull(p)
        assertEquals 0.00566, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").FRONTAL_AREA_INDEX_H0_5_D30_60, 0.00001
        assertEquals 0.00321, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 1").FRONTAL_AREA_INDEX_H50_61_D30_60, 0.00001
        assertEquals 0.00321, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 4").FRONTAL_AREA_INDEX_H50_61_D30_60, 0.00001
        assertEquals 0.0, h2GIS.firstRow("SELECT * FROM ${p} WHERE id_rsu = 5").FRONTAL_AREA_INDEX_H0_5_D30_60, 0.00001
    }

    @Test
    void rsuPopulationTest1() {
        h2GIS.execute("""DROP TABLE if exists population_grid, rsu;
        CREATE TABLE population_grid (ID_POP integer, POP float, THE_GEOM GEOMETRY);
        INSERT INTO population_grid VALUES(1, 10, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY);
        CREATE TABLE rsu (ID_rsu integer, THE_GEOM GEOMETRY);
        INSERT INTO rsu VALUES(1, 'POLYGON ((3 6, 6 6, 6 3, 3 3, 3 6))'::GEOMETRY);
        """.toString())
        String process = Geoindicators.RsuIndicators.rsuPopulation(h2GIS, "rsu", "population_grid",
                ["pop"])
        assertNotNull(process)
        assertEquals(10f, (float) h2GIS.firstRow("select pop from ${process}").pop)
    }

    @Test
    void groundLayer() {
        h2GIS.load(SpatialUnitsTests.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("building_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("hydro_test.geojson"), true)
        def zone = h2GIS.load(SpatialUnitsTests.getResource("zone_test.geojson"), true)

        def env = h2GIS.getSpatialTable(zone).getExtent()
        if (env) {
            def gridP = Geoindicators.SpatialUnits.createGrid(h2GIS, env, 100, 100)
            assertNotNull(gridP)
            String ground = Geoindicators.RsuIndicators.groundLayer(h2GIS, gridP, "id_grid",
                    "building_test", "road_test", "hydro_test", "veget_test", "")
            assertTrue((h2GIS.firstRow("select sum(st_area(the_geom)) as area from  building_test where zindex=0".toString()).area - h2GIS.firstRow("select sum(st_area(the_geom)) as area from  $ground where layer = 'building'".toString()).area) < 10)
            assertTrue((h2GIS.firstRow("select sum(st_area(the_geom)) as area from  hydro_test where zindex=0".toString()).area - h2GIS.firstRow("select sum(st_area(the_geom)) as area from  $ground where layer ='water'".toString()).area) < 10)
        }
    }

}

