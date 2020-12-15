package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open
import static org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager.load

class RsuIndicatorsTests {

    private static def h2GIS
    private static def randomDbName() {"${RsuIndicatorsTests.simpleName}_${UUID.randomUUID().toString().replaceAll"-", "_"}"}

    @BeforeAll
    static void beforeAll(){
        h2GIS = open"./target/${randomDbName()};AUTO_SERVER=TRUE"
    }

    @BeforeEach
    void beforeEach(){
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

        def p = Geoindicators.RsuIndicators.freeExternalFacadeDensity()
        assert p([
                buildingTable               : "tempo_build",
                rsuTable                    : "rsu_tempo",
                buContiguityColumn          : "contiguity",
                buTotalFacadeLengthColumn   : "total_facade_length",
                prefixName                  : "test",
                datasource                  : h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_free_external_facade_density WHERE id_rsu = 1"){
            row -> concat+= row.free_external_facade_density
        }
        assert 0.947 == concat
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

        def p = Geoindicators.RsuIndicators.groundSkyViewFactor()
        assertTrue p.execute([
                rsuTable                    : "rsu_test",
                correlationBuildingTable    : "corr_tempo",
                pointDensity                : 0.008,
                rayLength                   : 100,
                numberOfDirection           : 60,
                prefixName                  : "test",
                datasource                  : h2GIS])
        assertEquals 0.54, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor " +
                "WHERE id_rsu = 8").ground_sky_view_factor, 0.05
        // For RSU having no buildings in and around them
        assertEquals 1, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 1").ground_sky_view_factor
        // For buildings that are RSU...
        assertEquals 0.5, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 18").ground_sky_view_factor, 0.03
    }

    @Test
    void aspectRatioTest() {
        def p = Geoindicators.RsuIndicators.aspectRatio()
        assertTrue p.execute([rsuTable: "rsu_test", rsuFreeExternalFacadeDensityColumn:
                "rsu_free_external_facade_density", rsuBuildingDensityColumn: "rsu_building_density",
                   prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_aspect_ratio WHERE id_rsu = 1"){
            row -> concat+= row.aspect_ratio
        }
        assertEquals(0.672, concat, 0.001)
    }

    @Test
    void aspectRatioTest2() {
        def p = Geoindicators.RsuIndicators.aspectRatio()
        assertTrue p.execute([rsuTable: "rsu_test", rsuFreeExternalFacadeDensityColumn:
                "rsu_free_external_facade_density", rsuBuildingDensityColumn: "rsu_building_density",
                              prefixName: "test", datasource: h2GIS])
        def result = h2GIS.firstRow("SELECT aspect_ratio FROM test_rsu_aspect_ratio WHERE id_rsu = 17")
        assertEquals(null, result["aspect_ratio"])
    }

    @Test
    void projectedFacadeAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, test_rsu_projected_facade_area_distribution;" +
                " CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 6"

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def rangeDeg = 360/numberOfDirection
        def p = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
        assertTrue p.execute([buildingTable: "tempo_build", rsuTable: "rsu_test", listLayersBottom: listLayersBottom,
                              numberOfDirection: numberOfDirection, prefixName: "test", datasource: h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_projected_facade_area_distribution WHERE id_rsu = 1"){
            row ->
                // Iterate over columns
                def names = []
                for (i in 1..listLayersBottom.size()){
                    names[i-1]="projected_facade_area_distribution_H${listLayersBottom[i-1]}"+
                            "_${listLayersBottom[i]}"
                    if (i == listLayersBottom.size()){
                        names[listLayersBottom.size()-1]="projected_facade_area_distribution"+
                                "_H${listLayersBottom[listLayersBottom.size()-1]}"
                    }
                    for (int d=0; d<numberOfDirection/2; d++){
                        int dirDeg = d*360/numberOfDirection
                        concat+= row["${names[i-1]}_D${dirDeg}_${dirDeg+rangeDeg}".toString()].round(2).toString()+"\n"
                    }
                }

        }
        assertEquals("637.1\n637.1\n32.53\n32.53\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n", concat)
    }

    @Test
    void projectedFacadeAreaDistributionTest2() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, test_rsu_projected_facade_area_distribution;" +
                " CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 1"

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def rangeDeg = 360/numberOfDirection
        def p = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
        assertTrue p.execute([buildingTable: "tempo_build", rsuTable: "rsu_test", listLayersBottom: listLayersBottom,
                              numberOfDirection: numberOfDirection, prefixName: "test", datasource: h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_projected_facade_area_distribution WHERE id_rsu = 1"){
            row ->
                // Iterate over columns
                def names = []
                for (i in 1..listLayersBottom.size()){
                    names[i-1]="projected_facade_area_distribution_H${listLayersBottom[i-1]}"+
                            "_${listLayersBottom[i]}"
                    if (i == listLayersBottom.size()){
                        names[listLayersBottom.size()-1]="projected_facade_area_distribution"+
                                "_H${listLayersBottom[listLayersBottom.size()-1]}"
                    }
                    for (int d=0; d<numberOfDirection/2; d++){
                        int dirDeg = d*360/numberOfDirection
                        concat+= row["${names[i-1]}_D${dirDeg}_${dirDeg+rangeDeg}".toString()].round(2).toString()+"\n"
                    }
                }
        }
        assertEquals("0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n", concat)
    }

    @Test
    void roofAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS tempo_build, test_rsu_roof_area_distribution; " +
                "CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6 OR " +
                "id_build < 29 AND id_build > 26"

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def p = Geoindicators.RsuIndicators.roofAreaDistribution()
        assertTrue p.execute([rsuTable: "rsu_test", buildingTable: "tempo_build",
                   listLayersBottom: listLayersBottom, prefixName: "test",
                   datasource: h2GIS])
        def concat1 = ""
        def concat2 = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1"){
            row ->
                // Iterate over columns
                for (i in 1..listLayersBottom.size()){
                    if (i == listLayersBottom.size()) {
                        concat1 += row["non_vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                        concat1 += row["vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                    }
                    else {
                        concat1+=row["non_vert_roof_area_H${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                        concat1+=row["vert_roof_area_H${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                    }
                }
        }
        h2GIS.eachRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13"){
            row ->
                // Iterate over columns
                for (i in 1..listLayersBottom.size()){
                    if (i == listLayersBottom.size()) {
                        concat2 += row["non_vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                        concat2 += row["vert_roof_area_H${listLayersBottom[listLayersBottom.size() - 1]}"].round(2) + "\n"
                    }
                    else {
                        concat2+=row["non_vert_roof_area_H${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                        concat2+=row["vert_roof_area_H${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                    }
                }
        }
        assertEquals("405.25\n56.48\n289.27\n45.64\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n0.0\n",
                concat1)
        assertEquals("355.02\n163.23\n404.01\n141.88\n244.92\n235.5\n48.98\n6.73\n0.0\n0.0\n0.0\n0.0\n",
                concat2)

        // Test the optionally calculated roof densities
        def NV1 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1").NON_VERT_ROOF_DENSITY
        def V1 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1").VERT_ROOF_DENSITY
        def NV2 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13").NON_VERT_ROOF_DENSITY
        def V2 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13").VERT_ROOF_DENSITY
        assertEquals(796.64/2000, NV1+V1, 0.001)
        assertEquals(1600.27/10000, NV2+V2, 0.001)
    }

    @Test
    void effectiveTerrainRoughnesslengthTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS("DROP TABLE IF EXISTS tempo_build, rsu_table, BUILDING_INTERSECTION, BUILDING_INTERSECTION_EXPL, BUILDINGFREE, BUILDINGLAYER; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6")

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def pFacadeDistrib = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
        assertTrue pFacadeDistrib.execute([buildingTable: "tempo_build",
                                           rsuTable: "rsu_test",
                                           listLayersBottom: listLayersBottom,
                                           numberOfDirection: numberOfDirection,
                                           prefixName: "test",
                                           datasource: h2GIS])
        def pGeomAvg = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assertTrue pGeomAvg.execute([inputLowerScaleTableName: "tempo_build",
                                     inputUpperScaleTableName: "rsu_build_corr",
                                     inputIdUp: "id_rsu",
                                     inputIdLow: "id_build",
                                     inputVarAndOperations: ["height_roof": ["GEOM_AVG"]],
                                     prefixName: "test",
                                     datasource: h2GIS])

        // Add the geometry field in the previous resulting Tables
        h2GIS "ALTER TABLE test_unweighted_operation_from_lower_scale add column the_geom GEOMETRY;" +
                "UPDATE test_unweighted_operation_from_lower_scale set the_geom = (SELECT a.the_geom FROM " +
                "rsu_test a WHERE a.id_rsu = test_unweighted_operation_from_lower_scale.id_rsu);"

        h2GIS "CREATE TABLE rsu_table AS SELECT a.*, b.geom_avg_height_roof, b.the_geom " +
                "FROM test_rsu_projected_facade_area_distribution a, test_unweighted_operation_from_lower_scale b " +
                "WHERE a.id_rsu = b.id_rsu"
        def p = Geoindicators.RsuIndicators.effectiveTerrainRoughnessLength()
        assertTrue p.execute([rsuTable: "rsu_table",
                              projectedFacadeAreaName: "projected_facade_area_distribution",
                              geometricMeanBuildingHeightName: "geom_avg_height_roof",
                              prefixName: "test",
                              listLayersBottom: listLayersBottom,
                              numberOfDirection: numberOfDirection,
                              datasource: h2GIS])

        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness_length WHERE id_rsu = 1"){
            row -> concat += row["effective_terrain_roughness_length"].round(2)
        }
        assertEquals(1.6, concat)
    }

    @Test
    void linearRoadOperationsTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS road_tempo; CREATE TABLE road_tempo AS SELECT * " +
                "FROM road_test WHERE id_road < 7"

        def p1 = Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p1.execute([rsuTable: "rsu_test",
                               roadTable: "road_test",
                               operations: ["road_direction_distribution", "linear_road_density"],
                               prefixName: "test",
                               angleRangeSize: 30,
                               levelConsiderated: null,
                               datasource: h2GIS])
        def t0 = h2GIS.firstRow("SELECT road_direction_distribution_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        def t1 = h2GIS.firstRow("SELECT road_direction_distribution_d90_120 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        def t2 = h2GIS.firstRow("SELECT linear_road_density " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(25.59, t0.road_direction_distribution_d0_30.round(2))
        assertEquals(10.0, t1.road_direction_distribution_d90_120)
        assertEquals(0.0142, t2.linear_road_density.round(4))

        def p2 = Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p2.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["road_direction_distribution"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [0], datasource: h2GIS])
        def t01 = h2GIS.firstRow("SELECT road_direction_distribution_h0_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(20, t01.road_direction_distribution_h0_d0_30)

        def p3 = Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p3.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["linear_road_density"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [-1], datasource: h2GIS])
        def t001 = h2GIS.firstRow("SELECT linear_road_density_hminus1 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(0.00224, t001.linear_road_density_hminus1.round(5))
    }

    @Test
    void effectiveTerrainRoughnessClassTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT *, CASEWHEN(id_rsu = 1, 2.3," +
                "CASEWHEN(id_rsu = 2, 0.1, null)) AS effective_terrain_roughness_length FROM rsu_test"

        def p = Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass()
        assertTrue p.execute([datasource: h2GIS, rsuTable: "rsu_tempo", effectiveTerrainRoughnessLength: "effective_terrain_roughness_length",
                   prefixName: "test"])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness_class WHERE id_rsu < 4 ORDER BY id_rsu ASC"){
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

        def p = Geoindicators.RsuIndicators.extendedFreeFacadeFraction()
        assertTrue p.execute([buildingTable: "tempo_build",
                              rsuTable: "rsu_tempo",
                              buContiguityColumn: "contiguity",
                              buTotalFacadeLengthColumn: "total_facade_length",
                              prefixName: "test", buffDist : 30, datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_extended_free_facade_fraction WHERE id_rsu = 1"){
            row -> concat+= row.extended_free_facade_fraction.round(3)
        }
        assertEquals(0.177, concat)
    }


    @Test
    void smallestCommunGeometryTest() {
        h2GIS.load(SpatialUnitsTests.class.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("building_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("zone_test.geojson"),true)

        def prepareData = Geoindicators.SpatialUnits.prepareRSUData()
        assertTrue prepareData.execute([zoneTable: 'zone_test', roadTable: 'road_test',  railTable: '',
                                        vegetationTable : 'veget_test',
                                        hydrographicTable :'hydro_test',
                                        prefixName: "prepare_rsu", datasource: h2GIS])

        def outputTableGeoms = prepareData.results.outputTableName

        assertNotNull h2GIS.getTable(outputTableGeoms)

        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName

        def p = Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                              rsuTable: outputTable,buildingTable: "building_test", roadTable:"road_test",vegetationTable: "veget_test",waterTable: "hydro_test",
                              prefixName: "test", datasource: h2GIS])
        def outputTableStats = p.results.outputTableName

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

        assertTrue h2GIS.firstRow("select count(*) as count from building_compare_stats where diff > 1").count==0


        //Check the sum of low vegetation areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_vegetation_low;
                          CREATE TABLE stats_vegetation_low as select sum(st_area(the_geom)) as vegetation_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from veget_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.type='low' ) group by id_rsu;
                           CREATE INDEX ON stats_vegetation_low(id_rsu);                           
                           DROP TABLE IF EXISTS vegetation_low_compare_stats;
                           CREATE TABLE vegetation_low_compare_stats as select (a.low_VEGETATION_sum- b.vegetation_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_vegetation_low as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from vegetation_low_compare_stats where diff > 1").count==0

        //Check the sum of high vegetation areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_vegetation_high;
                          CREATE TABLE stats_vegetation_high as select sum(st_area(the_geom)) as vegetation_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from veget_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.type='high' ) group by id_rsu;
                           CREATE INDEX ON stats_vegetation_high(id_rsu);                           
                           DROP TABLE IF EXISTS vegetation_high_compare_stats;
                           CREATE TABLE vegetation_high_compare_stats as select (a.high_VEGETATION_sum- b.vegetation_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_vegetation_high as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from vegetation_high_compare_stats where diff > 1").count==0

        //Check the sum of water areas by USR
        h2GIS """DROP TABLE IF EXISTS stats_water;
                          CREATE TABLE stats_water as select sum(st_area(the_geom)) as water_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from hydro_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.zindex=0 ) group by id_rsu;
                           CREATE INDEX ON stats_water(id_rsu);                           
                           DROP TABLE IF EXISTS water_compare_stats;
                           CREATE TABLE water_compare_stats as select (a.water_sum- b.water_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_water as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from water_compare_stats where diff > 1").count==0

    }

    @Test
    void surfaceFractionTest() {
        // Only the RSU 4 is conserved for the test
        h2GIS "DROP TABLE IF EXISTS rsu_tempo;" +
                "CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test WHERE id_rsu = 4"

        // Need to create the smallest geometries used as input of the surface fraction process
        def p = Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                rsuTable: "rsu_tempo",buildingTable: "building_test",vegetationTable: "veget_test",waterTable: "hydro_test",
                prefixName: "test", datasource: h2GIS])
        def tempoTable = p.results.outputTableName

        // Apply the surface fractions for different combinations
        // combination 1
        def p0 = Geoindicators.RsuIndicators.surfaceFractions()
        def superpositions0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        assertTrue p0.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositions: superpositions0,
                priorities: priorities0,
                prefixName: "test", datasource: h2GIS])
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0.results.outputTableName}")
        assertEquals(1.0/5, result0["high_vegetation_building_fraction"])
        assertEquals(3.0/20, result0["high_vegetation_low_vegetation_fraction"])
        assertEquals(3.0/20, result0["high_vegetation_fraction"])
        assertEquals(3.0/20, result0["low_vegetation_fraction"])
        assertEquals(1.0/4, result0["water_fraction"])
        assertEquals(1.0/10, result0["building_fraction"])

        // combination 2
        def p1 = Geoindicators.RsuIndicators.surfaceFractions()
        def superpositions1 = ["high_vegetation": ["building", "water", "low_vegetation", "road", "impervious"]]
        def priorities1 = ["building", "water", "high_vegetation", "low_vegetation", "road", "impervious"]
        assertTrue p1.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositions: superpositions1,
                priorities: priorities1,
                prefixName: "test", datasource: h2GIS])
        def result1 = h2GIS.firstRow("SELECT * FROM ${p1.results.outputTableName}")
        assertEquals(1.0/5, result1["high_vegetation_building_fraction"])
        assertEquals(3.0/20, result1["high_vegetation_low_vegetation_fraction"])
        assertEquals(3.0/20, result1["high_vegetation_fraction"])
        assertEquals(3.0/20, result1["low_vegetation_fraction"])
        assertEquals(3.0/20, result1["water_fraction"])
        assertEquals(1.0/5, result1["building_fraction"])

        // combination 3
        def p2 = Geoindicators.RsuIndicators.surfaceFractions()
        def superpositions2 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"],
                "building": ["low_vegetation"]]
        assertTrue p2.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositions: superpositions2,
                priorities: priorities0,
                prefixName: "test", datasource: h2GIS])
        def result2 = h2GIS.firstRow("SELECT * FROM ${p2.results.outputTableName}")
        assertEquals(1.0/5, result2["high_vegetation_building_fraction"])
        assertEquals(3.0/20, result2["high_vegetation_low_vegetation_fraction"])
        assertEquals(3.0/20, result2["high_vegetation_fraction"])
        assertEquals(1.0/10, result2["building_low_vegetation_fraction"])
        assertEquals(3.0/20, result2["low_vegetation_fraction"])
        assertEquals(1.0/4, result2["water_fraction"])
        assertEquals(0, result2["building_fraction"])
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
        def p = Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                rsuTable: "rsu_tempo", roadTable: "road_tempo",
                prefixName: "test", datasource: h2GIS])
        def tempoTable = p.results.outputTableName

        // Apply the surface fractions for different combinations
        // combination 1
        def p0 = Geoindicators.RsuIndicators.surfaceFractions()
        def superpositions0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        assertTrue p0.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositions: superpositions0,
                priorities: priorities0,
                prefixName: "test", datasource: h2GIS])
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0.results.outputTableName}")
        assertEquals(5.0/100, result0["road_fraction"])
    }

    @Test
    void surfaceFractionTest4() {
        // Test whether the rsu having no surface fraction is not set to null
        h2GIS """DROP TABLE IF EXISTS rsu_tempo, smallest_geom;
                            CREATE TABLE rsu_tempo(id_rsu int, the_geom geometry);
                            INSERT INTO rsu_tempo VALUES  (1, 'POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY);
                            CREATE TABLE smallest_geom(area double, low_vegetation boolean, high_vegetation boolean,
                                water boolean, impervious boolean, road boolean, building boolean, id_rsu integer);
                                INSERT INTO smallest_geom VALUES (923, 0, 1, 0, 0, 0, 0, 2)"""

        
        // Apply the surface fractions for different combinations
        // combination 1
        def p0 = Geoindicators.RsuIndicators.surfaceFractions()
        def superpositions0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        assertTrue p0.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: "smallest_geom",
                superpositions: superpositions0,
                priorities: priorities0,
                prefixName: "test", datasource: h2GIS])
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0.results.outputTableName} WHERE ID_RSU=1")
        assertEquals(0.0, result0["building_fraction"])
    }
}
