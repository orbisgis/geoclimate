package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class RsuIndicatorsTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open('./target/rsuindicatorsdb;AUTO_SERVER=TRUE')
    }

    @BeforeEach
    void initData(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void freeExternalFacadeDensityTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 8"
        // The geometry of the RSU is useful for the calculation, then it is inserted inside the build/rsu correlation table
        h2GIS.execute "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test"

        def  p =  Geoindicators.RsuIndicators.freeExternalFacadeDensity()
        assertTrue p.execute([buildingTable: "tempo_build",
                   rsuTable: "rsu_tempo",
                   buContiguityColumn: "contiguity",
                   buTotalFacadeLengthColumn: "total_facade_length",
                   prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_free_external_facade_density WHERE id_rsu = 1"){
            row -> concat+= row.free_external_facade_density
        }
        assertEquals(0.947, concat)
    }

    @Test
    void groundSkyViewFactorTest() {
        // Only the first 1 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build > 8 AND id_build < 27"
        // The geometry of the buildings are useful for the calculation, then they are inserted inside
        // the build/rsu correlation table
        h2GIS.execute "DROP TABLE IF EXISTS corr_tempo; CREATE TABLE corr_tempo AS SELECT a.*, b.the_geom, b.height_wall " +
                "FROM rsu_build_corr a, tempo_build b WHERE a.id_build = b.id_build"

        def  p =  Geoindicators.RsuIndicators.groundSkyViewFactor()
        assertTrue p.execute([rsuTable: "rsu_test",correlationBuildingTable: "corr_tempo", pointDensity: 0.008,
                              rayLength: 100, numberOfDirection: 60, prefixName: "test",
                   datasource: h2GIS])
        assertEquals(0.54, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 8")["ground_sky_view_factor"], 0.05)
        assertEquals(1, h2GIS.firstRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 1")["ground_sky_view_factor"])
    }

    @Test
    void aspectRatioTest() {
        def  p =  Geoindicators.RsuIndicators.aspectRatio()
        assertTrue p.execute([rsuTable: "rsu_test", rsuFreeExternalFacadeDensityColumn:
                "rsu_free_external_facade_density", rsuBuildingDensityColumn: "rsu_building_density",
                   prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_aspect_ratio WHERE id_rsu = 1"){
            row -> concat+= row.aspect_ratio
        }
        assertEquals(1.344, concat, 0.001)
    }

    @Test
    void projectedFacadeAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, test_rsu_projected_facade_area_distribution;" +
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
    void roofAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, test_rsu_roof_area_distribution; " +
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
        def NV1 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1")["NON_VERT_ROOF_DENSITY"]
        def V1 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 1")["VERT_ROOF_DENSITY"]
        def NV2 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13")["NON_VERT_ROOF_DENSITY"]
        def V2 = h2GIS.firstRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13")["VERT_ROOF_DENSITY"]
        assertEquals(796.64/2000, NV1+V1, 0.001)
        assertEquals(1600.27/10000, NV2+V2, 0.001)
    }

    @Test
    void effectiveTerrainRoughnesslengthTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, rsu_table, BUILDING_INTERSECTION, BUILDING_INTERSECTION_EXPL, BUILDINGFREE, BUILDINGLAYER; CREATE TABLE tempo_build AS SELECT * " +
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
        def  pGeomAvg =  Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
        assertTrue pGeomAvg.execute([inputLowerScaleTableName: "tempo_build",
                                     inputUpperScaleTableName: "rsu_build_corr",
                                     inputIdUp: "id_rsu",
                                     inputIdLow: "id_build",
                                     inputVarAndOperations: ["height_roof": ["GEOM_AVG"]],
                                     prefixName: "test",
                                     datasource: h2GIS])

        // Add the geometry field in the previous resulting Tables
        h2GIS.execute "ALTER TABLE test_unweighted_operation_from_lower_scale add column the_geom GEOMETRY;" +
                "UPDATE test_unweighted_operation_from_lower_scale set the_geom = (SELECT a.the_geom FROM " +
                "rsu_test a WHERE a.id_rsu = test_unweighted_operation_from_lower_scale.id_rsu);"

        h2GIS.execute "CREATE TABLE rsu_table AS SELECT a.*, b.geom_avg_height_roof, b.the_geom " +
                "FROM test_rsu_projected_facade_area_distribution a, test_unweighted_operation_from_lower_scale b " +
                "WHERE a.id_rsu = b.id_rsu"
        def  p =  Geoindicators.RsuIndicators.effectiveTerrainRoughnessLength()
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
        h2GIS.execute "DROP TABLE IF EXISTS road_tempo; CREATE TABLE road_tempo AS SELECT * " +
                "FROM road_test WHERE id_road < 7"

        def p1 =  Geoindicators.RsuIndicators.linearRoadOperations()
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

        def p2 =  Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p2.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["road_direction_distribution"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [0], datasource: h2GIS])
        def t01 = h2GIS.firstRow("SELECT road_direction_distribution_h0_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(20, t01.road_direction_distribution_h0_d0_30)

        def p3 =  Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p3.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["linear_road_density"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [-1], datasource: h2GIS])
        def t001 = h2GIS.firstRow("SELECT linear_road_density_hminus1 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(0.00224, t001.linear_road_density_hminus1.round(5))
    }

    @Test
    void effectiveTerrainRoughnessClassTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT *, CASEWHEN(id_rsu = 1, 2.3," +
                "CASEWHEN(id_rsu = 2, 0.1, null)) AS effective_terrain_roughness_length FROM rsu_test"

        def p =  Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass()
        assertTrue p.execute([datasource: h2GIS, rsuTable: "rsu_tempo", effectiveTerrainRoughnessLength: "effective_terrain_roughness_length",
                   prefixName: "test"])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness_class WHERE id_rsu < 4 ORDER BY id_rsu ASC"){
            row -> concat += "${row["effective_terrain_roughness_class"]}\n".toString()
        }
        assertEquals("8\n4\nnull\n", concat)
    }

    @Test
    void vegetationFractionTest() {
        // Only the first 4 first created vegetation areas are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_veget; CREATE TABLE tempo_veget AS SELECT * " +
                "FROM veget_test WHERE id_veget < 4"

        def  p0 =  Geoindicators.RsuIndicators.vegetationFraction()
        assertTrue p0.execute([rsuTable: "rsu_test", vegetTable: "tempo_veget", fractionType: ["low"],
                               prefixName: "zero", datasource: h2GIS])
        def concat = ["",""]
        h2GIS.eachRow("SELECT * FROM zero_rsu_vegetation_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row -> concat[0]+= "${row.low_vegetation_fraction}\n"
        }
        def  p1 =  Geoindicators.RsuIndicators.vegetationFraction()
        assertTrue p1.execute([rsuTable: "rsu_test", vegetTable: "tempo_veget", fractionType: ["high", "all"], prefixName: "one",
                    datasource: h2GIS])
        h2GIS.eachRow("SELECT * FROM one_rsu_vegetation_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row ->
                concat[1]+= "${row.high_vegetation_fraction}\n"
                concat[1]+= "${row.all_vegetation_fraction}\n"
        }
        assertEquals("0.0016\n0.02\n", concat[0])
        assertEquals("0.02\n0.0216\n0.0\n0.02\n", concat[1])
    }

    @Test
    void roadFractionTest() {
        // Only the first 4 first created vegetation areas are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_road; CREATE TABLE tempo_road AS SELECT * " +
                "FROM road_test WHERE id_road < 7"

        def  p0 =  Geoindicators.RsuIndicators.roadFraction()
        assertTrue p0.execute([rsuTable: "rsu_test", roadTable: "tempo_road", levelToConsiders: ["underground":[-4, -3, -2, -1]],
                    prefixName: "zero",
                    datasource: h2GIS])
        def concat = ["",""]
        h2GIS.eachRow("SELECT * FROM zero_rsu_road_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row -> concat[0]+= "${row.underground_road_fraction.round(5)}\n"
        }
        def  p1 =  Geoindicators.RsuIndicators.roadFraction()
        assertTrue p1.execute([rsuTable: "rsu_test", roadTable: "tempo_road",
                               levelToConsiders: ["underground":[-4, -3, -2, -1], "ground":[0]],
                               prefixName: "one",
                               datasource: h2GIS])
        h2GIS.eachRow("SELECT * FROM one_rsu_road_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row ->
                concat[1]+= "${row.underground_road_fraction.round(5)}\n"
                concat[1]+= "${row.ground_road_fraction.round(5)}\n"
        }
        assertEquals("0.01005\n0.08\n", concat[0])
        assertEquals("0.01005\n0.06161\n0.08\n0.15866\n", concat[1])
    }

    @Test
    void waterFractionTest() {
        // Only the first 4 first created vegetation areas are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_hydro; CREATE TABLE tempo_hydro AS SELECT * " +
                "FROM hydro_test WHERE id_hydro < 2"

        def  p =  Geoindicators.RsuIndicators.waterFraction()
        assertTrue p.execute([rsuTable: "rsu_test", waterTable: "tempo_hydro", prefixName: "test",
                   datasource: h2GIS])
        def concat = [""]
        h2GIS.eachRow("SELECT * FROM test_rsu_water_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row -> concat[0]+= "${row.water_fraction}\n"
        }
        assertEquals("0.004\n0.04\n", concat[0])
    }

    @Test
    void perviousnessFractionTest() {
        // Only the first created vegetation, road and water areas are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_hydro, tempo_veget, tempo_road; CREATE TABLE tempo_hydro AS SELECT * " +
                "FROM hydro_test WHERE id_hydro < 2; CREATE TABLE tempo_veget AS SELECT * " +
                "FROM veget_test WHERE id_veget < 4; CREATE TABLE tempo_road AS SELECT * " +
                "FROM road_test WHERE id_road < 7"

        // The corresponding fractions are calculated
        def  pveg =  Geoindicators.RsuIndicators.vegetationFraction()
        assertTrue pveg.execute([rsuTable: "rsu_test", vegetTable: "tempo_veget", fractionType: ["low"],
                                 prefixName: "test", datasource: h2GIS])
        def  pwat =  Geoindicators.RsuIndicators.waterFraction()
        assertTrue pwat.execute([rsuTable: "rsu_test", waterTable: "tempo_hydro", prefixName: "test",
                                 datasource: h2GIS])
        def  proad =  Geoindicators.RsuIndicators.roadFraction()
        assertTrue proad.execute([rsuTable: "rsu_test", roadTable: "tempo_road",
                                  levelToConsiders: ["underground":[-4, -3, -2, -1], "ground":[0]],
                                  prefixName: "test", datasource: h2GIS])

        // The data useful for pervious fraction calculation are gathered in a same Table
        h2GIS.execute("DROP TABLE IF EXISTS needed_data; CREATE TABLE needed_data AS SELECT a.*, " +
                "b.water_fraction, c.ground_road_fraction FROM test_rsu_vegetation_fraction a, test_rsu_water_fraction b," +
                "test_rsu_road_fraction c WHERE a.id_rsu = b.id_rsu AND a.id_rsu = c.id_rsu;")

        def pfin = Geoindicators.RsuIndicators.perviousnessFraction()
        assertTrue pfin.execute([rsuTable: "needed_data",
                                 operationsAndComposition: [
                                         "pervious_fraction" : ["low_vegetation_fraction", "water_fraction"],
                                         "impervious_fraction" : ["ground_road_fraction"]],
                                 prefixName: "test", datasource: h2GIS])
        def concat = ["", ""]
        h2GIS.eachRow("SELECT * FROM test_rsu_perviousness_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row ->
                concat[0]+= "${row.pervious_fraction}\n"
                concat[1]+= "${row.impervious_fraction.round(5)}\n"
        }
        assertEquals("0.0056\n0.06\n", concat[0])
        assertEquals("0.06161\n0.15866\n", concat[1])
    }

    @Test
    void extendedFreeFacadeFractionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build, rsu_free_external_facade_density; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6 OR id_build = 35"
        // The geometry of the RSU is useful for the calculation, then it is inserted inside the build/rsu correlation table
        h2GIS.execute "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test WHERE id_rsu = 1"

        def  p =  Geoindicators.RsuIndicators.extendedFreeFacadeFraction()
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

        def  prepareData = Geoindicators.SpatialUnits.prepareRSUData()
        assertTrue prepareData.execute([zoneTable: 'zone_test', roadTable: 'road_test',  railTable: '',
                                        vegetationTable : 'veget_test',
                                        hydrographicTable :'hydro_test',
                                        prefixName: "prepare_rsu", datasource: h2GIS])

        def outputTableGeoms = prepareData.results.outputTableName

        assertNotNull h2GIS.getTable(outputTableGeoms)

        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName

        def  p =  Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                              rsuTable: outputTable,buildingTable: "building_test", roadTable:"road_test",vegetationTable: "veget_test",waterTable: "hydro_test",
                              prefixName: "test", datasource: h2GIS])
        def outputTableStats = p.results.outputTableName

        h2GIS.execute """DROP TABLE IF EXISTS stats_rsu;
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
        h2GIS.execute """DROP TABLE IF EXISTS stats_building;
                          CREATE TABLE stats_building as select sum(st_area(the_geom)) as building_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from building_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.zindex=0 ) group by id_rsu;
                           CREATE INDEX ON stats_building(id_rsu);                           
                           DROP TABLE IF EXISTS building_compare_stats;
                           CREATE TABLE building_compare_stats as select (a.building_sum- b.building_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_building as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from building_compare_stats where diff > 1").count==0


        //Check the sum of low vegetation areas by USR
        h2GIS.execute """DROP TABLE IF EXISTS stats_vegetation_low;
                          CREATE TABLE stats_vegetation_low as select sum(st_area(the_geom)) as vegetation_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from veget_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.type='low' ) group by id_rsu;
                           CREATE INDEX ON stats_vegetation_low(id_rsu);                           
                           DROP TABLE IF EXISTS vegetation_low_compare_stats;
                           CREATE TABLE vegetation_low_compare_stats as select (a.low_VEGETATION_sum- b.vegetation_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_vegetation_low as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from vegetation_low_compare_stats where diff > 1").count==0

        //Check the sum of high vegetation areas by USR
        h2GIS.execute """DROP TABLE IF EXISTS stats_vegetation_high;
                          CREATE TABLE stats_vegetation_high as select sum(st_area(the_geom)) as vegetation_areas, id_rsu from 
                            (select st_intersection(ST_force2d(a.the_geom), b.the_geom) as the_geom, b.id_rsu from veget_test as a,
                            $outputTable as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) and a.type='high' ) group by id_rsu;
                           CREATE INDEX ON stats_vegetation_high(id_rsu);                           
                           DROP TABLE IF EXISTS vegetation_high_compare_stats;
                           CREATE TABLE vegetation_high_compare_stats as select (a.high_VEGETATION_sum- b.vegetation_areas) as diff, a.id_rsu from stats_rsu as a,
                           stats_vegetation_high as b where a.id_rsu=b.id_rsu;"""

        assertTrue h2GIS.firstRow("select count(*) as count from vegetation_high_compare_stats where diff > 1").count==0

        //Check the sum of water areas by USR
        h2GIS.execute """DROP TABLE IF EXISTS stats_water;
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
        // Only the RSU 5 is conserved for the test
        h2GIS.execute "DROP TABLE IF EXISTS rsu_tempo;" +
                "CREATE TABLE rsu_tempo AS SELECT * " +
                "FROM rsu_test WHERE id_rsu = 5"

        // Need to create the smallest geometries used as input of the surface fraction process
        def  p =  Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                rsuTable: "rsu_tempo",buildingTable: "building_test",vegetationTable: "veget_test",waterTable: "hydro_test",
                prefixName: "test", datasource: h2GIS])
        def tempoTable = p.results.outputTableName

        // Apply the surface fractions for different combinations
        // combination 1
        def  p0 =  Geoindicators.RsuIndicators.surfaceFractions()
        def superpositionsWithPriorities0 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]]
        def priorities0 = ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"]
        assertTrue p0.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositionsWithPriorities: superpositionsWithPriorities0,
                priorities: priorities0,
                prefixName: "test", datasource: h2GIS])
        def result0 = h2GIS.firstRow("SELECT * FROM ${p0.results.outputTableName}")
        assertEquals(1.0/5, result0["high_vegetation_building_fraction"])
        assertEquals(3.0/20, result0["high_vegetation_high_vegetation_fraction"])
        assertEquals(3.0/20, result0["high_vegetation_fraction"])
        assertEquals(3.0/20, result0["low_vegetation_fraction"])
        assertEquals(1.0/4, result0["water_fraction"])
        assertEquals(1.0/10, result0["building_fraction"])

        // combination 2
        def  p1 =  Geoindicators.RsuIndicators.surfaceFractions()
        def superpositionsWithPriorities1 = ["high_vegetation": ["building", "water", "low_vegetation", "road", "impervious"]]
        def priorities1 = ["building", "water", "high_vegetation", "low_vegetation", "road", "impervious"]
        assertTrue p1.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositionsWithPriorities: superpositionsWithPriorities1,
                priorities: priorities1,
                prefixName: "test", datasource: h2GIS])
        def result1 = h2GIS.firstRow("SELECT * FROM ${p1.results.outputTableName}")
        assertEquals(1.0/5, result1["high_vegetation_building_fraction"])
        assertEquals(3.0/20, result1["high_vegetation_high_vegetation_fraction"])
        assertEquals(3.0/20, result1["high_vegetation_fraction"])
        assertEquals(3.0/20, result1["low_vegetation_fraction"])
        assertEquals(3.0/20, result1["water_fraction"])
        assertEquals(1.0/5, result1["building_fraction"])

        // combination 3
        def  p2 =  Geoindicators.RsuIndicators.surfaceFractions()
        def superpositionsWithPriorities2 = ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"],
                "building": ["low_vegetation"]]
        assertTrue p2.execute([
                rsuTable: "rsu_tempo", spatialRelationsTable: tempoTable,
                superpositionsWithPriorities: superpositionsWithPriorities2,
                priorities: priorities0,
                prefixName: "test", datasource: h2GIS])
        def result2 = h2GIS.firstRow("SELECT * FROM ${p2.results.outputTableName}")
        assertEquals(1.0/5, result2["high_vegetation_building_fraction"])
        assertEquals(3.0/20, result2["high_vegetation_high_vegetation_fraction"])
        assertEquals(3.0/20, result2["high_vegetation_fraction"])
        assertEquals(1.0/10, result2["building_low_vegetation_fraction"])
        assertEquals(3.0/20, result2["low_vegetation_fraction"])
        assertEquals(1.0/4, result2["water_fraction"])
        assertEquals(0, result2["building_fraction"])
    }
}