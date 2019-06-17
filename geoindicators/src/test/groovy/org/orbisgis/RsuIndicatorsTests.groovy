package org.orbisgis

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.geoindicators.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class RsuIndicatorsTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
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
                   buContiguityColumn: "building_contiguity",
                   buTotalFacadeLengthColumn: "building_total_facade_length",
                   prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_free_external_facade_density WHERE id_rsu = 1"){
            row -> concat+= row.rsu_free_external_facade_density
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
        assertTrue p.execute([rsuTable: "rsu_test",correlationBuildingTable: "corr_tempo", rsuBuildingDensityColumn:
                "rsu_building_density", pointDensity: 0.008, rayLength: 100, numberOfDirection: 60, prefixName: "test",
                   datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_ground_sky_view_factor WHERE id_rsu = 8"){
            row -> concat+= row.rsu_ground_sky_view_factor
        }
        assertEquals(0.54, concat, 0.05)
    }

    @Test
    void aspectRatioTest() {
        def  p =  Geoindicators.RsuIndicators.aspectRatio()
        assertTrue p.execute([rsuTable: "rsu_test", rsuFreeExternalFacadeDensityColumn:
                "rsu_free_external_facade_density", rsuBuildingDensityColumn: "rsu_building_density",
                   prefixName: "test", datasource: h2GIS])
        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_aspect_ratio WHERE id_rsu = 1"){
            row -> concat+= row.rsu_aspect_ratio
        }
        assertEquals(1.344, concat, 0.001)
    }

    @Test
    void projectedFacadeAreaDistributionTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS tempo_build; CREATE TABLE tempo_build AS SELECT * " +
                "FROM building_test WHERE id_build < 6"

        def listLayersBottom = [0, 10, 20, 30, 40, 50]
        def numberOfDirection = 4
        def dirMedDeg = 180/numberOfDirection
        def p = Geoindicators.RsuIndicators.projectedFacadeAreaDistribution()
        assertTrue p.execute([buildingTable: "tempo_build", rsuTable: "rsu_test", listLayersBottom: listLayersBottom,
                              numberOfDirection: numberOfDirection, prefixName: "test", datasource: h2GIS])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_rsu_projected_facade_area_distribution WHERE id_rsu = 1"){
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
                        concat1 += row["rsu_non_vert_roof_area${listLayersBottom[listLayersBottom.size() - 1]}_"].round(2) + "\n"
                        concat1 += row["rsu_vert_roof_area${listLayersBottom[listLayersBottom.size() - 1]}_"].round(2) + "\n"
                    }
                    else {
                        concat1+=row["rsu_non_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                        concat1+=row["rsu_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]}"].round(2)+"\n"
                    }
                }
        }
        h2GIS.eachRow("SELECT * FROM test_rsu_roof_area_distribution WHERE id_rsu = 13"){
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
    void effectiveTerrainRoughnessHeightTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, rsu_table; CREATE TABLE tempo_build AS SELECT * " +
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
        def  p =  Geoindicators.RsuIndicators.effectiveTerrainRoughnessHeight()
        assertTrue p.execute([rsuTable: "rsu_table",
                              projectedFacadeAreaName: "rsu_projected_facade_area_distribution",
                              geometricMeanBuildingHeightName: "geom_avg_height_roof",
                              prefixName: "test",
                              listLayersBottom: listLayersBottom,
                              numberOfDirection: numberOfDirection,
                              datasource: h2GIS])

        def concat = 0
        h2GIS.eachRow("SELECT * FROM test_rsu_effective_terrain_roughness WHERE id_rsu = 1"){
            row -> concat += row["rsu_effective_terrain_roughness"].round(2)
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
                               operations: ["rsu_road_direction_distribution", "rsu_linear_road_density"],
                               prefixName: "test",
                               angleRangeSize: 30,
                               levelConsiderated: null,
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

        def p2 =  Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p2.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["rsu_road_direction_distribution"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [0], datasource: h2GIS])
        def t01 = h2GIS.firstRow("SELECT rsu_road_direction_distribution_h0_d0_30 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(20, t01.rsu_road_direction_distribution_h0_d0_30)

        def p3 =  Geoindicators.RsuIndicators.linearRoadOperations()
        assertTrue p3.execute([rsuTable: "rsu_test", roadTable: "road_test", operations: ["rsu_linear_road_density"],
                    prefixName: "test", angleRangeSize: 30, levelConsiderated: [-1], datasource: h2GIS])
        def t001 = h2GIS.firstRow("SELECT rsu_linear_road_density_hminus1 " +
                "FROM test_rsu_road_linear_properties WHERE id_rsu = 14")
        assertEquals(0.00224, t001.rsu_linear_road_density_hminus1.round(5))
    }

    @Test
    void effectiveTerrainRoughnessClassTest() {
        // Only the first 5 first created buildings are selected for the tests
        h2GIS.execute "DROP TABLE IF EXISTS rsu_tempo; CREATE TABLE rsu_tempo AS SELECT *, CASEWHEN(id_rsu = 1, 2.3," +
                "CASEWHEN(id_rsu = 2, 0.1, null)) AS rsu_effective_terrain_roughness_height FROM rsu_test"

        def p =  Geoindicators.RsuIndicators.effectiveTerrainRoughnessClass()
        assertTrue p.execute([datasource: h2GIS, rsuTable: "rsu_tempo", effectiveTerrainRoughnessHeight: "rsu_effective_terrain_roughness_height",
                   prefixName: "test"])
        def concat = ""
        h2GIS.eachRow("SELECT * FROM test_effective_terrain_roughness_class WHERE id_rsu < 4 ORDER BY id_rsu ASC"){
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
        h2GIS.eachRow("SELECT * FROM zero_vegetation_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row -> concat[0]+= "${row.low_vegetation_fraction}\n"
        }
        def  p1 =  Geoindicators.RsuIndicators.vegetationFraction()
        assertTrue p1.execute([rsuTable: "rsu_test", vegetTable: "tempo_veget", fractionType: ["high", "all"], prefixName: "one",
                    datasource: h2GIS])
        h2GIS.eachRow("SELECT * FROM one_vegetation_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
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
        h2GIS.eachRow("SELECT * FROM zero_road_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row -> concat[0]+= "${row.underground_road_fraction.round(5)}\n"
        }
        def  p1 =  Geoindicators.RsuIndicators.roadFraction()
        assertTrue p1.execute([rsuTable: "rsu_test", roadTable: "tempo_road",
                               levelToConsiders: ["underground":[-4, -3, -2, -1], "ground":[0]],
                               prefixName: "one",
                               datasource: h2GIS])
        h2GIS.eachRow("SELECT * FROM one_road_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
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
        h2GIS.eachRow("SELECT * FROM test_water_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
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
                "b.water_fraction, c.ground_road_fraction FROM test_vegetation_fraction a, test_water_fraction b," +
                "test_road_fraction c WHERE a.id_rsu = b.id_rsu AND a.id_rsu = c.id_rsu;")

        def pfin = Geoindicators.RsuIndicators.perviousnessFraction()
        assertTrue pfin.execute([rsuTable: "needed_data",
                                 operationsAndComposition: [
                                         "pervious_fraction" : ["low_vegetation_fraction", "water_fraction"],
                                         "impervious_fraction" : ["ground_road_fraction"]],
                                 prefixName: "test", datasource: h2GIS])
        def concat = ["", ""]
        h2GIS.eachRow("SELECT * FROM test_perviousness_fraction WHERE id_rsu = 14 OR id_rsu = 15"){
            row ->
                concat[0]+= "${row.pervious_fraction}\n"
                concat[1]+= "${row.impervious_fraction.round(5)}\n"
        }
        assertEquals("0.0056\n0.06\n", concat[0])
        assertEquals("0.06161\n0.15866\n", concat[1])
    }
}