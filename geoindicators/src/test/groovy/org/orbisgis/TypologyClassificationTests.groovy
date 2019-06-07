package org.orbisgis

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class TypologyClassificationTests {

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
    void identifyLczTypeTest() {
        h2GIS.execute """
                DROP TABLE IF EXISTS tempo_rsu_for_lcz;
                CREATE TABLE tempo_rsu_for_lcz AS SELECT * FROM rsu_test_for_lcz;
        """
        def  pavg =  Geoclimate.TypologyClassification.identifyLczType()
        assertTrue pavg.execute([rsuLczIndicators: "tempo_rsu_for_lcz", normalisationType: "AVG",
                   mapOfWeights: ["sky_view_factor": 1,
                                  "aspect_ratio": 1, "building_surface_fraction": 1, "impervious_surface_fraction": 1,
                                  "pervious_surface_fraction": 1, "height_of_roughness_elements": 1,
                                  "terrain_roughness_class": 1],
                   prefixName: "test", datasource: h2GIS])

        h2GIS.getTable(pavg.results.outputTableName).eachRow {
            row ->
                if (row.id_rsu == 1) {
                    assertEquals("LCZ1", row.LCZ1,)
                    assertEquals(0, row.min_distance)
                } else {
                    assertEquals("LCZ8", row.LCZ1)
                    assertTrue(row.min_distance > 0)
                    assertTrue(row.PSS < 1)
                }
        }
        def  pmed =  Geoclimate.TypologyClassification.identifyLczType()
        assertTrue pmed.execute([rsuLczIndicators: "tempo_rsu_for_lcz", normalisationType: "MEDIAN",
                   mapOfWeights: ["sky_view_factor": 1,
                                  "aspect_ratio": 1, "building_surface_fraction": 1, "impervious_surface_fraction": 1,
                                  "pervious_surface_fraction": 1, "height_of_roughness_elements": 1,
                                  "terrain_roughness_class": 1],
                   prefixName: "test", datasource: h2GIS])

        h2GIS.getTable(pmed.results.outputTableName).eachRow {
            row ->
                if(row.id_rsu==1){
                    assertEquals("LCZ1", row.LCZ1,)
                    assertEquals(0, row.min_distance)
                }
                else{
                    assertEquals("LCZ8", row.LCZ1)
                    assertTrue(row.min_distance>0)
                    assertTrue(row.PSS<1)
                }
        }
    }
}