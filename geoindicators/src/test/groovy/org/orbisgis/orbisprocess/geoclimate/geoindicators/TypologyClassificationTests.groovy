package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.dataframe.DataFrame
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class TypologyClassificationTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open( './target/buildingdb;AUTO_SERVER=TRUE')
    }

    @BeforeEach
    void initData(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void identifyLczTypeTest() {
        h2GIS.execute """
                DROP TABLE IF EXISTS tempo_rsu_for_lcz;
                CREATE TABLE tempo_rsu_for_lcz AS SELECT a.*, b.the_geom FROM rsu_test_for_lcz a LEFT JOIN rsu_test b
                ON a.id_rsu = b.id_rsu;
        """
        def  pavg =  Geoindicators.TypologyClassification.identifyLczType()
        assertTrue pavg.execute([rsuLczIndicators: "tempo_rsu_for_lcz", normalisationType: "AVG",
                   mapOfWeights: ["sky_view_factor": 1,
                                  "aspect_ratio": 1, "building_surface_fraction": 1, "impervious_surface_fraction": 1,
                                  "pervious_surface_fraction": 1, "height_of_roughness_elements": 1,
                                  "terrain_roughness_class": 1],
                   prefixName: "test", datasource: h2GIS])

        h2GIS.getTable(pavg.results.outputTableName).eachRow {
            row ->
                if (row.id_rsu == 1) {
                    assertEquals(1, row.LCZ1,)
                    assertEquals(0, row.min_distance)
                } else {
                    assertEquals(8, row.LCZ1)
                    assertTrue(row.min_distance > 0)
                    assertTrue(row.PSS < 1)
                }
        }
        def  pmed =  Geoindicators.TypologyClassification.identifyLczType()
        assertTrue pmed.execute([rsuLczIndicators: "tempo_rsu_for_lcz", normalisationType: "MEDIAN",
                   mapOfWeights: ["sky_view_factor": 1,
                                  "aspect_ratio": 1, "building_surface_fraction": 1, "impervious_surface_fraction": 1,
                                  "pervious_surface_fraction": 1, "height_of_roughness_elements": 1,
                                  "terrain_roughness_class": 1],
                   prefixName: "test", datasource: h2GIS])

        h2GIS.getTable(pmed.results.outputTableName).eachRow {
            row ->
                if(row.id_rsu==1){
                    assertEquals(1, row.LCZ1,)
                    assertEquals(0, row.min_distance)
                }
                else{
                    assertEquals(8, row.LCZ1)
                    assertTrue(row.min_distance>0)
                    assertTrue(row.PSS<1)
                }
        }
    }

    @Test
    void createRandomForestClassifTest() {
        h2GIS.execute """
                DROP TABLE IF EXISTS tempo_rsu_for_lcz;
                CREATE TABLE tempo_rsu_for_lcz AS SELECT a.*, b.the_geom FROM rsu_test_for_lcz a LEFT JOIN rsu_test b
                ON a.id_rsu = b.id_rsu;
        """
        // Informations about where to find the training dataset for the test
        def tableTraining = "testRandomForest"
        def urlToDownload = ""
        def pathAndNameFile = System.getProperty("user.home") + "/geoclimate/"+ tableTraining

        // Variable to model
        def var2model = "I_TYPO"
        // File path to save the model
        def fileAndPath = "/tmp/model.txt"

        // Columns useless for the classification
        String[] colsToRemove = ["THE_GEOM", "CITY", "PK_BUILDIN", "PK2", "PK_USR", "PK",\
                        "PK_BLOCK_Z", "ID", "I_INHAB", "U_INHAB", "U_HOUSE", "U_COL_HOU", "U_HOUSE_A",\
                     "U_COL_HOU_", "I_H_ORIGIN", "I_H", "I_LEVELS", "I_FLOOR", \
                     "I_VOL", "I_COMP_B", "I_COMP_N", "I_WALL_A", "I_PWALL_A",\
                     "I_FREE_EXT", "B_VOL", "B_H_MEAN", "B_H_STD", "B_COMP_N",\
                     "U_FLOOR", "U_COS", "U_COMP_NWM", "U_COMP_WME", "U_H_MEAN",\
                     "U_H_STD", "U_PASSIV_V", "U_VOL", "U_VOL_MEAN", "U_BH_STD_M",\
                     "U_BCOMP_NW", "U_BCOMP_WM", "U_BCOMP_ST", "U_FWALL_A"]

        // If the training dataset is not already loaded in the database...
        if (h2GIS.getTable(tableTraining) == null){
            // Test if the training dataset file if already exists...
            File file = new File(pathAndNameFile)
            if (!file){
                // If not, download it
                InputStream inputStream = new URL(urlToDownload).openStream()
                Files.copy(inputStream, Paths.get(pathAndNameFile),
                        StandardCopyOption.REPLACE_EXISTING)
            }
            h2GIS.execute """DROP TABLE IF EXISTS $tableTraining; 
                                    CALL SHPREAD('$pathAndNameFile', '$tableTraining'); 
                                    ALTER TABLE $tableTraining DROP COLUMN ${colsToRemove.join(",")};"""
        }

        def  pmed =  Geoindicators.TypologyClassification.createRandomForestClassif()
        assertTrue pmed.execute([trainingTableName: tableTraining, varToModel: var2model,
                                 save: true, pathAndFileName: fileAndPath,
                                 ntrees: 300, mtry: 7, rule: SplitRule.GINI, maxDepth: 100,
                                 maxNodes: 300, nodeSize: 5, subsample: 0.25, datasource: h2GIS])

        // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
        DataFrame df = DataFrame.of(h2GIS.getTable(tableTraining)).factorize(var2model).omitNullRows()
        def model = pmed.results.RfModel
        assertEquals 0.844, Accuracy.of(df.apply(var2model).toIntArray(), Validation.test(model, df)).round(3), 0.002

        // Test that the model is well written in the file and can be used to recover the variable names for example
        /*XStream xs = new XStream()
        FileInputStream fs = new FileInputStream(fileAndPath)
        RandomForest modelRead = xs.fromXML(fs)
        assertEquals h2GIS.getTable(tableTraining).getColumns().keySet().minus(var2model).sort().join(","),
                modelRead.formula.x(df).names().sort().join(",")*/
    }

}