package org.orbisgis.orbisprocess.geoclimate.geoindicators

import com.thoughtworks.xstream.XStream
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.dataframe.DataFrame
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import smile.validation.Accuracy
import smile.validation.Validation

import java.util.zip.GZIPInputStream

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class TypologyClassificationTests {

    @Test
    void identifyLczTypeTest() {
        H2GIS h2GIS = H2GIS.open( './target/buildingdb;AUTO_SERVER=TRUE')
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
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
        H2GIS h2GIS = H2GIS.open( './target/buildingdb;AUTO_SERVER=TRUE')
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
        h2GIS.execute """
                DROP TABLE IF EXISTS tempo_rsu_for_lcz;
                CREATE TABLE tempo_rsu_for_lcz AS SELECT a.*, b.the_geom FROM rsu_test_for_lcz a LEFT JOIN rsu_test b
                ON a.id_rsu = b.id_rsu;
        """
        // Informations about where to find the training dataset for the test
        def tableTraining = "training_table"
        def urlToDownload = ""
        def trainingFile = "/home/ebocher/Autres/codes/geoclimate/model/rf/training_data.shp"

        def fileAndPath =  "/tmp/geoclimate_rf.model"

        h2GIS.load(trainingFile, tableTraining,true)

        // Variable to model
        def var2model = "I_TYPO"

        // Columns useless for the classification
        String[] colsToRemove = ["PK2", "THE_GEOM", "PK"]

        // Remove unnecessary column
        if (h2GIS.getTable(tableTraining) != null){
            h2GIS.execute """ ALTER TABLE $tableTraining DROP COLUMN ${colsToRemove.join(",")};"""
        }
        
        def  pmed =  Geoindicators.TypologyClassification.createRandomForestClassif()
        assertTrue pmed.execute([trainingTableName: tableTraining, varToModel: var2model,
                                 save: true, pathAndFileName: fileAndPath,
                                 ntrees: 300, mtry: 7, rule: "GINI", maxDepth: 100,
                                 maxNodes: 300, nodeSize: 5, subsample: 0.25, datasource: h2GIS])

        // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
        def df = DataFrame.of(h2GIS.getTable(tableTraining)).factorize(var2model).omitNullRows()
        def model = pmed.results.RfModel
        assertEquals 0.844, Accuracy.of(df.apply(var2model).toIntArray(), Validation.test(model, df)).round(3), 0.002


        // Test that the model is well written in the file and can be used to recover the variable names for example
        XStream xs = new XStream()
        def fs = new GZIPInputStream(new FileInputStream(fileAndPath))
        def modelRead = xs.fromXML(fs)
        assertEquals h2GIS.getTable(tableTraining).getColumns().minus(var2model).sort().join(","),
                modelRead.formula.x(df).names().sort().join(",")
        fs.close()
    }

}