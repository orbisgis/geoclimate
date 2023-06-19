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

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.io.xml.StaxDriver
import com.thoughtworks.xstream.security.NoTypePermission
import com.thoughtworks.xstream.security.NullPermission
import com.thoughtworks.xstream.security.PrimitiveTypePermission
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.dataframe.DataFrame
import org.orbisgis.geoclimate.Geoindicators
import smile.classification.DataFrameClassifier
import smile.validation.Accuracy
import smile.validation.Validation

import java.util.zip.GZIPInputStream

import static org.junit.jupiter.api.Assertions.*
import static org.orbisgis.data.H2GIS.open

class TypologyClassificationTests {

    @TempDir
    static File folder
    private static def h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "typologyClassificationTests;AUTO_SERVER=TRUE")
    }

    @Test
    void identifyLczTypeTest() {
        h2GIS.executeScript(this.getClass().getResourceAsStream("data_for_tests.sql"))
        def pavg = Geoindicators.TypologyClassification.identifyLczType(h2GIS,
                "rsu_test_lcz_indics", "rsu_test_all_indics_for_lcz",
                "AVG", "test")
        assertNotNull(pavg)

        h2GIS.save(pavg, "/tmp/lcz.json", true)

        h2GIS.save("rsu_test_all_indics_for_lcz", "/tmp/lcz_indic.json", true)
        def results = [:]
        h2GIS."$pavg".eachRow { row ->
            def id = row.id_rsu
            results[id] = [:]
            results[id]["LCZ_PRIMARY"] = row.LCZ_PRIMARY
            results[id]["LCZ_SECONDARY"] = row.LCZ_SECONDARY
            results[id]["min_distance"] = row.min_distance
            results[id]["PSS"] = row.LCZ_EQUALITY_VALUE
            assert results[id]["LCZ_PRIMARY"] != results[id]["LCZ_SECONDARY"]
        }
        assert 1 == results[1]["LCZ_PRIMARY"]
        assert 0 == results[1]["min_distance"]
        assert 8 == results[2]["LCZ_PRIMARY"]
        assert results[2]["min_distance"] > 0
        assert results[2]["PSS"] < 1
        assert 107 == results[3]["LCZ_PRIMARY"]
        assert !results[3]["LCZ_SECONDARY"]
        assert !results[3]["min_distance"]
        assert !results[3]["PSS"]
        assert 102 == results[4]["LCZ_PRIMARY"]
        assert 101 == results[5]["LCZ_PRIMARY"]
        assert 104 == results[6]["LCZ_PRIMARY"]
        assert 105 == results[7]["LCZ_PRIMARY"]
        assert 107 == results[18]["LCZ_PRIMARY"]
        assert 8 == results[19]["LCZ_PRIMARY"]
        assert 4 == results[20]["LCZ_PRIMARY"]

        h2GIS """
                DROP TABLE IF EXISTS buff_rsu_test_lcz_indics, buff_rsu_test_all_indics_for_lcz;
                CREATE TABLE buff_rsu_test_lcz_indics AS 
                    SELECT a.*, b.the_geom
                    FROM rsu_test_lcz_indics a, rsu_test b
                    WHERE a.id_rsu = b.id_rsu;
                CREATE TABLE buff_rsu_test_all_indics_for_lcz AS 
                    SELECT a.*, b.the_geom
                    FROM rsu_test_all_indics_for_lcz a, rsu_test b
                    WHERE a.id_rsu = b.id_rsu;  
        """

        def pmed = Geoindicators.TypologyClassification.identifyLczType(
                h2GIS, "buff_rsu_test_lcz_indics",
                "buff_rsu_test_all_indics_for_lcz",
                "MEDIAN", "test")
        assertNotNull(pmed)
        assert h2GIS."$pmed".columns.contains("THE_GEOM")

        h2GIS."$pmed".eachRow {
            row ->
                if (row.id_rsu == 1) {
                    assert 1 == row.LCZ_PRIMARY
                    assert 0 == row.min_distance
                } else if (row.id_rsu == 2) {
                    assert 8 == row.LCZ_PRIMARY
                    assert row.min_distance > 0
                    assert row.LCZ_EQUALITY_VALUE < 1
                } else if (row.id_rsu == 8) {
                    assert 104 == row.LCZ_PRIMARY
                    assert -1 == row.min_distance
                } else if (row.id_rsu == 9) {
                    assert 105 == row.LCZ_PRIMARY
                } else if (row.id_rsu == 10) {
                    assert 101 == row.LCZ_PRIMARY
                } else if (row.id_rsu == 10) {
                    assert 102 == row.LCZ_PRIMARY
                } else if (row.id_rsu == 12) {
                    assert 10 == row.LCZ_PRIMARY
                }
        }
        // Test with real indicator values (Montreuil ID_RSU 795), (l'haye les roses ID_RSU 965 and 1026)
        def pReal = Geoindicators.TypologyClassification.identifyLczType(h2GIS,
                "buff_rsu_test_lcz_indics",
                "buff_rsu_test_all_indics_for_lcz",
                "AVG",
                ["sky_view_factor"             : 4,
                 "aspect_ratio"                : 3,
                 "building_surface_fraction"   : 8,
                 "impervious_surface_fraction" : 0,
                 "pervious_surface_fraction"   : 0,
                 "height_of_roughness_elements": 6,
                 "terrain_roughness_length"    : 0.5],
                "test")
        assertNotNull(pReal)
        assert 6 == h2GIS.firstRow("SELECT LCZ_PRIMARY FROM ${pReal} WHERE ID_RSU = 13").LCZ_PRIMARY
        assert 6 == h2GIS.firstRow("SELECT LCZ_PRIMARY FROM ${pReal} WHERE ID_RSU = 14").LCZ_PRIMARY
        assert 4 == h2GIS.firstRow("SELECT LCZ_PRIMARY FROM ${pReal} WHERE ID_RSU = 15").LCZ_PRIMARY
        assert 5 == h2GIS.firstRow("SELECT LCZ_SECONDARY FROM ${pReal} WHERE ID_RSU = 15").LCZ_SECONDARY
        assert 6 == h2GIS.firstRow("SELECT LCZ_PRIMARY FROM ${pReal} WHERE ID_RSU = 16").LCZ_PRIMARY
        assert 102 == h2GIS.firstRow("SELECT LCZ_PRIMARY FROM ${pReal} WHERE ID_RSU = 17").LCZ_PRIMARY
    }

    @Test
    void createRandomForestClassifTest() {
        h2GIS.executeScript(this.getClass().getResourceAsStream("data_for_tests.sql"))
        h2GIS """
                DROP TABLE IF EXISTS tempo_rsu_for_lcz;
                CREATE TABLE tempo_rsu_for_lcz AS 
                    SELECT a.*, b.the_geom 
                    FROM rsu_test_lcz_indics a 
                        LEFT JOIN rsu_test b
                        ON a.id_rsu = b.id_rsu;
        """
        // Information about where to find the training dataset for the test
        def trainingTableName = "training_table"
        def trainingURL = TypologyClassificationTests.getResource("model/rf/training_data.shp")

        def uuid = UUID.randomUUID().toString().replaceAll("-", "_")
        String savePath = new File(folder, "geoclimate_rf_${uuid}.model").getAbsolutePath()

        def trainingTable = h2GIS.table(h2GIS.load(trainingURL, trainingTableName, true))
        assert trainingTable

        // Variable to model
        def var2model = "I_TYPO"

        // Columns useless for the classification
        def colsToRemove = ["PK2", "THE_GEOM", "PK"]

        // Remove unnecessary column
        h2GIS "ALTER TABLE $trainingTableName DROP COLUMN ${colsToRemove.join(",")};"

        //Reload the table due to the schema modification
        trainingTable.reload()

        def model = Geoindicators.TypologyClassification.createRandomForestModel(h2GIS,
                trainingTableName,
                var2model, [],
                true,
                savePath,
                300,
                7,
                "GINI",
                100,
                300,
                5,
                0.25)
        assert model
        assert model instanceof DataFrameClassifier

        // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
        def df = DataFrame.of(trainingTable)
        assert df
        df = df.factorize(var2model)
        assert df
        df = df.omitNullRows()
        def vector = df.apply(var2model)
        assert vector
        def truth = vector.toIntArray()
        assert truth
        def prediction = Validation.test(model, df)
        assert prediction
        def accuracy = Accuracy.of(truth, prediction)
        assert accuracy
        assertEquals 0.844, accuracy.round(3), 0.003


        // Test that the model is well written in the file and can be used to recover the variable names for example
        def xs = new XStream(new StaxDriver())
        // clear out existing permissions and start a whitelist
        xs.addPermission(NoTypePermission.NONE);
        // allow some basics
        xs.addPermission(NullPermission.NULL);
        xs.addPermission(PrimitiveTypePermission.PRIMITIVES);
        xs.allowTypeHierarchy(Collection.class);
        // allow any type from the packages
        xs.allowTypesByWildcard(new String[]{
                TypologyClassification.class.getPackage().getName() + ".*",
                "smile.regression.*", "smile.data.formula.*", "smile.data.type.*", "smile.data.measure.*", "smile.data.measure.*",
                "smile.base.cart.*", "smile.classification.*", "java.lang.*", "java.util.*"
        })
        def fileInputStream = new FileInputStream(savePath)
        assert fileInputStream
        def gzipInputStream = new GZIPInputStream(fileInputStream)
        assert gzipInputStream
        def modelRead = xs.fromXML(gzipInputStream)
        assert modelRead
        assert modelRead instanceof DataFrameClassifier
        def formula = modelRead.formula()
        assert formula
        def x = formula.x(df)
        assert x
        def names = x.names()
        assert names
        names = names.sort()
        assert names
        def namesStr = names.join(",")
        assert namesStr

        def columns = trainingTable.columns
        assert columns
        columns = columns.minus(var2model)
        assert columns
        columns = columns.sort()
        assert columns
        def columnsStr = columns.join(",")
        assert columnsStr


        assert columnsStr == namesStr
    }

    @Test
    @Disabled
    void lczTestValuesBdTopov2() {
        // Maps of weights for bd topo
        def mapOfWeights = ["sky_view_factor"             : 1,
                            "aspect_ratio"                : 1,
                            "building_surface_fraction"   : 2,
                            "impervious_surface_fraction" : 0,
                            "pervious_surface_fraction"   : 0,
                            "height_of_roughness_elements": 1,
                            "terrain_roughness_length"    : 1]

        // Define table names
        def lczIndicTable = "lcz_Indic_Table"
        def indicatorsRightId = "indicator_right_id"

        // Load the training data (LCZ classes that are possible and those which are not possible)
        def trainingDataPath = getClass().getResource("lczTests/expectedLczArea.geojson").toURI()
        def trainingDataTable = "expectedLcz"
        h2GIS.load(trainingDataPath, trainingDataTable)

        // Load the indicators table (indicators characterizing each RSU that are in the training data)
        def indicatorsPath = getClass().getResource("lczTests/trainingDatabdtopov2.geojson").toURI()
        def indicatorsTable = "trainingDatabdtopov2"
        h2GIS.load(indicatorsPath, indicatorsTable)

        // Replace the id_rsu (coming from a specific city) by the id (coming from the true values of LCZ)
        def allColumns = h2GIS.getTable(indicatorsTable).columns
        allColumns.remove("ID_RSU")
        allColumns.remove("ID")

        h2GIS """
                DROP TABLE IF EXISTS $indicatorsRightId;
                CREATE TABLE $indicatorsRightId AS 
                    SELECT id_prim AS id_rsu, ${allColumns.join(",")} FROM $indicatorsTable;
        """

        def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"              : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                             "BUILDING_FRACTION_LCZ"             : "BUILDING_SURFACE_FRACTION",
                             "ASPECT_RATIO"                      : "ASPECT_RATIO",
                             "GROUND_SKY_VIEW_FACTOR"            : "SKY_VIEW_FACTOR",
                             "PERVIOUS_FRACTION_LCZ"             : "PERVIOUS_SURFACE_FRACTION",
                             "IMPERVIOUS_FRACTION_LCZ"           : "IMPERVIOUS_SURFACE_FRACTION",
                             "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH": "TERRAIN_ROUGHNESS_LENGTH"]

        // Get into a new table the ID, geometry column and the 7 indicators defined by Stewart and Oke (2012)
        // for LCZ classification (rename the indicators with the real names)
        def queryReplaceNames = ""
        lczIndicNames.each { oldIndic, newIndic ->
            queryReplaceNames += "ALTER TABLE $lczIndicTable ALTER COLUMN $oldIndic RENAME TO $newIndic;"
        }
        h2GIS """
                DROP TABLE IF EXISTS $lczIndicTable;
                CREATE TABLE $lczIndicTable AS 
                    SELECT id_rsu, the_geom, ${lczIndicNames.keySet().join(",")} FROM $indicatorsTable;
                $queryReplaceNames
        """.toString()


        // The classification algorithm is called
        def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
        if (!classifyLCZ([rsuLczIndicators : lczIndicTable,
                          rsuAllIndicators : indicatorsRightId,
                          normalisationType: "AVG",
                          mapOfWeights     : mapOfWeights,
                          prefixName       : "test",
                          datasource       : h2GIS])) {
            fail("Cannot compute the LCZ classification.")
        }
        def rsuLcz = classifyLCZ.results.outputTableName

        h2GIS """
                DROP TABLE IF EXISTS ${rsuLcz}_buff;
                CREATE TABLE ${rsuLcz}_buff AS 
                    SELECT a.*, b.id, b.id_prim FROM $rsuLcz a LEFT JOIN $indicatorsTable b ON a.id_rsu = b.id_prim"""

        h2GIS.eachRow("SELECT  a.lcz, a.bdtopov2, b.* FROM $trainingDataTable a RIGHT JOIN ${rsuLcz}_buff b ON a.id = b.id") { row ->
            def lczExpected = row.lcz.split(",")
            // When attributing manually a LCZ to a zone, some very small RSU may have no building
            // and could not be classified while they are... Thus set 999 in all cases...
            lczExpected += "999"
            assert lczExpected.contains(row.lcz1.toString()) || lczExpected.contains(row.lcz2.toString())
        }
    }

    @Disabled
    //TODO reduce the size of the input data
    @Test
    void applyRandomForestClassif() {
        // Information about where to find the training dataset for the test
        def trainingTableName = "training_table"
        def trainingURL = "../models/TRAINING_DATA_LCZ_OSM_RF_1_0.gz"
        def savePath = "../models/LCZ_OSM_RF_1_0.model"
        def ID = "ID_RSU"

        h2GIS """ drop table if exists $trainingTableName; CALL GEOJSONREAD('${trainingURL}', '$trainingTableName');"""

        // Columns useless for the classification
        def colsToRemove = ["THE_GEOM", "LCZ"]

        // Remove unnecessary column
        h2GIS "ALTER TABLE $trainingTableName DROP COLUMN ${colsToRemove.join(",")};"

        // Add an ID column
        h2GIS """ALTER TABLE $trainingTableName ADD COLUMN $ID INT AUTO_INCREMENT PRIMARY KEY"""

        //Reload the table due to the schema modification
        h2GIS.getTable(trainingTableName).reload()

        // Input data creation
        h2GIS "CREATE TABLE inputDataTable AS SELECT * FROM $trainingTableName LIMIT 3000;"


        def pmed = Geoindicators.TypologyClassification.applyRandomForestModel()
        assert pmed.execute([
                explicativeVariablesTableName: "inputDataTable",
                pathAndFileName              : savePath,
                idName                       : ID,
                prefixName                   : "test",
                datasource                   : h2GIS])
        def predicted = pmed.results.outputTableName

        // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
        def nb_null = h2GIS.firstRow("SELECT COUNT(*) AS count FROM $predicted WHERE LCZ=1")
        assertEquals(nb_null.COUNT, 0)
    }

    //This test is used to create the training model from a specific dataset
    @Disabled
    @Test
    void tempoCreateRandomForestClassifTest() {
        // Specify the model and training datat appropriate to the right use
        def model_name = "UTRF_OSM_RF_2_2"
        def training_data_name = "TRAINING_DATA_UTRF_OSM_RF_2_2"
        // Name of the variable to model
        def var2model = "I_TYPO"
        def var2ModelFinal = "I_TYPO"
        // Whether the RF is a classif or a regression
        def classif = true

        // Information about where to find the training dataset for the test
        def trainingTableName = "training_table"
        String directory = "/home/decide/Code/Intel/geoclimate/models"
        def savePath = directory + File.separator + model_name + ".model"

        if (new File(directory).exists()) {
            // Read the training data
            h2GIS """ CALL GEOJSONREAD('${directory + File.separator + training_data_name + ".geojson.gz"}', 'tempo0')"""

            // Select only specific data
            h2GIS """   DROP TABLE IF EXISTS tempo;
                        CREATE TABLE tempo
                            AS SELECT * 
                            FROM tempo0
                            WHERE NOT (I_TYPO=1 AND BUILD_TYPE='residential')"""

            // Remove unnecessary column
            h2GIS "ALTER TABLE tempo DROP COLUMN the_geom;"
            //Reload the table due to the schema modification
            h2GIS.getTable("tempo").reload()

            def columns = h2GIS.getTable("tempo").getColumns()
            columns = columns.minus(var2model)

            h2GIS """   DROP TABLE IF EXISTS $trainingTableName;
                    CREATE TABLE $trainingTableName
                            AS SELECT CAST($var2model AS INTEGER) AS $var2ModelFinal, ${columns.join(",")}
                            FROM tempo"""

            assert h2GIS."$trainingTableName"
            def model = Geoindicators.TypologyClassification.createRandomForestModel(h2GIS,
                    trainingTableName,
                    var2ModelFinal, [],
                    true,
                    savePath,
                    500,
                    15,
                    "GINI",
                    80,
                    300,
                    1,
                    1.0)
            assert model

            // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
            def df = DataFrame.of(h2GIS."$trainingTableName")
            df = df.factorize(var2model)
            df = df.omitNullRows()
            def vector = df.apply(var2model)
            def truth = vector.toIntArray()
            def prediction = Validation.test(model, df)
            def accuracy = Accuracy.of(truth, prediction)
            assertEquals 0.725, accuracy.round(3), 1.5
        } else {
            println("The model has not been create because the output directory doesn't exist")
        }
    }


    //This test is used to create the random forest model to estimate building height on OSM data
    @Disabled
    @Test
    //TODO filter  avg_height_roof=0
    void createRandomForestModelBUILDING_HEIGHT_OSM_RF() {
        // Specify the model and training data appropriate to the right use
        def model_name = "BUILDING_HEIGHT_OSM_RF_2_2.model"
        // Name of the variable to model
        def var2model = "HEIGHT_ROOF"
        def var2ModelFinal = "HEIGHT_ROOF"

        // Information about where to find the training dataset for the test
        def trainingTableName = "training_table"
        String directory = "../geoclimate/models/"
        def savePath = directory + File.separator + model_name + ".model"

        if (new File(directory).exists()) {
            // Read the two training data files
            h2GIS """ CALL GEOJSONREAD('${"TRAINING_DATA_BUILDINGHEIGHT_OSM_RF_2_2_part1.geojson"}', 'tempo_a', true)"""
            h2GIS """ CALL GEOJSONREAD('${"TRAINING_DATA_BUILDINGHEIGHT_OSM_RF_2_2_part2.geojson"}', 'tempo_b', true)"""

            h2GIS.execute('''DROP TABLE IF EXISTS tempo; CREATE TABLE tempo as select * from tempo_a union all select * from tempo_b''')


            // Remove unnecessary column
            h2GIS "ALTER TABLE tempo DROP COLUMN the_geom;"
            //Reload the table due to the schema modification
            h2GIS.getTable("tempo").reload()

            def columns = h2GIS.getTable("tempo").getColumns()
            columns = columns.minus(var2model)

            h2GIS """   DROP TABLE IF EXISTS $trainingTableName;
                    CREATE TABLE $trainingTableName
                            AS SELECT $var2model  AS $var2ModelFinal, ${columns.join(",")}
                            FROM tempo"""

            //Parameters
            def ntree = 350
            def min_size_node = 35
            def nb_var_tree = 25
            def max_depth = 33
            def max_leaf_nodes = 1100

            assert h2GIS."$trainingTableName"
            def model = Geoindicators.TypologyClassification.createRandomForestModel(h2GIS,
                    trainingTableName,
                    var2ModelFinal, [],
                    true,
                    savePath,
                    ntree,
                    nb_var_tree,
                    "GINI",
                    max_depth,
                    max_leaf_nodes,
                    min_size_node,
                    1.0)
            assert model

            // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
            def df = DataFrame.of(h2GIS."$trainingTableName")
            df = df.omitNullRows()
            def vector = df.apply(var2model)
            def truth = vector.toDoubleArray()
            def prediction = model.predict(df)
            def accuracy = Accuracy.of(truth, prediction)
            assertEquals 0.725, accuracy.round(3), 1.5
        } else {
            println("The model has not been create because the output directory doesn't exist")
        }
    }

    //This test is used to create the random forest model to build the urban typology with BDTOPO V2.2
    @Disabled
    @Test
    void createRandomForestModelUTRF_BDTOPO_V2_RF() {
        // Specify the model and training datat appropriate to the right use
        def model_name = "UTRF_BDTOPO_V2_RF_2_2.model"
        def training_data_name = "TRAINING_DATA_UTRF_BDTOPO_V2_RF_2_2.geojson.gz"
        // Name of the variable to model
        def var2model = "I_TYPO"
        def var2ModelFinal = "I_TYPO"

        // Information about where to find the training dataset for the test
        def trainingTableName = "training_table"
        String directory = "../geoclimate/models/"
        def savePath = directory + File.separator + model_name

        if (new File(directory).exists()) {
            // Read the training data
            h2GIS """ CALL GEOJSONREAD('${directory + File.separator + training_data_name}', 'tempo0')"""

            // Select only specific data
            h2GIS """   DROP TABLE IF EXISTS tempo;
                        CREATE TABLE tempo
                            AS SELECT * 
                            FROM tempo0
                            WHERE NOT (I_TYPO=1 AND BUILD_TYPE='residential')"""

            // Remove unnecessary column
            h2GIS "ALTER TABLE tempo DROP COLUMN the_geom;"
            //Reload the table due to the schema modification
            h2GIS.getTable("tempo").reload()

            def columns = h2GIS.getTable("tempo").getColumns()
            columns = columns.minus(var2model)

            h2GIS """   DROP TABLE IF EXISTS $trainingTableName;
                    CREATE TABLE $trainingTableName
                            AS SELECT CAST($var2model AS INTEGER) AS $var2ModelFinal, ${columns.join(",")}
                            FROM tempo"""

            //Parameters
            def ntree = 500
            def min_size_node = 1
            def nb_var_tree = 15
            def max_depth = 20
            def max_leaf_nodes = 400

            assert h2GIS."$trainingTableName"
            def model = Geoindicators.TypologyClassification.createRandomForestModel(h2GIS,
                    trainingTableName,
                    var2ModelFinal, [],
                    true, savePath,
                    ntree,
                    nb_var_tree,
                    "GINI",
                    max_depth,
                    max_leaf_nodes,
                    min_size_node,
                    1.0,
                    true)
            assert model

            // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
            def df = DataFrame.of(h2GIS."$trainingTableName")
            df = df.factorize(var2model)
            df = df.omitNullRows()
            def vector = df.apply(var2model)
            def truth = vector.toIntArray()
            def prediction = Validation.test(model, df)
            def accuracy = Accuracy.of(truth, prediction)
            assertEquals 0.725, accuracy.round(3), 1.5
        } else {
            println("The model has not been create because the output directory doesn't exist")
        }
    }

    //This test is used to create the random forest model to build the urban typology with OSM
    @Disabled
    @Test
    void createRandomForestModelUTRF_OSM_RF() {
        // Specify the model and training datat appropriate to the right use
        def model_name = "UTRF_OSM_RF_2_2.model"
        def training_data_name = "TRAINING_DATA_UTRF_OSM_RF_2_2.geojson.gz"
        // Name of the variable to model
        def var2model = "I_TYPO"
        def var2ModelFinal = "I_TYPO"

        // Information about where to find the training dataset for the test
        def trainingTableName = "training_table"
        String directory = "../geoclimate/models/"

        def savePath = directory + File.separator + model_name

        if (new File(directory).exists()) {
            // Read the training data
            h2GIS """ CALL GEOJSONREAD('${directory + File.separator + training_data_name}', 'tempo0')"""

            // Select only specific data
            h2GIS """   DROP TABLE IF EXISTS tempo;
                        CREATE TABLE tempo
                            AS SELECT * 
                            FROM tempo0
                            WHERE NOT (I_TYPO=1 AND BUILD_TYPE='residential')"""

            // Remove unnecessary column
            h2GIS "ALTER TABLE tempo DROP COLUMN the_geom;"
            //Reload the table due to the schema modification
            h2GIS.getTable("tempo").reload()

            def columns = h2GIS.getTable("tempo").getColumns()
            columns = columns.minus(var2model)

            h2GIS """   DROP TABLE IF EXISTS $trainingTableName;
                    CREATE TABLE $trainingTableName
                            AS SELECT CAST($var2model AS INTEGER) AS $var2ModelFinal, ${columns.join(",")}
                            FROM tempo"""

            //Parameters
            def ntree = 500
            def min_size_node = 1
            def nb_var_tree = 15
            def max_depth = 80
            def max_leaf_nodes = 300

            assert h2GIS."$trainingTableName"
            def model = Geoindicators.TypologyClassification.createRandomForestModel(h2GIS,
                    trainingTableName,
                    var2ModelFinal, [],
                    true,
                    savePath,
                    ntree,
                    nb_var_tree,
                    "GINI",
                    max_depth,
                    max_leaf_nodes,
                    min_size_node,
                    1.0)
            assert model

            // Test that the model has been correctly calibrated (that it can be applied to the same dataset)
            def df = DataFrame.of(h2GIS."$trainingTableName")
            df = df.factorize(var2model)
            df = df.omitNullRows()
            def vector = df.apply(var2model)
            def truth = vector.toIntArray()
            def prediction = Validation.test(model, df)
            def accuracy = Accuracy.of(truth, prediction)
            assertEquals 0.725, accuracy.round(3), 1.5
        } else {
            println("The model has not been create because the output directory doesn't exist")
        }
    }
}
