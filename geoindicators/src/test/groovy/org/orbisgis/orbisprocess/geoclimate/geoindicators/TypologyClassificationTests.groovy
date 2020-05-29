package org.orbisgis.orbisprocess.geoclimate.geoindicators

import com.thoughtworks.xstream.XStream
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.dataframe.DataFrame
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import smile.classification.DataFrameClassifier
import smile.validation.Accuracy
import smile.validation.Validation
import java.util.zip.GZIPInputStream

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.fail
import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open
import static org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager.load

class TypologyClassificationTests {

    private static def h2GIS
    private static def GI
    private static def randomDbName() {"${TypologyClassificationTests.simpleName}_${UUID.randomUUID().toString().replaceAll"-", "_"}"}

    @BeforeAll
    static void beforeAll(){
        h2GIS = open"./target/${randomDbName()};AUTO_SERVER=TRUE"
        GI = load Geoindicators
    }

    @BeforeEach
    void beforeEach(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void identifyLczTypeTest() {
        def pavg = GI.TypologyClassification.identifyLczType
        assert pavg.execute([
                rsuLczIndicators    : "rsu_test_lcz_indics",
                rsuAllIndicators    : "rsu_test_all_indics_for_lcz",
                normalisationType   : "AVG",
                mapOfWeights        : ["sky_view_factor"                : 1,
                                       "aspect_ratio"                   : 1,
                                       "building_surface_fraction"      : 1,
                                       "impervious_surface_fraction"    : 1,
                                       "pervious_surface_fraction"      : 1,
                                       "height_of_roughness_elements"   : 1,
                                       "terrain_roughness_length"       : 1],
                prefixName          : "test",
                datasource          : h2GIS])
        def results = [:]
        h2GIS."$pavg.results.outputTableName".eachRow { row ->
            def id = row.id_rsu
            results[id] = [:]
            results[id]["LCZ1"] = row.LCZ1
            results[id]["min_distance"] = row.min_distance
            results[id]["PSS"] = row.PSS
        }
        assert 1 == results[1]["LCZ1"]
        assert 0 == results[1]["min_distance"]
        assert 8 == results[2]["LCZ1"]
        assert results[2]["min_distance"] > 0
        assert results[2]["PSS"] < 1
        assert 107 == results[3]["LCZ1"]
        assert !results[3]["LCZ2"]
        assert !results[3]["min_distance"]
        assert !results[3]["PSS"]
        assert 102 == results[4]["LCZ1"]
        assert 101 == results[5]["LCZ1"]
        assert 104 == results[6]["LCZ1"]
        assert 105 == results[7]["LCZ1"]

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

        def pmed = GI.TypologyClassification.identifyLczType
        assert pmed([
                rsuLczIndicators    : "buff_rsu_test_lcz_indics",
                rsuAllIndicators    : "buff_rsu_test_all_indics_for_lcz",
                normalisationType   : "MEDIAN",
                mapOfWeights        : ["sky_view_factor"                : 1,
                                       "aspect_ratio"                   : 1,
                                       "building_surface_fraction"      : 1,
                                       "impervious_surface_fraction"    : 1,
                                       "pervious_surface_fraction"      : 1,
                                       "height_of_roughness_elements"   : 1,
                                       "terrain_roughness_length"       : 1],
                prefixName          : "test",
                datasource          : h2GIS])

        assert h2GIS."$pmed.results.outputTableName".columns.contains("THE_GEOM")

        h2GIS."$pmed.results.outputTableName".eachRow {
            row ->
                if(row.id_rsu == 1){
                    assert 1 == row.LCZ1
                    assert 0 == row.min_distance
                }
                else if(row.id_rsu == 2){
                    assert 8 == row.LCZ1
                    assert row.min_distance > 0
                    assert row.PSS < 1
                }
                else if(row.id_rsu == 8){
                    assert 999 == row.LCZ1
                }
        }
    }

    @Test
    void createRandomForestClassifTest() {
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
        def savePath = "target/geoclimate_rf_${uuid}.model"

        def trainingTable = h2GIS.load(trainingURL, trainingTableName,true)
        assert trainingTable

        // Variable to model
        def var2model = "I_TYPO"

        // Columns useless for the classification
        def colsToRemove = ["PK2", "THE_GEOM", "PK"]

        // Remove unnecessary column
        h2GIS "ALTER TABLE $trainingTableName DROP COLUMN ${colsToRemove.join(",")};"

        //Reload the table due to the schema modification
        trainingTable.reload()

        def pmed =  GI.TypologyClassification.createRandomForestClassif
        assert pmed.execute([
                trainingTableName   : trainingTableName,
                varToModel          : var2model,
                save                : true,
                pathAndFileName     : savePath,
                ntrees              : 300,
                mtry                : 7,
                rule                : "GINI",
                maxDepth            : 100,
                maxNodes            : 300,
                nodeSize            : 5,
                subsample           : 0.25,
                datasource          : h2GIS])
        def model = pmed.results.RfModel
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
        assertEquals 0.844, accuracy.round(3), 0.002


        // Test that the model is well written in the file and can be used to recover the variable names for example
        def xs = new XStream()
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
        def mapOfWeights = ["sky_view_factor"               : 1,
                            "aspect_ratio"                  : 1,
                            "building_surface_fraction"     : 2,
                            "impervious_surface_fraction"   : 0,
                            "pervious_surface_fraction"     : 0,
                            "height_of_roughness_elements"  : 1,
                            "terrain_roughness_length"      : 1]

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

        def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"                 : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                             "BUILDING_FRACTION_LCZ"                : "BUILDING_SURFACE_FRACTION",
                             "ASPECT_RATIO"                         : "ASPECT_RATIO",
                             "GROUND_SKY_VIEW_FACTOR"               : "SKY_VIEW_FACTOR",
                             "PERVIOUS_FRACTION_LCZ"                : "PERVIOUS_SURFACE_FRACTION",
                             "IMPERVIOUS_FRACTION_LCZ"              : "IMPERVIOUS_SURFACE_FRACTION",
                             "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH"   : "TERRAIN_ROUGHNESS_LENGTH"]

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
        """


        // The classification algorithm is called
        def classifyLCZ = GI.TypologyClassification.identifyLczType
        if(!classifyLCZ([rsuLczIndicators   : lczIndicTable,
                         rsuAllIndicators   : indicatorsRightId,
                         normalisationType  : "AVG",
                         mapOfWeights       : mapOfWeights,
                         prefixName         : "test",
                         datasource         : h2GIS])){
            fail("Cannot compute the LCZ classification.")
        }
        def rsuLcz = classifyLCZ.results.outputTableName

        h2GIS """
                DROP TABLE IF EXISTS ${rsuLcz}_buff;
                CREATE TABLE ${rsuLcz}_buff AS 
                    SELECT a.*, b.id, b.id_prim FROM $rsuLcz a LEFT JOIN $indicatorsTable b ON a.id_rsu = b.id_prim"""

        h2GIS.eachRow("SELECT  a.lcz, a.bdtopov2, b.* FROM $trainingDataTable a RIGHT JOIN ${rsuLcz}_buff b ON a.id = b.id"){row ->
            def lczExpected = row.lcz.split(",")
            // When attributing manually a LCZ to a zone, some very small RSU may have no building
            // and could not be classified while they are... Thus set 999 in all cases...
            lczExpected += "999"
           assert lczExpected.contains(row.lcz1.toString()) || lczExpected.contains(row.lcz2.toString())
        }
    }
}