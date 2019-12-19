package org.orbisgis.orbisprocess.geoclimate.geoindicators

import com.thoughtworks.xstream.XStream
import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.api.dataset.ITable
import org.orbisgis.orbisdata.datamanager.dataframe.DataFrame
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import smile.base.cart.SplitRule
import smile.classification.RandomForest
import smile.data.formula.Formula
import smile.validation.Accuracy
import smile.validation.Validation

import java.util.zip.GZIPOutputStream

@BaseScript Geoindicators geoindicators

/**
 * This process is used to assign to a Reference Spatial Unit (RSU) a Local Climate Zone type (Stewart et Oke, 2012).
 * This assignment is performed according to the 7 indicators used for LCZ classification. Each LCZ type has a
 * given range for each of the 7 indicators. Then the method to find the LCZ type that is the most appropriate for
 * a given RSU is based on the minimum distance to each LCZ (in the 7 dimensions space). In order to calculate this
 * distance, each dimension is normalized according to the mean and the standard deviation (or median and absolute median
 * deviation) of the interval values. Some of the indicators may be more important (or reliable) than the other
 * for the LCZ identification. In order to manage this point, a map containing weights may be passed and will be
 * used to multiply the distance due to a given indicator.
 *
 * @param rsuLczIndicators The table name where are stored ONLY the LCZ indicator values, the RSU id and the RSU geometries
 * @param normalisationType The indicators used for normalisation of the indicators
 *          --> "AVG": the mean and the standard deviation are used
 *          --> "MEDIAN": the median and the mediane absolute deviation are used
 * @param mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
 * @param prefixName String use as prefix to name the output table
 * @param datasource A connection to a database
 *
 * References:
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of
 * the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 *
 * @return A database table name.
 *
 * @author Jérémy Bernard
 */
IProcess identifyLczType() {
    def final OPS = ["AVG", "MEDIAN"]
    def final ID_FIELD_RSU = "id_rsu"
    def final CENTER_NAME = "center"
    def final VARIABILITY_NAME = "variability"
    def final BASE_NAME = "RSU_LCZ"
    def final GEOMETRIC_FIELD = "the_geom"
    
    return create({
        title "Set the LCZ type of each RSU"
        inputs rsuLczIndicators: String, prefixName: String, datasource: JdbcDataSource, normalisationType: "AVG",
                mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_class": 1]
        outputs outputTableName: String
        run { rsuLczIndicators, prefixName, datasource, normalisationType, mapOfWeights ->

            info "Set the LCZ type of each RSU"

            // List of possible operations

            if (OPS.contains(normalisationType)) {
                def centerValue = [:]
                def variabilityValue = [:]
                def queryRangeNorm = ""
                def queryValuesNorm = ""
                def queryForPivot = ""
                def queryPerkinsSkill = ""

                // The name of the outputTableName is constructed
                def outputTableName = getOutputTableName(prefixName, BASE_NAME)

                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names are defined
                def LCZ_classes = "LCZ_classes$uuid"
                def normalizedValues = "normalizedValues$uuid"
                def normalizedRange = "normalizedRange$uuid"
                def buffLczTable = "buffLczTable$uuid"
                def allLczTable = "allLczTable$uuid"
                def pivotedTable = "pivotedTable$uuid"
                def mainLczTable = "mainLczTable$uuid"

                // I. Each dimension (each of the 7 indicators) is normalized according to average and standard deviation
                // (or median and median of the variability)

                // The LCZ definitions are created in a Table of the DataBase
                datasource.execute "DROP TABLE IF EXISTS $LCZ_classes; " +
                        "CREATE TABLE $LCZ_classes(name VARCHAR," +
                        "sky_view_factor_low FLOAT, sky_view_factor_upp FLOAT," +
                        " aspect_ratio_low FLOAT,  aspect_ratio_upp FLOAT, " +
                        "building_surface_fraction_low FLOAT, building_surface_fraction_upp FLOAT, " +
                        "impervious_surface_fraction_low FLOAT, impervious_surface_fraction_upp FLOAT, " +
                        "pervious_surface_fraction_low FLOAT, pervious_surface_fraction_upp FLOAT," +
                        " height_of_roughness_elements_low FLOAT, height_of_roughness_elements_upp FLOAT," +
                        "terrain_roughness_class_low FLOAT, terrain_roughness_class_upp FLOAT);" +
                        "INSERT INTO $LCZ_classes VALUES " +
                        "('1',0.2,0.4,2.0,null,0.4,0.6,0.4,0.6,0.0,0.1,25.0,null,7.5,8.5)," +
                        "('2',0.3,0.6,0.8,2.0,0.4,0.7,0.3,0.5,0.0,0.2,10.0,25.0,5.5,7.5)," +
                        "('3',0.2,0.6,0.8,1.5,0.4,0.7,0.2,0.5,0.0,0.3,3.0,10.0,5.5,6.5)," +
                        "('4',0.5,0.7,0.8,1.3,0.2,0.4,0.3,0.4,0.3,0.4,25.0,null,6.5,8.5)," +
                        "('5',0.5,0.8,0.3,0.8,0.2,0.4,0.3,0.5,0.2,0.4,10.0,25.0,4.5,6.5)," +
                        "('6',0.6,0.9,0.3,0.8,0.2,0.4,0.2,0.5,0.3,0.6,3.0,10.0,4.5,6.5)," +
                        "('7',0.2,0.5,1.0,2.0,0.6,0.9,0.0,0.2,0.0,0.3,2.0,4.0,3.5,5.5)," +
                        "('8',0.7,1.0,0.1,0.3,0.3,0.5,0.4,0.5,0.0,0.2,3.0,10.0,4.5,5.5)," +
                        "('9',0.8,1.0,0.1,0.3,0.1,0.2,0.0,0.2,0.6,0.8,3.0,10.0,4.5,6.5)," +
                        "('10',0.6,0.9,0.2,0.5,0.2,0.3,0.2,0.4,0.4,0.5,5.0,15.0,4.5,6.5)," +
                        "('101',0.0,0.4,1.0,null,0.0,0.1,0.0,0.1,0.9,1.0,3.0,30.0,7.5,8.5)," +
                        "('102',0.5,0.8,0.3,0.8,0.0,0.1,0.0,0.1,0.9,1.0,3.0,15.0,4.5,6.5)," +
                        "('103',0.7,0.9,0.3,1.0,0.0,0.1,0.0,0.1,0.9,1.0,0.0,2.0,3.5,5.5)," +
                        "('104',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,1.0,2.5,4.5)," +
                        "('105',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0.5,2.5)," +
                        "('106',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0.5,2.5)," +
                        "('107',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.0,0.5,1.5);"

                // For each LCZ indicator...
                datasource.getTable(rsuLczIndicators).columns.collect { indicCol ->
                    if (!indicCol.equalsIgnoreCase(ID_FIELD_RSU) && !indicCol.equalsIgnoreCase(GEOMETRIC_FIELD)) {
                        // The values used for normalization ("mean" and "standard deviation") are calculated
                        // (for each column) and stored into maps
                        centerValue[indicCol] = datasource.firstRow("SELECT ${normalisationType}(all_val) " +
                                "AS $CENTER_NAME FROM (SELECT ${indicCol}_low AS all_val FROM $LCZ_classes " +
                                "WHERE ${indicCol}_low IS NOT NULL UNION ALL SELECT ${indicCol}_upp AS all_val " +
                                "FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)")[CENTER_NAME]
                        if (normalisationType == "AVG") {
                            variabilityValue[indicCol] = datasource.firstRow("SELECT STDDEV_POP(all_val) " +
                                    "AS $VARIABILITY_NAME FROM (SELECT ${indicCol}_low AS all_val " +
                                    "FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                    "SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes WHERE ${indicCol}_upp " +
                                    "IS NOT NULL)")[VARIABILITY_NAME]
                        } else {
                            variabilityValue[indicCol] = datasource.firstRow("SELECT MEDIAN(ABS(all_val-" +
                                    "${centerValue[indicCol]})) AS $VARIABILITY_NAME FROM " +
                                    "(SELECT ${indicCol}_low AS all_val FROM $LCZ_classes WHERE ${indicCol}_low " +
                                    "IS NOT NULL UNION ALL SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes " +
                                    "WHERE ${indicCol}_upp IS NOT NULL)")[VARIABILITY_NAME]
                        }
                        // Piece of query useful for normalizing the LCZ indicator intervals
                        queryRangeNorm += " (${indicCol}_low-${centerValue[indicCol]})/${variabilityValue[indicCol]}" +
                                " AS ${indicCol}_low, (${indicCol}_upp-${centerValue[indicCol]})/" +
                                "${variabilityValue[indicCol]} AS ${indicCol}_upp, "

                        // Piece of query useful for normalizing the LCZ input values
                        queryValuesNorm += " ($indicCol-${centerValue[indicCol]})/${variabilityValue[indicCol]} AS " +
                                "$indicCol, "
                    }
                }
                // The indicator interval of the LCZ types are normalized according to "center" and "variability" values
                datasource.execute "DROP TABLE IF EXISTS $normalizedRange; CREATE TABLE $normalizedRange " +
                        "AS SELECT name, ${queryRangeNorm[0..-3]} FROM $LCZ_classes"

                // The input indicator values are normalized according to "center" and "variability" values
                datasource.execute "DROP TABLE IF EXISTS $normalizedValues; CREATE TABLE $normalizedValues " +
                        "AS SELECT $ID_FIELD_RSU, $GEOMETRIC_FIELD, ${queryValuesNorm[0..-3]} FROM $rsuLczIndicators"


                // II. The distance of each RSU to each of the LCZ types is calculated in the normalized interval.
                // The two LCZ types being the closest to the RSU indicators are associated to this RSU. An indicator
                // of uncertainty based on the Perkin Skill Score method is also associated to this "assignment".

                // Create the table where will be stored the distance to each LCZ for each RSU
                datasource.execute "DROP TABLE IF EXISTS $allLczTable; CREATE TABLE $allLczTable(" +
                        "pk serial, $GEOMETRIC_FIELD GEOMETRY, $ID_FIELD_RSU integer, lcz varchar, distance float);"


                // For each LCZ type...
                datasource.eachRow("SELECT * FROM $normalizedRange") { LCZ ->
                    def queryLczDistance = ""
                    // For each indicator...
                    datasource.getTable(rsuLczIndicators).columns.collect { indic ->
                        if (!indic.equalsIgnoreCase(ID_FIELD_RSU) && !indic.equalsIgnoreCase(GEOMETRIC_FIELD)) {
                            // Define columns names where are stored lower and upper range values of the current LCZ
                            // and current indicator
                            def valLow = indic + "_low"
                            def valUpp = indic + "_upp"
                            // Piece of query useful for calculating the RSU distance to the current LCZ
                            // for the current indicator
                            queryLczDistance +=
                                    "POWER(${mapOfWeights[indic.toLowerCase()]}*CASEWHEN(${LCZ[valLow]} IS NULL," +
                                            "CASEWHEN($indic<${LCZ[valUpp]}, 0, ${LCZ[valUpp]}-$indic)," +
                                            "CASEWHEN(${LCZ[valUpp]} IS NULL," +
                                            "CASEWHEN($indic>${LCZ[valLow]},0,${LCZ[valLow]}-$indic)," +
                                            "CASEWHEN($indic<${LCZ[valLow]},${LCZ[valLow]}-$indic," +
                                            "CASEWHEN($indic<${LCZ[valUpp]},0,${LCZ[valUpp]}-$indic)))),2)+"
                        }
                    }

                    // Fill the table where are stored the distance of each RSU to each LCZ type
                    datasource.execute "DROP TABLE IF EXISTS $buffLczTable; ALTER TABLE $allLczTable RENAME TO $buffLczTable;" +
                            "DROP TABLE IF EXISTS $allLczTable; " +
                            "CREATE TABLE $allLczTable(pk serial, $GEOMETRIC_FIELD GEOMETRY, $ID_FIELD_RSU integer, lcz varchar, distance float) " +
                            "AS (SELECT pk, $GEOMETRIC_FIELD, $ID_FIELD_RSU, lcz, distance FROM $buffLczTable UNION ALL " +
                            "SELECT null, $GEOMETRIC_FIELD, $ID_FIELD_RSU, '${LCZ.name}', SQRT(${queryLczDistance[0..-2]}) FROM $normalizedValues)"

                }

                //

                // The name of the two closest LCZ types are conserved
                datasource.execute "DROP TABLE IF EXISTS $mainLczTable;" +
                        "CREATE INDEX IF NOT EXISTS all_id ON $allLczTable USING BTREE($ID_FIELD_RSU); " +
                        "CREATE TABLE $mainLczTable AS SELECT a.$ID_FIELD_RSU, a.$GEOMETRIC_FIELD, " +
                        "CAST(SELECT b.lcz FROM $allLczTable b " +
                        "WHERE a.$ID_FIELD_RSU = b.$ID_FIELD_RSU " +
                        "ORDER BY b.distance ASC LIMIT 1 AS INTEGER) AS LCZ1," +
                        "CAST(SELECT b.lcz FROM $allLczTable b " +
                        "WHERE a.$ID_FIELD_RSU = b.$ID_FIELD_RSU " +
                        "ORDER BY b.distance ASC LIMIT 1 OFFSET 1 AS INTEGER) AS LCZ2 " +
                        "FROM $allLczTable a GROUP BY $ID_FIELD_RSU"

                // Recover the LCZ TYPES list and the number of types in a map
                def lczTypeTempo = datasource.rows "SELECT name FROM $LCZ_classes"
                def lczType = []
                lczTypeTempo.each { l ->
                    lczType.add("${l["NAME"]}")
                }
                // For each LCZ type...
                datasource.eachRow("SELECT name FROM $LCZ_classes") { LCZ ->
                    // Piece of query that will be useful for pivoting the LCZ distance table
                    queryForPivot += "MAX(CASEWHEN(lcz = '${LCZ.name}', distance, null)) AS \"${LCZ.name}\","

                    // Piece of query that will be useful for the calculation of the Perkins Skill Score
                    queryPerkinsSkill += "LEAST(1./${lczType.size()}, b.\"${LCZ.name}\"/(b.\"${lczType.join("\"+b.\"")}\"))+"
                }

                // The table is pivoted in order to have the distance for each LCZ type as column and for each RSU as row.
                // Then for each RSU, the distance to the closest LCZ type is stored and the Perkins Skill Score is calculated
                datasource.execute "DROP TABLE IF EXISTS $pivotedTable;" +
                        "CREATE TABLE $pivotedTable AS SELECT $ID_FIELD_RSU," +
                        "${queryForPivot[0..-2]} FROM $allLczTable GROUP BY $ID_FIELD_RSU;" +
                        "DROP TABLE IF EXISTS $outputTableName;" +
                        "CREATE INDEX IF NOT EXISTS main_id ON $mainLczTable USING BTREE($ID_FIELD_RSU);" +
                        "CREATE INDEX IF NOT EXISTS piv_id ON $pivotedTable USING BTREE($ID_FIELD_RSU);" +
                        "CREATE TABLE $outputTableName AS SELECT a.*, LEAST(b.\"${lczType.join("\",b.\"")}\") AS min_distance, " +
                        "${queryPerkinsSkill[0..-2]} AS PSS FROM $mainLczTable a LEFT JOIN " +
                        "$pivotedTable b ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;"

                // Temporary tables are deleted
                datasource.execute "DROP TABLE IF EXISTS $LCZ_classes, $normalizedValues, $normalizedRange," +
                        "$buffLczTable, $allLczTable, $pivotedTable, $mainLczTable;"

                info "The LCZ classification has been performed."

                [outputTableName: outputTableName]
            } else {
                error "The 'normalisationType' argument is not valid."
            }
        }
    })
}

/**
 * This process is used to create a classification model based on a RandomForest algorithm.
 * The training dataset and the variables to use for the training may be gathered in a
 * same table. All parameters of the randomForest algorithm have default values but may be
 * modified. The resulting model is returned and may also be saved into a file.
 *
 * Note that the algorithm is based on the Smile library (cf. https://github.com/haifengl/smile)
 *
 * @param trainingTableName The name of the training table where are stored ONLY the explicative variables
 * and the one to model
 * @param varToModel String where is saved the name of the field to model
 * @param save Boolean to save the model into a file if needed
 * @param pathAndFileName String of the path and name where the model has to be saved (default "/home/RfModel")
 * @param ntrees The number of trees to build the forest
 * @param mtry The number of input variables to be used to determine the decision
 * at a node of the tree. p/3 seems to give generally good performance,
 * where p is the number of variables
 * @param rule Decision tree split rule (The function to measure the quality of a split. Supported rules
 * are “gini” for the Gini impurity and “entropy” for the information gain)
 * @param maxDepth The maximum depth of the tree.
 * @param maxNodes The maximum number of leaf nodes in the tree.
 * @param nodeSize The number of instances in a node below which the tree will
 * not split, setting nodeSize = 5 generally gives good results.
 * @param subsample The sampling rate for training tree. 1.0 means sampling with replacement. < 1.0 means
 * sampling without replacement.
 * @param datasource A connection to a database
 *
 * @return RfModel A randomForest model (see smile library for further information about the object)
 *
 * @author Jérémy Bernard
 */
IProcess createRandomForestClassif() {
    return create({
        title "Create a Random Forest model"
        inputs trainingTableName: String, varToModel: String, save: boolean, pathAndFileName: String, ntrees: int,
                mtry: int, rule: "GINI", maxDepth: int, maxNodes: int, nodeSize: int, subsample: double,
                datasource: JdbcDataSource
        outputs RfModel: RandomForest
        run { String trainingTableName, String varToModel, save, pathAndFileName,  ntrees, mtry, rule, maxDepth,
              maxNodes, nodeSize, subsample, JdbcDataSource datasource ->

            def splitRule
            if(rule){
                switch(rule.toUpperCase()) {
                    case "GINI":
                    case "ENTROPY":
                        splitRule = SplitRule.valueOf(rule)
                        break
                    default:
                        error "The rule value ${rule} is not supported. Please use 'GINI' or 'ENTROPY'"
                        return null;
                }
            }
            else{
                error "The rule value cannot be null or empty. Please use 'GINI' or 'ENTROPY'"
                return null;
            }
            info "Create a Random Forest model"

            //Check if the column names exists
            def columnTypo = "I_TYPO"

            def trainingTable = datasource.getTable(trainingTableName)

            if(!trainingTable.hasColumn(columnTypo, String.class)){
                error "The training table should have a String column name 'I_TYPO'"
                return null
            }

            // Read the training table as a DataFrame
            def df = DataFrame.of(trainingTable)

            def formula = Formula.lhs(varToModel)

            // Convert the variable to model into factors (if string for example) and remove rows containing null values
            df = df.factorize(varToModel).omitNullRows()

            // Create the randomForest
            def model = RandomForest.fit(formula, df, ntrees, mtry, splitRule, maxDepth, maxNodes, nodeSize, subsample)


            // Calculate the prediction using the same sample in order to identify what is the
            // data rate that has been well classified
            int[] prediction = Validation.test(model, df)
            int[] truth = df.apply(varToModel).toIntArray()
            def accuracy = Accuracy.of(truth, prediction)
            logger.info "The percentage of the data that have been well classified is : ${accuracy*100}%"

            try {
                if (save) {
                    def zOut = new GZIPOutputStream(new FileOutputStream(pathAndFileName));
                    def xs = new XStream()
                    xs.toXML(model, zOut)
                    zOut.close()
                }
            }
            catch (Exception e){
                logger.error("Cannot save the model", e)
                return null
            }

            [RfModel: model]
        }
    })
}
