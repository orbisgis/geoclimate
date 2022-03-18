package org.orbisgis.geoclimate.geoindicators

import com.thoughtworks.xstream.XStream
import com.thoughtworks.xstream.security.*
import com.thoughtworks.xstream.io.xml.StaxDriver
import groovy.transform.BaseScript
import org.h2gis.utilities.TableLocation
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.orbisdata.datamanager.dataframe.DataFrame
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import smile.base.cart.SplitRule
import smile.classification.RandomForest as RandomForestClassification
import smile.data.type.DataType
import smile.data.vector.DoubleVector
import smile.regression.RandomForest as RandomForestRegression
import smile.data.formula.Formula
import smile.data.vector.IntVector
import smile.regression.RegressionTree
import smile.validation.Accuracy
import smile.validation.RMSE
import smile.validation.Validation
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Statement
import java.util.zip.GZIPInputStream
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
 * @param rsuLczIndicators The table name where are stored ONLY the LCZ indicator such as defined by Stewart et Oke (2012),
 * the RSU id and the RSU geometries
 * @param rsuAllIndicators The table name where are stored all the RSU indicators (useful to improve the classification algorithm)
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
    return create {
        title "Set the LCZ type of each RSU"
        id "identifyLczType"
        inputs rsuLczIndicators: String, rsuAllIndicators: String, prefixName: String,
                datasource: JdbcDataSource, normalisationType: "AVG",
                mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_length": 1]
        outputs outputTableName: String
        run { rsuLczIndicators, rsuAllIndicators, prefixName, datasource, normalisationType, mapOfWeights ->

            def OPS = ["AVG", "MEDIAN"]
            def ID_FIELD_RSU = "id_rsu"
            def CENTER_NAME = "center"
            def VARIABILITY_NAME = "variability"
            def BASE_NAME = "RSU_LCZ"
            def GEOMETRIC_FIELD = "THE_GEOM"

            debug "Set the LCZ type of each RSU"

            // List of possible operations

            if (OPS.contains(normalisationType)) {
                def centerValue = [:]
                def variabilityValue = [:]
                def queryRangeNorm = ""
                def queryValuesNorm = ""

                // The name of the outputTableName is constructed
                def outputTableName = prefix prefixName, BASE_NAME

                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names are defined
                def LCZ_classes = postfix "LCZ_classes"
                def normalizedValues = postfix "normalizedValues"
                def normalizedRange = postfix "normalizedRange"
                def distribLczTable = postfix "distribLczTable"
                def distribLczTableWithoutLcz1 = postfix "distribLczTableWithout_lcz_primary"
                def distribLczTableInt = postfix "distribLczTableInt"
                def ruralLCZ = postfix "ruralLCZ"
                def classifiedRuralLCZ = postfix "classifiedRuralLCZ"
                def urbanLCZ = postfix "urbanLCZ"
                def urbanLCZExceptIndus = postfix "urbanLCZExceptIndus"
                def classifiedLcz = postfix "classifiedLcz"
                def classifiedIndustrialCommercialLcz = postfix "classifiedIndustrialLcz"
                def ruralAndIndustrialCommercialLCZ = postfix "ruralAndIndustrialLCZ"


                // The LCZ definitions are created in a Table of the DataBase (note that the "terrain_roughness_class"
                // is replaced by the "terrain_roughness_length" in order to have a continuous interval of values)
                datasource """DROP TABLE IF EXISTS $LCZ_classes; 
                        CREATE TABLE $LCZ_classes(name VARCHAR,
                        sky_view_factor_low FLOAT, sky_view_factor_upp FLOAT,
                         aspect_ratio_low FLOAT,  aspect_ratio_upp FLOAT, 
                        building_surface_fraction_low FLOAT, building_surface_fraction_upp FLOAT, 
                        impervious_surface_fraction_low FLOAT, impervious_surface_fraction_upp FLOAT, 
                        pervious_surface_fraction_low FLOAT, pervious_surface_fraction_upp FLOAT,
                         height_of_roughness_elements_low FLOAT, height_of_roughness_elements_upp FLOAT,
                        terrain_roughness_length_low FLOAT, terrain_roughness_length_upp FLOAT);
                        INSERT INTO $LCZ_classes VALUES 
                        ('1',0.2,0.4,2.0,null,0.4,0.6,0.4,0.6,0.0,0.1,25.0,null,1.5,null),
                        ('2',0.3,0.6,0.8,2.0,0.4,0.7,0.3,0.5,0.0,0.2,10.0,25.0,0.375,1.5),
                        ('3',0.2,0.6,0.8,1.5,0.4,0.7,0.2,0.5,0.0,0.3,3.0,10.0,0.375,0.75),
                        ('4',0.5,0.7,0.8,1.3,0.2,0.4,0.3,0.4,0.3,0.4,25.0,null,0.75,null),
                        ('5',0.5,0.8,0.3,0.8,0.2,0.4,0.3,0.5,0.2,0.4,10.0,25.0,0.175,0.75),
                        ('6',0.6,0.9,0.3,0.8,0.2,0.4,0.2,0.5,0.3,0.6,3.0,10.0,0.175,0.75),
                        ('7',0.2,0.5,1.0,2.0,0.6,0.9,0.0,0.2,0.0,0.3,2.0,4.0,0.175,0.375),
                        ('8',0.7,1.0,0.1,0.3,0.3,0.5,0.4,0.5,0.0,0.2,3.0,10.0,0.175,0.375),
                        ('9',0.8,1.0,0.1,0.3,0.1,0.2,0.0,0.2,0.6,0.8,3.0,10.0,0.175,0.75);""".toString()
                /* The rural LCZ types (AND INDUSTRIAL) are excluded from the algorithm, they have their own one
                    "('10',0.6,0.9,0.2,0.5,0.2,0.3,0.2,0.4,0.4,0.5,5.0,15.0,0.175,0.75),"
                    "('101',0.0,0.4,1.0,null,0.0,0.1,0.0,0.1,0.9,1.0,3.0,30.0,1.5,null)," +
                    "('102',0.5,0.8,0.3,0.8,0.0,0.1,0.0,0.1,0.9,1.0,3.0,15.0,0.175,0.75)," +
                    "('103',0.7,0.9,0.3,1.0,0.0,0.1,0.0,0.1,0.9,1.0,0.0,2.0,0.065,0.375)," +
                    "('104',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,1.0,0.01525,0.175)," +
                    "('105',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0,0.01525)," +
                    "('106',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0,0.01525)," +
                    "('107',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.0,0,0.00035);"
            */

                // LCZ types need to be String when using the IProcess 'distributionCHaracterization',
                // thus need to define a correspondence table
                def correspondenceMap = [   "LCZ1": 1,
                                            "LCZ2": 2,
                                            "LCZ3": 3,
                                            "LCZ4": 4,
                                            "LCZ5": 5,
                                            "LCZ6": 6,
                                            "LCZ7": 7,
                                            "LCZ8": 8,
                                            "LCZ9": 9]

                // I. Rural LCZ types are classified according to a "manual" decision tree
                datasource."$rsuAllIndicators".BUILDING_FRACTION_LCZ.createIndex()
                datasource."$rsuAllIndicators".ASPECT_RATIO.createIndex()

                datasource """
                    DROP TABLE IF EXISTS $ruralLCZ; 
                    CREATE TABLE $ruralLCZ AS 
                        SELECT $ID_FIELD_RSU, 
                            IMPERVIOUS_FRACTION_LCZ, 
                            PERVIOUS_FRACTION_LCZ, 
                            WATER_FRACTION_LCZ, 
                            IMPERVIOUS_FRACTION,
                            CASE 
                                WHEN IMPERVIOUS_FRACTION_LCZ+WATER_FRACTION_LCZ+BUILDING_FRACTION_LCZ=1
                                    THEN null
                                    ELSE HIGH_VEGETATION_FRACTION_LCZ/(1-IMPERVIOUS_FRACTION_LCZ-WATER_FRACTION_LCZ-BUILDING_FRACTION_LCZ)
                                    END
                                AS HIGH_ALL_VEGETATION,
                            LOW_VEGETATION_FRACTION_LCZ+HIGH_VEGETATION_FRACTION_LCZ AS ALL_VEGETATION
                        FROM $rsuAllIndicators
                        WHERE (BUILDING_FRACTION_LCZ < 0.1 OR BUILDING_FRACTION_LCZ IS NULL) 
                        AND ASPECT_RATIO < 0.1;""".toString()

                datasource."$ruralLCZ".IMPERVIOUS_FRACTION_LCZ.createIndex()
                datasource."$ruralLCZ".PERVIOUS_FRACTION_LCZ.createIndex()
                datasource."$ruralLCZ".HIGH_ALL_VEGETATION.createIndex()
                datasource."$ruralLCZ".ALL_VEGETATION.createIndex()
                datasource """DROP TABLE IF EXISTS $classifiedRuralLCZ;
                                CREATE TABLE $classifiedRuralLCZ
                                        AS SELECT   $ID_FIELD_RSU,
                                                CASE WHEN IMPERVIOUS_FRACTION_LCZ>ALL_VEGETATION AND IMPERVIOUS_FRACTION_LCZ>WATER_FRACTION_LCZ AND IMPERVIOUS_FRACTION_LCZ>0.1
                                                        THEN 105
                                                        ELSE CASE WHEN ALL_VEGETATION<WATER_FRACTION_LCZ AND WATER_FRACTION_LCZ> 0.31
                                                                THEN 107
                                                                ELSE CASE WHEN HIGH_ALL_VEGETATION IS NULL OR HIGH_ALL_VEGETATION<0.05
                                                                        THEN 104
                                                                        ELSE CASE WHEN HIGH_ALL_VEGETATION<0.75
                                                                                THEN 102
                                                                                ELSE 101 END END END END AS LCZ1,
                                                null AS LCZ2,
                                                CASE WHEN IMPERVIOUS_FRACTION_LCZ+PERVIOUS_FRACTION_LCZ<0.5 
                                                THEN -1
                                                ELSE null END AS min_distance, 
                                                null AS LCZ_UNIQUENESS_VALUE, 
                                                null AS LCZ_EQUALITY_VALUE
                                        FROM $ruralLCZ""".toString()
                // II. Urban LCZ types are classified

                // Keep only the RSU that have not been classified as rural
                datasource """DROP TABLE IF EXISTS $urbanLCZ;
                                CREATE TABLE $urbanLCZ
                                        AS SELECT   a.*, 
                                                    a.AREA_FRACTION_COMMERCIAL + a.AREA_FRACTION_LIGHT_INDUSTRY AS AREA_FRACTION_LOWRISE_TYPO
                                        FROM $rsuAllIndicators a
                                        LEFT JOIN $ruralLCZ b
                                        ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU
                                        WHERE b.$ID_FIELD_RSU IS NULL;""".toString()

                // 0. Set as industrial areas or large low-rise (commercial) having more of industrial or commercial than residential
                // and at least 1/3 of fraction
                if (datasource."$urbanLCZ".hasColumn("AREA_FRACTION_HEAVY_INDUSTRY")) {
                    datasource """DROP TABLE IF EXISTS $classifiedIndustrialCommercialLcz;
                                CREATE TABLE $classifiedIndustrialCommercialLcz
                                        AS SELECT   $ID_FIELD_RSU,
                                                    CASE WHEN AREA_FRACTION_HEAVY_INDUSTRY > AREA_FRACTION_LOWRISE_TYPO
                                                        THEN 10 
                                                        ELSE 8  END AS LCZ1,
                                                    null        AS LCZ2, 
                                                    null        AS min_distance,
                                                    null        AS LCZ_UNIQUENESS_VALUE,
                                                    null        AS LCZ_EQUALITY_VALUE
                                        FROM $urbanLCZ 
                                        WHERE   AREA_FRACTION_HEAVY_INDUSTRY > AREA_FRACTION_LOWRISE_TYPO AND AREA_FRACTION_HEAVY_INDUSTRY>0.33
                                                OR AREA_FRACTION_LOWRISE_TYPO > AREA_FRACTION_RESIDENTIAL AND AREA_FRACTION_LOWRISE_TYPO>0.33
                                                    AND AVG_NB_LEV_AREA_WEIGHTED < 3
                                                    AND LOW_VEGETATION_FRACTION_LCZ+HIGH_VEGETATION_FRACTION_LCZ<0.2
                                                    AND GROUND_SKY_VIEW_FACTOR > 0.7;
                                DROP TABLE IF EXISTS $ruralAndIndustrialCommercialLCZ;
                                CREATE TABLE $ruralAndIndustrialCommercialLCZ
                                            AS SELECT * 
                                            FROM $classifiedIndustrialCommercialLcz 
                                        UNION ALL 
                                            SELECT *
                                            FROM $classifiedRuralLCZ""".toString()
                } else {
                    datasource """ALTER TABLE $classifiedRuralLCZ RENAME TO $ruralAndIndustrialCommercialLCZ""".toString()
                }
                datasource."$ruralAndIndustrialCommercialLCZ"."$ID_FIELD_RSU".createIndex()
                datasource."$rsuLczIndicators"."$ID_FIELD_RSU".createIndex()

                datasource """DROP TABLE IF EXISTS $urbanLCZExceptIndus;
                                CREATE TABLE $urbanLCZExceptIndus
                                        AS SELECT a.*
                                        FROM $rsuLczIndicators a
                                        LEFT JOIN $ruralAndIndustrialCommercialLCZ b
                                        ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU
                                        WHERE b.$ID_FIELD_RSU IS NULL;""".toString()

                // 1. Each dimension (each of the 7 indicators) is normalized according to average and standard deviation
                // (or median and median of the variability)

                // For each LCZ indicator...
                def urbanLCZExceptIndusColumns = datasource."$urbanLCZExceptIndus".columns
                urbanLCZExceptIndusColumns.collect { indicCol ->
                    if (!indicCol.equalsIgnoreCase(ID_FIELD_RSU) && !indicCol.equalsIgnoreCase(GEOMETRIC_FIELD)) {
                        // The values used for normalization ("mean" and "standard deviation") are calculated
                        // (for each column) and stored into maps
                        centerValue[indicCol] = datasource.firstRow("SELECT ${normalisationType}(all_val) " +
                                "AS $CENTER_NAME FROM (SELECT ${indicCol}_low AS all_val FROM $LCZ_classes " +
                                "WHERE ${indicCol}_low IS NOT NULL UNION ALL SELECT ${indicCol}_upp AS all_val " +
                                "FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)")."$CENTER_NAME"
                        if (normalisationType == "AVG") {
                            variabilityValue[indicCol] = datasource.firstRow("SELECT STDDEV_POP(all_val) " +
                                    "AS $VARIABILITY_NAME FROM (SELECT ${indicCol}_low AS all_val " +
                                    "FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                    "SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes WHERE ${indicCol}_upp " +
                                    "IS NOT NULL)")."$VARIABILITY_NAME"
                        } else {
                            variabilityValue[indicCol] = datasource.firstRow("SELECT MEDIAN(ABS(all_val-" +
                                    "${centerValue[indicCol]})) AS $VARIABILITY_NAME FROM " +
                                    "(SELECT ${indicCol}_low AS all_val FROM $LCZ_classes WHERE ${indicCol}_low " +
                                    "IS NOT NULL UNION ALL SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes " +
                                    "WHERE ${indicCol}_upp IS NOT NULL)")."$VARIABILITY_NAME"
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
                datasource """DROP TABLE IF EXISTS $normalizedRange; CREATE TABLE $normalizedRange 
                        AS SELECT name, ${queryRangeNorm[0..-3]} FROM $LCZ_classes""".toString()

                // The input indicator values are normalized according to "center" and "variability" values
                datasource """DROP TABLE IF EXISTS $normalizedValues; CREATE TABLE $normalizedValues 
                        AS SELECT $ID_FIELD_RSU, ${queryValuesNorm[0..-3]} FROM $urbanLCZExceptIndus""".toString()


                // 2. The distance of each RSU to each of the LCZ types is calculated in the normalized interval.
                // The two LCZ types being the closest to the RSU indicators are associated to this RSU. An indicator
                // of uncertainty based on the Perkin Skill Score method is also associated to this "assignment".

                // For each LCZ type...
                def queryLczDistance = ""
                datasource.eachRow("SELECT * FROM $normalizedRange") { LCZ ->
                    queryLczDistance += "SQRT("
                    // For each indicator...
                    urbanLCZExceptIndusColumns.collect { indic ->
                        if (!indic.equalsIgnoreCase(ID_FIELD_RSU) && !indic.equalsIgnoreCase(GEOMETRIC_FIELD)) {
                            // Define columns names where are stored lower and upper range values of the current LCZ
                            // and current indicator
                            def valLow = indic + "_low"
                            def valUpp = indic + "_upp"
                            // Piece of query useful for calculating the RSU distance to the current LCZ
                            // for the current indicator
                            queryLczDistance +=
                                    "${mapOfWeights[indic.toLowerCase()]}*POWER(CASEWHEN(${LCZ[valLow]} IS NULL," +
                                            "CASEWHEN($indic<${LCZ[valUpp]}, 0, ${LCZ[valUpp]}-$indic)," +
                                            "CASEWHEN(${LCZ[valUpp]} IS NULL," +
                                            "CASEWHEN($indic>${LCZ[valLow]},0,${LCZ[valLow]}-$indic)," +
                                            "CASEWHEN($indic<${LCZ[valLow]},${LCZ[valLow]}-$indic," +
                                            "CASEWHEN($indic<${LCZ[valUpp]},0,${LCZ[valUpp]}-$indic)))),2)+"
                        }
                    }

                    queryLczDistance = "${queryLczDistance[0..-2]}) AS LCZ${LCZ.name},"
                }

                // Create the distribution table (the distance to each LCZ type - as column - is calculated for each RSU - as row)
                datasource  """DROP TABLE IF EXISTS $distribLczTable;
                            CREATE TABLE $distribLczTable 
                                    AS SELECT   $ID_FIELD_RSU, ${queryLczDistance[0..-2]}
                                    FROM        $normalizedValues;""".toString()

                // Specific behavior for LCZ type 1 (compact high rise): we suppose it is impossible to have a LCZ1 if
                // the mean average nb of building level in the RSU <10 (we set LCZ1 value to -9999.99 in this case)
                def colDistribTable = datasource.getTable(distribLczTable).getColumns()
                colDistribTable=colDistribTable.minus(["LCZ1"])
                datasource."$distribLczTable"."$ID_FIELD_RSU".createIndex()
                datasource."$urbanLCZ"."$ID_FIELD_RSU".createIndex()
                datasource."$urbanLCZ".AVG_NB_LEV_AREA_WEIGHTED.createIndex()
                datasource """  DROP TABLE IF EXISTS $distribLczTableWithoutLcz1;
                                CREATE TABLE $distribLczTableWithoutLcz1 
                                    AS SELECT   a.${colDistribTable.join(", a.")},
                                                CASE WHEN b.AVG_NB_LEV_AREA_WEIGHTED < 10 
                                                    THEN -9999.99 
                                                    ELSE LCZ1 END AS LCZ1
                                    FROM        $distribLczTable a 
                                    LEFT JOIN   $urbanLCZ b
                                        ON a.$ID_FIELD_RSU=b.$ID_FIELD_RSU;""".toString()

                // The distribution is characterized
                datasource """DROP TABLE IF EXISTS ${prefix prefixName, 'DISTRIBUTION_REPARTITION'}""".toString()
                def computeDistribChar = Geoindicators.GenericIndicators.distributionCharacterization()
                computeDistribChar([distribTableName: distribLczTableWithoutLcz1,
                                    inputId         : ID_FIELD_RSU,
                                    initialTable    : distribLczTableWithoutLcz1,
                                    distribIndicator: ["equality", "uniqueness"],
                                    extremum        : "LEAST",
                                    keep2ndCol      : true,
                                    keepColVal      : true,
                                    prefixName      : prefixName,
                                    datasource      : datasource])
                def resultsDistrib = computeDistribChar.results.outputTableName

                // Rename the standard indicators into names consistent with the current IProcess (LCZ type...)
                datasource """  ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_COL TO LCZ_PRIMARY;
                                ALTER TABLE $resultsDistrib RENAME COLUMN UNIQUENESS_VALUE TO LCZ_UNIQUENESS_VALUE;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EQUALITY_VALUE TO LCZ_EQUALITY_VALUE;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_COL2 TO LCZ_SECONDARY;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_VAL TO MIN_DISTANCE;""".toString()

                // Need to replace the string LCZ values by an integer
                datasource."$resultsDistrib".lcz_primary.createIndex()
                datasource."$resultsDistrib".lcz_secondary.createIndex()
                def casewhenQuery1 = ""
                def casewhenQuery2 = ""
                def parenthesis = ""
                correspondenceMap.each{lczString, lczInt ->
                    casewhenQuery1 += "CASEWHEN(LCZ_PRIMARY = '$lczString', $lczInt, "
                    casewhenQuery2 += "CASEWHEN(LCZ_SECONDARY = '$lczString', $lczInt, "
                    parenthesis += ")"
                }
                datasource """  DROP TABLE IF EXISTS $distribLczTableInt;
                                CREATE TABLE $distribLczTableInt
                                        AS SELECT   $ID_FIELD_RSU, $casewhenQuery1 null$parenthesis AS LCZ_PRIMARY,
                                                    $casewhenQuery2 null$parenthesis AS LCZ_SECONDARY, 
                                                    MIN_DISTANCE, LCZ_UNIQUENESS_VALUE, LCZ_EQUALITY_VALUE 
                                        FROM $resultsDistrib""".toString()

                // Then urban and rural LCZ types are merged into a single table
                datasource """DROP TABLE IF EXISTS $classifiedLcz;
                                CREATE TABLE $classifiedLcz 
                                        AS SELECT   * 
                                        FROM        $distribLczTableInt
                                        UNION ALL   SELECT * FROM $ruralAndIndustrialCommercialLCZ b;""".toString()

                // If the input tables contain a geometric field, we add it to the output table
                if (datasource."$rsuAllIndicators".hasColumn(GEOMETRIC_FIELD)) {
                    datasource """DROP TABLE IF EXISTS $outputTableName;
                                    CREATE TABLE $outputTableName
                                            AS SELECT   a.*, b.$GEOMETRIC_FIELD
                                            FROM        $classifiedLcz a
                                            LEFT JOIN   $rsuAllIndicators b
                                            ON          a.$ID_FIELD_RSU=b.$ID_FIELD_RSU""".toString()
                } else if (datasource."$rsuLczIndicators".hasColumn(GEOMETRIC_FIELD)) {
                    datasource """DROP TABLE IF EXISTS $outputTableName;
                                    CREATE TABLE $outputTableName
                                            AS SELECT   a.*, b.$GEOMETRIC_FIELD
                                            FROM        $classifiedLcz a
                                            LEFT JOIN   $rsuLczIndicators b
                                            ON          a.$ID_FIELD_RSU=b.$ID_FIELD_RSU""".toString()
                } else {
                    datasource """ALTER TABLE $classifiedLcz RENAME TO $outputTableName;""".toString()
                }
/*
                // Temporary tables are deleted
                datasource """DROP TABLE IF EXISTS ${prefixName}_distribution_repartition,
                    $LCZ_classes, $normalizedValues, $normalizedRange,
                    $distribLczTable, $distribLczTableInt, $allLczTable, $pivotedTable, $mainLczTable,
                    $classifiedLcz, $classifiedUrbanLcz, $classifiedRuralLCZ, $distribLczTableWithoutLcz1;"""
*/
                debug "The LCZ classification has been performed."

                [outputTableName: outputTableName]
            } else {
                error "The 'normalisationType' argument is not valid."
            }
        }
    }
}

/**
 * This process is used to create a random Forest model for regression or classification purpose.
 * The training dataset and the variables to use for the training may be gathered in a
 * same table. The resulting model is returned and may also be saved into a file.
 *
 * Note that the algorithms are based on the Smile library (cf. https://github.com/haifengl/smile)
 *
 * @param trainingTableName The name of the training table where are stored ONLY the explicative variables
 * and the one to model
 * @param varToModel Name of the field to model
 * @param explicativeVariables List of the explicative variables to use in the training. If empty, all columns in
 * the training table (except 'varToModel') are used (default []).
 * @param save Boolean to save the model into a file if needed
 * @param pathAndFileName String of the path and name where the model has to be saved (default "/home/RfModel")
 * @param ntrees The number of trees to build the forest
 * @param mtry The number of input variables to be used to determine the decision
 * at a node of the tree. p/3 seems to give generally good performance,
 * where p is the number of variables
 * @param rule Decision tree split rule FOR CLASSIFICATION ONLY (The function to measure the quality of a split. Supported rules
 * are “gini” for the Gini impurity and “entropy” for the information gain)
 * @param maxDepth The maximum depth of the tree.
 * @param maxNodes The maximum number of leaf nodes in the tree.
 * @param nodeSize The number of instances in a node below which the tree will
 * not split, setting nodeSize = 5 generally gives good results.
 * @param subsample The sampling rate for training tree. 1.0 means sampling with replacement. < 1.0 means
 * sampling without replacement.
 * @param classif Boolean to specify whether the randomForest is a classification or a regression
 * @param datasource A connection to a database
 *
 * @return RfModel A randomForest model (see smile library for further information about the object)
 *
 * @author Jérémy Bernard
 */
IProcess createRandomForestModel() {
    return create {
        title "Create a Random Forest model"
        id "createRandomForest"
        inputs trainingTableName: String, varToModel: String, explicativeVariables: [], save: boolean, pathAndFileName: String, ntrees: int,
                mtry: int, rule: "GINI", maxDepth: int, maxNodes: int, nodeSize: int, subsample: double,
                datasource: JdbcDataSource, classif: true
        outputs RfModel: RandomForestRegression
        run { String trainingTableName, String varToModel, explicativeVariables, save, pathAndFileName, ntrees, mtry, rule, maxDepth,
              maxNodes, nodeSize, subsample, JdbcDataSource datasource, classif ->

            def splitRule
            if (rule) {
                switch (rule.toUpperCase()) {
                    case "GINI":
                    case "ENTROPY":
                        splitRule = SplitRule.valueOf(rule)
                        break
                    default:
                        error "The rule value ${rule} is not supported. Please use 'GINI' or 'ENTROPY'"
                        return
                }
            } else {
                error "The rule value cannot be null or empty. Please use 'GINI' or 'ENTROPY'"
                return
            }
            debug "Create a Random Forest model"

            def trainingTable = datasource."$trainingTableName"

            //Check if the column names exists
            if (!trainingTable.hasColumn(varToModel)) {
                error "The training table should have a column named $varToModel"
                return
            }
            // If needed, select only some specific columns for the training in the dataframe
            def df
            if (explicativeVariables){
                def tabFin = datasource.getTable(trainingTableName).columns(explicativeVariables).getTable()
                df = DataFrame.of(tabFin)
            }
            else{
                def tabFin = datasource.getTable(trainingTableName)
                df = DataFrame.of(tabFin)
            }
            def formula = Formula.lhs(varToModel)
            def columnTypes = df.getColumnsTypes()
            def dfFactorized = df.omitNullRows()

            // Identify columns being string (thus needed to be factorized)
            columnTypes.each{colName, colType ->
                if(colType == "String"){
                    dfFactorized = dfFactorized.factorize(colName)
                }
            }
            // Create the randomForest
            def model
            if(classif){
                model = RandomForestClassification.fit(formula, dfFactorized, ntrees, mtry, splitRule, maxDepth, maxNodes, nodeSize, subsample)
            }
            else{
                model = RandomForestRegression.fit(formula, dfFactorized, ntrees, mtry, maxDepth, maxNodes, nodeSize, subsample)
            }


            // Calculate the prediction using the same sample in order to identify what is the
            // data rate that has been well classified
            def prediction = Validation.test(model, dfFactorized)
            def truth
            if(DataType.isDouble(dfFactorized.schema().field(varToModel).type)){
                truth = dfFactorized.apply(varToModel).toDoubleArray()
                def rmse = RMSE.of(truth, prediction)
                error "The root mean square error is : ${rmse}"
            }
            else{
                truth = dfFactorized.apply(varToModel).toIntArray()
                def accuracy = Accuracy.of(truth, prediction)
                debug "The percentage of the data that have been well classified is : ${accuracy * 100}%"
            }

            try {
                if (save) {
                    def zOut = new GZIPOutputStream(new FileOutputStream(pathAndFileName))
                    def xs = new XStream(new StaxDriver())
                    xs.toXML(model, zOut)
                    zOut.close()
                }
            }
            catch (Exception e) {
                error "Cannot save the model", e
                return
            }
            [RfModel: model]
        }
    }

}

/**
 * This process is used to apply a RandomForest model on a given dataset (the model may be downloaded on a default
 * folder or provided by the user). A table containing the predicted values and the id is returned.
 *
 * @param explicativeVariablesTableName The name of the table containing the indicators used by the random forest model
 * @param pathAndFileName If the user wants to use its own model, URL of the model file on the user machine (default: "")
 * @param idName Name of the ID column (which will be removed for the application of the random Forest)
 * @param prefixName String use as prefix to name the output table

 * @param datasource A connection to a database
 *
 * @return RfModel A randomForest model (see smile library for further information about the object)
 *
 * @author Jérémy Bernard
 */
IProcess applyRandomForestModel() {
    return create {
        title "Apply a Random Forest classification"
        id "applyRandomForestModel"
        inputs explicativeVariablesTableName: String, pathAndFileName: String, idName: String,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { String explicativeVariablesTableName, String pathAndFileName, String idName,
              String prefixName, JdbcDataSource datasource ->
            debug "Apply a Random Forest model"
            def modelName;
            File inputModelFile = new File(pathAndFileName)
            modelName = FilenameUtils.getBaseName(pathAndFileName)
            if (!inputModelFile.exists()) {
                //We try to find this model in geoclimate
                def modelURL = "https://github.com/orbisgis/geoclimate/raw/master/models/${modelName}.model"
                def localInputModelFile = new File(System.getProperty("user.home") + File.separator + ".geoclimate" + File.separator + modelName + ".model")
                // The model doesn't exist on the local folder we download it
                if (!localInputModelFile.exists()) {
                    FileUtils.copyURLToFile(new URL(modelURL), localInputModelFile)
                    if (!localInputModelFile.exists()) {
                        error "Cannot find any model file to apply the classification tree"
                        return null
                    }
                }
                inputModelFile=localInputModelFile;
            } else {
                if(FilenameUtils.isExtension(pathAndFileName, "model")){
                    modelName = FilenameUtils.getBaseName(pathAndFileName)
                }
                else{
                    error "The extension of the model file must be .model"
                    return null
                }
            }
            def fileInputStream = new FileInputStream(inputModelFile)
            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, modelName.toLowerCase();
            // Load the RandomForest model
            def xs = new XStream(new StaxDriver())
            // clear out existing permissions and start a whitelist
            xs.addPermission(NoTypePermission.NONE);
            // allow some basics
            xs.addPermission(NullPermission.NULL);
            xs.addPermission(PrimitiveTypePermission.PRIMITIVES);
            xs.allowTypeHierarchy(Collection.class);
            // allow any type from the packages
            xs.allowTypesByWildcard(new String[] {
                    TypologyClassification.class.getPackage().getName()+".*",
                    "smile.regression.*","smile.data.formula.*", "smile.data.type.*", "smile.data.measure.*", "smile.data.measure.*",
                    "smile.base.cart.*","smile.classification.*","java.lang.*"
            })

            // Load the model and recover the name of the variable to model
            def gzipInputStream = new GZIPInputStream(fileInputStream)
            def model = xs.fromXML(gzipInputStream)
            def varType
            def var2model
            def tree = model.trees[0]
            def modelColumnNames =[]
            if(tree instanceof RegressionTree){
                def response = tree.response
                varType = response.type
                var2model = response.name
                modelColumnNames = tree.schema.fields.collect {
                    it.name
                }
            }else{
                def response = tree.tree.response
                varType = response.type
                var2model = response.name
                modelColumnNames = tree.tree.schema.fields.collect {
                    it.name
                }
            }
            //Check the column names before building the dataframe and apply the model
            def inputColumns = datasource."$explicativeVariablesTableName".getColumnsTypes()

            def allowedColumnNames = modelColumnNames.intersect(inputColumns.keySet())
            def notSameColumnNames = allowedColumnNames.size() != modelColumnNames.size()
            if (!allowedColumnNames && notSameColumnNames) {
                error "Cannot find the requiered columns to apply the model"
                return
            }

            def isDouble = false
            if(DataType.isDouble(varType)){
                isDouble=true
            }
            // We need to add the name of the predicted variable in order to use the model
            datasource.execute """ALTER TABLE $explicativeVariablesTableName ADD COLUMN $var2model ${isDouble?"DOUBLE PRECISION":"INTEGER"} DEFAULT 0""".toString()
            datasource."$explicativeVariablesTableName".reload()

            // The table containing explicative variables is recovered
            def explicativeVariablesTable = datasource."$explicativeVariablesTableName"


            // Read the table containing the explicative variables as a DataFrame
            def dfNofactorized = DataFrame.of(explicativeVariablesTable)

            def df = dfNofactorized
            // Identify columns being string (thus needed to be factorized)
            inputColumns.each{colName, colType ->
                if(colType == "STRING" || colType=="VARCHAR" || colType=="CHARACTER VARYING"){
                    df = df.factorize(colName)
                }
            }
            // Remove the id for the application of the randomForest
            def df_var = df.drop(idName.toUpperCase())



            def prediction = Validation.test(model, df_var)
            // We need to add the remove the initial predicted variable in order to not have duplicated...
            df=df.drop(var2model)
            df=df.merge(isDouble?DoubleVector.of(var2model, prediction):IntVector.of(var2model, prediction))


            //TODO change this after SMILE answer's
            // Keep only the id and the value of the classification
            df = df.select(idName.toUpperCase(), var2model.toUpperCase())
            String tableName = TableLocation.parse(outputTableName, datasource.getDataBaseType()).toString();
            try {
                PreparedStatement preparedStatement = null;
                Connection outputconnection = datasource.getConnection();
                try {
                    Statement outputconnectionStatement = outputconnection.createStatement();
                    outputconnectionStatement.execute("DROP TABLE IF EXISTS " + tableName);
                    def create_table_ = "CREATE TABLE ${tableName} (${idName.toUpperCase()} INTEGER, ${var2model.toUpperCase()} ${isDouble?"DOUBLE PRECISION":"INTEGER"})" ;
                    def insertTable = "INSERT INTO ${tableName}  VALUES(?,?)";
                    outputconnection.setAutoCommit(false);
                    outputconnectionStatement.execute(create_table_.toString());
                    preparedStatement = outputconnection.prepareStatement(insertTable.toString());
                    long batch_size = 0;
                    int batchSize = 1000;
                    while (df.next()) {
                        def id = df.getString(0)
                        def predictedValue = df.getObject(1)
                        preparedStatement.setObject( 1, id);
                        preparedStatement.setObject( 2, predictedValue);
                        preparedStatement.addBatch();
                        batch_size++;
                        if (batch_size >= batchSize) {
                            preparedStatement.executeBatch();
                            preparedStatement.clearBatch();
                            batchSize = 0;
                        }
                    }
                    if (batch_size > 0) {
                        preparedStatement.executeBatch();
                    }
                } catch (SQLException e) {
                    error("Cannot save the dataframe.\n", e);
                    return null;
                } finally {
                    outputconnection.setAutoCommit(true);
                    if (preparedStatement != null) {
                        preparedStatement.close();
                    }
                }
            } catch (SQLException e) {
                error("Cannot save the dataframe.\n", e);
                return null;
            }
            [outputTableName: tableName]
        }
    }
}
