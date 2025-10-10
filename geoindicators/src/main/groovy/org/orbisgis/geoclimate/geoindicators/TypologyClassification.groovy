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
import groovy.transform.BaseScript
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.h2gis.utilities.TableLocation
import org.orbisgis.data.dataframe.DataFrame
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators
import smile.base.cart.SplitRule
import smile.classification.RandomForest as RandomForestClassification
import smile.data.formula.Formula
import smile.data.type.DataType
import smile.data.type.DataTypes
import smile.data.vector.DoubleVector
import smile.data.vector.IntVector
import smile.regression.RandomForest as RandomForestRegression
import smile.regression.RegressionTree
import smile.validation.Accuracy
import smile.validation.RMSE
import smile.validation.Validation

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
 * @author Erwan Bocher, CNRS
 */
String identifyLczType(JdbcDataSource datasource, String rsuLczIndicators, String rsuAllIndicators, String normalisationType = "AVG",
                       Map mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                                           "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                                           "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
                       String prefixName) throws Exception {
    def OPS = ["AVG", "MEDIAN"]
    def ID_FIELD_RSU = "id_rsu"
    def CENTER_NAME = "center"
    def VARIABILITY_NAME = "variability"
    def BASE_NAME = "RSU_LCZ"
    def GEOMETRIC_FIELD = "THE_GEOM"

    debug "Set the LCZ type of each RSU"

    // List of possible operations

    if (OPS.contains(normalisationType)) {
        def tablesToDrop = []
        def centerValue = [:]
        def variabilityValue = [:]
        def queryRangeNorm = ""
        def queryValuesNorm = ""

        // The name of the outputTableName is constructed
        def outputTableName = postfix(BASE_NAME)

        // To avoid overwriting the output files of this step, a unique identifier is created
        // Temporary table names are defined
        def LCZ_classes = postfix "LCZ_classes"
        def normalizedValues = postfix "normalizedValues"
        def normalizedRange = postfix "normalizedRange"
        def distribLczTable = postfix "distribLczTable"
        def distribLczTableWithoutLcz1 = postfix "distribLczTableWithout_lcz_primary"
        def distribLczTableInt = postfix "distribLczTableInt"
        def ruralLCZ = postfix "ruralLCZ"
        def ruralLCZUncertainty = postfix "ruralLCZUncertainty"
        def classifiedRuralLCZ = postfix "classifiedRuralLCZ"
        def urbanLCZ = postfix "urbanLCZ"
        def urbanLCZExceptIndus = postfix "urbanLCZExceptIndus"
        def classifiedLcz = postfix "classifiedLcz"
        def classifiedIndustrialCommercialLcz = postfix "classifiedIndustrialLcz"
        def ruralAndIndustrialCommercialLCZ = postfix "ruralAndIndustrialLCZ"


        // The LCZ definitions are created in a Table of the DataBase (note that the "terrain_roughness_class"
        // is replaced by the "terrain_roughness_length" in order to have a continuous interval of values)
        datasource.execute("""DROP TABLE IF EXISTS $LCZ_classes; 
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
                        ('9',0.8,1.0,0.1,0.3,0.1,0.2,0.0,0.2,0.6,0.8,3.0,10.0,0.175,0.75);""".toString())
        /* The land cover LCZ types (AND LCZ8 and 10) are excluded from the algorithm, they have their own one
            "('8',0.7,1.0,0.1,0.3,0.3,0.5,0.4,0.5,0.0,0.2,3.0,10.0,0.175,0.375)," +
            "('10',0.6,0.9,0.2,0.5,0.2,0.3,0.2,0.4,0.4,0.5,5.0,15.0,0.175,0.75)," +
            "('101',0.0,0.4,1.0,null,0.0,0.1,0.0,0.1,0.9,1.0,3.0,30.0,1.5,null)," +
            "('102',0.5,0.8,0.3,0.8,0.0,0.1,0.0,0.1,0.9,1.0,3.0,15.0,0.175,0.75)," +
            "('103',0.7,0.9,0.3,1.0,0.0,0.1,0.0,0.1,0.9,1.0,0.0,2.0,0.065,0.375)," +
            "('104',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,1.0,0.01525,0.175)," +
            "('105',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0,0.01525)," +
            "('106',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0,0.01525)," +
            "('107',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.0,0,0.00035);"
    */

        // LCZ types need to be String when using the method 'distributionCHaracterization',
        // thus need to define a correspondence table
        def correspondenceMap = ["LCZ1": 1,
                                 "LCZ2": 2,
                                 "LCZ3": 3,
                                 "LCZ4": 4,
                                 "LCZ5": 5,
                                 "LCZ6": 6,
                                 "LCZ7": 7,
                                 "LCZ8": 8,
                                 "LCZ9": 9]

        // Define thresholds of LCZ method as variables
        double urbanBuildFracMin = 0.1
        double urbanAspectRatioMin = 0.1
        double scatteredTreeLow = 0.05
        double scatteredTreeHigh = 0.75
        double lcz8LevNumbMax = 3
        double lcz8VegFracMax = 0.2
        double lcz8SVFMin = 0.7
        double lcz10IndFracMin = 0.33
        double lcz8LLRFracMin = 0.33

        // I. Land cover LCZ types are classified according to a "manual" decision tree
        datasource.createIndex(rsuAllIndicators, "BUILDING_FRACTION_LCZ")
        datasource.createIndex(rsuAllIndicators, "ASPECT_RATIO")
        datasource.createIndex(rsuAllIndicators, ID_FIELD_RSU)

        datasource """
                    DROP TABLE IF EXISTS $ruralLCZ; 
                    CREATE TABLE $ruralLCZ AS 
                        SELECT $ID_FIELD_RSU, 
                            BUILDING_FRACTION_LCZ,
                            ASPECT_RATIO,
                            HIGH_VEGETATION_FRACTION_LCZ,
                            IMPERVIOUS_FRACTION_LCZ, 
                            PERVIOUS_FRACTION_LCZ, 
                            WATER_FRACTION_LCZ, 
                            IMPERVIOUS_FRACTION,
                            CASE WHEN IMPERVIOUS_FRACTION_LCZ+WATER_FRACTION_LCZ+BUILDING_FRACTION_LCZ=0 and HIGH_VEGETATION_FRACTION_LCZ =0 
                            THEN NULL 
                            WHEN IMPERVIOUS_FRACTION_LCZ+WATER_FRACTION_LCZ+BUILDING_FRACTION_LCZ=1
                            THEN CASE WHEN HIGH_VEGETATION_FRACTION_LCZ = 0
                                 THEN 0
                                 ELSE 1
                                 END
                            ELSE
                                CASE  WHEN (1-IMPERVIOUS_FRACTION_LCZ+WATER_FRACTION_LCZ+BUILDING_FRACTION_LCZ) <= HIGH_VEGETATION_FRACTION_LCZ
                                THEN 1
                                ELSE HIGH_VEGETATION_FRACTION_LCZ/(1-IMPERVIOUS_FRACTION_LCZ-WATER_FRACTION_LCZ-BUILDING_FRACTION_LCZ)
                                END
                            END
                                AS HIGH_ALL_VEGETATION,
                            LOW_VEGETATION_FRACTION_LCZ+HIGH_VEGETATION_FRACTION_LCZ AS ALL_VEGETATION
                        FROM $rsuAllIndicators
                        WHERE BUILDING_FRACTION_LCZ < $urbanBuildFracMin 
                        AND ASPECT_RATIO < $urbanAspectRatioMin;""".toString()

        // LCZ primary classification
        datasource.createIndex(ruralLCZ, ID_FIELD_RSU)
        datasource.createIndex(ruralLCZ, "IMPERVIOUS_FRACTION_LCZ")
        datasource.createIndex(ruralLCZ, "PERVIOUS_FRACTION_LCZ")
        datasource.createIndex(ruralLCZ, "HIGH_ALL_VEGETATION")
        datasource.createIndex(ruralLCZ, "ALL_VEGETATION")
        datasource """DROP TABLE IF EXISTS $classifiedRuralLCZ;
                                CREATE TABLE $classifiedRuralLCZ
                                        AS SELECT   *,
                                                CASE WHEN IMPERVIOUS_FRACTION_LCZ>ALL_VEGETATION AND IMPERVIOUS_FRACTION_LCZ>WATER_FRACTION_LCZ AND IMPERVIOUS_FRACTION_LCZ>0.1
                                                        THEN 105
                                                        ELSE CASE WHEN ALL_VEGETATION<=WATER_FRACTION_LCZ AND WATER_FRACTION_LCZ> 0.31
                                                                THEN 107
                                                                ELSE CASE WHEN HIGH_ALL_VEGETATION IS NULL OR HIGH_ALL_VEGETATION<$scatteredTreeLow
                                                                        THEN 104
                                                                        ELSE CASE WHEN HIGH_ALL_VEGETATION<$scatteredTreeHigh
                                                                                THEN 102
                                                                                ELSE 101 END END END END AS LCZ1,
                                                null AS LCZ2,
                                                CASE WHEN IMPERVIOUS_FRACTION_LCZ+PERVIOUS_FRACTION_LCZ<0.5 
                                                THEN -1
                                                ELSE null END AS min_distance, 
                                                null AS LCZ_UNIQUENESS_VALUE, 
                                                null AS LCZ_EQUALITY_VALUE
                                        FROM $ruralLCZ""".toString()

        // Classification uncertainty indicators are calculated
        Map fraction_indic_map = [101:"HIGH_VEGETATION_FRACTION_LCZ",
                                  104:"ALL_VEGETATION-HIGH_VEGETATION_FRACTION_LCZ",
                                  105:"IMPERVIOUS_FRACTION_LCZ",
                                  107:"WATER_FRACTION_LCZ"]
        List query_case_when_list = []
        fraction_indic_map.each{lcz_type,land_cover_indic ->
            query_case_when_list.add("""CASE WHEN LCZ1 = $lcz_type 
                                  THEN 0.25 * ($urbanAspectRatioMin - ASPECT_RATIO) / $urbanAspectRatioMin
                                       + 0.25 * ($urbanBuildFracMin - BUILDING_FRACTION_LCZ) / $urbanBuildFracMin
                                       + 0.5 * ($land_cover_indic - $urbanBuildFracMin) / (1 - $urbanBuildFracMin)""")
        }
        String query_case_when = query_case_when_list.join(" ELSE ")

        String query_case_when_end = ""
        fraction_indic_map.each{
            query_case_when_end += " END"
        }
        datasource """DROP TABLE IF EXISTS $ruralLCZUncertainty;
                    CREATE TABLE $ruralLCZUncertainty
                            AS SELECT   $ID_FIELD_RSU,
                                    LCZ1,
                                    LCZ2,
                                    min_distance, 
                                    $query_case_when ELSE
                                    CASE WHEN LCZ1 = 102
                                    THEN 0.25 * ($urbanAspectRatioMin - ASPECT_RATIO) / $urbanAspectRatioMin
                                         + 0.25 * ($urbanBuildFracMin - BUILDING_FRACTION_LCZ) / $urbanBuildFracMin
                                         + 0.25 * (ALL_VEGETATION - $urbanBuildFracMin) / (1 - $urbanBuildFracMin) 
                                         + 0.25 * (($scatteredTreeHigh - $scatteredTreeLow) / 2 
                                                    - ABS(HIGH_ALL_VEGETATION - ($scatteredTreeHigh - $scatteredTreeLow) / 2))
                                                    / (($scatteredTreeHigh - $scatteredTreeLow) / 2 )
                                         $query_case_when_end END AS LCZ_UNIQUENESS_VALUE, 
                                    null AS LCZ_EQUALITY_VALUE
                            FROM $classifiedRuralLCZ""".toString()

        tablesToDrop << ruralLCZ
        tablesToDrop << ruralLCZUncertainty
        // II. Urban LCZ types are classified
        // Keep only the RSU that have not been classified as rural
        datasource """DROP TABLE IF EXISTS $urbanLCZ;
                                CREATE TABLE $urbanLCZ
                                        AS SELECT   a.*, 
                                                    a.AREA_FRACTION_COMMERCIAL_LCZ + a.AREA_FRACTION_LIGHT_INDUSTRY_LCZ AS AREA_FRACTION_LOWRISE_TYPO
                                        FROM $rsuAllIndicators a
                                        LEFT JOIN $ruralLCZUncertainty b
                                        ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU
                                        WHERE b.$ID_FIELD_RSU IS NULL;""".toString()

        // 0. Set as industrial areas or large low-rise (commercial) having more of industrial or commercial than residential
        // and at least 1/3 of fraction
        if (datasource.getColumnNames(urbanLCZ).contains("AREA_FRACTION_HEAVY_INDUSTRY_LCZ")) {
            datasource """DROP TABLE IF EXISTS $classifiedIndustrialCommercialLcz;
                                CREATE TABLE $classifiedIndustrialCommercialLcz
                                        AS SELECT   *,
                                                    CASE WHEN AREA_FRACTION_HEAVY_INDUSTRY_LCZ > AREA_FRACTION_LOWRISE_TYPO
                                                        THEN 10 
                                                        ELSE 8  
                                                        END AS LCZ1,
                                                    null        AS LCZ2, 
                                                    null        AS min_distance,
                                                    null        AS LCZ_UNIQUENESS_VALUE,
                                                    null        AS LCZ_EQUALITY_VALUE
                                        FROM $urbanLCZ 
                                        WHERE   AREA_FRACTION_HEAVY_INDUSTRY_LCZ > AREA_FRACTION_LOWRISE_TYPO AND AREA_FRACTION_HEAVY_INDUSTRY_LCZ > $lcz10IndFracMin
                                                OR AREA_FRACTION_LOWRISE_TYPO > AREA_FRACTION_RESIDENTIAL_LCZ AND AREA_FRACTION_LOWRISE_TYPO > $lcz8LLRFracMin
                                                    AND AVG_NB_LEV_AREA_WEIGHTED < $lcz8LevNumbMax
                                                    AND LOW_VEGETATION_FRACTION_LCZ+HIGH_VEGETATION_FRACTION_LCZ < $lcz8VegFracMax
                                                    AND GROUND_SKY_VIEW_FACTOR > $lcz8SVFMin;
                                DROP TABLE IF EXISTS $ruralAndIndustrialCommercialLCZ;
                                CREATE TABLE $ruralAndIndustrialCommercialLCZ
                                            AS SELECT $ID_FIELD_RSU,
                                                      LCZ1,
                                                      LCZ2,
                                                      min_distance,
                                                      CASE WHEN LCZ1 = 10 
                                                      THEN (AREA_FRACTION_HEAVY_INDUSTRY_LCZ - $lcz10IndFracMin) / (1 - $lcz10IndFracMin) ELSE
                                                      CASE WHEN LCZ1 = 8 
                                                      THEN (AREA_FRACTION_LOWRISE_TYPO - $lcz8LLRFracMin) / (1 - $lcz8LLRFracMin) END END AS LCZ_UNIQUENESS_VALUE,
                                                      LCZ_EQUALITY_VALUE
                                            FROM $classifiedIndustrialCommercialLcz 
                                        UNION ALL 
                                            SELECT *
                                            FROM $ruralLCZUncertainty""".toString()
            tablesToDrop << classifiedIndustrialCommercialLcz
        } else {
            datasource """ALTER TABLE $classifiedRuralLCZ RENAME TO $ruralAndIndustrialCommercialLCZ""".toString()
        }
        tablesToDrop << classifiedRuralLCZ
        datasource.createIndex(ruralAndIndustrialCommercialLCZ, ID_FIELD_RSU)
        datasource.createIndex(rsuLczIndicators, ID_FIELD_RSU)
        datasource.save(classifiedIndustrialCommercialLcz, "/tmp/test.csv", true)
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
        def urbanLCZExceptIndusColumns = datasource.getColumnNames(urbanLCZExceptIndus)
        urbanLCZExceptIndusColumns.collect { indicCol ->
            if (!indicCol.equalsIgnoreCase(ID_FIELD_RSU) && !indicCol.equalsIgnoreCase(GEOMETRIC_FIELD)) {
                // The values used for normalization ("mean" and "standard deviation") are calculated
                // (for each column) and stored into maps
                centerValue[indicCol] = datasource.firstRow("""SELECT ${normalisationType}(all_val) 
                                AS $CENTER_NAME FROM (SELECT ${indicCol}_low AS all_val FROM $LCZ_classes 
                                WHERE ${indicCol}_low IS NOT NULL UNION ALL SELECT ${indicCol}_upp AS all_val 
                                FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)""".toString())."$CENTER_NAME"
                if (normalisationType == "AVG") {
                    variabilityValue[indicCol] = datasource.firstRow("""SELECT STDDEV_POP(all_val) 
                                    AS $VARIABILITY_NAME FROM (SELECT ${indicCol}_low AS all_val 
                                    FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL 
                                    SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes WHERE ${indicCol}_upp 
                                    IS NOT NULL)""".toString())."$VARIABILITY_NAME"
                } else {
                    variabilityValue[indicCol] = datasource.firstRow("""SELECT MEDIAN(ABS(all_val-
                                    ${centerValue[indicCol]})) AS $VARIABILITY_NAME FROM 
                                    (SELECT ${indicCol}_low AS all_val FROM $LCZ_classes WHERE ${indicCol}_low 
                                    IS NOT NULL UNION ALL SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes 
                                    WHERE ${indicCol}_upp IS NOT NULL)""".toString())."$VARIABILITY_NAME"
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
        tablesToDrop << normalizedRange
        tablesToDrop << LCZ_classes

        // The input indicator values are normalized according to "center" and "variability" values
        datasource """DROP TABLE IF EXISTS $normalizedValues; CREATE TABLE $normalizedValues 
                        AS SELECT $ID_FIELD_RSU, ${queryValuesNorm[0..-3]} FROM $urbanLCZExceptIndus""".toString()

        tablesToDrop << urbanLCZExceptIndus

        // 2. The distance of each RSU to each of the LCZ types is calculated in the normalized interval.
        // The two LCZ types being the closest to the RSU indicators are associated to this RSU. An indicator
        // of uncertainty based on the Perkin Skill Score method is also associated to this "assignment".

        // For each LCZ type...
        def queryLczDistance = ""
        datasource.eachRow("SELECT * FROM $normalizedRange".toString()) { LCZ ->
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
        datasource """DROP TABLE IF EXISTS $distribLczTable;
                            CREATE TABLE $distribLczTable 
                                    AS SELECT   $ID_FIELD_RSU, ${queryLczDistance[0..-2]}
                                    FROM        $normalizedValues;""".toString()

        tablesToDrop << normalizedValues

        // Specific behavior for LCZ type 1 (compact high rise): we suppose it is impossible to have a LCZ1 if
        // the mean average nb of building level in the RSU <10 (we set LCZ1 value to -9999.99 in this case)
        def colDistribTable = datasource.getColumnNames(distribLczTable)
        colDistribTable = colDistribTable.minus(["LCZ1"])
        datasource.createIndex(distribLczTable, ID_FIELD_RSU)
        datasource.createIndex(urbanLCZ, ID_FIELD_RSU)
        datasource """  DROP TABLE IF EXISTS $distribLczTableWithoutLcz1;
                                CREATE TABLE $distribLczTableWithoutLcz1 
                                    AS SELECT   a.${colDistribTable.join(", a.")},
                                                CASE WHEN b.AVG_NB_LEV_AREA_WEIGHTED < 10 
                                                    THEN -9999.99 
                                                    ELSE LCZ1 END AS LCZ1
                                    FROM        $distribLczTable a 
                                    LEFT JOIN   $urbanLCZ b
                                        ON a.$ID_FIELD_RSU=b.$ID_FIELD_RSU;""".toString()
        tablesToDrop << distribLczTable
        tablesToDrop << urbanLCZ
        // The distribution is characterized
        datasource """DROP TABLE IF EXISTS ${prefix prefixName, 'DISTRIBUTION_REPARTITION'}""".toString()
        def resultsDistrib = Geoindicators.GenericIndicators.distributionCharacterization(datasource,
                distribLczTableWithoutLcz1, distribLczTableWithoutLcz1,
                ID_FIELD_RSU,
                ["equality", "uniqueness"], "LEAST",
                true, true, prefixName)

        tablesToDrop << distribLczTableWithoutLcz1
        // Rename the standard indicators into names consistent with the current method (LCZ type...)
        datasource """  ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_COL TO LCZ_PRIMARY;
                                ALTER TABLE $resultsDistrib RENAME COLUMN UNIQUENESS_VALUE TO LCZ_UNIQUENESS_VALUE;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EQUALITY_VALUE TO LCZ_EQUALITY_VALUE;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_COL2 TO LCZ_SECONDARY;
                                ALTER TABLE $resultsDistrib RENAME COLUMN EXTREMUM_VAL TO MIN_DISTANCE;""".toString()

        // Need to replace the string LCZ values by an integer
        datasource.createIndex(resultsDistrib, "lcz_primary")
        datasource.createIndex(resultsDistrib, "lcz_secondary")
        def casewhenQuery1 = ""
        def casewhenQuery2 = ""
        def parenthesis = ""
        correspondenceMap.each { lczString, lczInt ->
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
        tablesToDrop << resultsDistrib
        // Then urban and rural LCZ types are merged into a single table
        datasource """DROP TABLE IF EXISTS $classifiedLcz;
                                CREATE TABLE $classifiedLcz 
                                        AS SELECT   * 
                                        FROM        $distribLczTableInt
                                        UNION ALL   SELECT * FROM $ruralAndIndustrialCommercialLCZ b;""".toString()
        datasource.createIndex(classifiedLcz, ID_FIELD_RSU)

        tablesToDrop << ruralAndIndustrialCommercialLCZ

        // If the input tables contain a geometric field, we add it to the output table
        if (datasource.hasGeometryColumn(rsuLczIndicators)) {
            datasource """DROP TABLE IF EXISTS $outputTableName;
                                    CREATE TABLE $outputTableName
                                            AS SELECT   a.*, b.$GEOMETRIC_FIELD
                                            FROM        $classifiedLcz a
                                            LEFT JOIN   $rsuAllIndicators b
                                            ON          a.$ID_FIELD_RSU=b.$ID_FIELD_RSU""".toString()
        } else if (datasource.hasGeometryColumn(rsuLczIndicators)) {
            datasource """DROP TABLE IF EXISTS $outputTableName;
                                    CREATE TABLE $outputTableName
                                            AS SELECT   a.*, b.$GEOMETRIC_FIELD
                                            FROM        $classifiedLcz a
                                            LEFT JOIN   $rsuLczIndicators b
                                            ON          a.$ID_FIELD_RSU=b.$ID_FIELD_RSU""".toString()
        } else {
            datasource """DROP TABLE IF EXISTS $outputTableName; ALTER TABLE $classifiedLcz RENAME TO $outputTableName;""".toString()
        }
        tablesToDrop << distribLczTableInt
        tablesToDrop << classifiedLcz
        datasource.dropTable(tablesToDrop)
        debug "The LCZ classification has been performed."

        return outputTableName
    } else {
        error "The 'normalisationType' argument is not valid."
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
def createRandomForestModel(JdbcDataSource datasource, String trainingTableName, String varToModel, List explicativeVariables,
                            boolean save,
                            String pathAndFileName, int ntrees, int mtry, String rule = "GINI", int maxDepth,
                            int maxNodes, int nodeSize, double subsample, boolean classif = true) {
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

    def trainingTableColumns = datasource.getColumnNames(trainingTableName)

    //Check if the column names exists
    if (!trainingTableColumns.contains(varToModel)) {
        throw new IllegalArgumentException("The training table should have a column named $varToModel".toString())
    }
    // If needed, select only some specific columns for the training in the dataframe
    def df
    if (explicativeVariables) {
        def tabFin = datasource.getTable(trainingTableName).columns(explicativeVariables).getTable()
        df = DataFrame.of(tabFin)
    } else {
        def tabFin = datasource.getTable(trainingTableName)
        df = DataFrame.of(tabFin)
    }
    def formula = Formula.lhs(varToModel)
    def columnTypes = df.getColumnNamesTypes()
    def dfFactorized = df.omitNullRows()

    // Identify columns being string (thus needed to be factorized)
    columnTypes.each { colName, colType ->
        if (colType == "String") {
            dfFactorized = dfFactorized.factorize(colName)
        }
    }
    // Create the randomForest
    def model
    if (classif) {
        model = RandomForestClassification.fit(formula, dfFactorized, ntrees, mtry, splitRule, maxDepth, maxNodes, nodeSize, subsample)
    } else {
        model = RandomForestRegression.fit(formula, dfFactorized, ntrees, mtry, maxDepth, maxNodes, nodeSize, subsample)
    }


    // Calculate the prediction using the same sample in order to identify what is the
    // data rate that has been well classified
    def prediction = Validation.test(model, dfFactorized)
    def truth
    if (DataType.isDouble(dfFactorized.schema().field(varToModel).type)) {
        truth = dfFactorized.apply(varToModel).toDoubleArray()
        def rmse = RMSE.of(truth, prediction)
        error "The root mean square error is : ${rmse}"
    } else {
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
    return model
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
 * @return a table that contains the result of RfModel
 *
 * @author Jérémy Bernard
 */
String applyRandomForestModel(JdbcDataSource datasource, String explicativeVariablesTableName, String pathAndFileName, String idName,
                              String prefixName) throws Exception {
    debug "Apply a Random Forest model"
    File inputModelFile = new File(pathAndFileName)
    def modelName = FilenameUtils.getBaseName(pathAndFileName)
    def model = getModel(modelName)
    if (!model) {
        if (!inputModelFile.exists()) {
            //We try to find this model in geoclimate
            def modelURL = "https://github.com/orbisgis/geoclimate/raw/master/models/${modelName}.model"
            def localInputModelFile = new File(System.getProperty("user.home") + File.separator + ".geoclimate" + File.separator + modelName + ".model")
            // The model doesn't exist on the local folder we download it
            if (!localInputModelFile.exists()) {
                FileUtils.copyURLToFile(new URL(modelURL), localInputModelFile)
                if (!localInputModelFile.exists()) {
                    throw new IllegalArgumentException("Cannot find any model file to apply the classification tree")
                }
            }
            inputModelFile = localInputModelFile;
        } else {
            if (FilenameUtils.isExtension(pathAndFileName, "model")) {
                modelName = FilenameUtils.getBaseName(pathAndFileName)
            } else {
                throw new IllegalArgumentException("The extension of the model file must be .model")
            }
        }
        def fileInputStream = new FileInputStream(inputModelFile)
        // Load the RandomForest model
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
        // Load the model and recover the name of the variable to model
        def gzipInputStream = new GZIPInputStream(fileInputStream)
        model = xs.fromXML(gzipInputStream)
        putModel(modelName, model)
    }
    if (!model) {
        throw new IllegalArgumentException("Cannot find the required columns to apply the model")
    }

    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, modelName.toLowerCase();

    def varType
    def var2model
    def tree = model.trees[0]
    def modelColumnNames = []
    if (tree instanceof RegressionTree) {
        def response = tree.response
        varType = response.type
        var2model = response.name
        modelColumnNames = tree.schema.fields.collect {
            it.name
        }
    } else {
        def response = tree.tree.response
        varType = response.type
        var2model = response.name
        modelColumnNames = tree.tree.schema.fields.collect {
            it.name
        }
    }
    //Check the column names before building the dataframe and apply the model
    def inputColumns = datasource.getColumnNamesTypes(explicativeVariablesTableName)

    def allowedColumnNames = modelColumnNames.intersect(inputColumns.keySet())
    def notSameColumnNames = allowedColumnNames.size() != modelColumnNames.size()
    if (!allowedColumnNames && notSameColumnNames) {
        error "Cannot find the requiered columns to apply the model"
        return
    }
    def isDouble = false
    if (DataType.isDouble(varType)) {
        isDouble = true
    }
    // Read the table containing the explicative variables as a DataFrame
    datasource.save(explicativeVariablesTableName, "/tmp/utrf.fgb", true)
    def dfNofactorized = DataFrame.of(datasource.getTable("""(SELECT ${modelColumnNames.join(",")}, 
            ${isDouble ? "CAST (0 AS DOUBLE PRECISION) AS " + var2model : "CAST(0 AS INTEGER) AS " + var2model},
            $idName from $explicativeVariablesTableName)""".toString()))

    def df = dfNofactorized
    //Factorize only string field
    df.schema().fields().each { field ->
        if (DataTypes.StringType.equals(field.type)) {
            df = df.factorize(field.name)
        }
    }

    // Remove the id for the application of the randomForest
    def df_var = df.drop(idName.toUpperCase())

    def prediction = Validation.test(model, df_var)
    // We need to add the remove the initial predicted variable in order to not have duplicated...
    df = df.drop(var2model)
    df = df.merge(isDouble ? DoubleVector.of(var2model, prediction) : IntVector.of(var2model, prediction))

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
            def create_table_ = "CREATE TABLE ${tableName} (${idName.toUpperCase()} INTEGER, ${var2model.toUpperCase()} ${isDouble ? "DOUBLE PRECISION" : "INTEGER"})";
            def insertTable = "INSERT INTO ${tableName}  VALUES(?,?)";
            outputconnection.setAutoCommit(false);
            outputconnectionStatement.execute(create_table_.toString());
            preparedStatement = outputconnection.prepareStatement(insertTable.toString());
            long batch_size = 0
            int batchSize = 100
            while (df.next()) {
                def id = df.getString(0)
                def predictedValue = df.getObject(1)
                preparedStatement.setObject(1, id);
                preparedStatement.setObject(2, predictedValue)
                preparedStatement.addBatch();
                batch_size++;
                if (batch_size >= batchSize) {
                    preparedStatement.executeBatch()
                    preparedStatement.clearBatch()
                    batchSize = 0
                }
            }
            if (batch_size > 0) {
                preparedStatement.executeBatch();
            }
        } catch (SQLException e) {
            throw new SQLException("Cannot save the dataframe.", e)
        } finally {
            outputconnection.setAutoCommit(true);
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    } catch (SQLException e) {
        throw new SQLException("Cannot save the dataframe.", e)
    }
    return tableName
}
