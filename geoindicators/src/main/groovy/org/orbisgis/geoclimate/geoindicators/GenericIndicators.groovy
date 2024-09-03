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

import groovy.transform.BaseScript
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.SQLException

@BaseScript Geoindicators geoindicators

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume). Note that for
 * surface fractions it is highly recommended to use the surfaceFractions method (since it considers the potential
 * layer superpositions (building, high vegetation, low vegetation, etc.).
 *
 * @param inputLowerScaleTableName the table name where are stored low scale objects (e.g. buildings)
 * and the id of the upper scale zone (e.g. RSU) they are belonging to
 * @param inputUpperScaleTableName the table of the upper scale where the informations have to be aggregated to (e.g. RSU)
 * @param inputIdUp the ID of the upper scale
 * @param inputVarAndOperations a map containing as key the informations that has to be transformed from the lower to
 * the upper scale and as values a LIST of operation to apply to this information (e.g. ["building_area":["SUM", "NB_DENS"]]).
 * Operations should be in the following list:
 *          --> "SUM": sum the geospatial variables at the upper scale
 *          --> "AVG": average the geospatial variables at the upper scale
 *          --> "STD": population standard deviation of the geospatial variables at the upper scale
 *          --> "GEOM_AVG": average the geospatial variables at the upper scale using a geometric average
 *          --> "DENS": sum the geospatial variables at the upper scale and divide by the area of the upper scale
 *          --> "NB_DENS" : count the number of lower scale objects within the upper scale object and divide by the upper
 *          scale area. NOTE THAT FOR THIS ONE, THE MAP KEY WILL BE USED TO CREATE THE PREFIX NAME OF THE INDICATOR
 *          (e.g. "BUILDING_AREA" in the case of the example given above ==> the indicator will be BUILDING_AREA_NUMBER_DENSITY)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 *
 * @author Jérémy Bernard
 */
String unweightedOperationFromLowerScale(JdbcDataSource datasource, String inputLowerScaleTableName, String inputUpperScaleTableName, String inputIdUp,
                                         String inputIdLow, Map inputVarAndOperations, String prefixName) throws Exception {
    try {
        def GEOMETRIC_FIELD_UP = "the_geom"
        def BASE_NAME = "unweighted_operation_from_lower_scale"
        def SUM = "SUM"
        def AVG = "AVG"
        def GEOM_AVG = "GEOM_AVG"
        def DENS = "DENS"
        def NB_DENS = "NB_DENS"
        def STD = "STD"
        def COLUMN_TYPE_TO_AVOID = ["GEOMETRY", "VARCHAR"]
        def SPECIFIC_OPERATIONS = [NB_DENS]

        debug "Executing Unweighted statistical operations from lower scale"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        datasource.createIndex(inputLowerScaleTableName, inputIdUp)
        datasource.createIndex(inputUpperScaleTableName, inputIdUp)

        def query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

        def columnNamesTypes = datasource.getColumnNamesTypes(inputLowerScaleTableName)
        def filteredColumns = columnNamesTypes.findAll { !COLUMN_TYPE_TO_AVOID.contains(it.value.toUpperCase()) }
        inputVarAndOperations.each { var, operations ->
            // Some operations may not need to use an existing variable thus not concerned by the column filtering
            def filteredOperations = operations - SPECIFIC_OPERATIONS
            if (filteredColumns.containsKey(var.toUpperCase()) | (filteredOperations.isEmpty())) {
                operations.each {
                    def op = it.toUpperCase()
                    switch (op) {
                        case GEOM_AVG:
                            query += "COALESCE(EXP(1.0/COUNT(a.*)*SUM(LOG(a.$var))),0) AS ${op + "_" + var},"
                            break
                        case DENS:
                            query += "COALESCE(SUM(a.$var::float)/ST_AREA(b.$GEOMETRIC_FIELD_UP),0) AS ${var + "_DENSITY"},"
                            break
                        case NB_DENS:
                            query += "COALESCE(COUNT(a.$inputIdLow)/ST_AREA(b.$GEOMETRIC_FIELD_UP),0) AS ${var + "_NUMBER_DENSITY"},"
                            break
                        case SUM:
                            query += "COALESCE(SUM(a.$var::float),0) AS ${op + "_" + var},"
                            break
                        case AVG:
                            query += "COALESCE($op(a.$var::float),0) AS ${op + "_" + var},"
                            break
                        case STD:
                            query += "COALESCE(STDDEV_POP(a.$var::float),0) AS ${op + "_" + var},"
                            break
                        default:
                            break
                    }
                }
            } else {
                debug("""The column $var doesn't exist or should be numeric""")
            }
        }
        query += "b.$inputIdUp FROM $inputLowerScaleTableName a RIGHT JOIN $inputUpperScaleTableName b " +
                "ON a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp"

        datasource.execute(query)

        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot execute the unweighted operation from lower scale", e)
    }
}

/**
 * This process is used to compute weighted average and standard deviation on a specific variable from a lower scale (for
 * example the mean building roof height within a reference spatial unit where the roof height values are weighted
 * by the values of building area)
 *
 * @param inputLowerScaleTableName the table name where are stored low scale objects (e.g. buildings)
 *  and the id of the upper scale zone (e.g. RSU) they are belonging to
 * @param inputUpperScaleTableName the table of the upper scale where the informations have to be aggregated to (e.g. RSU)
 * @param inputIdUp the ID of the upper scale
 * @param inputVarWeightsOperations a map containing as key the informations that has to be transformed from the lower to
 *  the upper scale, and as value a map containing as key the variable that has to be used as weight and as values
 *  a LIST of operation to apply to this couple (information/weight). Note that the allowed operations should be in
 *  the following list:
 *           --> "STD": weighted standard deviation
 *           --> "AVG": weighted mean
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 * @author Erwan Bocher
 */
String weightedAggregatedStatistics(JdbcDataSource datasource, String inputLowerScaleTableName,
                                    String inputUpperScaleTableName, String inputIdUp,
                                    Map inputVarWeightsOperations, String prefixName) throws Exception {
    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def weighted_mean = postfix "weighted_mean"
    try {
        def AVG = "AVG"
        def STD = "STD"
        def BASE_NAME = "weighted_aggregated_statistics"

        debug "Executing Weighted statistical operations from lower scale"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        datasource.createIndex(inputLowerScaleTableName, inputIdUp)
        datasource.createIndex(inputUpperScaleTableName, inputIdUp)

        // The weighted mean is calculated in all cases since it is useful for the STD calculation
        def weightedMeanQuery = "DROP TABLE IF EXISTS $weighted_mean; " +
                "CREATE TABLE $weighted_mean($inputIdUp INTEGER,"
        def nameAndType = ""
        def weightedMean = ""
        inputVarWeightsOperations.each { var, weights ->
            weights.each { weight, operations ->
                nameAndType += "weighted_avg_${var}_$weight DOUBLE PRECISION DEFAULT 0,"
                weightedMean += "CASE WHEN SUM(a.$weight)=0  THEN 0 ELSE COALESCE(SUM(a.$var*a.$weight) / SUM(a.$weight),0) END AS weighted_avg_${var}_$weight,"
            }
        }
        weightedMeanQuery += nameAndType[0..-2] + ") AS (SELECT b.$inputIdUp, ${weightedMean[0..-2]}" +
                " FROM $inputLowerScaleTableName a RIGHT JOIN $inputUpperScaleTableName b " +
                "ON a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp)"
        datasource weightedMeanQuery.toString()

        // The weighted std is calculated if needed and only the needed fields are returned
        def weightedStdQuery = "CREATE INDEX IF NOT EXISTS id_lcorr ON $weighted_mean ($inputIdUp); " +
                "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.$inputIdUp,"
        inputVarWeightsOperations.each { var, weights ->
            weights.each { weight, operations ->
                // The operation names are transformed into upper case
                operations.replaceAll { it.toUpperCase() }
                if (operations.contains(AVG)) {
                    weightedStdQuery += "COALESCE(b.weighted_avg_${var}_$weight,0) AS avg_${var}_${weight}_weighted,"
                }
                if (operations.contains(STD)) {
                    weightedStdQuery += "CASE WHEN SUM(a.$weight)=0 THEN 0 ELSE COALESCE(POWER(SUM(a.$weight*POWER(a.$var-b.weighted_avg_${var}_$weight,2))/" +
                            "SUM(a.$weight),0.5),0) END AS std_${var}_${weight}_weighted,"
                }
            }
        }
        weightedStdQuery = weightedStdQuery[0..-2] + " FROM $inputLowerScaleTableName a RIGHT JOIN $weighted_mean b " +
                "ON a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp"

        datasource.execute(weightedStdQuery)
        // The temporary tables are deleted
        datasource.dropTable(weighted_mean)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot execute the weighted aggregated statistics operation", e)
    } finally {
        // The temporary tables are deleted
        datasource.dropTable(weighted_mean)
    }
}

/**
 * This process extract geometry properties.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputFields Fields to conserved from the input table in the output table
 * @param inputTableName The name of the input ITable where are stored the geometries to apply the geometry operations
 * @param operations Operations that have to be applied. These operations should be in the following list (see http://www.h2gis.org/docs/dev/functions/):
 * ["st_geomtype","st_srid", "st_length","st_perimeter","st_area", "st_dimension", "st_coorddim", "st_num_geoms",
 * "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Erwan Bocher, CNRS
 */
String geometryProperties(JdbcDataSource datasource, String inputTableName, List inputFields,
                          List operations, String prefixName) throws Exception {
    try {
        def GEOMETRIC_FIELD = "the_geom"
        def OPS = ["st_geomtype", "st_srid", "st_length", "st_perimeter", "st_area", "st_dimension",
                   "st_coorddim", "st_num_geoms", "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]
        def BASE_NAME = "geometry_properties"

        debug "Executing Geometry properties"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        def query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

        // The operation names are transformed into lower case
        operations.replaceAll { it.toLowerCase() }
        operations.each {
            if (OPS.contains(it)) {
                query += "$it($GEOMETRIC_FIELD) as ${it.substring(3)},"
            }
        }
        query += "${inputFields.join(",")} from $inputTableName"

        datasource.execute(query)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the geometry properties", e)
    }
}

/**
 * This process is used to compute (within a block or a RSU) the main building direction and
 * indicators that qualify the repartition of the direction distribution. This second indicator may be:
 * - an indicator of general inequality: the Perkins Skill Score is calculated (Perkins et al., 2007)
 * - an indicator of uniqueness: the weight of the first main direction is divided by
 * "the weight of the second main direction + the weight of the first main direction"
 * The building direction distribution is calculated according to the length of the building SMBR sides (width and length).
 * Note that the distribution has an "angle_range_size" interval that has to be defined by the user (default 15).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildingTableName the name of the input ITable where are stored the geometry field, the building and the block/RSU ID
 * @param tableUp the name of the table where are stored all objects of the upper scale (usually RSU or block)
 * @param inputIdUp the ID of the upper scale
 * @param angleRangeSize the range size (in °) of each interval angle used to calculate the distribution of building direction
 * (should be a divisor of 180 - default 15°)
 * @param distribIndicator List containing the type of indicator to calculate to define the repartition of the distribution
 * (default ["inequality", "uniqueness"])
 *      --> "inequality": the Perkins Skill Score is calculated
 *      --> "uniqueness": the weight of the first main direction is divided by
 *                      "the weight of the second main direction + the weight of the first main direction"
 * @param prefixName String use as prefix to name the output table
 *
 * Reference:
 * -> Perkins, S. E., Pitman, A. J., Holbrook, N. J., & McAneney, J. (2007). Evaluation of the AR4 climate models’
 * simulated daily maximum temperature, minimum temperature, and precipitation over Australia using probability
 * density functions. Journal of climate, 20(17), 4356-4376.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String buildingDirectionDistribution(JdbcDataSource datasource, String buildingTableName,
                                     String tableUp, String inputIdUp, float angleRangeSize = 15f
                                     , List distribIndicator = ["equality", "uniqueness"], String prefixName) throws Exception {
    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def build_min_rec = postfix "build_min_rec"
    def build_dir360 = postfix "build_dir360"
    def build_dir180 = postfix "build_dir180"
    def build_dir_dist = postfix "build_dir_dist"

    try {
        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD_BU = "id_build"
        def INEQUALITY = "BUILDING_DIRECTION_EQUALITY"
        def UNIQUENESS = "BUILDING_DIRECTION_UNIQUENESS"
        def BASENAME = "MAIN_BUILDING_DIRECTION"

        debug "Executing Perkins skill score building direction"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASENAME

        // Test whether the angleRangeSize is a divisor of 180°
        if ((180 % angleRangeSize) == 0 && (180 / angleRangeSize) > 1) {
            def med_angle = angleRangeSize / 2

            // The minimum diameter of the minimum rectangle is created for each building
            datasource.execute("""DROP TABLE IF EXISTS $build_min_rec; CREATE TABLE $build_min_rec AS 
                        SELECT $ID_FIELD_BU, $inputIdUp, ST_MINIMUMDIAMETER(ST_MINIMUMRECTANGLE($GEOMETRIC_FIELD)) 
                        AS the_geom FROM $buildingTableName;""")

            datasource.createIndex(buildingTableName, "id_build")

            // The length and direction of the smallest and the longest sides of the Minimum rectangle are calculated
            datasource.execute("""CREATE INDEX IF NOT EXISTS id_bua ON $build_min_rec ($ID_FIELD_BU);
                        DROP TABLE IF EXISTS $build_dir360; CREATE TABLE $build_dir360 AS 
                        SELECT a.$inputIdUp, ST_LENGTH(a.the_geom) AS LEN_L, 
                        ST_AREA(b.the_geom)/ST_LENGTH(a.the_geom) AS LEN_H, 
                        ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(a.the_geom), ST_ENDPOINT(a.the_geom)))) AS ANG_L, 
                        ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(a.the_geom, pi()/2)), 
                        ST_ENDPOINT(ST_ROTATE(a.the_geom, pi()/2))))) AS ANG_H FROM $build_min_rec a  
                        LEFT JOIN $buildingTableName b ON a.$ID_FIELD_BU=b.$ID_FIELD_BU""")

            // The angles are transformed in the [0, 180]° interval
            datasource.execute("""DROP TABLE IF EXISTS $build_dir180; CREATE TABLE $build_dir180 AS 
                        SELECT $inputIdUp, LEN_L, LEN_H, CASEWHEN(ANG_L>=180, ANG_L-180, ANG_L) AS ANG_L, 
                        CASEWHEN(ANG_H>180, ANG_H-180, ANG_H) AS ANG_H FROM $build_dir360""")

            datasource.execute("CREATE INDEX ON $build_dir180 ($inputIdUp)")

            // The query aiming to create the building direction distribution is created
            def sqlQueryDist = "DROP TABLE IF EXISTS $build_dir_dist; CREATE TABLE $build_dir_dist AS SELECT "
            for (int i = angleRangeSize; i <= 180; i += angleRangeSize) {
                def nameAngle = (i - med_angle).toString().replace(".", "_")
                sqlQueryDist += "SUM(CASEWHEN(ANG_L>=${i - angleRangeSize} AND ANG_L<$i, LEN_L, " +
                        "CASEWHEN(ANG_H>=${i - angleRangeSize} AND ANG_H<$i, LEN_H, 0))) AS ANG$nameAngle, "
            }
            sqlQueryDist += "$inputIdUp FROM $build_dir180 GROUP BY $inputIdUp;"

            // The query is executed
            datasource sqlQueryDist.toString()

            // The main building direction and indicators characterizing the distribution are calculated
            def resultsDistrib = distributionCharacterization(datasource, build_dir_dist,
                    tableUp, inputIdUp, distribIndicator, "GREATEST", prefixName)

            // Rename the standard indicators into names consistent with the current method (building direction...)
            datasource """DROP TABLE IF EXISTS $outputTableName;
                                    ALTER TABLE $resultsDistrib RENAME TO $outputTableName;
                                    ALTER TABLE $outputTableName RENAME COLUMN EXTREMUM_COL TO $BASENAME;
                                    ALTER TABLE $outputTableName RENAME COLUMN UNIQUENESS_VALUE TO $UNIQUENESS;
                                    ALTER TABLE $outputTableName RENAME COLUMN EQUALITY_VALUE TO $INEQUALITY;""".toString()
            // The temporary tables are deleted
            datasource """DROP TABLE IF EXISTS $build_min_rec, $build_dir360, $build_dir180, 
                        $build_dir_dist;""".toString()
            /*
            if (distribIndicator.contains("uniqueness")){
            // Reorganise the distribution Table (having the same number of column than the number
            // of direction of analysis) into a simple two column table (ID and SURF)
            def sqlQueryUnique = "DROP TABLE IF EXISTS $build_dir_bdd; CREATE TABLE $build_dir_bdd AS SELECT "
            def columnNames = datasource.getTable(build_dir_dist).columns
            columnNames.remove(inputIdUp)
            for (col in columnNames.take(columnNames.size() - 1)){
                sqlQueryUnique += "$inputIdUp, $col AS SURF FROM $build_dir_dist UNION ALL SELECT "
            }
            sqlQueryUnique += """$inputIdUp, ${columnNames[-1]} AS SURF FROM $build_dir_dist;
                                CREATE INDEX ON $build_dir_bdd USING BTREE($inputIdUp);
                                CREATE INDEX ON $build_dir_bdd USING BTREE(SURF);"""
            datasource sqlQueryUnique

            def sqlQueryLast = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                                "SELECT b.$inputIdUp, a.main_building_direction,"

            if (distribIndicator.contains("inequality")) {
                sqlQueryLast += " a.$INEQUALITY, "
            }
            sqlQueryLast += """a.max_surf/(MAX(b.SURF)+a.max_surf) AS $UNIQUENESS
                               FROM         $build_perk_fin a
                               RIGHT JOIN   $build_dir_bdd b
                               ON           a.$inputIdUp = b.$inputIdUp
                               WHERE        b.SURF < a.max_surf
                               GROUP BY     b.$inputIdUp;"""

            datasource "CREATE INDEX ON $build_perk_fin ($inputIdUp);"
            datasource sqlQueryLast
        }
        else{
            datasource "ALTER TABLE $build_perk_fin RENAME TO $outputTableName;"
        }
        */
            return outputTableName
        }
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the building direction distribution", e)
    } finally {
        // The temporary tables are deleted
        datasource """DROP TABLE IF EXISTS $build_min_rec, $build_dir360, $build_dir180, 
                        $build_dir_dist;""".toString()
    }

}

/**
 * This process is used to compute indicators that qualify the repartition of a distribution. First, it calculates
 * the lowest or the greatest class of the distribution. Then it calculates the shape of the distribution through
 * two possible indicators:
 * - an indicator of general equality: the Perkins Skill Score is calculated (Perkins et al., 2007)
 * - an indicator of uniqueness: the weight of the greatest (or the lowest) class is divided by
 * "the weight of the second greatest (or lowest) class + the weight of the greatest (or the lowest) class"
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param distribTableName the name of the input ITable where are stored an ID of the geometry to consider and each column
 * of the distribution to characterize
 * @param initialTable the name of the input ITable where are stored all the objects where the informations of
 * distribution characterization are aggregated
 * @param inputId The ID of the input data
 * @param distribIndicator List containing the type of indicator to calculate to define the repartition of the distribution
 * (default ["equality", "uniqueness"])
 *      --> "equality": the Perkins Skill Score is calculated
 *      --> "uniqueness": the weight of the greatest (or the lowest) class is divided by
 *                      "the weight of the second greatest (or lowest) class + the weight of the greatest (or the lowest) class"
 * @param extremum String indicating the kind of extremum to calculate (default "GREATEST")
 *      --> "LEAST"
 *      --> "GREATEST"
 * @param keep2ndCol Whether or not the 2nd extremum value should be conserved (default False)
 * @param keepColVal Whether or not the value of the extremum col should be conserved (default False)
 * @param prefixName String use as prefix to name the output table
 *
 * Reference:
 * -> Perkins, S. E., Pitman, A. J., Holbrook, N. J., & McAneney, J. (2007). Evaluation of the AR4 climate models’
 * simulated daily maximum temperature, minimum temperature, and precipitation over Australia using probability
 * density functions. Journal of climate, 20(17), 4356-4376.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String distributionCharacterization(JdbcDataSource datasource, String distribTableName,
                                    String initialTable, String inputId,
                                    List distribIndicator = ["equality", "uniqueness"], String extremum = "GREATEST",
                                    boolean keep2ndCol = false, boolean keepColVal = false, String prefixName) throws Exception {
    // Create temporary tables
    def outputTableMissingSomeObjects = postfix "output_table_missing_some_objects"
    def distribTableNameNoNull = postfix "distrib_table_name_no_null"
    try {
        def EQUALITY = "EQUALITY_VALUE"
        def UNIQUENESS = "UNIQUENESS_VALUE"
        def EXTREMUM_COL = "EXTREMUM_COL"
        def EXTREMUM_COL2 = "EXTREMUM_COL2"
        def EXTREMUM_VAL = "EXTREMUM_VAL"
        def BASENAME = "DISTRIBUTION_REPARTITION"
        def GEOMETRY_FIELD = "THE_GEOM"

        debug "Executing equality and uniqueness indicators"

        if (extremum.toUpperCase() == "GREATEST" || extremum.toUpperCase() == "LEAST") {
            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASENAME

            // Get all columns from the distribution table and remove the geometry column if exists
            def allColumns = datasource.getColumnNames(distribTableName)
            if (allColumns.contains(GEOMETRY_FIELD)) {
                allColumns -= GEOMETRY_FIELD
            }
            // Get the distribution columns and the number of columns
            def distribColumns = allColumns.minus(inputId.toUpperCase())
            def nbDistCol = distribColumns.size()

            if (distribColumns.size() == 0) {
                throw new IllegalArgumentException("Any columns to compute the distribution from the table $distribTableName".toString())
            }

            def idxExtrem = nbDistCol - 1
            def idxExtrem_1 = nbDistCol - 2
            if (extremum.toUpperCase() == "LEAST") {
                idxExtrem = 0
                idxExtrem_1 = 1
            }

            def queryCoalesce = ""

            // Delete rows having null values (and remove the geometry field if exists)
            datasource """  DROP TABLE IF EXISTS $distribTableNameNoNull;
                                CREATE TABLE $distribTableNameNoNull 
                                    AS SELECT ${allColumns.join(",")} 
                                    FROM $distribTableName 
                                    WHERE ${distribColumns.join(" IS NOT NULL AND ")} IS NOT NULL""".toString()

            if (distribIndicator.contains("equality") && !distribIndicator.contains("uniqueness")) {
                def queryCreateTable = """CREATE TABLE $outputTableMissingSomeObjects($inputId integer, 
                                                                    $EQUALITY DOUBLE PRECISION,
                                                                    $EXTREMUM_COL VARCHAR)"""
                // If the second extremum col should be conserved
                if (keep2ndCol) {
                    queryCreateTable = "${queryCreateTable[0..-2]}, $EXTREMUM_COL2 VARCHAR)"
                }
                // If the value of the extremum column should be conserved
                if (keepColVal) {
                    queryCreateTable = "${queryCreateTable[0..-2]}, $EXTREMUM_VAL DOUBLE PRECISION)"
                }
                datasource queryCreateTable.toString()
                // Will insert values by batch of 100 in the table
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow("SELECT * FROM $distribTableNameNoNull".toString()) { row ->
                        def rowMap = row.toRowResult()
                        def id_rsu = rowMap."$inputId"
                        rowMap.remove(inputId.toUpperCase())
                        def sortedMap = rowMap.sort { it.value }
                        // We want to get rid of some of the values identified as -9999.99
                        while (sortedMap.values().remove(-9999.99 as double));
                        def queryInsert = """INSERT INTO $outputTableMissingSomeObjects 
                                                VALUES ($id_rsu, ${getEquality(sortedMap, nbDistCol)},
                                                        '${sortedMap.keySet()[idxExtrem]}')"""
                        // If the second extremum col should be conserved
                        if (keep2ndCol) {
                            queryInsert = "${queryInsert[0..-2]}, '${sortedMap.keySet()[idxExtrem_1]}')"
                        }
                        // If the value of the extremum column should be conserved
                        if (keepColVal) {
                            queryInsert = "${queryInsert[0..-2]}, ${sortedMap.values()[idxExtrem]})"
                        }
                        stmt.addBatch queryInsert.toString()
                    }
                }
                queryCoalesce += """    COALESCE(a.$EQUALITY, -1) AS $EQUALITY,
                                            COALESCE(a.$EXTREMUM_COL, 'unknown') AS $EXTREMUM_COL,"""

            } else if (!distribIndicator.contains("equality") && distribIndicator.contains("uniqueness")) {
                def queryCreateTable = """CREATE TABLE $outputTableMissingSomeObjects($inputId integer, 
                                                                    $UNIQUENESS DOUBLE PRECISION,
                                                                    $EXTREMUM_COL VARCHAR)"""
                // If the second extremum col should be conserved
                if (keep2ndCol) {
                    queryCreateTable = "${queryCreateTable[0..-2]}, $EXTREMUM_COL2 VARCHAR)"
                }
                // If the value of the extremum column should be conserved
                if (keepColVal) {
                    queryCreateTable = "${queryCreateTable[0..-2]}, $EXTREMUM_VAL DOUBLE PRECISION)"
                }

                datasource queryCreateTable.toString()
                // Will insert values by batch of 100 in the table
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow("SELECT * FROM $distribTableNameNoNull".toString()) { row ->
                        def rowMap = row.toRowResult()
                        def id_rsu = rowMap."$inputId"
                        rowMap.remove(inputId.toUpperCase())
                        def sortedMap = rowMap.sort { it.value }
                        // We want to get rid of some of the values identified as -9999.99
                        while (sortedMap.values().remove(-9999.99 as double));
                        def queryInsert = """INSERT INTO $outputTableMissingSomeObjects 
                                                VALUES ($id_rsu, ${getUniqueness(sortedMap, idxExtrem, idxExtrem_1)},
                                                        '${sortedMap.keySet()[idxExtrem]}')"""
                        // If the second extremum col should be conserved
                        if (keep2ndCol) {
                            queryInsert = "${queryInsert[0..-2]}, '${sortedMap.keySet()[idxExtrem_1]}')"
                        }
                        // If the value of the extremum column should be conserved
                        if (keepColVal) {
                            queryInsert = "${queryInsert[0..-2]}, ${sortedMap.values()[idxExtrem]})"
                        }
                        stmt.addBatch queryInsert.toString()
                    }
                }
                queryCoalesce += """    COALESCE(a.$UNIQUENESS, -1) AS $UNIQUENESS,
                                            COALESCE(a.$EXTREMUM_COL, 'unknown') AS $EXTREMUM_COL,"""
            } else if (distribIndicator.contains("equality") && distribIndicator.contains("uniqueness")) {
                def queryCreateTable = """CREATE TABLE $outputTableMissingSomeObjects($inputId integer, 
                                                                    $EQUALITY DOUBLE PRECISION,
                                                                    $UNIQUENESS DOUBLE PRECISION,
                                                                    $EXTREMUM_COL VARCHAR)"""
                // If the second extremum col should be conserved
                if (keep2ndCol) {
                    queryCreateTable = "${queryCreateTable[0..-2]}, $EXTREMUM_COL2 VARCHAR)"
                }
                // If the value of the extremum column should be conserved
                if (keepColVal) {
                    queryCreateTable = "${queryCreateTable[0..-2]}, $EXTREMUM_VAL DOUBLE PRECISION)"
                }

                datasource queryCreateTable.toString()

                // Will insert values by batch of 100 in the table
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow("SELECT * FROM $distribTableNameNoNull".toString()) { row ->
                        def rowMap = row.toRowResult()
                        def id_rsu = rowMap."$inputId"
                        def sortedMap = rowMap.findAll { it.key.toLowerCase() != inputId && (it.value != -9999.99) }.sort { it.value }
                        def queryInsert = """INSERT INTO $outputTableMissingSomeObjects 
                                                VALUES ($id_rsu, ${getEquality(sortedMap, nbDistCol)},
                                                        ${getUniqueness(sortedMap, idxExtrem, idxExtrem_1)},
                                                        '${sortedMap.keySet()[idxExtrem]}')"""
                        // If the second extremum col should be conserved
                        if (keep2ndCol) {
                            queryInsert = "${queryInsert[0..-2]}, '${sortedMap.keySet()[idxExtrem_1]}')"
                        }
                        // If the value of the extremum column should be conserved
                        if (keepColVal) {
                            queryInsert = "${queryInsert[0..-2]}, ${sortedMap.values()[idxExtrem]})"
                        }
                        stmt.addBatch queryInsert.toString()
                    }
                }
                queryCoalesce += """    COALESCE(a.$EQUALITY, -1) AS $EQUALITY,
                                            COALESCE(a.$UNIQUENESS, -1) AS $UNIQUENESS,
                                                                            COALESCE(a.$EXTREMUM_COL, 'unknown') AS $EXTREMUM_COL,"""
            }
            // If the second extremum col should be conserved
            if (keep2ndCol) {
                queryCoalesce += "COALESCE(a.$EXTREMUM_COL2, 'unknown') AS $EXTREMUM_COL2,  "
            }
            if (keepColVal) {
                queryCoalesce += "COALESCE(a.$EXTREMUM_VAL, -1) AS $EXTREMUM_VAL,  "
            }
            // Set to default value (for example if we characterize the building direction in a RSU having no building...)
            datasource.createIndex(outputTableMissingSomeObjects, inputId)
            datasource.createIndex(initialTable, inputId)
            datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
                                CREATE TABLE $outputTableName 
                                    AS SELECT       $queryCoalesce
                                                    b.$inputId 
                                        FROM $outputTableMissingSomeObjects a RIGHT JOIN $initialTable b
                                        ON a.$inputId = b.$inputId;
                                        """)
            datasource.execute("""DROP TABLE IF EXISTS $outputTableMissingSomeObjects, $distribTableNameNoNull""")

            return outputTableName
        } else {
            throw new SQLException("The 'extremum' input parameter should be equal to 'GREATEST' or 'LEAST'")
        }
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the distribution characterization", e)
    } finally {
        datasource.execute("""DROP TABLE IF EXISTS $outputTableMissingSomeObjects, $distribTableNameNoNull""")
    }
}

/**
 * This function calculates the UNIQUENESS indicator of a distribution for a given RSU
 * @param myMap The row of the table to examine
 * @param idxExtrem when the row is sorted by ascending values, id of the extremum value to get
 * @param idxExtrem_1 when the row is sorted by ascending values, id of the second extremum value to get
 * @return A double : the value of the UNIQUENESS indicator for this RSU
 */
static Double getUniqueness(def myMap, def idxExtrem, def idxExtrem_1) {
    def extrem = myMap.values()[idxExtrem]
    def extrem_1 = myMap.values()[idxExtrem_1]
    return extrem + extrem_1 > 0 ? Math.abs(extrem - extrem_1) / (extrem + extrem_1) : null
}

/**
 * This function calculates the EQUALITY indicator of a distribution for a given RSU
 * @param myMap The row of the table to examine
 * @param nbDistCol the number of columns of the distribution
 * @return A double : the value of the EQUALITY indicator for this RSU
 */
static Double getEquality(def myMap, def nbDistCol) {
    def sum = myMap.values().sum()
    def equality = 0
    myMap.values().each { it ->
        equality += Math.min(it, sum / nbDistCol)
    }

    return sum == 0 ? null : equality / sum
}

/**
 * This process is used to compute the area proportion of a certain type within a given object (e.g. a proportion
 * of industrial buildings within all buildings of a RSU). Note that for surface fractions within a given surface
 * you should use the surfaceFractions method.
 *
 * @param inputTableName the table name where are stored the objects having different types to characterize
 * @param idField ID of the scale used for the 'GROUP BY'
 * @param inputUpperTableName the name of the upper scale table (where is performed the group by - e.g. RSU if building type
 * must be gathered at RSU scale to have the proportion of industrial, residential, etc.)
 * @param typeFieldName The name of the field where is stored the type of the object
 * @param areaTypeAndComposition Types that should be calculated and objects included in this type
 * (e.g. for building table could be: ["residential": ["residential", "detached"]])
 * @param floorAreaTypeAndComposition (ONLY FOR BUILDING OBJECTS) Types that should be calculated and objects included in this type
 * (e.g. for building table could be: ["residential": ["residential", "detached"]])
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 *
 * @author Jérémy Bernard
 */
String typeProportion(JdbcDataSource datasource, String inputTableName, String idField, String typeFieldName,
                      String inputUpperTableName, Map areaTypeAndComposition, Map floorAreaTypeAndComposition,
                      String prefixName) throws Exception {
    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def caseWhenTab = postfix "case_when_tab"
    def outputTableWithNull = postfix "output_table_with_null"
    try {
        def GEOMETRIC_FIELD_LOW = "the_geom"
        def BASE_NAME = "type_proportion"
        def NB_LEV = "nb_lev"

        debug "Executing typeProportion"

        if (areaTypeAndComposition || floorAreaTypeAndComposition) {
            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASE_NAME

            // Define the pieces of query according to each type of the input table
            def queryCalc = []
            def queryCaseWh = []
            // For the area fractions
            if (areaTypeAndComposition) {
                queryCaseWh += " ST_AREA($GEOMETRIC_FIELD_LOW) AS AREA "
                areaTypeAndComposition.forEach { type, compo ->
                    queryCaseWh<< "CASE WHEN $typeFieldName='${compo.join("' OR $typeFieldName='")}' THEN ST_AREA($GEOMETRIC_FIELD_LOW) END AS AREA_${type}"
                    queryCalc << "CASE WHEN SUM(AREA)=0 THEN 0 ELSE SUM(AREA_${type})/SUM(AREA) END AS AREA_FRACTION_${type}"
                }
            }

            // For the floor area fractions in case the objects are buildings
            if (floorAreaTypeAndComposition) {
                queryCaseWh << "ST_AREA($GEOMETRIC_FIELD_LOW)*$NB_LEV AS FLOOR_AREA"
                floorAreaTypeAndComposition.forEach { type, compo ->
                    queryCaseWh << "CASE WHEN $typeFieldName='${compo.join("' OR $typeFieldName='")}' THEN ST_AREA($GEOMETRIC_FIELD_LOW)*$NB_LEV END AS FLOOR_AREA_${type}"
                    queryCalc << "CASE WHEN SUM(FLOOR_AREA) =0 THEN 0 ELSE SUM(FLOOR_AREA_${type})/SUM(FLOOR_AREA) END AS FLOOR_AREA_FRACTION_${type} "
                }
            }

            // Calculates the surface of each object depending on its type
            datasource.execute """DROP TABLE IF EXISTS $caseWhenTab;
                                    CREATE TABLE $caseWhenTab 
                                            AS SELECT $idField
                                                        ${!queryCaseWh.isEmpty()?","+queryCaseWh.join(","):""} 
                                            FROM $inputTableName""".toString()

            datasource.createIndex(caseWhenTab, idField)


            // Calculate the proportion of each type
            datasource.execute("""DROP TABLE IF EXISTS $outputTableWithNull;
                                    CREATE TABLE $outputTableWithNull 
                                            AS SELECT $idField ${!queryCalc.isEmpty()?","+queryCalc.join(","):""}
                                            FROM $caseWhenTab GROUP BY $idField""")

            // Set 0 as default value (for example if we characterize the building type in a RSU having no building...)
            def allFinalCol = datasource.getColumnNames(outputTableWithNull)
            allFinalCol = allFinalCol.minus([idField.toUpperCase()])
            datasource.createIndex(inputUpperTableName, idField)
            datasource.createIndex(outputTableWithNull, idField)
            def pieceOfQuery = []
            allFinalCol.each { col ->
                pieceOfQuery << "COALESCE(a.$col, 0) AS $col "
            }
            datasource """DROP TABLE IF EXISTS $outputTableName;
                                CREATE TABLE $outputTableName 
                                    AS SELECT       ${!pieceOfQuery.isEmpty()?pieceOfQuery.join(",")+",":""}
                                                    b.$idField 
                                        FROM $outputTableWithNull a RIGHT JOIN $inputUpperTableName b
                                        ON a.$idField = b.$idField;
                                        """.toString()
            datasource.execute """DROP TABLE IF EXISTS $outputTableWithNull, $caseWhenTab""".toString()
            return outputTableName
        } else {
            throw new SQLException("'floorAreaTypeAndComposition' or 'areaTypeAndComposition' arguments should be a Map " +
                    "with at least one combination key-value")
        }
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the type proportion", e)
    } finally {
        datasource.execute """DROP TABLE IF EXISTS $outputTableWithNull, $caseWhenTab""".toString()
    }
}


/**
 * This process is used to join building, block and RSU tables either at building scale or at RSU scale. For both scales
 * building indicators are averaged at RSU scale. If the 'targetedScale'="RSU", block indicators are also averaged at RSU scale.
 *
 * @param buildingTable the building table name
 * @param blockTable the block table name (default "")
 * @param rsuTable the rsu table name
 * @param targetedScale The scale of the resulting table (either "RSU" or "BUILDING", default "RSU")
 * @param operationsToApply Operations (using 'unweightedOperationFromLowerScale' method) to apply
 * to building and block in case 'targetedScale'="RSU" (default ["AVG", "STD"])
 * @param areaTypeAndComposition Types that should be calculated and objects included in this type
 * (e.g. for building table could be: ["residential": ["residential", "detached"]])
 * @param floorAreaTypeAndComposition (ONLY FOR BUILDING OBJECTS) Types that should be calculated and objects included in this type
 * (e.g. for building table could be: ["residential": ["residential", "detached"]])
 * @param prefixName String use as prefix to name the output table
 * @param removeNull If true, remove buildings having no RSU value (null value for id_rsu) (default true)
 *
 * @return A database table name.
 *
 * @author Jérémy Bernard
 */
String gatherScales(JdbcDataSource datasource, String buildingTable, String blockTable, String rsuTable,
                    String targetedScale = "RSU", List operationsToApply = ["AVG", "STD"],
                    boolean removeNull = true, String prefixName) throws Exception {
    // Temporary tables that will be deleted at the end of the process
    def finalScaleTableName = postfix "final_before_join"
    def scale1ScaleFin = postfix "scale1_scale_fin"
    def buildIndicRsuScale
    try {
        // List of columns to remove from the analysis in building and block tables
        def BLOCK_COL_TO_REMOVE = ["THE_GEOM", "ID_RSU", "ID_BLOCK", "MAIN_BUILDING_DIRECTION"]
        def BUILD_COL_TO_REMOVE = ["THE_GEOM", "ID_RSU", "ID_BUILD", "ID_BLOCK", "ID_ZONE", "NB_LEV", "ZINDEX", "MAIN_USE", "TYPE", "ROOF_SHAPE", "ID_SOURCE"]
        def BASE_NAME = "all_scales_table"

        debug """ Executing the gathering of scales (to building or to RSU scale)"""

        if ((targetedScale.toUpperCase() == "RSU") || (targetedScale.toUpperCase() == "BUILDING")) {


            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASE_NAME

            // Some tables will be needed to call only some specific columns
            def listblockFinalRename = []

            def blockIndicFinalScale = blockTable
            def idbuildForMerge
            def idBlockForMerge

            // Add operations to compute at RSU scale to each indicator of the building scale
            def inputVarAndOperationsBuild = [:]
            def buildIndicatorsColumns = datasource.getColumnNames(buildingTable)
            for (col in buildIndicatorsColumns) {
                if (!BUILD_COL_TO_REMOVE.contains(col)) {
                    inputVarAndOperationsBuild[col] = operationsToApply
                }
            }
            // Calculate building indicators averaged at RSU scale
            buildIndicRsuScale = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(datasource, buildingTable,
                    rsuTable, "id_rsu", "id_build", inputVarAndOperationsBuild,
                    "bu")

            // To avoid crashes of the join due to column duplicate, need to prefix some names
            def buildRsuCol2Rename = datasource.getColumnNames(buildIndicRsuScale)
            def listBuildRsuRename = []
            for (col in buildRsuCol2Rename) {
                if (col != "ID_BUILD" && col != "ID_BLOCK" && col != "ID_RSU" && col != "THE_GEOM") {
                    listBuildRsuRename.add("a.$col AS build_$col")
                }
            }

            // Special processes if the scale of analysis is RSU
            if (targetedScale.toUpperCase() == "RSU") {
                // Calculate building average and variance at RSU scale from each indicator of the block scale
                def inputVarAndOperationsBlock = [:]
                def blockIndicators = datasource.getColumnNames(blockTable)
                for (col in blockIndicators) {
                    if (!BLOCK_COL_TO_REMOVE.contains(col)) {
                        inputVarAndOperationsBlock[col] = operationsToApply
                    }
                }
                // Calculate block indicators averaged at RSU scale
                blockIndicFinalScale = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale(datasource,
                        blockTable, rsuTable, "id_rsu", "id_block",
                        inputVarAndOperationsBlock, "bl")

                // To avoid crashes of the join due to column duplicate, need to prefix some names
                def blockRsuCol2Rename = datasource.getColumnNames(blockIndicFinalScale)
                for (col in blockRsuCol2Rename) {
                    if (col != "ID_BLOCK" && col != "ID_RSU" && col != "THE_GEOM") {
                        listblockFinalRename.add("b.$col AS block_$col")
                    }
                }

                // Define generic name whatever be the 'targetedScale'
                finalScaleTableName = rsuTable
                // Useful for merge between buildings and rsu tables
                idbuildForMerge = "id_rsu"
                idBlockForMerge = "id_rsu"
                // Useful if the classif is a regression
                idName = "id_rsu"
            }

            // Special processes if the scale of analysis is building
            else if (targetedScale.toUpperCase() == "BUILDING") {
                // Need to join RSU and building tables
                def listRsuCol = datasource.getColumnNames(rsuTable)
                def listRsuRename = []
                for (col in listRsuCol) {
                    if (col != "ID_RSU" && col != "THE_GEOM") {
                        listRsuRename.add("a.$col AS rsu_$col")
                    }
                }
                def listBuildRename = []
                for (col in buildIndicatorsColumns) {
                    if (col != "ID_RSU" && col != "ID_BLOCK" && col != "ID_BUILD" && col != "THE_GEOM") {
                        listBuildRename.add("b.$col AS build_$col")
                    } else {
                        listBuildRename.add("b.$col")
                    }
                }

                // Merge scales (building and Rsu indicators)
                datasource.createIndex(rsuTable, "id_rsu")
                datasource.createIndex(buildingTable, "id_rsu")
                datasource.execute """ DROP TABLE IF EXISTS $finalScaleTableName;
                                CREATE TABLE $finalScaleTableName 
                                    AS SELECT ${listRsuRename.join(', ')}, ${listBuildRename.join(', ')} 
                                    FROM $rsuTable a LEFT JOIN $buildingTable b
                                    ON a.id_rsu = b.id_rsu;""".toString()

                // To avoid crashes of the join due to column duplicate, need to prefix some names
                def blockCol2Rename = datasource.getColumnNames(blockTable)
                for (col in blockCol2Rename) {
                    if (col != "ID_BLOCK" && col != "ID_RSU" && col != "THE_GEOM") {
                        listblockFinalRename.add("b.$col AS block_$col")
                    }
                }
                // Useful for merge between gathered building and rsu indicators and building indicators averaged at RSU scale
                idbuildForMerge = "id_rsu"
                idBlockForMerge = "id_block"
                // Useful if the classif is a regression
                idName = "id_build"
            }

            // Gather all indicators (coming from three different scales) in a single table (the 'targetTableScale' scale)
            // Note that in order to avoid crashes of the join due to column duplicate, indicators have been prefixed
            datasource.createIndex(buildIndicRsuScale, "id_rsu")
            datasource.createIndex(finalScaleTableName, "id_rsu")
            def queryRemoveNull = ""
            if (removeNull) {
                queryRemoveNull += " WHERE b.$idbuildForMerge IS NOT NULL"
            }
            datasource.execute """ DROP TABLE IF EXISTS $scale1ScaleFin;
                            CREATE TABLE $scale1ScaleFin 
                                AS SELECT ${listBuildRsuRename.join(', ')}, b.*
                                FROM $buildIndicRsuScale a RIGHT JOIN $finalScaleTableName b
                                ON a.$idbuildForMerge = b.$idbuildForMerge 
                                $queryRemoveNull;""".toString()

            datasource.createIndex(blockIndicFinalScale, idBlockForMerge)
            datasource.createIndex(scale1ScaleFin, idBlockForMerge)
            datasource.execute """ DROP TABLE IF EXISTS $outputTableName;
                            CREATE TABLE $outputTableName 
                                AS SELECT a.* ${!listblockFinalRename.isEmpty()?","+listblockFinalRename.join(', '):""}
                                FROM $scale1ScaleFin a LEFT JOIN $blockIndicFinalScale b
                                ON a.$idBlockForMerge = b.$idBlockForMerge;""".toString()
            datasource.dropTable(finalScaleTableName, scale1ScaleFin, buildIndicRsuScale)
            return outputTableName
        } else {
            throw new SQLException(""" The 'targetedScale' parameter should either be 'RSU' or 'BUILDING' """)
        }
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the join the tables at gather scale", e)
    } finally {
        datasource.dropTable(finalScaleTableName, scale1ScaleFin, buildIndicRsuScale)
    }
}

/**
 * This process is used to compute aggregate the area of a specific variable to a upper scale from a lower scale (for
 * example the LCZs variables within a Reference Spatial Unit)
 * The aggregate value is divided by the geometry area of the upper table
 *
 * @param upperTableName the name of the upper scale table
 * @param upperColumnId unique identifier for the upper scale table
 * @param lowerTableName the table of the lower scale to be aggregated
 * @param lowerColumName the name of the column to be aggregated
 * @param prefixName String used as prefix to name the output table
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 *        the resulting database will be stored
 * @return A database table name.
 *
 * @author Emmanuel Renault, CNRS, 2020
 * @author Erwan Bocher, CNRS, 2020
 */
String upperScaleAreaStatistics(JdbcDataSource datasource, String upperTableName,
                                String upperColumnId, String lowerTableName,
                                String lowerColumnName, String lowerColumnAlias, boolean keepGeometry = true, String prefixName) throws Exception {
    def spatialJoinTable = postfix("upper_table_join")
    def pivotTable = postfix("pivotAreaTable")
    try {
        def upperGeometryColumn = datasource.getGeometryColumn(upperTableName)
        if (!upperGeometryColumn) {
            throw new IllegalArgumentException("The upper scale table must contain a geometry column")
        }
        def lowerGeometryColumn = datasource.getGeometryColumn(lowerTableName)
        if (!lowerGeometryColumn) {
            throw new IllegalArgumentException("The lower scale table must contain a geometry column")
        }
        datasource.createSpatialIndex(upperTableName, upperGeometryColumn)
        datasource.createIndex(upperTableName, upperColumnId)
        datasource.createSpatialIndex(lowerTableName, lowerGeometryColumn)


        def spatialJoin = """
                              DROP TABLE IF EXISTS $spatialJoinTable;
                              CREATE TABLE $spatialJoinTable 
                              AS SELECT b.$upperColumnId, a.$lowerColumnName,
                                        ST_AREA(ST_INTERSECTION(a.$lowerGeometryColumn, 
                                        b.$upperGeometryColumn)) AS area
                              FROM $lowerTableName a, $upperTableName b
                              WHERE a.$lowerGeometryColumn && b.$upperGeometryColumn AND 
                              ST_INTERSECTS(a.$lowerGeometryColumn, b.$upperGeometryColumn);
                              """
        datasource.execute(spatialJoin.toString())
        datasource """CREATE INDEX ON $spatialJoinTable ($lowerColumnName);
            CREATE INDEX ON $spatialJoinTable ($upperColumnId)""".toString()


        // Creation of a list which contains all indicators of distinct values
        def qIndicator = """
                             SELECT DISTINCT $lowerColumnName 
                             AS val FROM $spatialJoinTable
                             """
        def listValues = datasource.rows(qIndicator.toString())

        def isString = datasource.getTable(spatialJoinTable).getColumnType(lowerColumnName) == "VARCHAR"

        // Creation of the pivot table which contains for each upper geometry
        def query = """
                        DROP TABLE IF EXISTS $pivotTable;
                        CREATE TABLE $pivotTable
                        AS SELECT $upperColumnId
                        """
        listValues.each {
            def aliasColumn = "${lowerColumnAlias}_${it.val.toString().replace('.', '_')}"
            query += """
                         , SUM($aliasColumn)
                         AS $aliasColumn
                         """
        }
        query += " FROM (SELECT $upperColumnId"
        listValues.each {
            def aliasColumn = "${lowerColumnAlias}_${it.val.toString().replace('.', '_')}"
            if (it.val) {
                if (isString) {
                    query += """
                         , CASE WHEN $lowerColumnName='${it.val}'
                         THEN SUM(area) ELSE 0 END
                         AS $aliasColumn
                         """
                } else {
                    query += """
                         , CASE WHEN $lowerColumnName='${it.val}'
                         THEN SUM(area) ELSE 0 END
                         AS $aliasColumn
                         """
                }
            } else {
                query += """
                         , CASE WHEN $lowerColumnName is null
                         THEN SUM(area) ELSE 0 END
                         AS $aliasColumn
                         """
            }
        }
        query += """
                     FROM $spatialJoinTable
                     GROUP BY $upperColumnId, $lowerColumnName)
                     GROUP BY $upperColumnId;
                     """
        datasource.execute(query.toString())
        //Build indexes
        datasource "CREATE INDEX ON $pivotTable ($upperColumnId)".toString()

        // Creation of a table which is built from
        // the union of the upperTable and pivot tables based on the same cell 'id'
        def outputTableName = prefix prefixName, "upper_scale_statistics_area"
        def qjoin = """
                        DROP TABLE IF EXISTS $outputTableName;
                        CREATE TABLE $outputTableName
                        AS SELECT b.$upperColumnId
                        """
        if (keepGeometry) {
            qjoin += ", b.$upperGeometryColumn"
        }
        listValues.each {
            def aliasColumn = "${lowerColumnAlias}_${it.val.toString().replace('.', '_')}"
            qjoin += """
                         , CASE WHEN $aliasColumn IS NULL THEN NULL ELSE $aliasColumn / ST_AREA(b.$upperGeometryColumn) END
                         AS $aliasColumn
                         """
        }
        qjoin += """
                     FROM $upperTableName b
                     LEFT JOIN $pivotTable a
                     ON (a.$upperColumnId = b.$upperColumnId);
                     """
        datasource.execute(qjoin.toString())
        // Drop intermediate tables created during process
        datasource.execute("DROP TABLE IF EXISTS $spatialJoinTable, $pivotTable;".toString())
        debug "The zonal area table have been created"
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the aggregation to a upper scale from a lower scale", e)
    } finally {
        // Drop intermediate tables created during process
        datasource.execute("DROP TABLE IF EXISTS $spatialJoinTable, $pivotTable;".toString())
    }
}