package org.orbisgis.orbisprocess.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess


@BaseScript Geoindicators geoindicators

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume). Note that for
 * surface fractions it is highly recommended to use the surfaceFractions IProcess (since it considers the potential
 * layer superpositions (building, high vegetation, low vegetation, etc.).
 *
 * @param inputLowerScaleTableName the table name where are stored low scale objects and the id of the upper scale
 * zone they are belonging to (e.g. buildings)
 * @param inputUpperScaleTableName the table of the upper scale where the informations has to be aggregated to
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
 *          scale area. NOTE THAT THE THREE FIRST LETTERS OF THE MAP KEY WILL BE USED TO CREATE THE NAME OF THE INDICATOR
 *          (e.g. "BUI" in the case of the example given above)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 *
 * @author Jérémy Bernard
 */
IProcess unweightedOperationFromLowerScale() {
    def final GEOMETRIC_FIELD_UP = "the_geom"
    def final BASE_NAME = "unweighted_operation_from_lower_scale"
    def final SUM = "SUM"
    def final AVG = "AVG"
    def final GEOM_AVG = "GEOM_AVG"
    def final DENS = "DENS"
    def final NB_DENS = "NB_DENS"
    def final STD = "STD"
    def final COLUMN_TYPE_TO_AVOID = ["GEOMETRY", "VARCHAR"]

    return create({
        title "Unweighted statistical operations from lower scale"
        inputs inputLowerScaleTableName: String, inputUpperScaleTableName: String, inputIdUp: String,
                inputIdLow: String, inputVarAndOperations: Map, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputLowerScaleTableName, inputUpperScaleTableName, inputIdUp, inputIdLow, inputVarAndOperations, prefixName,
              datasource ->

            info "Executing Unweighted statistical operations from lower scale"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            datasource.getTable(inputLowerScaleTableName)."$inputIdUp".createIndex()
            datasource.getTable(inputUpperScaleTableName)."$inputIdUp".createIndex()

            def query =  "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

            inputVarAndOperations.each { var, operations ->
                if (!COLUMN_TYPE_TO_AVOID.contains(datasource.getTable(inputLowerScaleTableName)[var].getType())){
                    operations.each { op ->
                        op = op.toUpperCase()
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
                                query += "COALESCE(STDDEV_POP(a.$var::float),0) AS  ${op + "_" + var},"
                            default:
                                break
                        }
                    }
                }
                else{
                    logger.error """ The column $var should be numeric"""
                }

            }
            query += "b.$inputIdUp FROM $inputLowerScaleTableName a RIGHT JOIN $inputUpperScaleTableName b " +
                    "ON a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp"

            datasource.execute query

            [outputTableName: outputTableName]
        }
    })
}

/**
 * This process is used to compute weighted average and standard deviation on a specific variable from a lower scale (for
 * example the mean building roof height within a reference spatial unit where the roof height values are weighted
 * by the values of building area)
 *
 *  @param inputLowerScaleTableName the table name where are stored low scale objects and the id of the upper scale
 *  zone they are belonging to (e.g. buildings)
 *  @param inputUpperScaleTableName the table of the upper scale where the informations has to be aggregated to
 *  @param inputIdUp the ID of the upper scale
 *  @param inputVarWeightsOperations a map containing as key the informations that has to be transformed from the lower to
 *  the upper scale, and as value a map containing as key the variable that has to be used as weight and as values
 *  a LIST of operation to apply to this couple (information/weight). Note that the allowed operations should be in
 *  the following list:
 *           --> "STD": weighted standard deviation
 *           --> "AVG": weighted mean
 *  @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess weightedAggregatedStatistics() {
    def final AVG = "AVG"
    def final STD = "STD"
    def final BASE_NAME = "weighted_aggregated_statistics"

    return create({
        title "Weighted statistical operations from lower scale"
        inputs inputLowerScaleTableName: String, inputUpperScaleTableName: String, inputIdUp: String,
                inputVarWeightsOperations: Map, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputLowerScaleTableName, inputUpperScaleTableName, inputIdUp, inputVarWeightsOperations, prefixName,
              datasource ->

            info "Executing Weighted statistical operations from lower scale"

            // The name of the outputTableName is constructed
            String outputTableName = prefixName + "_" + BASE_NAME

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def weighted_mean = "weighted_mean" + uuid

            datasource.getSpatialTable(inputLowerScaleTableName)."$inputIdUp".createIndex()
            datasource.getSpatialTable(inputUpperScaleTableName)."$inputIdUp".createIndex()

            // The weighted mean is calculated in all cases since it is useful for the STD calculation
            def weightedMeanQuery =  "DROP TABLE IF EXISTS $weighted_mean; CREATE TABLE $weighted_mean($inputIdUp INTEGER,"
            def nameAndType = ""
            def weightedMean = ""
            inputVarWeightsOperations.each { var, weights ->
                weights.each { weight, operations ->
                    nameAndType += "weighted_avg_${var}_$weight DOUBLE DEFAULT 0,"
                    weightedMean += "COALESCE(SUM(a.$var*a.$weight) / SUM(a.$weight),0) AS weighted_avg_${var}_$weight,"
                }
            }
            weightedMeanQuery += nameAndType[0..-2] + ") AS (SELECT b.$inputIdUp, ${weightedMean[0..-2]}" +
                    " FROM $inputLowerScaleTableName a RIGHT JOIN $inputUpperScaleTableName b " +
                    "ON a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp)"
            datasource.execute weightedMeanQuery

            // The weighted std is calculated if needed and only the needed fields are returned
            def weightedStdQuery = "CREATE INDEX IF NOT EXISTS id_lcorr ON $weighted_mean USING BTREE($inputIdUp); " +
                    "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.$inputIdUp,"
            inputVarWeightsOperations.each { var, weights ->
                weights.each { weight, operations ->
                    // The operation names are transformed into upper case
                    operations.replaceAll({ s -> s.toUpperCase() })
                    if (operations.contains(AVG)) {
                        weightedStdQuery += "COALESCE(b.weighted_avg_${var}_$weight,0) AS avg_${var}_${weight}_weighted,"
                    }
                    if (operations.contains(STD)) {
                        weightedStdQuery += "COALESCE(POWER(SUM(a.$weight*POWER(a.$var-b.weighted_avg_${var}_$weight,2))/" +
                                "SUM(a.$weight),0.5),0) AS std_${var}_${weight}_weighted,"
                    }
                }
            }
            weightedStdQuery = weightedStdQuery[0..-2] + " FROM $inputLowerScaleTableName a RIGHT JOIN $weighted_mean b " +
                    "ON a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp"

            datasource.execute weightedStdQuery

            // The temporary tables are deleted
            datasource.execute "DROP TABLE IF EXISTS $weighted_mean"

            [outputTableName: outputTableName]
        }
    })
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
 * @author Erwan Bocher
 */
IProcess geometryProperties() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final OPS = ["st_geomtype","st_srid", "st_length","st_perimeter","st_area", "st_dimension",
               "st_coorddim", "st_num_geoms", "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]
    def final BASE_NAME = "geometry_properties"

    return create({
        title "Geometry properties"
        inputs inputTableName: String, inputFields: String[], operations: String[], prefixName: String,
                datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputTableName, inputFields, operations, prefixName, datasource ->

            info "Executing Geometry properties"

            // The name of the outputTableName is constructed
            def outputTableName = getOutputTableName(prefixName, BASE_NAME)

            def query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

            // The operation names are transformed into lower case
            operations.replaceAll { s -> s.toLowerCase() }
            operations.each { operation ->
                if (OPS.contains(operation)) {
                    query += "$operation($GEOMETRIC_FIELD) as ${operation.substring(3)},"
                }
            }
            query += "${inputFields.join(",")} from $inputTableName"

            datasource.execute query

            [outputTableName: outputTableName]
        }
    })
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
IProcess buildingDirectionDistribution() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD_BU = "id_build"
    def final INEQUALITY = "BUILDING_DIRECTION_EQUALITY"
    def final UNIQUENESS = "BUILDING_DIRECTION_UNIQUENESS"
    def final BASENAME = "MAIN_BUILDING_DIRECTION"

    return create({
        title "Perkins skill score building direction"
        inputs buildingTableName: String, inputIdUp: String, angleRangeSize: 15, prefixName: String,
                datasource: JdbcDataSource, distribIndicator: ["equality", "uniqueness"]
        outputs outputTableName: String
        run { buildingTableName, inputIdUp, angleRangeSize, prefixName, datasource, distribIndicator ->

            info "Executing Perkins skill score building direction"

            // The name of the outputTableName is constructed
            def outputTableName = getOutputTableName(prefixName, BASENAME)

            // Test whether the angleRangeSize is a divisor of 180°
            if ((180 % angleRangeSize) == 0 && (180 / angleRangeSize) > 1) {
                def med_angle = angleRangeSize / 2
                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names
                def build_min_rec = "build_min_rec$uuid"
                def build_dir360 = "build_dir360$uuid"
                def build_dir180 = "build_dir180$uuid"
                def build_dir_dist = "build_dir_dist$uuid"

                // The minimum diameter of the minimum rectangle is created for each building
                datasource.execute "DROP TABLE IF EXISTS $build_min_rec; CREATE TABLE $build_min_rec AS " +
                        "SELECT $ID_FIELD_BU, $inputIdUp, ST_MINIMUMDIAMETER(ST_MINIMUMRECTANGLE($GEOMETRIC_FIELD)) " +
                        "AS the_geom FROM $buildingTableName"

                datasource.getSpatialTable(buildingTableName).id_build.createIndex()

                // The length and direction of the smallest and the longest sides of the Minimum rectangle are calculated
                datasource.execute "CREATE INDEX IF NOT EXISTS id_bua ON $build_min_rec USING BTREE($ID_FIELD_BU);" +
                        "DROP TABLE IF EXISTS $build_dir360; CREATE TABLE $build_dir360 AS " +
                        "SELECT a.$inputIdUp, ST_LENGTH(a.the_geom) AS LEN_L, " +
                        "ST_AREA(b.the_geom)/ST_LENGTH(a.the_geom) AS LEN_H, " +
                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(a.the_geom), ST_ENDPOINT(a.the_geom)))) AS ANG_L, " +
                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(a.the_geom, pi()/2)), " +
                        "ST_ENDPOINT(ST_ROTATE(a.the_geom, pi()/2))))) AS ANG_H FROM $build_min_rec a  " +
                        "LEFT JOIN $buildingTableName b ON a.$ID_FIELD_BU=b.$ID_FIELD_BU"

                // The angles are transformed in the [0, 180]° interval
                datasource.execute "DROP TABLE IF EXISTS $build_dir180; CREATE TABLE $build_dir180 AS " +
                        "SELECT $inputIdUp, LEN_L, LEN_H, CASEWHEN(ANG_L>=180, ANG_L-180, ANG_L) AS ANG_L, " +
                        "CASEWHEN(ANG_H>180, ANG_H-180, ANG_H) AS ANG_H FROM $build_dir360"

                datasource.execute "CREATE INDEX ON $build_dir180 ($inputIdUp)"

                // The query aiming to create the building direction distribution is created
                String sqlQueryDist = "DROP TABLE IF EXISTS $build_dir_dist; CREATE TABLE $build_dir_dist AS SELECT "
                for (int i = angleRangeSize; i <= 180; i += angleRangeSize) {
                    def nameAngle = (i-med_angle).toString().replace(".", "_")
                    sqlQueryDist += "SUM(CASEWHEN(ANG_L>=${i - angleRangeSize} AND ANG_L<$i, LEN_L, " +
                            "CASEWHEN(ANG_H>=${i - angleRangeSize} AND ANG_H<$i, LEN_H, 0))) AS ANG$nameAngle, "
                }
                sqlQueryDist += "$inputIdUp FROM $build_dir180 GROUP BY $inputIdUp;"

                // The query is executed
                datasource.execute sqlQueryDist

                // The main building direction and indicators characterizing the distribution are calculated
                IProcess computeDistribChar = distributionCharacterization()
                computeDistribChar.execute([distribTableName:   build_dir_dist,
                                            inputId:            inputIdUp,
                                            distribIndicator:   distribIndicator,
                                            extremum:           "GREATEST",
                                            prefixName:         prefixName,
                                            datasource:         datasource])
                def resultsDistrib = computeDistribChar.getResults().outputTableName

                // Rename the standard indicators into names consistent with the current IProcess (building direction...)
                datasource.execute """DROP TABLE IF EXISTS $outputTableName;
                                        ALTER TABLE $resultsDistrib RENAME TO $outputTableName;
                                        ALTER TABLE $outputTableName RENAME COLUMN EXTREMUM_COL TO $BASENAME;
                                        ALTER TABLE $outputTableName RENAME COLUMN UNIQUENESS_VALUE TO $UNIQUENESS;
                                        ALTER TABLE $outputTableName RENAME COLUMN EQUALITY_VALUE TO $INEQUALITY;"""

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
                    datasource.execute sqlQueryUnique

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

                    datasource.execute "CREATE INDEX ON $build_perk_fin ($inputIdUp);"
                    datasource.execute sqlQueryLast
                }
                else{
                    datasource.execute "ALTER TABLE $build_perk_fin RENAME TO $outputTableName;"
                }
                */
                // The temporary tables are deleted
                datasource.execute "DROP TABLE IF EXISTS $build_min_rec, $build_dir360, $build_dir180, " +
                        "$build_dir_dist;"

                [outputTableName: outputTableName]
            }
        }
    })
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
 * @param inputId The ID of the input data
 * @param distribIndicator List containing the type of indicator to calculate to define the repartition of the distribution
 * (default ["equality", "uniqueness"])
 *      --> "equality": the Perkins Skill Score is calculated
 *      --> "uniqueness": the weight of the greatest (or the lowest) class is divided by
 *                      "the weight of the second greatest (or lowest) class + the weight of the greatest (or the lowest) class"
 * @param extremum String indicating the kind of extremum to calculate (default "GREATEST")
 *      --> "LEAST"
 *      --> "GREATEST"
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
IProcess distributionCharacterization() {
    def final EQUALITY = "EQUALITY_VALUE"
    def final UNIQUENESS = "UNIQUENESS_VALUE"
    def final EXTREMUM_COL = "EXTREMUM_COL"
    def final BASENAME = "DISTRIBUTION_REPARTITION"

    return create({
        title "Distribution characterization"
        inputs distribTableName: String, inputId: String, prefixName: String,
                datasource: JdbcDataSource, distribIndicator: ["equality", "uniqueness"], extremum:"GREATEST"
        outputs outputTableName: String
        run { distribTableName, inputId, prefixName, datasource, distribIndicator, extremum ->

            info "Executing equality and uniqueness indicators"

            if(extremum.toUpperCase() == "GREATEST" || extremum.toUpperCase() == "LEAST") {
                // The name of the outputTableName is constructed
                def outputTableName = getOutputTableName(prefixName, BASENAME)

                // Get the distribution columns and the number of columns
                def distribColumns = datasource.getTable(distribTableName).getColumns()
                distribColumns.remove(inputId.toUpperCase())
                def nbDistCol = distribColumns.size()

                def idxExtrem = nbDistCol - 1
                def idxExtrem_1 = nbDistCol - 2
                if (extremum.toUpperCase() == "LEAST") {
                    idxExtrem = 0
                    idxExtrem_1 = 1
                }


                if (distribIndicator.contains("equality") && !distribIndicator.contains("uniqueness")) {
                    datasource.execute """CREATE TABLE $outputTableName($inputId integer, 
                                                                        $EQUALITY DOUBLE,
                                                                        $EXTREMUM_COL VARCHAR)"""
                    // Will insert values by batch of 1000 in the table
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow("SELECT * FROM $distribTableName") { row ->
                            def rowMap = row.toRowResult()
                            def id_rsu = rowMap[inputId]
                            rowMap.remove(inputId.toUpperCase())
                            def sortedMap = rowMap.sort { it.value }
                            stmt.addBatch """INSERT INTO $outputTableName 
                                                    VALUES ($id_rsu, ${getEquality(sortedMap, nbDistCol)},
                                                            '${sortedMap.keySet()[idxExtrem]}')"""
                        }
                    }

                } else if (!distribIndicator.contains("equality") && distribIndicator.contains("uniqueness")) {
                    datasource.execute """CREATE TABLE $outputTableName($inputId integer, 
                                                                        $UNIQUENESS DOUBLE,
                                                                        $EXTREMUM_COL VARCHAR)"""
                    // Will insert values by batch of 1000 in the table
                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow("SELECT * FROM $distribTableName") { row ->
                            def rowMap = row.toRowResult()
                            def id_rsu = rowMap[inputId]
                            rowMap.remove(inputId.toUpperCase())
                            def sortedMap = rowMap.sort { it.value }
                            stmt.addBatch """INSERT INTO $outputTableName 
                                                    VALUES ($id_rsu, ${getUniqueness(sortedMap, idxExtrem, idxExtrem_1)},
                                                            '${sortedMap.keySet()[idxExtrem]}')"""
                        }
                    }
                } else if (distribIndicator.contains("equality") && distribIndicator.contains("uniqueness")) {
                    datasource.execute """CREATE TABLE $outputTableName($inputId integer, 
                                                                        $EQUALITY DOUBLE,
                                                                        $UNIQUENESS DOUBLE,
                                                                        $EXTREMUM_COL VARCHAR)"""

                    // Will insert values by batch of 1000 in the table
                    datasource.withBatch(1000) { stmt ->

                        datasource.eachRow("SELECT * FROM $distribTableName") { row ->
                            def rowMap = row.toRowResult()
                            def id_rsu = rowMap[inputId]
                            rowMap.remove(inputId)
                            def sortedMap = rowMap.sort { it.value }
                            stmt.addBatch """INSERT INTO $outputTableName 
                                                    VALUES ($id_rsu, ${getEquality(sortedMap, nbDistCol)},
                                                            ${getUniqueness(sortedMap, idxExtrem, idxExtrem_1)},
                                                            '${sortedMap.keySet()[idxExtrem]}')"""
                        }
                    }
                }
                [outputTableName: outputTableName]
            }
            else{
                error """The 'extremum' input parameter should be equal to "GREATEST" or "LEAST"""
            }
        }
    })
}

/**
 * This function calculates the UNIQUENESS indicator of a distribution for a given RSU
 * @param myMap The row of the table to examine
 * @param idxExtrem when the row is sorted by ascending values, id of the extremum value to get
 * @param idxExtrem_1 when the row is sorted by ascending values, id of the second extremum value to get
 * @return A double : the value of the UNIQUENESS indicator for this RSU
 */
static double getUniqueness(def myMap, def idxExtrem, def idxExtrem_1) {
    def Extrem = myMap.values()[idxExtrem]
    def Extrem_1 = myMap.values()[idxExtrem_1]

    def uniqueness = 0
    if(Extrem+Extrem_1 > 0){
        uniqueness = Math.abs(Extrem-Extrem_1)/(Extrem+Extrem_1)
    }

    return uniqueness
}

/**
 * This function calculates the EQUALITY indicator of a distribution for a given RSU
 * @param myMap The row of the table to examine
 * @param nbDistCol the number of columns of the distribution
 * @return A double : the value of the EQUALITY indicator for this RSU
 */
static double getEquality(def myMap, def nbDistCol) {
    def sum = myMap.values().sum()
    def equality = 0
    myMap.values().each{it ->
        equality += Math.min(it, sum/nbDistCol)
    }

    return equality/sum
}