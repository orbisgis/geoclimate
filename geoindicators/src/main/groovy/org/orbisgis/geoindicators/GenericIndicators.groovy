package org.orbisgis.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoindicators geoindicators

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume)
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

    return create({
        title "Unweighted statistical operations from lower scale"
        inputs inputLowerScaleTableName: String, inputUpperScaleTableName: String, inputIdUp: String,
                inputVarAndOperations: Map, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputLowerScaleTableName, inputUpperScaleTableName, inputIdUp, inputVarAndOperations, prefixName,
              datasource ->

            info "Executing Unweighted statistical operations from lower scale"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            def query = "CREATE INDEX IF NOT EXISTS id_l ON $inputLowerScaleTableName($inputIdUp); " +
                    "CREATE INDEX IF NOT EXISTS id_ucorr ON $inputUpperScaleTableName($inputIdUp); " +
                    "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

            inputVarAndOperations.each { var, operations ->
                operations.each { op ->
                    op = op.toUpperCase()
                    switch (op) {
                        case GEOM_AVG:
                            query += "COALESCE(EXP(1.0/COUNT(*)*SUM(LOG(a.$var))),0) AS ${op + "_" + var},"
                            break
                        case DENS:
                            query += "COALESCE(SUM(a.$var::float)/ST_AREA(b.$GEOMETRIC_FIELD_UP),0) AS ${op + "_" + var},"
                            break
                        case NB_DENS:
                            query += "COALESCE(COUNT(a.*)/ST_AREA(b.$GEOMETRIC_FIELD_UP),0) AS ${op + "_" + var},"
                            break
                        case SUM:
                        case AVG:
                            query += "$op(a.$var::float) AS ${op + "_" + var},"
                            break
                        default:
                            break
                    }
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
            def weighted_mean = "weighted_mean" + uuid()

            // The weighted mean is calculated in all cases since it is useful for the STD calculation
            def weightedMeanQuery = "CREATE INDEX IF NOT EXISTS id_l ON $inputLowerScaleTableName($inputIdUp); " +
                    "CREATE INDEX IF NOT EXISTS id_ucorr ON $inputUpperScaleTableName($inputIdUp); " +
                    "DROP TABLE IF EXISTS $weighted_mean; CREATE TABLE $weighted_mean($inputIdUp INTEGER,"
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
            def weightedStdQuery = "CREATE INDEX IF NOT EXISTS id_lcorr ON $weighted_mean($inputIdUp); " +
                    "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.$inputIdUp,"
            inputVarWeightsOperations.each { var, weights ->
                weights.each { weight, operations ->
                    // The operation names are transformed into upper case
                    operations.replaceAll({ s -> s.toUpperCase() })
                    if (operations.contains(AVG)) {
                        weightedStdQuery += "COALESCE(b.weighted_avg_${var}_$weight,0) AS weighted_avg_${var}_$weight,"
                    }
                    if (operations.contains(STD)) {
                        weightedStdQuery += "COALESCE(POWER(SUM(a.$weight*POWER(a.$var-b.weighted_avg_${var}_$weight,2))/" +
                                "SUM(a.$weight),0.5),0) AS weighted_std_${var}_$weight,"

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
            def outputTableName = prefixName + "_" + BASE_NAME

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
 * This process is used to compute the Perkins SKill Score (included within a [0 - 1] interval) of the distribution
 * of building direction within a block or a RSU. This indicator gives an idea of the building direction variability within
 * each block / RSU. The length of the building SMBR sides is considered to calculate a distribution of building orientation.
 * Then the distribution is used to calculate the Perkins SKill Score. The distribution has an "angle_range_size"
 * interval that has to be defined by the user (default 15).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildingTableName the name of the input ITable where are stored the geometry field, the building and the block/RSU ID
 * @param inputIdUp the ID of the upper scale
 * @param angleRangeSize the range size (in °) of each interval angle used to calculate the distribution of building direction
 * (should be a divisor of 180 - default 15°)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess perkinsSkillScoreBuildingDirection() {
    def final GEOMETIC_FIELD = "the_geom"
    def final ID_FIELD_BU = "id_build"

    return create({
        title "Block Perkins skill score building direction"
        inputs buildingTableName: String, inputIdUp: String, angleRangeSize: 15, prefixName: String,
                datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTableName, inputIdUp, angleRangeSize, prefixName, datasource ->

            info "Executing Block Perkins skill score building direction"

            // The name of the outputTableName is constructed
            String baseName = inputIdUp[3..-1] + "_perkins_skill_score_building_direction"
            String outputTableName = prefixName + "_" + baseName

            // Test whether the angleRangeSize is a divisor of 180°
            if ((180 % angleRangeSize) == 0 && (180 / angleRangeSize) > 1) {
                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names
                def build_min_rec = "build_min_rec$uuid"
                def build_dir360 = "build_dir360$uuid"
                def build_dir180 = "build_dir180$uuid"
                def build_dir_dist = "build_dir_dist$uuid"
                def build_dir_tot = "build_dir_tot$uuid"

                // The minimum diameter of the minimum rectangle is created for each building
                datasource.execute "DROP TABLE IF EXISTS $build_min_rec; CREATE TABLE $build_min_rec AS " +
                        "SELECT $ID_FIELD_BU, $inputIdUp, ST_MINIMUMDIAMETER(ST_MINIMUMRECTANGLE($GEOMETIC_FIELD)) " +
                        "AS the_geom FROM $buildingTableName"

                // The length and direction of the smallest and the longest sides of the Minimum rectangle are calculated
                datasource.execute "CREATE INDEX IF NOT EXISTS id_bua ON $buildingTableName($ID_FIELD_BU);" +
                        "CREATE INDEX IF NOT EXISTS id_bua ON $build_min_rec($ID_FIELD_BU);" +
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

                // The query aiming to create the building direction distribution is created
                // The total of building linear of direction is calculated for each block/RSU
                // The Perkings Skill score is finally calculated using a last query

                String sqlQueryDist = "DROP TABLE IF EXISTS $build_dir_dist; CREATE TABLE $build_dir_dist AS SELECT "
                String sqlQueryTot = "DROP TABLE IF EXISTS $build_dir_tot; CREATE TABLE $build_dir_tot AS SELECT *, "
                String sqlQueryPerkins = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT $inputIdUp, "

                for (int i = angleRangeSize; i < 180; i += angleRangeSize) {
                    sqlQueryDist += "SUM(CASEWHEN(ANG_L>=${i - angleRangeSize} AND ANG_L<$i, LEN_L, " +
                            "CASEWHEN(ANG_H>=${i - angleRangeSize} AND ANG_H<$i, LEN_H, 0))) AS ANG$i, "
                    sqlQueryTot += "ANG$i + "
                    sqlQueryPerkins += "LEAST(1./(${180 / angleRangeSize}), ANG$i::float/ANG_TOT) + "
                }

                sqlQueryDist += "$inputIdUp FROM $build_dir180 GROUP BY $inputIdUp;"
                sqlQueryTot = sqlQueryTot[0..-3] + "AS ANG_TOT FROM $build_dir_dist;"
                sqlQueryPerkins = sqlQueryPerkins[0..-3] + "AS block_perkins_skill_score_building_direction" +
                        " FROM $build_dir_tot;"

                datasource.execute sqlQueryDist
                datasource.execute sqlQueryTot
                datasource.execute sqlQueryPerkins

                // The temporary tables are deleted
                datasource.execute "DROP TABLE IF EXISTS $build_min_rec, $build_dir360, $build_dir180, " +
                        "$build_dir_dist, $build_dir_tot"

                [outputTableName: outputTableName]
            }
        }
    })
}