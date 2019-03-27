package org.orbisgis

import com.sun.org.apache.xpath.internal.functions.FuncFalse
import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume)
 * @return A database table name.
 * @author Erwan Bocher
 */
static IProcess unweightedOperationFromLowerScale() {
return processFactory.create(
        "Unweighted statistical operations from lower scale",
        [inputLowerScaleTableName: String,inputCorrelationTableName: String,inputIdLow: String, inputIdUp: String,
         inputToTransfo: String,operations: String[], outputTableName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { inputLowerScaleTableName, inputCorrelationTableName, inputIdLow, inputIdUp,
          inputToTransfo, operations, outputTableName, datasource ->
            def ops = ["SUM","AVG"]

            String query = "CREATE INDEX IF NOT EXISTS id_l ON $inputLowerScaleTableName($inputIdLow); "+
                            "CREATE INDEX IF NOT EXISTS id_lcorr ON $inputCorrelationTableName($inputIdLow); "+
                            "CREATE INDEX IF NOT EXISTS id_ucorr ON $inputCorrelationTableName($inputIdUp); "+
                            "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

            operations.each {operation ->
                if(ops.contains(operation)){
                    query += "$operation(a.$inputToTransfo::float) AS ${operation+"_"+inputToTransfo},"
                }
            }
            query += "b.$inputIdUp FROM $inputLowerScaleTableName a, $inputCorrelationTableName b " +
                    "WHERE a.$inputIdLow = b.$inputIdLow GROUP BY b.$inputIdUp"
            logger.info("Executing $query")
            datasource.execute query
            [outputTableName: outputTableName]
            }
    )}

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume)
 * @return A database table name.
 * @author Erwan Bocher
 */
static IProcess weightedAggregatedStatistics() {
    return processFactory.create(
            "Weighted statistical operations from lower scale",
            [inputLowerScaleTableName: String,inputCorrelationTableName: String,inputIdLow: String, inputIdUp: String,
             inputToTransfo: String, inputWeight: String, operations: String[], outputTableName: String,
             datasource: JdbcDataSource],
            [outputTableName : String],
            { inputLowerScaleTableName, inputCorrelationTableName, inputIdLow, inputIdUp,
              inputToTransfo, inputWeight, operations, outputTableName, datasource ->
                def ops = ["AVG", "STD"]

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = System.currentTimeMillis()

                // Temporary table names
                def weighted_mean = "weighted_mean"+uid_out.toString()
                def corr_weight_mean = "corr_weight_mean"+uid_out.toString()
                def weighted_all = "weighted_all"+uid_out.toString()

                // The weighted mean is calculated in all cases since it is useful for the STD calculation
                datasource.execute(("CREATE INDEX IF NOT EXISTS id_l ON $inputLowerScaleTableName($inputIdLow); "+
                        "CREATE INDEX IF NOT EXISTS id_lcorr ON $inputCorrelationTableName($inputIdLow); "+
                        "CREATE INDEX IF NOT EXISTS id_ucorr ON $inputCorrelationTableName($inputIdUp); "+
                        "DROP TABLE IF EXISTS $weighted_mean; CREATE TABLE $weighted_mean AS SELECT b.$inputIdUp, " +
                        "SUM(a.$inputToTransfo*a.$inputWeight) / SUM(a.$inputWeight) " +
                        "AS ${"weighted_avg_"+inputToTransfo} FROM $inputLowerScaleTableName a, " +
                        "$inputCorrelationTableName b WHERE a.$inputIdLow = b.$inputIdLow GROUP BY " +
                        "b.$inputIdUp").toString())

                // The weighted std is calculated if needed
                if(operations.contains("STD")) {
                    // First, the weighted mean should be set for each rsu in the building/rsu correlation table
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_ucorr ON $weighted_mean($inputIdUp); " +
                            "DROP TABLE IF EXISTS $corr_weight_mean; CREATE TABLE $corr_weight_mean AS SELECT " +
                            "a.*, b.${"weighted_avg_"+inputToTransfo} FROM $inputCorrelationTableName a " +
                            "LEFT JOIN $weighted_mean b ON a.$inputIdUp = b.$inputIdUp").toString())
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_lcorr ON $corr_weight_mean($inputIdLow); " +
                            "CREATE INDEX IF NOT EXISTS id_ucorr ON $corr_weight_mean($inputIdUp); " +
                            "DROP TABLE IF EXISTS $weighted_all; CREATE TABLE $weighted_all AS SELECT " +
                            "POWER(SUM(a.$inputWeight*POWER(a.$inputToTransfo-b.${"weighted_avg_"+inputToTransfo},2))/" +
                            "SUM(a.$inputWeight),0.5)" +
                            " AS ${"weighted_std_" + inputToTransfo}, b.${"weighted_avg_" + inputToTransfo}, " +
                            "b.$inputIdUp FROM $inputLowerScaleTableName a, $corr_weight_mean b" +
                            " WHERE a.$inputIdLow = b.$inputIdLow GROUP BY b.$inputIdUp").toString())
                }
                // If STD is not calculated, the name of weighted_mean is modified
                else{
                    datasource.execute("ALTER TABLE $weighted_mean RENAME TO $weighted_all;".toString())
                }

                // Return only the needed fields and the object ID
                String query = "CREATE TABLE $outputTableName AS SELECT "
                operations.each {operation ->
                    if(operation=="AVG"){
                        query += "${"weighted_avg_"+inputToTransfo},"
                    }
                    if(operation=="STD"){
                        query += "${"weighted_std_"+inputToTransfo},"
                    }
                }
                query += " $inputIdUp FROM $weighted_all"
                logger.info("Executing $query")
                println(query)
                datasource.execute query

                // The temporary tables are deleted
                datasource.execute("DROP TABLE IF EXISTS $weighted_mean, $corr_weight_mean, $weighted_all".toString())
                [outputTableName: outputTableName]
            }
    )}

/**
 * This process is used to compute the Perkins SKill Score (included within a [0.5 - 1] interval) of the distribution
 * of building direction within a block. This indicator gives an idea of the building direction variability within
 * each block. The length of the building SMBR sides is considered to calculate a distribution of building orientation.
 * Then the distribution is used to calculate the Perkins SKill Score. The distribution has an "angle_range_size"
 * interval that has to be defined by the user (default 15).
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess blockPerkinsSkillScoreBuildingDirection() {
    return processFactory.create(
            "Block Perkins skill score building direction",
            [inputBuildingTableName: String,inputCorrelationTableName: String,
             angleRangeSize: Integer, outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, inputCorrelationTableName, angleRangeSize = 15, outputTableName, datasource->

                def geometricField = "the_geom"
                def idFieldBu = "id_build"
                def idFieldBl = "id_block"

                // Test whether the angleRangeSize is a divisor of 180°
                if (180%angleRangeSize==0){
                    // To avoid overwriting the output files of this step, a unique identifier is created
                    uid_out = System.currentTimeMillis()

                    // Temporary table names
                    def build_min_rec = "build_min_rec"+uid_out.toString()
                    def build_dir360 = "build_dir360"+uid_out.toString()
                    def build_dir180 = "build_dir180"+uid_out.toString()
                    def build_dir_dist = "build_dir_dist"+uid_out.toString()
                    def build_dir_tot = "build_dir_tot"+uid_out.toString()


                    // The minimum diameter of the minimum rectangle is created for each building
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_bua ON $inputBuildingTableName($idFieldBu); "+
                                        "CREATE INDEX IF NOT EXISTS id_bub ON $inputCorrelationTableName($idFieldBu); "+
                                        "DROP TABLE IF EXISTS $build_min_rec; CREATE TABLE $build_min_rec AS "+
                                        "SELECT b.$idFieldBl, ST_MINIMUMDIAMETER(ST_MINIMUMRECTANGLE(a.$geometricField)) "+
                                        "AS the_geom FROM $inputBuildingTableName a, $inputCorrelationTableName b "+
                                        "WHERE a.$idFieldBu = b.$idFieldBu").toString())

                    // The length and direction of the smallest and the longest sides of the Minimum rectangle are calculated
                    datasource.execute(("DROP TABLE IF EXISTS $build_dir360; CREATE TABLE $build_dir360 AS "+
                                        "SELECT $idFieldBl, ST_LENGTH(the_geom) AS LEN_L, "+
                                        "ST_AREA(the_geom)/ST_LENGTH(the_geom) AS LEN_H, "+
                                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)))) AS ANG_L, "+
                                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(the_geom, pi()/2)), "+
                                        "ST_ENDPOINT(ST_ROTATE(the_geom, pi()/2))))) AS ANG_H FROM $build_min_rec").toString())

                    // The angles are transformed in the [0, 180]° interval
                    datasource.execute(("DROP TABLE IF EXISTS $build_dir180; CREATE TABLE $build_dir180 AS "+
                                        "SELECT $idFieldBl, LEN_L, LEN_H, CASEWHEN(ANG_L>180, ANG_L-180, ANG_L) AS ANG_L, "+
                                        "CASEWHEN(ANG_H>180, ANG_H-180, ANG_H) AS ANG_H FROM $build_dir360").toString())

                    // The query aiming to create the building direction distribution is created
                    sqlQueryDist = "DROP TABLE IF EXISTS $build_dir_dist; CREATE TABLE $build_dir_dist AS SELECT "
                    for (i=angleRangeSize, i<180, i+=angleRangeSize){
                        sqlQueryDist += "CASEWHEN(ANG_L>=${i-angleRangeSize} AND ANG_L<$i, LEN_L, " +
                                        "CASEWHEN(ANG_H>=${i-angleRangeSize} AND ANG_H<$i, LEN_H, 0)) AS ANG$i, "
                    }
                    sqlQueryDist = sqlQueryDist[0..-3] + " FROM $build_dir180 GROUP BY $idFieldBl;"
                    datasource.execute(sqlQueryDist)

                    // The total of building linear of direction is calculated for each block
                    sqlQueryTot = "DROP TABLE IF EXISTS $build_dir_tot; CREATE TABLE $build_dir_tot AS SELECT *, "
                    for (i=angleRangeSize, i<180, i+=angleRangeSize){
                        sqlQueryTot += "ANG$i + "
                    }
                    sqlQueryTot = sqlQueryTot[0..-3] + "AS ANG_TOT FROM $build_dir_dist;"
                    datasource.execute(sqlQueryTot)

                    // The Perkings Skill score is finally calculated using a last query
                    sqlQueryPerkins = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "
                    for (i=angleRangeSize, i<180, i+=angleRangeSize){
                        sqlQueryPerkins += "LEAST(1/(${180/angleRangeSize}), ANG$i/ANG_TOT) + "
                    }
                    sqlQueryPerkins = sqlQueryPerkins[0..-3] + "AS $block_perkins_skill_score_building_direction" +
                                        " FROM $build_dir_tot;"
                    datasource.execute(sqlQueryPerkins)

                    // The temporary tables are deleted
                    datasource.execute(("DROP TABLE IF EXISTS $build_min_rec, $build_dir360, $build_dir180, "+
                                        "$build_dir_dist, $build_dir_tot").toString())

                    [outputTableName: outputTableName]
                }
            }
    )}