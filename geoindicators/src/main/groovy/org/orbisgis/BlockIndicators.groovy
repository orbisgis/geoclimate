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
 * This process is used to compute weighted average and standard deviation on a specific variable from a lower scale (for
 * example the mean building roof height within a reference spatial unit where the roof height values are weighted
 * by the values of building area)
 *
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