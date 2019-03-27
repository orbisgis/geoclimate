package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume)
 * @return A database table name.
 * @author Jérémy Bernard
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