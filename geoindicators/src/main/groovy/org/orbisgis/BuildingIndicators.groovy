package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * This process extract geometry properties.
 * @return A database table.
 * @author Erwan Bocher
 */
static IProcess geometryProperties() {
return processFactory.create(
        "Geometry properties",
        [inputTableName: String,inputFields:String[],operations: String[]
         , outputTableName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { inputTableName,inputFields, operations, outputTableName, datasource ->
            String query = "CREATE TABLE $outputTableName AS SELECT "
            def geometricField = "the_geom";
            def ops = ["st_geomtype","st_srid", "st_length","st_perimeter","st_area", "st_dimension",
                   "st_coorddim", "st_num_geoms", "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]
            operations.each {operation ->
                if(ops.contains(operation)){
                    query += "$operation($geometricField) as ${operation.substring(3)},"
                }
            }
            query+= "${inputFields.join(",")} from $inputTableName"
            logger.info("Executing $query")
            datasource.execute query
            [outputTableName: outputTableName]
            }
)}