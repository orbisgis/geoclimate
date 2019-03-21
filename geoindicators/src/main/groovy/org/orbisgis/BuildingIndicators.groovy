package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * This process extract geometry properties.
 * @return A database table name.
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
                    query += "$operation($geometricField) as $operation,"
                }
            }
            query+= "${inputFields.join(",")} from $inputTableName"
            logger.info("Executing $query")
            datasource.execute query
            [outputTableName: outputTableName]
            }
)}

/**
 * This process extract building size properties.
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingSizeProperties() {
    return processFactory.create(
            "Building size properties",
            [inputBuildingTableName: String,inputFields:String[],operations: String[]
             , outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName,inputFields, operations, outputTableName, datasource ->
                String query = "CREATE TABLE $outputTableName AS SELECT "
                def geometricField = "the_geom"
                def dist_passiv = 3
                def ops = ["building_volume","building_floor_area", "building_total_facade_length",
                           "building_passive_volume_ratio"]

                operations.each {operation ->
                    if(operation=="building_volume") {
                        query += "ST_AREA($geometricField)*0.5*(height_wall+height_roof) AS building_volume,"
                    }
                    else if(operation=="building_floor_area"){
                        query += "ST_AREA($geometricField)*nb_lev AS building_floor_area,"
                    }
                    else if(operation=="building_total_facade_length"){
                        query += "ST_PERIMETER($geometricField)+ST_PERIMETER(ST_HOLES($geometricField))" +
                                " AS building_total_facade_length,"
                    }
                    else if(operation=="building_passive_volume_ratio") {
                        query += "ST_AREA(ST_BUFFER($geometricField, -$dist_passiv, 'join=mitre'))/" +
                                "ST_AREA($geometricField) AS building_passive_volume_ratio,"
                    }
                }
                query+= "${inputFields.join(",")} FROM $inputBuildingTableName"
                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}
