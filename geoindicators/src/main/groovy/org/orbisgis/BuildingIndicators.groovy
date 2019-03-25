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


/**
 * This process extract building interactions properties.
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingNeighborsProperties() {
    return processFactory.create(
            "Building interactions properties",
            [inputBuildingTableName: String,inputFields:String[],operations: String[]
             , outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName,inputFields, operations, outputTableName, datasource ->
                def geometricField = "the_geom"
                def idField = "id_build"
                def height_wall = "height_wall"
                def ops = ["building_contiguity","building_common_wall_fraction",
                           "building_number_building_neighbor"]
                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = System.currentTimeMillis()
                // Temporary table names
                def build_intersec = "build_intersec"+uid_out.toString()


                String query = "CREATE INDEX IF NOT EXISTS buff_ids ON $inputBuildingTableName($geometricField) USING RTREE; " +
                        "CREATE INDEX IF NOT EXISTS buff_id ON $inputBuildingTableName($idField);" +
                        " CREATE TABLE $build_intersec AS SELECT "

                String query_update = ""

                operations.each {operation ->
                    if(operation=="building_contiguity") {
                        query += "sum(least(a.$height_wall, b.$height_wall)*" +
                                "st_length(ST_INTERSECTION(a.$geometricField,b.$geometricField)))/" +
                                "((ST_PERIMETER(a.$geometricField)+ST_PERIMETER(ST_HOLES(a.$geometricField)))*a.$height_wall)" +
                                " AS $operation,"
                    }
                    else if(operation=="building_common_wall_fraction"){
                        query += "sum(ST_LENGTH(ST_INTERSECTION(a.$geometricField, b.$geometricField)))/" +
                                "(ST_PERIMETER(a.$geometricField)+ST_PERIMETER(ST_HOLES(a.$geometricField))) " +
                                "AS $operation,"
                    }
                    else if(operation=="building_number_building_neighbor"){
                        query += "COUNT(ST_INTERSECTION(a.$geometricField, b.$geometricField))" +
                                " AS $operation,"
                    }
                    // The buildingNeighborProperty is set to 0 for the buildings that have no intersection with their building neighbors
                    if(ops.contains(operation)){
                        query_update+= "UPDATE $outputTableName SET $operation = 0 WHERE $operation IS null;"
                    }
                }
                query+= "a.${inputFields.join(",a.")} FROM $inputBuildingTableName a, $inputBuildingTableName b" +
                        " WHERE a.$geometricField && b.$geometricField AND " +
                        "ST_INTERSECTS(a.$geometricField, b.$geometricField) AND a.$idField <> b.$idField" +
                        " GROUP BY a.$idField;" +
                        "CREATE INDEX IF NOT EXISTS buff_id ON $build_intersec($idField);" +
                        "CREATE TABLE $outputTableName AS SELECT b.${operations.join(",b.")}, a.${inputFields.join(",a.")}" +
                        " FROM $inputBuildingTableName a LEFT JOIN $build_intersec b ON a.$idField = b.$idField;"
                query+= query_update

                // The temporary tables are deleted
                query+= "DROP TABLE IF EXISTS $build_intersec"

                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}