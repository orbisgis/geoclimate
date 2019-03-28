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
 *
 * --> "building_volume": defined as the building area multiplied by the mean of the building
 * wall height and the building roof height.
 * --> "building_floor_area": defined as the number of level multiplied by the building area (cf. Bocher et al. - 2018)
 * --> "building_total_facade_length": defined as the total linear of facade (sum of the building perimeter and
 * the perimeter of the building courtyards)
 *
 * References:
 *   Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 *
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
 *
 * --> "building_contiguity": defined as the shared wall area divided by the total building wall area
 * (cf. Bocher et al. - 2018)
 * --> "building_common_wall_fraction": defined as ratio between the lenght of building wall shared with other buildings
 * and the length of total building walls
 * --> "building_number_building_neighbor": defined as the number of building  neighbors in contact with the building
 * (cf. Bocher et al. - 2018)
 *
 * References:
 *   Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 *
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
                def build_intersec = "build_intersec"+uid_out


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


/**
 * This process extract building form properties.
 *
 * --> "building_concavity": defined as the building area divided by the convex hull area (cf. Bocher et al. - 2018)
 * --> "building_form_factor": defined as ratio between the building area divided by the square of the building
 * perimeter (cf. Bocher et al. - 2018)
 * --> "building_raw_compacity": defined as the ratio between building surfaces (walls and roof) divided by the
 * building volume at the power 2./3. For the calculation, the roof is supposed to have a gable and the roof surface
 * is calculated considering that the building is square (otherwise, the assumption related to the gable direction
 * would strongly affect the result).
 *
 * References:
 *   Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingFormProperties() {
    return processFactory.create(
            "Building form properties",
            [inputBuildingTableName: String,inputFields:String[],operations: String[]
             , outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName,inputFields, operations, outputTableName, datasource ->
                def geometricField = "the_geom"
                def height_wall = "height_wall"
                def height_roof = "height_roof"
                def ops = ["building_concavity","building_form_factor",
                           "building_raw_compacity"]

                String query = " CREATE TABLE $outputTableName AS SELECT "

                operations.each {operation ->
                    if(operation=="building_concavity"){
                        query += "ST_AREA($geometricField)/ST_AREA(ST_CONVEXHULL($geometricField)) AS $operation,"
                    }
                    else if(operation=="building_form_factor"){
                        query += "ST_AREA($geometricField)/POWER(ST_PERIMETER($geometricField), 2) AS $operation,"
                    }
                    else if(operation=="building_raw_compacity") {
                        query += "((ST_PERIMETER($geometricField)+ST_PERIMETER(ST_HOLES($geometricField)))*$height_wall+" +
                                "POWER(POWER(ST_AREA($geometricField),2)+4*ST_AREA($geometricField)*" +
                                "POWER($height_roof-$height_wall, 2),0.5)+POWER(ST_AREA($geometricField),0.5)*" +
                                "($height_roof-$height_wall))/POWER(ST_AREA($geometricField)*" +
                                "($height_wall+$height_roof)/2, 2./3) AS $operation,"
                    }
                }
                query+= "${inputFields.join(",")} FROM $inputBuildingTableName"

                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}

/**
 * This process extract the building closest distance to an other building. A buffer of defined size (bufferDist argument,
 * default 100 m) is used to get the buildings within the building of interest and then the minimum distance is calculated.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingMinimumBuildingSpacing() {
    return processFactory.create(
            "Building minimum building spacing",
            [inputBuildingTableName: String,inputFields:String[],bufferDist: Double
             , outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName,inputFields, bufferDist = 100, outputTableName, datasource ->
                def geometricField = "the_geom"
                def idField = "id_build"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = System.currentTimeMillis()

                // Temporary table names
                def build_buffer = "build_buffer"+uid_out
                def build_within_buffer = "build_within_buffer"+uid_out

                // The buffer is created
                datasource.execute(("CREATE TABLE $build_buffer AS SELECT $idField, ST_BUFFER($geometricField, $bufferDist)"+
                                    " AS $geometricField FROM $inputBuildingTableName; " +
                                    "CREATE INDEX IF NOT EXISTS buff_ids ON $build_buffer($geometricField) USING RTREE;"+
                                    "CREATE INDEX IF NOT EXISTS inpute_ids ON $inputBuildingTableName($geometricField) USING RTREE;"+
                                    "CREATE INDEX IF NOT EXISTS buff_id ON $build_buffer($idField); "+
                                    "CREATE INDEX IF NOT EXISTS inpute_id ON $inputBuildingTableName($idField)").toString())
                // The building located within the buffer are identified
                datasource.execute(("DROP TABLE IF EXISTS $build_within_buffer; CREATE TABLE $build_within_buffer AS"+
                                    " SELECT b.$idField, a.$geometricField FROM $inputBuildingTableName a, $build_buffer b "+
                                    "WHERE a.$geometricField && b.$geometricField AND "+
                                    "ST_INTERSECTS(a.$geometricField, b.$geometricField) AND a.$idField <> b.$idField;"+
                                    "CREATE INDEX IF NOT EXISTS with_buff_id ON $build_within_buffer($idField);"+
                                    "CREATE INDEX IF NOT EXISTS with_buff_ids ON $build_within_buffer($geometricField) "+
                                    "USING RTREE").toString())
                // The minimum distance is calculated
                datasource.execute(("DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                                    "SELECT MIN(ST_DISTANCE(a.$geometricField, b.$geometricField)) AS building_minimum_building_spacing, "+
                                    "a.${inputFields.join(",a.")} FROM $build_within_buffer b RIGHT JOIN $inputBuildingTableName a ON a.$idField = b.$idField"+
                                    " GROUP BY a.$idField").toString())
                // The minimum distance is set to the $inputE value for buildings having no building neighbors in a $inputE meters distance
                datasource.execute(("UPDATE $outputTableName SET building_minimum_building_spacing = $bufferDist "+
                                    "WHERE building_minimum_building_spacing IS null").toString())
                // The temporary tables are deleted
                datasource.execute("DROP TABLE IF EXISTS $build_buffer, $build_within_buffer".toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * This process extract the building closest distance to a road. A buffer of defined size (bufferDist argument)
 * is used to get the roads within the building of interest and then the minimum distance is calculated.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingRoadDistance() {
    return processFactory.create(
            "Building road distance",
            [inputBuildingTableName: String, inputRoadTableName: String, inputFields:String[], bufferDist: Double
             , outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, inputRoadTableName, inputFields, bufferDist = 100, outputTableName, datasource ->
                def geometricField = "the_geom"
                def idFieldBu = "id_build"
                def road_width = "width"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = System.currentTimeMillis()

                // Temporary table names
                def build_buffer = "build_buffer"+uid_out
                def road_surf = "road_surf"+uid_out
                def road_within_buffer = "road_within_buffer"+uid_out

                // The buffer is created
                datasource.execute(("CREATE TABLE $build_buffer AS SELECT $idFieldBu, ST_BUFFER($geometricField, $bufferDist)"+
                                " AS $geometricField FROM $inputBuildingTableName; "+
                                "CREATE INDEX IF NOT EXISTS buff_ids ON $build_buffer($geometricField) USING RTREE").toString())
                // The road surfaces are created
                datasource.execute(("CREATE TABLE $road_surf AS SELECT ST_BUFFER($geometricField, $road_width/2) "+
                                    "AS $geometricField FROM $inputRoadTableName; "+
                                    "CREATE INDEX IF NOT EXISTS buff_ids ON $road_surf($geometricField) USING RTREE").toString())
                // The roads located within the buffer are identified
                datasource.execute(("DROP TABLE IF EXISTS $road_within_buffer; CREATE TABLE $road_within_buffer AS "+
                                    "SELECT a.$idFieldBu, b.$geometricField FROM $build_buffer a, $road_surf b "+
                                    "WHERE a.$geometricField && b.$geometricField AND "+
                                    "ST_INTERSECTS(a.$geometricField, b.$geometricField); "+
                                    "CREATE INDEX IF NOT EXISTS with_buff_id ON $road_within_buffer($idFieldBu); "+
                                    "CREATE INDEX IF NOT EXISTS a_id ON $inputBuildingTableName($idFieldBu)").toString())
                // The minimum distance is calculated between each building and the surrounding roads
                datasource.execute(("DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                                    "SELECT LEAST(st_distance(a.$geometricField, b.$geometricField)) AS building_road_distance, "+
                                    "a.${inputFields.join(",a.")} FROM $road_within_buffer b RIGHT JOIN $inputBuildingTableName a "+
                                    "ON a.$idFieldBu = b.$idFieldBu GROUP BY a.$idFieldBu").toString())
                // The minimum distance is set to the bufferDist value for buildings having no road within a bufferDist meters distance
                datasource.execute(("UPDATE $outputTableName SET building_road_distance = $bufferDist "+
                                    "WHERE building_road_distance IS null").toString())
                // The temporary tables are deleted
                datasource.execute("DROP TABLE IF EXISTS $build_buffer, $road_within_buffer, $road_surf".toString())
                
                [outputTableName: outputTableName]
            }
    )}