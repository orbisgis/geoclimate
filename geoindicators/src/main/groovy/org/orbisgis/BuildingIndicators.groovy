package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

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
static IProcess geometryProperties() {
return processFactory.create(
        "Geometry properties",
        [inputTableName: String,inputFields:String[],operations: String[]
         , prefixName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { inputTableName,inputFields, operations, prefixName, datasource ->

            def geometricField = "the_geom";
            def ops = ["st_geomtype","st_srid", "st_length","st_perimeter","st_area", "st_dimension",
                   "st_coorddim", "st_num_geoms", "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]

            // The name of the outputTableName is constructed
            String baseName = "geometry_properties"
            String outputTableName = prefixName + "_" + baseName

            String query = "CREATE TABLE $outputTableName AS SELECT "

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
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param operations Operations that have to be applied. These operations should be in the following list:
 *          --> "building_volume": defined as the building area multiplied by the mean of the building
 *          wall height and the building roof height.
 *          --> "building_floor_area": defined as the number of level multiplied by the building area
 *          (cf. Bocher et al. - 2018)
 *          --> "building_total_facade_length": defined as the total linear of facade (sum of the building perimeter
 *          and the perimeter of the building courtyards)
 * @param prefixName String use as prefix to name the output table
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
            [inputBuildingTableName: String, operations: String[]
             , prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, operations, prefixName, datasource ->

                def geometricField = "the_geom"
                def columnIdBu ="id_build"
                def dist_passiv = 3
                def ops = ["building_volume","building_floor_area", "building_total_facade_length",
                           "building_passive_volume_ratio"]

                // The name of the outputTableName is constructed
                String baseName = "building_size_properties"
                String outputTableName = prefixName + "_" + baseName

                String query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

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
                query+= "$columnIdBu FROM $inputBuildingTableName"
                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}


/**
 * This process extract building interactions properties.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param operations Operations that have to be applied. These operations should be in the following list:
 *              --> "building_contiguity": defined as the shared wall area divided by the total building wall area
 *              (cf. Bocher et al. - 2018)
 *              --> "building_common_wall_fraction": defined as ratio between the lenght of building wall shared with other buildings
 *              and the length of total building walls
 *              --> "building_number_building_neighbor": defined as the number of building  neighbors in contact with the building
 *              (cf. Bocher et al. - 2018)
 * @param prefixName String use as prefix to name the output table

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
            [inputBuildingTableName: String, operations: String[]
             , prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, operations, prefixName, datasource ->
                def geometricField = "the_geom"
                def idField = "id_build"
                def height_wall = "height_wall"
                def ops = ["building_contiguity","building_common_wall_fraction",
                           "building_number_building_neighbor"]
                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")
                // Temporary table names
                def build_intersec = "build_intersec"+uid_out

                // The name of the outputTableName is constructed
                String baseName = "building_neighbors_properties"
                String outputTableName = prefixName + "_" + baseName

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
                query+= "a.$idField FROM $inputBuildingTableName a, $inputBuildingTableName b" +
                        " WHERE a.$geometricField && b.$geometricField AND " +
                        "ST_INTERSECTS(a.$geometricField, b.$geometricField) AND a.$idField <> b.$idField" +
                        " GROUP BY a.$idField;" +
                        "CREATE INDEX IF NOT EXISTS buff_id ON $build_intersec($idField);" +
                        "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                        "SELECT b.${operations.join(",b.")}, a.$idField" +
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
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param operations Operations that have to be applied. These operations should be in the following list:
 *              --> "building_concavity": defined as the building area divided by the convex hull area (cf. Bocher et al. - 2018)
 *              --> "building_form_factor": defined as ratio between the building area divided by the square of the building
 *              perimeter (cf. Bocher et al. - 2018)
 *              --> "building_raw_compacity": defined as the ratio between building surfaces (walls and roof) divided by the
 *              building volume at the power 2./3. For the calculation, the roof is supposed to have a gable and the roof surface
 *              is calculated considering that the building is square (otherwise, the assumption related to the gable direction
 *              would strongly affect the result).
 *              --> "building_convexhull_perimeter_density": defined as the ratio between building convexhull perimeter and
 *              building perimeter.
 * @param prefixName String use as prefix to name the output table
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
            [inputBuildingTableName: String,operations: String[]
             , prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, operations, prefixName, datasource ->
                def geometricField = "the_geom"
                def idField = "id_build"
                def height_wall = "height_wall"
                def height_roof = "height_roof"
                def ops = ["building_concavity","building_form_factor",
                           "building_raw_compacity", "building_convexhull_perimeter_density"]

                // The name of the outputTableName is constructed
                String baseName = "building_form_properties"
                String outputTableName = prefixName + "_" + baseName

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
                    else if(operation=="building_convexhull_perimeter_density") {
                        query += "ST_PERIMETER(ST_CONVEXHULL($geometricField))/(ST_PERIMETER($geometricField)+" +
                                "ST_PERIMETER(ST_HOLES($geometricField))) AS $operation,"
                    }
                }
                query+= "$idField FROM $inputBuildingTableName"

                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}

/**
 * This process extract the building closest distance to an other building. A buffer of defined size (bufferDist argument,
 * default 100 m) is used to get the buildings within the building of interest and then the minimum distance is calculated.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param bufferDist Distance (in meter) used to consider the neighbors of a building. If there is no building within this buffer
 * distance of a building, the minimum distance is set to this value.
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingMinimumBuildingSpacing() {
    return processFactory.create(
            "Building minimum building spacing",
            [inputBuildingTableName: String,bufferDist: double
             , prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, bufferDist = 100, prefixName, datasource ->
                def geometricField = "the_geom"
                def idField = "id_build"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def build_buffer = "build_buffer"+uid_out
                def build_within_buffer = "build_within_buffer"+uid_out

                // The name of the outputTableName is constructed
                String baseName = "building_minimum_building_spacing"
                String outputTableName = prefixName + "_" + baseName

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
                // The minimum distance is calculated (The minimum distance is set to the $inputE value for buildings
                // having no building neighbors in a bufferDist meters distance
                datasource.execute(("DROP TABLE IF EXISTS $outputTableName; CREATE TABLE " +
                        "$outputTableName(building_minimum_building_spacing DOUBLE, $idField INTEGER)" +
                        " AS (SELECT COALESCE(MIN(ST_DISTANCE(a.$geometricField, b.$geometricField)), $bufferDist), a.$idField " +
                        "FROM $build_within_buffer b RIGHT JOIN $inputBuildingTableName a ON a.$idField = b.$idField"+
                        " GROUP BY a.$idField)").toString())
                // The temporary tables are deleted
                datasource.execute("DROP TABLE IF EXISTS $build_buffer, $build_within_buffer".toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * This process extract the building closest distance to a road. A buffer of defined size (bufferDist argument)
 * is used to get the roads within the building of interest and then the minimum distance is calculated.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param inputRoadTableName The name of the input ITable where are stored the road geometries
 * @param bufferDist Distance (in meter) used to consider the neighbors of a building. If there is no road within
 * this buffer distance of a building, the minimum distance to a road is set to this value.
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess buildingRoadDistance() {
    return processFactory.create(
            "Building road distance",
            [inputBuildingTableName: String, inputRoadTableName: String, bufferDist: double
             , prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputBuildingTableName, inputRoadTableName, bufferDist = 100, prefixName, datasource ->
                def geometricField = "the_geom"
                def idFieldBu = "id_build"
                def road_width = "width"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def build_buffer = "build_buffer"+uid_out
                def road_surf = "road_surf"+uid_out
                def road_within_buffer = "road_within_buffer"+uid_out

                // The name of the outputTableName is constructed
                String baseName = "building_road_distance"
                String outputTableName = prefixName + "_" + baseName

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
                // The minimum distance is calculated between each building and the surrounding roads (he minimum distance
                // is set to the bufferDist value for buildings having no road within a bufferDist meters distance)
                datasource.execute(("DROP TABLE IF EXISTS $outputTableName; CREATE TABLE " +
                        "$outputTableName(building_road_distance DOUBLE, $idFieldBu INTEGER) AS "+
                                    "(SELECT COALESCE(LEAST(st_distance(a.$geometricField, b.$geometricField)), $bufferDist), "+
                                    "a.$idFieldBu FROM $road_within_buffer b RIGHT JOIN $inputBuildingTableName a "+
                                    "ON a.$idFieldBu = b.$idFieldBu GROUP BY a.$idFieldBu)").toString())

                // The temporary tables are deleted
                datasource.execute("DROP TABLE IF EXISTS $build_buffer, $road_within_buffer, $road_surf".toString())
                
                [outputTableName: outputTableName]
            }
    )}