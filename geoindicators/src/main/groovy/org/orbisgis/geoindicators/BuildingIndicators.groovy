package org.orbisgis.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoindicators geoindicators

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
IProcess sizeProperties() {
    def final OP_VOLUME = "building_volume"
    def final OP_FLOOR_AREA = "building_floor_area"
    def final OP_FACADE_LENGTH = "building_total_facade_length"
    def final OP_PASSIVE_VOLUME_RATIO = "building_passive_volume_ratio"

    def final GEOMETRIC_FIELD = "the_geom"
    def final COLUMN_ID_BU ="id_build"
    def final DIST_PASSIV = 3
    def final BASE_NAME = "building_size_properties"

    return create({
        title "Building size properties"
        inputs inputBuildingTableName: String, operations: String[], prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputBuildingTableName, operations, prefixName, datasource ->

            logger.info("Executing Building size properties")

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            def query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

            // The operation names are transformed into lower case
            operations.replaceAll { s -> s.toLowerCase() }
            operations.each { operation ->
                switch (operation) {
                    case OP_VOLUME:
                        query += "ST_AREA($GEOMETRIC_FIELD)*0.5*(height_wall+height_roof) AS building_volume,"
                        break
                    case OP_FLOOR_AREA:
                        query += "ST_AREA($GEOMETRIC_FIELD)*nb_lev AS building_floor_area,"
                        break
                    case OP_FACADE_LENGTH:
                        query += "ST_PERIMETER($GEOMETRIC_FIELD)+ST_PERIMETER(ST_HOLES($GEOMETRIC_FIELD))" +
                                " AS building_total_facade_length,"
                        break
                    case OP_PASSIVE_VOLUME_RATIO:
                        query += "ST_AREA(ST_BUFFER($GEOMETRIC_FIELD, -$DIST_PASSIV, 'join=mitre'))/" +
                                "ST_AREA($GEOMETRIC_FIELD) AS building_passive_volume_ratio,"
                        break
                }
            }
            query += "$COLUMN_ID_BU FROM $inputBuildingTableName"

            datasource.execute query
            [outputTableName: outputTableName]
        }
    })}


/**
 * This process extract building interactions properties.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param operations Operations that have to be applied. These operations should be in the following list:
 *              --> "building_contiguity": defined as the shared wall area divided by the total building wall area
 *              (cf. Bocher et al. - 2018)
 *              --> "building_common_wall_fraction": defined as ratio between the lenght of building wall shared with
 *              other buildings and the length of total building walls
 *              --> "building_number_building_neighbor": defined as the number of building  neighbors in contact with
 *              the building
 *              (cf. Bocher et al. - 2018)
 * @param prefixName String use as prefix to name the output table

 * References:
 *   Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess neighborsProperties() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD = "id_build"
    def final HEIGHT_WALL = "height_wall"
    def final OP_CONTIGUITY = "building_contiguity"
    def final OP_COMMON_WALL_FRACTION = "building_common_wall_fraction"
    def final OP_NUMBER_BUILDING_NEIGHBOR = "building_number_building_neighbor"
    def final OPS = [OP_CONTIGUITY, OP_COMMON_WALL_FRACTION, OP_NUMBER_BUILDING_NEIGHBOR]
    def final BASE_NAME = "building_neighbors_properties"

    return create({
        title "Building interactions properties"
        inputs inputBuildingTableName: String, operations: String[], prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputBuildingTableName, operations, prefixName, datasource ->

            logger.info("Executing Building interactions properties")
            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def build_intersec = "build_intersec" + uuid()

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            def query = "CREATE INDEX IF NOT EXISTS buff_ids ON $inputBuildingTableName($GEOMETRIC_FIELD) " +
                    "USING RTREE; " +
                    "CREATE INDEX IF NOT EXISTS buff_id ON $inputBuildingTableName($ID_FIELD);" +
                    " CREATE TABLE $build_intersec AS SELECT "

            def query_update = ""

            // The operation names are transformed into lower case
            operations.replaceAll { s -> s.toLowerCase() }
            operations.each { operation ->
                switch (operation) {
                    case OP_CONTIGUITY:
                        query += "sum(least(a.$HEIGHT_WALL, b.$HEIGHT_WALL)*" +
                                "st_length(ST_INTERSECTION(a.$GEOMETRIC_FIELD,b.$GEOMETRIC_FIELD)))/" +
                                "((ST_PERIMETER(a.$GEOMETRIC_FIELD)+" +
                                "ST_PERIMETER(ST_HOLES(a.$GEOMETRIC_FIELD)))*a.$HEIGHT_WALL)" +
                                " AS $operation,"
                        break
                    case OP_COMMON_WALL_FRACTION:
                        query += "sum(ST_LENGTH(ST_INTERSECTION(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)))/" +
                                "(ST_PERIMETER(a.$GEOMETRIC_FIELD)+ST_PERIMETER(ST_HOLES(a.$GEOMETRIC_FIELD))) " +
                                "AS $operation,"
                        break
                    case OP_NUMBER_BUILDING_NEIGHBOR:
                        query += "COUNT(ST_INTERSECTION(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD))" +
                                " AS $operation,"
                        break
                }
                // The buildingNeighborProperty is set to 0 for the buildings that have no intersection with their
                // building neighbors
                if (OPS.contains(operation)) {
                    query_update += "UPDATE $outputTableName SET $operation = 0 WHERE $operation IS null;"
                }
            }
            query += "a.$ID_FIELD FROM $inputBuildingTableName a, $inputBuildingTableName b" +
                    " WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD AND " +
                    "ST_INTERSECTS(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD) AND a.$ID_FIELD <> b.$ID_FIELD" +
                    " GROUP BY a.$ID_FIELD;" +
                    "CREATE INDEX IF NOT EXISTS buff_id ON $build_intersec($ID_FIELD);" +
                    "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                    "SELECT b.${operations.join(",b.")}, a.$ID_FIELD" +
                    " FROM $inputBuildingTableName a LEFT JOIN $build_intersec b ON a.$ID_FIELD = b.$ID_FIELD;"
            query += query_update

            // The temporary tables are deleted
            query += "DROP TABLE IF EXISTS $build_intersec"

            datasource.execute query
            [outputTableName: outputTableName]
        }
    })}


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
IProcess formProperties() {

    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD = "id_build"
    def final HEIGHT_WALL = "height_wall"
    def final HEIGHT_ROOF = "height_roof"
    def final OP_CONCAVITY = "building_concavity"
    def final OP_FORM_FACTOR = "building_form_factor"
    def final OP_RAW_COMPACITY = "building_raw_compacity"
    def final OP_CONVEX_HULL_PERIMETER_DENSITY = "building_convexhull_perimeter_density"
    def final BASE_NAME = "building_form_properties"

    return create({
            title "Building form properties"
            inputs inputBuildingTableName: String,operations: String[], prefixName: String, datasource: JdbcDataSource
            outputs outputTableName : String
            run { inputBuildingTableName, operations, prefixName, datasource ->

                logger.info("Executing Building form properties")

                // The name of the outputTableName is constructed
                def outputTableName = prefixName + "_" + BASE_NAME

                def query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

                // The operation names are transformed into lower case
                operations.replaceAll {s -> s.toLowerCase()}
                operations.each {operation ->
                    switch(operation){
                        case OP_CONCAVITY:
                            query += "ST_AREA($GEOMETRIC_FIELD)/ST_AREA(ST_CONVEXHULL($GEOMETRIC_FIELD)) AS $operation,"
                            break
                        case OP_FORM_FACTOR:
                            query += "ST_AREA($GEOMETRIC_FIELD)/POWER(ST_PERIMETER($GEOMETRIC_FIELD), 2) AS $operation,"
                            break
                        case OP_RAW_COMPACITY:
                            query += "((ST_PERIMETER($GEOMETRIC_FIELD)+ST_PERIMETER(ST_HOLES($GEOMETRIC_FIELD)))*$HEIGHT_WALL+" +
                                    "POWER(POWER(ST_AREA($GEOMETRIC_FIELD),2)+4*ST_AREA($GEOMETRIC_FIELD)*" +
                                    "POWER($HEIGHT_ROOF-$HEIGHT_WALL, 2),0.5)+POWER(ST_AREA($GEOMETRIC_FIELD),0.5)*" +
                                    "($HEIGHT_ROOF-$HEIGHT_WALL))/POWER(ST_AREA($GEOMETRIC_FIELD)*" +
                                    "($HEIGHT_WALL+$HEIGHT_ROOF)/2, 2./3) AS $operation,"
                            break
                        case OP_CONVEX_HULL_PERIMETER_DENSITY:
                            query += "ST_PERIMETER(ST_CONVEXHULL($GEOMETRIC_FIELD))/(ST_PERIMETER($GEOMETRIC_FIELD)+" +
                                    "ST_PERIMETER(ST_HOLES($GEOMETRIC_FIELD))) AS $operation,"
                            break
                    }
                }
                query+= "$ID_FIELD FROM $inputBuildingTableName"

                datasource.execute query
                [outputTableName: outputTableName]
            }
})}

/**
 * This process extract the building closest distance to an other building. A buffer of defined size (bufferDist
 * argument, default 100 m) is used to get the buildings within the building of interest and then the minimum distance
 * is calculated.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param bufferDist Distance (in meter) used to consider the neighbors of a building. If there is no building within
 * this buffer distance of a building, the minimum distance is set to this value.
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess minimumBuildingSpacing() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD = "id_build"
    def final BASE_NAME = "building_minimum_building_spacing"

    return create({
        title "Building minimum building spacing"
        inputs inputBuildingTableName: String, bufferDist: 100D, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputBuildingTableName, bufferDist, prefixName, datasource ->
            logger.info("Executing Building minimum building spacing")

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def build_buffer = "build_buffer" + uuid()
            def build_within_buffer = "build_within_buffer" + uuid()

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // The buffer is created
            datasource.execute "CREATE TABLE $build_buffer AS SELECT $ID_FIELD, " +
                    "ST_BUFFER($GEOMETRIC_FIELD, $bufferDist) AS $GEOMETRIC_FIELD FROM $inputBuildingTableName; " +
                    "CREATE INDEX IF NOT EXISTS buff_ids ON $build_buffer($GEOMETRIC_FIELD) USING RTREE;" +
                    "CREATE INDEX IF NOT EXISTS inpute_ids ON $inputBuildingTableName($GEOMETRIC_FIELD) USING RTREE;" +
                    "CREATE INDEX IF NOT EXISTS buff_id ON $build_buffer($ID_FIELD); " +
                    "CREATE INDEX IF NOT EXISTS inpute_id ON $inputBuildingTableName($ID_FIELD)"
            // The building located within the buffer are identified
            datasource.execute "DROP TABLE IF EXISTS $build_within_buffer; CREATE TABLE $build_within_buffer AS" +
                    " SELECT b.$ID_FIELD, a.$GEOMETRIC_FIELD FROM $inputBuildingTableName a, $build_buffer b " +
                    "WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD AND " +
                    "ST_INTERSECTS(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD) AND a.$ID_FIELD <> b.$ID_FIELD;" +
                    "CREATE INDEX IF NOT EXISTS with_buff_id ON $build_within_buffer($ID_FIELD);" +
                    "CREATE INDEX IF NOT EXISTS with_buff_ids ON $build_within_buffer($GEOMETRIC_FIELD) USING RTREE"
            // The minimum distance is calculated (The minimum distance is set to the $inputE value for buildings
            // having no building neighbors in a bufferDist meters distance
            datasource.execute "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE " +
                    "$outputTableName(building_minimum_building_spacing DOUBLE, $ID_FIELD INTEGER)" +
                    " AS (SELECT COALESCE(MIN(ST_DISTANCE(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)), $bufferDist), " +
                    "a.$ID_FIELD " +
                    "FROM $build_within_buffer b RIGHT JOIN $inputBuildingTableName a ON a.$ID_FIELD = b.$ID_FIELD" +
                    " GROUP BY a.$ID_FIELD)"
            // The temporary tables are deleted
            datasource.execute "DROP TABLE IF EXISTS $build_buffer, $build_within_buffer"

            [outputTableName: outputTableName]
        }
    })}

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
IProcess roadDistance() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD_BU = "id_build"
    def final ROAD_WIDTH = "width"
    def final BASE_NAME = "building_road_distance"

    return create({
        title "Building road distance"
        inputs inputBuildingTableName: String, inputRoadTableName: String, bufferDist: 100D, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputBuildingTableName, inputRoadTableName, bufferDist, prefixName, datasource ->

            logger.info("Executing Building road distance")

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def build_buffer = "build_buffer" + uuid()
            def road_surf = "road_surf" + uuid()
            def road_within_buffer = "road_within_buffer" + uuid()

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // The buffer is created
            datasource.execute "DROP TABLE IF EXISTS $build_buffer; " +
                    "CREATE TABLE $build_buffer AS SELECT $ID_FIELD_BU," +
                    " ST_BUFFER($GEOMETRIC_FIELD, $bufferDist)" +
                    " AS $GEOMETRIC_FIELD FROM $inputBuildingTableName; " +
                    "CREATE INDEX IF NOT EXISTS buff_ids ON $build_buffer($GEOMETRIC_FIELD) USING RTREE"
            // The road surfaces are created
            datasource.execute "CREATE TABLE $road_surf AS " +
                    "SELECT ST_BUFFER($GEOMETRIC_FIELD, $ROAD_WIDTH/2,'endcap=flat') " +
                    "AS $GEOMETRIC_FIELD FROM $inputRoadTableName; " +
                    "CREATE INDEX IF NOT EXISTS buff_ids ON $road_surf($GEOMETRIC_FIELD) USING RTREE"
            // The roads located within the buffer are identified
            datasource.execute "DROP TABLE IF EXISTS $road_within_buffer; CREATE TABLE $road_within_buffer AS " +
                    "SELECT a.$ID_FIELD_BU, b.$GEOMETRIC_FIELD FROM $build_buffer a, $road_surf b " +
                    "WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD AND " +
                    "ST_INTERSECTS(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD); " +
                    "CREATE INDEX IF NOT EXISTS with_buff_id ON $road_within_buffer($ID_FIELD_BU); " +
                    "CREATE INDEX IF NOT EXISTS a_id ON $inputBuildingTableName($ID_FIELD_BU)"
            // The minimum distance is calculated between each building and the surrounding roads (he minimum
            // distance is set to the bufferDist value for buildings having no road within a bufferDist meters
            // distance)
            datasource.execute "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE " +
                    "$outputTableName(building_road_distance DOUBLE, $ID_FIELD_BU INTEGER) AS " +
                    "(SELECT COALESCE(MIN(st_distance(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)), $bufferDist), " +
                    "a.$ID_FIELD_BU FROM $road_within_buffer b RIGHT JOIN $inputBuildingTableName a " +
                    "ON a.$ID_FIELD_BU = b.$ID_FIELD_BU GROUP BY a.$ID_FIELD_BU)"

            // The temporary tables are deleted
            datasource.execute "DROP TABLE IF EXISTS $build_buffer, $road_within_buffer, $road_surf"

            [outputTableName: outputTableName]
        }
    })}

/**
 * Script to compute the building closeness to a 50 m wide isolated building ("building_number_building_neighbor" = 0).
 * The step 9 of the manual decision tree for building type of the classification consists of checking whether
 * buildings have a horizontal extent larger than 50 m. We therefore introduce an indicator which  measures the
 * horizontal extent of buildings. This indicator is based on the largest side of the building  minimum rectangle.
 * We use a logistic function to avoid threshold effects (e.g. totally different result for building sizes
 * of 49 m and 51 m). The gamma and x0 parameters in the logistic function are specified after analysis of the
 * training data to identify the real size of the buildings classified as larger than 50 m in the
 * subjective training process.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildingTableName The name of the input ITable where are stored the building geometries
 * @param nbOfBuildNeighbors The field name corresponding to the "building_number_building_neighbor" (in the
 * "buildingTableName" ITable)
 * @param prefixName String used as prefix to name the output table
 *
 * References:
 *   Amossé, A. Identification automatique d'une typologie urbaine des îlots urbains en France. Tech. rep., Laboratoire
 * de recherche en architecture, Laboratoire de recherche en architecture, Toulouse, France, 2015.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 *
 */
IProcess likelihoodLargeBuilding() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD_BU = "id_build"
    def final BASE_NAME = "building_likelihood_large_building"

    return create({
        title "Building closeness to a 50 m wide building"
        inputs inputBuildingTableName: String, nbOfBuildNeighbors: String, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputBuildingTableName, nbOfBuildNeighbors, prefixName, datasource ->

            logger.info("Executing Building closeness to a 50 m wide building")

            // Processes used for the indicator calculation
            // a and r are the two parameters necessary for the logistic regression calculation (their value is
            // set according to the training sample of the MaPuce dataset)
            def a = Math.exp(6.5)
            def r = 0.25

            // The name of the outputTableName is constructed
            String outputTableName = prefixName + "_" + BASE_NAME

            // The calculation of the logistic function is performed only for buildings having no neighbors
            datasource.execute "CREATE INDEX ON $inputBuildingTableName($ID_FIELD_BU); " +
                    "DROP TABLE IF EXISTS $outputTableName; " +
                    "CREATE TABLE $outputTableName AS SELECT a.$ID_FIELD_BU, " +
                    "CASEWHEN(a.$nbOfBuildNeighbors>0, 0," +
                    " 1/(1+$a*exp(-$r*st_maxdistance(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)))) AS $BASE_NAME " +
                    "FROM $inputBuildingTableName a LEFT JOIN $inputBuildingTableName b " +
                    "ON a.$ID_FIELD_BU = b.$ID_FIELD_BU"

            [outputTableName: outputTableName]
        }
    })
}