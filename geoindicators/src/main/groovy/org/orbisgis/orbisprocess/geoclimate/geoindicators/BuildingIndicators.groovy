package org.orbisgis.orbisprocess.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.*
import org.orbisgis.orbisdata.processmanager.process.*

@BaseScript GroovyProcessFactory pf

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
 *
 * @author Jérémy Bernard
 */
create {
    title "Building size properties"
    id "sizeProperties"
    inputs inputBuildingTableName: String, operations: String[], prefixName: String, datasource: JdbcDataSource
    outputs outputTableName: String
    run { inputBuildingTableName, operations, prefixName, datasource ->

        def OP_VOLUME = "volume"
        def OP_FLOOR_AREA = "floor_area"
        def OP_FACADE_LENGTH = "total_facade_length"
        def OP_PASSIVE_VOLUME_RATIO = "passive_volume_ratio"

        def GEOMETRIC_FIELD = "the_geom"
        def COLUMN_ID_BU = "id_build"
        def DIST_PASSIV = 3
        def BASE_NAME = "building_size_properties"

        info "Executing Building size properties"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        def query = "DROP TABLE IF EXISTS $outputTableName; " +
                "CREATE TABLE $outputTableName AS SELECT "

        // The operation names are transformed into lower case
        operations.replaceAll { it.toLowerCase() }
        operations.each {
            switch (it) {
                case OP_VOLUME:
                    query += "ST_AREA($GEOMETRIC_FIELD)*0.5*(height_wall+height_roof) AS $OP_VOLUME,"
                    break
                case OP_FLOOR_AREA:
                    query += "ST_AREA($GEOMETRIC_FIELD)*nb_lev AS $OP_FLOOR_AREA,"
                    break
                case OP_FACADE_LENGTH:
                    query += "ST_PERIMETER($GEOMETRIC_FIELD)+ST_PERIMETER(ST_HOLES($GEOMETRIC_FIELD))" +
                            " AS $OP_FACADE_LENGTH,"
                    break
                case OP_PASSIVE_VOLUME_RATIO:
                    query += "ST_AREA(ST_BUFFER($GEOMETRIC_FIELD, -$DIST_PASSIV, 'join=mitre'))/" +
                            "ST_AREA($GEOMETRIC_FIELD) AS $OP_PASSIVE_VOLUME_RATIO,"
                    break
            }
        }
        query += "$COLUMN_ID_BU FROM $inputBuildingTableName"

        datasource query
        [outputTableName: outputTableName]
    }
}


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
 *
 * @author Jérémy Bernard
 */
create {
    title "Building interactions properties"
    id "neighborsProperties"
    inputs inputBuildingTableName: String, operations: String[], prefixName: String, datasource: JdbcDataSource
    outputs outputTableName: String
    run { inputBuildingTableName, operations, prefixName, datasource ->

        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD = "id_build"
        def HEIGHT_WALL = "height_wall"
        def OP_CONTIGUITY = "contiguity"
        def OP_COMMON_WALL_FRACTION = "common_wall_fraction"
        def OP_NUMBER_BUILDING_NEIGHBOR = "number_building_neighbor"
        def OPS = [OP_CONTIGUITY, OP_COMMON_WALL_FRACTION, OP_NUMBER_BUILDING_NEIGHBOR]
        def BASE_NAME = "building_neighbors_properties"

        info "Executing Building interactions properties"
        // To avoid overwriting the output files of this step, a unique identifier is created
        // Temporary table names
        def build_intersec = postfix "build_intersec"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        datasource."$inputBuildingTableName".the_geom.createIndex()
        datasource."$inputBuildingTableName".id_build.createIndex()

        def query = " CREATE TABLE $build_intersec AS SELECT "

        // The operation names are transformed into lower case
        operations.replaceAll { it.toLowerCase() }
        operations.each { 
            switch (it) {
                case OP_CONTIGUITY:
                    query += """sum(least(a_height_wall, b_height_wall)* 
                            st_length(the_geom)/(perimeter* a_height_wall)) AS $it,"""
                    break
                case OP_COMMON_WALL_FRACTION:
                    query += """SUM(st_length(the_geom)/perimeter)
                             AS $it,"""
                    break
                case OP_NUMBER_BUILDING_NEIGHBOR:
                    query += "COUNT($ID_FIELD) AS $it,"
                    break
            }
        }
        def list = []
        operations.each { 
            list << "(CASE WHEN b.$it is null then 0 else b.$it END) as $it"
        }
        query += """$ID_FIELD FROM (
                SELECT 
                    ST_INTERSECTION(ST_MAKEVALID(a.$GEOMETRIC_FIELD),
                    ST_MAKEVALID(b.$GEOMETRIC_FIELD)) AS the_geom,
                    a.$ID_FIELD, 
                    ST_PERIMETER(a.$GEOMETRIC_FIELD) + ST_PERIMETER(ST_HOLES(a.$GEOMETRIC_FIELD)) AS perimeter, 
                    a.$HEIGHT_WALL AS a_height_wall, 
                    b.$HEIGHT_WALL AS b_height_wall 
                FROM $inputBuildingTableName a, $inputBuildingTableName b 
                WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD 
                    AND ST_INTERSECTS(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD) 
                    AND a.$ID_FIELD <> b.$ID_FIELD)
                GROUP BY $ID_FIELD;
                CREATE INDEX IF NOT EXISTS buff_id ON $build_intersec USING BTREE($ID_FIELD);
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName AS 
                    SELECT  ${list.join(",")} ,  a.$ID_FIELD
                    FROM $inputBuildingTableName a 
                    LEFT JOIN $build_intersec b 
                    ON a.$ID_FIELD = b.$ID_FIELD;"""
        
        // The temporary tables are deleted
        query += "DROP TABLE IF EXISTS $build_intersec"

        datasource query
        [outputTableName: outputTableName]
    }
}


/**
 * This process extract building form properties.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputBuildingTableName The name of the input ITable where are stored the building geometries
 * @param operations Operations that have to be applied. These operations should be in the following list:
 *              --> "area_concavity": defined as the building area divided by the convex hull area (cf. Bocher et al. - 2018)
 *              --> "form_factor": defined as ratio between the building area divided by the square of the building
 *              perimeter (cf. Bocher et al. - 2018)
 *              --> "raw_compactness": defined as the ratio between building surfaces (walls and roof) divided by the
 *              building volume at the power 2./3. For the calculation, the roof is supposed to have a gable and the roof surface
 *              is calculated considering that the building is square (otherwise, the assumption related to the gable direction
 *              would strongly affect the result).
 *              --> "perimeter_convexity": defined as the ratio between building convexhull perimeter and
 *              building perimeter.
 * @param prefixName String use as prefix to name the output table
 *
 * References:
 *   Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 *
 * @return A database table name.
 *
 * @author Jérémy Bernard
 */
create {
    title "Building form properties"
    id "formProperties"
    inputs inputBuildingTableName: String, operations: String[], prefixName: String, datasource: JdbcDataSource
    outputs outputTableName: String
    run { inputBuildingTableName, operations, prefixName, datasource ->

        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD = "id_build"
        def HEIGHT_WALL = "height_wall"
        def HEIGHT_ROOF = "height_roof"
        def OP_CONCAVITY = "area_concavity"
        def OP_FORM_FACTOR = "form_factor"
        def OP_RAW_COMPACTNESS = "raw_compactness"
        def OP_CONVEXITY = "perimeter_convexity"
        def BASE_NAME = "building_form_properties"

        info "Executing Building form properties"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        def query = "DROP TABLE IF EXISTS $outputTableName; " +
                "CREATE TABLE $outputTableName AS SELECT "

        // The operation names are transformed into lower case
        operations.replaceAll { it.toLowerCase() }
        operations.each {
            switch (it) {
                case OP_CONCAVITY:
                    query += "ST_AREA($GEOMETRIC_FIELD)/ST_AREA(ST_CONVEXHULL($GEOMETRIC_FIELD)) AS $it,"
                    break
                case OP_FORM_FACTOR:
                    query += "ST_AREA($GEOMETRIC_FIELD)/POWER(ST_PERIMETER($GEOMETRIC_FIELD), 2) AS $it,"
                    break
                case OP_RAW_COMPACTNESS:
                    query += "((ST_PERIMETER($GEOMETRIC_FIELD)+ST_PERIMETER(ST_HOLES($GEOMETRIC_FIELD)))*$HEIGHT_WALL+" +
                            "POWER(POWER(ST_AREA($GEOMETRIC_FIELD),2)+4*ST_AREA($GEOMETRIC_FIELD)*" +
                            "POWER($HEIGHT_ROOF-$HEIGHT_WALL, 2),0.5)+POWER(ST_AREA($GEOMETRIC_FIELD),0.5)*" +
                            "($HEIGHT_ROOF-$HEIGHT_WALL))/POWER(ST_AREA($GEOMETRIC_FIELD)*" +
                            "($HEIGHT_WALL+$HEIGHT_ROOF)/2, 2./3) AS $it,"
                    break
                case OP_CONVEXITY:
                    query += "ST_PERIMETER(ST_CONVEXHULL($GEOMETRIC_FIELD))/(ST_PERIMETER($GEOMETRIC_FIELD)+ST_PERIMETER(ST_HOLES($GEOMETRIC_FIELD)))" +
                            " AS $it,"
                    break
            }
        }
        query += "$ID_FIELD FROM $inputBuildingTableName"

        datasource query
        [outputTableName: outputTableName]
    }
}

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
 *
 * @author Jérémy Bernard
 * @author Erwan Bocher
 */
create {
    title "Building minimum building spacing"
    id "minimumBuildingSpacing"
    inputs inputBuildingTableName: String, bufferDist: 100D, prefixName: String, datasource: JdbcDataSource
    outputs outputTableName: String
    run { inputBuildingTableName, bufferDist, prefixName, datasource ->

        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD = "id_build"
        def BASE_NAME = "minimum_building_spacing"

        info "Executing Building minimum building spacing"

        // To avoid overwriting the output files of this step, a unique identifier is created
        // Temporary table names
        def build_min_distance = postfix "build_min_distance"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "building_" + BASE_NAME

        datasource."$inputBuildingTableName".the_geom.createIndex()
        datasource."$inputBuildingTableName".id_build.createIndex()

        datasource """
                DROP TABLE IF EXISTS $build_min_distance; 
                CREATE TABLE $build_min_distance AS 
                    SELECT b.$ID_FIELD, 
                        min(ST_distance(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)) AS min_distance 
                    FROM $inputBuildingTableName a, $inputBuildingTableName b 
                    WHERE st_expand(a.$GEOMETRIC_FIELD, $bufferDist) && b.$GEOMETRIC_FIELD 
                    AND a.$ID_FIELD <> b.$ID_FIELD 
                    GROUP BY b.$ID_FIELD;
                 CREATE INDEX IF NOT EXISTS with_buff_id ON $build_min_distance USING BTREE($ID_FIELD); """

        // The minimum distance is calculated (The minimum distance is set to the $inputE value for buildings
        // having no building neighbors in a envelope meters distance
        datasource """DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName($ID_FIELD INTEGER, $BASE_NAME FLOAT) AS 
                    SELECT a.$ID_FIELD, 
                        CASE WHEN b.min_distance IS NOT NULL 
                            THEN b.min_distance 
                            ELSE 100 END 
                    FROM $inputBuildingTableName a LEFT JOIN $build_min_distance b 
                    ON a.$ID_FIELD = b.$ID_FIELD """
        // The temporary tables are deleted
        datasource "DROP TABLE IF EXISTS $build_min_distance"

        [outputTableName: outputTableName]
    }
}

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
 *
 * @author Jérémy Bernard
 */
create {
    title "Building road distance"
    id "roadDistance"
    inputs inputBuildingTableName: String, inputRoadTableName: String, bufferDist: 100D, prefixName: String, datasource: JdbcDataSource
    outputs outputTableName: String
    run { inputBuildingTableName, inputRoadTableName, bufferDist, prefixName, datasource ->

        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD_BU = "id_build"
        def ROAD_WIDTH = "width"
        def BASE_NAME = "road_distance"

        info "Executing Building road distance"

        // To avoid overwriting the output files of this step, a unique identifier is created
        // Temporary table names
        def build_buffer = postfix "build_buffer"
        def road_surf = postfix"road_surf"
        def road_within_buffer = postfix "road_within_buffer"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "building_" + BASE_NAME

        datasource."$inputBuildingTableName".id_build.createIndex()

        // The buffer is created
        datasource """DROP TABLE IF EXISTS $build_buffer;
                CREATE TABLE $build_buffer AS
                    SELECT $ID_FIELD_BU,  ST_BUFFER($GEOMETRIC_FIELD, $bufferDist) AS $GEOMETRIC_FIELD 
                    FROM $inputBuildingTableName;
                CREATE INDEX IF NOT EXISTS buff_ids ON $build_buffer USING RTREE($GEOMETRIC_FIELD)"""
        // The road surfaces are created
        datasource """
                DROP TABLE IF EXISTS $road_surf;
                CREATE TABLE $road_surf AS 
                    SELECT ST_BUFFER($GEOMETRIC_FIELD, $ROAD_WIDTH::double precision/2,'endcap=flat') AS $GEOMETRIC_FIELD 
                    FROM $inputRoadTableName; 
                CREATE INDEX IF NOT EXISTS buff_ids ON $road_surf USING RTREE($GEOMETRIC_FIELD)"""
        // The roads located within the buffer are identified
        datasource """
                DROP TABLE IF EXISTS $road_within_buffer; 
                CREATE TABLE $road_within_buffer AS 
                    SELECT a.$ID_FIELD_BU, b.$GEOMETRIC_FIELD 
                    FROM $build_buffer a, $road_surf b 
                    WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD 
                    AND ST_INTERSECTS(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD); 
                CREATE INDEX IF NOT EXISTS with_buff_id ON $road_within_buffer USING BTREE($ID_FIELD_BU); """

        // The minimum distance is calculated between each building and the surrounding roads (the minimum
        // distance is set to the bufferDist value for buildings having no road within a bufferDist meters
        // distance)
        datasource """
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName($BASE_NAME DOUBLE, $ID_FIELD_BU INTEGER) AS (
                    SELECT COALESCE(MIN(st_distance(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)), $bufferDist), a.$ID_FIELD_BU 
                    FROM $road_within_buffer b 
                    RIGHT JOIN $inputBuildingTableName a 
                    ON a.$ID_FIELD_BU = b.$ID_FIELD_BU 
                    GROUP BY a.$ID_FIELD_BU)"""

        // The temporary tables are deleted
        datasource "DROP TABLE IF EXISTS $build_buffer, $road_within_buffer, $road_surf"

        [outputTableName: outputTableName]
    }
}

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
 *
 * @author Jérémy Bernard
 *
 */
create {
    title "Building closeness to a 50 m wide building"
    id "likelihoodLargeBuilding"
    inputs inputBuildingTableName: String, nbOfBuildNeighbors: String, prefixName: String, datasource: JdbcDataSource
    outputs outputTableName: String
    run { inputBuildingTableName, nbOfBuildNeighbors, prefixName, datasource ->

        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD_BU = "id_build"
        def BASE_NAME = "likelihood_large_building"

        info "Executing Building closeness to a 50 m wide building"

        // Processes used for the indicator calculation
        // a and r are the two parameters necessary for the logistic regression calculation (their value is
        // set according to the training sample of the MaPuce dataset)
        def a = Math.exp(6.5)
        def r = 0.25

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "building_" + BASE_NAME

        datasource.getSpatialTable(inputBuildingTableName).id_build.createIndex()

        // The calculation of the logistic function is performed only for buildings having no neighbors
        datasource """DROP TABLE IF EXISTS $outputTableName; 
                 CREATE TABLE $outputTableName AS 
                    SELECT a.$ID_FIELD_BU, 
                        CASEWHEN(
                            a.$nbOfBuildNeighbors>0, 
                            0, 
                            1/(1+$a*exp(-$r*st_maxdistance(a.$GEOMETRIC_FIELD, b.$GEOMETRIC_FIELD)))) 
                        AS $BASE_NAME 
                 FROM $inputBuildingTableName a 
                 LEFT JOIN $inputBuildingTableName b 
                 ON a.$ID_FIELD_BU = b.$ID_FIELD_BU"""

        [outputTableName: outputTableName]
    }
}