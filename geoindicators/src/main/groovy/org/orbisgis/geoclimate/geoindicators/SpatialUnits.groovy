/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.h2.value.ValueGeometry
import org.h2gis.functions.spatial.create.ST_MakeGrid
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.data.api.dataset.ISpatialTable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException

import static org.h2gis.network.functions.ST_ConnectedComponents.getConnectedComponents

@BaseScript Geoindicators geoindicators

/**
 * This process is used to create the Topographical Spatial Units used reference spatial units (RSU)
 *
 * @param inputzone The zone table to keep the RSU inside
 * Default value is empty so all RSU are kept.
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @param area TSU less or equals than area are removed, default 1d
 * @param road The road table to be processed
 * @param rail The rail table to be processed
 * @param vegetation The vegetation table to be processed
 * @param water The water table to be processed
 * @param sea_land_mask The table to distinguish sea from land
 * @param surface_vegetation A double value to select the vegetation geometry areas.
 * Expressed in geometry unit of the vegetationTable, default 10000
 * @param surface_hydro A double value to select the hydrographic geometry areas.
 * Expressed in geometry unit of the vegetationTable, default 2500
 *
 * @return A database table name and the name of the column ID
 */
String createTSU(JdbcDataSource datasource, String zone,
                 double area = 1f, String road, String rail, String vegetation,
                 String water, String sea_land_mask,
                 double surface_vegetation, double surface_hydro, String prefixName) {
    def BASE_NAME = "rsu"

    debug "Creating the reference spatial units"
    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_NAME
    datasource """DROP TABLE IF EXISTS $outputTableName;""".toString()

    def tsuDataPrepared = prepareTSUData(datasource,
            zone, road, rail,
            vegetation, water, sea_land_mask, surface_vegetation, surface_hydro, prefixName)
    if (!tsuDataPrepared) {
        info "Cannot prepare the data for RSU calculation."
        return
    }
    def outputTsuTableName = Geoindicators.SpatialUnits.createTSU(datasource, tsuDataPrepared, zone,
            area, prefixName)
    if (!outputTsuTableName) {
        info "Cannot compute the RSU."
        return
    }

    datasource """ALTER TABLE $outputTsuTableName RENAME TO $outputTableName;""".toString()

    debug "Reference spatial units table created"

    return outputTableName
}

/**
 * This process is used to create the Topographical Spatial Units (TSU)
 *
 * @param inputTableName The input spatial table to be processed
 * @param inputzone The zone table to keep the TSU inside
 * Default value is empty so all TSU are kept.
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 * @param area TSU less or equals than area are removed
 * @return A database table name and the name of the column ID
 */
String createTSU(JdbcDataSource datasource, String inputTableName, String inputzone, double area = 1d, String prefixName) {
    def COLUMN_ID_NAME = "id_rsu"
    def BASE_NAME = "tsu"

    debug "Creating the reference spatial units"
    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_NAME

    if (!inputTableName) {
        error "The input data to compute the TSU cannot be null or empty"
        return null
    }
    def epsg = datasource.getSpatialTable(inputTableName).srid

    if (area <= 0) {
        error "The area value to filter the TSU must be greater to 0"
        return null
    }
    if (inputzone) {
        datasource.createSpatialIndex(inputTableName,"the_geom")
        datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName AS 
                        SELECT EXPLOD_ID AS $COLUMN_ID_NAME, ST_SETSRID(a.the_geom, $epsg) AS the_geom
                        FROM ST_EXPLODE('(
                                SELECT ST_POLYGONIZE(ST_UNION(ST_NODE(ST_ACCUM(the_geom)))) AS the_geom 
                                FROM $inputTableName)') AS a,
                            $inputzone AS b
                        WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(ST_POINTONSURFACE(a.THE_GEOM), b.the_geom) and st_area(a.the_geom) > $area
            """.toString()
    } else {
        datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName AS 
                        SELECT EXPLOD_ID AS $COLUMN_ID_NAME, ST_SETSRID(ST_FORCE2D(the_geom), $epsg) AS the_geom 
                        FROM ST_EXPLODE('(
                                SELECT ST_POLYGONIZE(ST_UNION(ST_NODE(ST_ACCUM(the_geom)))) AS the_geom 
                                FROM $inputTableName)') where st_area(the_geom) > $area""".toString()
    }

    debug "Reference spatial units table created"

    return outputTableName
}

/**
 * This process is used to prepare the input abstract model
 * in order to compute the Topographical Spatial Units (TSU)
 *
 * @param zone The area of zone to be processed
 * @param road The road table to be processed
 * @param rail The rail table to be processed
 * @param vegetation The vegetation table to be processed
 * @param water The hydrographic table to be processed
 * @param surface_vegetation A double value to select the vegetation geometry areas.
 * Expressed in geometry unit of the vegetationTable. 10000 m² seems correct.
 * @param sea_land_mask The table to distinguish sea from land
 * @param surface_hydro A double value to select the hydrographic geometry areas.
 * Expressed in geometry unit of the vegetationTable. 2500 m² seems correct.
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 * @param outputTableName The name of the output table
 * @return A database table name.
 */
String prepareTSUData(JdbcDataSource datasource, String zone, String road, String rail,
                      String vegetation, String water, String sea_land_mask,
                      double surface_vegetation, double surface_hydro, String prefixName = "unified_abstract_model") {
    if (surface_vegetation <= 100) {
        error("The surface of vegetation must be greater or equal than 100 m²")
        return
    }
    if (surface_hydro <= 100) {
        error("The surface of water must be greater or equal than 100 m²")
        return
    }
    def BASE_NAME = "prepared_tsu_data"

    debug "Preparing the abstract model to build the TSU"

    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_NAME

    // Create temporary table names (for tables that will be removed at the end of the method)
    def vegetation_indice
    def vegetation_unified
    def vegetation_tmp
    def hydrographic_indice
    def hydrographic_unified
    def hydrographic_tmp

    def queryCreateOutputTable = [:]
    def dropTableList = []

    def numberZone = datasource.firstRow("SELECT COUNT(*) AS nb FROM $zone".toString()).nb

    if (numberZone == 1) {
        def epsg = datasource."$zone".srid
        //Add the land mask
        if (sea_land_mask && datasource.hasTable(sea_land_mask)) {
            debug "Preparing land mask..."
            queryCreateOutputTable += [land_mask_tmp: "(SELECT ST_ToMultiLine(THE_GEOM) FROM $sea_land_mask where type ='land')"]
        }
        if (vegetation && datasource.hasTable(vegetation)) {
                debug "Preparing vegetation..."
                vegetation_indice = postfix vegetation
                vegetation_unified = postfix "vegetation_unified"
                vegetation_tmp = postfix "vegetation_tmp"

                datasource "DROP TABLE IF EXISTS " + vegetation_indice
                datasource "CREATE TABLE " + vegetation_indice + "(THE_GEOM geometry, ID serial," +
                        " CONTACT integer) AS (SELECT the_geom, EXPLOD_ID, 0 FROM ST_EXPLODE('" +
                        "(SELECT * FROM " + vegetation + " WHERE ZINDEX=0)') " +
                        " WHERE ST_DIMENSION(the_geom)>0 AND ST_ISEMPTY(the_geom)=FALSE)"
                datasource """CREATE SPATIAL INDEX IF NOT EXISTS veg_indice_idx ON $vegetation_indice (THE_GEOM);
                        UPDATE $vegetation_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)
                                 FROM $vegetation_indice a, $vegetation_indice b WHERE a.THE_GEOM && b.THE_GEOM AND 
                                ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)""".toString()

                datasource "DROP TABLE IF EXISTS " + vegetation_unified
                datasource "CREATE TABLE " + vegetation_unified + " AS " +
                        "(SELECT ST_SETSRID(the_geom," + epsg + ") AS the_geom FROM ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM))" +
                        " AS THE_GEOM FROM " + vegetation_indice + " WHERE CONTACT=1)') " +
                        "WHERE ST_DIMENSION(the_geom)>0 AND ST_ISEMPTY(the_geom)=FALSE AND " +
                        "ST_AREA(the_geom)> " + surface_vegetation + ") " +
                        "UNION ALL (SELECT THE_GEOM FROM " + vegetation_indice + " WHERE contact=0 AND " +
                        "ST_AREA(the_geom)> " + surface_vegetation + ")"

                datasource "CREATE SPATIAL INDEX IF NOT EXISTS veg_unified_idx ON $vegetation_unified (THE_GEOM)"

                datasource """DROP TABLE IF EXISTS $vegetation_tmp;
                        CREATE TABLE $vegetation_tmp AS SELECT a.the_geom AS THE_GEOM FROM 
                                $vegetation_unified AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                                AND ST_INTERSECTS(a.the_geom, b.the_geom)""".toString()

                queryCreateOutputTable += [vegetation_tmp: "(SELECT ST_ToMultiLine(THE_GEOM) AS THE_GEOM FROM $vegetation_tmp)"]
                dropTableList.addAll([vegetation_indice,
                                      vegetation_unified,
                                      vegetation_tmp])

        }

        if (water && datasource.hasTable(water)) {
            if (datasource."$water") {
                //Extract water
                debug "Preparing hydrographic..."
                hydrographic_indice = postfix water
                hydrographic_unified = postfix "hydrographic_unified"
                hydrographic_tmp = postfix "hydrographic_tmp"

                datasource "DROP TABLE IF EXISTS $hydrographic_indice".toString()
                datasource("""CREATE TABLE  $hydrographic_indice (THE_GEOM geometry, ID serial,
                                 CONTACT integer) AS (SELECT st_makevalid(THE_GEOM) AS the_geom, EXPLOD_ID , 0 FROM 
                                ST_EXPLODE('(SELECT * FROM $water WHERE ZINDEX=0)') 
                                 WHERE ST_DIMENSION(the_geom)>0 AND ST_ISEMPTY(the_geom)=false)""").toString()

                datasource """CREATE SPATIAL INDEX IF NOT EXISTS hydro_indice_idx ON $hydrographic_indice (THE_GEOM);
                          CREATE INDEX IF NOT EXISTS hydro_indice_id ON $hydrographic_indice (ID); 
                         UPDATE $hydrographic_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)
                                 FROM $hydrographic_indice a, $hydrographic_indice b WHERE a.THE_GEOM && b.THE_GEOM
                                 AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID);
                        CREATE INDEX ON $hydrographic_indice (contact)""".toString()


                datasource """DROP TABLE IF EXISTS $hydrographic_unified;
                        CREATE TABLE $hydrographic_unified AS (SELECT ST_SETSRID(the_geom, $epsg) as the_geom FROM 
                                ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM
                                 $hydrographic_indice  WHERE CONTACT=1)') where st_dimension(the_geom)>0
                                 AND st_isempty(the_geom)=false AND st_area(the_geom)> $surface_hydro) 
                                 UNION ALL (SELECT  the_geom FROM $hydrographic_indice WHERE contact=0 AND 
                                 st_area(the_geom)> $surface_hydro)""".toString()


                datasource """CREATE SPATIAL INDEX IF NOT EXISTS hydro_unified_idx ON $hydrographic_unified (THE_GEOM);
                        DROP TABLE IF EXISTS $hydrographic_tmp;
                        CREATE TABLE $hydrographic_tmp AS SELECT a.the_geom 
                                 AS THE_GEOM FROM $hydrographic_unified AS a, $zone AS b 
                                WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)""".toString()

                queryCreateOutputTable += [hydrographic_tmp: "(SELECT ST_ToMultiLine(THE_GEOM) as the_geom FROM $hydrographic_tmp)"]
                dropTableList.addAll([hydrographic_indice,
                                      hydrographic_unified,
                                      hydrographic_tmp])
            }
        }

        if (road && datasource.hasTable(road)) {
            if (datasource."$road") {
                debug "Preparing road..."
                queryCreateOutputTable += [road_tmp: "(SELECT ST_ToMultiLine(THE_GEOM) FROM $road where (zindex=0 or crossing = 'bridge') and type!='service')"]
            }
        }

        if (rail && datasource.hasTable(rail) && !datasource."$rail".isEmpty()) {
            if (datasource."$rail") {
                debug "Preparing rail..."
                queryCreateOutputTable += [rail_tmp: "(SELECT ST_ToMultiLine(THE_GEOM) FROM $rail where zindex=0 or crossing = 'bridge')"]
            }
        }

        // The input table that contains the geometries to be transformed as TSU
        debug "Grouping all tables..."
        if (queryCreateOutputTable) {
            datasource """
                        DROP TABLE if exists $outputTableName;
                        CREATE TABLE $outputTableName(the_geom GEOMETRY) AS 
                            (
                                SELECT st_setsrid(ST_ToMultiLine(THE_GEOM),$epsg) as the_geom
                                FROM $zone) 
                            UNION ${queryCreateOutputTable.values().join(' union ')};
                        DROP TABLE IF EXISTS ${queryCreateOutputTable.keySet().join(' , ')}
                """.toString()
        } else {
            datasource """DROP TABLE if exists $outputTableName;
            CREATE TABLE $outputTableName(the_geom GEOMETRY) AS (SELECT st_setsrid(ST_ToMultiLine(THE_GEOM),$epsg) 
            FROM $zone);""".toString()
        }
        if (dropTableList) {
            datasource "DROP TABLE IF EXISTS ${dropTableList.join(',')};".toString()
        }
        debug "TSU created..."

    } else {
        error "Cannot compute the TSU. The input zone table must have one row."
        outputTableName = null
    }
    return outputTableName
}

/**
 * This process is used to merge the geometries that touch each other
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputTableName The input table tos create the block (group of geometries)
 * @param snappingTolerance A distance to group the geometries
 * @param prefixName A prefix used to name the output table
 * @param outputTableName The name of the output table
 * @return A database table name and the name of the column ID
 */
String createBlocks(JdbcDataSource datasource, String inputTableName, double snappingTolerance = 0.0d, String prefixName = "block") {

    def BASE_NAME = "blocks"

    debug "Creating the blocks..."

    def columnIdName = "id_block"

    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_NAME
    //Find all neighbors for each building
    debug "Building index to perform the process..."
    datasource.createSpatialIndex(inputTableName,"the_geom")
    datasource.createIndex(inputTableName, "id_build")

    debug "Building spatial clusters..."

    // Create temporary table names (for tables that will be removed at the end of the method)
    def graphTable = postfix "spatial_clusters"
    def subGraphTableNodes = postfix graphTable, "NODE_CC"
    def subGraphTableEdges = postfix graphTable, "EDGE_CC"
    def subGraphBlocks = postfix "subgraphblocks"

    datasource """
                DROP TABLE IF EXISTS $graphTable; 
                CREATE TABLE $graphTable (EDGE_ID SERIAL, START_NODE INT, END_NODE INT) AS 
                    SELECT CAST((row_number() over()) as Integer), a.id_build as START_NODE, b.id_build AS END_NODE 
                    FROM $inputTableName AS a, $inputTableName AS b 
                    WHERE a.id_build<>b.id_build AND a.the_geom && b.the_geom 
                    AND ST_DWITHIN(b.the_geom,a.the_geom, $snappingTolerance);
        """.toString()

    datasource "DROP TABLE IF EXISTS $subGraphTableEdges, $subGraphTableNodes;".toString()

    getConnectedComponents(datasource.getConnection(), graphTable, "undirected")

    //Unify buildings that share a boundary
    debug "Merging spatial clusters..."

    if (snappingTolerance > 0) {
        datasource """
                    CREATE INDEX ON $subGraphTableNodes (NODE_ID);
                    DROP TABLE IF EXISTS $subGraphBlocks;
                    CREATE TABLE $subGraphBlocks AS
                        SELECT ST_UNION(ST_ACCUM(ST_buffer(A.THE_GEOM, $snappingTolerance))) AS THE_GEOM
                        FROM $inputTableName A, $subGraphTableNodes B
                        WHERE A.id_build=B.NODE_ID GROUP BY B.CONNECTED_COMPONENT;
            """.toString()
    } else {
        datasource """
        CREATE INDEX ON $subGraphTableNodes (NODE_ID);
        DROP TABLE IF EXISTS $subGraphBlocks;
        CREATE TABLE $subGraphBlocks
        AS SELECT ST_UNION(ST_ACCUM(ST_MAKEVALID(A.THE_GEOM))) AS THE_GEOM
        FROM $inputTableName A, $subGraphTableNodes B
        WHERE A.id_build=B.NODE_ID GROUP BY B.CONNECTED_COMPONENT;""".toString()
    }
    //Create the blocks
    debug "Creating the block table..."

    datasource """DROP TABLE IF EXISTS $outputTableName; 
        CREATE TABLE $outputTableName ($columnIdName SERIAL, THE_GEOM GEOMETRY) 
        AS SELECT CAST((row_number() over()) as Integer), the_geom FROM 
        ((SELECT st_force2d(ST_MAKEVALID(THE_GEOM)) as the_geom FROM $subGraphBlocks) 
        UNION ALL (SELECT  st_force2d(ST_MAKEVALID(a.the_geom)) as the_geom FROM $inputTableName a 
        LEFT JOIN $subGraphTableNodes b ON a.id_build = b.NODE_ID WHERE b.NODE_ID IS NULL));""".toString()

    // Temporary tables are deleted
    datasource """DROP TABLE IF EXISTS  $graphTable, ${graphTable + "_EDGE_CC"}, 
                    $subGraphBlocks, ${subGraphBlocks + "_NODE_CC"}, $subGraphTableNodes;""".toString()

    debug "The blocks have been created"
    return outputTableName
}
/**
 * This process is used to spatially link polygons coming from two tables. It may be used to find the relationships
 * between a building and a block, a building and a RSU but also between a building from a first dataset and a building
 * from a second dataset (the datasets may come from a different data provider or from a different year).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param sourceTable A first input table where are stored polygons (note that all columns will be conserved in the resulting table)
 * @param targetTable A second input table where are stored polygons
 * @param idColumnTarget The column name where is stored the ID of table target
 * @param prefixName A prefix used to name the output table
 * @param pointOnSurface Whether or not the spatial join may be performed on pointOnSurfaceTypes of spatial join that may be used (default false):
 *          --> True: polygons from the first table are converted to points before to be spatially join with polygons from the second table
 *          --> False: polygons from the first table are directly spatially joined with polygons from the second table
 * @param nbRelations If 'pointOnSurface' is False, number of relations that one polygon in Table 2 may have with
 * polygons from Table 1 (e.g. if nbRelations = 1 for buildings and RSU, the buildings can have only one RSU as relation).
 * The selection of which polygon(s) need to be conserved is based on shared polygons area. By default, this parameter
 * has no value and thus all relations are conserved
 *
 * @return outputTableName A table name containing ID from table 1, ID from table 2 and AREA shared by the two objects (if pointOnSurface = false)
 */
String spatialJoin(JdbcDataSource datasource, String sourceTable, String targetTable,
                   String idColumnTarget, boolean pointOnSurface = false, Integer nbRelations, String prefixName) {
    def GEOMETRIC_COLUMN_SOURCE = "the_geom"
    def GEOMETRIC_COLUMN_TARGET = "the_geom"

    debug "Creating a spatial join between objects from two tables :  $sourceTable and $targetTable"

    // The name of the outputTableName is constructed (the prefix name is not added since it is already contained
    // in the inputLowerScaleTableName object
    def outputTableName = postfix "${sourceTable}_${targetTable}", "join"
    datasource.createSpatialIndex(sourceTable, "the_geom")
    datasource.createSpatialIndex(targetTable,"the_geom")

    if (pointOnSurface) {
        datasource """    DROP TABLE IF EXISTS $outputTableName;
                                CREATE TABLE $outputTableName AS SELECT a.*, b.$idColumnTarget 
                                        FROM $sourceTable a, $targetTable b 
                                        WHERE   ST_POINTONSURFACE(a.$GEOMETRIC_COLUMN_SOURCE) && b.$GEOMETRIC_COLUMN_TARGET AND 
                                                ST_INTERSECTS(ST_POINTONSURFACE(a.$GEOMETRIC_COLUMN_SOURCE), b.$GEOMETRIC_COLUMN_TARGET)""".toString()
    } else {
        if (nbRelations != null) {
            datasource """  DROP TABLE IF EXISTS $outputTableName;
                                    CREATE TABLE $outputTableName 
                                            AS SELECT   a.*, 
                                                        (SELECT b.$idColumnTarget 
                                                            FROM $targetTable b 
                                                            WHERE a.$GEOMETRIC_COLUMN_SOURCE && b.$GEOMETRIC_COLUMN_TARGET AND 
                                                                 ST_INTERSECTS(a.$GEOMETRIC_COLUMN_SOURCE, 
                                                                                            b.$GEOMETRIC_COLUMN_TARGET) 
                                                        ORDER BY ST_AREA(ST_INTERSECTION(ST_BUFFER(ST_PRECISIONREDUCER(a.$GEOMETRIC_COLUMN_SOURCE, 3),0),
                                                                                         ST_BUFFER(ST_PRECISIONREDUCER(b.$GEOMETRIC_COLUMN_TARGET,3),0)))
                                                        DESC LIMIT $nbRelations) AS $idColumnTarget 
                                            FROM $sourceTable a""".toString()
        } else {
            datasource """  DROP TABLE IF EXISTS $outputTableName;
                                    CREATE TABLE $outputTableName 
                                            AS SELECT   a.*, b.$idColumnTarget,
                                                        ST_AREA(ST_INTERSECTION(ST_PRECISIONREDUCER(a.$GEOMETRIC_COLUMN_SOURCE, 3), 
                                                        ST_PRECISIONREDUCER(b.$GEOMETRIC_COLUMN_TARGET,3))) AS AREA
                                            FROM    $sourceTable a, $targetTable b
                                            WHERE   a.$GEOMETRIC_COLUMN_SOURCE && b.$GEOMETRIC_COLUMN_TARGET AND 
                                                    ST_INTERSECTS(a.$GEOMETRIC_COLUMN_SOURCE, b.$GEOMETRIC_COLUMN_TARGET);""".toString()
        }
    }

    debug "The spatial join have been performed between :  $sourceTable and $targetTable"

    return outputTableName
}

/**
 * This process is used to generate a continuous cartesian grid
 * on which indicators have to be aggregated.
 *
 * @param geometry A geometry that defines either Point, Line or Polygon
 * @param deltaX The horizontal spatial step of a cell in meter
 * @param deltaY The vertical spatial step of a cell in meter
 * @param tableName A Table that contains the geometry of the grid
 * @param datasource A connexion to a database (H2GIS, POSTGIS, ...) where are stored the input Table and in which
 *        the resulting database will be stored
 * @return The name of the created table
 *
 * @author Emmanuel Renault, CNRS, 2020
 * */
String createGrid(JdbcDataSource datasource, Geometry geometry, double deltaX, double deltaY, boolean rowCol = false, String prefixName = "") {
    if (rowCol) {
        if (!deltaX || !deltaY || deltaX < 1 || deltaY < 1) {
            debug "Invalid grid size padding. Must be greater or equal than 1"
            return
        }
    } else {
        if (!deltaX || !deltaY || deltaX <= 0 || deltaY <= 0) {
            error "Invalid grid size padding. Must be greater than 0"
            return
        }
    }
    if (!geometry) {
        error "The envelope is null or empty. Cannot compute the grid"
        return
    }

    def BASENAME = "grid"
    def outputTableName = prefix prefixName, BASENAME
    datasource "DROP TABLE IF EXISTS $outputTableName;"

    if (datasource instanceof H2GIS) {
        debug "Creating grid with H2GIS"
        datasource """
                           CREATE TABLE $outputTableName AS SELECT the_geom, id as id_grid,ID_COL, ID_ROW FROM 
                           ST_MakeGrid(st_geomfromtext('$geometry',${geometry.getSRID()}), $deltaX, $deltaY,$rowCol);
                           """.toString()
    } else if (datasource instanceof POSTGIS) {
        debug "Creating grid with POSTGIS"
        PreparedStatement preparedStatement = null
        Connection outputConnection = datasource.getConnection()
        try {
            def createTable = "CREATE TABLE $outputTableName(THE_GEOM GEOMETRY(POLYGON), ID_GRID INT, ID_COL INT, ID_ROW INT);"
            def insertTable = "INSERT INTO $outputTableName VALUES (?, ?, ?, ?);"
            datasource.execute(createTable.toString())
            preparedStatement = outputConnection.prepareStatement(insertTable.toString())
            def result = ST_MakeGrid.createGrid(outputConnection, ValueGeometry.getFromGeometry(geometry), deltaX, deltaY, rowCol)
            long batch_size = 0
            int batchSize = 1000

            while (result.next()) {
                preparedStatement.setObject(1, result.getObject(1))
                preparedStatement.setObject(2, result.getInt(2))
                preparedStatement.setObject(3, result.getInt(3))
                preparedStatement.setObject(4, result.getInt(4))
                preparedStatement.addBatch()
                batch_size++
                if (batch_size >= batchSize) {
                    preparedStatement.executeBatch()
                    preparedStatement.clearBatch()
                    batchSize = 0;
                }
            }
            if (batch_size > 0) {
                preparedStatement.executeBatch()
            }
        } catch (SQLException e) {
            error("Cannot create the grid with the parameters.\n", e)
            return null
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close()
            }
        }
    }
    debug "The table $outputTableName has been created"
    return outputTableName
}