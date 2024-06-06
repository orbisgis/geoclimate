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
package org.orbisgis.geoclimate.osmtools

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.jdbc.JdbcDataSource

import static org.orbisgis.geoclimate.osmtools.utils.GeometryTypes.LINES
import static org.orbisgis.geoclimate.osmtools.utils.GeometryTypes.POLYGONS

@BaseScript OSMTools pf

/**
 * This process is used to extract all the points from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all points
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
String toPoints(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [],
                Geometry geometry) {
    String outputTableName = postfix "OSM_POINTS"
    def pointsNodes = OSMTools.TransformUtils.extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode, outputTableName, tags, columnsToKeep, geometry)
    if (pointsNodes) {
        debug "The points have been built."
    } else {
        warn "Cannot extract any point."
        return
    }
    return outputTableName
}

/**
 * This process is used to extract all the points from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all points
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
String toPoints(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = []) {
    String outputTableName = postfix "OSM_POINTS"
    def pointsNodes = OSMTools.TransformUtils.extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode, outputTableName, tags, columnsToKeep, null)
    if (pointsNodes) {
        debug "The points have been built."
    } else {
        warn "Cannot extract any point."
        return
    }
    return outputTableName
}


/**
 * This process is used to extract all the lines from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
String toLines(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = []) {
    return OSMTools.TransformUtils.toPolygonOrLine(LINES, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, null)
}

/**
 * This process is used to extract all the lines from the OSM tables
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 * @param geometry a geometry to reduce the area
 *
 * @return outputTableName a name for the table that contains all lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
String toLines(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], Geometry geometry) {
    return OSMTools.TransformUtils.toPolygonOrLine(LINES, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry)
}
/**
 * This process is used to extract all the polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 * @param geometry a geometry to reduce the area
 * @param valid_geom true to valid the geometries
 *
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 */
String toPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], Geometry geometry, boolean valid_geom) {
    return OSMTools.TransformUtils.toPolygonOrLine(POLYGONS, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry, valid_geom)
}

/**
 * This process is used to extract all the polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 * @param geometry a geometry to reduce the area
 *
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
String toPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], Geometry geometry) {
    return OSMTools.TransformUtils.toPolygonOrLine(POLYGONS, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry, false)
}

/**
 * This process is used to extract all the polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 */
String toPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = []) {
    return OSMTools.TransformUtils.toPolygonOrLine(POLYGONS, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, null)
}

/**
 * This process is used to extract all the polygons from the OSM tables
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 * @param valid_geom true to valid the geometries
 *
 * @return outputTableName a name for the table that contains all polygons
 * @author Erwan Bocher CNRS LAB-STICC
 */
String toPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], boolean  valid_geom) {
    return OSMTools.TransformUtils.toPolygonOrLine(POLYGONS, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, null, valid_geom)
}

/**
 * This process is used to extract ways as polygons
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all ways transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
String extractWaysAsPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [],  boolean valid_geom = false) {
    return extractWaysAsPolygons( datasource,  osmTablesPrefix,  epsgCode ,  tags ,  columnsToKeep ,  null, valid_geom)
}

/**
 * This process is used to extract ways as polygons
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all ways transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
String extractWaysAsPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], Geometry geometry, boolean valid_geom = false) {
    if (!datasource) {
        error "Please set a valid database connection"
        return
    }
    if (epsgCode == -1) {
        error "Invalid EPSG code : $epsgCode"
        return
    }
    if (tags == null) {
        error "The tag list cannot be null"
        return
    }

    def outputTableName = postfix "WAYS_POLYGONS"
    def idWaysPolygons = postfix "ID_WAYS_POLYGONS"
    def osmTableTag = prefix osmTablesPrefix, "way_tag"
    def countTagsQuery = OSMTools.TransformUtils.getCountTagsQuery(osmTableTag, tags)
    def columnsSelector = OSMTools.TransformUtils.getColumnSelector(osmTableTag, tags, columnsToKeep)
    def tagsFilter = OSMTools.TransformUtils.createWhereFilter(tags)

    if (!datasource.hasTable(osmTableTag)) {
        debug "No tags table found to build the table. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
        return outputTableName
    }

    if (datasource.firstRow(countTagsQuery.toString()).count <= 0) {
        debug "No keys or values found to extract ways. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
            """.toString()
        return outputTableName
    }

    debug "Build way polygons"
    def waysPolygonTmp = postfix "WAYS_POLYGONS_TMP"
    datasource "DROP TABLE IF EXISTS $waysPolygonTmp;".toString()

    if (tagsFilter) {
        datasource """
                    DROP TABLE IF EXISTS $idWaysPolygons;
                    CREATE TABLE $idWaysPolygons AS
                        SELECT DISTINCT id_way
                        FROM $osmTableTag as a
                        WHERE $tagsFilter;
                    CREATE INDEX ON $idWaysPolygons(id_way);
            """.toString()
    } else {
        datasource """
                    DROP TABLE IF EXISTS $idWaysPolygons;
                    CREATE TABLE $idWaysPolygons AS
                        SELECT DISTINCT id_way
                        FROM $osmTableTag;
                    CREATE INDEX ON $idWaysPolygons(id_way);
            """.toString()
    }

    if (columnsToKeep) {
        if (datasource.firstRow("""
                    SELECT count(*) AS count 
                    FROM $idWaysPolygons AS a, ${osmTablesPrefix}_WAY_TAG AS b 
                    WHERE a.ID_WAY = b.ID_WAY AND b.TAG_KEY IN ('${columnsToKeep.join("','")}')
            """.toString())[0] < 1) {
            debug "Any columns to keep. Cannot create any geometry polygons. An empty table will be returned."
            datasource """
                        DROP TABLE IF EXISTS $outputTableName;
                        CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
            return outputTableName
        }
    }

    datasource """
                CREATE TABLE $waysPolygonTmp AS
                    SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), $epsgCode) AS the_geom, id_way
                    FROM(
                        SELECT(
                            SELECT ST_ACCUM(the_geom) AS the_geom
                            FROM(
                                SELECT n.id_node, n.the_geom, wn.id_way idway
                                FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn
                                WHERE n.id_node = wn.id_node
                                ORDER BY wn.node_order)
                            WHERE  idway = w.id_way
                        ) the_geom ,w.id_way  
                        FROM ${osmTablesPrefix}_way w, $idWaysPolygons b
                        WHERE w.id_way = b.id_way
                    ) geom_table
                    WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) 
                    AND ST_NUMGEOMETRIES(the_geom) > 3;
                CREATE INDEX ON $waysPolygonTmp(id_way);
        """.toString()

    def allPolygonsTables = postfix "all_polygons_table"
    String query = """ DROP TABLE IF EXISTS $allPolygonsTables; 
                CREATE TABLE $allPolygonsTables AS 
                    SELECT 'w'||a.id_way AS id,"""
    if (valid_geom) {
        query += """ case when st_isvalid(a.the_geom) then a.the_geom else  st_makevalid(a.the_geom) end as the_geom ${OSMTools.TransformUtils.createTagList(datasource, columnsSelector, columnsToKeep)} 
               """
    } else {
        query += """ a.the_geom ${OSMTools.TransformUtils.createTagList(datasource, columnsSelector, columnsToKeep)}  """
    }

    query += " FROM $waysPolygonTmp AS a, $osmTableTag b WHERE a.id_way=b.id_way and st_isempty(a.the_geom)=false "

    if(columnsToKeep) {
        query += " AND b.TAG_KEY IN ('${columnsToKeep.join("','")}') "
    }

    //TODO : Due to some H2 limitations with the execution plan we create the whole table and then we apply a spatial filter
    //https://github.com/orbisgis/geoclimate/issues/876
    query += " GROUP BY  a.id_way;"
    datasource.execute(query)
    if (geometry) {
        def query_out  ="""DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName AS SELECT * FROM $allPolygonsTables as a where """
        int geom_srid = geometry.getSRID()
        if (geom_srid == -1) {
            query_out += " a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom, st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else if (geom_srid == epsgCode) {
            query_out += "  a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom,st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else {
            query_out += "  a.the_geom && st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode) and st_intersects(a.the_geom,st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode)) "
        }
        datasource.createSpatialIndex(allPolygonsTables, "the_geom")
        datasource """
                $query_out;
                DROP TABLE IF EXISTS $waysPolygonTmp, $idWaysPolygons, $allPolygonsTables;""".toString()
        return outputTableName
    }
    else{
        datasource """
                ALTER TABLE $allPolygonsTables RENAME TO $outputTableName;
                DROP TABLE IF EXISTS $waysPolygonTmp, $idWaysPolygons;
        """.toString()
        return outputTableName
    }
}

/**
 * This process is used to extract relations as polygons
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all relations transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
def extractRelationsAsPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], boolean valid_geom = false) {
    return extractRelationsAsPolygons(datasource, osmTablesPrefix, epsgCode, tags , columnsToKeep, null, valid_geom)
}

/**
 * This process is used to extract relations as polygons
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all relations transformed as polygons
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
def extractRelationsAsPolygons(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], org.locationtech.jts.geom.Geometry geometry, boolean valid_geom = false) {
    if (!datasource) {
        error "Please set a valid database connection"
        return
    }
    if (epsgCode == -1) {
        error "Invalid EPSG code : $epsgCode"
        return
    }

    if (tags == null) {
        error "The tag list cannot be null"
        return
    }

    def outputTableName = postfix "RELATION_POLYGONS"
    def osmTableTag = prefix osmTablesPrefix, "relation_tag"
    def countTagsQuery = OSMTools.TransformUtils.getCountTagsQuery(osmTableTag, tags)
    def columnsSelector = OSMTools.TransformUtils.getColumnSelector(osmTableTag, tags, columnsToKeep)
    def tagsFilter = OSMTools.TransformUtils.createWhereFilter(tags)

    if (!datasource.hasTable(osmTableTag)) {
        debug "No tags table found to build the table. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
        return outputTableName
    }

    if (datasource.firstRow(countTagsQuery.toString()).count <= 0) {
        debug "No keys or values found in the relations. An empty table will be returned."
        datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
            """.toString()
        return outputTableName
    }
    debug "Build outer polygons"
    def relationsPolygonsOuter = postfix "RELATIONS_POLYGONS_OUTER"
    def relationFilteredKeys = postfix "RELATION_FILTERED_KEYS"
    def outer_condition
    def inner_condition
    if (!tagsFilter) {
        outer_condition = "WHERE w.id_way = br.id_way AND br.role='outer'"
        inner_condition = "WHERE w.id_way = br.id_way AND br.role='inner'"
    } else {
        datasource """
                    DROP TABLE IF EXISTS $relationFilteredKeys;
                    CREATE TABLE $relationFilteredKeys AS 
                        SELECT DISTINCT id_relation
                        FROM ${osmTablesPrefix}_relation_tag as a 
                        WHERE $tagsFilter;
                    CREATE INDEX ON $relationFilteredKeys(id_relation);
            """.toString()

        if (columnsToKeep) {
            if (datasource.getRowCount(relationFilteredKeys)< 1) {
                debug "Any columns to keep. Cannot create any geometry polygons. An empty table will be returned."
                datasource """
                            DROP TABLE IF EXISTS $outputTableName;
                            CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                    """.toString()
                return outputTableName
            }
        }

        outer_condition = """, $relationFilteredKeys g 
                    WHERE br.id_relation=g.id_relation
                    AND w.id_way = br.id_way
                    AND br.role='outer'
            """
        inner_condition = """, $relationFilteredKeys g
                    WHERE br.id_relation=g.id_relation
                    AND w.id_way = br.id_way
                    AND br.role='inner'
            """
    }

    datasource.execute("""
                DROP TABLE IF EXISTS $relationsPolygonsOuter;
                CREATE TABLE $relationsPolygonsOuter AS 
                SELECT ST_LINEMERGE(ST_ACCUM(the_geom)) as the_geom, id_relation 
                FROM(
                    SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), $epsgCode) AS  the_geom, id_relation, role, id_way 
                    FROM(
                        SELECT(
                            SELECT ST_ACCUM(the_geom) the_geom 
                            FROM(
                                SELECT n.id_node, n.the_geom, wn.id_way idway 
                                FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                                WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                            WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                        FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br $outer_condition) geom_table
                        WHERE st_numgeometries(the_geom)>=2) 
                GROUP BY id_relation;
        """.toString())

    debug "Build inner polygons"
    def relationsPolygonsInner = postfix "RELATIONS_POLYGONS_INNER"
    datasource """
                DROP TABLE IF EXISTS $relationsPolygonsInner;
                CREATE TABLE $relationsPolygonsInner AS 
                SELECT ST_LINEMERGE(ST_ACCUM(the_geom)) the_geom, id_relation 
                FROM(
                    SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), $epsgCode) AS the_geom, id_relation, role, id_way 
                    FROM(     
                        SELECT(
                            SELECT ST_ACCUM(the_geom) the_geom 
                            FROM(
                                SELECT n.id_node, n.the_geom, wn.id_way idway 
                                FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                                WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                            WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
                        FROM ${osmTablesPrefix}_way w, ${osmTablesPrefix}_way_member br ${inner_condition}) geom_table 
                        WHERE st_numgeometries(the_geom)>=2) 
                GROUP BY id_relation;
        """.toString()

    debug "Explode outer polygons"
    def relationsPolygonsOuterExploded = postfix "RELATIONS_POLYGONS_OUTER_EXPLODED"
    datasource """
                DROP TABLE IF EXISTS $relationsPolygonsOuterExploded;
                CREATE TABLE $relationsPolygonsOuterExploded AS 
                    SELECT ST_MAKEPOLYGON(the_geom) AS the_geom, id_relation 
                    FROM st_explode('$relationsPolygonsOuter') 
                    WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom)
                    AND ST_NPoints(the_geom)>=4;
        """.toString()

    debug "Explode inner polygons"
    def relationsPolygonsInnerExploded = postfix "RELATIONS_POLYGONS_INNER_EXPLODED"
    datasource """
                DROP TABLE IF EXISTS $relationsPolygonsInnerExploded;
                CREATE TABLE $relationsPolygonsInnerExploded AS 
                    SELECT the_geom AS the_geom, id_relation 
                    FROM st_explode('$relationsPolygonsInner') 
                    WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom)
                    AND ST_NPoints(the_geom)>=4; 
        """.toString()

    debug "Build all polygon relations"
    def relationsMpHoles = postfix "RELATIONS_MP_HOLES"
    datasource """
                CREATE SPATIAL INDEX ON $relationsPolygonsOuterExploded (the_geom);
                CREATE SPATIAL INDEX ON $relationsPolygonsInnerExploded (the_geom);
                CREATE INDEX ON $relationsPolygonsOuterExploded(id_relation);
                CREATE INDEX ON $relationsPolygonsInnerExploded(id_relation);       
                DROP TABLE IF EXISTS $relationsMpHoles;
                CREATE TABLE $relationsMpHoles AS (
                    SELECT ST_MAKEPOLYGON(ST_EXTERIORRING(a.the_geom), ST_ACCUM(b.the_geom)) AS the_geom, a.ID_RELATION
                    FROM $relationsPolygonsOuterExploded AS a 
                    LEFT JOIN $relationsPolygonsInnerExploded AS b 
                    ON(
                        a.the_geom && b.the_geom 
                        AND st_contains(a.the_geom, b.THE_GEOM) 
                        AND a.ID_RELATION=b.ID_RELATION)
                    GROUP BY a.the_geom, a.id_relation)
                UNION(
                    SELECT a.the_geom as the_geom , a.ID_RELATION 
                    FROM $relationsPolygonsOuterExploded AS a 
                    LEFT JOIN  $relationsPolygonsInnerExploded AS b 
                    ON a.id_relation=b.id_relation 
                    WHERE b.id_relation IS NULL);
                CREATE INDEX ON $relationsMpHoles(id_relation);
        """.toString()

    String allRelationPolygons = postfix("all_relation_polygons")
    def query = """
                DROP TABLE IF EXISTS $allRelationPolygons;     
                CREATE TABLE $allRelationPolygons AS 
                    SELECT 'r'||a.id_relation AS id,"""
    if (valid_geom) {
        query += """ case when st_isvalid(a.the_geom) then a.the_geom else st_makevalid(a.the_geom) end as the_geom ${OSMTools.TransformUtils.createTagList(datasource, columnsSelector, columnsToKeep)}
        FROM $relationsMpHoles AS a, ${osmTablesPrefix}_relation_tag  b 
                    WHERE a.id_relation=b.id_relation and st_isempty(a.the_geom)=false """
    } else {
        query += """ st_normalize(a.the_geom) as the_geom ${OSMTools.TransformUtils.createTagList(datasource, columnsSelector, columnsToKeep)} 
        FROM $relationsMpHoles AS a, ${osmTablesPrefix}_relation_tag  b 
                    WHERE a.id_relation=b.id_relation and st_isempty(a.the_geom)=false  """
    }

    if(columnsToKeep) {
        query += " AND b.TAG_KEY IN ('${columnsToKeep.join("','")}') "
    }
    query += " GROUP BY  a.the_geom, a.id_relation;"
    datasource.execute(query.toString())

    if (geometry) {
        def query_out ="""DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName as SELECT * FROM $allRelationPolygons as a where"""
        int geom_srid = geometry.getSRID()
        if (geom_srid == -1) {
            query_out += " a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom, st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else if (geom_srid == epsgCode) {
            query_out += " a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom,st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else {
            query_out += " a.the_geom && st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode) and st_intersects(a.the_geom,st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode)) "
        }
        datasource.createSpatialIndex(allRelationPolygons, "the_geom")
        datasource.execute(query_out.toString())
        datasource.dropTable(relationsPolygonsOuter, relationsPolygonsInner, relationsPolygonsOuterExploded,
                relationsPolygonsInnerExploded,relationsMpHoles,relationFilteredKeys, allRelationPolygons)
        return outputTableName
    }else{
        datasource.execute("""ALTER TABLE $allRelationPolygons RENAME TO $outputTableName""".toString())
        datasource.dropTable(relationsPolygonsOuter, relationsPolygonsInner, relationsPolygonsOuterExploded,
                relationsPolygonsInnerExploded,relationsMpHoles,relationFilteredKeys)
        return outputTableName
    }

}

/**
 * This process is used to extract ways as lines
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all ways transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
String extractWaysAsLines(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = []) {
    return extractWaysAsLines(datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, null)
}

/**
 * This process is used to extract ways as lines
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area
 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all ways transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
String extractWaysAsLines(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], Geometry geometry) {
    if (!datasource) {
        error "Please set a valid database connection"
        return
    }
    if (epsgCode == -1) {
        error "Invalid EPSG code : $epsgCode"
        return
    }
    if (tags == null) {
        error "The tag list cannot be null"
        return
    }

    def outputTableName = postfix "WAYS_LINES"
    def idWaysTable = postfix "ID_WAYS"
    def osmTableTag = prefix osmTablesPrefix, "way_tag"

    if (!datasource.hasTable(osmTableTag)) {
        debug "No tags table found to build the table. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
        return outputTableName
    }

    def countTagsQuery = OSMTools.TransformUtils.getCountTagsQuery(osmTableTag, tags)
    def columnsSelector = OSMTools.TransformUtils.getColumnSelector(osmTableTag, tags, columnsToKeep)
    def tagsFilter = OSMTools.TransformUtils.createWhereFilter(tags)

    if (datasource.firstRow(countTagsQuery.toString()).count <= 0) {
        debug "No keys or values found in the ways. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
            """.toString()
        return outputTableName
    }
    debug "Build ways as lines"
    def waysLinesTmp = postfix "WAYS_LINES_TMP"

    if (!tagsFilter) {
        idWaysTable = prefix osmTablesPrefix, "way_tag"
    } else {
        datasource """
                    DROP TABLE IF EXISTS $idWaysTable;
                    CREATE TABLE $idWaysTable AS
                        SELECT DISTINCT id_way
                        FROM ${osmTablesPrefix}_way_tag as a
                        WHERE $tagsFilter;
                    CREATE INDEX ON $idWaysTable(id_way);
            """.toString()
    }

    if (columnsToKeep) {
        if (datasource.firstRow("""
                    SELECT count(*) AS count 
                    FROM $idWaysTable AS a, ${osmTablesPrefix}_WAY_TAG AS b 
                    WHERE a.ID_WAY = b.ID_WAY AND b.TAG_KEY IN ('${columnsToKeep.join("','")}')
            """.toString())[0] < 1) {
            debug "Any columns to keep. Cannot create any geometry lines. An empty table will be returned."
            datasource """
                        DROP TABLE IF EXISTS $outputTableName;
                        CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
            return outputTableName
        }
    }


    datasource """
                DROP TABLE IF EXISTS $waysLinesTmp; 
                CREATE TABLE  $waysLinesTmp AS 
                    SELECT id_way,ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(THE_GEOM), 4326), $epsgCode) the_geom 
                    FROM(
                        SELECT(
                            SELECT ST_ACCUM(the_geom) the_geom 
                            FROM(
                                SELECT n.id_node, n.the_geom, wn.id_way idway 
                                FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn 
                                WHERE n.id_node = wn.id_node
                                ORDER BY wn.node_order) 
                            WHERE idway = w.id_way
                        ) the_geom, w.id_way  
                        FROM ${osmTablesPrefix}_way w, $idWaysTable b 
                        WHERE w.id_way = b.id_way) geom_table 
                    WHERE ST_NUMGEOMETRIES(the_geom) >= 2;
                CREATE INDEX ON $waysLinesTmp(ID_WAY);
        """.toString()

    def allLinesTables = postfix "all_lines_table"

    def query = """
                DROP TABLE IF EXISTS $allLinesTables;
                CREATE TABLE $allLinesTables AS 
                    SELECT 'w'||a.id_way AS id, a.the_geom ${OSMTools.TransformUtils.createTagList(datasource, columnsSelector,columnsToKeep)} 
                    FROM $waysLinesTmp AS a, ${osmTablesPrefix}_way_tag b 
                    WHERE a.id_way=b.id_way and st_isempty(a.the_geom)=false """

    if(columnsToKeep) {
        query += " AND b.TAG_KEY IN ('${columnsToKeep.join("','")}') "
    }

    query += " GROUP BY a.id_way;"
    datasource.execute(query.toString())
    if (geometry) {
        def  query_out =  """DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName as select * from $allLinesTables as a where """
        int geom_srid = geometry.getSRID()
        if (geom_srid == -1) {
            query_out += " a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom, st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else if (geom_srid == epsgCode) {
            query_out += " a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom,st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else {
            query_out += " a.the_geom && st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode) and st_intersects(a.the_geom,st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode)) "
        }
        datasource.createSpatialIndex(allLinesTables, "the_geom")
        datasource """
                $query_out;
                DROP TABLE IF EXISTS $waysLinesTmp, $idWaysTable, $allLinesTables;
        """.toString()
        return outputTableName
    }else{
        datasource """
                ALTER TABLE $allLinesTables RENAME TO $outputTableName;
                DROP TABLE IF EXISTS $waysLinesTmp, $idWaysTable;
        """.toString()
        return outputTableName
    }

}

/**
 * This process is used to extract relations as lines
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.

 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all relations transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
String extractRelationsAsLines(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = []) {
    return extractRelationsAsLines(datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, null)
}

/**
 * This process is used to extract relations as lines
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area

 * The name of a column corresponds to a key name
 *
 * @return outputTableName a name for the table that contains all relations transformed as lines
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
String extractRelationsAsLines(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode = 4326, def tags = [], def columnsToKeep = [], Geometry geometry) {
    if (!datasource) {
        error "Please set a valid database connection"
        return
    }
    if (epsgCode == -1) {
        error "Invalid EPSG code : $epsgCode"
        return
    }

    if (tags == null) {
        error "The tag list cannot be null"
        return
    }

    def outputTableName = postfix "RELATIONS_LINES"
    def osmTableTag = prefix osmTablesPrefix, "relation_tag"

    if (!datasource.hasTable(osmTableTag)) {
        debug "No tags table found to build the table. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
        return outputTableName
    }

    def countTagsQuery = OSMTools.TransformUtils.getCountTagsQuery(osmTableTag, tags)
    def tagsFilter = OSMTools.TransformUtils.createWhereFilter(tags)

    if (datasource.firstRow(countTagsQuery.toString()).count <= 0) {
        debug "No keys or values found in the relations. An empty table will be returned."
        datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
            """.toString()
        return outputTableName
    }
    def relationsLinesTmp = postfix "RELATIONS_LINES_TMP"
    def relationsFilteredKeys = postfix "RELATION_FILTERED_KEYS"

    if (!tagsFilter) {
        relationsFilteredKeys = prefix osmTablesPrefix, "relation"
    } else {
        datasource """
                    DROP TABLE IF EXISTS $relationsFilteredKeys;
                    CREATE TABLE $relationsFilteredKeys AS
                        SELECT DISTINCT id_relation
                        FROM ${osmTablesPrefix}_relation_tag as a
                        WHERE $tagsFilter;
                    CREATE INDEX ON $relationsFilteredKeys(id_relation);
            """.toString()
    }

    if (columnsToKeep) {
        if (datasource.firstRow("""
                    SELECT count(*) AS count 
                    FROM $relationsFilteredKeys AS a, ${osmTablesPrefix}_RELATION_TAG AS b 
                    WHERE a.ID_RELATION = b.ID_RELATION AND b.TAG_KEY IN ('${columnsToKeep.join("','")}')
            """.toString())[0] < 1) {
            debug "Any columns to keep. Cannot create any geometry lines. An empty table will be returned."
            datasource """ 
                        DROP TABLE IF EXISTS $outputTableName;
                        CREATE TABLE $outputTableName (the_geom GEOMETRY(GEOMETRY,$epsgCode));
                """.toString()
            return outputTableName
        }
    }

    datasource """
                DROP TABLE IF EXISTS $relationsLinesTmp;
                CREATE TABLE $relationsLinesTmp AS
                    SELECT ST_ACCUM(THE_GEOM) AS the_geom, id_relation
                    FROM(
                        SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), $epsgCode) the_geom, id_relation, id_way
                        FROM(
                            SELECT(
                                SELECT ST_ACCUM(the_geom) the_geom
                                FROM(
                                    SELECT n.id_node, n.the_geom, wn.id_way idway
                                    FROM ${osmTablesPrefix}_node n, ${osmTablesPrefix}_way_node wn
                                    WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
                                WHERE idway = w.id_way
                            ) the_geom, w.id_way, br.id_relation
                            FROM ${osmTablesPrefix}_way w, (
                                SELECT br.id_way, g.ID_RELATION
                                FROM ${osmTablesPrefix}_way_member br , $relationsFilteredKeys g
                                WHERE br.id_relation=g.id_relation
                            ) br
                            WHERE w.id_way = br.id_way
                        ) geom_table
                        WHERE st_numgeometries(the_geom)>=2)
                    GROUP BY id_relation;
                CREATE INDEX ON $relationsLinesTmp(id_relation);
        """.toString()

    def columnsSelector = OSMTools.TransformUtils.getColumnSelector(osmTableTag, tags, columnsToKeep)

    def allRelationLines =  postfix("all_relation_lines")
    def query = """
                DROP TABLE IF EXISTS $allRelationLines;
                CREATE TABLE $allRelationLines AS
                    SELECT 'r'||a.id_relation AS id, a.the_geom ${OSMTools.TransformUtils.createTagList(datasource, columnsSelector, columnsToKeep)}
                    FROM $relationsLinesTmp AS a, ${osmTablesPrefix}_relation_tag  b
                    WHERE a.id_relation=b.id_relation and st_isempty(a.the_geom)=false """
    if(columnsToKeep) {
        query += " AND b.TAG_KEY IN ('${columnsToKeep.join("','")}') "
    }
    query += " GROUP BY a.id_relation;"
    datasource.execute(query.toString())
    if (geometry) {
        def query_out =""" DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName as SELECT * FROM $allRelationLines as a where """
        int geom_srid = geometry.getSRID()
        if (geom_srid == -1) {
            query_out += " a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom, st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else if (geom_srid == epsgCode) {
            query_out += "  a.the_geom && st_setsrid('$geometry'::geometry, $epsgCode) and st_intersects(a.the_geom,st_setsrid('$geometry'::geometry, $epsgCode)) "
        } else {
            query_out += "  a.the_geom && st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode) and st_intersects(a.the_geom,st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $epsgCode)) "
        }
        datasource.createSpatialIndex(allRelationLines, "the_geom")
        datasource.execute(""" $query_out   ;
                DROP TABLE IF EXISTS $relationsLinesTmp, $relationsFilteredKeys, $allRelationLines;
        """.toString())
        return outputTableName
    }else{
        datasource.execute(""" ALTER TABLE $allRelationLines RENAME TO $outputTableName;
                DROP TABLE IF EXISTS $relationsLinesTmp, $relationsFilteredKeys;
        """.toString())
        return outputTableName
    }
}
