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
package org.orbisgis.geoclimate.osmtools.utils

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.osmtools.OSMTools


/**
 * Class containing utility methods for the {@link org.orbisgis.geoclimate.osmtools.Transform} script to keep only processes inside
 * the groovy script.
 * @author Erwan Bocher (CNRS LAB-STICC)
 */
@BaseScript OSMTools pf


/**
 * Merge arrays into one.
 *
 * @param removeDuplicated If true remove duplicated values.
 * @param arrays Array of collections or arrays to merge.
 *
 * @return One array containing the values of the given arrays.
 */
def arrayUnion(boolean removeDuplicated, Collection... arrays) {
    def union = []
    if (arrays == null) return union
    for (Object[] array : arrays) {
        union.addAll(array)
    }
    if (removeDuplicated) union.unique()
    union.sort()
    return union
}

/**
 * Extract all the polygons/lines from the OSM tables
 *
 * @param type
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 * @param valid_geom true to valid the geometries
 *
 * @return The name for the table that contains all polygons/lines
 */
String toPolygonOrLine(GeometryTypes type, JdbcDataSource datasource, String osmTablesPrefix, int epsgCode, def tags, def columnsToKeep, boolean valid_geom = false) {
    return toPolygonOrLine(type, datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, null, valid_geom)
}


/**
 * Extract all the polygons/lines from the OSM tables
 *
 * @param type
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param tags list of keys and values to be filtered
 * @param columnsToKeep a list of columns to keep. The name of a column corresponds to a key name
 * @param geometry a geometry to reduce the area
 * @param valid_geom true to valid the geometries
 *
 * @return The name for the table that contains all polygons/lines
 */
String toPolygonOrLine(GeometryTypes type, JdbcDataSource datasource, String osmTablesPrefix, int epsgCode, def tags, def columnsToKeep, org.locationtech.jts.geom.Geometry geometry, boolean valid_geom = false) {
    //Check if parameters a good
    if (!datasource) {
        error "Please set a valid database connection"
        return
    }
    if (!epsgCode || epsgCode == -1) {
        error "Invalid EPSG code : $epsgCode"
        return
    }
    if (!tags && !columnsToKeep) {
        error "No tags nor columns to keep"
        return
    }

    //Start the transformation
    def outputTableName = postfix("OSM_${type.name()}_")
    debug "Start ${type.name().toLowerCase()} transformation"
    debug "Indexing osm tables..."
    buildIndexes(datasource, osmTablesPrefix)

    //Run the processes according to the type
    String outputWay
    String outputRelation
    switch (type) {
        case GeometryTypes.POLYGONS:
            outputWay = OSMTools.Transform.extractWaysAsPolygons(datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry, valid_geom)
            outputRelation = OSMTools.Transform.extractRelationsAsPolygons(datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry, valid_geom)
            break
        case GeometryTypes.LINES:
            outputWay = OSMTools.Transform.extractWaysAsLines(datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry)
            outputRelation = OSMTools.Transform.extractRelationsAsLines(datasource, osmTablesPrefix, epsgCode, tags, columnsToKeep, geometry)
            break
        default:
            error "Wrong type '${type}'."
            return
    }

    if (outputWay && outputRelation) {
        //Merge ways and relations
        def columnsWays = datasource.getColumnNames(outputWay)
        def columnsRelations = datasource.getColumnNames(outputRelation)
        def allColumns = arrayUnion(true, columnsWays, columnsRelations)
        def leftSelect = ""
        def rightSelect = ""
        allColumns.each { column ->
            leftSelect += columnsWays.contains(column) ? "\"$column\"," : "null AS \"$column\","
            rightSelect += columnsRelations.contains(column) ? "\"$column\"," : "null AS \"$column\","
        }
        leftSelect = leftSelect[0..-2]
        rightSelect = rightSelect[0..-2]

        datasource.execute """
                            DROP TABLE IF EXISTS $outputTableName;
                            CREATE TABLE $outputTableName AS 
                                SELECT $leftSelect
                                FROM $outputWay
                                UNION ALL
                                SELECT $rightSelect
                                FROM $outputRelation;
                            DROP TABLE IF EXISTS $outputWay, $outputRelation;
            """.toString()
        debug "The way and relation ${type.name()} have been built."
    } else if (outputWay) {
        datasource.execute "ALTER TABLE $outputWay RENAME TO $outputTableName".toString()
        debug "The way ${type.name()} have been built."
    } else if (outputRelation) {
        datasource.execute "ALTER TABLE $outputRelation RENAME TO $outputTableName".toString()
        debug "The relation ${type.name()} have been built."
    } else {
        warn "Cannot extract any ${type.name()}."
        return
    }
    return outputTableName
}

/**
 * Return the column selection query
 *
 * @param osmTableTag Name of the table of OSM tag
 * @param tags List of keys and values to be filtered
 * @param columnsToKeep List of columns to keep
 *
 * @return The column selection query
 */
def getColumnSelector(osmTableTag, tags, columnsToKeep) {
    if (!osmTableTag) {
        error "The table name should not be empty or null."
        return null
    }
    def tagKeys = []
    if (tags != null) {
        def tagKeysList = tags in Map ? tags.keySet() : tags
        tagKeys.addAll(tagKeysList.findResults { it != null && it != "null" && !it.isEmpty() ? it : null })
    }
    if (columnsToKeep != null) {
        tagKeys.addAll(columnsToKeep)
    }
    tagKeys.removeAll([null])

    def query = "SELECT distinct tag_key FROM $osmTableTag as a "
    if (tagKeys) query += " WHERE tag_key IN ('${tagKeys.unique().join("','")}')"
    return query
}

/**
 * Return the tag count query
 *
 * @param osmTableTag Name of the table of OSM tag
 * @param tags List of keys and values to be filtered
 * @param columnsToKeep List of columns to keep
 *
 * @return The tag count query
 */
def getCountTagsQuery(osmTableTag, tags) {
    if (!osmTableTag) return null
    def countTagsQuery = "SELECT count(*) AS count FROM $osmTableTag as a"
    def whereFilter = createWhereFilter(tags)
    if (whereFilter) {
        countTagsQuery += " WHERE $whereFilter"
    }
    return countTagsQuery
}

/**
 * This function is used to extract nodes as points
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param outputNodesPoints the name of the nodes points table
 * @param tagKeys list ok keys to be filtered
 * @param columnsToKeep a list of columns to keep.
 * The name of a column corresponds to a key name
 *
 * @return true if some points have been built
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
boolean extractNodesAsPoints(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode,
                             String outputNodesPoints, def tags, def columnsToKeep) {
    return extractNodesAsPoints(datasource, osmTablesPrefix, epsgCode,
            outputNodesPoints, tags, columnsToKeep, null)
}

/**
 * This function is used to extract nodes as points
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 * @param epsgCode EPSG code to reproject the geometries
 * @param outputNodesPoints the name of the nodes points table
 * @param tagKeys list ok keys to be filtered
 * @param columnsToKeep a list of columns to keep.
 * @param geometry a geometry to reduce the area
 * The name of a column corresponds to a key name
 *
 * @return true if some points have been built
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
boolean extractNodesAsPoints(JdbcDataSource datasource, String osmTablesPrefix, int epsgCode,
                             String outputNodesPoints, def tags, def columnsToKeep, Geometry geometry) throws Exception{
    if (!datasource) {
        throw new Exception("The datasource should not be null")
    }
    if (osmTablesPrefix == null) {
        throw new Exception("Invalid null OSM table prefix")
    }
    if (epsgCode == -1) {
        throw new Exception("Invalid EPSG code")
    }
    if (tags == null) {
        throw new Exception("The tag list cannot be null")
    }
    if (outputNodesPoints == null) {
        throw new Exception("Invalid null output node points table name")
    }
    def tableNode = "${osmTablesPrefix}_node"
    def tableNodeTag = "${osmTablesPrefix}_node_tag"
    def filterTableNode = postfix("${osmTablesPrefix}_node_filtered")

    def tablesToDrop = []
    if (!datasource.hasTable(tableNodeTag)) {
        debug "No tags table found to build the table. An empty table will be returned."
        datasource """ 
                    DROP TABLE IF EXISTS $outputNodesPoints;
                    CREATE TABLE $outputNodesPoints (the_geom GEOMETRY(POINT,$epsgCode));
                """.toString()
        return outputNodesPoints
    }

    def countTagsQuery = getCountTagsQuery(tableNodeTag, tags)
    def columnsSelector = getColumnSelector(tableNodeTag, tags, columnsToKeep)
    def tagsFilter = createWhereFilter(tags)

    if (datasource.firstRow(countTagsQuery).count <= 0) {
        debug "No keys or values found in the nodes. An empty table will be returned."
        datasource.execute """
                    DROP TABLE IF EXISTS $outputNodesPoints;
                    CREATE TABLE $outputNodesPoints (the_geom GEOMETRY(POINT,4326));
            """.toString()
        return false
    }

    debug "Start points transformation"
    debug "Indexing osm tables..."
    datasource.execute """
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_index                     ON ${osmTablesPrefix}_node(id_node);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_tag_id_node_index         ON ${osmTablesPrefix}_node_tag(id_node);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_tag_tag_key_index         ON ${osmTablesPrefix}_node_tag(tag_key);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_tag_tag_value_index       ON ${osmTablesPrefix}_node_tag(tag_value);
        """.toString()

    debug "Build nodes as points"
    def tagList = createTagList datasource, columnsSelector, columnsToKeep
    if (tagsFilter.isEmpty()) {
        if (columnsToKeep) {
            if (datasource.firstRow("select count(*) as count from $tableNodeTag where TAG_KEY in ('${columnsToKeep.join("','")}')")[0] < 1) {
                datasource.execute """
                            DROP TABLE IF EXISTS $outputNodesPoints;
                            CREATE TABLE $outputNodesPoints (the_geom GEOMETRY(POINT,4326));
                    """.toString()
                return true
            }
        }

        if (geometry) {
            def query = """  DROP TABLE IF EXISTS $filterTableNode;
                                  CREATE TABLE  $filterTableNode as select * from $tableNode as a   """
            int geom_srid = geometry.getSRID()
            int sridNode = datasource.getSrid(tableNode)
            if (geom_srid == -1) {
                query += " where the_geom && st_setsrid('$geometry'::geometry, $sridNode) and st_intersects(the_geom, st_setsrid('$geometry'::geometry, $sridNode)) "
            } else if (geom_srid == sridNode) {
                query += " where the_geom && st_setsrid('$geometry'::geometry, $sridNode) and st_intersects(the_geom,st_setsrid('$geometry'::geometry, $sridNode)) "
            } else {
                query += " where the_geom && st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $sridNode) and st_intersects(the_geom,st_transform(st_setsrid('$geometry'::geometry, $geom_srid),$sridNode)) "
            }

            datasource.createSpatialIndex(tableNode, "the_geom")

            datasource.execute(query.toString())
            datasource.createIndex(filterTableNode, "id_node")
            tablesToDrop << filterTableNode
        } else {
            filterTableNode = tableNode
        }
        def lastQuery =  """DROP TABLE IF EXISTS $outputNodesPoints;
                CREATE TABLE $outputNodesPoints AS
                    SELECT a.id_node,ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), $epsgCode) AS the_geom $tagList
                    FROM $filterTableNode AS a, $tableNodeTag b
                    WHERE a.id_node = b.id_node """
        if(columnsToKeep) {
            lastQuery += " AND b.TAG_KEY IN ('${columnsToKeep.join("','")}') "
        }
        lastQuery+= " GROUP BY a.id_node"
        datasource.execute(lastQuery.toString())

    } else {
        if (columnsToKeep) {
            if (datasource.firstRow("select count(*) as count from $tableNodeTag where TAG_KEY in ('${columnsToKeep.join("','")}')")[0] < 1) {
                datasource.execute """
                            DROP TABLE IF EXISTS $outputNodesPoints;
                            CREATE TABLE $outputNodesPoints (the_geom GEOMETRY(POINT,4326));
                    """.toString()
                return true
            }
        }
        if (geometry) {
            int sridNode = datasource.getSrid(tableNode)
            def query = """  DROP TABLE IF EXISTS $filterTableNode;
                                  CREATE TABLE  $filterTableNode as select * from $tableNode   """
            int geom_srid = geometry.getSRID()
            if (geom_srid == -1) {
                query += " where the_geom && st_setsrid('$geometry'::geometry, $sridNode) and st_intersects(the_geom, st_setsrid('$geometry'::geometry, $sridNode)) "
            } else if (geom_srid == sridNode) {
                query += " where the_geom && st_setsrid('$geometry'::geometry, $sridNode) and st_intersects(the_geom,st_setsrid('$geometry'::geometry, $sridNode)) "
            } else {
                query += " where the_geom && st_transform(st_setsrid('$geometry'::geometry, $geom_srid), $sridNode) and st_intersects(the_geom,st_transform(st_setsrid('$geometry'::geometry, $geom_srid),$sridNode)) "
            }
            datasource.createSpatialIndex(tableNode, "the_geom")
            datasource.execute(query.toString())
            datasource.createIndex(filterTableNode, "id_node")
            tablesToDrop << filterTableNode
        } else {
            filterTableNode = tableNode
        }

        def filteredNodes = postfix("FILTERED_NODES_")
        def lastQuery = """
                CREATE TABLE $filteredNodes AS
                    SELECT DISTINCT id_node FROM ${osmTablesPrefix}_node_tag as a WHERE $tagsFilter;
                CREATE INDEX ON $filteredNodes(id_node);
                DROP TABLE IF EXISTS $outputNodesPoints;
                CREATE TABLE $outputNodesPoints AS
                    SELECT a.id_node, ST_TRANSFORM(ST_SETSRID(a.THE_GEOM, 4326), $epsgCode) AS the_geom $tagList
                    FROM $filterTableNode AS a, $tableNodeTag  b, $filteredNodes c
                    WHERE a.id_node=b.id_node
                    AND a.id_node=c.id_node  """
        if(columnsToKeep){
            lastQuery += " AND b.TAG_KEY IN ('${columnsToKeep.join("','")}') "
        }
        lastQuery+=" GROUP BY a.id_node"
        datasource.execute(lastQuery.toString())
    }
    datasource.dropTable(tablesToDrop)
    return true
}

/**
 * Method to build a where filter based on a list of key, values
 *
 * @param tags the input Map of key and values with the following signature
 * ["building", "landcover"] or
 * ["building": ["yes"], "landcover":["grass", "forest"]]
 * @return a where filter as
 * tag_key in '(building', 'landcover') or
 * (tag_key = 'building' and tag_value in ('yes')) or (tag_key = 'landcover' and tag_value in ('grass','forest')))
 */
def createWhereFilter(def tags) {
    if (!tags) {
        warn "The tag map is empty"
        return ""
    }
    def whereKeysValuesFilter = ""
    if (tags in Map) {
        def whereQuery = []
        tags.each { tag ->
            def keyIn = ''
            def valueIn = ''
            if (tag.key) {
                if (tag.key instanceof Collection) {
                    keyIn += "a.tag_key IN ('${tag.key.join("','")}')"
                } else {
                    keyIn += "a.tag_key = '${tag.key}'"
                }
            }
            if (tag.value) {
                def valueList = (tag.value instanceof Collection) ? tag.value.flatten().findResults { it } : [tag.value]
                valueIn += "a.tag_value IN ('${valueList.join("','")}')"
            }

            if (!keyIn.isEmpty() && !valueIn.isEmpty()) {
                whereQuery += "$keyIn AND $valueIn"
            } else if (!keyIn.isEmpty()) {
                whereQuery += "$keyIn"
            } else if (!valueIn.isEmpty()) {
                whereQuery += "$valueIn"
            }
        }
        whereKeysValuesFilter = "(${whereQuery.join(') OR (')})"
    } else {
        def tagArray = []
        tagArray.addAll(tags)
        tagArray.removeAll([null])
        if (tagArray) {
            whereKeysValuesFilter = "a.tag_key IN ('${tagArray.join("','")}')"
        }
    }
    return whereKeysValuesFilter
}


/**
 * Build a case when expression to pivot keys
 * @param datasource a connection to a database
 * @param selectTableQuery the table that contains the keys and values to pivot
 * @return the case when expression
 */
def createTagList(JdbcDataSource datasource, def selectTableQuery) {
    return createTagList(datasource, selectTableQuery, [])
}

/**
 * Build a case when expression to pivot keys
 * @param datasource a connection to a database
 * @param selectTableQuery the table that contains the keys and values to pivot
 * @param columnsToKeep a list of columns (key value) to keep. If the key doesn't exist set the key to null.
 * @return the case when expression
 */
def createTagList(JdbcDataSource datasource, def selectTableQuery, List columnsToKeep) {
    if (!datasource) {
        error "The datasource should not be null."
        return null
    }
    def rowskeys = datasource.rows(selectTableQuery.toString())
    def list = []
    if(!columnsToKeep){
        rowskeys.tag_key.each { it ->
            if (it != null ) {
                list << "MAX(CASE WHEN b.tag_key = '$it' THEN b.tag_value END) AS \"${it}\""
            }
        }
    }else {
        def nullColumns = columnsToKeep.collect()
        rowskeys.tag_key.each { it ->
            if (it != null && columnsToKeep.contains(it)) {
                list << "MAX(CASE WHEN b.tag_key = '$it' THEN b.tag_value END) AS \"${it}\""
                nullColumns.remove(it)
            }
        }
        nullColumns.each { it ->
            list << "CAST(NULL AS VARCHAR) AS \"${it}\""
        }
    }

    def tagList = ""
    if (!list.isEmpty()) {
        tagList = ", ${list.join(",")}"
    }
    return tagList
}

/**
 * Build the indexes to perform analysis quicker
 *
 * @param datasource a connection to a database
 * @param osmTablesPrefix prefix name for OSM tables
 *
 * @return
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
def buildIndexes(JdbcDataSource datasource, String osmTablesPrefix) throws Exception{
    if (!datasource) {
        throw new Exception("The datasource should not be null.")
    }
    if (!osmTablesPrefix) {
        throw new Exception("The osmTablesPrefix should not be null or empty.")
    }
    datasource.execute """
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_index                     ON ${osmTablesPrefix}_node(id_node);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_tag_id_node_index         ON ${osmTablesPrefix}_node_tag(id_node);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_tag_tag_key_index         ON ${osmTablesPrefix}_node_tag(tag_key);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_node_tag_tag_value_index         ON ${osmTablesPrefix}_node_tag(tag_value);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_node_id_node_index         ON ${osmTablesPrefix}_way_node(id_node);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_node_order_index           ON ${osmTablesPrefix}_way_node(node_order);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_node_id_way_index          ON ${osmTablesPrefix}_way_node(id_way);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_index                      ON ${osmTablesPrefix}_way(id_way);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_tag_key_tag_index          ON ${osmTablesPrefix}_way_tag(tag_key);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_tag_id_way_index           ON ${osmTablesPrefix}_way_tag(id_way);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_tag_value_index            ON ${osmTablesPrefix}_way_tag(tag_value);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_id_relation_index     ON ${osmTablesPrefix}_relation(id_relation);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_tag_key_tag_index     ON ${osmTablesPrefix}_relation_tag(tag_key);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_tag_id_relation_index ON ${osmTablesPrefix}_relation_tag(id_relation);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_relation_tag_tag_value_index   ON ${osmTablesPrefix}_relation_tag(tag_value);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_member_id_relation_index   ON ${osmTablesPrefix}_way_member(id_relation);
            CREATE INDEX IF NOT EXISTS ${osmTablesPrefix}_way_id_way                     ON ${osmTablesPrefix}_way(id_way);
        """.toString()
    return true
}


