/*-------------------------------------------------------------
 *
 * Description : This script is to build a GIS layer that contains roads
 *
 * Author : Le Saux Wiederhold Elisabeth, LABSTICC
 *
 * Last update : 28/01/2019
 * -------------------------------------------------------------
 */

 /* TODO : the values of the "highway" tag have to be verified when the dictionary is completed */

-- Create the indexes
CREATE INDEX IF NOT EXISTS map_node_index on map_node(id_node);
CREATE INDEX IF NOT EXISTS map_way_node_index on map_way_node(id_node);
CREATE INDEX IF NOT EXISTS map_way_node_index2 on map_way_node(node_order);
CREATE INDEX IF NOT EXISTS map_way_node_index3 ON map_way_node(id_way);
CREATE INDEX IF NOT EXISTS map_way_index on map_way(id_way);
CREATE INDEX IF NOT EXISTS map_way_tag_id_index on map_way_tag(id_tag);
CREATE INDEX IF NOT EXISTS map_way_tag_va_index on map_way_tag(id_way);
CREATE INDEX IF NOT EXISTS map_tag_id_index on map_tag(id_tag);
CREATE INDEX IF NOT EXISTS map_tag_key_index on map_tag(tag_key);
CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(id_tag);
CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index2 ON map_relation_tag(id_relation);
CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(tag_value);
CREATE INDEX IF NOT EXISTS map_relation_tag_rel_index ON map_relation(id_relation);
CREATE INDEX IF NOT EXISTS map_way_member_index ON map_way_member(id_relation);

/*
 * Select all the "id_way" of the "way_tag" table which have an
 * "id_tag" corresponding to an "id_tag" of the table "map_tag"
 * where the "tag_key" is "highway". Then the geometries of the
 * lines constituting the roads are recreated by selecting
 * as parameter of the "st_makeline" function, the geometries of
 * the nodes whose "id_node" of the "map_node" table corresponds
 * to an "id_node" of the "map_way_node" table whose "id_way"
 * corresponds to an "id_way" present in the selection made in
 * the previous step, ordered by "node_order" and if the number
 * of nodes is at least 2. Then change the coordinate system
 * for a coordinate system using the metric system.
 */

DROP TABLE IF EXISTS roads_raw;

CREATE TABLE roads_raw AS
SELECT id_way, val highway, ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom
FROM
    (SELECT w.id_way, val,
        (SELECT ST_ACCUM(the_geom) the_geom
        FROM
            (SELECT n.id_node, n.the_geom, wn.id_way idway
            FROM map_node n, map_way_node wn
            WHERE n.id_node = wn.id_node
            ORDER BY wn.node_order)
        WHERE  idway = w.id_way) the_geom
    FROM map_way w,
        (SELECT DISTINCT id_way, value as val
        FROM map_way_tag wt, map_tag t
        WHERE wt.id_tag = t.id_tag AND t.tag_key IN ('highway')) b
    WHERE w.id_way = b.id_way) geom_table
WHERE ST_NUMGEOMETRIES(the_geom) >= 2
and val in ('residential', 'secondary', 'tertiary','primary','unclassified');
CREATE INDEX IF NOT EXISTS roads_raw_index ON roads_raw(id_way);

;
/*
 * Several outer joins with the tag table to find width, surface and sidewalk values
 */

DROP TABLE IF EXISTS roads;

CREATE TABLE roads (id serial, id_source varchar, the_geom linestring, width float, type varchar, surface varchar, sidewalk varchar) AS
    SELECT null, a.id_way, a.the_geom, b.width, a.highway, c.surface, d.sidewalk
    FROM roads_raw a
        LEFT JOIN
            (SELECT DISTINCT wt.id_way, VALUE width
            FROM map_way_tag wt, map_tag t
            WHERE wt.id_tag = t.id_tag
            AND t.tag_key IN ('width')
            ) b
        ON a.id_way = b.id_way
        LEFT JOIN
            (SELECT DISTINCT wt.id_way, VALUE surface
            FROM map_way_tag wt, map_tag t
            WHERE wt.id_tag = t.id_tag
            AND t.tag_key IN ('surface')) c
        ON a.id_way = c.id_way
        LEFT JOIN
            (SELECT DISTINCT wt.id_way, VALUE sidewalk
            FROM map_way_tag wt, map_tag t
            WHERE wt.id_tag = t.id_tag
            AND t.tag_key IN ('sidewalk')) d
        ON a.id_way = d.id_way;

DROP TABLE IF EXISTS roads_raw;



