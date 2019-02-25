/*-------------------------------------------------------------
 * 
 * Description : This script is to build a GIS layer that contains building based on
 * the folowing tags 
 *
 * Author : Guyon Sebastien <guyonsebastien1@gmail.com>, CNRS, LABSTICC
 * Author : Bocher Erwan <erwan.bocher@univ-ubs.fr>, CNRS, LABSTICC
 *
 * Last update : 18/11/2018
 * -------------------------------------------------------------
 */

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
 * where the "tag_key" is "building". Then the geometries of the 
 * polygons constituting the buildings are recreated by selecting 
 * as parameter of the "st_makeline" function, the geometries of 
 * the nodes whose "id_node" of the "map_node" table corresponds 
 * to an "id_node" of the "map_way_node" table whose the "id_way" 
 * corresponds to an "id_way" present in the selection made in 
 * the previous step, ordered by "node_order" and if the geometry 
 * of the first node is equal to the geometry of the last node 
 * and the number of nodes is greater than 3. Then use this result 
 * to feed the function "st_makepolygon" and change the coordinate 
 * system for a coordinate system using the metric system.
 */

DROP TABLE IF EXISTS buildings_simp_raw; 
CREATE TABLE buildings_simp_raw AS 
SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), 2154) the_geom 
FROM 
    (SELECT 
        (SELECT ST_ACCUM(the_geom) the_geom 
        FROM 
            (SELECT n.id_node, n.the_geom, wn.id_way idway 
            FROM map_node n, map_way_node wn 
            WHERE n.id_node = wn.id_node 
            ORDER BY wn.node_order) 
        WHERE  idway = w.id_way) the_geom ,w.id_way 
    FROM map_way w, 
        (SELECT DISTINCT id_way 
        FROM map_way_tag wt, map_tag t 
        WHERE wt.id_tag = t.id_tag AND t.tag_key IN ('building')) b 
    WHERE w.id_way = b.id_way) geom_table 
WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) 
AND ST_NUMGEOMETRIES(the_geom) > 3;
CREATE INDEX IF NOT EXISTS buildings_simp_raw_index ON buildings_simp_raw(id_way);

/*
 * Join to this table in a new "height" column the values of the 
 * "value" column of the "tag" table whose "id_tag" corresponds 
 * to a "tag_id" of the "way_tag" table where the "tag_key" is 
 * "Height" and "id_way" corresponds to an "id_way" of the table 
 * created in the previous step and whose value is composed only 
 * of digits. Also join in a new column "levels", the values of 
 * the column "value" of the table "tag" where the "id_tag", 
 * corresponding to a "tag_id" of the table "way_tag" where the 
 * "tag_key" is "building:levels" and the" id_way" corresponds to 
 * an "id way" of the table created previously and whose value is 
 * composed only of digits, as well as the value taken by the key 
 * building to give an indication on the type of building.
 */

DROP TABLE IF EXISTS buildings_simp; 
CREATE TABLE buildings_simp (the_geom polygon, id_way varchar, height int, levels int, b_type varchar) AS 
SELECT a.the_geom, a.id_way, b.height, c.levels, d.b_type 
FROM buildings_simp_raw a 
LEFT JOIN     
    (SELECT DISTINCT br.id_way, VALUE height 
    FROM map_way_tag wt, map_tag t, buildings_simp_raw br 
    WHERE wt.id_tag = t.id_tag AND t.tag_key IN ('height') 
    AND br.id_way = wt.id_way 
    AND VALUE REGEXP ('^\\d*\$')) b 
ON a.id_way = b.id_way 
LEFT JOIN     
    (SELECT DISTINCT br.id_way, VALUE levels 
    FROM map_way_tag wt, map_tag t, buildings_simp_raw br 
    WHERE wt.id_tag = t.id_tag 
    AND t.tag_key IN ('building:levels') 
    AND br.id_way = wt.id_way 
    AND VALUE REGEXP ('^\\d*\$')) c 
ON a.id_way = c.id_way 
LEFT JOIN     
    (SELECT DISTINCT br.id_way, VALUE b_type 
    FROM map_way_tag wt, map_tag t, buildings_simp_raw br 
    WHERE wt.id_tag = t.id_tag 
    AND t.tag_key IN ('building') 
    AND br.id_way = wt.id_way) d 
ON a.id_way = d.id_way; 
CREATE INDEX IF NOT EXISTS buildings_simp_index ON buildings_simp (id_way);

DROP TABLE IF EXISTS buildings_simp_raw;

/*
 * Create a table of polygons containing the buildings resulting 
 * from a relation and formed of several polygons.
.* Select the "id_way", "id_relation" and their "role" of the 
 * "way_member" table which correspond to a "id_relation" of the 
 * "relation_tag" table, which have a "tag_id" corresponding to 
 * a "tag_id" the "tag" table associated with the "tag_key" "building".
 */


DROP TABLE IF EXISTS buildings_rel_way; 
CREATE TABLE buildings_rel_way (id_relation varchar, id_way varchar, role varchar) AS 
SELECT rt.id_relation, wm.id_way, wm.role 
FROM map_relation_tag rt, map_tag t, map_way_member wm 
WHERE rt.id_tag = t.id_tag AND t.tag_key IN ('building') AND rt.id_relation = wm.id_relation; 
CREATE INDEX IF NOT EXISTS buildings_rel_way_index on buildings_rel_way (id_way);
CREATE INDEX IF NOT EXISTS buildings_rel_way_index2 on buildings_rel_way (id_relation);

/*
 * The geometries of the polygons constituting the buildings are 
 * recreated by selecting as parameter of the "st_makeline" 
 * function, the geometries of the nodes whose "id_node" of the 
 * "map_node" table corresponds to an "id_node" of the 
 * "map_way_node" table whose "Id_way" corresponds to an "id_way" 
 * present in the selection made in the previous step, ordered 
 * by "node_order". The coordinate system is changed for a 
 * coordinate system using the metric system.
 * Geometries that are part of the same relationship and have 
 * the same role are aggregated using the functions st_accum, 
 * st_union, and st_linemerge.
 */


DROP TABLE IF EXISTS buildings_rel_raw; 
CREATE TABLE buildings_rel_raw AS 
SELECT ST_LINEMERGE(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation, role
    FROM 
        (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, role, id_way 
        FROM      
            (SELECT 
                (SELECT ST_ACCUM(the_geom) the_geom 
                FROM 
                    (SELECT n.id_node, n.the_geom, wn.id_way idway 
                    FROM map_node n, map_way_node wn 
                    WHERE n.id_node = wn.id_node ORDER BY wn.node_order) 
                WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role 
            FROM map_way w, buildings_rel_way br 
            WHERE w.id_way = br.id_way) geom_table where st_numgeometries(the_geom)>=2)
    GROUP BY id_relation, role;


-- Separate multilinestring to obtain linestring

DROP TABLE IF EXISTS buildings_rel_raw2; 
CREATE TABLE buildings_rel_raw2 AS 
SELECT * FROM ST_Explode('buildings_rel_raw'); 

/* 
 * Connect the lines to form polygons if the coordinates of the 
 * first point are equal to that of the last point
 */

DROP TABLE IF EXISTS buildings_rel_raw3; 
CREATE TABLE buildings_rel_raw3 AS 
SELECT ST_MAKEPOLYGON(the_geom) the_geom, id_relation, role
FROM buildings_rel_raw2
WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom);

-- Assign the outer role to polygons with an outline role

UPDATE buildings_rel_raw3 SET role =
CASE WHEN role = 'outline' THEN 'outer'
    ELSE role
END;

/*
 * Separate this table in two according to the role allocated to 
 * each polygon in its relationship. Then, in order to recreate 
 * the geometries of the buildings composed of several polygons, 
 * the function st_difference is used with as input parameters 
 * the geometry of the outer polygons and an accumulation of the 
 * geometries of the inner polygons grouped by "id_relation"
 * Then alone outer polygons (without inner polygons) are added 
 * to this result
 */

DROP TABLE IF EXISTS buildings_rel_tot; 
CREATE TABLE buildings_rel_tot AS 
SELECT ST_difference(st_union(st_accum(to.the_geom)),st_union(st_accum(ti.the_geom))) as the_geom, to.id_relation 
FROM      
    (SELECT the_geom, id_relation, role 
    FROM buildings_rel_raw3 
    WHERE role = 'outer') to,     
    (SELECT the_geom, id_relation, role 
    FROM buildings_rel_raw3 
    WHERE role = 'inner') ti 
where ti.id_relation = to.id_relation
GROUP BY to.id_relation
UNION 
SELECT a.the_geom, a.id_relation 
FROM 
    (SELECT the_geom, id_relation, role
    FROM buildings_rel_raw3
    WHERE role = 'outer') a
LEFT JOIN 
    (SELECT the_geom, id_relation, role
    FROM buildings_rel_raw3
    where role = 'inner') b
ON a.id_relation = b.id_relation
WHERE b.id_relation IS NULL;

/*
 * The building type, the height and the level number attributes 
 * are associated with their respective relationship if they exist.
 */

DROP TABLE IF EXISTS buildings_rel; 
CREATE TABLE buildings_rel AS 
SELECT a.the_geom, a.id_relation, b.height, c.levels, d.b_type 
FROM buildings_rel_tot a 
    LEFT JOIN 
        (SELECT DISTINCT br.id_relation, tag_value height 
        FROM map_relation_tag rt, map_tag t, buildings_rel_tot br 
        WHERE rt.id_tag = t.id_tag AND t.tag_key IN ('height') AND br.id_relation = rt.id_relation AND tag_value REGEXP ('^\\d*\$')) b 
    ON a.id_relation = b.id_relation 
    LEFT JOIN 
        (SELECT DISTINCT br.id_relation, tag_value levels 
        FROM map_relation_tag rt, map_tag t, buildings_rel_tot br 
        WHERE rt.id_tag = t.id_tag AND t.tag_key IN ('building:levels') AND br.id_relation = rt.id_relation AND tag_value REGEXP ('^\\d*\$')) c 
    ON a.id_relation = c.id_relation 
    LEFT JOIN 
        (SELECT DISTINCT br.id_relation, tag_value b_type 
        FROM map_relation_tag rt, map_tag t, buildings_rel_tot br 
        WHERE rt.id_tag = t.id_tag AND t.tag_key IN ('building') AND br.id_relation = rt.id_relation) d 
    ON a.id_relation = d.id_relation;  

-- Join the two tables containing the frame


DROP TABLE IF EXISTS buildings; 
CREATE TABLE buildings (id_bat SERIAL, the_geom GEOMETRY, height INT, levels INT, b_type VARCHAR) AS 
SELECT NULL, ST_MAKEVALID(the_geom), height, levels, b_type 
FROM buildings_simp 
UNION 
SELECT NULL, ST_MAKEVALID(the_geom), height, levels, b_type  
FROM buildings_rel;


-- Delete the intermediates tables

DROP TABLE IF EXISTS buildings_rel;
DROP TABLE IF EXISTS buildings_rel_raw;
DROP TABLE IF EXISTS buildings_rel_raw2;
DROP TABLE IF EXISTS buildings_rel_raw3;
DROP TABLE IF EXISTS buildings_rel_tot;
DROP TABLE IF EXISTS buildings_rel_way;
DROP TABLE IF EXISTS buildings_simp;


