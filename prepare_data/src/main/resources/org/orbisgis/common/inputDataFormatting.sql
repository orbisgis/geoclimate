-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--
-- 																								--
-- Title : Data Input Model pre-process 														--
-- Project : URCLIM / Paendora (ADEME - MODEVAL 2017-5)													--
-- Abstract : This script aims at pre-process input data that will then feed the RSU chain.		--
--																								--
-- Author : Gwendall Petit (DECIDE Team, Lab-STICC CNRS UMR 6285)								--
-- Last update : 04/04/2019																		--
-- Licence : GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)                                  --
--																								--
-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--

--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
-- ENTER INTO THE GEOCLIMATE MODEL
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------
-- 1. PROCESS BUILDINGS
---------------------------------------------------------------------------------

-- Add an id (primary key, called ID_BUILD) to the input layer ($INPUT_BUILDING) and create indexes

DROP TABLE IF EXISTS $BU_ZONE;
CREATE TABLE $BU_ZONE (THE_GEOM geometry, ID_BUILD serial, ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer) AS SELECT THE_GEOM, null, ID_SOURCE, HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE, MAIN_USE, ZINDEX FROM $INPUT_BUILDING;
CREATE INDEX ON $BU_ZONE(the_geom) USING RTREE;
CREATE INDEX ON $BU_ZONE(ID_BUILD);


-------------------------------------------------------
-- Identify the city id where the building is
-------------------------------------------------------

-- 1- Select buildings that are within a city and assign the INSEE CODE to the building
DROP TABLE IF EXISTS $BUILD_WITHIN_ZONE;
CREATE TABLE $BUILD_WITHIN_ZONE AS SELECT a.ID_BUILD, b.ID_ZONE as ID_ZONE FROM $BU_ZONE a, $ZONE_NEIGHBORS b WHERE a.the_geom && b.the_geom AND ST_CONTAINS(b.the_geom, a.the_geom);

-- 2- Select buildings that are on a boundary (not within a city)
DROP TABLE IF EXISTS $BUILD_OUTER_ZONE;
CREATE TABLE $BUILD_OUTER_ZONE AS SELECT * FROM $BU_ZONE WHERE ID_BUILD NOT IN (SELECT ID_BUILD FROM $BUILD_WITHIN_ZONE);

-- 3- Associate building to city, depending on the maximum surface of intersection, only for builings that are not within a city
DROP TABLE IF EXISTS $BUILD_OUTER_ZONE_MATRIX ;
CREATE TABLE $BUILD_OUTER_ZONE_MATRIX (ID_BUILD integer primary key, ID_ZONE integer) AS SELECT a.ID_BUILD , (SELECT ID_ZONE FROM $ZONE_NEIGHBORS b WHERE a.THE_GEOM && b.THE_GEOM ORDER BY ST_AREA(ST_INTERSECTION(a.THE_GEOM, b.THE_GEOM)) DESC LIMIT 1) AS ID_ZONE FROM $BUILD_OUTER_ZONE a WHERE ST_NUMGEOMETRIES(a.THE_GEOM)=1;

-- 4- Merge into one single table these informations
DROP TABLE IF EXISTS $BUILD_ZONE_MATRIX ;
CREATE TABLE $BUILD_ZONE_MATRIX (ID_BUILD integer primary key, ID_ZONE integer) AS SELECT * FROM $BUILD_WITHIN_ZONE UNION SELECT * FROM $BUILD_OUTER_ZONE_MATRIX;
CREATE INDEX ON $BUILD_ZONE_MATRIX(ID_BUILD);

-- Join this "matrix" to the intial table (with all building informations) (FC = First Control)
DROP TABLE IF EXISTS $BUILDING_FC ;
CREATE TABLE $BUILDING_FC AS SELECT a.*, b.ID_ZONE FROM $BU_ZONE a LEFT JOIN $BUILD_ZONE_MATRIX b ON a.ID_BUILD=b.ID_BUILD;
-- Create indexes to improve upcoming controls
CREATE INDEX ON $BUILDING_FC(ID_ZONE);
CREATE INDEX ON $BUILDING_FC(HEIGHT_WALL);

DROP TABLE IF EXISTS $BUILD_WITHIN_ZONE, $BUILD_OUTER_ZONE, $BUILD_OUTER_ZONE_MATRIX, $BUILD_ZONE_MATRIX;


-------------------------------------------------------
-- First Control
-------------------------------------------------------

-- At the city level
-----------------------

DROP TABLE IF EXISTS $FC_BUILD_H_ZERO, $FC_BUILD_H_NULL, $FC_BUILD_H_RANGE;
CREATE TABLE $FC_BUILD_H_ZERO AS SELECT $ID_ZONE as ID_ZONE, COUNT(*) as FC_H_ZERO FROM $BUILDING_FC WHERE HEIGHT_WALL=0 AND ID_ZONE=$ID_ZONE;
CREATE TABLE $FC_BUILD_H_NULL AS SELECT COUNT(*) as FC_H_NULL FROM $BUILDING_FC WHERE HEIGHT_WALL is null AND ID_ZONE=$ID_ZONE;
CREATE TABLE $FC_BUILD_H_RANGE AS SELECT COUNT(*) as FC_H_RANGE FROM $BUILDING_FC WHERE HEIGHT_WALL < 0 AND HEIGHT_WALL > 1000 AND ID_ZONE=$ID_ZONE;

DROP TABLE IF EXISTS $FC_BUILD_STATS_ZONE;
CREATE TABLE $FC_BUILD_STATS_ZONE AS SELECT a.ID_ZONE, a.FC_H_ZERO, b.FC_H_NULL, c.FC_H_RANGE FROM $FC_BUILD_H_ZERO a, $FC_BUILD_H_NULL b, $FC_BUILD_H_RANGE c;

DROP TABLE IF EXISTS $FC_BUILD_H_ZERO, $FC_BUILD_H_NULL, $FC_BUILD_H_RANGE;


-- At the city buffer level
-----------------------

DROP TABLE IF EXISTS $FC_BUILD_H_ZERO, $FC_BUILD_H_NULL, $FC_BUILD_H_RANGE;
CREATE TABLE $FC_BUILD_H_ZERO AS SELECT $ID_ZONE as ID_ZONE, COUNT(*) as FC_H_ZERO FROM $BUILDING_FC WHERE HEIGHT_WALL=0;
CREATE TABLE $FC_BUILD_H_NULL AS SELECT COUNT(*) as FC_H_NULL FROM $BUILDING_FC WHERE HEIGHT_WALL is null;
CREATE TABLE $FC_BUILD_H_RANGE AS SELECT COUNT(*) as FC_H_RANGE FROM $BUILDING_FC WHERE HEIGHT_WALL < 0 AND HEIGHT_WALL > 1000;

DROP TABLE IF EXISTS $FC_BUILD_STATS_EXT_ZONE;
CREATE TABLE $FC_BUILD_STATS_EXT_ZONE AS SELECT a.ID_ZONE, a.FC_H_ZERO, b.FC_H_NULL, c.FC_H_RANGE FROM $FC_BUILD_H_ZERO a, $FC_BUILD_H_NULL b, $FC_BUILD_H_RANGE c;

DROP TABLE IF EXISTS $FC_BUILD_H_ZERO, $FC_BUILD_H_NULL, $FC_BUILD_H_RANGE;


-------------------------------------------------------
-- Normalize buildings
-------------------------------------------------------

-- If the ZINDEX is null, then it's initialised to 0
DROP TABLE IF EXISTS $BUILDING;
CREATE TABLE $BUILDING (THE_GEOM geometry, ID_BUILD integer PRIMARY KEY, ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer, ID_ZONE varchar)
   AS SELECT ST_NORMALIZE(THE_GEOM), ID_BUILD, ID_SOURCE, HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE, MAIN_USE, CASE WHEN ZINDEX is null THEN 0 ELSE ZINDEX END, ID_ZONE FROM $BUILDING_FC;

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_BUILDING, $BU_ZONE, $BUILDING_FC;


-- Initialisation of heights and number of levels
---------------------------------------------------------

-- Update HEIGHT_WALL
UPDATE $BUILDING SET HEIGHT_WALL = CASE WHEN HEIGHT_WALL is null or HEIGHT_WALL = 0 THEN
(CASE WHEN HEIGHT_ROOF is null or HEIGHT_ROOF = 0 THEN (CASE WHEN NB_LEV is null or NB_LEV = 0 THEN $H_LEV_MIN ELSE (NB_LEV*$H_LEV_MIN) END) ELSE HEIGHT_ROOF END) ELSE HEIGHT_WALL END;

-- Update HEIGHT_ROOF
UPDATE $BUILDING SET HEIGHT_ROOF = CASE WHEN HEIGHT_ROOF is null or HEIGHT_ROOF = 0 THEN (CASE WHEN HEIGHT_WALL is null or HEIGHT_WALL = 0 THEN (
CASE WHEN NB_LEV is null or NB_LEV = 0 THEN $H_LEV_MIN ELSE (NB_LEV*$H_LEV_MIN) END) ELSE HEIGHT_WALL END) ELSE HEIGHT_ROOF END;

-- Update NB_LEV
-- If the NB_LEV parameter (in the abstract table) is equal to 1 or 2 (and HEIGHT_WALL > 10m) then apply the rule. Else, the NB_LEV is equal to 1
UPDATE $BUILDING SET NB_LEV = CASE WHEN TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=1) or
(TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=2) and HEIGHT_WALL>$H_THRESHOLD_LEV2) THEN
(CASE WHEN NB_LEV is null or NB_LEV = 0 THEN (CASE WHEN HEIGHT_WALL is null or HEIGHT_WALL = 0 THEN (CASE WHEN HEIGHT_ROOF is null or HEIGHT_ROOF = 0 THEN 1
ELSE TRUNCATE(HEIGHT_ROOF /$H_LEV_MIN) END)
 ELSE TRUNCATE(HEIGHT_WALL /$H_LEV_MIN) END) ELSE NB_LEV END) ELSE 1 END;

-- Control of heights and number of levels
---------------------------------------------------------

-- Check if HEIGHT_ROOF is lower than HEIGHT_WALL. If yes, then correct HEIGHT_ROOF
UPDATE $BUILDING SET HEIGHT_ROOF = 	CASE WHEN HEIGHT_WALL > HEIGHT_ROOF THEN HEIGHT_WALL ELSE HEIGHT_ROOF END;
-- Check if there is a high difference beetween the "real" and "theorical (based on the level number) roof heights
UPDATE $BUILDING SET HEIGHT_ROOF = 	CASE WHEN (NB_LEV * $H_LEV_MIN) > HEIGHT_ROOF THEN (NB_LEV * $H_LEV_MIN) ELSE HEIGHT_ROOF END;
-- Check if there is a high difference beetween the "real" and "theorical" (based on the level number) wall heights
UPDATE $BUILDING SET NB_LEV = 	CASE WHEN TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=1) or (TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=2) and HEIGHT_WALL>$H_THRESHOLD_LEV2) THEN (
									CASE WHEN (NB_LEV * $H_LEV_MAX) < HEIGHT_WALL THEN (HEIGHT_WALL / $H_LEV_MAX) ELSE NB_LEV END)
								ELSE NB_LEV END;


-- Create (spatial) indexes to improve upcoming treatments
CREATE INDEX ON $BUILDING(the_geom) USING RTREE;
CREATE INDEX ON $BUILDING(ID_ZONE);


-------------------------------------------------------
-- Second Control
-------------------------------------------------------

-- 1. Analyze at the city level
-----------------------

DROP TABLE IF EXISTS $BUILD_NUMB, $BUILD_VALID_ZONE, $BUILD_EMPTY, $BUILD_EQUALS_ZONE, $BUILD_OVERLAP_ZONE, $BUILD_H, $BUILD_H_RANGE, $BUILD_H_WALL_ROOF, $BUILD_LEV, $BUILD_LEV_RANGE, $BUILD_TYPE, $BUILD_TYPE_RANGE, $BUILDING_STATS_ZONE;

CREATE TABLE $BUILD_NUMB AS SELECT COUNT(*) as NB_BUILD FROM $BUILDING WHERE ID_ZONE=$ID_ZONE;
-- Count the number of buildings which geometry is not valid
CREATE TABLE $BUILD_VALID_ZONE AS SELECT COUNT(*) as NOT_VALID FROM $BUILDING WHERE NOT ST_IsValid(the_geom) AND ID_ZONE=$ID_ZONE;
-- Count the number of buildings which geometry is empty
CREATE TABLE $BUILD_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $BUILDING WHERE ST_IsEmpty(the_geom) AND ID_ZONE=$ID_ZONE;
-- Count the number of buildings which geometry is equal to an another one
CREATE TABLE $BUILD_EQUALS_ZONE AS SELECT COUNT(a.*) as IS_EQUALS FROM $BUILDING a, $BUILDING b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_BUILD<>b.ID_BUILD AND a.ID_ZONE=$ID_ZONE AND b.ID_ZONE=$ID_ZONE;
-- Count the number of building which overlaps another one.
-- Remark : The building ids have to be different and the buildings from table A (where we count) have to be in the studied city
CREATE TABLE $BUILD_OVERLAP_ZONE AS SELECT COUNT(a.*) as OVERLAP FROM $BUILDING a, $BUILDING b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_BUILD<>b.ID_BUILD AND a.ID_ZONE=$ID_ZONE;
-- Count the number of buildings which height is null or is outside the range [0;1000]
CREATE TABLE $BUILD_H AS SELECT COUNT(*) as H_NULL FROM $BUILDING WHERE HEIGHT_WALL is null AND ID_ZONE=$ID_ZONE;
CREATE TABLE $BUILD_H_RANGE AS SELECT COUNT(*) as H_RANGE FROM $BUILDING WHERE HEIGHT_WALL < 0 AND HEIGHT_WALL > 1000 AND ID_ZONE=$ID_ZONE;
-- Count the number of buildings where H_ROOF is smaller than H_WALL
CREATE TABLE $BUILD_H_WALL_ROOF AS SELECT COUNT(*) as H_ROOF_MIN_WALL FROM $BUILDING WHERE HEIGHT_WALL > HEIGHT_ROOF AND ID_ZONE=$ID_ZONE;
-- Count the number of buildings which number of level is null or is outside the range [1;200]
CREATE TABLE $BUILD_LEV AS SELECT COUNT(*) as LEV_NULL FROM $BUILDING WHERE NB_LEV is null AND ID_ZONE=$ID_ZONE;
CREATE TABLE $BUILD_LEV_RANGE AS SELECT COUNT(*) as LEV_RANGE FROM $BUILDING WHERE NB_LEV < 1 AND NB_LEV > 200 AND ID_ZONE=$ID_ZONE;
-- Count the number of buildings with no TYPE or where TYPE is not in the list of tags (Table NATURE_TAGS)
CREATE TABLE $BUILD_TYPE AS SELECT COUNT(*) as NO_TYPE FROM $BUILDING WHERE TYPE is null AND ID_ZONE=$ID_ZONE;
CREATE TABLE $BUILD_TYPE_RANGE AS SELECT COUNT(*) as TYPE_RANGE FROM $BUILDING WHERE TYPE NOT IN (SELECT TERM FROM $BUILDING_ABSTRACT_USE_TYPE) AND ID_ZONE=$ID_ZONE;

-- Merge all these information into one single table
CREATE TABLE $BUILDING_STATS_ZONE AS SELECT a.ID_ZONE, b.NB_BUILD, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP, g.FC_H_ZERO, g.FC_H_NULL, g.FC_H_RANGE, h.H_NULL, i.H_RANGE, j.H_ROOF_MIN_WALL, k.LEV_NULL, l.LEV_RANGE, m.NO_TYPE, n.TYPE_RANGE FROM $ZONE a, $BUILD_NUMB b, $BUILD_VALID_ZONE c, $BUILD_EMPTY d, $BUILD_EQUALS_ZONE e, $BUILD_OVERLAP_ZONE f, $FC_BUILD_STATS_ZONE g, $BUILD_H h, $BUILD_H_RANGE i, $BUILD_H_WALL_ROOF j, $BUILD_LEV k, $BUILD_LEV_RANGE l, $BUILD_TYPE m, $BUILD_TYPE_RANGE n;

-- Clean not needed layers
DROP TABLE IF EXISTS $BUILD_NUMB, $BUILD_EMPTY, $FC_BUILD_STATS_ZONE, $BUILD_H, $BUILD_H_RANGE, $BUILD_H_WALL_ROOF, $BUILD_LEV, $BUILD_LEV_RANGE, $BUILD_TYPE, $BUILD_TYPE_RANGE;


-- 2. Analyze at the city buffer level
-----------------------

DROP TABLE IF EXISTS $BUILD_VALID_EXT_ZONE, $BUILD_EMPTY, $BUILD_EQUALS_EXT_ZONE, $BUILD_OVERLAP_EXT_ZONE, $BUILD_H, $BUILD_H_RANGE, $BUILD_H_WALL_ROOF, $BUILD_LEV, $BUILD_LEV_RANGE, $BUILD_TYPE, $BUILD_TYPE_RANGE, $BUILDING_STATS_EXT_ZONE;

-- Count the number of invalid building, that are not in the studied city
-- This information will then be merged with the number of invalid builing, inside the studied city (from table $BUILD_VALID_ZONE), to compute the total number of invalid buildings at the extended city level
CREATE TABLE $BUILD_VALID_EXT_ZONE AS SELECT COUNT(*) as NOT_VALID FROM $BUILDING WHERE NOT ST_IsValid(the_geom) AND ID_ZONE<>$ID_ZONE;
CREATE TABLE $BUILD_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $BUILDING WHERE ST_IsEmpty(the_geom);
-- Count the number of building, that are not in the studied city, and which geometry is equal to an another one.
-- This information will then be merged with the number of builing that are equal, inside the studied city (from table $BUILD_EQUALS_ZONE), to compute the total number of buildings that are equal at the extended city level
CREATE TABLE $BUILD_EQUALS_EXT_ZONE AS SELECT COUNT(a.*) as IS_EQUALS FROM $BUILDING a, $BUILDING b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_BUILD<>b.ID_BUILD AND a.ID_ZONE<>$ID_ZONE AND b.ID_ZONE<>$ID_ZONE;
-- Count the number of building, that are not in the studied city, and which overlaps with another builing (which can be outside or inside the studied city)
-- This information will then be merged with the number of builing that overlaps, inside the studied city (from table $BUILD_OVERLAP_ZONE), to compute the total number of buildings that overlaps at the extended city level
CREATE TABLE $BUILD_OVERLAP_EXT_ZONE AS SELECT COUNT(a.*) as OVERLAP FROM $BUILDING a, $BUILDING b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_BUILD<>b.ID_BUILD AND a.ID_ZONE<>$ID_ZONE;
CREATE TABLE $BUILD_H AS SELECT COUNT(*) as H_NULL FROM $BUILDING WHERE HEIGHT_WALL is null;
CREATE TABLE $BUILD_H_RANGE AS SELECT COUNT(*) as H_RANGE FROM $BUILDING WHERE HEIGHT_WALL < 0 AND HEIGHT_WALL > 1000;
CREATE TABLE $BUILD_H_WALL_ROOF AS SELECT COUNT(*) as H_ROOF_MIN_WALL FROM $BUILDING WHERE HEIGHT_WALL > HEIGHT_ROOF;
CREATE TABLE $BUILD_LEV AS SELECT COUNT(*) as LEV_NULL FROM $BUILDING WHERE NB_LEV is null;
CREATE TABLE $BUILD_LEV_RANGE AS SELECT COUNT(*) as LEV_RANGE FROM $BUILDING WHERE NB_LEV < 0 AND NB_LEV > 200;
CREATE TABLE $BUILD_TYPE AS SELECT COUNT(*) as NO_TYPE FROM $BUILDING WHERE TYPE is null;
CREATE TABLE $BUILD_TYPE_RANGE AS SELECT COUNT(*) as TYPE_RANGE FROM $BUILDING WHERE TYPE NOT IN (SELECT TERM FROM $BUILDING_ABSTRACT_USE_TYPE);

CREATE TABLE $BUILDING_STATS_EXT_ZONE AS SELECT a.ID_ZONE, COUNT(b.*) as NB_BUILD, (c.NOT_VALID + q.NOT_VALID) as NOT_VALID, d.IS_EMPTY, (e.IS_EQUALS + o.IS_EQUALS) as IS_EQUALS, (f.OVERLAP + p.OVERLAP) as OVERLAP, g.FC_H_ZERO, g.FC_H_NULL, g.FC_H_RANGE, h.H_NULL, i.H_RANGE, j.H_ROOF_MIN_WALL, k.LEV_NULL, l.LEV_RANGE, m.NO_TYPE, n.TYPE_RANGE FROM $ZONE a, $BUILDING b, $BUILD_VALID_ZONE c, $BUILD_EMPTY d, $BUILD_EQUALS_ZONE e, $BUILD_OVERLAP_ZONE f, $FC_BUILD_STATS_EXT_ZONE g, $BUILD_H h, $BUILD_H_RANGE i, $BUILD_H_WALL_ROOF j, $BUILD_LEV k, $BUILD_LEV_RANGE l, $BUILD_TYPE m, $BUILD_TYPE_RANGE n, $BUILD_EQUALS_EXT_ZONE o, $BUILD_OVERLAP_EXT_ZONE p, $BUILD_VALID_EXT_ZONE q;

DROP TABLE IF EXISTS $BUILD_VALID_ZONE, $BUILD_VALID_EXT_ZONE, $BUILD_EMPTY, $BUILD_EQUALS_ZONE, $BUILD_OVERLAP_ZONE, $BUILD_EQUALS_EXT_ZONE, $BUILD_OVERLAP_EXT_ZONE, $FC_BUILD_STATS_EXT_ZONE, $BUILD_H, $BUILD_H_RANGE, $BUILD_H_WALL_ROOF, $BUILD_LEV, $BUILD_LEV_RANGE, $BUILD_TYPE, $BUILD_TYPE_RANGE;



---------------------------------------------------------------------------------
-- 2. PROCESS ROADS
---------------------------------------------------------------------------------


-------------------------------------------------------
-- First Control
-------------------------------------------------------

-- 1. Analyze at the city level
-----------------------

DROP TABLE IF EXISTS $ROAD_FC_W_ZERO, $ROAD_FC_W_NULL, $ROAD_FC_W_RANGE, $R_FC_STATS_ZONE;
CREATE TABLE $ROAD_FC_W_ZERO AS SELECT COUNT(a.*) as FC_W_ZERO FROM $INPUT_ROAD a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.WIDTH=0;
CREATE TABLE $ROAD_FC_W_NULL AS SELECT COUNT(a.*) as FC_W_NULL FROM $INPUT_ROAD a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.WIDTH is null;
CREATE TABLE $ROAD_FC_W_RANGE AS SELECT COUNT(a.*) as FC_W_RANGE FROM $INPUT_ROAD a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.WIDTH<0 and a.WIDTH>100;

CREATE TABLE $R_FC_STATS_ZONE AS SELECT a.ID_ZONE, b.FC_W_ZERO, c.FC_W_NULL, d.FC_W_RANGE FROM $ZONE a, $ROAD_FC_W_ZERO b, $ROAD_FC_W_NULL c, $ROAD_FC_W_RANGE d;

-- 2. Analyze at the city buffer level
-----------------------

DROP TABLE IF EXISTS $ROAD_FC_W_ZERO, $ROAD_FC_W_NULL, $ROAD_FC_W_RANGE, $R_FC_STATS_EXT_ZONE;
CREATE TABLE $ROAD_FC_W_ZERO AS SELECT COUNT(*) as FC_W_ZERO FROM $INPUT_ROAD WHERE WIDTH=0;
CREATE TABLE $ROAD_FC_W_NULL AS SELECT COUNT(*) as FC_W_NULL FROM $INPUT_ROAD WHERE WIDTH is null;
CREATE TABLE $ROAD_FC_W_RANGE AS SELECT COUNT(*) as FC_W_RANGE FROM $INPUT_ROAD WHERE WIDTH<0 and WIDTH>100;


CREATE TABLE $R_FC_STATS_EXT_ZONE AS SELECT a.ID_ZONE, b.FC_W_ZERO, c.FC_W_NULL, d.FC_W_RANGE FROM $ZONE a, $ROAD_FC_W_ZERO b, $ROAD_FC_W_NULL c, $ROAD_FC_W_RANGE d;

-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $ROAD_FC_W_ZERO, $ROAD_FC_W_NULL, $ROAD_FC_W_RANGE;


-------------------------------------------------------
-- Normalize roads
-------------------------------------------------------

-- If the ZINDEX is null, then it's initialised to 0
DROP TABLE IF EXISTS $ROAD;
CREATE TABLE $ROAD (THE_GEOM geometry, ID_ROAD serial, ID_SOURCE varchar(24), WIDTH double, TYPE varchar, SURFACE varchar, SIDEWALK varchar, ZINDEX integer)
    AS SELECT THE_GEOM, null, ID_SOURCE, WIDTH, TYPE, SURFACE, SIDEWALK, CASE WHEN ZINDEX is null THEN 0 ELSE ZINDEX END FROM ST_EXPLODE('$INPUT_ROAD');

-- Updating the width using the rule ("If null or equal to 0 then replace by the minimum width defined in the ROAD ABSTRACT_PARAMETERS table")
UPDATE $ROAD SET WIDTH = (SELECT b.MIN_WIDTH FROM $ROAD_ABSTRACT_PARAMETERS b WHERE b.TERM=TYPE) WHERE WIDTH = 0 or WIDTH is null;

CREATE INDEX ON $ROAD(the_geom) USING RTREE;


-------------------------------------------------------
-- Second Control
-------------------------------------------------------

-- 1. Analyze at the city level
-----------------------

-- Filter the roads that intersects the $ZONE and store it into a dedicated temporary table in order to optimize upcoming controls (especially $ROAD_EQUALS and $ROAD_OVERLAP)
DROP TABLE IF EXISTS $ROAD_ZONE;
CREATE TABLE $ROAD_ZONE AS SELECT a.* FROM $ROAD a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
CREATE INDEX ON $ROAD_ZONE(the_geom) USING RTREE;


DROP TABLE IF EXISTS $ROAD_VALID, $ROAD_EMPTY, $ROAD_EQUALS, $ROAD_OVERLAP, $ROAD_W, $ROAD_W_RANGE, $ROAD_TYPE, $ROAD_TYPE_RANGE, $ROAD_STATS_ZONE;
-- Count the number of rails which geometry is not valid
CREATE TABLE $ROAD_VALID AS SELECT COUNT(*) as NOT_VALID FROM $ROAD_ZONE WHERE NOT ST_IsValid(the_geom);
-- Count the number of rails which geometry is empty
CREATE TABLE $ROAD_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $ROAD_ZONE WHERE ST_IsEmpty(the_geom);
-- Count the number of rails which geometry is equal to an another one
CREATE TABLE $ROAD_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $ROAD_ZONE a, $ROAD_ZONE b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_ROAD<>b.ID_ROAD;
-- Count the number of rails which overlaps another one
CREATE TABLE $ROAD_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $ROAD_ZONE a, $ROAD_ZONE b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_ROAD<>b.ID_ROAD;
-- Count the number of roads where the width is null or outside the range [0;100]
CREATE TABLE $ROAD_W AS SELECT COUNT(*) as W_NULL FROM $ROAD_ZONE WHERE WIDTH is null;
CREATE TABLE $ROAD_W_RANGE AS SELECT COUNT(*) as W_RANGE FROM $ROAD_ZONE WHERE WIDTH<0 AND WIDTH>100;
-- Count the number of vegetation areas with no TYPE or where TYPE is not in the list of tags (Table NATURE_TAGS)
CREATE TABLE $ROAD_TYPE AS SELECT COUNT(*) as NO_TYPE FROM $ROAD_ZONE WHERE TYPE is null;
CREATE TABLE $ROAD_TYPE_RANGE AS SELECT COUNT(*) as TYPE_RANGE FROM $ROAD_ZONE WHERE TYPE NOT IN (SELECT TERM FROM $ROAD_ABSTRACT_TYPE);

-- Merge all these information into one single table
CREATE TABLE $ROAD_STATS_ZONE AS SELECT a.ID_ZONE, COUNT(b.*) as NB_ROAD, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP, g.FC_W_ZERO, g.FC_W_NULL, g.FC_W_RANGE, j.W_NULL, k.W_RANGE, l.NO_TYPE, m.TYPE_RANGE FROM $ZONE a, $ROAD_ZONE b, $ROAD_VALID c, $ROAD_EMPTY d, $ROAD_EQUALS e, $ROAD_OVERLAP f, $R_FC_STATS_ZONE g, $ROAD_W j, $ROAD_W_RANGE k, $ROAD_TYPE l, $ROAD_TYPE_RANGE m;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $ROAD_VALID, $ROAD_EMPTY, $ROAD_EQUALS, $ROAD_OVERLAP, $ROAD_W, $ROAD_W_RANGE, $ROAD_TYPE, $ROAD_TYPE_RANGE, $R_FC_STATS_ZONE;


-- 2. Analyze at the city buffer level
-----------------------

DROP TABLE IF EXISTS $ROAD_VALID, $ROAD_EMPTY, $ROAD_EQUALS, $ROAD_OVERLAP, $ROAD_W, $ROAD_W_RANGE, $ROAD_TYPE, $ROAD_TYPE_RANGE, $ROAD_STATS_EXT_ZONE;
-- Count the number of rails which geometry is not valid
CREATE TABLE $ROAD_VALID AS SELECT COUNT(*) as NOT_VALID FROM $ROAD WHERE NOT ST_IsValid(the_geom);
-- Count the number of rails which geometry is empty
CREATE TABLE $ROAD_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $ROAD WHERE ST_IsEmpty(the_geom);
-- Count the number of rails which geometry is equal to an another one
CREATE TABLE $ROAD_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $ROAD a, $ROAD b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_ROAD<>b.ID_ROAD;
-- Count the number of rails which overlaps another one
CREATE TABLE $ROAD_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $ROAD a, $ROAD b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_ROAD<>b.ID_ROAD;
-- Count the number of roads where the width is null or outside the range [0;20]
CREATE TABLE $ROAD_W AS SELECT COUNT(*) as W_NULL FROM $ROAD WHERE WIDTH is null;
CREATE TABLE $ROAD_W_RANGE AS SELECT COUNT(*) as W_RANGE FROM $ROAD WHERE WIDTH < 0 AND WIDTH > 20;
-- Count the number of vegetation areas with no TYPE or where TYPE is not in the list of tags (Table NATURE_TAGS)
CREATE TABLE $ROAD_TYPE AS SELECT COUNT(*) as NO_TYPE FROM $ROAD WHERE TYPE is null;
CREATE TABLE $ROAD_TYPE_RANGE AS SELECT COUNT(*) as TYPE_RANGE FROM $ROAD WHERE TYPE NOT IN (SELECT TERM FROM $ROAD_ABSTRACT_TYPE);

-- Merge all these information into one single table
CREATE TABLE $ROAD_STATS_EXT_ZONE AS SELECT a.ID_ZONE, COUNT(b.*) as NB_ROAD, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP, g.FC_W_ZERO, g.FC_W_NULL, g.FC_W_RANGE, j.W_NULL, k.W_RANGE, l.NO_TYPE, m.TYPE_RANGE FROM $ZONE a, $ROAD b, $ROAD_VALID c, $ROAD_EMPTY d, $ROAD_EQUALS e, $ROAD_OVERLAP f, $R_FC_STATS_EXT_ZONE g, $ROAD_W j, $ROAD_W_RANGE k, $ROAD_TYPE l, $ROAD_TYPE_RANGE m;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $ROAD_ZONE, $INPUT_ROAD, $ROAD_VALID, $ROAD_EMPTY, $ROAD_EQUALS, $ROAD_OVERLAP, $ROAD_W, $ROAD_W_RANGE, $ROAD_TYPE, $ROAD_TYPE_RANGE, $R_FC_STATS_EXT_ZONE;



---------------------------------------------------------------------------------
-- 3. PROCESS RAILS
---------------------------------------------------------------------------------


-------------------------------------------------------
-- Normalize
-------------------------------------------------------

-- If the ZINDEX is null, then it's initialised to 0
DROP TABLE IF EXISTS $RAIL;
CREATE TABLE $RAIL(THE_GEOM geometry, ID_RAIL serial, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
	AS SELECT THE_GEOM, null, ID_SOURCE, TYPE, CASE WHEN ZINDEX is null THEN 0 ELSE ZINDEX END FROM ST_EXPLODE('$INPUT_RAIL');
CREATE INDEX ON $RAIL(the_geom) USING RTREE;

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_RAIL;


-------------------------------------------------------
-- Control
-------------------------------------------------------

-- Only analyze at the city level (no need to intersect with the $ZONE. It has already been done when importing the layer)
-----------------------------------------------
DROP TABLE IF EXISTS $RAIL_NB, $RAIL_VALID, $RAIL_EMPTY, $RAIL_EQUALS, $RAIL_OVERLAP, $RAIL_TYPE, $RAIL_TYPE_RANGE, $RAIL_STATS_ZONE;
-- Count the number of rails
CREATE TABLE $RAIL_NB AS SELECT COUNT(*) as NB_RAIL FROM $RAIL;
-- Count the number of rails which geometry is not valid
CREATE TABLE $RAIL_VALID AS SELECT COUNT(*) as NOT_VALID FROM $RAIL WHERE NOT ST_IsValid(the_geom);
-- Count the number of rails which geometry is empty
CREATE TABLE $RAIL_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $RAIL WHERE ST_IsEmpty(the_geom);
-- Count the number of rails which geometry is equal to an another one
CREATE TABLE $RAIL_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $RAIL a, $RAIL b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_RAIL<>b.ID_RAIL;
-- Count the number of rails which overlaps another one
CREATE TABLE $RAIL_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $RAIL a, $RAIL b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_RAIL<>b.ID_RAIL;
-- Count the number of rails with no TYPE or where TYPE is not in the list of tags (Table NATURE_TAGS)
CREATE TABLE $RAIL_TYPE AS SELECT COUNT(*) as NO_TYPE FROM $RAIL WHERE TYPE is null;
CREATE TABLE $RAIL_TYPE_RANGE AS SELECT COUNT(*) as TYPE_RANGE FROM $RAIL WHERE TYPE NOT IN (SELECT TERM FROM $RAIL_ABSTRACT_TYPE);

-- Merge all these informations into one single table
CREATE TABLE $RAIL_STATS_ZONE AS SELECT a.ID_ZONE, b.NB_RAIL, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP, g.NO_TYPE, h.TYPE_RANGE FROM $ZONE a, $RAIL_NB b, $RAIL_VALID c, $RAIL_EMPTY d, $RAIL_EQUALS e, $RAIL_OVERLAP f, $RAIL_TYPE g, $RAIL_TYPE_RANGE h;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $RAIL_NB, $RAIL_VALID, $RAIL_EMPTY, $RAIL_EQUALS, $RAIL_OVERLAP, $RAIL_TYPE, $RAIL_TYPE_RANGE;



---------------------------------------------------------------------------------
-- 4. PROCESS HYDROGRAPHIC AREAS
---------------------------------------------------------------------------------


-------------------------------------------------------
-- Normalize
-------------------------------------------------------

DROP TABLE IF EXISTS $HYDRO;
CREATE TABLE $HYDRO (THE_GEOM geometry, ID_HYDRO serial, ID_SOURCE varchar(24))
	AS SELECT THE_GEOM, null, ID_SOURCE FROM ST_EXPLODE('$INPUT_HYDRO');
CREATE INDEX ON $HYDRO(the_geom) USING RTREE;

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_HYDRO;


-------------------------------------------------------
-- Control
-------------------------------------------------------

-- 1. Analyze at the city level
-----------------------------------------------

DROP TABLE IF EXISTS $HYDRO_NUM_ZONE, $HYDRO_VALID, $HYDRO_EMPTY, $HYDRO_EQUALS, $HYDRO_OVERLAP, $HYDRO_STATS_ZONE;

-- Count the number of hydrographic areas within the city
CREATE TABLE $HYDRO_NUM_ZONE AS SELECT COUNT(a.*) as NB_HYDRO FROM $HYDRO a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
-- Count the number of hydrographic areas which geometry is not valid
CREATE TABLE $HYDRO_VALID AS SELECT COUNT(a.*) as NOT_VALID FROM $HYDRO a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND NOT ST_IsValid(a.the_geom);
-- Count the number of hydrographic areas which geometry is empty
CREATE TABLE $HYDRO_EMPTY AS SELECT COUNT(a.*) as IS_EMPTY FROM $HYDRO a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND ST_IsEmpty(a.the_geom);
-- Count the number of hydrographic areas which geometry is equal to an another one
CREATE TABLE $HYDRO_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $HYDRO a, $HYDRO b, $ZONE c WHERE a.the_geom && c.the_geom AND ST_INTERSECTS(a.the_geom, c.the_geom) AND a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_HYDRO<>b.ID_HYDRO;
-- Count the number of hydrographic areas which overlaps another one
CREATE TABLE $HYDRO_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $HYDRO a, $HYDRO b, $ZONE c WHERE a.the_geom && c.the_geom AND ST_INTERSECTS(a.the_geom, c.the_geom) AND a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_HYDRO<>b.ID_HYDRO;

-- Merge all these informations into one single table
CREATE TABLE $HYDRO_STATS_ZONE AS SELECT a.ID_ZONE, b.NB_HYDRO, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP FROM $ZONE a, $HYDRO_NUM_ZONE b, $HYDRO_VALID c, $HYDRO_EMPTY d, $HYDRO_EQUALS e, $HYDRO_OVERLAP f;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $HYDRO_NUM_ZONE, $HYDRO_VALID, $HYDRO_EMPTY, $HYDRO_EQUALS, $HYDRO_OVERLAP;


-- 2. Analyze at the extended city level
-----------------------------------------------

DROP TABLE IF EXISTS $HYDRO_VALID, $HYDRO_EMPTY, $HYDRO_EQUALS, $HYDRO_OVERLAP, $HYDRO_STATS_EXT_ZONE;

CREATE TABLE $HYDRO_VALID AS SELECT COUNT(*) as NOT_VALID FROM $HYDRO WHERE NOT ST_IsValid(the_geom);
CREATE TABLE $HYDRO_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $HYDRO WHERE ST_IsEmpty(the_geom);
CREATE TABLE $HYDRO_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $HYDRO a, $HYDRO b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_HYDRO<>b.ID_HYDRO;
CREATE TABLE $HYDRO_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $HYDRO a, $HYDRO b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_HYDRO<>b.ID_HYDRO;

CREATE TABLE $HYDRO_STATS_EXT_ZONE AS SELECT a.ID_ZONE, COUNT(b.*) as NB_HYDRO, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP FROM $ZONE a, $HYDRO b, $HYDRO_VALID c, $HYDRO_EMPTY d, $HYDRO_EQUALS e, $HYDRO_OVERLAP f;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $HYDRO_VALID, $HYDRO_EMPTY, $HYDRO_EQUALS, $HYDRO_OVERLAP;



---------------------------------------------------------------------------------
-- 5. PROCESS VEGETATION AREAS
---------------------------------------------------------------------------------


-------------------------------------------------------
-- Normalize
-------------------------------------------------------

DROP TABLE IF EXISTS $VEGET;
CREATE TABLE $VEGET (THE_GEOM geometry, ID_VEGET serial, ID_SOURCE varchar(24), TYPE varchar, HEIGHT_CLASS varchar)
	AS SELECT THE_GEOM, null, ID_SOURCE, TYPE, null FROM ST_EXPLODE('$INPUT_VEGET');
CREATE INDEX ON $VEGET(the_geom) USING RTREE;

-- Update the vegetation height class (high or low)
UPDATE $VEGET SET HEIGHT_CLASS = (SELECT b.HEIGHT_CLASS FROM $VEGET_ABSTRACT_PARAMETERS b WHERE b.TERM=TYPE);

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_VEGET;


-------------------------------------------------------
-- Control
-------------------------------------------------------

-- 1. Analyze at the city level
--------------------------------

DROP TABLE IF EXISTS $VEGET_NUM_ZONE, $VEGET_VALID, $VEGET_EMPTY, $VEGET_EQUALS, $VEGET_OVERLAP, $VEGET_TYPE, $VEGET_TYPE_RANGE, $VEGET_STATS_ZONE;

-- Count the number of vegetation areas within the city
CREATE TABLE $VEGET_NUM_ZONE AS SELECT COUNT(a.*) as NB_VEGET FROM $VEGET a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
-- Count the number of vegetation areas which geometry is not valid
CREATE TABLE $VEGET_VALID AS SELECT COUNT(a.*) as NOT_VALID FROM $VEGET a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND NOT ST_IsValid(a.the_geom);
-- Count the number of vegetation areas which geometry is empty
CREATE TABLE $VEGET_EMPTY AS SELECT COUNT(a.*) as IS_EMPTY FROM $VEGET a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND ST_IsEmpty(a.the_geom);
-- Count the number of vegetation areas which geometry is equal to an another one
CREATE TABLE $VEGET_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $VEGET a, $VEGET b, $ZONE c WHERE a.the_geom && c.the_geom AND ST_INTERSECTS(a.the_geom, c.the_geom) AND a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_VEGET<>b.ID_VEGET;
-- Count the number of vegetation areas which overlaps another one
CREATE TABLE $VEGET_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $VEGET a, $VEGET b, $ZONE c WHERE a.the_geom && c.the_geom AND ST_INTERSECTS(a.the_geom, c.the_geom) AND a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_VEGET<>b.ID_VEGET;
-- Count the number of vegetation areas with no TYPE or where TYPE is not in the list of tags (Table NATURE_TAGS)
CREATE TABLE $VEGET_TYPE AS SELECT COUNT(a.*) as NO_TYPE FROM $VEGET a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.TYPE is null;
CREATE TABLE $VEGET_TYPE_RANGE AS SELECT COUNT(a.*) as TYPE_RANGE FROM $VEGET a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.TYPE NOT IN (SELECT TERM FROM $VEGET_ABSTRACT_TYPE);

-- Merge all these informations into one single table
CREATE TABLE $VEGET_STATS_ZONE AS SELECT a.ID_ZONE, b.NB_VEGET, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP, g.NO_TYPE, h.TYPE_RANGE FROM $ZONE a, $VEGET_NUM_ZONE b, $VEGET_VALID c, $VEGET_EMPTY d, $VEGET_EQUALS e, $VEGET_OVERLAP f, $VEGET_TYPE g, $VEGET_TYPE_RANGE h;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $VEGET_NUM_ZONE, $VEGET_VALID, $VEGET_EMPTY, $VEGET_EQUALS, $VEGET_OVERLAP, $VEGET_TYPE, $VEGET_TYPE_RANGE;


-- 2. Analyze at the extended city level
--------------------------------

DROP TABLE IF EXISTS $VEGET_VALID, $VEGET_EMPTY, $VEGET_EQUALS, $VEGET_OVERLAP, $VEGET_TYPE, $VEGET_TYPE_RANGE, $VEGET_STATS_EXT_ZONE;

CREATE TABLE $VEGET_VALID AS SELECT COUNT(*) as NOT_VALID FROM $VEGET WHERE NOT ST_IsValid(the_geom);
CREATE TABLE $VEGET_EMPTY AS SELECT COUNT(*) as IS_EMPTY FROM $VEGET WHERE ST_IsEmpty(the_geom);
CREATE TABLE $VEGET_EQUALS AS SELECT COUNT(a.*) as IS_EQUALS FROM $VEGET a, $VEGET b WHERE a.the_geom && b.the_geom AND ST_Equals(a.the_geom, b.the_geom) AND a.ID_VEGET<>b.ID_VEGET;
CREATE TABLE $VEGET_OVERLAP AS SELECT COUNT(a.*) as OVERLAP FROM $VEGET a, $VEGET b WHERE a.the_geom && b.the_geom AND ST_OVERLAPS(a.the_geom, b.the_geom) AND a.ID_VEGET<>b.ID_VEGET;
CREATE TABLE $VEGET_TYPE AS SELECT COUNT(*) as NO_TYPE FROM $VEGET WHERE TYPE is null;
CREATE TABLE $VEGET_TYPE_RANGE AS SELECT COUNT(*) as TYPE_RANGE FROM $VEGET WHERE TYPE NOT IN (SELECT TERM FROM $VEGET_ABSTRACT_TYPE);

CREATE TABLE $VEGET_STATS_EXT_ZONE AS SELECT a.ID_ZONE, COUNT(b.*) as NB_VEGET, c.NOT_VALID, d.IS_EMPTY, e.IS_EQUALS, f.OVERLAP, g.NO_TYPE, h.TYPE_RANGE FROM $ZONE a, $VEGET b, $VEGET_VALID c, $VEGET_EMPTY d, $VEGET_EQUALS e, $VEGET_OVERLAP f, $VEGET_TYPE g, $VEGET_TYPE_RANGE h;


-- Clean not needed layers
-----------------------
DROP TABLE IF EXISTS $VEGET_VALID, $VEGET_EMPTY, $VEGET_EQUALS, $VEGET_OVERLAP, $VEGET_TYPE, $VEGET_TYPE_RANGE;