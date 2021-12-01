-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--
-- 																								--
-- Title : Data Input Model pre-process 														--
-- Project : URCLIM / Paendora (ADEME - MODEVAL 2017-5)											--
-- Abstract : This script aims at pre-process input data that will then feed the RSU chain.		--
--																								--
-- Author : Gwendall Petit (DECIDE Team, Lab-STICC CNRS UMR 6285)
-- Author : Erwan Bocher (DECIDE Team, Lab-STICC CNRS UMR 6285)	                                --
-- Last update : 12/11/2020 - remove statistics																		--
-- Licence : GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)                                  --
-- 																					            --
-- Parameters, to be used in this script:                                                       --
--  - EXPAND : The distance used to select objects around the ZONE (in meters - default 1000)   --
--  - hLevMin: theoretical minimum (building) level height (in meters - default value = 3)      --
--  - hLevMax: theoretical maximum (building) level height (in meters - default value = 15)     --
--  - hThresholdLev2: threshold used to take into account (or not) commercial buildings         --
--    (where Nb_lev_rule = 2) (in meters - default value = 10)                                  --
--																								--
-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--

--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
-- ENTER INTO THE GEOCLIMATE MODEL
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------
-- 0. CREATE ZONE_NEIGHBORS TABLE (using the EXPAND parameter and ZONE table)
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS $ZONE_NEIGHBORS;
CREATE TABLE $ZONE_NEIGHBORS (the_geom geometry, ID_ZONE varchar) AS SELECT ST_FORCE2D(THE_GEOM) as the_geom, ID_ZONE FROM $ZONE UNION SELECT ST_DIFFERENCE(ST_EXPAND(the_geom, $EXPAND), the_geom) as the_geom, 'outside' FROM $ZONE;
CREATE SPATIAL INDEX ON $ZONE_NEIGHBORS (the_geom);

---------------------------------------------------------------------------------
-- 1. PROCESS BUILDINGS
---------------------------------------------------------------------------------

-- Add an id (primary key, called ID_BUILD) to the input layer ($INPUT_BUILDING) and create indexes

DROP TABLE IF EXISTS $BU_ZONE;
CREATE TABLE $BU_ZONE (THE_GEOM geometry, ID_BUILD serial, ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer) AS SELECT ST_FORCE2D(THE_GEOM), CAST((row_number() over()) as Integer), ID_SOURCE, HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE, MAIN_USE, ZINDEX FROM $INPUT_BUILDING;
CREATE SPATIAL INDEX ON $BU_ZONE (the_geom);
CREATE INDEX ON $BU_ZONE (ID_BUILD);


-------------------------------------------------------
-- Identify the city id where the building is
-------------------------------------------------------

-- 1- Select buildings that are within a city and assign the a ID_ZONE to the building
DROP TABLE IF EXISTS $BUILD_WITHIN_ZONE;
CREATE TABLE $BUILD_WITHIN_ZONE AS SELECT a.ID_BUILD, b.ID_ZONE as ID_ZONE FROM $BU_ZONE a, $ZONE_NEIGHBORS b WHERE a.the_geom && b.the_geom AND ST_CONTAINS(b.the_geom, a.the_geom);

-- 2- Select buildings that are on a boundary (not within a city)
DROP TABLE IF EXISTS $BUILD_OUTER_ZONE;
CREATE TABLE $BUILD_OUTER_ZONE AS SELECT * FROM $BU_ZONE WHERE ID_BUILD NOT IN (SELECT ID_BUILD FROM $BUILD_WITHIN_ZONE);

-- 3- Associate building to city, depending on the maximum surface of intersection, only for buildings that are not within a city
DROP TABLE IF EXISTS $BUILD_OUTER_ZONE_MATRIX ;
CREATE TABLE $BUILD_OUTER_ZONE_MATRIX (ID_BUILD integer primary key, ID_ZONE varchar) AS SELECT a.ID_BUILD , (SELECT ID_ZONE FROM $ZONE_NEIGHBORS b WHERE a.THE_GEOM && b.THE_GEOM ORDER BY ST_AREA(ST_INTERSECTION(a.THE_GEOM, b.THE_GEOM)) DESC LIMIT 1) AS ID_ZONE FROM $BUILD_OUTER_ZONE a WHERE ST_NUMGEOMETRIES(a.THE_GEOM)=1;

-- 4- Merge into one single table these information
DROP TABLE IF EXISTS $BUILD_ZONE_MATRIX ;
CREATE TABLE $BUILD_ZONE_MATRIX (ID_BUILD integer primary key, ID_ZONE varchar) AS SELECT * FROM $BUILD_WITHIN_ZONE UNION SELECT * FROM $BUILD_OUTER_ZONE_MATRIX;
CREATE INDEX ON $BUILD_ZONE_MATRIX USING BTREE(ID_BUILD);

-- Join this "matrix" to the initial table (with all building information) (FC = First Control)
DROP TABLE IF EXISTS $BUILDING_FC ;
CREATE TABLE $BUILDING_FC AS SELECT a.*, b.ID_ZONE FROM $BU_ZONE a LEFT JOIN $BUILD_ZONE_MATRIX b ON a.ID_BUILD=b.ID_BUILD;

DROP TABLE IF EXISTS $ZONE_NEIGHBORS, $BUILD_WITHIN_ZONE, $BUILD_OUTER_ZONE, $BUILD_OUTER_ZONE_MATRIX, $BUILD_ZONE_MATRIX;

-------------------------------------------------------
-- Normalize buildings
-------------------------------------------------------

-- If the ZINDEX is null, then it's initialised to 0
DROP TABLE IF EXISTS $BUILDING;
CREATE TABLE $BUILDING (THE_GEOM geometry, ID_BUILD integer PRIMARY KEY, ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer, ID_ZONE varchar)
   AS SELECT ST_FORCE2D(ST_MAKEVALID(THE_GEOM)) as the_geom, ID_BUILD, ID_SOURCE, HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE, MAIN_USE, CASE WHEN ZINDEX is null THEN 0 ELSE ZINDEX END, ID_ZONE FROM $BUILDING_FC;

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_BUILDING, $BU_ZONE;


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
ELSE CASE WHEN ROUND(HEIGHT_ROOF /$H_LEV_MIN)=0 THEN 1 ELSE ROUND(HEIGHT_ROOF /$H_LEV_MIN) END END)
 ELSE CASE WHEN ROUND(HEIGHT_ROOF /$H_LEV_MIN)=0 THEN 1 ELSE ROUND(HEIGHT_ROOF /$H_LEV_MIN) END END) ELSE NB_LEV END) ELSE 1 END;

-- Control of heights and number of levels
---------------------------------------------------------

-- Check if HEIGHT_ROOF is lower than HEIGHT_WALL. If yes, then correct HEIGHT_ROOF
UPDATE $BUILDING SET HEIGHT_ROOF = 	CASE WHEN HEIGHT_WALL > HEIGHT_ROOF THEN HEIGHT_WALL ELSE HEIGHT_ROOF END;
-- Check if there is a high difference between the "real" and "theoretical" (based on the level number) roof heights
UPDATE $BUILDING SET HEIGHT_ROOF = 	CASE WHEN (NB_LEV * $H_LEV_MIN) > HEIGHT_ROOF THEN (NB_LEV * $H_LEV_MIN) ELSE HEIGHT_ROOF END;
-- Check if there is a high difference between the "real" and "theoretical" (based on the level number) wall heights
UPDATE $BUILDING SET NB_LEV = 	CASE WHEN TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=1) or (TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=2) and HEIGHT_WALL>$H_THRESHOLD_LEV2) THEN (
									CASE WHEN (NB_LEV * $H_LEV_MAX) < HEIGHT_WALL THEN (HEIGHT_WALL / $H_LEV_MAX) ELSE NB_LEV END)
								ELSE NB_LEV END;

---------------------------------------------------------------------------------
-- 2. PROCESS ROADS
---------------------------------------------------------------------------------

-- If the ZINDEX is null, then it's initialised to 0
DROP TABLE IF EXISTS $ROAD;
CREATE TABLE $ROAD (THE_GEOM geometry, ID_ROAD serial, ID_SOURCE varchar(24), WIDTH DOUBLE PRECISION, TYPE varchar, SURFACE varchar, SIDEWALK varchar, ZINDEX integer, CROSSING varchar, MAXSPEED INTEGER, DIRECTION INTEGER)
    AS SELECT ST_FORCE2D(ST_MAKEVALID(THE_GEOM)) as the_geom, CAST((row_number() over()) as Integer), ID_SOURCE, WIDTH, TYPE, SURFACE, SIDEWALK, CASE WHEN ZINDEX is null THEN 0 ELSE ZINDEX END, CROSSING, -1,
    CASE WHEN SENS='Double' then 3 WHEN SENS='Direct' then 1  WHEN SENS='Inverse' then 2 else -1 end  FROM ST_EXPLODE('$INPUT_ROAD');

-- Updating the width using the rule ("If null or equal to 0 then replace by the minimum width defined in the ROAD ABSTRACT_PARAMETERS table")
UPDATE $ROAD SET WIDTH = (SELECT b.MIN_WIDTH FROM $ROAD_ABSTRACT_PARAMETERS b WHERE b.TERM=TYPE) WHERE WIDTH = 0 or WIDTH is null or WIDTH < 0;

-- Filling the CROSSING column with a 'null' value when no value
UPDATE $ROAD SET CROSSING = 'null' WHERE CROSSING is null;

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_ROAD;


---------------------------------------------------------------------------------
-- 3. PROCESS RAILS
---------------------------------------------------------------------------------


-- If the ZINDEX is null, then it's initialised to 0
DROP TABLE IF EXISTS $RAIL;
CREATE TABLE $RAIL(THE_GEOM geometry, ID_RAIL serial, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer, CROSSING varchar)
	AS SELECT ST_FORCE2D(ST_MAKEVALID(THE_GEOM)) as the_geom, CAST((row_number() over()) as Integer), ID_SOURCE, TYPE, CASE WHEN ZINDEX is null THEN 0 ELSE ZINDEX END, CROSSING FROM ST_EXPLODE('$INPUT_RAIL');

-- Filling the CROSSING column with a 'null' value when no value
UPDATE $RAIL SET CROSSING = 'null' WHERE CROSSING is null;

-- Clean not needed layersi
DROP TABLE IF EXISTS $INPUT_RAIL;


---------------------------------------------------------------------------------
-- 4. PROCESS HYDROGRAPHIC AREAS
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS $HYDRO;
CREATE TABLE $HYDRO (THE_GEOM geometry, ID_HYDRO serial, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
	AS SELECT ST_FORCE2D(ST_MAKEVALID(THE_GEOM)) as the_geom, CAST((row_number() over()) as Integer), ID_SOURCE, 'water', zindex FROM ST_EXPLODE('$INPUT_HYDRO');

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_HYDRO;



---------------------------------------------------------------------------------
-- 5. PROCESS VEGETATION AREAS
---------------------------------------------------------------------------------


DROP TABLE IF EXISTS $VEGET;
CREATE TABLE $VEGET (THE_GEOM geometry, ID_VEGET serial, ID_SOURCE varchar(24), TYPE varchar, HEIGHT_CLASS varchar, ZINDEX integer)
	AS SELECT ST_FORCE2D(ST_MAKEVALID(THE_GEOM)) as the_geom, CAST((row_number() over()) as Integer), ID_SOURCE, TYPE, null, ZINDEX FROM ST_EXPLODE('$INPUT_VEGET');

-- Update the vegetation height class (high or low)
UPDATE $VEGET SET HEIGHT_CLASS = (SELECT b.HEIGHT_CLASS FROM $VEGET_ABSTRACT_PARAMETERS b WHERE b.TERM=TYPE);

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_VEGET;


---------------------------------------------------------------------------------
-- 6. PROCESS IMPERVIOUS AREAS
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS $IMPERVIOUS;
CREATE TABLE $IMPERVIOUS (THE_GEOM geometry, ID_IMPERVIOUS serial, ID_SOURCE varchar(24))
	AS SELECT ST_FORCE2D(ST_MAKEVALID(THE_GEOM)) as the_geom, CAST((row_number() over()) as Integer), ID_SOURCE FROM ST_EXPLODE('$INPUT_IMPERVIOUS');

-- Clean not needed layers
DROP TABLE IF EXISTS $INPUT_IMPERVIOUS;