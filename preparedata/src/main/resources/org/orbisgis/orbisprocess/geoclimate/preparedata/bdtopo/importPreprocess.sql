-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--
-- 																								--
-- Title : Data Input Model pre-process 														--
-- Project : URCLIM / Paendora (ADEME - MODEVAL 2017-5)											--
-- Abstract : This script aims at pre-process data (coming from the french BD Topo) in order    --
--			  to feed (at the end of this script) the GeoClimate model.	                        --
--																								--
-- Author : Gwendall Petit (DECIDE Team, Lab-STICC CNRS UMR 6285)								--
-- Last update : 15/03/2019																		--
-- Licence : GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)                                  --
-- Comments :																					--
--   - Input layers : IRIS_GE,BATI_INDIFFERENCIE, BATI_INDUSTRIEL, BATI_REMARQUABLE,            --
--					  ROUTE, TRONCON_VOIE_FERREE, SURFACE_EAU and ZONE_VEGETATION               --
--   - Output layers, that will feed the GeoClimate model :                                     --
--					  ZONE, ZONE_BUFFER, ZONE_EXTENDED, ZONE_NEIGHBORS,                         --
--					  INPUT_BUILDING, INPUT_ROAD, INPUT_RAIL, INPUT_HYDRO, INPUT_VEGET          --
--																								--
-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--


--------------------------------------------------------------------------------------------------
-- 1- Declaration of variables, to be used in this script
--------------------------------------------------------------------------------------------------

-- The unique ID of the zone (in France, a commune defined by its INSEE CODE)
--SET @ID_ZONE=56260;
-- The distance (exprimed in meters) used to generate the buffer area around the studied zone
--SET @DIST_BUFFER=500;
-- The distance (exprimed in meters) used to select objects around the studied zone
--SET @EXPAND=1000;


--------------------------------------------------------------------------------------------------
-- 2- Create (spatial) indexes if not already exists on the input layers
--------------------------------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_geom_IRIS_GE ON $IRIS_GE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_insee_IRIS_GE ON $IRIS_GE(INSEE_COM);
CREATE INDEX IF NOT EXISTS idx_geom_BATI_INDIFFERENCIE ON $BATI_INDIFFERENCIE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_BATI_INDUSTRIEL ON $BATI_INDUSTRIEL(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_BATI_REMARQUABLE ON $BATI_REMARQUABLE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_ROUTE ON $ROUTE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_TRONCON_VOIE_FERREE ON $TRONCON_VOIE_FERREE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_SURFACE_EAU ON $SURFACE_EAU(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_ZONE_VEGETATION ON $ZONE_VEGETATION(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_TERRAIN_SPORT ON $TERRAIN_SPORT(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_CONSTRUCTION_SURFACIQUE ON $CONSTRUCTION_SURFACIQUE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_SURFACE_ROUTE ON $SURFACE_ROUTE(the_geom) USING RTREE;
CREATE INDEX IF NOT EXISTS idx_geom_SURFACE_ACTIVITE ON $SURFACE_ACTIVITE(the_geom) USING RTREE;


--------------------------------------------------------------------------------------------------
-- 3- Preparation of the study area (zone_xx)
--    In the Paendora (BD Topo) context, a zone is defined by a city ("commune" in french)
--------------------------------------------------------------------------------------------------

-- Extraction of IRIS on the study commune
DROP TABLE IF EXISTS $TMP_IRIS;
CREATE TABLE $TMP_IRIS AS SELECT * FROM $IRIS_GE WHERE INSEE_COM=$ID_ZONE;
CREATE INDEX ON $TMP_IRIS(the_geom) USING RTREE;

-- Generation of the geometry of the commune, on the basis of the IRIS
DROP TABLE IF EXISTS $ZONE;
CREATE TABLE $ZONE AS SELECT INSEE_COM as ID_ZONE, ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM $TMP_IRIS GROUP BY INSEE_COM;
CREATE INDEX ON $ZONE (the_geom) USING RTREE;

-- Generation of a buffer area around the studied commune
DROP TABLE IF EXISTS $ZONE_BUFFER;
CREATE TABLE $ZONE_BUFFER AS SELECT ST_BUFFER(the_geom, $DIST_BUFFER) as the_geom FROM $ZONE;
CREATE INDEX ON $ZONE_BUFFER(the_geom) USING RTREE;

-- Generation of a rectangular area (bbox) around the studied commune
DROP TABLE IF EXISTS $ZONE_EXTENDED;
CREATE TABLE $ZONE_EXTENDED AS SELECT ST_EXPAND(the_geom, $EXPAND) as the_geom FROM $ZONE;
CREATE INDEX ON $ZONE_EXTENDED(the_geom) USING RTREE;

-- Generation of the geometries of the neighbouring communes to the one studied
DROP TABLE IF EXISTS $ZONE_NEIGHBORS;
CREATE TABLE $ZONE_NEIGHBORS AS SELECT ST_UNION(ST_ACCUM(a.the_geom)) as the_geom, INSEE_COM as ID_ZONE FROM $IRIS_GE a, $ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) GROUP BY a.INSEE_COM;
CREATE INDEX ON $ZONE_NEIGHBORS(the_geom) USING RTREE;


--------------------------------------------------------------------------------------------------
-- 4- Call needed data from BD TOPO
--------------------------------------------------------------------------------------------------

-------------------------------------
-- Building (from the layers "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL" and "BATI_REMARQUABLE") that are in the study area (ZONE_BUFFER)
-------------------------------------
DROP TABLE IF EXISTS $BU_ZONE_INDIF, $BU_ZONE_INDUS, $BU_ZONE_REMARQ;
CREATE TABLE $BU_ZONE_INDIF AS SELECT a.the_geom, a.ID as ID_SOURCE, a.HAUTEUR as HEIGHT_WALL FROM $BATI_INDIFFERENCIE a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.HAUTEUR>=0;
CREATE TABLE $BU_ZONE_INDUS AS SELECT a.the_geom, a.ID as ID_SOURCE, a.HAUTEUR as HEIGHT_WALL, a.NATURE as TYPE FROM $BATI_INDUSTRIEL a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0;
CREATE TABLE $BU_ZONE_REMARQ AS SELECT a.the_geom, a.ID as ID_SOURCE, a.HAUTEUR as HEIGHT_WALL, a.NATURE as TYPE FROM $BATI_REMARQUABLE a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0;

-- Merge the 3 tables into one, keeping informations about the initial table name
-- The fields 'HEIGHT_ROOF' and 'NB_LEV' fields are left empty. They will be updated later in the geoclimate procedure
-- Since there is no such information into the BD Topo, the field 'ZINDEX' is initialized to 0
DROP TABLE IF EXISTS $INPUT_BUILDING;
CREATE TABLE $INPUT_BUILDING (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer)
   AS SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, null, null, '', '', 0 FROM ST_EXPLODE('$BU_ZONE_INDIF')
UNION SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, null, null, TYPE, '', 0 FROM ST_EXPLODE('$BU_ZONE_INDUS')
UNION SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, null, null, TYPE, '', 0 FROM ST_EXPLODE('$BU_ZONE_REMARQ');

-- Update the BUILDING table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_BUILDING SET TYPE=(SELECT c.TERM FROM $BUILDING_BD_TOPO_USE_TYPE b, $BUILDING_ABSTRACT_USE_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_BUILDING}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $BUILDING_BD_TOPO_USE_TYPE b);

-------------------------------------
-- Road (from the layer "ROUTE") that are in the study area (ZONE_BUFFER)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_ROAD;
CREATE TABLE $INPUT_ROAD (THE_GEOM geometry, ID_SOURCE varchar(24), WIDTH double precision, TYPE varchar, SURFACE varchar, SIDEWALK varchar, ZINDEX integer, CROSSING varchar)
AS SELECT a.THE_GEOM, a.ID, a.LARGEUR, a.NATURE, '', '', a.POS_SOL, a.FRANCHISST FROM $ROUTE a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
CREATE INDEX ON $INPUT_ROAD(the_geom) USING RTREE;

-- Update the ROAD table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_ROAD SET TYPE=(SELECT c.TERM FROM $ROAD_BD_TOPO_TYPE b, $ROAD_ABSTRACT_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_ROAD}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $ROAD_BD_TOPO_TYPE b);
-- Update the ROAD table with the new appropriate crossing key, coming from the abstract table
UPDATE $INPUT_ROAD SET CROSSING=(SELECT c.TERM FROM $ROAD_BD_TOPO_CROSSING b, $ROAD_ABSTRACT_CROSSING c WHERE c.ID_CROSSING=b.ID_CROSSING and ${INPUT_ROAD}.CROSSING=b.FRANCHISSMT) WHERE CROSSING IN (SELECT b.FRANCHISSMT FROM $ROAD_BD_TOPO_CROSSING b);

-------------------------------------
-- Rail (from the layer "TRONCON_VOIE_FERREE") that are in the study area (ZONE)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_RAIL;
CREATE TABLE $INPUT_RAIL (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer, CROSSING varchar)
AS SELECT a.THE_GEOM, a.ID, a.NATURE, a.POS_SOL, a.FRANCHISST FROM $TRONCON_VOIE_FERREE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;

-- Update the RAIL table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_RAIL SET TYPE=(SELECT c.TERM FROM $RAIL_BD_TOPO_TYPE b, $RAIL_ABSTRACT_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_RAIL}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $RAIL_BD_TOPO_TYPE b);
-- Update the ROAD table with the new appropriate crossing key, coming from the abstract table
UPDATE $INPUT_RAIL SET CROSSING=(SELECT c.TERM FROM $RAIL_BD_TOPO_CROSSING b, $RAIL_ABSTRACT_CROSSING c WHERE c.ID_CROSSING=b.ID_CROSSING and ${INPUT_RAIL}.CROSSING=b.FRANCHISST) WHERE CROSSING IN (SELECT b.FRANCHISST FROM $RAIL_BD_TOPO_CROSSING b);

-------------------------------------
-- Hydrography (from the layer "SURFACE_EAU") that are in the study area (ZONE_EXTENDED)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_HYDRO;
CREATE TABLE $INPUT_HYDRO AS SELECT a.the_geom, a.ID as ID_SOURCE FROM $SURFACE_EAU a, $ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);

-------------------------------------
-- Vegetation (from the layer "ZONE_VEGETATION") that are in the study area (ZONE_EXTENDED)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_VEGET;
CREATE TABLE $INPUT_VEGET AS SELECT a.the_geom, a.ID as ID_SOURCE, a.NATURE as TYPE FROM $ZONE_VEGETATION a, $ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);

-- Update the VEGET table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_VEGET SET TYPE=(SELECT c.TERM FROM $VEGET_BD_TOPO_TYPE b, $VEGET_ABSTRACT_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_VEGET}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $VEGET_BD_TOPO_TYPE b);

-------------------------------------
-- Impervious areas (from the layers "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE", "SURFACE_ROUTE" and "SURFACE_ACTIVITE") that are in the study area (ZONE)
-------------------------------------
DROP TABLE IF EXISTS $TMP_IMPERV_TERRAIN_SPORT, $TMP_IMPERV_CONSTRUCTION_SURFACIQUE, $TMP_IMPERV_SURFACE_ROUTE, $TMP_IMPERV_SURFACE_ACTIVITE;
CREATE TABLE $TMP_IMPERV_TERRAIN_SPORT AS SELECT a.the_geom, a.ID as ID_SOURCE FROM $TERRAIN_SPORT a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE='Piste de sport';
CREATE TABLE $TMP_IMPERV_CONSTRUCTION_SURFACIQUE AS SELECT a.the_geom, a.ID as ID_SOURCE FROM $CONSTRUCTION_SURFACIQUE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND (a.NATURE='Barrage' OR a.NATURE='Ecluse' OR a.NATURE='Escalier');
CREATE TABLE $TMP_IMPERV_SURFACE_ROUTE AS SELECT a.the_geom, a.ID as ID_SOURCE FROM $SURFACE_ROUTE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
CREATE TABLE $TMP_IMPERV_SURFACE_ACTIVITE AS SELECT a.the_geom, a.ID as ID_SOURCE FROM $SURFACE_ACTIVITE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND (a.CATEGORIE='Administratif' OR a.CATEGORIE='Enseignement' OR a.CATEGORIE='Santé');

DROP TABLE IF EXISTS $INPUT_IMPERVIOUS;
CREATE TABLE $INPUT_IMPERVIOUS (THE_GEOM geometry, ID_SOURCE varchar(24))
	AS SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_TERRAIN_SPORT')
 UNION SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_CONSTRUCTION_SURFACIQUE')
 UNION SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_SURFACE_ROUTE')
 UNION SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_SURFACE_ACTIVITE');

DROP TABLE IF EXISTS $TMP_IMPERV_TERRAIN_SPORT, $TMP_IMPERV_CONSTRUCTION_SURFACIQUE, $TMP_IMPERV_SURFACE_ROUTE, $TMP_IMPERV_SURFACE_ACTIVITE;


--------------------------------------------------------------------------------------------------
-- Clear not needed tables
--------------------------------------------------------------------------------------------------
DROP TABLE $TMP_IRIS, $ZONE_BUFFER, $ZONE_EXTENDED, $BU_ZONE_INDIF, $BU_ZONE_INDUS, $BU_ZONE_REMARQ;