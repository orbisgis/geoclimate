-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--
-- 																								--
-- Title : Data Input Model pre-process 														--
-- Project : URCLIM / Paendora (ADEME - MODEVAL 2017-5)											--
-- Abstract : This script aims at pre-process data (coming from the french BD Topo) in order    --
--			  to feed (at the end of this script) the GeoClimate model.	                        --
--																								--
-- Author : Gwendall Petit (DECIDE Team, Lab-STICC CNRS UMR 6285)								--
-- Last update : 12/11/2020																	--
-- Licence : GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)                                  --
-- Comments :																					--
--   - Input layers : COMMUNE,BATI_INDIFFERENCIE, BATI_INDUSTRIEL, BATI_REMARQUABLE,            --
--					  ROUTE, TRONCON_VOIE_FERREE, SURFACE_EAU, ZONE_VEGETATION                  --
--					  TERRAIN_SPORT, CONSTRUCTION_SURFACIQUE, SURFACE_ROUTE, SURFACE_ACTIVITE   --
--   - Output layers, that will feed the GeoClimate model :                                     --
--					  ZONE, ZONE_BUFFER, ZONE_EXTENDED, INPUT_BUILDING, INPUT_ROAD              --
--					  INPUT_RAIL, INPUT_HYDRO, INPUT_VEGET, INPUT_IMPERVIOUS                    --
--	 - Variables, to be used in this script:                                                    --
--       - ID_ZONE : The zone unique ID (in France, a commune is defined by its INSEE CODE)    --
--       - DIST_BUFFER : The distance used to generate the buffer area around the studied      --
--         zone (expressed in meters - default value = 500)                                     --
--       - EXPAND : The distance used to select objects around the studied zone                --
--         (expressed in meters - default value = 1000)										    --
--                                                                                              --
-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--

--------------------------------------------------------------------------------------------------
-- 1- Create (spatial) indexes if not already exists on the input layers
--------------------------------------------------------------------------------------------------


CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDIFFERENCIE ON $BATI_INDIFFERENCIE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDUSTRIEL ON $BATI_INDUSTRIEL (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_REMARQUABLE ON $BATI_REMARQUABLE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ROUTE ON $ROUTE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_TRONCON_VOIE_FERREE ON $TRONCON_VOIE_FERREE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_EAU ON $SURFACE_EAU (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ZONE_VEGETATION ON $ZONE_VEGETATION (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_TERRAIN_SPORT ON $TERRAIN_SPORT (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_CONSTRUCTION_SURFACIQUE ON $CONSTRUCTION_SURFACIQUE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_ROUTE ON $SURFACE_ROUTE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_ACTIVITE ON $SURFACE_ACTIVITE (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_PISTE_AERODROME ON $PISTE_AERODROME (the_geom);
CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_$RESERVOIR ON $RESERVOIR (the_geom);

--------------------------------------------------------------------------------------------------
-- 2- Preparation of the study area (zone_xx)
--    In the Paendora (BD Topo) context, a zone is defined by a city ("commune" in french)
--------------------------------------------------------------------------------------------------

--Create the zone table
DROP TABLE IF EXISTS $ZONE;
CREATE TABLE $ZONE AS SELECT ST_FORCE2D(the_geom) as the_geom, CODE_INSEE AS ID_ZONE  FROM $COMMUNE;

CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ZONE ON $ZONE (the_geom);
CREATE INDEX IF NOT EXISTS idx_ID_ZONE ON $ZONE (ID_ZONE);



-- Generation of a buffer area around the studied commune
DROP TABLE IF EXISTS $ZONE_BUFFER;
CREATE TABLE $ZONE_BUFFER AS SELECT ST_BUFFER(the_geom, $DIST_BUFFER) as the_geom FROM $ZONE;
CREATE SPATIAL INDEX ON $ZONE_BUFFER (the_geom);

-- Generation of a rectangular area (bbox) around the studied commune
DROP TABLE IF EXISTS $ZONE_EXTENDED;
CREATE TABLE $ZONE_EXTENDED AS SELECT ST_EXPAND(the_geom, $EXPAND) as the_geom FROM $ZONE;
CREATE SPATIAL INDEX ON $ZONE_EXTENDED (the_geom);


--------------------------------------------------------------------------------------------------
-- 3- Call needed data from BD TOPO
--------------------------------------------------------------------------------------------------

-------------------------------------
-- Building (from the layers "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL" and "BATI_REMARQUABLE") that are in the study area (ZONE_BUFFER)
-------------------------------------
DROP TABLE IF EXISTS $BU_ZONE_INDIF, $BU_ZONE_INDUS, $BU_ZONE_REMARQ, $BU_ZONE_RESERVOIR;
CREATE TABLE $BU_ZONE_INDIF (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL integer)
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(ST_NORMALIZE(a.THE_GEOM)) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, a.HAUTEUR FROM $BATI_INDIFFERENCIE a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.HAUTEUR>=0;
CREATE TABLE $BU_ZONE_INDUS (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL integer, TYPE varchar)
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(ST_NORMALIZE(a.THE_GEOM)) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, a.HAUTEUR, a.NATURE FROM $BATI_INDUSTRIEL a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0;
CREATE TABLE $BU_ZONE_REMARQ (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL integer, TYPE varchar)
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(ST_NORMALIZE(a.THE_GEOM)) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, a.HAUTEUR, a.NATURE FROM $BATI_REMARQUABLE a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0;
CREATE TABLE $BU_ZONE_RESERVOIR
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(ST_NORMALIZE(a.THE_GEOM)) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END as the_geom, a.ID
    as id_source, a.HAUTEUR as HEIGHT_WALL, a.NATURE as type FROM $RESERVOIR a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.NATURE='Réservoir industriel' and a.HAUTEUR>0;


-- Merge the 3 tables into one, keeping informations about the initial table name
-- The fields 'HEIGHT_ROOF' and 'NB_LEV' fields are left empty. They will be updated later in the geoclimate procedure
-- Since there is no such information into the BD Topo, the field 'ZINDEX' is initialized to 0
DROP TABLE IF EXISTS $INPUT_BUILDING;
CREATE TABLE $INPUT_BUILDING (THE_GEOM geometry(polygon, $SRID), ID_SOURCE varchar(24), HEIGHT_WALL integer, HEIGHT_ROOF integer, NB_LEV integer, TYPE varchar, MAIN_USE varchar, ZINDEX integer)
   AS SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, null, null, '', '', 0 FROM ST_EXPLODE('$BU_ZONE_INDIF')
UNION ALL SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, null, null, TYPE, '', 0 FROM ST_EXPLODE('$BU_ZONE_INDUS')
UNION ALL SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, null, null, TYPE, '', 0 FROM ST_EXPLODE('$BU_ZONE_REMARQ')
UNION ALL SELECT THE_GEOM, ID_SOURCE, HEIGHT_WALL, HEIGHT_WALL, 0, 'heavy_industry', 'industrial', 0 FROM ST_EXPLODE('$BU_ZONE_RESERVOIR');

-- Update the BUILDING table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_BUILDING SET TYPE=(SELECT c.TERM FROM $BUILDING_BD_TOPO_USE_TYPE b, $BUILDING_ABSTRACT_USE_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_BUILDING}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $BUILDING_BD_TOPO_USE_TYPE b);

-------------------------------------
-- Road (from the layer "ROUTE") that are in the study area (ZONE_BUFFER)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_ROAD;
CREATE TABLE $INPUT_ROAD (THE_GEOM geometry, ID_SOURCE varchar(24), WIDTH DOUBLE PRECISION, TYPE varchar, SURFACE varchar, SIDEWALK varchar, ZINDEX integer, CROSSING varchar, SENS varchar)
AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, a.LARGEUR, a.NATURE, '', '', a.POS_SOL, a.FRANCHISST, a.SENS FROM $ROUTE a, $ZONE_BUFFER b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
CREATE SPATIAL INDEX ON $INPUT_ROAD (the_geom);

-- Update the ROAD table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_ROAD SET TYPE=(SELECT c.TERM FROM $ROAD_BD_TOPO_TYPE b, $ROAD_ABSTRACT_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_ROAD}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $ROAD_BD_TOPO_TYPE b);
-- Update the ROAD table with the new appropriate crossing key, coming from the abstract table
UPDATE $INPUT_ROAD SET CROSSING=(SELECT c.TERM FROM $ROAD_BD_TOPO_CROSSING b, $ROAD_ABSTRACT_CROSSING c WHERE c.ID_CROSSING=b.ID_CROSSING and ${INPUT_ROAD}.CROSSING=b.FRANCHISST) WHERE CROSSING IN (SELECT b.FRANCHISST FROM $ROAD_BD_TOPO_CROSSING b);

-------------------------------------
-- Rail (from the layer "TRONCON_VOIE_FERREE") that are in the study area (ZONE)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_RAIL;
CREATE TABLE $INPUT_RAIL (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer, CROSSING varchar)
AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, a.NATURE, a.POS_SOL, a.FRANCHISST FROM $TRONCON_VOIE_FERREE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;

-- Update the RAIL table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_RAIL SET TYPE=(SELECT c.TERM FROM $RAIL_BD_TOPO_TYPE b, $RAIL_ABSTRACT_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_RAIL}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $RAIL_BD_TOPO_TYPE b);
-- Update the ROAD table with the new appropriate crossing key, coming from the abstract table
UPDATE $INPUT_RAIL SET CROSSING=(SELECT c.TERM FROM $RAIL_BD_TOPO_CROSSING b, $RAIL_ABSTRACT_CROSSING c WHERE c.ID_CROSSING=b.ID_CROSSING and ${INPUT_RAIL}.CROSSING=b.FRANCHISST) WHERE CROSSING IN (SELECT b.FRANCHISST FROM $RAIL_BD_TOPO_CROSSING b);

-------------------------------------
-- Hydrography (from the layer "SURFACE_EAU") that are in the study area (ZONE_EXTENDED)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_HYDRO;
CREATE TABLE $INPUT_HYDRO (THE_GEOM geometry, ID_SOURCE varchar(24), ZINDEX integer)
AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, 0  FROM $SURFACE_EAU a, $ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);

-------------------------------------
-- Vegetation (from the layer "ZONE_VEGETATION") that are in the study area (ZONE_EXTENDED)
-------------------------------------
DROP TABLE IF EXISTS $INPUT_VEGET;
CREATE TABLE $INPUT_VEGET (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID, a.NATURE, 0 FROM $ZONE_VEGETATION a, $ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);

-- Update the VEGET table with the new appropriate type key, coming from the abstract table
UPDATE $INPUT_VEGET SET TYPE=(SELECT c.TERM FROM $VEGET_BD_TOPO_TYPE b, $VEGET_ABSTRACT_TYPE c WHERE c.ID_TYPE=b.ID_TYPE and ${INPUT_VEGET}.TYPE=b.NATURE) WHERE TYPE IN (SELECT b.NATURE FROM $VEGET_BD_TOPO_TYPE b);

-------------------------------------
-- Impervious areas (from the layers "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE", "SURFACE_ROUTE" and "SURFACE_ACTIVITE") that are in the study area (ZONE)
-------------------------------------
DROP TABLE IF EXISTS $TMP_IMPERV_TERRAIN_SPORT, $TMP_IMPERV_CONSTRUCTION_SURFACIQUE, $TMP_IMPERV_SURFACE_ROUTE, $TMP_IMPERV_SURFACE_ACTIVITE;
CREATE TABLE $TMP_IMPERV_TERRAIN_SPORT (THE_GEOM geometry, ID_SOURCE varchar(24))
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID FROM $TERRAIN_SPORT a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE='Piste de sport';
CREATE TABLE $TMP_IMPERV_CONSTRUCTION_SURFACIQUE (THE_GEOM geometry, ID_SOURCE varchar(24))
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID FROM $CONSTRUCTION_SURFACIQUE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND (a.NATURE='Barrage' OR a.NATURE='Ecluse' OR a.NATURE='Escalier');
CREATE TABLE $TMP_IMPERV_SURFACE_ROUTE (THE_GEOM geometry, ID_SOURCE varchar(24))
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID FROM $SURFACE_ROUTE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
CREATE TABLE $TMP_IMPERV_SURFACE_ACTIVITE (THE_GEOM geometry, ID_SOURCE varchar(24))
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) END, a.ID FROM $SURFACE_ACTIVITE a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) AND (a.CATEGORIE='Administratif' OR a.CATEGORIE='Enseignement' OR a.CATEGORIE='Santé');

--ADD PISTE_AERODROME TABLE TO IMPERVIOUS DATA
DROP TABLE IF EXISTS $TMP_IMPERV_PISTE_AERODROME;
CREATE TABLE $TMP_IMPERV_PISTE_AERODROME (THE_GEOM geometry, ID_SOURCE varchar(24))
    AS SELECT CASE WHEN ST_ISVALID(a.THE_GEOM) THEN ST_FORCE2D(a.THE_GEOM) ELSE ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM))
    END, a.ID FROM $PISTE_AERODROME a, $ZONE b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);


DROP TABLE IF EXISTS $INPUT_IMPERVIOUS;
CREATE TABLE $INPUT_IMPERVIOUS (THE_GEOM geometry, ID_SOURCE varchar(24))
	AS SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_TERRAIN_SPORT')
 UNION ALL SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_CONSTRUCTION_SURFACIQUE')
 UNION ALL SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_SURFACE_ROUTE')
 UNION ALL SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_SURFACE_ACTIVITE')
  UNION ALL SELECT THE_GEOM, ID_SOURCE FROM ST_EXPLODE('$TMP_IMPERV_PISTE_AERODROME');


DROP TABLE IF EXISTS $TMP_IMPERV_TERRAIN_SPORT, $TMP_IMPERV_CONSTRUCTION_SURFACIQUE, $TMP_IMPERV_SURFACE_ROUTE, $TMP_IMPERV_SURFACE_ACTIVITE,$TMP_IMPERV_PISTE_AERODROME,
$ZONE_BUFFER, $ZONE_EXTENDED, $BU_ZONE_INDIF, $BU_ZONE_INDUS, $BU_ZONE_REMARQ, $BU_ZONE_RESERVOIR;
