-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--
-- 																								--
-- Title: BD Topo data description													            --
-- Project: Urclim / Paendora (ADEME - MODEVAL 2017-5)											--
-- Abstract: This script aims to create the tables that describe the data types (from BD Topo)  --
--     and to define the corresponding values in the respective tables from the abstract model	--
--																								--
-- Author: Gwendall Petit (DECIDE Team, Lab-STICC CNRS UMR 6285)								--
-- Last update: 21/11/2019																		--
-- Licence : GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)                                  --
--																								--
-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--

----------------------------------------------------------------------------------------------------------------------
-- FOR BUILDING
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Building type, from BD Topo
--------------------------------------------------------

-- Initialize the table with all available types (NATURE in the BD Topo v2.0)
DROP TABLE IF EXISTS $BUILDING_BD_TOPO_USE_TYPE;
CREATE TABLE $BUILDING_BD_TOPO_USE_TYPE (ID_NATURE serial, NATURE varchar, TABLE_NAME varchar, ID_TYPE integer);

-- Feed the table

-- For "BATI_INDIFFERENCIE"
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, '', 'BATI_INDIFFERENCIE', null);
-- For "BATI_INDUSTRIEL"
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Bâtiment agricole', 'BATI_INDUSTRIEL', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Bâtiment commercial', 'BATI_INDUSTRIEL', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Bâtiment industriel', 'BATI_INDUSTRIEL', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Serre', 'BATI_INDUSTRIEL', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Silo', 'BATI_INDUSTRIEL', null);
-- For "BATI_REMARQUABLE"
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Aérogare', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Arc de triomphe', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Arène ou théâtre antique', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Bâtiment religieux divers', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Bâtiment sportif', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Chapelle', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Château', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Eglise', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Fort, blockhaus, casemate', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Gare', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Mairie', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Monument', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Péage', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Préfecture', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Sous-préfecture', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Tour, donjon, moulin', 'BATI_REMARQUABLE', null);
INSERT INTO $BUILDING_BD_TOPO_USE_TYPE VALUES(null, 'Tribune', 'BATI_REMARQUABLE', null);

-- Define the correspondences between the BD Topo and the abstract table "$BUILDING_ABSTRACT_USE_TYPE"

-- For "BATI_INDIFFERENCIE"
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='building') WHERE NATURE ='';
-- For "BATI_INDUSTRIEL"
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='farm_auxiliary') WHERE NATURE ='Bâtiment agricole';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='commercial') WHERE NATURE ='Bâtiment commercial';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='light_industry') WHERE NATURE ='Bâtiment industriel';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='greenhouse') WHERE NATURE ='Serre';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='silo') WHERE NATURE ='Silo';
-- For "BATI_REMARQUABLE"
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='terminal') WHERE NATURE ='Aérogare';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='monument') WHERE NATURE ='Arc de triomphe';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='monument') WHERE NATURE ='Arène ou théâtre antique';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='religious') WHERE NATURE ='Bâtiment religieux divers';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='sports_centre') WHERE NATURE ='Bâtiment sportif';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='chapel') WHERE NATURE ='Chapelle';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='castle') WHERE NATURE ='Château';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='church') WHERE NATURE ='Eglise';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='military') WHERE NATURE ='Fort, blockhaus, casemate';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='train_station') WHERE NATURE ='Gare';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='townhall') WHERE NATURE ='Mairie';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='monument') WHERE NATURE ='Monument';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='toll_booth') WHERE NATURE ='Péage';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='government') WHERE NATURE ='Préfecture';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='government') WHERE NATURE ='Sous-préfecture';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='historic') WHERE NATURE ='Tour, donjon, moulin';
UPDATE $BUILDING_BD_TOPO_USE_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $BUILDING_ABSTRACT_USE_TYPE b WHERE b.TERM='grandstand') WHERE NATURE ='Tribune';


----------------------------------------------------------------------------------------------------------------------
-- FOR ROAD
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Road type, from BD Topo
--------------------------------------------------------

-- Initialize the table with all available types (NATURE in the BD Topo v2.0)
DROP TABLE IF EXISTS $ROAD_BD_TOPO_TYPE;
CREATE TABLE $ROAD_BD_TOPO_TYPE (ID_NATURE serial, NATURE varchar, TABLE_NAME varchar, ID_TYPE integer);

-- Feed the table

INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Autoroute', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Quasi-autoroute', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Bretelle', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Route à 2 chaussées', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Route à 1 chaussée', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Route empierrée', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Chemin', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Bac auto', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Bac piéton', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Piste cyclable', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Sentier', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_TYPE VALUES(null, 'Escalier', 'ROUTE', null);

-- Define the correspondences between the BD Topo and the abstract table "$ROAD_ABSTRACT_TYPE"

UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='motorway') WHERE NATURE ='Autoroute';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='trunk') WHERE NATURE ='Quasi-autoroute';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='highway_link') WHERE NATURE ='Bretelle';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='primary') WHERE NATURE ='Route à 2 chaussées';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='unclassified') WHERE NATURE ='Route à 1 chaussée';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='track') WHERE NATURE ='Route empierrée';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='track') WHERE NATURE ='Chemin';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='ferry') WHERE NATURE ='Bac auto';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='ferry') WHERE NATURE ='Bac piéton';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='cycleway') WHERE NATURE ='Piste cyclable';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='path') WHERE NATURE ='Sentier';
UPDATE $ROAD_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $ROAD_ABSTRACT_TYPE b WHERE b.TERM='steps') WHERE NATURE ='Escalier';


--------------------------------------------------------
-- Road crossing, from BD Topo
--------------------------------------------------------

-- Initialize the table with all available CROSSING (FRANCHISST in the BD Topo v2.0)
DROP TABLE IF EXISTS $ROAD_BD_TOPO_CROSSING;
CREATE TABLE $ROAD_BD_TOPO_CROSSING (ID_FRANCHISST serial, FRANCHISST varchar, TABLE_NAME varchar, ID_CROSSING integer);

-- Feed the table

INSERT INTO $ROAD_BD_TOPO_CROSSING VALUES(null, 'Gué ou radier', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_CROSSING VALUES(null, 'Pont', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_CROSSING VALUES(null, 'Tunnel', 'ROUTE', null);
INSERT INTO $ROAD_BD_TOPO_CROSSING VALUES(null, 'NC', 'ROUTE', null);

-- Define the correspondences between the BD Topo and the abstract table "$ROAD_ABSTRACT_CROSSING"

UPDATE $ROAD_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $ROAD_ABSTRACT_CROSSING b WHERE b.TERM='null') WHERE FRANCHISST ='Gué ou radier';
UPDATE $ROAD_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $ROAD_ABSTRACT_CROSSING b WHERE b.TERM='bridge') WHERE FRANCHISST ='Pont';
UPDATE $ROAD_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $ROAD_ABSTRACT_CROSSING b WHERE b.TERM='tunnel') WHERE FRANCHISST ='Tunnel';
UPDATE $ROAD_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $ROAD_ABSTRACT_CROSSING b WHERE b.TERM='null') WHERE FRANCHISST ='NC';


----------------------------------------------------------------------------------------------------------------------
-- FOR RAIL
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Rail type, from BD Topo
--------------------------------------------------------

-- Initialize the table with all available types (NATURE in the BD Topo v2.0)
DROP TABLE IF EXISTS $RAIL_BD_TOPO_TYPE;
CREATE TABLE $RAIL_BD_TOPO_TYPE (ID_NATURE serial, NATURE varchar, TABLE_NAME varchar, ID_TYPE integer);

-- Feed the table

INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'LGV', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Principale', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Voie de service', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Voie non exploitée', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Transport urbain', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Funiculaire ou crémaillère', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Metro', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_TYPE VALUES(null, 'Tramway', 'TRONCON_VOIE_FERREE', null);

-- Define the correspondences between the BD Topo and the abstract table "$RAIL_ABSTRACT_TYPE"

UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='highspeed') WHERE NATURE ='LGV';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='rail') WHERE NATURE ='Principale';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='service_track') WHERE NATURE ='Voie de service';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='disused') WHERE NATURE ='Voie non exploitée';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='tram') WHERE NATURE ='Transport urbain';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='funicular') WHERE NATURE ='Funiculaire ou crémaillère';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='subway') WHERE NATURE ='Metro';
UPDATE $RAIL_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $RAIL_ABSTRACT_TYPE b WHERE b.TERM='tram') WHERE NATURE ='Tramway';


--------------------------------------------------------
-- Rail crossing, from BD Topo
--------------------------------------------------------

-- Initialize the table with all available CROSSING (FRANCHISST in the BD Topo v2.0)
DROP TABLE IF EXISTS $RAIL_BD_TOPO_CROSSING;
CREATE TABLE $RAIL_BD_TOPO_CROSSING (ID_FRANCHISST serial, FRANCHISST varchar, TABLE_NAME varchar, ID_CROSSING integer);

-- Feed the table

INSERT INTO $RAIL_BD_TOPO_CROSSING VALUES(null, 'Pont', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_CROSSING VALUES(null, 'Tunnel', 'TRONCON_VOIE_FERREE', null);
INSERT INTO $RAIL_BD_TOPO_CROSSING VALUES(null, 'NC', 'TRONCON_VOIE_FERREE', null);

-- Define the correspondences between the BD Topo and the abstract table "$RAIL_ABSTRACT_CROSSING"

UPDATE $RAIL_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $RAIL_ABSTRACT_CROSSING b WHERE b.TERM='bridge') WHERE FRANCHISST ='Pont';
UPDATE $RAIL_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $RAIL_ABSTRACT_CROSSING b WHERE b.TERM='tunnel') WHERE FRANCHISST ='Tunnel';
UPDATE $RAIL_BD_TOPO_CROSSING SET ID_CROSSING=(SELECT b.ID_CROSSING FROM $RAIL_ABSTRACT_CROSSING b WHERE b.TERM='null') WHERE FRANCHISST ='NC';


----------------------------------------------------------------------------------------------------------------------
-- FOR VEGETATION
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Vegetation type, from BD Topo
--------------------------------------------------------

-- Initialize the table with all available types (NATURE in the BD Topo v2.0)
DROP TABLE IF EXISTS $VEGET_BD_TOPO_TYPE;
CREATE TABLE $VEGET_BD_TOPO_TYPE (ID_NATURE serial, NATURE varchar, TABLE_NAME varchar, ID_TYPE integer);

-- Feed the table

INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Zone arborée', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Forêt fermée de feuillus', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Forêt fermée mixte', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Forêt fermée de conifères', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Forêt ouverte', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Peupleraie', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Haie', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Lande ligneuse', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Verger', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Vigne', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Bois', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Bananeraie', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Mangrove', 'ZONE_VEGETATION', null);
INSERT INTO $VEGET_BD_TOPO_TYPE VALUES(null, 'Canne à sucre', 'ZONE_VEGETATION', null);


-- Define the correspondences between the BD Topo and the abstract table "$VEGET_ABSTRACT_TYPE"

UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='wood') WHERE NATURE ='Zone arborée';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='forest') WHERE NATURE ='Forêt fermée de feuillus';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='forest') WHERE NATURE ='Forêt fermée mixte';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='forest') WHERE NATURE ='Forêt fermée de conifères';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='forest') WHERE NATURE ='Forêt ouverte';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='forest') WHERE NATURE ='Peupleraie';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='hedge') WHERE NATURE ='Haie';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='heath') WHERE NATURE ='Lande ligneuse';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='orchard') WHERE NATURE ='Verger';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='vineyard') WHERE NATURE ='Vigne';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='forest') WHERE NATURE ='Bois';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='banana_plants') WHERE NATURE ='Bananeraie';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='mangrove') WHERE NATURE ='Mangrove';
UPDATE $VEGET_BD_TOPO_TYPE SET ID_TYPE=(SELECT b.ID_TYPE FROM $VEGET_ABSTRACT_TYPE b WHERE b.TERM='sugar_cane') WHERE NATURE ='Canne à sucre';