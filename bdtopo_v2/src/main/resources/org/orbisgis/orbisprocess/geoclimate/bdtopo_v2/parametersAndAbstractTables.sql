-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--
-- 																								--
-- Title: Data description abstract model													    --
-- Projects: URCLIM / Paendora (ADEME - MODEVAL 2017-5)		    								--
-- Abstract: This script aims to create an abstract model to describe the data through          --
--           their types, surface and other specific parameters.                         	    --
--																								--
-- Author: Gwendall Petit (DECIDE Team, Lab-STICC CNRS UMR 6285)								--
-- Last update: 21/11/2019																		--
-- Licence : GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)                                  --
--																								--
-- Comments: - the types concerns the buildings, the roads, the rails and the vegetation areas  --
--           - the surface concerns only the roads                                              --
--           - the crossing concerns the roads and the rails                                    --
--																								--
-- -*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/-*/--

----------------------------------------------------------------------------------------------------------------------
-- FOR BUILDING
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the building type
--------------------------------------------------------

DROP TABLE IF EXISTS $BUILDING_ABSTRACT_USE_TYPE;
CREATE TABLE $BUILDING_ABSTRACT_USE_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'building', 'Given object as a building', 'https://wiki.openstreetmap.org/wiki/Key:building');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'house', 'Single dwelling unit usually inhabited by one family', 'https://wiki.openstreetmap.org/wiki/Tag:building=house');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'detached', 'Free-standing residential building usually housing a single-family', 'https://wiki.openstreetmap.org/wiki/Tag:building=detached');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'residential', 'Building used primarily for residential purposes', 'https://wiki.openstreetmap.org/wiki/Tag:building=residential');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'apartments', 'Building arranged into individual dwellings, often on separate floors. May also have retail outlets on the ground floor', 'https://wiki.openstreetmap.org/wiki/Tag:building=apartments');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'bungalow', 'Small, single-storey detached house in the form of a bungalow', 'https://wiki.openstreetmap.org/wiki/Tag:building=bungalow');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'historic', 'Any places or buildings, with a historical dimension', 'https://wiki.openstreetmap.org/wiki/Key:historic');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'monument', 'Memorial object, which is especially large, built to remember, show respect to a person or group of people or to commemorate an event', 'https://wiki.openstreetmap.org/wiki/Tag:historic=monument');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'ruins', 'House that is an abandoned (but still a building)', 'https://wiki.openstreetmap.org/wiki/Tag:building=ruins');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'castle', 'Various kinds of structures, most of which were originally built as fortified residences of a lord or noble', 'https://wiki.openstreetmap.org/wiki/Tag:historic=castle');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'agricultural', 'Building, machinery, facilities, related to agricultural production', 'https://www.eionet.europa.eu/gemet/en/concept/8014');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'farm', 'A farmhouse is the main building of a farm', 'https://wiki.openstreetmap.org/wiki/Tag:building=farm');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'farm_auxiliary', 'Building on a farm that is not a dwelling', 'https://wiki.openstreetmap.org/wiki/Tag:building=farm_auxiliary');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'barn', 'Agricultural building used for storage and as a covered workplace', 'https://wiki.openstreetmap.org/wiki/Tag:building=barn');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'greenhouse', 'A Greenhouse (also called a glasshouse) is a building in which plants are grown. It typically has a roof and walls made of clear glass or plastic to allow sunlight to enter', 'https://wiki.openstreetmap.org/wiki/Tag:building=greenhouse');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'silo', 'Storage container for bulk material, often grains such as corn or wheat', 'https://wiki.openstreetmap.org/wiki/Tag:man_made=silo');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'commercial', 'Building where non-specific commercial activities take place', 'https://wiki.openstreetmap.org/wiki/Tag:building=commercial');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'industrial', 'Building where some industrial process takes place', 'https://wiki.openstreetmap.org/wiki/Tag:building=industrial');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'sport', 'Buildings, constructions, installations, organized areas and equipment for indoor and outdoor sport activities', 'https://wiki.openstreetmap.org/wiki/Tag:building=sport');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'sports_centre', 'Building that is designed for sports, e.g. for school sports, university or club sports', 'https://wiki.openstreetmap.org/wiki/Tag:building=sports_centre');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'grandstand', 'Building for the main stand, usually roofed, commanding the best view for spectators at racecourses or sports grounds', 'https://wiki.openstreetmap.org/wiki/Tag:building=grandstand');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'transportation', 'Building, construction, installation, organized areas and equipment for transportation', 'https://wiki.openstreetmap.org/wiki/Tag:building=transportation');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'train_station', 'Train station building', 'https://wiki.openstreetmap.org/wiki/Tag:building=train_station');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'toll_booth', 'Toll roads charge money for some or all traffic', 'https://wiki.openstreetmap.org/wiki/Tag:barrier=toll_booth');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'terminal', 'Airport passenger building', 'https://wiki.openstreetmap.org/wiki/Tag:aeroway=terminal');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'healthcare', 'All places that provide healthcare', 'https://wiki.openstreetmap.org/wiki/Key:healthcare');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'education', 'All places that provide education', 'https://wiki.openstreetmap.org/wiki/Map_Features#Education');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'entertainment_arts_culture', 'All places that provide entertainment, arts and culture', 'https://wiki.openstreetmap.org/wiki/Key:amenity#Entertainment.2C_Arts_.26_Culture');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'sustenance', 'Buildings, constructions, installations, organized areas and equipment  of any food commodity or related food products. ex : bar, pub...', 'https://wiki.openstreetmap.org/wiki/Map_Features#Sustenance');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'military', 'Buildings, constructions, installations necessary to the performance of military activities, either combat or noncombat', 'https://wiki.openstreetmap.org/wiki/Key:military"');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'religious', 'Unspecific religious building', 'https://wiki.openstreetmap.org/wiki/Key:building#Religious');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'chapel', 'Religious building, often pretty small. One can enter in it to pray or meditate', 'https://wiki.openstreetmap.org/wiki/Tag:building=chapel');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'church', 'Building that was built as a church', 'https://wiki.openstreetmap.org/wiki/Tag:building=church');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'government', 'Building built to house government offices', 'https://wiki.openstreetmap.org/wiki/Tag:building=government');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'townhall', 'Building that may serve as an administrative center, or may be merely a community meeting place', 'https://wiki.openstreetmap.org/wiki/Tag:amenity=townhall');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(null, 'office', 'Office block typically houses companies, but offices may be also rented by any other kind of organization like charities, government, any NGO etc.', 'https://wiki.openstreetmap.org/wiki/Tag:building=office');


--------------------------------------------------------
-- Abstract Building parameters
--------------------------------------------------------

DROP TABLE IF EXISTS $BUILDING_ABSTRACT_PARAMETERS;
CREATE TABLE $BUILDING_ABSTRACT_PARAMETERS (ID_TYPE serial, TERM varchar, NB_LEV integer);

-- Add comments
COMMENT ON COLUMN $BUILDING_ABSTRACT_PARAMETERS."TERM" IS 'Building type';
COMMENT ON COLUMN $BUILDING_ABSTRACT_PARAMETERS."NB_LEV" IS 'Specifies whether or not the building type is taken into account when calculating the number of levels (0 = not taken into account (in this case, the number of levels will be forced to 0) / 1= taken into account (in this case, a formula will be used to deduct the number) / 2= taken into account only when the wall height is higher than 10m)';

-- Feed the table

INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'building', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'house', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'detached', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'residential', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'apartments', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'bungalow', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'historic', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'monument', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'ruins', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'castle', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'agricultural', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'farm', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'farm_auxiliary', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'barn', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'greenhouse', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'silo', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'commercial', 2);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'industrial', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'sport', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'sports_centre', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'grandstand', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'transportation', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'train_station', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'toll_booth', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'terminal', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'healthcare', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'education', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'entertainment_arts_culture', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'sustenance', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'military', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'religious', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'chapel', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'church', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'government', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'townhall', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(null, 'office', 1);



----------------------------------------------------------------------------------------------------------------------
-- FOR ROAD
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the road type
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_TYPE;
CREATE TABLE $ROAD_ABSTRACT_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'highway', 'Any kind of street or way', 'https://wiki.openstreetmap.org/wiki/Key:highway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'motorway', 'Highest-performance highway within a territory that deserve main towns. Usually have a reglemented access', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=motorway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'trunk', 'Important high-performance highway that are not motorways. Deserving main towns', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=trunk');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'primary', 'Important highway linking large towns. Usually have two lanes but not separeted by a central barrier', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=primary');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'secondary', 'Highway linking large towns. Usually have two lanes but not separeted by a central barrier', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=secondary');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'tertiary', 'Highway linking small settlements, or the local centres of a large town or city', 'https://wiki.openstreetmap.org/wiki/Tag:highway=tertiary');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'residential', 'Highway generally used for local traffic within settlement. Usually highway accesing or around residential areas', 'https://wiki.openstreetmap.org/wiki/Tag:highway=residential');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'unclassified', 'Minor public highway typically at the lowest level of the interconnecting grid network. Have lower importance in the highway network than tertiary and are not residential streets or agricultural tracks', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=unclassified');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'track', 'Roads for mostly agricultural use, forest tracks etc.; usually unpaved (unsealed) but may apply to paved tracks as well, that are suitable for two-track vehicles, such as tractors or jeeps', 'https://wiki.openstreetmap.org/wiki/Tag:highway=track');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'path', 'A generic multi-use path open to non-motorized vehicles', 'https://wiki.openstreetmap.org/wiki/Tag:highway=path');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'footway', 'For designated footpaths, i.e. mainly/exclusively for pedestrians', 'https://wiki.openstreetmap.org/wiki/Tag:highway=footway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'cycleway', 'Separated way for the use of cyclists', 'https://wiki.openstreetmap.org/wiki/Tag:highway=cycleway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'steps', 'For flights of steps on footways and paths', 'https://wiki.openstreetmap.org/wiki/Tag:highway=steps');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'highway_link', 'Connecting ramp to/from a highway', 'https://wiki.openstreetmap.org/wiki/Key:highway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'roundabout', 'Generally a circular (self-intersecting) highway junction where the traffic on the roundabout has right of way', 'https://wiki.openstreetmap.org/wiki/Tag:junction=roundabout');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(null, 'ferry', 'A ferry route used to transport things or people from one bank of a watercourse or inlet to the other, or as a permanent or seasonal local maritime link, and a link to a foreign country', 'http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf');


--------------------------------------------------------
-- Abstract table for the road surface
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_SURFACE;
CREATE TABLE $ROAD_ABSTRACT_SURFACE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'unpaved', 'Generic term to qualify the surface of a highway that is predominantly unsealed along its length; i.e., it has a loose covering ranging from compacted stone chippings to ground', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'paved', 'Surface with coating. Generic term for a highway with a stabilized and hard surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'ground', 'Surface of the ground itself with no specific fraction of rock', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'gravel', 'Surface composed of broken/crushed rock larger than sand grains and thinner than pebblestone', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'concrete', 'Cement based concrete surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'grass', 'Grass covered ground', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'compacted', 'A mixture of larger (e.g., gravel) and smaller (e.g., sand) parts, compacted', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'sand', 'Small to very small fractions of rock as findable alongside body of water', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'cobblestone', 'Any cobbled surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'wood', 'Highway made of wooden surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'pebblestone', 'Surface made of rounded rock as pebblestone findable alongside body of water', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'mud', 'Wet unpaved surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'metal', 'Metallic surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(null, 'water', 'Used to qualify the surface of ferry route that uses water waterbodies, rivers, seas,...) as a traffic surface', '');


--------------------------------------------------------
-- Abstract road parameters
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_PARAMETERS;
CREATE TABLE $ROAD_ABSTRACT_PARAMETERS (ID_TYPE serial, TERM varchar, MIN_WIDTH integer);

-- Add comments
COMMENT ON COLUMN $ROAD_ABSTRACT_PARAMETERS."TERM" IS 'Road type';
COMMENT ON COLUMN $ROAD_ABSTRACT_PARAMETERS."MIN_WIDTH" IS 'Specifies the road minimum width (in meter)';

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'highway', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'motorway', 24);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'trunk', 16);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'primary', 10);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'secondary', 10);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'tertiary', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'residential', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'unclassified', 3);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'track', 2);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'path', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'footway', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'cycleway', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'steps', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'highway_link', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'roundabout', 4);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(null, 'ferry', 0);


--------------------------------------------------------
-- Abstract table for the road crossing
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_CROSSING;
CREATE TABLE $ROAD_ABSTRACT_CROSSING (ID_CROSSING serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_CROSSING VALUES(null, 'bridge', 'Artificial construction that spans features such as roads, railways, waterways or valleys and carries a road, railway or other feature', 'https://wiki.openstreetmap.org/wiki/Key:bridge');
INSERT INTO $ROAD_ABSTRACT_CROSSING VALUES(null, 'tunnel', 'Underground passage for roads, railways or similar', 'https://wiki.openstreetmap.org/wiki/Key:tunnel');
INSERT INTO $ROAD_ABSTRACT_CROSSING VALUES(null, 'null', 'Everything but a bridge or a tunnel', '');


----------------------------------------------------------------------------------------------------------------------
-- FOR RAIL
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the rail type
--------------------------------------------------------

DROP TABLE IF EXISTS $RAIL_ABSTRACT_TYPE;
CREATE TABLE $RAIL_ABSTRACT_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'highspeed', 'Railway track for highspeed rail', 'https://wiki.openstreetmap.org/wiki/Key:highspeed');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'rail', 'Railway track for full sized passenger or freight trains in the standard gauge for the country or state', 'https://wiki.openstreetmap.org/wiki/Tag:railway=rail');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'service_track', 'Railway track mainly used for sorting or temporary parking of freight trains', 'http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'disused', 'A section of railway which is no longer used but where the track and infrastructure remain in place', 'https://wiki.openstreetmap.org/wiki/Tag:railway=disused');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'funicular', 'Cable railway in which a cable attached to a pair of tram-like vehicles on rails moves them up and down a steep slope, the ascending and descending vehicles counterbalancing each other', 'https://wiki.openstreetmap.org/wiki/Tag:railway=funicular');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'subway', 'Rails used for city public transport that are always completely separated from other traffic, often underground', 'https://wiki.openstreetmap.org/wiki/Tag:railway=subway');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(null, 'tram', 'Railway track which is mainly or exclusively used for trams, or where tram tracks are laid within a normal road open to all traffic, often called street running', 'https://wiki.openstreetmap.org/wiki/Tag:railway=tram');


--------------------------------------------------------
-- Abstract table for the rail crossing
--------------------------------------------------------

DROP TABLE IF EXISTS $RAIL_ABSTRACT_CROSSING;
CREATE TABLE $RAIL_ABSTRACT_CROSSING (ID_CROSSING serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $RAIL_ABSTRACT_CROSSING VALUES(null, 'bridge', 'Artificial construction that spans features such as roads, railways, waterways or valleys and carries a road, railway or other feature', 'https://wiki.openstreetmap.org/wiki/Key:bridge');
INSERT INTO $RAIL_ABSTRACT_CROSSING VALUES(null, 'tunnel', 'Underground passage for roads, railways or similar', 'https://wiki.openstreetmap.org/wiki/Key:tunnel');
INSERT INTO $RAIL_ABSTRACT_CROSSING VALUES(null, 'null', 'Everything but a bridge or a tunnel', '');


----------------------------------------------------------------------------------------------------------------------
-- FOR VEGETATION
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the vegetation type
--------------------------------------------------------

DROP TABLE IF EXISTS $VEGET_ABSTRACT_TYPE;
CREATE TABLE $VEGET_ABSTRACT_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'tree', 'A single tree', 'https://wiki.openstreetmap.org/wiki/Tag:natural=tree');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'wood', 'Tree-covered area (a forest or wood) not managed for economic purposes', 'https://wiki.openstreetmap.org/wiki/Tag:natural=wood');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'forest', 'Managed woodland or woodland plantation. Wooded area maintained by human to obtain forest products', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=forest');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'scrub', 'Uncultivated land covered with bushes or stunted trees', 'https://wiki.openstreetmap.org/wiki/Tag:natural=scrub');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'grassland', 'Natural areas where the vegetation is dominated by grasses (Poaceae) and other herbaceous (non-woody) plants', 'https://wiki.openstreetmap.org/wiki/Tag:natural=grassland');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'heath', 'A dwarf-shrub habitat, characterised by open, low growing woody vegetation, often dominated by plants of the Ericaceae', 'https://wiki.openstreetmap.org/wiki/Tag:natural=heath');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'tree_row', 'A line of trees', 'https://wiki.openstreetmap.org/wiki/Tag:natural=tree_row');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'hedge', 'A line of closely spaced shrubs and tree species, which form a barrier or mark the boundary of an area', 'https://wiki.openstreetmap.org/wiki/Tag:barrier=hedge');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'mangrove', 'It is formed by forests of salt tolerant mangrove trees in the tidal zone of tropical coasts with water temperatures above 20° C', 'https://wiki.openstreetmap.org/wiki/Tag:wetland=mangrove');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'orchard', 'Intentional planting of trees or shrubs maintained for food production', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=orchard');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'vineyard', 'A piece of land where grapes are grown', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=vineyard');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'banana_plants', 'A banana plantation', 'https://wiki.openstreetmap.org/wiki/Key:trees');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(null, 'sugar_cane', 'A piece of land where sugar cane are grown', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=farmland');


--------------------------------------------------------
-- Abstract vegetation parameters
--------------------------------------------------------

DROP TABLE IF EXISTS $VEGET_ABSTRACT_PARAMETERS;
CREATE TABLE $VEGET_ABSTRACT_PARAMETERS (ID_TYPE serial, TERM varchar, HEIGHT_CLASS varchar);

-- Add comments
COMMENT ON COLUMN $VEGET_ABSTRACT_PARAMETERS."TERM" IS 'Vegetation type';
COMMENT ON COLUMN $VEGET_ABSTRACT_PARAMETERS."HEIGHT_CLASS" IS 'Specifies whether it is high or low vegetation';

-- Feed the table

INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'tree', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'wood', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'forest', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'scrub', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'grassland', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'heath', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'tree_row', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'hedge', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'mangrove', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'orchard', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'vineyard', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'banana_plants', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(null, 'sugar_cane', 'low');