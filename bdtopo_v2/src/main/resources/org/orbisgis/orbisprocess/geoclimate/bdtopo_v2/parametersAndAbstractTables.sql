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

INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(1, 'building', 'Given object as a building', 'https://wiki.openstreetmap.org/wiki/Key:building');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(2, 'house', 'Single dwelling unit usually inhabited by one family', 'https://wiki.openstreetmap.org/wiki/Tag:building=house');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(3, 'detached', 'Free-standing residential building usually housing a single-family', 'https://wiki.openstreetmap.org/wiki/Tag:building=detached');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(4, 'residential', 'Building used primarily for residential purposes', 'https://wiki.openstreetmap.org/wiki/Tag:building=residential');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(5, 'apartments', 'Building arranged into individual dwellings, often on separate floors. May also have retail outlets on the ground floor', 'https://wiki.openstreetmap.org/wiki/Tag:building=apartments');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(6, 'bungalow', 'Small, single-storey detached house in the form of a bungalow', 'https://wiki.openstreetmap.org/wiki/Tag:building=bungalow');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(7, 'historic', 'Any places or buildings, with a historical dimension', 'https://wiki.openstreetmap.org/wiki/Key:historic');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(8, 'monument', 'Memorial object, which is especially large, built to remember, show respect to a person or group of people or to commemorate an event', 'https://wiki.openstreetmap.org/wiki/Tag:historic=monument');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(9, 'ruins', 'House that is an abandoned (but still a building)', 'https://wiki.openstreetmap.org/wiki/Tag:building=ruins');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(10, 'castle', 'Various kinds of structures, most of which were originally built as fortified residences of a lord or noble', 'https://wiki.openstreetmap.org/wiki/Tag:historic=castle');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(11, 'agricultural', 'Building, machinery, facilities, related to agricultural production', 'https://www.eionet.europa.eu/gemet/en/concept/8014');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(12, 'farm', 'A farmhouse is the main building of a farm', 'https://wiki.openstreetmap.org/wiki/Tag:building=farm');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(13, 'farm_auxiliary', 'Building on a farm that is not a dwelling', 'https://wiki.openstreetmap.org/wiki/Tag:building=farm_auxiliary');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(14, 'barn', 'Agricultural building used for storage and as a covered workplace', 'https://wiki.openstreetmap.org/wiki/Tag:building=barn');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(15, 'greenhouse', 'A Greenhouse (also called a glasshouse) is a building in which plants are grown. It typically has a roof and walls made of clear glass or plastic to allow sunlight to enter', 'https://wiki.openstreetmap.org/wiki/Tag:building=greenhouse');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(16, 'silo', 'Storage container for bulk material, often grains such as corn or wheat', 'https://wiki.openstreetmap.org/wiki/Tag:man_made=silo');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(17, 'commercial', 'Building where non-specific commercial activities take place', 'https://wiki.openstreetmap.org/wiki/Tag:building=commercial');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(18, 'industrial', 'Building where some industrial process takes place', 'https://wiki.openstreetmap.org/wiki/Tag:building=industrial');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(19, 'sport', 'Buildings, constructions, installations, organized areas and equipment for indoor and outdoor sport activities', 'https://wiki.openstreetmap.org/wiki/Tag:building=sport');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(20, 'sports_centre', 'Building that is designed for sports, e.g. for school sports, university or club sports', 'https://wiki.openstreetmap.org/wiki/Tag:building=sports_centre');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(21, 'grandstand', 'Building for the main stand, usually roofed, commanding the best view for spectators at racecourses or sports grounds', 'https://wiki.openstreetmap.org/wiki/Tag:building=grandstand');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(22, 'transportation', 'Building, construction, installation, organized areas and equipment for transportation', 'https://wiki.openstreetmap.org/wiki/Tag:building=transportation');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(23, 'train_station', 'Train station building', 'https://wiki.openstreetmap.org/wiki/Tag:building=train_station');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(24, 'toll_booth', 'Toll roads charge money for some or all traffic', 'https://wiki.openstreetmap.org/wiki/Tag:barrier=toll_booth');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(25, 'terminal', 'Airport passenger building', 'https://wiki.openstreetmap.org/wiki/Tag:aeroway=terminal');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(26, 'healthcare', 'All places that provide healthcare', 'https://wiki.openstreetmap.org/wiki/Key:healthcare');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(27, 'education', 'All places that provide education', 'https://wiki.openstreetmap.org/wiki/Map_Features#Education');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(28, 'entertainment_arts_culture', 'All places that provide entertainment, arts and culture', 'https://wiki.openstreetmap.org/wiki/Key:amenity#Entertainment.2C_Arts_.26_Culture');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(29, 'sustenance', 'Buildings, constructions, installations, organized areas and equipment  of any food commodity or related food products. ex : bar, pub...', 'https://wiki.openstreetmap.org/wiki/Map_Features#Sustenance');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(30, 'military', 'Buildings, constructions, installations necessary to the performance of military activities, either combat or noncombat', 'https://wiki.openstreetmap.org/wiki/Key:military"');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(31, 'religious', 'Unspecific religious building', 'https://wiki.openstreetmap.org/wiki/Key:building#Religious');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(32, 'chapel', 'Religious building, often pretty small. One can enter in it to pray or meditate', 'https://wiki.openstreetmap.org/wiki/Tag:building=chapel');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(33, 'church', 'Building that was built as a church', 'https://wiki.openstreetmap.org/wiki/Tag:building=church');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(34, 'government', 'Building built to house government offices', 'https://wiki.openstreetmap.org/wiki/Tag:building=government');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(35, 'townhall', 'Building that may serve as an administrative center, or may be merely a community meeting place', 'https://wiki.openstreetmap.org/wiki/Tag:amenity=townhall');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(36, 'office', 'Office block typically houses companies, but offices may be also rented by any other kind of organization like charities, government, any NGO etc.', 'https://wiki.openstreetmap.org/wiki/Tag:building=office');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(37, 'heavy_industry', 'Low-rise and midrise industrial structures (towers, tanks, stacks)', 'https://en.wikipedia.org/wiki/Heavy_industry');
INSERT INTO $BUILDING_ABSTRACT_USE_TYPE VALUES(38, 'light_industry', 'Industrial structure that require fewer raw materials, space and power. For example, electronics manufacturing', 'https://en.wikipedia.org/wiki/Light_industry');

CREATE INDEX ON $BUILDING_ABSTRACT_USE_TYPE(TERM);

--------------------------------------------------------
-- Abstract Building parameters
--------------------------------------------------------

DROP TABLE IF EXISTS $BUILDING_ABSTRACT_PARAMETERS;
CREATE TABLE $BUILDING_ABSTRACT_PARAMETERS (ID_TYPE serial, TERM varchar, NB_LEV integer);

-- Add comments
COMMENT ON COLUMN $BUILDING_ABSTRACT_PARAMETERS."TERM" IS 'Building type';
COMMENT ON COLUMN $BUILDING_ABSTRACT_PARAMETERS."NB_LEV" IS 'Specifies whether or not the building type is taken into account when calculating the number of levels (0 = not taken into account (in this case, the number of levels will be forced to 0) / 1= taken into account (in this case, a formula will be used to deduct the number) / 2= taken into account only when the wall height is higher than 10m)';

-- Feed the table

INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(1, 'building', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(2, 'house', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(3, 'detached', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(4, 'residential', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(5, 'apartments', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(6, 'bungalow', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(7, 'historic', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(8, 'monument', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(9, 'ruins', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(10, 'castle', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(11, 'agricultural', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(12, 'farm', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(13, 'farm_auxiliary', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(14, 'barn', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(15, 'greenhouse', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(16, 'silo', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(17, 'commercial', 2);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(18, 'industrial', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(19, 'sport', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(20, 'sports_centre', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(21, 'grandstand', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(22, 'transportation', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(23, 'train_station', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(24, 'toll_booth', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(25, 'terminal', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(26, 'healthcare', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(27, 'education', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(28, 'entertainment_arts_culture', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(29, 'sustenance', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(30, 'military', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(31, 'religious', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(32, 'chapel', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(33, 'church', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(34, 'government', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(35, 'townhall', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(36, 'office', 1);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(37, 'heavy_industry', 0);
INSERT INTO $BUILDING_ABSTRACT_PARAMETERS VALUES(38, 'light_industry', 0);

CREATE INDEX ON $BUILDING_ABSTRACT_PARAMETERS(TERM);
CREATE INDEX ON $BUILDING_ABSTRACT_PARAMETERS(NB_LEV);

----------------------------------------------------------------------------------------------------------------------
-- FOR ROAD
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the road type
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_TYPE;
CREATE TABLE $ROAD_ABSTRACT_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(1, 'highway', 'Any kind of street or way', 'https://wiki.openstreetmap.org/wiki/Key:highway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(2, 'motorway', 'Highest-performance highway within a territory that deserve main towns. Usually have a reglemented access', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=motorway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(3, 'trunk', 'Important high-performance highway that are not motorways. Deserving main towns', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=trunk');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(4, 'primary', 'Important highway linking large towns. Usually have two lanes but not separeted by a central barrier', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=primary');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(5, 'secondary', 'Highway linking large towns. Usually have two lanes but not separeted by a central barrier', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=secondary');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(6, 'tertiary', 'Highway linking small settlements, or the local centres of a large town or city', 'https://wiki.openstreetmap.org/wiki/Tag:highway=tertiary');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(7, 'residential', 'Highway generally used for local traffic within settlement. Usually highway accesing or around residential areas', 'https://wiki.openstreetmap.org/wiki/Tag:highway=residential');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(8, 'unclassified', 'Minor public highway typically at the lowest level of the interconnecting grid network. Have lower importance in the highway network than tertiary and are not residential streets or agricultural tracks', 'https://wiki.openstreetmap.org/wiki/FR:Tag:highway=unclassified');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(9, 'track', 'Roads for mostly agricultural use, forest tracks etc.; usually unpaved (unsealed) but may apply to paved tracks as well, that are suitable for two-track vehicles, such as tractors or jeeps', 'https://wiki.openstreetmap.org/wiki/Tag:highway=track');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(10, 'path', 'A generic multi-use path open to non-motorized vehicles', 'https://wiki.openstreetmap.org/wiki/Tag:highway=path');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(11, 'footway', 'For designated footpaths, i.e. mainly/exclusively for pedestrians', 'https://wiki.openstreetmap.org/wiki/Tag:highway=footway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(12, 'cycleway', 'Separated way for the use of cyclists', 'https://wiki.openstreetmap.org/wiki/Tag:highway=cycleway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(13, 'steps', 'For flights of steps on footways and paths', 'https://wiki.openstreetmap.org/wiki/Tag:highway=steps');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(14, 'highway_link', 'Connecting ramp to/from a highway', 'https://wiki.openstreetmap.org/wiki/Key:highway');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(15, 'roundabout', 'Generally a circular (self-intersecting) highway junction where the traffic on the roundabout has right of way', 'https://wiki.openstreetmap.org/wiki/Tag:junction=roundabout');
INSERT INTO $ROAD_ABSTRACT_TYPE VALUES(16, 'ferry', 'A ferry route used to transport things or people from one bank of a watercourse or inlet to the other, or as a permanent or seasonal local maritime link, and a link to a foreign country', 'http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf');

CREATE INDEX ON $ROAD_ABSTRACT_TYPE(TERM);

--------------------------------------------------------
-- Abstract table for the road surface
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_SURFACE;
CREATE TABLE $ROAD_ABSTRACT_SURFACE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(1, 'unpaved', 'Generic term to qualify the surface of a highway that is predominantly unsealed along its length; i.e., it has a loose covering ranging from compacted stone chippings to ground', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(2, 'paved', 'Surface with coating. Generic term for a highway with a stabilized and hard surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(3, 'ground', 'Surface of the ground itself with no specific fraction of rock', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(4, 'gravel', 'Surface composed of broken/crushed rock larger than sand grains and thinner than pebblestone', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(5, 'concrete', 'Cement based concrete surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(6, 'grass', 'Grass covered ground', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(7, 'compacted', 'A mixture of larger (e.g., gravel) and smaller (e.g., sand) parts, compacted', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(8, 'sand', 'Small to very small fractions of rock as findable alongside body of water', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(9, 'cobblestone', 'Any cobbled surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(10, 'wood', 'Highway made of wooden surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(11, 'pebblestone', 'Surface made of rounded rock as pebblestone findable alongside body of water', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(12, 'mud', 'Wet unpaved surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(13, 'metal', 'Metallic surface', 'https://wiki.openstreetmap.org/wiki/Key:surface');
INSERT INTO $ROAD_ABSTRACT_SURFACE VALUES(14, 'water', 'Used to qualify the surface of ferry route that uses water waterbodies, rivers, seas,...) as a traffic surface', '');

CREATE INDEX ON $ROAD_ABSTRACT_SURFACE(TERM);

--------------------------------------------------------
-- Abstract road parameters
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_PARAMETERS;
CREATE TABLE $ROAD_ABSTRACT_PARAMETERS (ID_TYPE serial, TERM varchar, MIN_WIDTH integer);

-- Add comments
COMMENT ON COLUMN $ROAD_ABSTRACT_PARAMETERS."TERM" IS 'Road type';
COMMENT ON COLUMN $ROAD_ABSTRACT_PARAMETERS."MIN_WIDTH" IS 'Specifies the road minimum width (in meter)';

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(1, 'highway', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(2, 'motorway', 24);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(3, 'trunk', 16);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(4, 'primary', 10);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(5, 'secondary', 10);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(6, 'tertiary', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(7, 'residential', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(8, 'unclassified', 3);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(9, 'track', 2);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(10, 'path', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(11, 'footway', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(12, 'cycleway', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(13, 'steps', 1);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(14, 'highway_link', 8);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(15, 'roundabout', 4);
INSERT INTO $ROAD_ABSTRACT_PARAMETERS VALUES(16, 'ferry', 0);

CREATE INDEX ON $ROAD_ABSTRACT_PARAMETERS(TERM);
CREATE INDEX ON $ROAD_ABSTRACT_PARAMETERS(MIN_WIDTH);

--------------------------------------------------------
-- Abstract table for the road crossing
--------------------------------------------------------

DROP TABLE IF EXISTS $ROAD_ABSTRACT_CROSSING;
CREATE TABLE $ROAD_ABSTRACT_CROSSING (ID_CROSSING serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $ROAD_ABSTRACT_CROSSING VALUES(1, 'bridge', 'Artificial construction that spans features such as roads, railways, waterways or valleys and carries a road, railway or other feature', 'https://wiki.openstreetmap.org/wiki/Key:bridge');
INSERT INTO $ROAD_ABSTRACT_CROSSING VALUES(2, 'tunnel', 'Underground passage for roads, railways or similar', 'https://wiki.openstreetmap.org/wiki/Key:tunnel');
INSERT INTO $ROAD_ABSTRACT_CROSSING VALUES(3, 'null', 'Everything but a bridge or a tunnel', '');


----------------------------------------------------------------------------------------------------------------------
-- FOR RAIL
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the rail type
--------------------------------------------------------

DROP TABLE IF EXISTS $RAIL_ABSTRACT_TYPE;
CREATE TABLE $RAIL_ABSTRACT_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(1, 'highspeed', 'Railway track for highspeed rail', 'https://wiki.openstreetmap.org/wiki/Key:highspeed');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(2, 'rail', 'Railway track for full sized passenger or freight trains in the standard gauge for the country or state', 'https://wiki.openstreetmap.org/wiki/Tag:railway=rail');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(3, 'service_track', 'Railway track mainly used for sorting or temporary parking of freight trains', 'http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(4, 'disused', 'A section of railway which is no longer used but where the track and infrastructure remain in place', 'https://wiki.openstreetmap.org/wiki/Tag:railway=disused');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(5, 'funicular', 'Cable railway in which a cable attached to a pair of tram-like vehicles on rails moves them up and down a steep slope, the ascending and descending vehicles counterbalancing each other', 'https://wiki.openstreetmap.org/wiki/Tag:railway=funicular');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(6, 'subway', 'Rails used for city public transport that are always completely separated from other traffic, often underground', 'https://wiki.openstreetmap.org/wiki/Tag:railway=subway');
INSERT INTO $RAIL_ABSTRACT_TYPE VALUES(7, 'tram', 'Railway track which is mainly or exclusively used for trams, or where tram tracks are laid within a normal road open to all traffic, often called street running', 'https://wiki.openstreetmap.org/wiki/Tag:railway=tram');


--------------------------------------------------------
-- Abstract table for the rail crossing
--------------------------------------------------------

DROP TABLE IF EXISTS $RAIL_ABSTRACT_CROSSING;
CREATE TABLE $RAIL_ABSTRACT_CROSSING (ID_CROSSING serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $RAIL_ABSTRACT_CROSSING VALUES(1, 'bridge', 'Artificial construction that spans features such as roads, railways, waterways or valleys and carries a road, railway or other feature', 'https://wiki.openstreetmap.org/wiki/Key:bridge');
INSERT INTO $RAIL_ABSTRACT_CROSSING VALUES(2, 'tunnel', 'Underground passage for roads, railways or similar', 'https://wiki.openstreetmap.org/wiki/Key:tunnel');
INSERT INTO $RAIL_ABSTRACT_CROSSING VALUES(3, 'null', 'Everything but a bridge or a tunnel', '');


----------------------------------------------------------------------------------------------------------------------
-- FOR VEGETATION
----------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------
-- Abstract table for the vegetation type
--------------------------------------------------------

DROP TABLE IF EXISTS $VEGET_ABSTRACT_TYPE;
CREATE TABLE $VEGET_ABSTRACT_TYPE (ID_TYPE serial, TERM varchar, DEFINITION varchar, SOURCE varchar);

-- Feed the table

INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(1, 'tree', 'A single tree', 'https://wiki.openstreetmap.org/wiki/Tag:natural=tree');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(2, 'wood', 'Tree-covered area (a forest or wood) not managed for economic purposes', 'https://wiki.openstreetmap.org/wiki/Tag:natural=wood');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(3, 'forest', 'Managed woodland or woodland plantation. Wooded area maintained by human to obtain forest products', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=forest');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(4, 'scrub', 'Uncultivated land covered with bushes or stunted trees', 'https://wiki.openstreetmap.org/wiki/Tag:natural=scrub');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(5, 'grassland', 'Natural areas where the vegetation is dominated by grasses (Poaceae) and other herbaceous (non-woody) plants', 'https://wiki.openstreetmap.org/wiki/Tag:natural=grassland');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(6, 'heath', 'A dwarf-shrub habitat, characterised by open, low growing woody vegetation, often dominated by plants of the Ericaceae', 'https://wiki.openstreetmap.org/wiki/Tag:natural=heath');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(7, 'tree_row', 'A line of trees', 'https://wiki.openstreetmap.org/wiki/Tag:natural=tree_row');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(8, 'hedge', 'A line of closely spaced shrubs and tree species, which form a barrier or mark the boundary of an area', 'https://wiki.openstreetmap.org/wiki/Tag:barrier=hedge');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(9, 'mangrove', 'It is formed by forests of salt tolerant mangrove trees in the tidal zone of tropical coasts with water temperatures above 20° C', 'https://wiki.openstreetmap.org/wiki/Tag:wetland=mangrove');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(10, 'orchard', 'Intentional planting of trees or shrubs maintained for food production', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=orchard');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(11, 'vineyard', 'A piece of land where grapes are grown', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=vineyard');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(12, 'banana_plants', 'A banana plantation', 'https://wiki.openstreetmap.org/wiki/Key:trees');
INSERT INTO $VEGET_ABSTRACT_TYPE VALUES(13, 'sugar_cane', 'A piece of land where sugar cane are grown', 'https://wiki.openstreetmap.org/wiki/Tag:landuse=farmland');


--------------------------------------------------------
-- Abstract vegetation parameters
--------------------------------------------------------

DROP TABLE IF EXISTS $VEGET_ABSTRACT_PARAMETERS;
CREATE TABLE $VEGET_ABSTRACT_PARAMETERS (ID_TYPE serial, TERM varchar, HEIGHT_CLASS varchar);

-- Add comments
COMMENT ON COLUMN $VEGET_ABSTRACT_PARAMETERS."TERM" IS 'Vegetation type';
COMMENT ON COLUMN $VEGET_ABSTRACT_PARAMETERS."HEIGHT_CLASS" IS 'Specifies whether it is high or low vegetation';

-- Feed the table

INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(1, 'tree', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(2, 'wood', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(3, 'forest', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(4, 'scrub', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(5, 'grassland', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(6, 'heath', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(7, 'tree_row', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(8, 'hedge', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(9, 'mangrove', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(10, 'orchard', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(11, 'vineyard', 'low');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(12, 'banana_plants', 'high');
INSERT INTO $VEGET_ABSTRACT_PARAMETERS VALUES(13, 'sugar_cane', 'low');