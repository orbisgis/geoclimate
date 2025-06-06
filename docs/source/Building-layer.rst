Building layer
==============

The building layer represents the footprint of the building as a set of
Polygons or MultiPolygons in 2D coordinates.

+------+---+----------------------------+------------------------------+
| Name | T | Constaints                 | Definition                   |
|      | y |                            |                              |
|      | p |                            |                              |
|      | e |                            |                              |
+======+===+============================+==============================+
| the_ | P | X Y dimensions             | Geometry                     |
| geom | O |                            |                              |
|      | L |                            |                              |
|      | Y |                            |                              |
|      | G |                            |                              |
|      | O |                            |                              |
|      | N |                            |                              |
+------+---+----------------------------+------------------------------+
| id_b | I | Primary Key                | Unique Identifier            |
| uild | N |                            |                              |
|      | T |                            |                              |
|      | E |                            |                              |
|      | G |                            |                              |
|      | E |                            |                              |
|      | R |                            |                              |
+------+---+----------------------------+------------------------------+
| id_s | V | *not null*                 | Identifier of the feature    |
| ourc | A |                            | from the input data source   |
| e    | R |                            |                              |
|      | C |                            |                              |
|      | H |                            |                              |
|      | A |                            |                              |
|      | R |                            |                              |
+------+---+----------------------------+------------------------------+
| id_z | V | *not null*                 | Studied zone identifier      |
| one  | A |                            |                              |
|      | R |                            |                              |
|      | C |                            |                              |
|      | H |                            |                              |
|      | A |                            |                              |
|      | R |                            |                              |
+------+---+----------------------------+------------------------------+
| heig | F | *not null; > 0*            | The (corrected) height of    |
| ht_w | L |                            | the building in meters.      |
| all  | O |                            | Height of the building       |
|      | A |                            | measured between the ground  |
|      | T |                            | and the gutter (maximum      |
|      |   |                            | altitude of the polyline     |
|      |   |                            | describing the building).    |
|      |   |                            | *(expressed in meters)*      |
+------+---+----------------------------+------------------------------+
| heig | F | *not null ; > 0 ; >=       | The maximum height of a      |
| ht_r | L | height_wall*               | building is the distance     |
| oof  | O |                            | between the top edge of the  |
|      | A |                            | building (including the      |
|      | T |                            | roof, but excluding          |
|      |   |                            | antennas, spires and other   |
|      |   |                            | equipment mounted on the     |
|      |   |                            | roof) and the lowest point   |
|      |   |                            | at the bottom where the      |
|      |   |                            | building meets the ground.   |
|      |   |                            | *(expressed in meters)*      |
+------+---+----------------------------+------------------------------+
| nb_l | I | *not null; > 0*            | Number of levels (have to be |
| ev   | N |                            | greater than 0)              |
|      | T |                            |                              |
|      | E |                            |                              |
|      | G |                            |                              |
|      | E |                            |                              |
|      | R |                            |                              |
+------+---+----------------------------+------------------------------+
| type | V | *not null ; in type list*  | Value allowing to            |
|      | A |                            | distinguish the type of      |
|      | R |                            | building according to its    |
|      | C |                            | architecture. These values   |
|      | H |                            | are listed in the            |
|      | A |                            | BUILDING_use_and_type        |
|      | R |                            | section.                     |
+------+---+----------------------------+------------------------------+
| main | V | *in type list*             | Main use of the building.    |
| _use | A |                            | The use of a building        |
|      | R |                            | corresponds to a de facto    |
|      | C |                            | element, relating to what it |
|      | H |                            | is used for. These values    |
|      | A |                            | are listed in the            |
|      | R |                            | BUILDING_use_and_type        |
|      |   |                            | section.                     |
+------+---+----------------------------+------------------------------+
| roof | V | *null allowed*             | Defines the shape of the     |
| _sha | A |                            | roof according `OSM common   |
| pe   | R |                            | values `__.                  |
|      | A |                            |                              |
|      | R |                            |                              |
+------+---+----------------------------+------------------------------+
| zind | I | *not null ; >-4 ; 4<*      | Defines the position with    |
| ex   | N |                            | respect to the ground. 0     |
|      | T |                            | indicates that the object is |
|      | E |                            | on the ground. 1 to 4        |
|      | G |                            | indicates that the objects   |
|      | E |                            | above the ground surface. -4 |
|      | R |                            | to -1 value indicates that   |
|      |   |                            | the object is underground.   |
+------+---+----------------------------+------------------------------+
| pop  | F |                            | Number of inhabitants        |
|      | L |                            | computed from `WorldPop      |
|      | O |                            | database `__.                |
|      | T |                            | This column is optional.     |
+------+---+----------------------------+------------------------------+

type and main_use column values
-------------------------------

Below is the list of all possible values for the ``type`` and the
``main_use`` attributes, in the ``BUILDING`` layer.

*“building”, “house”, “detached”, “residential”, “apartments”,
“bungalow”, “historic”, “monument”, “ruins”, “castle”, “agricultural”,
“farm”, “farm_auxiliary”, “barn”, “greenhouse”, “silo”, “commercial”,
“industrial”, “sport”, “sports_centre”, “grandstand”, “transport”,
“train_station”, “toll_booth”, “toll”, “terminal”, “airport_terminal”,
“healthcare”, “education”, “entertainment_arts_culture”, “sustenance”,
“military”, “religious”, “chapel”, “church”, “government”, “townhall”,
“office”, “emergency”, “hotel”, “hospital”, “parking”,
“slight_construction”, “water_tower” , “fortress”, “abbey”, “cathedral”,
“mosque”, “musalla”, “temple”, “synagogue”, “shrine”,
“place_of_worship”, “wayside_shrine”, “swimming_pool” ,
“fitness_centre”, “horse_riding”, “ice_rink” , “pitch”, “stadium”,
“track”, “sports_hall”, “ammunition”, “bunker”, “barracks”, “casemate”,
“station”, “government_office”, “stable”, “sty”, “cowshed”, “digester”,
“farmhouse”, “bank”, “bureau_de_change”, “boat_rental”, “car_rental” ,
“internet_cafe”, “kiosk”, “money_transfer”, “market”, “marketplace”,
“pharmacy” , “post_office” , “retail”, “shop” , “store”, “supermarket”,
“warehouse”, “factory”, “gas” , “heating_station”, “oil_mill” , “oil”,
“wellsite”, “well_cluster”, “grain_silo”, “villa”, “dormitory”,
“condominium”, “sheltered_housing”, “workers_dormitory”, “terrace”,
“transportation”, “hangar”, “tower”, “control_tower”, “aeroway”,
“roundhouse”, “social_facility”, “college”, “kindergarten”, “school”,
“university”, “cinema”, “arts_centre”, “brothel”, “casino”,
“community_centre”, “conference_centre”, “events_venue”,
“exhibition_centre”, “gambling”, “music_venue”, “nightclub”,
“planetarium”, “social_centre”, “studio”, “theatre”, “library”,
“museum”, “aquarium”, “gallery”, “information”, “restaurant”, “bar”,
“cafe”, “fast_food”, “ice_cream”, “pub”, “attraction”, “refinery”,
“hut”, “cabin”, “shed”*

We consider that a same value can be used to qualify a ``type`` or a
``main_use``. The definition for each of this type is given by the
`OpenStreetMap wiki `__ (for
example
`here `__ is the
page containing the “house” definition).

.. include:: _Footer.rst
