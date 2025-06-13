Building layer
==============

The building layer represents the footprint of the building as a set of Polygons or MultiPolygons in 2D coordinates.

.. list-table::
   :header-rows: 1
   :widths: 15 10 40 35

   * - Name
     - Type
     - Constraints
     - Definition
   * - the_geom
     - POLYGON
     - X Y dimensions
     - Geometry
   * - id_build
     - INTEGER
     - Primary Key
     - Unique Identifier
   * - id_source
     - VARCHAR
     - *not null*
     - Identifier of the feature from the input data source
   * - id_zone
     - VARCHAR
     - *not null*
     - Studied zone identifier
   * - height_wall
     - FLOAT
     - *not null; > 0*
     - The (corrected) height of the building in meters. Height of the building measured between the ground and the gutter (maximum altitude of the polyline describing the building). (expressed in meters)
   * - height_roof
     - FLOAT
     - *not null ; > 0 ; >= height_wall*
     - The maximum height of a building is the distance between the top edge of the building (including the roof, but excluding antennas, spires and other equipment mounted on the roof) and the lowest point at the bottom where the building meets the ground. (expressed in meters)
   * - nb_lev
     - INTEGER
     - *not null; > 0*
     - Number of levels (have to be greater than 0)
   * - type
     - VARCHAR
     - *not null ; in type list*
     - Value allowing to distinguish the type of building according to its architecture. These values are listed in the BUILDING_use_and_type section.
   * - main_use
     - VARCHAR
     - *in type list*
     - Main use of the building. The use of a building corresponds to a de facto element, relating to what it is used for. These values are listed in the BUILDING_use_and_type section.
   * - roof_shape
     - VARCHAR
     - *null allowed*
     - Defines the shape of the roof according to OSM common values: https://wiki.openstreetmap.org/wiki/Key:roof:shape
   * - zindex
     - INTEGER
     - *not null ; >-4 ; <4*
     - Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the objects are above the ground surface. -4 to -1 indicates that the object is underground.
   * - pop
     - FLOAT
     -
     - Number of inhabitants computed from WorldPop database: https://www.worldpop.org/. This column is optional.

type and main_use column values
-------------------------------

Below is the list of all possible values for the ``type`` and the ``main_use`` attributes, in the ``BUILDING`` layer.

"building", "house", "detached", "residential", "apartments", "bungalow", "historic", "monument", "ruins", "castle", "agricultural", "farm", "farm_auxiliary", "barn", "greenhouse", "silo", "commercial", "industrial", "sport", "sports_centre", "grandstand", "transport", "train_station", "toll_booth", "toll", "terminal", "airport_terminal", "healthcare", "education", "entertainment_arts_culture", "sustenance", "military", "religious", "chapel", "church", "government", "townhall", "office", "emergency", "hotel", "hospital", "parking", "slight_construction", "water_tower", "fortress", "abbey", "cathedral", "mosque", "musalla", "temple", "synagogue", "shrine", "place_of_worship", "wayside_shrine", "swimming_pool", "fitness_centre", "horse_riding", "ice_rink", "pitch", "stadium", "track", "sports_hall", "ammunition", "bunker", "barracks", "casemate", "station", "government_office", "stable", "sty", "cowshed", "digester", "farmhouse", "bank", "bureau_de_change", "boat_rental", "car_rental", "internet_cafe", "kiosk", "money_transfer", "market", "marketplace", "pharmacy", "post_office", "retail", "shop", "store", "supermarket", "warehouse", "factory", "gas", "heating_station", "oil_mill", "oil", "wellsite", "well_cluster", "grain_silo", "villa", "dormitory", "condominium", "sheltered_housing", "workers_dormitory", "terrace", "transportation", "hangar", "tower", "control_tower", "aeroway", "roundhouse", "social_facility", "college", "kindergarten", "school", "university", "cinema", "arts_centre", "brothel", "casino", "community_centre", "conference_centre", "events_venue", "exhibition_centre", "gambling", "music_venue", "nightclub", "planetarium", "social_centre", "studio", "theatre", "library", "museum", "aquarium", "gallery", "information", "restaurant", "bar", "cafe", "fast_food", "ice_cream", "pub", "attraction", "refinery", "hut", "cabin", "shed"

We consider that a same value can be used to qualify a ``type`` or a ``main_use``. The definition for each of these types is given by the OpenStreetMap wiki (for example here is the page containing the "house" definition: https://wiki.openstreetmap.org/wiki/Tag:building=house).

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

.. include:: _Footer.rst