Grid indicators
===============

The table grid_indicators contains the grid cell identifier (id_grid)
and a certain number of indicators described below. When calculating
grid indicators, you can calculate only a subset of these indicators.
The whole list is the following: ["BUILDING_FRACTION",
"BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION",
"WATER_FRACTION", "VEGETATION_FRACTION", "ROAD_FRACTION",
"IMPERVIOUS_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
"BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY",
"SEA_LAND_FRACTION", "ASPECT_RATIO", "SVF",
"HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS",
"UTRF_AREA_FRACTION", "UTRF_FLOOR_AREA_FRACTION", "LCZ_PRIMARY",
"URBAN_SPRAWL_AREAS", "URBAN_SPRAWL_DISTANCES",
"URBAN_SPRAWL_COOL_DISTANCES", "BUILDING_HEIGHT_DISTRIBUTION",
"STREET_WIDTH", "PROJECTED_FACADE_DENSITY_DIR", "GEOM_AVG_HEIGHT_ROOF",].

BUILDING_FRACTION
---------------------

**Corresponding name in the table**: BUILDING_FRACTION

**Description**: Total building fraction. If superimposed with other
layers, it is not counted twice. Instead, the following priorities are
used: "water", "building", "high_vegetation", "low_vegetation", "road",
"impervious".

**Method**: SUM(Bu_Area after superimposition removal) / Cell_Area


BUILDING_TYPE_FRACTION
---------------------

**Corresponding name in the table**: AREA_FRACTION_X

**Description**: Footprint fraction within the RSU of _type \x_ building. There are too many `building types <Building-layer.html#type-and-main_use-column-values>`_ to have a fraction of each one, thus several building types are gathered within a broader _type \x_. In the following, the key is the _type \x_ and the list of values are the building types described `here <Building-layer.html#type-and-main_use-column-values>`_:

* "undefined"            : ["building", "undefined"],
* "individual_housing"   : ["house", "detached", "bungalow", "farm", "villa", "terrace", "cabin"],
* "collective_housing"   : ["apartments", "barracks", "abbey", "dormitory",
                          "sheltered_housing", "workers_dormitory",
                          "condominium"],
* "undefined_residential": ["residential"],
* "commercial"           : ["commercial", "internet_cafe", "money_transfer", "pharmacy",
                          "post_office", "cinema", "arts_centre", "brothel", "casino",
                          "sustenance", "hotel", "restaurant", "bar", "cafe", "fast_food",
                          "ice_cream", "pub", "aquarium"],
* "tertiary"             : ["government", "townhall", "retail", "gambling", "music_venue", "nightclub",
                          "shop", "store", "supermarket", "office", "terminal", "airport_terminal", "bank",
                          "bureau_de_change", "boat_rental", "car_rental", "research_institute",
                          "community_centre", "conference_centre", "events_venue",
                          "exhibition_centre", "social_centre", "studio", "theatre",
                          "library", "healthcare", "entertainment_arts_culture",
                          "hospital", "information", "civic"],
* "education"            : ["education", "swimming-pool", "fitness_centre", "sports_centre",
                          "college", "kindergarten", "school", "university", "museum", "gallery"],
* "light_industrial"     : ["industrial", "factory", "warehouse", "port", "manufacture"],
* "heavy_industrial"     : ["refinery"],
* "non_heated"           : ["silo", "barn", "cowshed", "ruins", "church", "chapel", "military",
                          "castle", "monument", "fortress", "synagogue", "mosquee", "musalla",
                          "shrine", "cathedral", "agricultural", "farm_auxiliary", "digester",
                          "horse_riding", "stadium", "track", "pitch", "ice_rink", "sports_hall",
                          "ammunition", "bunker", "casemate", "shelter", "religious", "place_of_worship",
                          "wayside_shrine", "station", "stable", "sty", "greenhouse", "kiosk", "marketplace",
                          "marker", "warehouse", "planetarium", "fire_station", "water_tower", "grandstand",
                          "transportation", "toll_booth", "hut", "shed", "garage", "service", "storage_tank",
                          "slurry_tank"]

**Method**: ``SUM(Bu_AREA of type X) / RSU_Area``




HIGH_VEGETATION_FRACTION
----------------------------

**Corresponding name in the table**: HIGH_VEGETATION_FRACTION

**Description**: Total high vegetation fraction. If superimposed with
other layers, it is not counted twice. Instead, the following priorities
are used: "water", "building", "high_vegetation", "low_vegetation",
"road", "impervious".

**Method**: SUM(High_veg_Area after superimposition removal) / Cell_Area

LOW_VEGETATION_FRACTION
---------------------------

**Corresponding name in the table**: LOW_VEGETATION_FRACTION

**Description**: Total low vegetation fraction. If superimposed with
other layers, it is not counted twice. Instead, the following priorities
are used: "water", "building", "high_vegetation", "low_vegetation",
"road", "impervious".

**Method**: SUM(Low_veg_Area after superimposition removal) / Cell_Area

ROAD_FRACTION
-----------------

**Corresponding name in the table**: ROAD_FRACTION

**Description**: Total road fraction. If superimposed with other layers,
it is not counted twice. Instead, the following priorities are used:
"water", "building", "high_vegetation", "low_vegetation", "road",
"impervious".

**Method**: SUM(Road_Area after superimposition removal) / Cell_Area

IMPERVIOUS_FRACTION
-----------------------

**Corresponding name in the table**: IMPERVIOUS_FRACTION

**Description**: Total impervious fraction (other than roads). If
superimposed with other layers, it is not counted twice. Instead, the
following priorities are used: "water", "building", "high_vegetation",
"low_vegetation", "road", "impervious".

**Method**: SUM(Impervious_Area after superimposition removal) / Cell_Area

BUILDING_HEIGHT_WEIGHTED
----------------------------

Two indicators are calculated: average building height and standard
deviation building height.

The first:

**Corresponding name in the table**: AVG_HEIGHT_ROOF_AREA_WEIGHTED

**Description**: Mean building’s roof height within the RSU (the
building heights being weighted by the building areas)

**Method**: SUM(Bu_Wall_Height * Bu_Area) / SUM(Bu_Area)

The second:

**Corresponding name in the table**: STD_HEIGHT_ROOF_AREA_WEIGHTED

**Description**: Variability of the building’s roof height within the
RSU (the building heights being weighted by the building areas)

**Method**: By default, the indicator of variability is the Standard
Deviation (STD) defined as:

→ SUM(Bu_Area*(Bu_Wall_Height - AVG_HEIGHT_ROOF)^2)) / SUM (Bu_Area)

BUILDING_HEIGHT
-------------------

Two indicators are calculated: average building height and standard
deviation building height.

The first:

**Corresponding name in the table**: AVG_HEIGHT_ROOF

**Description**: Mean building’s roof height within the grid cell

**Method**: SUM(Bu_Wall_Height) / SUM(Bu_Area)

The second:

**Corresponding name in the table**: STD_HEIGHT_ROOF

**Description**: Standard deviation building’s roof height within the
grid cell

**Method**: SUM(Bu_Wall_Height) / SUM(Bu_Area)

BUILDING_SURFACE_DENSITY
----------------------------

**Corresponding name in the table**: BUILDING_SURFACE_DENSITY

**Description**: All building facades (free facades and roofs) included
in a grid cell divided by the cell area

**Method**: building_fraction + free_external_facade_density

FREE_EXTERNAL_FACADE_DENSITY
--------------------------------

**Corresponding name in the table**: FREE_EXTERNAL_FACADE_DENSITY

**Description**: Sum of all building free facades (roofs are excluded)
included in a grid cell, divided by the cell area.

**Method**: SUM(Bu_TotalFacadeLength * HEIGHT_WALL) - SUM(Bu_SharedFacadeLength * MIN(height_wall_Bu1, height_wall_Bu2)) / cell_Area

SEA_LAND_FRACTION
---------------------

Two indicators are calculated: the sea and the land fractions.

The first:

**Corresponding name in the table**: LAND_FRACTION

**Description**: Sum all patches of land that are in a grid cell (land
coming from the sea_land_mask layer) and divide by the cell area

**Method**: SUM(Land_Area) / Cell_Area

The second:

**Corresponding name in the table**: SEA_FRACTION

**Description**: Sum all patches of sea that are in a grid cell (sea
coming from the sea_land_mask layer) and divide by the cell area

**Method**: SUM(Sea_Area) / Cell_Area

ASPECT_RATIO
----------------

**Corresponding name in the table**: ASPECT_RATIO

**Description**: Aspect ratio such as defined by Stewart et Oke (2012):
mean height-to-width ratio of street canyons (LCZs 1-7), building
spacing (LCZs 8-10), and tree spacing (LCZs A - G).

**Method**: A simple approach based on the street canyons assumption is
used for the calculation. The sum of facade area within a given grid
cell area is divided by the area of free surfaces of the given grid cell
(not covered by buildings).

→ 0.5 * Cell_free_external_facade_density / (1 - Cell_building_fraction)

GROUND_SKY_VIEW_FACTOR
--------------------------

**Corresponding name in the table**: GROUND_SKY_VIEW_FACTOR

**Description**: Grid cell ground Sky View Factor such as defined by
Stewart et Oke (2012): ratio of the amount of sky hemisphere visible
from ground level to that of an unobstructed hemisphere. In our case,
only buildings are considered as obstructing the atmosphere.

**Method**: The calculation is based on the
`ST_SVF <http://www.h2gis.org/docs/dev/ST_SVF/>`_ function of H2GIS
using only buildings as obstacles and with the following parameters: ray
length = 100, number of directions = 60. Using a uniform grid mesh of 10
m resolution, the SVF obtained has a standard deviation of the estimate
of 0.03 when compared with the most accurate method (according to
`Bernard et al. (2018) <https://dx.doi.org/10.3390/cli6030060>`_).

Using a grid of regular points, the density of points used for the
calculation actually depends on building density (higher the building
density, lower the density of points). To avoid this phenomenon and have
the same density of points per free ground surface, we use an H2GIS
function to distribute randomly points within free surfaces
(ST_GeneratePoints). This density of points is set by default to 0.008,
based on the median of `Bernard et al. (2018) <https://dx.doi.org/10.3390/cli6030060>`_ dataset.

**References**:

- Stewart, Ian D., and Tim R. Oke. “`Local climate zones for urban
  temperature studies <https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1>`_.”
  Bulletin of the American Meteorological Society 93, no. 12 (2012):
  1879-1900.
- Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. “`Sky
  View Factor Calculation in Urban Context: Computational Performance
  and Accuracy Analysis of Two Open and Free GIS
  Tools <https://www.mdpi.com/2225-1154/6/3/60>`_.” Climate, MDPI,
  2018, Urban Overheating - Progress on Mitigation Science and
  Engineering Applications, 6 (3), pp.60.

EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH
--------------------------------------

**Corresponding name in the table**:
EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH

**Description**: Effective terrain roughness length (z0).

**Method**: The method for z0 calculation is based on the Hanna and
Britter (2010) procedure (see equation (17) and examples of calculation
p. 156 in the corresponding reference). The
``cell_frontal_area_index_distribution_Hx_y_Dw_z`` is used to calculate
the mean projected facade density (considering all directions) and
``z0`` is then obtained multiplying the resulting value by the
``cell_geometric_mean_height``.

**Warning**: With the current method, if a building facade follows the
line separating two grid cells, it will be counted for both grid cells
(even though this situation is probably almost impossible with a regular
rectangular grid).

**References**:

- Stewart, Ian D., and Tim R. Oke. “`Local climate zones for urban
  temperature studies <https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1>`_.”
  Bulletin of the American Meteorological Society 93, no. 12 (2012):
  1879-1900.
- Hanna, Steven R., and Rex E. Britter. “`Wind flow and vapor cloud
  dispersion at industrial and urban sites <https://www.wiley.com/en-fr/Wind+Flow+and+Vapor+Cloud+Dispersion+at+Industrial+and+Urban+Sites-p-9780816908639>`_.”
  Vol. 7. John Wiley & Sons, 2010.

EFFECTIVE_TERRAIN_ROUGHNESS_CLASS
-------------------------------------

**Corresponding name in the table**:
EFFECTIVE_TERRAIN_ROUGHNESS_CLASS

**Description**: Effective terrain class from the effective terrain
roughness length (z0). The classes are defined according to the
Davenport lookup Table (cf Table 5 in Stewart and Oke, 2012)

**Method**: The Davenport definition defines a class for a unique z0
value (instead of a range). Then there is no definition of the z0 range
corresponding to a certain class. We have arbitrarily defined the
boundary between two classes as the arithmetic average between the z0
values of each class.

**Warning**: The choice for the interval boundaries has been made
arbitrarily. A definition of the interval based on a log profile of
class = f(z0) could lead to different results (especially for classes 3,
4 and 5).

**References**:

- Stewart, Ian D., and Tim R. Oke. “`Local climate zones for urban
  temperature studies <https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1>`_.”
  Bulletin of the American Meteorological Society 93, no. 12 (2012):
  1879-1900.

UTRF_AREA_FRACTION
----------------------

Several indicators result from this choice. They are identical as the
ones described in the “`UTRF output table <UTRF-classification.html#utrf-at-rsu-scale-based-on-building-area>`_”.

UTRF_FLOOR_AREA_FRACTION
----------------------------

Several indicators result from this choice. They are identical as the
ones described in the `UTRF output table <UTRF-classification.html#utrf-at-rsu-scale-based-on-building-floor-area>`_.

LCZ_PRIMARY
---------------

Several indicators result from this choice. The LCZ are `calculated at
the RSU scale <LCZ-classification.html>`_ and then aggregated to the most represented LCZ at grid cell scale. The
LCZ fraction corresponding to each type is also an output, as well as
the uniqueness. The resulting indicator is the following:

**Corresponding name in the table**: LCZ_UNIQUENESS_VALUE

**Description**: Indicates how unique is the LCZ type attributed to a
given grid cell

**Range of values**: [0, 1] - the higher the value, the more unique is
the LCZ type

**Method**: | Area_First_LCZ - Area_Second_LCZ | / (Area_Second_LCZ + Area_First_LCZ)

URBAN_SPRAWL_AREAS
----------------------

This does not lead to the calculation of a given indicator but to the
creation of a new output file called (urban) `sprawl_areas <Sprawl-areas.html>`_.
It computes the urban sprawl areas layer from the grid cell geometries,
each containing the fraction area of each LCZ type. A sprawl geometry is
the union of grid cells having a majority of urban LCZs types (LCZ1 to
LCZ10 plus impervious or bare rock soil type - LCZE or LCZ105). Note
that: - for being a sprawl area, at least two urban grid cells should be
adjacent, - any non urban grid cell located within a sprawl area will be
considered within the sprawl area

URBAN_SPRAWL_DISTANCES
--------------------------

Two columns are created when this key word is in the list of indicators:
First indicator: **Corresponding name in the table**: SPRAWL_INDIST

**Description**: Compute the distance of each grid cell located inside
the urban sprawl to the urban sprawl boundaries.

**Range of values**: [0, +inf]

**Method**: distance from cell centroid to sprawl boundaries

Second indicator: **Corresponding name in the table**: SPRAWL_OUTDIST

**Description**: Compute the distance of each grid cell located outside
the urban sprawl to the urban sprawl boundaries.

**Range of values**: [0, +inf]

**Method**: distance from cell centroid to sprawl boundaries

URBAN_SPRAWL_COOL_DISTANCES
-------------------------------

The method to determine a cool area is quite similar as the one used for
determining urban areas. A cool geometry is the union of grid cells
having a majority of non-urban LCZs types (LCZA - LCZ 101 - to LCZG -
LCZ 107 - except impervious or bare rock soil type - LCZE or LCZ105).
Note that for being a cool area: - there should be at least 300 m of
adjacent cool grid cells in all directions - the cool grid cells should
be located within a sprawl area

**Corresponding name in the table**: SPRAWL_COOL_INDIST

**Description**: Compute the distance of each grid cell located inside
the urban sprawl to a cool area.

**Range of values**: [0, +inf]

**Method**: distance from cell centroid to cool area boundaries

BUILDING_HEIGHT_DISTRIBUTION
--------------------------------

**Corresponding name in the table**: ROOF_FRACTION_DISTRIBUTION_Hx_y

**Description**: Compute the fraction of building of a cell belonging to
the height interval [x, y[ meters.

**Range of values**: [0, 1]

**Method**: SUM(Bu_Area_Hx_y) / SUM(Bu_Area)

STREET_WIDTH
----------------

**Corresponding name in the table**: STREET_WIDTH

**Description**: Average street width needed by models such as TARGET

**Method**: A simple approach based on the street canyons assumption is
used for the calculation. The area weighted mean building height is
divided by the aspect ratio (defined as the sum of facade area within a
given RSU area divided by the area of free surfaces of the given RSU
(not covered by buildings).

→ Cell_avg_height_roof_area_weighted / Cell_aspect_ratio

------------------------------------------------------------


PROJECTED_FACADE_DENSITY_DIR
--------------------------------------------

**Corresponding name in the table**: PROJECTED_FACADE_AREA_DISTRIBUTION_Dw_z

**Description**: Distribution of projected facade area within a RSU per direction of analysis (ie. wind or sun direction - the angle range being from ``w`` to ``z`` within the range [0, 180[°).

**Method**: Each line representing the facades of a building are projected in order to be perpendicular to the median of each angle range of analysis. Only free facades are considered. The projected surfaces are then summed within each direction of analysis. The analysis is only performed within the [0, 180[° range since the projected facade of a building is identical for opposite directions.

**Warning**: To simplify the calculation, z0 is considered as equal for a given orientation independently of the direction. This assumption is right when the RSU do not split buildings but could slightly overestimate the results otherwise (the projected facade area is actually overestimated in one direction but OK in the opposite direction).


HEIGHT_OF_ROUGHNESS_ELEMENTS
----------------

**Corresponding name in the table**: GEOM_AVG_HEIGHT_ROOF

**Description**: RSU geometric mean of the building roof heights.

**Method**: ``EXP(SUM(LOG(Bu_ROOF_HEIGHT)) / NB_Building)``

------------------------------------------------------------


BUILDING_DIRECTION
----------------
Three columns are created when this key word is in the list of indicators:
First indicator: **Corresponding name in the table**: MAIN_BUILDING_DIRECTION

**Description**: Main direction of the buildings contained in a RSU.

**Method**: The building direction distribution is calculated according to the length of the building SMBR sides (width and length). The [0, 180]° angle range is splitted into ``nb_directions`` angle ranges. Then the length of each SMBR building side is attributed to one of these ranges according to the side direction. Within each angle range, the total length of SMBR sides are summed and then the mode of the distribution is taken as the main building direction.

Second indicator: **Corresponding name in the table**: BUILDING_DIRECTION_UNIQUENESS

**Description**: Indicates how unique is the RSU main building direction.

**Range of values**: [0, 1] - the higher the value, the more unique is the main building direction

**Method**: ``| Length_First_Dir - Length_Second_Dir | / (Length_Second_Dir + Length_First_Dir)``

Third indicator: **Corresponding name in the table**: BUILDING_DIRECTION_EQUALITY

**Description**: Indicates how equal is the RSU building direction distribution (having ``nb_direction`` directions of analysis).

**Method**: From the building direction distribution created in the ``MAIN_BUILDING_DIRECTION`` indicator calculation, an indicator of equality of the distribution is calculated:

→ ``Sum(Min(1/nb_direction, length_dir_i/length_all_dir))``

**Range of values**: [``nb_direction``, ``1``] - the higher the value the most equal is the distribution


BUILDING_NUMBER
----------------
Two columns are created when this key word is in the list of indicators:
First indicator: **Corresponding name in the table**: BUILDING_NUMBER_DENSITY

**Description**: RSU number of building density.

**Method**: ``Nb_Building / Rsu_Area``

Second indicator: **Corresponding name in the table**: BLOCK_NUMBER_DENSITY

**Description**: RSU number of block density.

**Method**: ``Nb_Block / Rsu_Area``


.. include:: _Footer.rst
