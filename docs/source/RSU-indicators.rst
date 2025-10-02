RSU indicators
==============

The table ``rsu_indicators`` contains the RSU identifier (id_rsu) and a certain number of indicators described below. Note that some indicators are generic and thus are described only once with x, y, w, z replacing a generic information (for example ``x_FRACTION`` is the fraction of input layer x within a RSU). Thus if you look for WATER_FRACTION, look for x_FRACTION, etc.

AREA
----

**Description**: RSU's area.

**Method**: Area of the RSU footprint

AREA_FRACTION_x
---------------

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

AREA_FRACTION_x_LCZ
--------------------

**Description**: Footprint fraction within the RSU of _type \x_ building useful for determining LCZ types. There are too many `building types <Building-layer.html#type-and-main_use-column-values>`_ to have a fraction of each one, thus several building types are gathered within a broader _type \x_. In the following, the key is the _type \x_ and the list of values are the building types described `here <Building-layer.html#type-and-main_use-column-values>`_:

* "undefined_lcz"     : ["building", "undefined"],
* "light_industry_lcz": ["industrial", "factory", "warehouse", "port", "manufacture"],
* "commercial_lcz"    : ["commercial", "shop", "retail", "port",
                       "exhibition_centre", "cinema"],
* "heavy_industry_lcz": ["refinery"],
* "residential_lcz"   : ["house", "detached", "bungalow", "farm", "apartments", "barracks",
                       "abbey", "condominium", "villa", "dormitory", "sheltered_housing",
                       "workers_dormitory", "terrace", "residential", "cabin"]

**Method**: ``SUM(Bu_AREA of type X) / RSU_Area``

ASPECT_RATIO
------------

**Description**: Aspect ratio such as defined by Stewart et Oke (2012): mean height-to-width ratio of street canyons (LCZs 1-7), building spacing (LCZs 8-10), and tree spacing (LCZs A - G).

**Method**: A simple approach based on the street canyons assumption is used for the calculation. The sum of facade area within a given RSU area is divided by the area of free surfaces of the given RSU (not covered by buildings).

→ ``RSU_free_external_facade_density / (1 - RSU_building_fraction)``

AVG_HEIGHT_ROOF
---------------

**Description**: Mean building’s roof height within the RSU

**Method**: ``SUM(Bu_Roof_Height) / Nb_Building``

AVG_HEIGHT_ROOF_AREA_WEIGHTED
-----------------------------

**Description**: Mean building’s roof height within the RSU (the building heights being weighted by the building areas)

**Method**: ``SUM(Bu_Roof_Height * Bu_Area) / SUM(Bu_Area)``

AVG_MINIMUM_BUILDING_SPACING
----------------------------

**Description**: RSU average minimum distance between buildings.

**Method**: ``SUM(Minimum_Building_Spacing) / Nb_Building``

AVG_NB_LEV_AREA_WEIGHTED
------------------------

**Description**: RSU average number of levels per building (the building levels being weighted by the building areas).

**Method**: ``SUM(Number_Building_Level * Bu_Area) / SUM(Bu_Area)``

AVG_NUMBER_BUILDING_NEIGHBOR
----------------------------

**Description**: RSU average number of neighbors per building.

**Method**: ``SUM(Number_Building_Neighbors) / Nb_Building``

AVG_VOLUME
----------

**Description**: RSU average building volume.

**Method**: ``SUM(Bu_Volume) / Nb_Building``

BUILDING_DIRECTION_EQUALITY
---------------------------

**Description**: Indicates how equal is the RSU building direction distribution (having ``nb_direction`` directions of analysis).

**Method**: From the building direction distribution created in the ``MAIN_BUILDING_DIRECTION`` indicator calculation, an indicator of equality of the distribution is calculated:

→ ``Sum(Min(1/nb_direction, length_dir_i/length_all_dir))``

**Range of values**: [``nb_direction``, ``1``] - the higher the value the most equal is the distribution

BUILDING_DIRECTION_UNIQUENESS
-----------------------------

**Description**: Indicates how unique is the RSU main building direction.

**Range of values**: [0, 1] - the higher the value, the more unique is the main building direction

**Method**: ``| Length_First_Dir - Length_Second_Dir | / (Length_Second_Dir + Length_First_Dir)``

BUILDING_FLOOR_AREA_DENSITY
---------------------------

**Description**: Density of building floor areas within the RSU.

**Method**: ``SUM(Bu_FLOOR_AREA) / RSU_Area``

BUILDING_FRACTION_LCZ
---------------------

**Description**: Building fraction used for the LCZ classification (by default, total building fraction).

**Method**: ``SUM(Bu_Area without superimposition + Bu_Area superimposed by high_vegetation) / RSU_Area``

BUILDING_NUMBER_DENSITY
-----------------------

**Description**: RSU number of building density.

**Method**: ``Nb_Building / Rsu_Area``

BUILDING_TOTAL_FRACTION
-----------------------

**Description**: Total fraction of building within the RSU (covered and not covered by high vegetation).

**Method**: ``SUM(Bu_Area without superimposition + Bu_Area superimposed by high_vegetation) / RSU_Area``

BUILDING_VOLUME_DENSITY
-----------------------

**Description**: Density of building volumes within the RSU.

**Method**: ``SUM(Bu_VOLUME) / NB_Building``

EFFECTIVE_TERRAIN_ROUGHNESS_CLASS
---------------------------------

**Description**: Effective terrain class from the effective terrain roughness length (z0). The classes are defined according to the Davenport lookup Table (cf Table 5 in Stewart and Oke, 2012)

**Method**: The Davenport definition defines a class for a unique z0 value (instead of a range). Then there is no definition of the z0 range corresponding to a certain class. We have arbitrarily defined the boundary between two classes as the arithmetic average between the z0 values of each class.

**Warning**: The choice for the interval boundaries has been made arbitrarily. A definition of the interval based on a log profile of class = f(z0) could lead to different results (especially for classes 3, 4 and 5).

**References**:

- Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.

EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH
----------------------------------

**Description**: Effective terrain roughness length (z0).

**Method**: The method for z0 calculation is based on the Hanna and Britter (2010) procedure (see equation (17) and examples of calculation p. 156 in the corresponding reference). The ``rsu_projected_facade_area_distribution_Hx_y_Dw_z`` is used to calculate the mean projected facade density (considering all directions) and ``z0`` is then obtained multiplying the resulting value by the ``rsu_geometric_mean_height``.

**Warning**: The calculation of z0 is only performed for angles included in the range [0, 180[°. To simplify the calculation, z0 is considered as equal for a given orientation independently of the direction. This assumption is right when the RSU do not split buildings but could slightly overestimate the results otherwise (z0 is actually overestimated in one direction but OK in the opposite direction).

**References**:

- Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
- Hanna, Steven R., and Rex E. Britter. Wind flow and vapor cloud dispersion at industrial and urban sites. Vol. 7. John Wiley & Sons, 2010.

'FLOOR_AREA_FRACTION_X'
-----------------------

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

**Method**: ``SUM(Bu_FLOOR_AREA of type X) / RSU_Area``

FREE_EXTERNAL_FACADE_DENSITY
----------------------------

**Description**: Sum of all building free facades (roofs are excluded) included in a RSU, divided by the RSU area.

**Method**: ``SUM((1 - Bu_Contiguity) * Bu_TotalFacadeLength * HEIGHT_WALL) / RSU_Area``

GEOM_AVG_HEIGHT_ROOF
--------------------

**Description**: RSU geometric mean of the building roof heights.

**Method**: ``EXP(SUM(LOG(Bu_ROOF_HEIGHT)) / NB_Building)``

GROUND_LINEAR_ROAD_DENSITY
--------------------------

**Description**: Road linear density, having a ZINDEX = 0, within the RSU.

**Method**: Linear of road at zindex = 0 within a RSU divided by the RSU area

GROUND_SKY_VIEW_FACTOR
----------------------

**Description**: RSU ground Sky View Factor such as defined by Stewart et Oke (2012): ratio of the amount of sky hemisphere visible from ground level to that of an unobstructed hemisphere. In our case, only buildings are considered as obstructing the atmosphere.

**Method**: The calculation is based on the `ST_SVF <http://www.h2gis.org/docs/dev/ST_SVF/>`_ function of H2GIS using only buildings as obstacles and with the following parameters: ray length = 100, number of directions = 60. Using a uniform grid mesh of 10 m resolution, the SVF obtained has a standard deviation of the estimate of 0.03 when compared with the most accurate method (according to `Bernard et al. (2018) <https://dx.doi.org/10.3390/cli6030060>`_).

Using a grid of regular points, the density of points used for the calculation actually depends on building density (higher the building density, lower the density of points). To avoid this phenomenon and have the same density of points per free ground surface, we use an H2GIS function to distribute randomly points within free surfaces (ST_GeneratePoints). This density of points is set by default to 0.008, based on the median of `Bernard et al. (2018) <https://dx.doi.org/10.3390/cli6030060>`_ dataset.

**References**:

- Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
- Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. "Sky View Factor Calculation in Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools." Climate, MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.

HIGH_VEGETATION_FRACTION_LCZ
----------------------------

**Description**: High vegetation fraction used for the LCZ classification (by default, total high_vegetation fraction).

**Method**: ``SUM(High_veg_Area without superimposition + High_veg_Area superimposing all other layers) / RSU_Area``

HIGH_VEGETATION_IMPERVIOUS_FRACTION_URB
---------------------------------------

**Description**: Fraction of high vegetation covering impervious layer such as defined for the UTRF classification.

**Method**: ``SUM(Imperv_Area with and without superimposition + Road_Area with and without superimposition) / RSU_Area``

HIGH_VEGETATION_PERVIOUS_FRACTION_URB
-------------------------------------

**Description**: Fraction of high vegetation covering pervious layer such as defined for the UTRF classification.

**Method**: ``SUM(Perv_Area with and without superimposition + Road_Area with and without superimposition) / RSU_Area``

IMPERVIOUS_FRACTION_LCZ
-----------------------

**Description**: Impervious fraction used for the LCZ classification (by default, total impervious fraction).

**Method**: ``SUM(Imperv_Area with and without superimposition + Road_Area with and without superimposition) / RSU_Area``

IMPERVIOUS_FRACTION_URB
-----------------------

**Description**: Impervious fraction used for the UTRF classification.

**Method**: ``SUM(Imperv_Area with and without superimposition + Road_Area with and without superimposition) / RSU_Area``

LOW_VEGETATION_FRACTION_LCZ
---------------------------

**Description**: Low vegetation fraction used for the LCZ classification.

**Method**: ``SUM(Low_veg_Area without superimposition) / RSU_Area``

LOW_VEGETATION_FRACTION_URB
---------------------------

**Description**: Low vegetation fraction used for the UTRF classification.

**Method**: ``SUM(Low_veg_Area without superimposition) / RSU_Area``

MAIN_BUILDING_DIRECTION
-----------------------

**Description**: Main direction of the buildings contained in a RSU.

**Method**: The building direction distribution is calculated according to the length of the building SMBR sides (width and length). The [0, 180]° angle range is splitted into ``nb_directions`` angle ranges. Then the length of each SMBR building side is attributed to one of these ranges according to the side direction. Within each angle range, the total length of SMBR sides are summed and then the mode of the distribution is taken as the main building direction.

NON_VERT_ROOF_AREA_Hx_y
-----------------------

**Description**: The non-vertical (horizontal and tilted) roofs area is calculated within each vertical layer of a RSU (the bottom of the layer being at ``x`` meters from the ground while the top is at ``y`` meters).

**Method**: The calculation is based on the assumption that all buildings having a roof height higher than the wall height have a gable roof (the other being horizontal). Since the direction of the gable is not taken into account for the moment, we consider that buildings are square in order to limit the potential calculation error (otherwise a choice should have been made to locate the line corresponding to the top of the roof).

NON_VERT_ROOF_DENSITY
---------------------

**Description**: RSU surface density of non-vertical roofs (horizontal and tilted roofs).

**Method**: ``SUM(Non_vert_roof_area_Hx_y) / RSU_Area``

PERVIOUS_FRACTION_LCZ
---------------------

**Description**: Pervious fraction used for the LCZ classification.

**Method**: ``SUM(Low_veg_Area with and without high vegetation superimposition + Water_Area with and without high vegetation superimposition + High_veg_Area without superimposition) / RSU_Area``

PROJECTED_FACADE_AREA_DISTRIBUTION_Hx_y_Dw_z
--------------------------------------------

**Description**: Distribution of projected facade area within a RSU per vertical layer (the height being from ``x`` to ``y``) and per direction of analysis (ie. wind or sun direction - the angle range being from ``w`` to ``z`` within the range [0, 180[°).

**Method**: Each line representing the facades of a building are projected in order to be perpendicular to the median of each angle range of analysis. Only free facades are considered. The projected surfaces are then summed within each layer and direction of analysis. The analysis is only performed within the [0, 180[° range since the projected facade of a building is identical for opposite directions. Thus because we consider all facades of a building in the calculation (facades upwind but also downwind), the final result is divided by 2.

**Warning**: To simplify the calculation, z0 is considered as equal for a given orientation independently of the direction. This assumption is right when the RSU do not split buildings but could slightly overestimate the results otherwise (the projected facade area is actually overestimated in one direction but OK in the opposite direction).

ROAD_DIRECTION_DISTRIBUTION_H0_Dw_z
-----------------------------------

**Description**: Distribution of road length within a RSU per direction of analysis (ie. wind or sun direction - the angle range being from ``w`` to ``z`` within the range [0, 180[°). Note that by default, only roads located at ground level are considered for the calculation (z_index = 0).

**Method**: The direction of each segment of road is calculated. The percentage of linear of road in each range of direction is then calculated (a range is defined - default 30°) for directions included in [0, 180[°.

ROAD_FRACTION_URB
-----------------

**Description**: Road fraction used for the UTRF classification.

**Method**: ``SUM(Road_Area with and without superimposition) / RSU_Area``

ROOF_FRACTION_DISTRIBUTION_HX_Y
-------------------------------

**Description**: Distribution of (only) horizontal roof area fraction for each layer of the urban canopy (the height being from ``x`` to ``y``).

**Method**: Divide the area of roofs being within ``x`` and ``y`` meters from ground level by the RSU area. If the height roof and height wall differ for a given building, take the average value of the building height. Note that this process first cut the buildings according to RSU in order to calculate the exact distribution within a RSU per vertical layer.

STD_HEIGHT_ROOF
---------------

**Description**: Variability of the building’s roof height within the RSU

**Method**: By default, the indicator of variability is the Standard Deviation (STD) defined as:

→ ``SUM((Bu_Roof_Height - AVG_HEIGHT_ROOF)^2)) / NB_Building``

STD_HEIGHT_ROOF_AREA_WEIGHTED
-----------------------------

**Description**: Variability of the building’s roof height within the RSU (the building heights being weighted by the building areas)

**Method**: By default, the indicator of variability is the Standard Deviation (STD) defined as:

→ ``SUM(Bu_Area*(Bu_Roof_Height - AVG_HEIGHT_ROOF_AREA_WEIGHTED)^2)) / SUM (Bu_Area)``

VEGETATION_FRACTION_URB
-----------------------

**Description**: Vegetation fraction used for the UTRF classification.

**Method**: ``SUM(High_veg_Area without superimposition + High_veg_Area superimposing all other layers + Low_veg_area without superimposition) / RSU_Area``

VERT_ROOF_AREA_Hxx_xx
---------------------

**Description**: Vertical roofs area is calculated within each vertical layer of a RSU (the bottom of the layer being at ``x`` meters from the ground while the top is at ``y`` meters).

**Method**: The calculation is based on the assumption that all buildings having a roof height higher than the wall height have a gable roof (the other being horizontal). Since the direction of the gable is not taken into account for the moment, we consider that buildings are square in order to limit the potential calculation error (otherwise a choice should have been made to locate the line corresponding to the top of the roof).

VERT_ROOF_DENSITY
-----------------

**Description**: RSU surface density of vertical roofs.

**Method**: ``SUM(Vert_roof_area_Hx_y) / RSU_Area``

WATER_FRACTION_LCZ
------------------

**Description**: Water fraction used for the LCZ classification.

**Method**: ``SUM(Water_Area with and without superimposition) / RSU_Area``

X_FRACTION
----------

**Description**: Fraction of the X input layer within the RSU which is not superimposed with any other Y input layer (note that the vegetation layer is split into a low_vegetation layer and a high_vegetation layer). Superimposed layer fraction are calculated in 'X_Y_FRACTION' when they are physically relevant (e.g. high_vegetation above impervious). When not relevant (e.g. low_vegetation and impervious), only one of the intersected layers is kept for fraction calculation. By default, superimposition is considered only between high_vegetation and all other layers and otherwise intersected layers are kept in the following priority order: "water", "building", "high_vegetation", "low_vegetation", "road", "impervious".

**Method**: ``SUM(X_Area without superimposition) / RSU_Area``

X_Y_FRACTION
------------

**Description**: Fraction of the X input layer within the RSU which superimposed the Y input layer (note that the vegetation layer is split into a low_vegetation layer and a high_vegetation layer). Superimposed layer fraction are calculated when they are physically relevant (e.g. high_vegetation above impervious). By default, superimposition is considered only between high_vegetation and all other layers and otherwise intersected layers.

**Method**: ``SUM(X_Area superimposing Y) / RSU_Area``

------------------------------------------------------

.. include:: _Footer.rst
