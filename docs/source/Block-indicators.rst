Block Indicators
================

The table ``block_indicators`` contains the block identifier (`id_block`), the identifier of the RSU it belongs to (`id_rsu`), 
and a set of indicators described below.

Indicators
----------

AREA
~~~~

**Description**: Area of the block footprint.

**Method**: Sum of building areas within the block.


AVG_HEIGHT_ROOF_AREA_WEIGHTED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Mean building roof height within a block, weighted by building area.

**Method**:  
SUM(Building Wall Height * Building Area) / SUM(Building Area)


BUILDING_DIRECTION_EQUALITY
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Measures how equal the block's building direction distribution is, considering `nb_direction` directions.

**Method**:  
From the building direction distribution (computed in `MAIN_BUILDING_DIRECTION`), the equality indicator is:  
Sum of Min(1/nb_direction, length_dir_i / length_all_dir)

**Range of values**: [1/nb_direction, 1] — the higher, the more equal the distribution.


BUILDING_DIRECTION_UNIQUENESS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Measures how unique the RSU main building direction is.

**Method**:  
``|Length_First_Dir - Length_Second_Dir|`` / (``Length_First_Dir`` + ``Length_Second_Dir``)

**Range of values**: [0, 1] — closer to 1 means more unique main building direction.


CLOSINGNESS
~~~~~~~~~~~

**Description**: Indicates if a block has a large closed courtyard. Useful for urban fabric classification.

**Method**:  
Difference between ``st_holes`` at block scale and sum of ``st_holes`` at building scale.

**Warning**:  
This method does not identify nearly closed blocks (e.g., 99% closed), which could be relevant for ventilation studies.

**References**:

- Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018).
  ``A geoprocessing framework to compute urban indicators: The MApUCE tools chain``, Urban Climate, 24, 153-174.

- Tornay, N., Schoetter, R., Bonhomme, M., Faraut, S., & Masson, V. (2017).
  ``GENIUS: A methodology to define a detailed description of buildings for urban climate and building energy consumption simulations``, Urban Climate, 20, 75-93.


FLOOR_AREA
~~~~~~~~~~

**Description**: Total floor area within the block.

**Method**: Sum of building floor areas.


HOLE_AREA_DENSITY
~~~~~~~~~~~~~~~~~

**Description**: Density of holes (courtyards) within a block.

**Method**: Block courtyard area / block area


MAIN_BUILDING_DIRECTION
~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Main orientation of buildings within the block (from North, clockwise).

**Method**:  
- Calculate the minimum bounding rectangle for each building.  
- Assign each side to a direction range (default: every 15° in [0°, 180°[ from North clockwise).  
- Sum side lengths per direction for all buildings in the block.  
- The mode (most frequent direction) is the main building direction.


NET_COMPACTNESS
~~~~~~~~~~~~~~~

**Description**: Net block compactness, ratio of free external facade area to building volume.

**Method**:  
SUM((Building Contiguity * Building Perimeter + Building Hole Perimeter) * Building Wall Height) / SUM(Building Volume)


STD_HEIGHT_ROOF_AREA_WEIGHTED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Variability of building roof heights in the block, weighted by building area.

**Method**:  
Standard deviation:  
SUM(Building Area * (Building Wall Height - AVG_HEIGHT_ROOF_AREA_WEIGHTED)^2) / SUM(Building Area)


VOLUME
~~~~~~

**Description**: Total volume of buildings composing the block.

**Method**: Sum of building volumes within the block.

-----------------------------------------------------
.. include:: _Footer.rst
