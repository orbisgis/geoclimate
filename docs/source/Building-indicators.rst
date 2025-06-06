Building Indicators
===================

The table ``building_indicators`` contains the initial information from the input table ``building``
(`id_build`, `id_source`, `height_wall`, `height_roof`, etc. — cf. `building input table <Building-layer.html>`_),
the identifiers of the block and the RSU it belongs to (`id_rsu`, `id_block`), and a set of building indicators described below.


AREA
~~~~

**Description**: Building's area.

**Method**: Area of the building footprint.


AREA_CONCAVITY
~~~~~~~~~~~~~~

**Description**: Calculates a degree of convexity of a building (based on the building surface).

**Method**: Area / Convex Hull area

**Range of values**: [0, 1] — the closer to 1, the more convex the building.


COMMON_WALL_FRACTION
~~~~~~~~~~~~~~~~~~~~

**Description**: Fraction of facade length (also called “party walls”) shared with other buildings.

**Method**: Shared facade length / total facade length


CONTIGUITY
~~~~~~~~~~

**Description**: Fraction of wall shared with other buildings.

**Method**: Shared wall area / total wall area


FLOOR_AREA
~~~~~~~~~~

**Description**: Building's floor area.

**Method**: Area * Number of levels


FORM_FACTOR
~~~~~~~~~~~

**Description**: Ratio between the building’s area and the square of the external building’s perimeter.

**Method**: Area / (perimeter)^2


LIKELIHOOD_LARGE_BUILDING
~~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Building closeness to a 50 m wide isolated building (where `NUMBER_BUILDING_NEIGHBOR` = 0).

**Method**:
Step 9 of the decision tree in the MaPUCE project manual building typology classification checks whether a building has a horizontal extent larger than 50 m.
This indicator measures the horizontal extent based on the largest side of the building's minimum bounding rectangle.
A logistic function is used to smooth threshold effects (e.g. difference between 49 m and 51 m). Parameters of the logistic function are set based on training data.


MINIMUM_BUILDING_SPACING
~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Closest distance (in meters) from the building to another building.

**Method**: Minimum distance to other buildings within a buffer distance (`bufferDist`), default 100 m.

**Warning**:
- If buildings touch, result = 0.
- If no building is within 100 m, result = 100 m (or the value of `bufferDist`).


NUMBER_BUILDING_NEIGHBOR
~~~~~~~~~~~~~~~~~~~~~~~~

**Description**: Number of neighboring buildings in contact (touching at least at one point).

**Method**: Count of buildings touching the building of interest.


PERIMETER
~~~~~~~~~

**Description**: Building's external perimeter (does not consider courtyards).

**Method**: External building perimeter length.


PERIMETER_CONVEXITY
~~~~~~~~~~~~~~~~~~~

**Description**: Degree of convexity of the building based on the perimeter.

**Method**: Convex Hull perimeter

---------------------------------

.. include:: _Footer.rst
