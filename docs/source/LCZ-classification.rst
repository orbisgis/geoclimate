LCZ Classification
==================

.. Note::

   To get more informations about the method used for LCZ classification, please see the corresponding article on the `References page <References.html>`_.

GeoClimate computes the Local Climate Zones (LCZ) at the RSU's scale.

The LCZ, introduced by *Stewart* & *Oke* (`2012 <http://journals.ametsoc.org/doi/abs/10.1175/BAMS-D-11-00019.1>`_,
`2014 <http://onlinelibrary.wiley.com/doi/10.1002/joc.3746/abstract>`_), is a classification scheme used to segment the climate areas of cities (and other).


Methodology
-----------

A LCZ type is assigned to a RSU. This "assignment" is performed according to 14 indicators:
- 7 indicators usually used for LCZ classification:
    - ``sky_view_factor``
    - ``aspect_ratio``
    - ``building_surface_fraction``
    - ``impervious_surface_fraction``
    - ``pervious_surface_fraction``
    - ``height_of_roughness_elements``
    - ``terrain_roughness_class``
- 7 additionnal indicators:
    - ``all vegetation fraction``
    - ``water fraction``
    - ``fraction of all land (except building, water and impervious) being high vegetation``
    - ``fraction of building being industrial``
    - ``fraction of building being large low-rise``
    - ``fraction of building being residential``
    - ``area weighted average number of building levels``

The classification to a given type follows the procedure illustrated on the Figure below.
.. figure:: /_static/image/LczProcedure.png

Each land cover type LCZ is classified according to a given set of indicators and threshold. The same apply for LCZ built types 8 and 10. A unique LCZ type (``LCZ_PRIMARY``) is associated to each RSU and ``LCZ_UNIQUENESS_VALUE`` is calculated to characterize the degree of certainty of the classified RSU. This calculation is unique per LCZ type:
- 



For the rest of the built types, their classification is based on the usual 7 indicators used to define LCZ classes. 
The method to find the most appropriate LCZ type for a given RSU is based on the minimum distance (``MIN_DISTANCE``) to each LCZ in the 7-dimensional space.
To calculate this distance, each dimension is normalized according to the mean and standard deviation (or median and absolute median deviation) of the interval values.

Some indicators may be more important (or reliable) than others for LCZ identification. To account for this, a map of weights may be applied to multiply the distance contribution of each indicator.
The default values are 4 for sky view factor, 3 for aspect ratio , 8 for building surface fraction, 0 for impervious surface fraction , 0 for pervious surface fraction , 6 for height of roughness elements,
and 0.5 for terrain roughness class.

The distance of each RSU to each LCZ type is calculated in the normalized interval space.
The two LCZ types closest to the RSU indicators (``LCZ_PRIMARY`` and ``LCZ_SECONDARY``) are assigned to this RSU.

Three indicators describe the degree of certainty of the allocated LCZ class:

- **MIN_DISTANCE**: Distance from a RSU point to the closest LCZ type (lower means more certain ``LCZ_PRIMARY``).
- **LCZ_UNIQUENESS_VALUE**: Indicates certainty of the primary LCZ type (closer to 1 means more certain).
- **LCZ_EQUALITY_VALUE**: Indicates whether the RSU's LCZ could be any LCZ type (closer to 0 means more certain).

Note:
This method is valid mostly for built LCZ types. For LCZ types 8, 10 and all land-cover LCZ types, the method differs and will be detailed in the forthcoming article on the `References page <References.html>`_.
For these LCZ types, ``LCZ_SECONDARY``, ``MIN_DISTANCE``, ``LCZ_UNIQUENESS_VALUE``, and ``LCZ_EQUALITY_VALUE`` are set to null.

The source code is available `at <https://github.com/orbisgis/geoclimate/blob/v1.0.0-RC1/geoindicators/src/main/groovy/org/orbisgis/orbisprocess/geoclimate/geoindicators/TypologyClassification.groovy>`_


Output LCZ layer
----------------

.. list-table::
   :header-rows: 1
   :widths: 20 20 40

   * - Field name
     - Field type
     - Definition
   * - ``ID_RSU``
     - integer
     - RSU's unique id
   * - ``LCZ_PRIMARY``
     - integer
     - Main LCZ type
   * - ``LCZ_SECONDARY``
     - integer
     - Secondary LCZ type
   * - ``MIN_DISTANCE``
     - double precision
     - Minimum distance to each LCZ
   * - ``LCZ_UNIQUENESS_VALUE``
     - double precision
     - Indicates how unique is the attributed LCZ type. Only valid for LCZ1 to LCZ7 and LCZ9.
       ``LCZ_UNIQUENESS_VALUE = (DISTANCE LCZ_PRIMARY - DISTANCE LCZ_SECONDARY) / (DISTANCE LCZ_PRIMARY - DISTANCE LCZ_SECONDARY)``
   * - ``LCZ_EQUALITY_VALUE``
     - double precision
     - Indicates whether the LCZ type of a RSU could be any LCZ type


LCZ_PRIMARY and LCZ_SECONDARY Column Values
-------------------------------------------

Each LCZ value is encoded using the following type codes.

For each, the LCZ class name and hexadecimal color code (for map styling) are given.
The default .sld file format can be downloaded at `this address <https://github.com/orbisgis/geoclimate/tree/master/geoindicators/src/main/resources/styles>`_.

.. list-table::
   :header-rows: 1
   :widths: 10 40 20

   * - Type
     - Type definition
     - Hexa Color code
   * - ``1``
     - LCZ 1: Compact high-rise
     - ``#8b0101``
   * - ``2``
     - LCZ 2: Compact mid-rise
     - ``#cc0200``
   * - ``3``
     - LCZ 3: Compact low-rise
     - ``#fc0001``
   * - ``4``
     - LCZ 4: Open high-rise
     - ``#be4c03``
   * - ``5``
     - LCZ 5: Open mid-rise
     - ``#ff6602``
   * - ``6``
     - LCZ 6: Open low-rise
     - ``#ff9856``
   * - ``7``
     - LCZ 7: Lightweight low-rise
     - ``#fbed08``
   * - ``8``
     - LCZ 8: Large low-rise
     - ``#bcbcba``
   * - ``9``
     - LCZ 9: Sparsely built
     - ``#ffcca7``
   * - ``10``
     - LCZ 10: Heavy industry
     - ``#57555a``
   * - ``101``
     - LCZ A: Dense trees
     - ``#006700``
   * - ``102``
     - LCZ B: Scattered trees
     - ``#05aa05``
   * - ``103``
     - LCZ C: Bush, scrub
     - ``#648423``
   * - ``104``
     - LCZ D: Low plants
     - ``#bbdb7a``
   * - ``105``
     - LCZ E: Bare rock or paved
     - ``#010101``
   * - ``106``
     - LCZ F: Bare soil or sand
     - ``#fdf6ae``
   * - ``107``
     - LCZ G: Water
     - ``#6d67fd``

------------------

.. include:: _Footer.rst
