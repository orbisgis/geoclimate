LCZ Classification
==================

.. Note::

   To get more informations about the method used for LCZ classification, please see the corresponding article on the `References page <References.html>`_. Note that in the manuscript, the uniqueness indicator was only valid for some of the LCZ types. It is now valid for all classes. 

GeoClimate computes the Local Climate Zones (LCZ) at the RSU's scale.

The LCZ, introduced by *Stewart* & *Oke* (`2012 <http://journals.ametsoc.org/doi/abs/10.1175/BAMS-D-11-00019.1>`_,
`2014 <http://onlinelibrary.wiley.com/doi/10.1002/joc.3746/abstract>`_), is a classification scheme used to segment the climate areas of cities (and other).


Methodology
-----------

A LCZ type is assigned to a RSU. This "assignment" is performed according to 14 indicators:

- 7 indicators usually used for LCZ classification:
    - :math: `SVF` : sky_view_factor
    - :math: `H/W` : aspect_ratio
    - :math: `F_B` : building_surface_fraction
    - :math: `F_I` : impervious_surface_fraction
    - :math: `F_P` : pervious_surface_fraction
    - :math: `H_r` : height_of_roughness_elements
    - :math: `z_0` : terrain_roughness_class

- 7 additionnal indicators:
    - :math: `F_B` : all vegetation fraction
    - :math: `F_W` : water fraction
    - :math: `F_{HV}` : fraction of all land (except building, water and impervious) being high vegetation
    - :math: `F_{IND/B}` : fraction of building being industrial
    - :math: `F_{LLR/B}` : fraction of building being large low-rise
    - :math: `F_{RES/B}`  fraction of building being residential
    - :math: `N_{lev}` : area weighted average number of building levels

The classification to a given type follows the procedure illustrated on the Figure below.
.. figure:: /_static/image/LczProcedure.png

Each land cover type LCZ is classified according to a given set of indicators and threshold. The same apply for LCZ built types 8 and 10. A unique LCZ type (``LCZ_PRIMARY``) is associated to each RSU and ``LCZ_UNIQUENESS_VALUE`` is calculated to characterize the degree of certainty of the classified RSU. This calculation is unique per LCZ type:

- LCZA: :math: `0.25 * (F_{B_{LC-max}} - F_B) / F_{B_{LC-max}} + 0.25 * (H/W_{B_{LC-max}} - H/W) / H/W_{B_{LC-max}} + 0.5 * (F_{HV} - F_{B_{LC-max}) / (1 - F_{B_{LC-max})`
- LCZB: :math: `0.25 * (F_{B_{LC-max}} - F_B) / F_{B_{LC-max}} + 0.25 * (H/W_{B_{LC-max}} - H/W) / H/W_{B_{LC-max}} + 0.25 * (F_{AV} - F_{B_{LC-max}) / (1 - F_{B_{LC-max}) + 0.25 * ((F_{HV/AV_{max}} - F_{HV/AV_{min}}) / 2 - |F_{HV/AV} - (F_{HV/AV_{max}} - F_{HV/AV_{min}}) / 2|) / ((F_{HV/AV_{max}} - F_{HV/AV_{min}}) / 2)`
- LCZD: :math: `0.25 * (F_{B_{LC-max}} - F_B) / F_{B_{LC-max}} + 0.25 * (H/W_{B_{LC-max}} - H/W) / H/W_{B_{LC-max}} + 0.5 * (F_{LV} - F_{B_{LC-max}) / (1 - F_{B_{LC-max})`
- LCZE: :math: `0.25 * (F_{B_{LC-max}} - F_B) / F_{B_{LC-max}} + 0.25 * (H/W_{B_{LC-max}} - H/W) / H/W_{B_{LC-max}} + 0.5 * (F_{I} - F_{B_{LC-max}) / (1 - F_{B_{LC-max})`
- LCZG: :math: `0.25 * (F_{B_{LC-max}} - F_B) / F_{B_{LC-max}} + 0.25 * (H/W_{B_{LC-max}} - H/W) / H/W_{B_{LC-max}} + 0.5 * (F_{W} - F_{B_{LC-max}) / (1 - F_{B_{LC-max})`
- LCZ8: :math: `0.25 * (F_{LLR/B} - F_{LLR/B_{min}}) / (1 - F_{LLR/B_{min}})`
- LCZ10: :math: `0.25 * (F_{IND/B} - F_{IND/B_{min}}) / (1 - F_{IND/B_{min}})`
where 
:math: `F_{B_{LC-max}}` and :math: `AR_{B_{LC-max}}` are the maximum value accepted for a land cover, for building fraction and aspect ratio, respectively
:math: `F_{HV/AV_{min}}` and :math: `F_{HV/AV_{max}}` are the minimum and maximum :math: `F_{HV/AV}` thresholds for being LCZ102 


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
- **LCZ_UNIQUENESS_VALUE**: Indicates certainty of the primary LCZ type (closer to 1 means more certain). Defines as (DISTANCE LCZ_PRIMARY - DISTANCE LCZ_SECONDARY) / (DISTANCE LCZ_PRIMARY - DISTANCE LCZ_SECONDARY)``
- **LCZ_EQUALITY_VALUE**: Indicates whether the RSU's LCZ could be any LCZ type (closer to 0 means more certain).



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
     - Minimum distance to each LCZ. Only valid for LCZ1 to LCZ7 and LCZ9.
   * - ``LCZ_UNIQUENESS_VALUE``
     - double precision
     - Indicates how unique (and thus how certain) is the attributed LCZ type.
   * - ``LCZ_EQUALITY_VALUE``
     - double precision
     - Indicates whether the LCZ type of a RSU could be any LCZ type. Based on distance calculation in the 7 dimension space, thus only valid for LCZ1 to LCZ7 and LCZ9.


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
