Road Layer
==========

The road layer represents any kind of roadways.

+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| Name        | Type                          | Constraints                                               | Definition                                                                                                                   |
+=============+===============================+===========================================================+==============================================================================================================================+
| the_geom    | LINESTRING or MULTILINESTRING | X Y dimensions                                            | Geometry                                                                                                                     |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| id_road     | INTEGER                       | Primary Key                                               | Unique Identifier                                                                                                            |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| id_source   | VARCHAR                       | *not null*                                                | Identifier of the feature from the input datasource                                                                          |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| width       | FLOAT                         | *not null*                                                | Width of the road *(expressed in meters)*                                                                                    |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| type        | VARCHAR                       | *not null*; in ``type`` list                              | Type of road                                                                                                                 |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| surface     | VARCHAR                       | in ``surface`` list                                       | The surface value provides information about material composition and/or structure.                                          |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| sidewalk    | VARCHAR                       | *not null*; in (``one``, ``two``, ``no``)                 | Specify if the road has one, two or no sidewalk(s). Default value should be ``no``.                                          |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| zindex      | INTEGER                       | *not null* ; ≥ -4 ; ≤ 4                                   | Defines the position relative to ground. 0: on ground, 1–4: above ground, -4–-1: underground.                                |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| crossing    | VARCHAR                       | ``bridge``, ``crossing``, or ``null``                     | Indicates if the road is located on a bridge or is crossing another path.                                                    |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| maxspeed    | INTEGER                       | -1 if unknown                                             | Indicates the maximum legal speed limit for general traffic                                                                  |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| direction   | INTEGER                       | -1 if unknown                                             | 1: one way with slope, 2: one way inverse slope, 3: bi-directional                                                           |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+

List of type column values
--------------------------

Possible values for the ``type`` column are:

- ``cycleway``: Separated way for the use of cyclists.
- ``ferry``: A ferry route used to transport people or vehicles across water.
- ``footway``: Designated path mainly/exclusively for pedestrians.
- ``highway``: Any kind of street or way.
- ``highway_link``: Connecting ramp to/from a highway.
- ``motorway``: Highest-performance highway serving main towns.
- ``path``: Generic multi-use path for non-motorized vehicles.
- ``primary``: Important highway linking large towns.
- ``residential``: Highway used for local traffic in residential areas.
- ``roundabout``: Circular highway junction where traffic on the roundabout has right of way.
- ``secondary``: Highway linking large towns, typically two lanes.
- ``steps``: Flights of steps on footways/paths.
- ``tertiary``: Highway linking small settlements or town centers.
- ``track``: Mostly agricultural or forest tracks, usually unpaved.
- ``trunk``: High-performance highway not classified as motorway.
- ``unclassified``: Minor public highways not residential or tertiary.

List of surface column values
-----------------------------

Possible values for the ``surface`` column are:

- ``asphalt``: Any asphalt surface.
- ``cobblestone``: Cobbled surface.
- ``compacted``: Compacted mix of gravel and sand.
- ``concrete``: Cement-based surface.
- ``grass``: Grass-covered ground.
- ``gravel``: Crushed rock surface.
- ``ground``: Natural surface with no specific composition.
- ``metal``: Metallic surface.
- ``mud``: Wet, unpaved surface.
- ``paved``: Stabilized and hardened surface.
- ``pebblestone``: Surface of rounded rocks found near water.
- ``sand``: Fine rock particles found near water.
- ``unpaved``: Loose surface, not stabilized.
- ``water``: Surface for ferry routes using water bodies.
- ``wood``: Wooden surface.

.. include:: _Footer.rst
