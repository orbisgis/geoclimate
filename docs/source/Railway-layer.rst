Railway Layer
=============

The Railway layer represents any kind of runways for wheeled equipment.
Geometries must be ``LINESTRING`` or ``MULTILINESTRING``.

+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| Name        | Type                          | Constraints                                               | Definition                                                                                                                   |
+=============+===============================+===========================================================+==============================================================================================================================+
| the_geom    | LINESTRING or MULTILINESTRING | X Y dimensions                                            | Geometry                                                                                                                     |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| id_railway  | INTEGER                       | Primary Key                                               | Unique Identifier                                                                                                            |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| id_source   | VARCHAR                       | *not null*                                                | Identifier of the feature from the input datasource                                                                          |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| type        | VARCHAR                       | *not null*; in ``type`` list                              | Type of rail                                                                                                                 |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| zindex      | INTEGER                       | *not null* ; ≥ -4 ; ≤ 4                                   | Defines the position relative to ground. 0: on ground, 1–4: above ground, -4–-1: underground.                                |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| crossing    | VARCHAR                       | ``bridge`` or ``null``                                    | Indicates whether the rail is located on a bridge or not.                                                                    |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| usage       | VARCHAR                       | ``null`` or in ``usage`` list                             | Indicates the usage of the railway.                                                                                          |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
| width       | FLOAT                         | *not null*                                                | Width of the railway (in meters). Default corresponds to standard gauge (1.435 m) + 1 m for ballast.                         |
+-------------+-------------------------------+-----------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+

Type Column Values
------------------

Possible values for the ``type`` column:

- ``highspeed``: Railway track for high-speed rail.
  *(See: https://wiki.openstreetmap.org/wiki/Key:highspeed)*

- ``rail``: Railway track for full-sized passenger or freight trains.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:railway=rail)*

- ``service track``: Track mainly for sorting or parking freight trains.
  *(See: http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf)*

- ``disused``: No longer used, but track and infrastructure remain.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:railway=disused)*

- ``funicular``: Cable railway on steep slopes, vehicles counterbalancing each other.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:railway=funicular)*

- ``subway``: City public transport rails, fully separated from traffic.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:railway=subway)*

- ``tram``: Rails for trams, often laid within roads.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:railway=tram)*

Usage Column Values
-------------------

The ``usage`` column follows the [OSM definition](https://wiki.openstreetmap.org/wiki/Key:usage).
Supported values are:

- ``main``
- ``branch``
- ``industrial``
- ``military``
- ``tourism``
- ``scientific``
- ``test``

----------

.. include:: _Footer.rst
