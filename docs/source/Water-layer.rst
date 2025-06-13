Water Layer
===========

The water layer represents any kind of surface (river, sea, lake, etc.).

+-----------+---------+---------------------------------------------------------+----------------------------------------------------------------------+
| Name      | Type    | Constraints                                             | Definition                                                           |
+===========+=========+=========================================================+======================================================================+
| the_geom  | POLYGON | X Y dimensions                                          | Geometry                                                             |
+-----------+---------+---------------------------------------------------------+----------------------------------------------------------------------+
| id_water  | INTEGER | Primary Key                                             | Unique Identifier                                                    |
+-----------+---------+---------------------------------------------------------+----------------------------------------------------------------------+
| id_source | VARCHAR | *not null*                                              | Identifier of the feature from the input datasource                  |
+-----------+---------+---------------------------------------------------------+----------------------------------------------------------------------+
| type      | VARCHAR | not null; value must be in the [type list]_             | Type of water                                                        |
+-----------+---------+---------------------------------------------------------+----------------------------------------------------------------------+
| zindex    | INTEGER | not null; ≥ 0; ≤ 1                                      | Defines position relative to ground: 0 = on ground, 1 = above ground |
+-----------+---------+---------------------------------------------------------+----------------------------------------------------------------------+

.. _type list: https://github.com/orbisgis/geoclimate/wiki/Water-layer/_edit#type

Type Column Values
------------------

List of possible values for the ``type`` column:

- ``aqueduct``
  A watercourse constructed to carry water from a source to a distribution point far away.

- ``canal``
  An artificial open flow waterway used to carry useful water for transportation, waterpower, or irrigation.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:waterway%3Dcanal)*

- ``bay``
  An inlet of a sea or lake mostly surrounded by land.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural%3Dbay)*

- ``lake``
  A large body of water localized in a basin surrounded by dry land.
  *(See: https://wiki.openstreetmap.org/wiki/FR:Tag:natural%3Dwater)*

- ``lagoon``
  A shallow body of water separated from a larger body of water by a narrow landform such as reefs or barrier islands.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=tree)*

- ``mangrove``
  Water part of a mangrove (see vegetation layer).
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=tree)*

- ``pond``
  A small area of fresh water.
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=tree)*

- ``basin``
  An area artificially graded to hold water.
  *(See: https://wiki.openstreetmap.org/wiki/Key%3Abasin)*

- ``water``
  Any water area.

-----------------

.. include:: _Footer.rst
