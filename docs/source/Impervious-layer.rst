Impervious Layer
================

The impervious layer means any kind of artificial surfaces which obstruct the percolation of water.

+------------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| Name             | Type    | Constraints                                             | Definition                                               |
+==================+=========+=========================================================+==========================================================+
| the_geom         | POLYGON | X Y dimensions                                          | Geometry                                                 |
+------------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| id_impervious    | INTEGER | Primary Key                                             | Unique Identifier                                        |
+------------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| id_source        | VARCHAR | *not null*                                              | Identifier of the feature from the input datasource      |
+------------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| type             | VARCHAR | *not null*                                              | Type of impervious areas                                 |
+------------------+---------+---------------------------------------------------------+----------------------------------------------------------+

Type Column Values
------------------

List of all possible values for the ``type`` column:

- ``government``
  Area with government buildings.

- ``entertainment_arts_culture``
  Area dedicated to entertainment, arts and culture.

- ``education``
  Area dedicated to education.

- ``military``
  Area dedicated to military constructions.

- ``industrial``
  Area dedicated to industrial constructions.

- ``commercial``
  Area dedicated to commercial buildings.

- ``healthcare``
  Area dedicated to healthcare infrastructures.

- ``transport``
  Area dedicated to transport infrastructures.

- ``building``
  Area dedicated to any kind of buildings.

- ``sport``
  Area dedicated to sport infrastructures.

- ``cemetery``
  Area dedicated to cemetery.

- ``religious``
  Area dedicated to religious buildings.

- ``power``
  Area dedicated to power generation and distribution.

------------------------------------------------------

.. include:: _Footer.rst
