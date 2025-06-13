Urban Areas Layer
=================

The urban areas layer qualifies the landuse functional units.

**Warning:** This layer is used in the Reference Spatial Unit (RSU) building process. It does not aim to represent the entire urban sprawl of a conurbation.

+---------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| Name          | Type    | Constraints                                             | Definition                                               |
+===============+=========+=========================================================+==========================================================+
| the_geom      | POLYGON | X Y dimensions                                          | Geometry                                                 |
+---------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| id_urban      | INTEGER | Primary Key                                             | Unique Identifier                                        |
+---------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| id_source     | VARCHAR | *not null*                                              | Identifier of the feature from the input datasource      |
+---------------+---------+---------------------------------------------------------+----------------------------------------------------------+
| type          | VARCHAR | *not null*                                              | Type of urban areas                                      |
+---------------+---------+---------------------------------------------------------+----------------------------------------------------------+

Type Column Values
------------------

List of all possible values for the ``type`` column:

- ``residential``
  A residential area is one in which people live.

- ``construction``
  Any area under construction.

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
