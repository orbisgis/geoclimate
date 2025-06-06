Vegetation Layer
================

The vegetation layer represents any kind of land areas that qualify a natural feature.

+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| Name        | Type    | Constraints                                            | Definition                                                                                      |
+=============+=========+========================================================+=================================================================================================+
| the_geom    | POLYGON | X Y dimensions                                         | Geometry                                                                                        |
+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| id_veget    | INTEGER | Primary Key                                            | Unique Identifier                                                                               |
+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| id_source   | VARCHAR | *not null*                                             | Identifier of the feature from the input datasource                                             |
+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| type        | VARCHAR | *not null*                                             | Type of vegetation                                                                              |
+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| height_class| VARCHAR | *not null*                                             | Height class (``low`` or ``high``)                                                              |
+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| zindex      | INTEGER | *not null* ; ≥ 0 ; ≤ 1                                 | Defines position relative to ground: 0 = on ground, 1 = above ground (e.g., on top of building) |
+-------------+---------+--------------------------------------------------------+-------------------------------------------------------------------------------------------------+

Type Column Values
------------------

List of possible values for the ``type`` column:

- ``tree``: A single tree
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=tree)*

- ``wood``: Tree-covered area (forest or wood), not managed economically
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=wood)*

- ``forest``: Managed woodland or plantation maintained for forest products
  *(See: https://wiki.openstreetmap.org/wiki/Tag:landuse=forest)*

- ``scrub``: Uncultivated land with bushes or stunted trees
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=scrub)*

- ``grassland``: Vegetation dominated by grasses and herbaceous plants
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=grassland)*

- ``heath``: Dwarf-shrub habitat with low woody vegetation, often Ericaceae
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=heath)*

- ``tree_row``: A line of trees
  *(See: https://wiki.openstreetmap.org/wiki/Tag:natural=tree_row)*

- ``hedge``: Line of closely spaced shrubs and trees forming a barrier or boundary
  *(See: https://wiki.openstreetmap.org/wiki/Tag:barrier=hedge)*

- ``mangrove``: Forests of salt tolerant mangrove trees in tropical tidal zones > 20°C
  *(See: https://wiki.openstreetmap.org/wiki/Tag:wetland=mangrove)*

- ``orchard``: Intentional planting of trees or shrubs for food production
  *(See: https://wiki.openstreetmap.org/wiki/Tag:landuse=orchard)*

- ``vineyard``: Land where grapes are grown
  *(See: https://wiki.openstreetmap.org/wiki/Tag:landuse=vineyard)*

- ``banana_plants``: Banana plantation
  *(See: https://wiki.openstreetmap.org/wiki/Key:trees)*

- ``sugar_cane``: Land where sugar cane is grown
  *(See: https://wiki.openstreetmap.org/wiki/Tag:landuse=farmland)*

.. include:: _Footer.rst
