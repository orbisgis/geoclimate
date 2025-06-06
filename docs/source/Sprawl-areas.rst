Sprawl areas
============

The sprawl areas layer represents the expansion of impervious areas. The
sprawl areas layer contains one or more polygons. It is calculated from
a grid of LCZ. Only LCZs with values from 1 to 10 plus 105 are
considered. Polygons are constructed using neighborhood analysis.

======== ======= ============== =================
Name     Type    Constraints    Definition
======== ======= ============== =================
the_geom POLYGON X Y dimensions Geometry
id       INTEGER Primary Key    Unique Identifier
======== ======= ============== =================

.. include:: _Footer.rst
