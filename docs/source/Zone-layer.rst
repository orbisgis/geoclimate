Zone layer
==========

The zone layer represents the studied area. Only one geometry is
expected. The zone layer provides additional informations on the data
processed.

+------------------------+----------+---------------+------------------------------------------------------------+
| Name                   | Type     | Constraints   | Definition                                                 |
+========================+==========+===============+============================================================+
| the_geom               | POLYGON  | X Y dimension | Geometry                                                   |
+------------------------+----------+---------------+------------------------------------------------------------+
| id_zone                | VARCHAR  | *not null*    | Identifier of the zone area                                |
+------------------------+----------+---------------+------------------------------------------------------------+
| nb_estimated_building  | INTEGER  |               | Number of buildings with a height estimated                |
+------------------------+----------+---------------+------------------------------------------------------------+
| nb_building            | INTEGER  |               | Number of buildings computed                               |
+------------------------+----------+---------------+------------------------------------------------------------+
| nb_block               | INTEGER  |               | Number of blocks computed                                  |
+------------------------+----------+---------------+------------------------------------------------------------+
| nb_rsu                 | INTEGER  |               | Number of RSU computed                                     |
+------------------------+----------+---------------+------------------------------------------------------------+
| computation_time       | INTEGER  |               | Duration in seconds to run all processes                   |
+------------------------+----------+---------------+------------------------------------------------------------+
| last_update            | VARCHAR  |               | System current date when the processes are executed        |
+------------------------+----------+---------------+------------------------------------------------------------+
| version                | VARCHAR  |               | Current GeoClimate version                                 |
+------------------------+----------+---------------+------------------------------------------------------------+
| build_number           | VARCHAR  |               | Current GeoClimate build number                            |
+------------------------+----------+---------------+------------------------------------------------------------+

.. include:: _Footer.rst
