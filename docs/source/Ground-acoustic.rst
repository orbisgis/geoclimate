Ground Acoustic
===============

The table ``ground_acoustic`` merges the vegetation, water, and impervious tables built by GeoClimate. This table contains an **absorption coefficient** (``g`` column) used to estimate the acoustic energy losses upon reflection for a specific surface.

Each surface is unique, and potential overlaps are eliminated by applying a **priority** to the input layers. The priorities and the mapping between the types of input geometries (vegetation, water, etc.) are described in a `JSON file <https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/resources/org/orbisgis/geoclimate/geoindicators/ground_acoustic_absorption.json>`_.

To compute the ``ground_acoustic`` table, the following parameters must be defined in the workflow configuration file:

.. code-block:: json

    "parameters": {
      "noise_indicators": { "ground_acoustic" : true }
    }

Schema of the ``ground_acoustic`` table:

+------------+---------+--------------------+--------------------------------------------------------------+
| Name       | Type    | Constraints        | Definition                                                   |
+============+=========+====================+==============================================================+
| the_geom   | POLYGON | X Y dimension      | Geometry                                                     |
+------------+---------+--------------------+--------------------------------------------------------------+
| id_ground  | SERIAL  | *not null*         | Identifier of the ground surface                             |
+------------+---------+--------------------+--------------------------------------------------------------+
| g          | FLOAT   |                    | Acoustic absorption coefficient                              |
+------------+---------+--------------------+--------------------------------------------------------------+
| type       | INTEGER |                    | Value distinguishing the surface type from input layers      |
+------------+---------+--------------------+--------------------------------------------------------------+
| layer      | VARCHAR |                    | Name of the input layer (e.g., vegetation, water, impervious)|
+------------+---------+--------------------+--------------------------------------------------------------+

.. include:: _Footer.rst
