UTRF classification
===================

Based on (`Tornay *et al*. 2017 <https://doi.org/10.1016/j.uclim.2017.03.002>`_) method, GeoClimate classifies the RSU areas according to urban classes.

The Urban Typology by Random Forest (UTRF) is defined in the table below with a proposed color to map them (default .sld file format can be downloaded at `this address <https://github.com/orbisgis/geoclimate/tree/master/geoindicators/src/main/resources/styles>`_).

.. list-table::
   :header-rows: 1
   :widths: 10 35 20

   * - Type
     - Type definition
     - Hexa Color code
   * - ``ba``
     - Industrial building
     - ``#8f8f8f``
   * - ``bgh``
     - High-rise building
     - ``#000d00``
   * - ``icif``
     - Block of buildings on closed urban islet
     - ``#d52623``
   * - ``icio``
     - Block of buildings on open urban islet
     - ``#f07923``
   * - ``id``
     - Detached building
     - ``#eccb27``
   * - ``local``
     - Informal building
     - ``#d728ac``
   * - ``pcif``
     - Residential on closed islet
     - ``#2b6724``
   * - ``pcio``
     - Residential on open islet
     - ``#36884a``
   * - ``pd``
     - Detached house
     - ``#22be2f``
   * - ``psc``
     - Semi-detached house
     - ``#05ff58``
   * - ``-``
     - Undefined
     - ``#ffffff``

Output urban typologies layer
-----------------------------
By default, three tables are calculated:

- ``building_utrf``
- ``rsu_utrf_area``
- ``rsu_utrf_floor_area``

UTRF at building scale
----------------------

.. list-table::
   :header-rows: 1

   * - Field name
     - Field type
     - Definition
   * - ``ID_BUILD``
     - INTEGER
     - BUILDING's unique id
   * - ``ID_RSU``
     - INTEGER
     - RSU's unique id the building belongs to
   * - ``THE_GEOM``
     - GEOMETRY
     - Building's geometry
   * - ``I_TYPO``
     - VARCHAR
     - Building urban typology

UTRF at RSU scale based on building area
----------------------------------------

The building typology is aggregated at RSU scale based on building area fraction.

.. list-table::
   :header-rows: 1

   * - Field name
     - Field type
     - Definition
   * - ``ID_RSU``
     - INTEGER
     - RSU's unique id
   * - ``THE_GEOM``
     - GEOMETRY
     - RSU's geometry
   * - ``TYPO_MAJ``
     - VARCHAR
     - Main urban typology
   * - ``UNIQUENESS_VALUE``
     - DOUBLE
     - The value of the uniqueness main class for the RSU defined as (FRACTION TYPO_MAJ - FRACTION TYPO_SECOND) / (FRACTION TYPO_MAJ + FRACTION TYPO_SECOND)
   * - ``TYPO_BA``
     - VARCHAR
     - Fraction of type "BA"
   * - ``TYPO_BGH``
     - VARCHAR
     - Fraction of type "BGH"
   * - ``TYPO_ICIF``
     - VARCHAR
     - Fraction of type "ICIF"
   * - ``TYPO_ICIO``
     - VARCHAR
     - Fraction of type "ICIO"
   * - ``TYPO_ID``
     - VARCHAR
     - Fraction of type "ID"
   * - ``TYPO_LOCAL``
     - VARCHAR
     - Fraction of type "LOCAL"
   * - ``TYPO_PCIF``
     - VARCHAR
     - Fraction of type "PCIF"
   * - ``TYPO_PCIO``
     - VARCHAR
     - Fraction of type "PCIO"
   * - ``TYPO_PD``
     - VARCHAR
     - Fraction of type "PD"
   * - ``TYPO_PSC``
     - VARCHAR
     - Fraction of type "PSC"

UTRF at RSU scale based on building floor area
----------------------------------------------

The building typology is aggregated at RSU scale based on building floor area fraction.

.. list-table::
   :header-rows: 1

   * - Field name
     - Field type
     - Definition
   * - ``ID_RSU``
     - INTEGER
     - RSU's unique id
   * - ``THE_GEOM``
     - GEOMETRY
     - RSU's geometry
   * - ``TYPO_MAJ``
     - VARCHAR
     - Main urban typology
   * - ``UNIQUENESS_VALUE``
     - DOUBLE
     - The value of the uniqueness main class for the RSU defined as (FRACTION TYPO_MAJ - FRACTION TYPO_SECOND) / (FRACTION TYPO_MAJ + FRACTION TYPO_SECOND)
   * - ``TYPO_BA``
     - VARCHAR
     - Fraction of type "BA"
   * - ``TYPO_BGH``
     - VARCHAR
     - Fraction of type "BGH"
   * - ``TYPO_ICIF``
     - VARCHAR
     - Fraction of type "ICIF"
   * - ``TYPO_ICIO``
     - VARCHAR
     - Fraction of type "ICIO"
   * - ``TYPO_ID``
     - VARCHAR
     - Fraction of type "ID"
   * - ``TYPO_LOCAL``
     - VARCHAR
     - Fraction of type "LOCAL"
   * - ``TYPO_PCIF``
     - VARCHAR
     - Fraction of type "PCIF"
   * - ``TYPO_PCIO``
     - VARCHAR
     - Fraction of type "PCIO"
   * - ``TYPO_PD``
     - VARCHAR
     - Fraction of type "PD"
   * - ``TYPO_PSC``
     - VARCHAR
     - Fraction of type "PSC"


.. include:: _Footer.rst
