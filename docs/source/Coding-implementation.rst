Coding implementation
=====================

GeoClimate algorithms are implemented as functions in Groovy Scripts.

GeoClimate is organized in 3 modules (Figure 1).

- GeoIndicators is the main module. It contains all the algorithms to
  build the units of analysis, compute the corresponding indicators and
  classify urban fabric by type. The *SpatialUnits* script creates the
  units of analysis (currently blocks and TSU). The
  *BuildingIndicators*, *BlockIndicators*, *RoadIndicators*,
  *RSUIndicators* calculate morphological and topographical indicators
  respectively at building, block, road, and RSU scales. The
  *GenericIndicators* script calculates indicators which can be applied
  to any scale (e.g., the area of a unit - building, block, RSU - or the
  aggregation of indicator from one scale to another - mean building
  height within a block or a RSU). The *TypologyClassification* script
  classifies units to a certain type (currently building to UTRF and
  TSU to LCZ) based on indicators value. The *DataUtil* script
  facilitates data handling (e.g., join several tables). All functions
  contained in the previous scripts may be called individually. To run
  several of them in a row, workflows are available in the
  *WorkflowGeoIndicators* script. The main one performs all the analysis
  (green arrows Figure 1): it produces the units of analysis, computes
  the indicators at the base scales (building and road), computes
  indicators at block scale, aggregates indicators from lower to upper
  scale, computes indicators at RSU scale, and then classifies urban
  fabric.

- OSM module extracts and transforms the OSM data to the GeoClimate
  abstract model. These data processing steps are specified in the two
  scripts *InputDataLoading* and *InputDataFormating*. The *WorkflowOSM*
  script chains algorithms (blue arrow Figure 1): it triggers the 2
  scripts dedicated to the OSM data preparation and then the
  *WorkflowGeoIndicators* script. It is the main entry to specify the
  area to be processed, the indicators, and the classifications to
  compute.

- BDTopo_V2 module follows the same logic as the OSM module except that
  it is dedicated to version 2.2 of the `IGN BDTopo database <https://www.ign.fr/>`_.

.. figure:: /_static/images/geoclimate_implementation.png
   :alt: Figure 1. The GeoClimate modules

   Figure 1. The GeoClimate modules

-----------------------------------

.. include:: _Footer.rst
