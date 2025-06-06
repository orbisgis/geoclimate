===========
Output data
===========

.. toctree::
    :maxdepth: 1

    Building-indicators
    Block-indicators
    RSU-indicators
    LCZ-classification
    UTRF-classification
    Road-indicators
    Ground-acoustic
    Grid-indicators
    Sprawl-areas

GeoClimate output data consists both in a set of indicators and classifications.

Building, block and reference spatial units Indicators
======================================================

GeoClimate indicators are used to:
* measure morphological properties (e.g. the form factor),
* describe spatial organizations (e.g. distance measurements, patch metrics, shape index, spatial density, etc).

They quantify the shape and pattern of urban and landscape structures.

GeoClimate indicators are calculated at 3 different scales:
1. building scale (see `building indicators output <https://github.com/orbisgis/geoclimate/wiki/Building-indicators>`_)
2. block of buildings (aggregation of buildings that are in contact – see `block indicators output <https://github.com/orbisgis/geoclimate/wiki/Block-indicators>`_)
3. chosen Reference Spatial Unit (RSU – see `RSU indicators output <https://github.com/orbisgis/geoclimate/wiki/RSU-indicators>`_)

Note that scale 2 (resp. 3) indicators need scale 1 (resp. 2) indicators to be computed.

Each indicator will be shortly described and its computation method roughly/simply defined.

Classifications
===============

Two classifications are available:
1. Local Climate Zones (LCZ) classification, at the RSU scale (see `LCZ classification output <https://github.com/orbisgis/geoclimate/wiki/LCZ-classification>`_)
2. Urban Typology by Random Forest (UTRF) classification, computed at the building scale and aggregated at the RSU scale (see `UTRF classification output <https://github.com/orbisgis/geoclimate/wiki/UTRF-classification>`_)

Noise-related indicators
========================

GeoClimate can also be used to produce spatial informations that can be used for noise-related purposes. Two main types of informations are created:
- road-traffic indicators: more detail can be found at the page of the `road indicators output <https://github.com/orbisgis/geoclimate/wiki/Road-indicators>`_
- ground acoustic informations: more detail can be found at the page of the `ground acoustic output <https://github.com/orbisgis/geoclimate/wiki/Ground-acoustic>`_

Grid indicators
===============

The indicators calculated at RSU scale suffer from one problem: the calculation is not exact if a building is located on the border separating two units. It is not a problem because the default RSU used by GeoClimate are Topographical Spatial Units (TSU) having borders that rarely cross the buildings. But if the RSU are regular grid cells, many buildings will be located on cell borders and then the smaller the RSU size compared to the building size, the higher the error.

To overcome this issue, the indicators can be calculated using a regular rectangular grid using a specific script. This specific script uses specific algorithms for indicator calculation, thus avoiding the problem raised by buildings located on borders in the default algorithm. The limitation is that this calculation is only limited to a given number of indicators/classifications. The exhaustive list is given at the `grid indicator output page <https://github.com/orbisgis/geoclimate/wiki/Grid-indicators>`_.

.. include:: _Footer.rst
