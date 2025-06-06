Create WRF inputs
=================

For those interested to simulate numerically the effect of urbanization
on local climate, GeoClimate now allows to calculate some of the spatial
inputs needed to run the `Weather Research Forecast (WRF)
model <https://ncar.ucar.edu/what-we-offer/models/weather-research-and-forecasting-model-wrf>`__.

**The bounding box method for OSM and BDTOPO v2.2 is presented in other
pages. Please make sure you have tried it on your computer before
starting the calculation of WRF inputs.**

In the bounding box method, the number of indicators calculated at the
grid scale is limited to a short list. To calculate the WRF inputs, you
just need to add some more indicators to the list (they correspond to
the indicators used in `Chen et al,
2022 <https://www.frontiersin.org/articles/10.3389/fclim.2021.771441/full>`__
- Table 1 that are discussed at #704).

Creates WRF inputs with OSM data
--------------------------------

The configuration file below is the config file you may use to calculate
WRF inputs for Pont-de-Veyle using OSM data.

.. code:: json

   {
       "description": "Processing OSM data",
       "input": {
           "locations": [
               [[46.257330,4.870033,46.269970,4.905224]]
           ]
       },
       "output": {
           "folder": "C:\\temp"
       },
       "parameters": {
           "rsu_indicators": {
               "indicatorUse": [
                   "LCZ",
                   "TEB",
                   "UTRF"
               ],
               "svfSimplified": true,
               "estimateHeight": true
           },
           "grid_indicators": {
               "x_size": 100,
           "y_size": 100,
           "rowCol": false,
           "output" : "geojson",
           "indicators" :["BUILDING_FRACTION",
                              "BUILDING_HEIGHT",
                              "BUILDING_HEIGHT_WEIGHTED",
                              "BUILDING_TYPE_FRACTION",
                              "WATER_FRACTION",
                              "VEGETATION_FRACTION",
                              "ROAD_FRACTION",
                              "IMPERVIOUS_FRACTION",
                              "FREE_EXTERNAL_FACADE_DENSITY",
                              "BUILDING_SURFACE_DENSITY",
                              "BUILDING_HEIGHT_DIST",
                              "FRONTAL_AREA_INDEX",
                              "SEA_LAND_FRACTION"]
            }
       }
   }

Creates WRF inputs with BDTOPO v2.2
-----------------------------------

.. code:: json

    {
      "description": "Processing BDTopo v 2.2 data",
      "input": {
        "folder": "C:\\home\\mydirectory\\Geoclimate\\BD_TOPO_v2\\",
        "locations": [
          [
            6737756.724564202,
            316124.01010211144,
            6743486.0484706545,
            321921.09550058335
          ]
        ]
      },
      "output": {
        "folder": "C:\\temp"
      },
      "parameters": {
        "rsu_indicators": {
          "indicatorUse": ["LCZ", "TEB", "UTRF"],
          "svfSimplified": true
        },
        "grid_indicators": {
          "x_size": 100,
          "y_size": 100,
          "rowCol": false,
          "output": "geojson",
          "indicators": [
            "BUILDING_HEIGHT",
            "BUILDING_HEIGHT_WEIGHTED",
            "BUILDING_TYPE_FRACTION",
            "WATER_FRACTION",
            "VEGETATION_FRACTION",
            "ROAD_FRACTION",
            "IMPERVIOUS_FRACTION",
            "FREE_EXTERNAL_FACADE_DENSITY",
            "BUILDING_SURFACE_DENSITY",
            "BUILDING_HEIGHT_DIST",
            "FRONTAL_AREA_INDEX",
            "SEA_LAND_FRACTION"
          ]
        }
      }
    }

.. include:: _Footer.rst
