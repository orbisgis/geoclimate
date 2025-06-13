Create WRF inputs from LCZ
==========================

For those interested to simulate numerically the effect of urbanization
on local climate, GeoClimate calculates the Local Climate Zones (LCZ)
that might be useful to creates the spatial inputs needed by the
`Weather Research Forecast (WRF)
model <https://ncar.ucar.edu/what-we-offer/models/weather-research-and-forecasting-model-wrf>`__.

The method is a two-steps approach: first the LCZ are calculated using
GeoClimate. Second, the Urban Canopy Parameters (UCP) needed in WRF are
calculated using the
`W2W <https://github.com/matthiasdemuzere/w2w#readme>`__ Python project.

1. Calculating the LCZ using GeoClimate
---------------------------------------

Using the OSM data
~~~~~~~~~~~~~~~~~~

The configuration file below is the config file you may use to calculate
LCZ for Pont-de-Veyle using OSM data.

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
                   "LCZ"
               ],
               "svfSimplified": true,
               "estimateHeight": true
           },
           "grid_indicators": {
               "x_size": 10,
           "y_size": 10,
           "rowCol": false,
           "output" : "geojson",
           "indicators" :["LCZ_PRIMARY"]
            }
       }
   }

Using the BDTOPO v2.2 data
~~~~~~~~~~~~~~~~~~~~~~~~~~

The configuration file below is the config file you may use to calculate
LCZ for Pont-de-Veyle using BDTOPO v2.2 data.

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
               "indicatorUse": [
                   "LCZ"
               ],
               "svfSimplified": true
           },
           "grid_indicators": {
               "x_size": 10,
           "y_size": 10,
           "rowCol": false,
           "output" : "geojson",
           "indicators" :["LCZ_PRIMARY"]
            }
       }
   }

2. Calculate the WRF UCP from LCZ
---------------------------------

The LCZ are used to calculate the UCP from the
`W2W <https://github.com/matthiasdemuzere/w2w#readme>`__ Python project.
The input data of the W2W project is the LCZ calculated by WUDAPT.

------------------------------------------------------------------

.. include:: _Footer.rst
