Bounding box case
=================

**The default cases for OSM and BDTOPO v2.2 are presented in other
pages. Please make sure that the default cases are running on your
computer before using the bounding box method.**

In the default case, the area of interest is specified using a city
name. You also have the possibility to calculate all the GeoClimate
outputs inside a given rectangle (i.e. a bounding box).

The only difference with the default case is that geographic coordinates
replace the city name. All the other elements of the configuration file
remain unchanged.

The coordinates of the bounding box are expressed as [minY, minX, maxY,
maxX].

Bounding box method with OSM
----------------------------

A bounding box has been determined for the city of Pont-de-Veyle, with
the following coordinates : 46.257330,4.870033,46.269970,4.905224

The configuration file below uses this bounding box method with OSM.

.. code:: json

   {
       "description": "Processing OSM data",
       "input": {
           "locations": [
               [46.257330, 4.870033, 46.269970, 4.905224]
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
           "indicators" :["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_POP",
                                  "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                                  "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                                  "BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY",
                                  "SEA_LAND_FRACTION", "ASPECT_RATIO", "SVF",
                                  "HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS_CLASS",
                                  "UTRF_AREA_FRACTION", "UTRF_FLOOR_AREA_FRACTION",
                                  "LCZ_PRIMARY", "BUILDING_HEIGHT_DISTRIBUTION", "STREET_WIDTH"]
            }
       }
   }

In order to determine the coordinates of your bounding box, you can use
the `bboxfinder <http://bboxfinder.com>`__ website. Make sure your
coordinates are in latitude / longitude (“Lat / Lon”) format. You can
choose the coordinates options on the bottom right of the bboxfinder
website.

Bounding box method with BDTOPO v2.2
------------------------------------

The configuration file below uses a bounding box method with BDTOPO
v2.2.

This bounding box represents the envelope of the city of Redon.

Note that the EPSG code for the projection system here is 2154 and not
“Lat / Lon” anymore.

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
                   "LCZ",
                   "TEB",
                   "UTRF"
               ],
               "svfSimplified": true
           },
           "grid_indicators": {
               "x_size": 100,
           "y_size": 100,
           "rowCol": false,
           "output" : "geojson",
           "indicators" :[
                    "BUILDING_FRACTION",
                    "BUILDING_HEIGHT",
                    "WATER_FRACTION",
                    "VEGETATION_FRACTION",
                    "ROAD_FRACTION",
                    "IMPERVIOUS_FRACTION",
                    "LCZ_FRACTION"
                ]
            }
       }
   }

----

.. include:: _Footer.rst