Default case with BDTopo
========================

This tutorial presents how to create Local Climate Zones with BDTopo
version 2.2 or 3.0.

Two tools are available : Command Line Interface (beginner user) and
Groovy (intermediate and advanced user)

Command Line Interface
----------------------

Get Geoclimate.jar on your computer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You will run the archive Geoclimate.jar in a Command Line Interface.

First, make sure Java (version 11 minimum) is installed in your
computer.

You need to download Geoclimate.jar
`here <Download.html#geoclimate-for-command-line-interface>`__.

Rename the downloaded file as “Geoclimate.jar”.

Create a folder in your documents (for instance
/home/mydirectory/Geoclimate) and place Geoclimate.jar in this folder.

Get data from BDTopo on your computer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Collect all your BDTopodata in one single folder (the folder can contain
many subfolders, the algorithm will find all needed data by itself as
long as you use the raw data supplied by the IGN). Currently, the
required files are the ones listed -
`here <https://github.com/orbisgis/geoclimate/tree/master/bdtopo/src/test/resources/org/orbisgis/geoclimate/bdtopo/v2/sample_12174>`__
for BDTopo 2.2 -
`here <https://github.com/orbisgis/geoclimate/tree/master/bdtopo/src/test/resources/org/orbisgis/geoclimate/bdtopo/v3/sample_12174>`__
for BDTopo 3.0

For each layer, the ‘.dbf’, ‘.shp’, ‘.shx’ and ‘.shp’ formats are
needed.

Create a subfolder (for instance
/home/mydirectory/Geoclimate/BD_TOPO_v2) and place your BDTopo data in
this folder.

Create and understand a configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to run Geoclimate, you need to write a configuration file. This
file specifies inputs, methods and outputs of Geoclimate.

An example of configuration file is presented below :

.. code:: json

   {
       "description": "Processing BDTopo v2 data",
       "input": {
           "folder":  "/home/mydirectory/Geoclimate/BD_TOPO_v2/",
           "locations":["12174"]    
       },
       "output": {
           "folder": "/tmp"
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
           "indicators" :["BUILDING_FRACTION", "BUILDING_HEIGHT",
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

You can copy this example in a notebook and name it
“my_first_config_file_bdtopov2.json” . Place this configuration file in
the same folder than Geoclimate.jar .

Understanding the configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The configuration file is structured in four main parts.

-  “description” is a character string that describes your process. You
   can name your process here.

-  “input” specifies the input data you will use. In this example, we
   specify “folder” for BDTopo 2.2 version, where the BD Topo 2.2 files
   are located on the computer
   (“/home/mydirectory/Geoclimate/BD_TOPO_v2/”) and which “locations”
   (communes using insee codes) should be run. If you want many communes
   of your data to be run, simply list their insee code:[“12174”,
   “12175”, etc.].

-  “output” specifies the format you expect for your output (here
   “folder”) and where you want to create your output files (here in
   /tmp).

-  “parameters” specifies the output you want to calculate based on your
   reference spatial units (“rsu_indicators”) or on a grid
   (“grid_indicators”).

-  At RSU scale, we calculate the LCZ, the TEB inputs and the UTRF
   (“indicatorUse”: [“LCZ”, “TEB”, “UTRF”]). We use the simplified
   method to calculate the sky view factor (“svfSimplified”: true).

-  With the grid approach, we specify the grid dimensions in meters
   (“x_size” and “y_size”) and the output format (“output” : “geojson”).
   Then, we specify the indicators we want to calculate for each cell of
   the grid (“BUILDING_FRACTION”, “BUILDING_HEIGHT”, “WATER_FRACTION”,
   “VEGETATION_FRACTION”, “ROAD_FRACTION”, “IMPERVIOUS_FRACTION”,
   “LCZ_FRACTION”).

Run Geoclimate
~~~~~~~~~~~~~~

Open a Command Line Interface.

Go to the folder where Geoclimate.jar is located using this command line
:

``cd /home/mydirectory/Geoclimate``

Then, you can run this command line which presents you the main options
of Geoclimate :

``java -jar Geoclimate.jar -h``

.. figure:: /_static/images/geoclimate_cli.png
   :alt: Groovy CLI

   Groovy CLI

In order to perform your first calculations with the configuration file
above, use

``java -jar Geoclimate.jar -f my_first_config_file_bdtopov2.json -w BDTOPO_V2``

Use the command BDTOPO_V3 instead of BDTOPO_V2 if you want to run
GeoClimate with BDTopo 3.0 datasources.

If everything runs well, you will obtain a message :
``The BDTOPO_V2 workflow has been successfully executed``

The results of your calculations are located in you ``\tmp`` folder.

.. figure:: /_static/images/results_bdtopo.png


-----------------

.. include:: _Footer.rst
