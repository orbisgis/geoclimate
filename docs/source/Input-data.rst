==========
Input data
==========


.. toctree::
   :maxdepth: 1

   Zone-layer
   Building-layer
   Road-layer
   Railway-layer
   Vegetation-layer
   Water-layer
   Impervious-layer
   Urban-areas-layer
   Sea-land-areas-layer


The indicators in GeoClimate are calculated from vector GIS layers that represent the main topographic features. To guarantee the use of the algorithms and their outputs, the GIS layers must follow a set of specifications. These specifications are defined for each layer.
They include the name of the columns, the values used by the attributes, the dimension of the geometry... 

Depending on the use of GeoClimate, the number of input GIS layers differs. 

Note that a GIS layer is an abstraction of reality specified by a geographic data model (geometry + attributes). It represents a single geographic subject. It consists in a set of data staged in a tabular way (rows, columns).

The GIS layer have to use a metric reference spatial system. Lat/Long coordinates are not supported by the algorithms. Thus if you have Lat/Long coordinates data, you first need to reproject in a local metric system. 

.. include:: _Footer.rst
