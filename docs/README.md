# Geoclimate user documentation



**This documentation is under construction.**



## The Geoclimate chain



Geoclimate chain is a collection of spatial processes to produce vector maps of cities. First, algorithms are used to extract and transform input data to a set of GIS layers (GeoClimate abstract model). The GIS layers are then processed  to compute urban indicators at three geographic scales : *Reference Spatial Unit* (RSU), block and building ([Bocher et al, 2018](http://dx.doi.org/10.1016/j.uclim.2018.01.008)). These indicators are used to feed the [TEB](http://www.umr-cnrm.fr/spip.php?article199) model, classify the urban tissues and build the *Local Climate Zones* ([LCZ](http://www.wudapt.org/lcz/)).

The algorithms of the Geoclimate chain are implemented on top of the open source library OrbisData (https://github.com/orbisgis/orbisdata). Orbisdata provides a unique access point to query, manage, retrieve data from a [PostGIS](https://postgis.net/) or a [H2GIS](http://www.h2gis.org/) database. Orbisdata is based on lambda expressions and sugar programming methods introduced since JAVA 8. Orbisdata is closed to Groovy syntax and provide an elegant and fluent framework to easily manage geospatial data. The processing chain is packaged in the GeoClimate repository available as a set of  Groovy scripts.

## Inputs

The Geoclimate chain is currently working with two kind of dataset : 

- [OpenStreetMap](https://www.openstreetmap.org) (OSM), on a worldwide context,
- BD Topo [V2.2](http://professionnels.ign.fr/ancienne-bdtopo) (from [IGN](http://ign.fr/)), only on the french scale.

### OpenStreetMap



### BD Topo (only in France)

## Scales of analysis

As described in [Bocher et al, 2018](http://dx.doi.org/10.1016/j.uclim.2018.01.008), three scales of analysis are used in the Geoclimate chain :

- the building
- the block : a set of buildings that touches (at least one point in common)
- the *Reference Spatial Unit*, also called RSU, which is a continuous and homogeneous way to divide the space, using topographic constraint such as roads, rail ways, big vegetation and hydrographic areas in addition to administrative boundaries.

On each of these scales, various indicators and analysis are processed.

## Outputs

Depending on the options choosed by the user, the Geoclimate chain may produce various layers, described below.

#### `BUILDING_INDICATORS`

A table that stores all the (geo)indicators computed at the building's scale.

![](./images/icons/arrow.png) Read the full [description](./BUILDING_INDICATORS.md) of this table.

####  `BLOCK_INDICATORS`
A table that stores all the (geo)indicators computed at the block's scale.

![](./images/icons/arrow.png) Read the full [description](./BLOCK_INDICATORS.md) of this table.

####  `RSU_INDICATORS`

A table that stores all the (geo)indicators computed at the RSU's scale.

![](./images/icons/arrow.png) Read the full [description](./RSU_INDICATORS.md) of this table.

####  `RSU_LCZ`

A table that stores all the *Local Climate Zones* (LCZ) main and secondary types computed at the RSU's scale.

![](./images/icons/arrow.png) Read the full [description](./RSU_LCZ.md) of this table.