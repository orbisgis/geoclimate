# Geoclimate

<span style="color:red">**Warning: this documentation is under construction**</span>

![bandeau_geoclimate](./resources/images/readme/bandeau_geoclimate.png)


## Introduction

The climate modelling is based on the type, the use and the shape of the studied area. 
At the urban scale, the type of land surface (pervious, impervious), the shape and the distribution of the buildings and the streets as well as the building use are the determinant parameters affecting the urban climate. 
Thus it is necessary to described accurately the urban fabric in order to apply the right energy balance. 

Geoclimate is a [Groovy](http://groovy-lang.org/) library that implements algorithms to compute geospatial indicators (e.g. density of building, sky view factor, building compacity, road distance, ...) based on vector [GIS](https://en.wikipedia.org/wiki/Geographic_information_system) layers.


The geospatial indicators are computed at three [spatial units](./chain_documentation/spatial_units/spatial_units.md):
- the building,
- the block defined as an aggregation of buildings that are in contact,
- the Reference Spatial Unit (RSU).

More than 60 urban indicators are yet available. At a first stage, those indicators have been selected to:
1. to feed the [TEB](http://www.umr-cnrm.fr/spip.php?article199) climate model developed by [Météo France](http://www.meteofrance.com),
2. to classify the urban tissues and build the *Local Climate Zones* ([LCZ](http://www.wudapt.org/lcz/)).

Even if Geoclimate have been developed for climate studies, the indicators can be used for other topics such as landscape ecology, land use, habitat conservation planning or any environmental or territory applications.


## Table of contents

You can navigate in the documentation through the following entries.

### Geoclimate

- [Approach](./chain_documentation/approach.md)
  - [Input data model](./chain_documentation/input_data_model.md)
  - [Spatial units](./chain_documentation/spatial_units/spatial_units.md)
  - [Indicators](./chain_documentation/indicators/indicators.md)
  - [Classifications](./chain_documentation/classsifications/classifications.md)  
- [Implementation](./chain_documentation/implementation.md)
- [Applications](./chain_documentation/workflow/description.md)
  - [With OSM](./chain_documentation/workflow/osm/intro.md)
  - [With BD Topo v2](./chain_documentation/workflow/bd_topo_v2/intro.md)

### How to use Geoclimate ?

- [Execution environment](./for_users/execution_environment.md)
- [Execution tools](./for_users/execution_tools.md)
- Run the chain : step by step tutorials
  - [With OSM](./for_users/execution_examples/run_osm.md)
  - [With BD Topo v2](./for_users/execution_examples/run_bd_topo_v2.md)

### Resources for developers

- [Good things to know](./for_developers/info.md) for developers wishing to reuse Geoclimate



## Authors

Geoclimate has been developed by researchers and engineers from the french [Lab-STICC](https://www.labsticc.fr/en/index/) laboratory (CNRS UMR 6285 - DECIDE team - GIS group). They are also authors of the [OrbisGIS](http://orbisgis.org/) and [H2GIS](http://h2gis.org/) applications.

## Funding

The Geoclimate library has been developed within the two following research projects:

- [URCLIM](http://www.urclim.eu/), part of ERA4CS, a project initiated by JPI Climate and co-funded by the European Union
- [PAENDORA](https://www.ademe.fr/sites/default/files/assets/documents/aprademeprojetsretenusen2017.pdf#page=39) , funded by [ADEME](https://www.ademe.fr/)