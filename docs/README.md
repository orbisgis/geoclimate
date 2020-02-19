# Geoclimate chain



<span style="color:red">**Warning: this documentation is under activ development**</span>



## What is Geoclimate?

Geoclimate is a set of methods and spatial processing chains for extracting climate knowledge, at the city scale and on the basis of vector data.

## Why?

Even if many other applications are possible, the two main objectives of Geoclimate are: 

1. to feed the [TEB](http://www.umr-cnrm.fr/spip.php?article199) climate model developed by [Météo France](http://www.meteofrance.com),
2. to classify the urban tissues and build the *Local Climate Zones* ([LCZ](http://www.wudapt.org/lcz/)).

To achieve these objectives, it is necessary to calculate a large number of morphological and geographical indicators at different spatial scales.

Geoclimate has been developed with this in mind and offers a turnkey solution for users to compute:

- a series of more than 60 urban indicators at three spatial scales : RSU, block and building ([see](#Scales-of-analysis)),
- Two segmentations of the territory at the RSU scale: LCZ classification and Urban typology



## Table of contents

You can navigate in the documentation through the following entries.

### Geoclimate chain in details

- [Overview](./chain_documentation/overview.md)
  - [The input data model](./chain_documentation/input_data_model.md)
  - The spatial units: [Building](./chain_documentation/spatial_units/building.md), [Block](./chain_documentation/spatial_units/block.md) and [RSU](./chain_documentation/spatial_units/rsu.md)
  - Indicators for [buildings](./chain_documentation/indicators/building.md), [blocks](./chain_documentation/indicators/block.md), [RSUs](./chain_documentation/indicators/rsu.md) and [RSU classifications](./chain_documentation/indicators/rsu_classifications.md)
- [Implementation](./chain_documentation/implementation.md)
- [Workflow](./chain_documentation/workflow/description.md)
  - [With OSM](./chain_documentation/workflow/osm/intro.md)
  - [With BD Topo v2](./chain_documentation/workflow/bd_topo_v2/intro.md)

### How to run the Geoclimate chain ?

- [Execution environment](./for_users/execution_environment.md)
- [Execution tools](./for_users/execution_tools.md)
- Run the chain : step by step tutorials
  - [With OSM](./for_users/execution_examples/run_osm.md)
  - [With BD Topo v2](./for_users/execution_examples/run_bd_topo_v2.md)

### Resources for developers

- [Good things to know](./for_developers/info.md) for developers wishing to reuse Geoclimate



## Authors

The Geoclimate chain has been developed by researchers and engineers from the french [Lab-STICC](https://www.labsticc.fr/en/index/) laboratory (CNRS UMR 6285 - DECIDE team - GIS group). They are also authors of the [OrbisGIS](http://orbisgis.org/) and [H2GIS](http://h2gis.org/) applications.

## Funding

The Geoclimate chain has been developed within the two following research projects:

- [URCLIM](http://www.urclim.eu/), part of ERA4CS, a project initiated by JPI Climate and co-funded by the European Union
- [PAENDORA](https://www.ademe.fr/sites/default/files/assets/documents/aprademeprojetsretenusen2017.pdf#page=39) , funded by [ADEME](https://www.ademe.fr/)