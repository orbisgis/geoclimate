# BD Topo v2 workflow



## Context

This workflow has been developed within the [PAENDORA](https://www.ademe.fr/sites/default/files/assets/documents/aprademeprojetsretenusen2017.pdf#page=39) research project, funded by [ADEME](https://www.ademe.fr/).



## The BD Topo

The BD Topo is a topographic vector database, made by the French National Geographic Institute ([IGN](http://ign.fr/)). This database is "*a 3D vectorial description (structured in objects) of the elements of the territory and its infrastructures, of metric precision, usable at  scales ranging from 1: 5 000 to 1: 50 000.*"

In this use case, we are working with the [version 2.2](http://professionnels.ign.fr/ancienne-bdtopo) (which was replaced in 2019 by the [version 3.0](http://professionnels.ign.fr/bdtopo) - a dedicated use case may be implemented in the coming months).

In practice, this database is:

- **not open**,
- free of charge **only** for research or education purposes,
- provided as shapefiles (.shp) or as [PostGreSQL](https://www.postgresql.org/) dumps, at the French department scale.



## Workflow description

The BD Topo v2 workflow can be divided in three modules:

- M1 - [Input data preparation](./data_preparation.md)
- M2 - [Formating and quality control](./data_formating.md)
- M3 - Production of indicators and classifications



![](/home/gpetit/Documents/Codes/geoclimate/docs/resources/images/chain_documentation/bdtopov2_modules_outputs.png)



### M1 - Input data preparation

The user chooses the data set of his choice and ensures that the information present corresponds to the ***Geoclimate input data model*** described [HERE](https://github.com/gpetit/geoclimate/blob/add_documentation/docs/input_data/INPUT_DATA_MODEL.md). So **M1 is feeding M2**.

[![img](https://github.com/gpetit/geoclimate/raw/add_documentation/docs/images/icons/arrow.png)](https://github.com/gpetit/geoclimate/blob/add_documentation/docs/images/icons/arrow.png) [Read more](./data_preparation.md) about this module.

### 

### M2 - Formating and quality control

The prepared data provided by the user, according to the input model, are controlled and enriched on the basis of pre-established rules. So **M2 is feeding M3**.

[![img](https://github.com/gpetit/geoclimate/raw/add_documentation/docs/images/icons/arrow.png)](https://github.com/gpetit/geoclimate/blob/add_documentation/docs/images/icons/arrow.png) [Read more](./data_formating.md) about this module.

### 

### M3 - Geoindicators and LCZ's production

The data are processed automatically, without user intervention, and the resulting layers are provided ([See](https://github.com/gpetit/geoclimate/tree/add_documentation/docs#Resulting-layers)).

### 

### Remark

The Geoclimate chain is designed to execute only one [`ZONE`](https://github.com/gpetit/geoclimate/blob/add_documentation/docs/data_preparation/DATA_PREPARATION.md#Working-areas) at a time (so the chain will be executed as many times as there are zones to be analyzed).