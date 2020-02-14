# Geoclimate description



Présentation générale : fonctionnement de la chaîne

- on rentre dans Geoclimate via un modèle de donnée
- une fois dans Geoclimate on créé les échelles spatiales
- ensuite on produit les indicateurs / classification
- enfin on produit des résultats, via les workflow



## Input data model

Le modèle de données d'entrée -> [Input data model](./input_data_model.md)



## Spatial scales

As described in [Bocher et al, 2018](http://dx.doi.org/10.1016/j.uclim.2018.01.008), three scales of analysis are used in Geoclimate:

- the **building** ([see more](./spatial_units/building.md))
- the **block** : a set of buildings that touches (at least one point in common) ([see how it's made](./spatial_units/block.md))
- the ***Reference Spatial Unit***, also called  RSU, which is a continuous and homogeneous way to divide the space,  using topographic constraint such as roads, rail ways, big vegetation  and hydrographic areas in addition to administrative boundaries ([see how it's made](./spatial_units/rsu.md))



<img src="../resources/images/chain_documentation/building_block_rsu.png" style="zoom:40%;" />




## Indicators

More than 60 (geo)indicators are computed within the Geoclimate chain. They are grouped by theme and presented below: 

- [Building's indicators](./indicators/building.md)
- [Block's indicators](./indicators/block.md)
- [RSU's indicators](./indicators/rsu.md)
- [RSU's classifications](./indicators/rsu_classifications.md)

 

## Implementation

l'implémentation "technique" -> [implementation](./implementation.md)



## Workflow


Le [workflow](./workflow/description.md) qui permet de faire "tourner" la chaîne