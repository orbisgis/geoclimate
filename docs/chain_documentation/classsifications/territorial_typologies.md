# Territorial typologies classification

Based on a random forest classification, this territorial typology aims *"to define a detailed description of buildings for urban climate and building energy consumption simulations"* ([Tornay *et al*. 2017](https://doi.org/10.1016/j.uclim.2017.03.002)).

This classification is computed at the RSU's scale.



## Typology

Below are presented the different territorial types. 

For each of them, we give the definition and the hexadecimal color code used to display those informations.

|  Type   |            Type definition            |                       Hexa Color code                        |
| :-----: | :-----------------------------------: | :----------------------------------------------------------: |
|  `ba`   |          Industrial building          | ![#f03c15](https://placehold.it/15/8f8f8f/000000?text=+) `#8f8f8f` |
|  `bgh`  |          High-rise building           | ![#cc0200](https://placehold.it/15/000d00/000000?text=+) `#000d00` |
| `icif`  | Linear building on closed urban islet | ![#fc0001](https://placehold.it/15/d52623/000000?text=+) `#d52623` |
| `icio`  |  Linear building on open urban islet  | ![#be4c03](https://placehold.it/15/f07923/000000?text=+) `#f07923` |
|  `id`   |           Detached building           | ![#ff6602](https://placehold.it/15/eccb27/000000?text=+) `#eccb27` |
| `local` |           Informal building           | ![#ff9856](https://placehold.it/15/d728ac/000000?text=+) `#d728ac` |
| `pcif`  |       Row house on closed islet       | ![#fbed08](https://placehold.it/15/2b6724/000000?text=+) `#2b6724` |
| `pcio`  |        Row house on open islet        | ![#bcbcba](https://placehold.it/15/36884a/000000?text=+) `#36884a` |
|  `pd`   |            Detached house             | ![#ffcca7](https://placehold.it/15/22be2f/000000?text=+) `#22be2f` |
|  `psc`  |          Semi-detached house          | ![#57555a](https://placehold.it/15/05ff58/000000?text=+) `#05ff58` |
|         |               Undefined               | ![#006700](https://placehold.it/15/ffffff/000000?text=+) `#ffffff` |

### Cartography


One `urban_typo.sld` style file, based on this classification, is provided in the `/processing_chain/src/main/resources/styles/` folder ([here](https://github.com/orbisgis/geoclimate/tree/v1.0.0-RC1/processingchain/src/main/resources/styles)).

![](../../resources/images/chain_documentation/urban_typo_legend.png)



