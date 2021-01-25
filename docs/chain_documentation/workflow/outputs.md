# Workflow outputs

Below are presented the 5 potential output tables, whose structure may vary according to the application parameters chosen when executing the [workflow](./description.md).

- [Building's indicators](#The-`BUILDING_INDICATORS`-table)
- [Block's indicators](#The-`BLOCK_INDICATORS`-table)
- [RSU's indicators](#The-`RSU_INDICATORS`-table)
- [LCZ classification](#The-`RSU_LCZ`-table)
- [Urban Typology](#The-`RSU_URBAN_TYPO`-table)



![output_data_tables](../../resources/images/chain_documentation/output_data_tables.png)

### Remarks

#### Application

For each indicator we indicate in column `App` whether it is used (and so returned) for the TEB (noted `T`), LCZ (noted `L`) and/or Urban Typology (noted `U`) application.

#### Output tables

In addition to the five potential tables presented below, the user will get 7 tables corresponding to the [input data](../input_data_model.md) of the model (`INPUT_BUILDING`, `INPUT_ROAD`, `INPUT_RAILS`, `INPUT_VEGET`, `INPUT_HYDRO`, `INPUT_IMPERVIOUS` and `ZONE`). These tables can be very interesting if you want to compare the results with the input data and see what corrections and additions have been made.

#### Buildings inside or outside the study area

For the purposes of some indicators (*e.g.* "minimum distance to another buildings"), we need to take into account buildings within a certain radius (*e.g*., 500 metres). 
Consequently, in the `INPUT_BUILDING` table (but also in the two resulting tables `BUILDING_INDICATORS`, `BLOCK_INDICATORS`), we have buildings that are outside the study area (the city).

![building_inside_or_outside](../../resources/images/chain_documentation/workflow_output_building_inside_or_outside.png)

The buildings are outside the studied zone:

- in the `INPUT_BUIDLING` table, when the column `id_zone` = `outside`
- in the `BUILDING_INDICATORS` or `BLOCK_INDICATORS` table, when the column `id_rsu` is null



## The `BUILDING_INDICATORS` table

This table stores all the indicators computed at the building's scale (and described [here](../indicators/building.md)).

![](../../resources/images/common/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/v1.0.0-RC1/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BuildingIndicators.groovy).

![](../../resources/images/common/icons/pk.png) PK = Primary Key

| Field name                                                   | Field type       | Definition                                          | App  |
| :----------------------------------------------------------- | :--------------- | --------------------------------------------------- | :--: |
| THE_GEOM                                                     | geometry         | Building's geometry                                 |      |
| **ID_BUILD** ![](../../resources/images/common/icons/pk.png) | integer          | Building's unique id                                |      |
| ID_SOURCE                                                    | varchar          | Original unique id                                  |      |
| ID_BLOCK                                                     | integer          | Belonging block id                                  |      |
| ID_RSU                                                       | integer          | Belonging RSU id                                    |      |
| HEIGHT_WALL                                                  | integer          | Wall height                                         |      |
| HEIGHT_ROOF                                                  | integer          | Roof height                                         |      |
| NB_LEV                                                       | integer          | Number of level                                     |      |
| TYPE                                                         | varchar          | Building's type                                     |      |
| MAIN_USE                                                     | varchar          | Building's main use                                 |  U   |
| ZINDEX                                                       | integer          | Altimetric positioning ([See](../indicators/building.md#ZINDEX)) |      |
| PERIMETER                                                    | DOUBLE | Building's perimeter                                |  U   |
| AREA                                                         | DOUBLE | Building's area (footprint)                         |  U   |
| VOLUME                                                       | DOUBLE | Building's volume ([See](../indicators/building.md#VOLUME))      |  U   |
| FLOOR_AREA                                                   | DOUBLE | [See](../indicators/building.md#FLOOR_AREA)                      |  U   |
| TOTAL_FACADE_LENGTH                                          | DOUBLE | [See](../indicators/building.md#TOTAL_FACADE_LENGTH)             |  U   |
| COMMON_WALL_FRACTION                                         | DOUBLE | [See](../indicators/building.md#COMMON_WALL_FRACTION)            |  U   |
| CONTIGUITY                                                   | DOUBLE | [See](../indicators/building.md#CONTIGUITY)                      |  U   |
| AREA_CONCAVITY                                               | DOUBLE | [See](../indicators/building.md#AREA_CONCAVITY)                  |  U   |
| FORM_FACTOR                                                  | DOUBLE | [See](../indicators/building.md#FORM_FACTOR)                     |  U   |
| RAW_COMPACTNESS                                              | DOUBLE | [See](../indicators/building.md#RAW_COMPACTNESS)                 |  U   |
| PERIMETER_CONVEXITY                                          | DOUBLE | [See](../indicators/building.md#PERIMETER_CONVEXITY)         |      |
| MINIMUM_BUILDING_SPACING                                     | DOUBLE | [See](../indicators/building.md#MINIMUM_BUILDING_SPACING)        |  U   |
| NUMBER_BUILDING_NEIGHBOR                                     | bigint           | [See](../indicators/building.md#NUMBER_BUILDING_NEIGHBOR)        |  U   |
| ROAD_DISTANCE                                                | DOUBLE | [See](../indicators/building.md#ROAD_DISTANCE)                   |      |
| LIKELIHOOD_LARGE_BUILDING                                    | DOUBLE | [See](../indicators/building.md#LIKELIHOOD_LARGE_BUILDING)       |  U   |

[Back to top](#Workflow-outputs)

## The `BLOCK_INDICATORS` table

This table stores all the indicators computed at the block's scale (and described [here](../indicators/block.md)).

![](../../resources/images/common/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/v1.0.0-RC1/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BlockIndicators.groovy).

![](../../resources/images/common/icons/pk.png) PK = Primary Key

| Field name                                                   | Field type       | Definition                                            | App  |
| :----------------------------------------------------------- | :--------------- | ----------------------------------------------------- | :--: |
| THE_GEOM                                                     | geometry         | Block's geometry                                      |      |
| **ID_BLOCK** ![](../../resources/images/common//icons/pk.png) | integer          | Block's unique id                                     |      |
| ID_RSU                                                       | integer          | Belonging RSU id                                      |      |
| AREA                                                         | DOUBLE | Block's area ([See](../indicators/block.md#AREA))     |  U   |
| FLOOR_AREA                                                   | DOUBLE | [See](../indicators/block.md#FLOOR_AREA)              |  U   |
| VOLUME                                                       | DOUBLE | Block's volume ([See](../indicators/block.md#VOLUME)) |  U   |
| HOLE_AREA_DENSITY                                            | DOUBLE | [See](../indicators/block.md#HOLE_AREA_DENSITY)                             |  U   |
| MAIN_BUILDING_DIRECTION                                      | DOUBLE | [See](../indicators/block.md#MAIN_BUILDING_DIRECTION)                       |  U   |
| INEQUALITY_BUILDING_DIRECTION                                | DOUBLE | [See](../indicators/block.md#INEQUALITY_BUILDING_DIRECTION)                 |  U   |
| CLOSINGNESS                                                  | DOUBLE | [See](../indicators/block.md#CLOSINGNESS)                                   |  U   |
| NET_COMPACTNESS                                              | DOUBLE | [See](../indicators/block.md#NET_COMPACTNESS)                               |  U   |
| AVG_HEIGHT_ROOF_AREA_WEIGHTED                                | DOUBLE | [See](../indicators/block.md#AVG_HEIGHT_ROOF_AREA_WEIGHTED)                 |  U   |
| STD_HEIGHT_ROOF_AREA_WEIGHTED                                | DOUBLE | [See](../indicators/block.md#STD_HEIGHT_ROOF_AREA_WEIGHTED)                 |  U   |

[Back to top](#Workflow-outputs)

## The `RSU_INDICATORS` table

This table stores all the indicators computed at the RSU's scale (and described [here](../indicators/rsu.md)).

![](../../resources/images/common/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/v1.0.0-RC1/geoindicators/src/main/groovy/org/orbisgis/geoindicators/RsuIndicators.groovy).

![](../../resources/images/common/icons/pk.png) PK = Primary Key

| Field name                                                 |    Field type    |                      Definition                      |   App   |
| :--------------------------------------------------------- | :--------------: | :--------------------------------------------------: | :-----: |
| THE_GEOM                                                   |     geometry     |                    RSU's geometry                    |         |
| **ID_RSU** ![](../../resources/images/common/icons/pk.png) |     integer      |                   RSU's unique id                    |         |
| AREA                                                       | DOUBLE |                      RSU's area                      |    U    |
| BUILDING_AREA_FRACTION                                     | DOUBLE |  [See](../indicators/rsu.md#BUILDING_AREA_FRACTION)  | L, T, U |
| BUILDING_FLOOR_AREA_DENSITY                                | DOUBLE |         [See](../indicators/rsu.md#BUILDING_FLOOR_AREA_DENSITY)          |    U    |
| BUILDING_VOLUME_DENSITY                                    | DOUBLE |           [See](../indicators/rsu.md#BUILDING_VOLUME_DENSITY)            |    U    |
| AVG_VOLUME_AREA_WEIGHTED                                   | DOUBLE |           [See](../indicators/rsu.md#AVG_VOLUME_AREA_WEIGHTED)           |    U    |
| GEOM_AVG_HEIGHT_ROOF                                       | DOUBLE |             [See](../indicators/rsu.md#GEOM_AVG_HEIGHT_ROOF)             |         |
| AVG_HEIGHT_ROOF_AREA_WEIGHTED                              | DOUBLE |        [See](../indicators/rsu.md#AVG_HEIGHT_ROOF_AREA_WEIGHTED)         |    U    |
| STD_HEIGHT_ROOF_AREA_WEIGHTED                              | DOUBLE |        [See](../indicators/rsu.md#STD_HEIGHT_ROOF_AREA_WEIGHTED)         |    U    |
| FREE_EXTERNAL_FACADE_DENSITY                               | DOUBLE |         [See](../indicators/rsu.md#FREE_EXTERNAL_FACADE_DENSITY)         |    U    |
| NON_VERT_ROOF_AREA_H0_10                                   | DOUBLE |           [See](../indicators/rsu.md#NON_VERT_ROOF_AREA_Hx_y)            |    T    |
| NON_VERT_ROOF_AREA_H10_20                                  | DOUBLE |                                                      |    T    |
| NON_VERT_ROOF_AREA_H20_30                                  | DOUBLE |                                                      |    T    |
| NON_VERT_ROOF_AREA_H30_40                                  | DOUBLE |                                                      |    T    |
| NON_VERT_ROOF_AREA_H40_50                                  | DOUBLE |                                                      |    T    |
| NON_VERT_ROOF_AREA_H50                                     | DOUBLE |                                                      |    T    |
| NON_VERT_ROOF_DENSITY                                      | DOUBLE |            [See](../indicators/rsu.md#NON_VERT_ROOF_DENSITY)             |    T    |
| VERT_ROOF_AREA_H0_10                                       | DOUBLE |            [See](../indicators/rsu.md#VERT_ROOF_AREA_Hxx_xx)             |    T    |
| VERT_ROOF_AREA_H10_20                                      | DOUBLE |                                                      |    T    |
| VERT_ROOF_AREA_H20_30                                      | DOUBLE |                                                      |    T    |
| VERT_ROOF_AREA_H30_40                                      | DOUBLE |                                                      |    T    |
| VERT_ROOF_AREA_H40_50                                      | DOUBLE |                                                      |    T    |
| VERT_ROOF_AREA_H50                                         | DOUBLE |                                                      |    T    |
| VERT_ROOF_DENSITY                                          | DOUBLE |              [See](../indicators/rsu.md#VERT_ROOF_DENSITY)               |    T    |
| AVG_MINIMUM_BUILDING_SPACING                               | DOUBLE |         [See](../indicators/rsu.md#AVG_MINIMUM_BUILDING_SPACING)         |    U    |
| AVG_NUMBER_BUILDING_NEIGHBOR                               | DOUBLE |         [See](../indicators/rsu.md#AVG_NUMBER_BUILDING_NEIGHBOR)         |    U    |
| BUILDING_NUMBER_DENSITY                                    | DOUBLE |           [See](../indicators/rsu.md#BUILDING_NUMBER_DENSITY)            |    U    |
| MAIN_BUILDING_DIRECTION                                    |     integer      |           [See](../indicators/rsu.md#MAIN_BUILDING_DIRECTION)            |         |
| INEQUALITY_BUILDING_DIRECTION                              | DOUBLE |        [See](../indicators/rsu.md#INEQUALITY_BUILDING_DIRECTION)         |    U    |
| UNIQUENESS_BUILDING_DIRECTION                              | DOUBLE |        [See](../indicators/rsu.md#UNIQUENESS_BUILDING_DIRECTION)         |         |
| ASPECT_RATIO                                               | DOUBLE |                 [See](../indicators/rsu.md#ASPECT_RATIO)                 |    L    |
| GROUND_SKY_VIEW_FACTOR                                     | DOUBLE |            [See](../indicators/rsu.md#GROUND_SKY_VIEW_FACTOR)            |    L    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D0_30             | DOUBLE | [See](../indicators/rsu.md#PROJECTED_FACADE_AREA_DISTRIBUTION_Hx_y_Dw_z) |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D0_30            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D0_30            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D0_30            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D0_30            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D0_30               | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D30_60            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D30_60           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D30_60           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D30_60           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D30_60           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D30_60              | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D60_90            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D60_90           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D60_90           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D60_90           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D60_90           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D60_90              | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D90_120           | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D90_120          | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D90_120          | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D90_120          | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D90_120          | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D90_120             | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D120_150          | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D120_150         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D120_150         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D120_150         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D120_150         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D120_150            | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D150_180          | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D150_180         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D150_180         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D150_180         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D150_180         | DOUBLE |                                                      |    T    |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D150_180            | DOUBLE |                                                      |    T    |
| EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH                         | DOUBLE |      [See](../indicators/rsu.md#EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH)      |    T    |
| EFFECTIVE_TERRAIN_ROUGHNESS_CLASS                          |     integer      |      [See](../indicators/rsu.md#EFFECTIVE_TERRAIN_ROUGHNESS_CLASS)       |    L    |
| ROAD_DIRECTION_DISTRIBUTION_H0_D0_30                       | DOUBLE |     [See](../indicators/rsu.md#ROAD_DIRECTION_DISTRIBUTION_H0_Dw_z)      |         |
| ROAD_DIRECTION_DISTRIBUTION_H0_D30_60                      | DOUBLE |                                                      |         |
| ROAD_DIRECTION_DISTRIBUTION_H0_D60_90                      | DOUBLE |                                                      |         |
| ROAD_DIRECTION_DISTRIBUTION_H0_D90_120                     | DOUBLE |                                                      |         |
| ROAD_DIRECTION_DISTRIBUTION_H0_D120_150                    | DOUBLE |                                                      |         |
| ROAD_DIRECTION_DISTRIBUTION_H0_D150_180                    | DOUBLE |                                                      |         |
| LINEAR_ROAD_DENSITY_H0                                     | DOUBLE |            [See](../indicators/rsu.md#LINEAR_ROAD_DENSITY_H0)            |    U    |
| GROUND_ROAD_FRACTION                                       | DOUBLE |             [See](../indicators/rsu.md#GROUND_ROAD_FRACTION)             |    U    |
| OVERGROUND_ROAD_FRACTION                                   | DOUBLE |           [See](../indicators/rsu.md#OVERGROUND_ROAD_FRACTION)           |    U    |
| WATER_FRACTION                                             | DOUBLE |                [See](../indicators/rsu.md#WATER_FRACTION)                |    U    |
| LOW_VEGETATION_FRACTION                                    | DOUBLE |           [See](../indicators/rsu.md#LOW_VEGETATION_FRACTION)            |    T    |
| HIGH_VEGETATION_FRACTION                                   | DOUBLE |           [See](../indicators/rsu.md#HIGH_VEGETATION_FRACTION)           |    T    |
| ALL_VEGETATION_FRACTION                                    | DOUBLE |           [See](../indicators/rsu.md#ALL_VEGETATION_FRACTION)            |  U, T   |
| PERVIOUS_FRACTION                                          | DOUBLE |              [See](../indicators/rsu.md#PERVIOUS_FRACTION)               |    L    |
| IMPERVIOUS_FRACTION                                        | DOUBLE |             [See](../indicators/rsu.md#IMPERVIOUS_FRACTION)              |    L    |

[Back to top](#Workflow-outputs)

## The `RSU_LCZ` table

This table stores the result of the LCZ classification (described [here](../classsifications/lcz.md)) computed at the RSU's scale.

![](../../resources/images/common/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/v1.0.0-RC1/geoindicators/src/main/groovy/org/orbisgis/geoindicators/RsuIndicators.groovy).

![](../../resources/images/common/icons/pk.png) PK = Primary Key

| Field name                                                 | Field type       | Definition                   |
| :--------------------------------------------------------- | :--------------- | ---------------------------- |
| **ID_RSU** ![](../../resources/images/common/icons/pk.png) | integer          | RSU's unique id              |
| LCZ1                                                       | integer          | Main LCZ type                |
| LCZ2                                                       | integer          | Secondary LCZ type           |
| MIN_DISTANCE                                               | DOUBLE | Minimum distance to each LCZ |
| PSS                                                        | DOUBLE | *Perkin Skill Score*         |

[Back to top](#Workflow-outputs)

## The `RSU_URBAN_TYPO` table

This table stores the result of the Urban typology (described [here](../classsifications/territorial_typologies.md)) computed at the RSU's scale.

![](../../resources/images/common/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/v1.0.0-RC1/geoindicators/src/main/groovy/org/orbisgis/geoindicators/RsuIndicators.groovy).

![](../../resources/images/common/icons/pk.png) PK = Primary Key

| Field name                                                 | Field type       | Definition      |
| :--------------------------------------------------------- | :--------------- | --------------- |
| **ID_RSU** ![](../../resources/images/common/icons/pk.png) | integer          | RSU's unique id |
| xxx                                                        | integer          | xxx             |
| xxx                                                        | DOUBLE | xxx             |

[Back to top](#Workflow-outputs)
