# The `RSU_INDICATORS` table

This table stores all the indicators computed at the RSU's scale.

![](../images/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/RsuIndicators.groovy).

## Table definition ![](../images/icons/table.png)

![](../images/icons/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------: | :----------------: |
| THE_GEOM       | geometry          | RSU's geometry    |
| **ID_RSU** ![](../images/icons/pk.png) | integer | RSU's unique id    |
| AREA | double precision | RSU's area |
| BUILDING_AREA_FRACTION | double precision | [Full definition](#BUILDING_AREA_FRACTION) |
| BUILDING_FLOOR_AREA_DENSITY | double precision | [Full definition](#BUILDING_FLOOR_AREA_DENSITY) |
| BUILDING_VOLUME_DENSITY | double precision | [Full definition](#BUILDING_VOLUME_DENSITY) |
| AVG_VOLUME_AREA_WEIGHTED | double precision | [Full definition](#AVG_VOLUME_AREA_WEIGHTED) |
| GEOM_AVG_HEIGHT_ROOF | double precision | [Full definition](#GEOM_AVG_HEIGHT_ROOF) |
| AVG_HEIGHT_ROOF_AREA_WEIGHTED | double precision | [Full definition](#AVG_HEIGHT_ROOF_AREA_WEIGHTED) |
| STD_HEIGHT_ROOF_AREA_WEIGHTED | double precision | [Full definition](#STD_HEIGHT_ROOF_AREA_WEIGHTED) |
| FREE_EXTERNAL_FACADE_DENSITY | double precision | [Full definition](#FREE_EXTERNAL_FACADE_DENSITY) |
| NON_VERT_ROOF_AREA_H0_10 | double precision | [Full definition](#NON_VERT_ROOF_AREA_Hxx_xx) |
| NON_VERT_ROOF_AREA_H10_20 | double precision |  |
| NON_VERT_ROOF_AREA_H20_30 | double precision |  |
| NON_VERT_ROOF_AREA_H30_40 | double precision |  |
| NON_VERT_ROOF_AREA_H40_50 | double precision |  |
| NON_VERT_ROOF_AREA_H50 | double precision |  |
| NON_VERT_ROOF_DENSITY | double precision | [Full definition](#NON_VERT_ROOF_DENSITY) |
| VERT_ROOF_AREA_H0_10 | double precision | [Full definition](#VERT_ROOF_AREA_Hxx_xx) |
| VERT_ROOF_AREA_H10_20 | double precision |  |
| VERT_ROOF_AREA_H20_30 | double precision |  |
| VERT_ROOF_AREA_H30_40 | double precision |  |
| VERT_ROOF_AREA_H40_50 | double precision |  |
| VERT_ROOF_AREA_H50 | double precision |  |
| VERT_ROOF_DENSITY | double precision | [Full definition](#VERT_ROOF_DENSITY) |
| AVG_MINIMUM_BUILDING_SPACING | double precision | [Full definition](#AVG_MINIMUM_BUILDING_SPACING) |
| AVG_NUMBER_BUILDING_NEIGHBOR | double precision | [Full definition](#AVG_NUMBER_BUILDING_NEIGHBOR) |
| BUILDING_NUMBER_DENSITY | double precision | [Full definition](#BUILDING_NUMBER_DENSITY) |
| MAIN_BUILDING_DIRECTION | integer | [Full definition](#MAIN_BUILDING_DIRECTION) |
| PERKINS_SKILL_SCORE_BUILDING_DIRECTION | double precision | [Full definition](#PERKINS_SKILL_SCORE_BUILDING_DIRECTION) |
| ASPECT_RATIO | double precision | [Full definition](#ASPECT_RATIO) |
| GROUND_SKY_VIEW_FACTOR | double precision | [Full definition](#GROUND_SKY_VIEW_FACTOR) |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D0_30     | double precision | [Full definition](#PROJECTED_FACADE_AREA_DISTRIBUTION_Hxx_xx_Dxx_xx) |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D0_30    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D0_30    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D0_30    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D0_30    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D0_30       | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D30_60    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D30_60   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D30_60   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D30_60   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D30_60   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D30_60      | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D60_90    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D60_90   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D60_90   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D60_90   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D60_90   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D60_90      | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D90_120   | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D90_120  | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D90_120  | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D90_120  | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D90_120  | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D90_120     | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D120_150  | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D120_150 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D120_150 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D120_150 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D120_150 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D120_150    | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D150_180  | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D150_180 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D150_180 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D150_180 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D150_180 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D150_180    | double precision |  |
| EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH | double precision | [Full definition](#EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH) |
| EFFECTIVE_TERRAIN_ROUGHNESS_CLASS | integer | [Full definition](#EFFECTIVE_TERRAIN_ROUGHNESS_CLASS) |
| ROAD_DIRECTION_DISTRIBUTION_H0_D0_30 | double precision | [Full definition](#ROAD_DIRECTION_DISTRIBUTION_H0_Dxx_xx) |
| ROAD_DIRECTION_DISTRIBUTION_H0_D30_60 | double precision |  |
| ROAD_DIRECTION_DISTRIBUTION_H0_D60_90 | double precision |  |
| ROAD_DIRECTION_DISTRIBUTION_H0_D90_120 | double precision |  |
| ROAD_DIRECTION_DISTRIBUTION_H0_D120_150 | double precision |  |
| ROAD_DIRECTION_DISTRIBUTION_H0_D150_180 | double precision |  |
| GROUND_LINEAR_ROAD_DENSITY | double precision | [Full definition](#GROUND_LINEAR_ROAD_DENSITY) |
| GROUND_ROAD_FRACTION | double precision | [Full definition](#GROUND_ROAD_FRACTION) |
| OVERGROUND_ROAD_FRACTION | double precision | [Full definition](#OVERGROUND_ROAD_FRACTION) |
| WATER_FRACTION | double precision | [Full definition](#WATER_FRACTION) |
| LOW_VEGETATION_FRACTION | double precision | [Full definition](#LOW_VEGETATION_FRACTION) |
| HIGH_VEGETATION_FRACTION | double precision | [Full definition](#HIGH_VEGETATION_FRACTION) |
| ALL_VEGETATION_FRACTION | double precision | [Full definition](#ALL_VEGETATION_FRACTION) |
| PERVIOUS_FRACTION | double precision | [Full definition](#PERVIOUS_FRACTION) |
| IMPERVIOUS_FRACTION | double precision | [Full definition](#IMPERVIOUS_FRACTION) |


## Indicators definition  ![](../images/icons/dico.png)



### `FREE_EXTERNAL_FACADE_DENSITY`

Definition: 

Formula: `xxxxxxx`

Source code: 


### `HEIGHT_OF_ROUGHNESS_ELEMENTS`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `BUILDING_SURFACE_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `GROUND_ROAD_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `WATER_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `LOW_VEGETATION_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `HIGH_VEGETATION_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `ALL_VEGETATION_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `PROJECTED_FACADE_AREA_DISTRIBUTIONx_xxx`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `ASPECT_RATIO`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `SKY_VIEW_FACTOR`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `PERVIOUS_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `IMPERVIOUS_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `EFFECTIVE_TERRAIN_ROUGHNESS`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `TERRAIN_ROUGHNESS_CLASS`

Definition: 

Formula: `xxxxxxx`

Source code: 
