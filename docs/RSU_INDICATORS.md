# The `RSU_INDICATORS` table

This table stores all the indicators computed at the RSU's scale.

![](./images/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/RsuIndicators.groovy).

## Table definition ![](./images/icons/table.png)

![](./images/icons/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | RSU's geometry    |
| **ID_RSU** ![](./images/icons/pk.png) | integer | RSU's unique id    |
| ID_ZONE | varchar | Belonging zone id |
| FREE_EXTERNAL_FACADE_DENSITY | double precision | [Full definition](#FREE_EXTERNAL_FACADE_DENSITY) |
| HEIGHT_OF_ROUGHNESS_ELEMENTS | double precision | [Full definition](#HEIGHT_OF_ROUGHNESS_ELEMENTS) |
| BUILDING_SURFACE_FRACTION | double precision | [Full definition](#BUILDING_SURFACE_FRACTION) |
| GROUND_ROAD_FRACTION | double precision | [Full definition](#GROUND_ROAD_FRACTION) |
| WATER_FRACTION | double precision | [Full definition](#WATER_FRACTION) |
| LOW_VEGETATION_FRACTION | double precision | [Full definition](#LOW_VEGETATION_FRACTION) |
| HIGH_VEGETATION_FRACTION | double precision | [Full definition](#HIGH_VEGETATION_FRACTION) |
| ALL_VEGETATION_FRACTION | double precision | [Full definition](#ALL_VEGETATION_FRACTION) |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D15 | double precision | [Full definition](#PROJECTED_FACADE_AREA_DISTRIBUTIONx_xxx) |
| PROJECTED_FACADE_AREA_DISTRIBUTION10_20D15 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION20_30D15 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION30_40D15 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION40_50D15 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D15 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D45 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION10_20D45 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION20_30D45 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION30_40D45 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION40_50D45 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D45 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D75 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION10_20D75 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION20_30D75 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION30_40D75 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION40_50D75 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D75 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D105 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION10_20D105 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION20_30D105 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION30_40D105 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION40_50D105 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D105 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D135 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION10_20D135 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION20_30D135 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION30_40D135 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION40_50D135 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D135 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D165 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION10_20D165 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION20_30D165 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION30_40D165 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION40_50D165 | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D165 | double precision | [Full definition](#xxxxxx) |
| ASPECT_RATIO | double precision | [Full definition](#ASPECT_RATIO) |
| SKY_VIEW_FACTOR | double precision | Sky View Factor ([Full definition](#SKY_VIEW_FACTOR)) |
| PERVIOUS_SURFACE_FRACTION | double precision | [Full definition](#PERVIOUS_SURFACE_FRACTION) |
| IMPERVIOUS_SURFACE_FRACTION | double precision | [Full definition](#IMPERVIOUS_SURFACE_FRACTION) |
| EFFECTIVE_TERRAIN_ROUGHNESS | double precision | [Full definition](#EFFECTIVE_TERRAIN_ROUGHNESS) |
| TERRAIN_ROUGHNESS_CLASS | integer | [Full definition](#TERRAIN_ROUGHNESS_CLASS) |


## Indicators definition  ![](./images/icons/dico.png)



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

### `LOW_VEGETATION_FRACTION`

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

### `PERVIOUS_SURFACE_FRACTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `IMPERVIOUS_SURFACE_FRACTION`

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
