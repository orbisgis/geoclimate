# The `BUILDING_INDICATORS` table

This table stores all the indicators computed at the building's scale.



## Table definition

![](/home/gpetit/Documents/Codes/geoclimate/docs/images/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | Building's geometry |
| **ID_BUILD** ![](/home/gpetit/Documents/Codes/geoclimate/docs/images/pk.png) | integer | Building's unique id |
| ID_SOURCE | varchar | Original unique id |
| ID_BLOCK | integer | Belonging block id |
| ID_RSU | integer | Belonging RSU id |
| ID_ZONE | varchar | Belonging zone id |
| HEIGHT_WALL | integer |  |
| HEIGHT_ROOF | integer |  |
| NB_LEV | integer | Number of level |
| TYPE | varchar | Building's type |
| MAIN_USE | varchar |  |
| ZINDEX | integer |  |
| AREA | double precision | Building's area |
| VOLUME | double precision | Building's volume ([Full definition](#volume)) |
| FLOOR_AREA | double precision | [Full definition](#floor_area) |
| TOTAL_FACADE_LENGTH | double precision | [Full definition](#floor_area) |
| CONTIGUITY | double precision | [Full definition](#floor_area) |
| COMMON_WALL_FRACTION | double precision | [Full definition](#floor_area) |
| NUMBER_BUILDING_NEIGHBOR | bigint | [Full definition](#floor_area) |
| CONCAVITY | double precision | [Full definition](#floor_area) |
| FORM_FACTOR | double precision | [Full definition](#floor_area) |
| RAW_COMPACITY | double precision | [Full definition](#floor_area) |
| CONVEXHULL_PERIMETER_DENSITY | double precision | [Full definition](#floor_area) |
| MINIMUM_BUILDING_SPACING | double precision | [Full definition](#floor_area) |
| ROAD_DISTANCE | double precision | [Full definition](#floor_area) |
| LIKELIHOOD_LARGE_BUILDING | double precision | [Full definition](#floor_area) |

## Indicators definition



### `VOLUME`

Definition: Building's volume.

Formula: `AREA * ((HEIGHT_WALL + HEIGHT_ROOF)/2)`

Source code: https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BuildingIndicators.groovy#L60

### `FLOOR_AREA`

Definition: Building's floor area.

Formula: `AREA * NB_LEV`

Source code: https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BuildingIndicators.groovy#L63



### `TOTAL_FACADE_LENGTH`

Definition: 

Formula: `xxxxxx`

Source code:



### `CONTIGUITY`

Definition: 

Formula: `xxxxxx`

Source code:



### `COMMON_WALL_FRACTION`

Definition: 

Formula: `xxxxxx`

Source code:



### `NUMBER_BUILDING_NEIGHBOR`

Definition: 

Formula: `xxxxxx`

Source code:



### `CONCAVITY`

Definition: 

Formula: `xxxxxx`

Source code:



### `FORM_FACTOR`

Definition: 

Formula: `xxxxxx`

Source code:



### `RAW_COMPACITY`

Definition: 

Formula: `xxxxxx`

Source code:



### `CONVEXHULL_PERIMETER_DENSITY`

Definition: 

Formula: `xxxxxx`

Source code:



### `MINIMUM_BUILDING_SPACING`

Definition: 

Formula: `xxxxxx`

Source code:



### `ROAD_DISTANCE`

Definition: 

Formula: `xxxxxx`

Source code:



### `LIKELIHOOD_LARGE_BUILDING`

Definition: 

Formula: `xxxxxx`

Source code:

