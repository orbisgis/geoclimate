# The `BLOCK_INDICATORS` table

This table stores all the indicators computed at the block's scale.

![](./images/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BlockIndicators.groovy).

## Table definition ![](./images/icons/table.png)

![](./images/icons/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | Block's geometry |
| **ID_BLOCK** ![](./images/icons/pk.png) | integer | Block's unique id    |
| ID_RSU | integer | Belonging RSU id |
| SUM_AREA | double precision | Block's area ([Full definition](#SUM_AREA)) |
| SUM_FLOOR_AREA | double precision | [Full definition](#SUM_FLOOR_AREA) |
| SUM_VOLUME | double precision | Block's volume ([Full definition](#SUM_VOLUME)) |
| HOLE_AREA_DENSITY | double precision |  [Full definition](#HOLE_AREA_DENSITY) |
| PERKINS_SKILL_SCORE_BUILDING_DIRECTION | double precision |  [Full definition](#PERKINS_SKILL_SCORE_BUILDING_DIRECTION) |
| MAIN_BUILDING_DIRECTION | double precision |  [Full definition](#MAIN_BUILDING_DIRECTION) |
| CLOSINGNESS | double precision |  [Full definition](#CLOSINGNESS) |
| NET_COMPACTNESS | double precision | [Full definition](#NET_COMPACTNESS) |
| WEIGHTED_AVG_HEIGHT_ROOF_AREA | double precision |  [Full definition](#WEIGHTED_AVG_HEIGHT_ROOF_AREA) |
| WEIGHTED_STD_HEIGHT_ROOF_AREA | double precision |  [Full definition](#WEIGHTED_STD_HEIGHT_ROOF_AREA) |



## Indicators definition  ![](./images/icons/dico.png)



### `SUM_AREA`

Definition: Block's area.

Formula: `xxxxxxx`

Source code: 



### `SUM_FLOOR_AREA`

Definition: 

Formula: `xxxxxxx`

Source code: 



### `SUM_VOLUME`

Definition: Building's volume.

Formula: `AREA * ((HEIGHT_WALL + HEIGHT_ROOF)/2)`

Source code: 



### `HOLE_AREA_DENSITY`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `PERKINS_SKILL_SCORE_BUILDING_DIRECTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `MAIN_BUILDING_DIRECTION`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `CLOSINGNESS`

Definition: 

Formula: `xxxxxxx`

Source code: 

### `NET_COMPACTNESS`

Definition: 

Formula: `xxxxxxx`

Source code: 



### `WEIGHTED_AVG_HEIGHT_ROOF_AREA`

Definition: 

Formula: `xxxxxxx`

Source code: 



### `WEIGHTED_STD_HEIGHT_ROOF_AREA`

Definition: 

Formula: `xxxxxxx`

Source code: 
