# The `BLOCK_INDICATORS` table

This table stores all the indicators computed at the block's scale.



## Table definition

![](./images/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | Block's geometry |
| **ID_BLOCK** ![](./images/pk.png) | integer | Block's unique id    |
| ID_RSU | integer | Belonging RSU id |
| ID_ZONE | varchar | Belonging zone id |
| AREA | double precision | Block's area |
| SUM_BUILDING_FLOOR_AREA | double precision |  |
| VOLUME | double precision | Block's volume |
| HOLE_AREA_DENSITY | double precision |  |
| PERKINS_SKILL_SCORE_BUILDING_DIRECTION | double precision |  |
| MAIN_BUILDING_DIRECTION | double precision |  |
| CLOSINGNESS | double precision |  |
| NET_COMPACITY | double precision |  |
| WEIGHTED_AVG_HEIGHT_ROOF_AREA | double precision |  |
| WEIGHTED_STD_HEIGHT_ROOF_AREA | double precision |  |





