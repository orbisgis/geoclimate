# The `RSU_INDICATORS` table

This table stores all the indicators computed at the RSU's scale.



## Table definition

![](/home/gpetit/Documents/Codes/geoclimate/docs/images/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | RSU's geometry    |
| **ID_RSU** ![](/home/gpetit/Documents/Codes/geoclimate/docs/images/pk.png) | integer | RSU's unique id    |
| ID_ZONE | varchar | Belonging zone id |
| FREE_EXTERNAL_FACADE_DENSITY | double precision |  |
| HEIGHT_OF_ROUGHNESS_ELEMENTS | double precision |  |
| BUILDING_SURFACE_FRACTION | double precision |  |
| GROUND_ROAD_FRACTION | double precision |  |
| WATER_FRACTION | double precision |  |
| LOW_VEGETATION_FRACTION | double precision |  |
| HIGH_VEGETATION_FRACTION | double precision |  |
| ALL_VEGETATION_FRACTION | double precision |  |
| PROJECTED_FACADE_AREA_DISTRIBUTION0_10D15 | double precision |  |
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
| PROJECTED_FACADE_AREA_DISTRIBUTION50_D165 | double precision |  |
| ASPECT_RATIO | double precision |  |
| SKY_VIEW_FACTOR | double precision | Sky View Factor (SVF) |
| PERVIOUS_SURFACE_FRACTION | double precision |  |
| IMPERVIOUS_SURFACE_FRACTION | double precision |  |
| EFFECTIVE_TERRAIN_ROUGHNESS | double precision |  |
| TERRAIN_ROUGHNESS_CLASS | integer |  |


