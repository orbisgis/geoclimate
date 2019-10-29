# Module 2 - Formating and quality control



The purpose of this module is to format the data from M1 in order to feed M3. As such, it allows the following two tasks in particular:
- enrich the data, in particular when there is missing values, based on pre-established rules,
- control data quality.



## Data enrichment



### Primary Key ![](../images/icons/pk.png)

All the `input_table` coming from M1 have to have an `id_source` column, that identify the input object. Since this *id* is stored as a text value (`VARCHAR`) we prefer to add a new unique and optimized *id* called `id_xxxx` (with `xxxx` the name of the layer - *e.g* `id_building`) and stored as a numeric (INTEGER /  SERIAL). In the same time, the PRIMARY KEY constraint is added in order to create an `INDEX` on this column (will be useful for the following processes).



### Building rules ![](../images/icons/building.png)



#### Belonging zone Id

Since the input buildings are selected on a larger area than the studied zone (see [`ZONE`](../input_data/INPUT_DATA_MODEL.md#Zones) layer), we're going to have buildings:
- inside the study area, 
- outside the study area,
- or straddling one or more areas.

For the optimization of upcoming processes, it is necessary to assign to each building the id (`id_zone`) of the belonging zone.

To determine this matching, we apply the "maximum area" rule, defined as follows:

> The building is associated to the zone with which it has the largest intersection area 



#### HEIGHTs and Number of levels 

In the layer `input_building` coming from M1, the fields `HEIGHT_WALL`, `HEIGHT_ROOF` and `NB_LEV` may have null values. For the upcoming processes (e.g when calculating morphological indicators), it may causes problems. So to resolve this issue, we propose a set of logical rules in order to deduce probable values, using `type` and potentially existing informations in the fields `HEIGHT_WALL`, `HEIGHT_ROOF` and `NB_LEV`.

##### BUILDING level *vs* type

These logical rules will depends on the building `type`. Indeed, we consider that a castle, a commercial area or a house should not be considered in the same way.

So, for each individual value concerning the building `type`  (values listed in the [BUILDING_use and type](#BUILDING-use-and-type) section), the following list specifies the rules for calculating the number of levels of a building in order to feed the `nb_lvl` field in the `input_building` table.

- **Type**: value used to describe the building `type`
- **Nb_lev_rule**: Specifies whether or not the building type is taken into account when calculating the number of levels:
  - `0` = not taken into account: in this case, the number of levels will be forced to 1,
  - `1`= taken into account: in this case, a formula will be used to deduce the number,
  - `2` = other situtation : we apply a specific rule.

|              Type               | Nb_lev_rule |
| :-----------------------------: | :---------: |
|            building             |      1      |
|              house              |      1      |
|            detached             |      1      |
|           residential           |      1      |
|           apartments            |      1      |
|            bungalow             |      0      |
|            historic             |      0      |
|            monument             |      0      |
|              ruins              |      0      |
|             castle              |      0      |
|          agricultural           |      0      |
|              farm               |      0      |
|         farm_auxiliary          |      0      |
|              barn               |      0      |
|           greenhouse            |      0      |
|              silo               |      0      |
|           commercial            |      2      |
|           industrial            |      0      |
|              sport              |      0      |
|          sports_centre          |      0      |
|           grandstand            |      0      |
|         transportation          |      0      |
|          train_station          |      0      |
|           toll_booth            |      0      |
|            terminal             |      0      |
|           healthcare            |      1      |
|            education            |      1      |
| entertainment, arts and culture |      0      |
|           sustenance            |      1      |
|            military             |      0      |
|            religious            |      0      |
|             chapel              |      0      |
|             church              |      0      |
|           government            |      1      |
|            townhall             |      1      |
|             office              |      1      |



##### Logical rules









```
UPDATE $BUILDING SET HEIGHT_WALL = CASE WHEN HEIGHT_WALL is null or HEIGHT_WALL = 0 THEN(CASE WHEN HEIGHT_ROOF is null or HEIGHT_ROOF = 0 THEN (CASE WHEN NB_LEV is null or NB_LEV = 0 THEN $H_LEV_MIN ELSE (NB_LEV*$H_LEV_MIN) END) ELSE HEIGHT_ROOF END) ELSE HEIGHT_WALL END;-- Update HEIGHT_ROOFUPDATE $BUILDING SET HEIGHT_ROOF = CASE WHEN HEIGHT_ROOF is null or HEIGHT_ROOF = 0 THEN (CASE WHEN HEIGHT_WALL is null or HEIGHT_WALL = 0 THEN (CASE WHEN NB_LEV is null or NB_LEV = 0 THEN $H_LEV_MIN ELSE (NB_LEV*$H_LEV_MIN) END) ELSE HEIGHT_WALL END) ELSE HEIGHT_ROOF END;-- Update NB_LEV-- If the NB_LEV parameter (in the abstract table) is equal to 1 or 2 (and HEIGHT_WALL > 10m) then apply the rule. Else, the NB_LEV is equal to 1UPDATE $BUILDING SET NB_LEV = CASE WHEN TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=1) or(TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=2) and HEIGHT_WALL>$H_THRESHOLD_LEV2) THEN(CASE WHEN NB_LEV is null or NB_LEV = 0 THEN (CASE WHEN HEIGHT_WALL is null or HEIGHT_WALL = 0 THEN (CASE WHEN HEIGHT_ROOF is null or HEIGHT_ROOF = 0 THEN 1ELSE TRUNCATE(HEIGHT_ROOF /$H_LEV_MIN) END) ELSE TRUNCATE(HEIGHT_WALL /$H_LEV_MIN) END) ELSE NB_LEV END) ELSE 1 END;-- Control of heights and number of levels----------------------------------------------------------- Check if HEIGHT_ROOF is lower than HEIGHT_WALL. If yes, then correct HEIGHT_ROOFUPDATE $BUILDING SET HEIGHT_ROOF =     CASE WHEN HEIGHT_WALL > HEIGHT_ROOF THEN HEIGHT_WALL ELSE HEIGHT_ROOF END;-- Check if there is a high difference beetween the "real" and "theorical (based on the level number) roof heightsUPDATE $BUILDING SET HEIGHT_ROOF =     CASE WHEN (NB_LEV * $H_LEV_MIN) > HEIGHT_ROOF THEN (NB_LEV * $H_LEV_MIN) ELSE HEIGHT_ROOF END;-- Check if there is a high difference beetween the "real" and "theorical" (based on the level number) wall heightsUPDATE $BUILDING SET NB_LEV =  CASE WHEN TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=1) or (TYPE in (SELECT TERM FROM $BUILDING_ABSTRACT_PARAMETERS WHERE NB_LEV=2) and HEIGHT_WALL>$H_THRESHOLD_LEV2) THEN (                           CASE WHEN (NB_LEV * $H_LEV_MAX) < HEIGHT_WALL THEN (HEIGHT_WALL / $H_LEV_MAX) ELSE NB_LEV END)                        ELSE NB_LEV END;
```



#### Z Index

If the `ZINDEX` is `null`, then it's initialized to `0`.




### Road rules ![](../images/icons/road.png)



### Width 

To calculates indicators such as the `GROUND_ROAD_FRACTION` ([See](../results/RSU_INDICATORS.md)), it is necessary to use the `width` column in the `input_road` layer. In case this information is missing in the input data, we deduce it from the road `type` (values listed in the [ROAD type](#ROAD-type) section) using the following matching table, where:

- **Type**: value used to qualify the `type` of the road
- **Min_width**: minimum road width *(expressed in meter)* to apply, if no information provided

|     Type     | Min_width |
| :----------: | :-------: |
| residential  |     8     |
|    track     |     2     |
| unclassified |     3     |
|   footway    |     1     |
|     path     |     1     |
|   tertiary   |     8     |
|  secondary   |    10     |
|   primary    |    10     |
|   cycleway   |     1     |
|    trunk     |    16     |
|    steps     |     1     |
|   motorway   |    24     |
| highway_link |     8     |
|  roundabout  |     4     |
|   highway    |     8     |
|    ferry     |     0     |





### Vegetation rules ![](../images/icons/vegetation.png)

### Height class 

For Geoclimate purposes, it is necessary to distinguish between `high` and `low` vegetation areas. Since this information is generally not available in the input data, we deduce it from the vegetation `type`.

The table below gives the correspondences between the `type` and the expected height class (`height_class` attribute) in the `input_veget` table.


|     Type      | Height class |
| :-----------: | :----------: |
|     tree      |     high     |
|     wood      |     high     |
|    forest     |     high     |
|     scrub     |     low      |
|   grassland   |     low      |
|     heath     |     low      |
|   tree_row    |     high     |
|     hedge     |     high     |
|   mangrove    |     high     |
|    orchard    |     high     |
|   vineyard    |     low      |
| banana_plants |     high     |
|  sugar_cane   |     low      |





## Quality control

