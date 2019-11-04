# The `BLOCK_INDICATORS` table

This table stores all the indicators computed at the block's scale.

![](../images/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BlockIndicators.groovy).

## Table definition ![](../images/icons/table.png)

![](../images/icons/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | Block's geometry |
| **ID_BLOCK** ![](../images/icons/pk.png) | integer | Block's unique id    |
| ID_RSU | integer | Belonging RSU id |
| AREA | double precision | Block's area ([Full definition](#AREA)) |
| FLOOR_AREA | double precision | [Full definition](#FLOOR_AREA) |
| VOLUME | double precision | Block's volume ([Full definition](#VOLUME)) |
| HOLE_AREA_DENSITY | double precision |  [Full definition](#HOLE_AREA_DENSITY) |
| MAIN_BUILDING_DIRECTION | double precision |  [Full definition](#MAIN_BUILDING_DIRECTION) |
| INEQUALITY_BUILDING_DIRECTION | double precision | [Full definition](#INEQUALITY_BUILDING_DIRECTION) |
| CLOSINGNESS | double precision |  [Full definition](#CLOSINGNESS) |
| NET_COMPACTNESS | double precision | [Full definition](#NET_COMPACTNESS) |
| AVG_HEIGHT_ROOF_AREA_WEIGHTED | double precision |  [Full definition](#AVG_HEIGHT_ROOF_AREA_WEIGHTED) |
| STD_HEIGHT_ROOF_AREA_WEIGHTED | double precision |  [Full definition](#STD_HEIGHT_ROOF_AREA_WEIGHTED) |

[back to top](#the-block_indicators-table)



## Indicators definition  ![](../images/icons/dico.png)


### `AREA`

**Description**: Area of the block footprint.

**Method**: `Sum(building area)`



### `FLOOR_AREA`

**Description**: Total floor area within the block.

**Method**: `Sum(building floor area) `



### `VOLUME`

**Description**: Block’s volume, defined as the sum of the buildings volumes belonging to the block.

**Method**: `Sum(building volume)`



### `HOLE_AREA_DENSITY`

**Description**: Ratio between the area of courtyard within a block and the area of the block

**Method**: `Block courtyard area / block area `



### `MAIN_BUILDING_DIRECTION`

**Description**: Main orientation of the buildings within the blocks (from the north, clockwise).

**Method**: Then the mode of the distribution is set as the main building direction.



### `INEQUALITY_BUILDING_DIRECTION`

**Description**: Indicates how unequal is the RSU building direction distribution (having `nb_direction` directions of analysis).

**Range of values**: [nb_direction, 1] - the lower the value the most unequal is the distribution

**Method**: `Sum(Min(1/nb_direction, length_dir_i/length_all_dir))`



### `UNIQUENESS_BUILDING_DIRECTION`

**Description**: Indicates how unique is the RSU main building direction.

**Range of values**: [1, +inf] - the higher the value, the more unique is the main building direction

**Method**: `Length_First_Dir / Length_Second_Dir `



### `CLOSINGNESS`

**Description**:  This calculation indicates if a block has a large closed courtyard. This information could be useful for the urban fabric classification proposed in Thornay et al. (2017) and also described in Bocher et al. (2018). It answers to the Step 11 of the manual decision tree which consists in checking whether the block is closed (continuous buildings the aligned along road).

**Method**: In order to identify the RSU with closed blocks, the difference between the `st_holes(bloc scale)` and `Sum(st_holes(building scale))` indicators is calculated.

**Consequence**: this method will not be able to identify blocks that are nearly closed (e.g. 99 % of the RSU perimeter) while it would be interesting.

**References**:

- Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). [A geoprocessing framework to compute urban indicators: The MApUCE tools chain](https://www.sciencedirect.com/science/article/pii/S2212095518300117). Urban climate, 24, 153-174.
- Tornay, Nathalie, Robert Schoetter, Marion Bonhomme, Serge Faraut, and Valéry Masson. "[GENIUS: A methodology to define a detailed description of buildings for urban climate and building energy consumption simulations.](https://www.sciencedirect.com/science/article/pii/S2212095517300214)" Urban Climate 20 (2017): 75-93.



### `NET_COMPACTNESS`

**Description**: Net block’s compactness, defined as the ratio between the area of its free external facade and its building volume.

**Method**: `SUM((Bu_Contiguity * Bu_Perimeter + Bu_Hole_Perimeter) * Bu_Wall_Height)/Sum(Bu_Volume)`



### `AVG_HEIGHT_ROOF_AREA_WEIGHTED`

**Description**: Block average building’s roof height (the building heights being weighted by the building areas)

**Method**: `SUM(Bu_Wall_Height * Bu_Area) / SUM(Bu_Area)`



### `STD_HEIGHT_ROOF_AREA_WEIGHTED`

**Description**: Block standard deviation building’s roof height (the building heights being weighted by the building areas)

**Method**: `SUM(Bu_Area*(Bu_Wall_Height - AVG_HEIGHT_ROOF_AREA_WEIGHTED)^2))/SUM (Bu_Area)`



[back to top](#the-block_indicators-table)