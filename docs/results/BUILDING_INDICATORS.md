# The `BUILDING_INDICATORS` table

This table stores all the indicators computed at the building's scale.

![](../images/icons/github.png) The indicators source code is available [here](https://github.com/orbisgis/geoclimate/blob/master/geoindicators/src/main/groovy/org/orbisgis/geoindicators/BuildingIndicators.groovy).



## Table definition ![](../images/icons/table.png)

![](../images/icons/pk.png) PK = Primary Key

| Field name   | Field type       | Definition         |
| :----------- | :--------------- | ------------------ |
| THE_GEOM       | geometry          | Building's geometry |
| **ID_BUILD** ![](../images/icons/pk.png) | integer | Building's unique id |
| ID_SOURCE | varchar | Original unique id |
| ID_BLOCK | integer | Belonging block id |
| ID_RSU | integer | Belonging RSU id |
| HEIGHT_WALL | integer | Wall height |
| HEIGHT_ROOF | integer | Roof height |
| NB_LEV | integer | Number of level |
| TYPE | varchar | Building's type |
| MAIN_USE | varchar | Building's main use |
| ZINDEX | integer | Altimetric positioning ([Full definition](#ZINDEX)) |
| PERIMETER | double precision | Building's perimeter |
| AREA | double precision | Building's area (footprint) |
| VOLUME | double precision | Building's volume ([Full definition](#VOLUME)) |
| FLOOR_AREA | double precision | [Full definition](#FLOOR_AREA) |
| TOTAL_FACADE_LENGTH | double precision | [Full definition](#TOTAL_FACADE_LENGTH) |
| COMMON_WALL_FRACTION | double precision | [Full definition](#COMMON_WALL_FRACTION) |
| CONTIGUITY | double precision | [Full definition](#CONTIGUITY) |
| AREA_CONCAVITY | double precision | [Full definition](#AREA_CONCAVITY) |
| FORM_FACTOR | double precision | [Full definition](#FORM_FACTOR) |
| RAW_COMPACTNESS | double precision | [Full definition](#RAW_COMPACTNESS) |
| PERIMETER_CONVEXITY | double precision | [Full definition](#**PERIMETER_CONVEXITY**) |
| MINIMUM_BUILDING_SPACING | double precision | [Full definition](#MINIMUM_BUILDING_SPACING) |
| NUMBER_BUILDING_NEIGHBOR | bigint | [Full definition](#NUMBER_BUILDING_NEIGHBOR) |
| ROAD_DISTANCE | double precision | [Full definition](#ROAD_DISTANCE) |
| LIKELIHOOD_LARGE_BUILDING | double precision | [Full definition](#LIKELIHOOD_LARGE_BUILDING) |



[back to top](#the-building_indicators-table)

## Indicators definition  ![](../images/icons/dico.png)

### `ZINDEX`

**Description**: Position of the building relative to the ground. 0 means that the building is on the ground. A negative value means that it is under the ground *(e. g. a metro station)* while a value greater than 0 means that it is in the air *(e. g. a bridge)*.



### `AREA `

**Description**: Building's area.

**Method**: `Area of the building footprint`




### `VOLUME`

**Description**: Building's volume defined as the product of the surface with the average height between the wall and the roof.

**Method**: `Area * ((Wall height + Roof height)/2)`




### `FLOOR_AREA`

**Description**: Building's floor area.

**Method**: `Area * Number of level`




### `TOTAL_FACADE_LENGTH`

**Description**: Total length of external facade.

**Method**: `Building perimeter + Courtyard perimeter`




### `COMMON_WALL_FRACTION`

**Description**: Fraction of linear of facade (also called “party walls”) shared with other buildings.

**Method**: `Shared facade length / total facade length`



### `CONTIGUITY`

**Description**: Fraction of wall shared with other buildings

**Method**: `Shared wall area / total wall area`



### `AREA_CONCAVITY`

**Description**: Calculates a degree of convexity of a building (according to the building surface).

**Method**: `Area / Convex Hull area`

**Range of values**: [`0`, `1`] - the closer the result from 1, the more convex the building.



### `FORM_FACTOR`

**Description**: Ratio between the building’s area and the square of the external building’s perimeter

**Method**: `Area / (perimeter)^2`



### `RAW_COMPACTNESS`

**Description**: Ratio between building external surfaces (walls and roof) and the building volume at the power 2/3. 

**Method**: `(External walls area + courtyard walls area + roof area) / (volume^(2/3)) `

**Warning**: For the calculation, the roof is supposed to have a gable and the roof surface is calculated considering that the building is square (otherwise, the choice related to the gable direction - which is not known - would strongly affect the result).



### `PERIMETER_CONVEXITY`

**Description**: Calculates a degree of convexity of a building (according to the building perimeter). 

**Method**: `Convex Hull perimeter / Perimeter`

**Range of values**: [`0`, `1`] - the closer the result from 1, the more convex the building.



### `MINIMUM_BUILDING_SPACING`

**Description**: Building closest distance *(expressed in meter)* to an other building.

**Method**: `Min(distance(building, other buildings within bufferDist))` , where the buffer size of search is defined in the `bufferDist` parameter *(default value = 100 m)*

**Warning**:

- If the building touches an other building, the result is 0.
- If there is no building in a 100m circle around the building, the result is set to 100m *(this value may be different if the `bufferDist` default value is modified)*.



### `NUMBER_BUILDING_NEIGHBOR`

**Description**: Number of neighboring buildings, in contact with the building.

**Method**: Count the number of buildings touching (at least one point) the building of interest.



### `ROAD_DISTANCE`

**Description**: Building closest distance *(expressed in meter)* to a road, 

**Method**: The search is made within a buffer area around the building whose size is defined in the `bufferDist` parameter *(default value = 100 m)*.

→ `Min(distance(building, roads within bufferDist))`

**Warning**:

- If the building touches a road, the result is 0.
- If the roads are further than 100m from the building, the result is set to 100m *(this value may be different if the `bufferDist` default value is modified)*.



### `LIKELIHOOD_LARGE_BUILDING`

**Description**: Building closeness to a 50 m wide isolated building (where `NUMBER_BUILDING_NEIGHBOR` = 0).

**Method**: The step 9 of the decision tree used for the MaPUCE project manual building typology classification consists of checking whether a building has a horizontal extent larger than 50 m. We therefore introduce an indicator which measures the horizontal extent of buildings. This indicator is based on the largest side of the building minimum rectangle. We then use a logistic function to avoid threshold effects (e.g. totally different result for building sizes of 49 m and 51 m). The gamma and *x0* parameters in the logistic function are specified after analysis of the training data to identify the real size of the buildings classified as larger than 50 m in the subjective training process.



[back to top](#the-building_indicators-table)