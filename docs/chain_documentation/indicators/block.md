# Block's indicators



Below are presented all the indicators computed at the block's scale.




### `AREA`

**Description**: Area of the block footprint.

**Method**: `Sum(building area)`



### `FLOOR_AREA`

**Description**: Total floor area within the block.

**Method**: `Sum(building floor area) `



### `VOLUME`

**Description**: Volume of buildings composing a block.

**Method**: `Sum(building volume)`



### `HOLE_AREA_DENSITY`

**Description**: Density of hole within a block.

**Method**: `Block courtyard area / block area `



### `MAIN_BUILDING_DIRECTION`

**Description**: Main orientation of the buildings within the blocks (from the North, clockwise).

**Method**: The smallest rectangle enclosing each building is calculated. Then each side of the rectangles are attributed to a direction range (by default every 15° within the [0, 180°[ interval - from North, clockwise). For each direction range, the length of the sides of all rectangles contained in a block are summed. Finally, the mode of this distribution is set as the main building direction within the block.



### `INEQUALITY_BUILDING_DIRECTION`

**Description**: Indicates how unequal is the RSU building direction distribution (having `nb_direction` directions of analysis).

**Method**: From the building direction distribution created in the `MAIN_BUILDING_DIRECTION` indicator calculation, an indicator of inequality of the distribution is calculated:

→ `Sum(Min(1/nb_direction, length_dir_i/length_all_dir))`

**Range of values**: [`nb_direction`, `1`] - the lower the value the most unequal is the distribution



### `UNIQUENESS_BUILDING_DIRECTION`

**Description**: Indicates how unique is the RSU main building direction.

**Method**: From the building direction distribution created in the `MAIN_BUILDING_DIRECTION` indicator calculation, an indicator of uniqueness of the main direction is calculated:

→ `Length_First_Dir / (Length_Second_Dir + Length_First_Dir)`

**Range of values**: [`0.5`, `1`] - the closer the value from 1, the more unique is the main building direction



### `CLOSINGNESS`

**Description**:  This calculation indicates if a block has a large closed courtyard. This information could be useful for the urban fabric classification proposed in Thornay et al. (2017) and also described in Bocher et al. (2018). It answers to the Step 11 of the manual decision tree which consists in checking whether the block is closed (continuous buildings the aligned along road).

**Method**: In order to identify the RSU with closed blocks, the difference between the `st_holes(bloc scale)` and `SUM(st_holes(building scale))` indicators is calculated.

**Warning**: this method will not be able to identify blocks that are nearly closed (e.g. 99 % of the RSU perimeter) while it would be interesting to know how much the block is closed (for ventilation purpose for example).

**References**:

- Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). [A geoprocessing framework to compute urban indicators: The MApUCE tools chain](https://www.sciencedirect.com/science/article/pii/S2212095518300117). Urban climate, 24, 153-174.
- Tornay, Nathalie, Robert Schoetter, Marion Bonhomme, Serge Faraut, and Valéry Masson. "[GENIUS: A methodology to define a detailed description of buildings for urban climate and building energy consumption simulations.](https://www.sciencedirect.com/science/article/pii/S2212095517300214)" Urban Climate 20 (2017): 75-93.



### `NET_COMPACTNESS`

**Description**: Net block’s compactness, defined as the ratio between the area of its free external facade and its building volume.

**Method**: 

→ `SUM((Bu_Contiguity * Bu_Perimeter + Bu_Hole_Perimeter) * Bu_Wall_Height)/Sum(Bu_Volume)`



### `AVG_HEIGHT_ROOF_AREA_WEIGHTED`

**Description**: Mean building’s roof height within a block (the building height being weighted by the building areas).

**Method**: `SUM(Bu_Wall_Height * Bu_Area) / SUM(Bu_Area)`



### `STD_HEIGHT_ROOF_AREA_WEIGHTED`

**Description**: Variability of the building’s roof height within a block (the building heights being weighted by the building areas).

**Method**: By default, the indicator of variability is the Standard Deviation (STD) defined as :

→ `SUM(Bu_Area*(Bu_Wall_Height - AVG_HEIGHT_ROOF_AREA_WEIGHTED)^2)) / SUM (Bu_Area)`



[back to top](#Block's-indicators)

