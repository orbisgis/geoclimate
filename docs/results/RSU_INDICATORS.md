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
| NON_VERT_ROOF_AREA_H0_10 | double precision | [Full definition](#NON_VERT_ROOF_AREA_Hx_y) |
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
| PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D0_30     | double precision | [Full definition](#PROJECTED_FACADE_AREA_DISTRIBUTION_Hx_y_Dw_z) |
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



[back to top](#the-rsu_indicators-table)

## Indicators definition  ![](../images/icons/dico.png)



### `BUILDING_AREA_FRACTION`

**Description**: Fraction of building areas within the RSU.

**Method**: `SUM(BU_AREA) / RSU_Area`



### `BUILDING_FLOOR_AREA_DENSITY`

**Description**: Density of building floor areas within the RSU.

**Method**: `SUM(BU_FLOOR_AREA) / RSU_Area`

 

### `BUILDING_VOLUME_DENSITY`

**Description**: Density of building volumes within the RSU.

**Method**: `SUM(BU_VOLUME) / RSU_Area`



### `AVG_VOLUME_AREA_WEIGHTED`

**Description**: RSU average building volume (the building volume being weighted by the building areas).

**Method**: `SUM(Bu_Volume * Bu_Area) / SUM(Bu_Area)`



### `GEOM_AVG_HEIGHT_ROOF`

**Description**: RSU geometric mean of the building roof heights.

**Method**: `EXP(SUM(LOG(BU_ROOF_HEIGHT)) / NB_Building)`



[back to top](#the-rsu_indicators-table)




### `AVG_HEIGHT_ROOF_AREA_WEIGHTED`

**Description**: RSU average building’s roof height (the building heights being weighted by the building areas).

**Method**: `SUM(Bu_Wall_Height * Bu_Area) / SUM(Bu_Area)`



### `STD_HEIGHT_ROOF_AREA_WEIGHTED`

**Description**: Block standard deviation building’s roof height (the building heights being weighted by the building areas).

**Method**: `SUM(Bu_Area*(Bu_Wall_Height - AVG_HEIGHT_ROOF_AREA_WEIGHTED)^2)) / SUM (Bu_Area)`

 

### `FREE_EXTERNAL_FACADE_DENSITY`

**Description**: Sum of all building free facades (roofs are excluded) included in a RSU, divided by the RSU area.

**Method**: `SUM((1 - Bu_Contiguity) * Bu_TotalFacadeLength * HEIGHT_WALL) / RSU_Area`

 

### `NON_VERT_ROOF_AREA_Hx_y`

**Description**: Non-vertical (horizontal and tilted) roofs area is calculated within each vertical layer of a RSU (the bottom of the layer being at `x` meters from the ground while the top is at `y` meters).

**Method**: The calculation is based on the assumption that all buildings having a roof height higher than the wall height have a gable roof (the other being horizontal). Since the direction of the gable is not taken into account for the moment, we consider that buildings are square in order to limit the potential calculation error (otherwise a choice should have been made to locate the line corresponding to the top of the roof).



### `NON_VERT_ROOF_DENSITY`

**Description**: RSU surface density of non-vertical roofs (horizontal and tilted roofs).

**Method**: `SUM(Non_vert_roof_area_Hx_y) / RSU_Area`



[back to top](#the-rsu_indicators-table)



### `VERT_ROOF_AREA_Hxx_xx`

**Description**: Vertical roofs area is calculated within each vertical layer of a RSU (the bottom of the layer being at `x` meters from the ground while the top is at `y` meters).

**Method**: The calculation is based on the assumption that all buildings having a roof height higher than the wall height have a gable roof (the other being horizontal). Since the direction of the gable is not taken into account for the moment, we consider that buildings are square in order to limit the potential calculation error (otherwise a choice should have been made to locate the line corresponding to the top of the roof).



### `VERT_ROOF_DENSITY`

**Description**: RSU surface density of vertical roofs.

**Method**: `SUM(Vert_roof_area_Hx_y) / RSU_Area`



### `AVG_MINIMUM_BUILDING_SPACING`

**Description**: RSU average minimum distance between buildings.

**Method**: `SUM(Minimum_Building_Spacing) / Nb_Building `



### `AVG_NUMBER_BUILDING_NEIGHBOR`

**Description**: RSU average number of neighbors per building.

**Method**: `SUM(Number_Building_Neighbors) / Nb_Building `




### `BUILDING_NUMBER_DENSITY` 

**Description**: RSU number of building density.

**Method**: `Nb_Building / Rsu_Area`



[back to top](#the-rsu_indicators-table)



### `MAIN_BUILDING_DIRECTION`

**Description**: Main direction of the buildings contained in a RSU.

**Method**: The building direction distribution is calculated according to the length of the building SMBR sides (width and length). The [0, 180]° angle range is splitted into `nb_directions` angle ranges . Then the length of each SMBR building side is attributed to one of these ranges according to the side direction. Within each angle range, the total length of SMBR sides are summed and then the mode of the distribution is taken as the main building direction.



### `INEQUALITY_BUILDING_DIRECTION` 

**Description**: Indicates how unequal is the RSU building direction distribution (having `nb_direction` directions of analysis).

**Range of values**: [1/nb_direction, 1] -> the lower the value the most unequal is the distribution

**Method**: `SUM(Min(1/Nb_direction, Length_dir_i / length_all_dir))`



### `UNIQUENESS_BUILDING_DIRECTION` 

**Description**: Indicates how unique is the RSU main building direction.

**Range of values**: [1, +inf] - the higher the value, the more unique is the main building direction

**Method**: `Length_First_Dir / Length_Second_Dir `



### `ASPECT_RATIO`

**Description**: aspect ratio such as defined by Stewart et Oke (2012): mean height-to-width ratio of street canyons (LCZs 1-7), building spacing (LCZs 8-10), and tree spacing (LCZs A - G).

**Method**: A simple approach based on the street canyons assumption is used for the calculation. The sum of facade area within a given RSU area is divided by the area of free surfaces of the given RSU (not covered by buildings).

```
RSU_free_external_facade_density / (1 - RSU_building_density)
```



### `GROUND_SKY_VIEW_FACTOR`

**Description**: RSU ground Sky View Factor such as defined by Stewart et Oke (2012): ratio of the amount of sky hemisphere visible from ground level to that of an unobstructed hemisphere.

**Method**: The calculation is based on the [ST_SVF](http://www.h2gis.org/docs/dev/ST_SVF/) function of H2GIS using the following parameters: ray length = 100, number of directions = 60. Using a uniform grid mesh of 10 m resolution, the SVF obtained has a standard deviation of the estimate of 0.03 when compared with the most accurate method (according to [Bernard et al. (2018)](https://dx.doi.org/10.3390/cli6030060)).

Using a grid of regular points, the density of points used for the calculation actually depends on building density (higher the building density, lower the density of points). To avoid this phenomenon and have the same density of points per free ground surface, we use an H2GIS function to distribute randomly points within free surfaces (ST_GeneratePoints). This density of points is set by default to 0.008, based on the median of [Bernard et al. (2018)](https://dx.doi.org/10.3390/cli6030060) dataset.

**References**:

- Stewart, Ian D., and Tim R. Oke. "[Local climate zones for urban temperature studies.](https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1)" Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
- Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. [Sky View Factor Calculation in Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools.](https://www.mdpi.com/2225-1154/6/3/60) Climate, MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.



 [back to top](#the-rsu_indicators-table)




### `PROJECTED_FACADE_AREA_DISTRIBUTION_Hx_y_Dw_z`

**Description**: Distribution of projected facade area within a RSU per vertical layer (the height being from `x` to `y`) and per direction of analysis (ie. wind or sun direction - the angle range being from `w` to `z` within the range [0, 180[°). Note that the method used is an approximation if the RSU split a building into two parts (the facade included within the RSU is counted half).

**Method**: Each line representing the facades of a building are projected in order to be perpendicular to the median of each angle range of analysis. Only free facades are considered. The projected surfaces are then summed within each layer and direction of analysis. The analysis is only performed within the [0, 180[° range since the projected facade of a building is identical for opposite directions. Thus because we consider all facades of a building in the calculation (facades upwind but also downwind), the final result is divided by 2.

**Warning**: To simplify the calculation, z0 is considered as equal for a given orientation independently of the direction. This assumption is right when the RSU do not split buildings but could slightly overestimate the results otherwise (the projected facade area is actually overestimated in one direction but OK in the opposite direction).

 

### `EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH`

**Description**: Effective terrain roughness length (z0).

**Method**:  The method for z0 calculation is based on the Hanna and Britter (2010) procedure (see equation (17) and examples of calculation p. 156 in the corresponding reference). The `rsu_projected_facade_area_distribution_Hx_y_Dw_z` is used to calculate the mean projected facade density (considering all directions) and `z0` is then obtained multiplying the resulting value by the `rsu_geometric_mean_height`.

**Warning**: the calculation of z0 is only performed for angles included in the range [0, 180[°. To simplify the calculation, z0 is considered as equal for a given orientation independently of the direction. This assumption is right when the RSU do not split buildings but could slightly overestimate the results otherwise (z0 is actually overestimated in one direction but OK in the opposite direction).

References:

- Stewart, Ian D., and Tim R. Oke. "[Local climate zones for urban temperature studies.](https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1)" Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
- Hanna, Steven R., and Rex E. Britter. [Wind flow and vapor cloud dispersion at industrial and urban sites](https://www.wiley.com/en-fr/Wind+Flow+and+Vapor+Cloud+Dispersion+at+Industrial+and+Urban+Sites-p-9780816908639). Vol. 7. John Wiley & Sons, 2010.

 

### `EFFECTIVE_TERRAIN_ROUGHNESS_CLASS`

**Description**: Effective terrain class from the effective terrain roughness length (z0). The classes are defined according to the Davenport lookup Table (cf Table 5 in Stewart and Oke, 2012)



**Method**: `xxxxxxx`

**Warning**: the Davenport definition defines a class for a given z0 value. Then there is no definition of the z0 range corresponding to a certain class. Then we have arbitrarily defined the z0 value corresponding to a certain Davenport class as the average of each interval, and the boundary between two classes is defined as the arithmetic average between the z0 values of each class. A definition of the interval based on the profile of class = f(z0) could lead to different results (especially for classes 3, 4 and 5).

**References**:

- Stewart, Ian D., and Tim R. Oke. "[Local climate zones for urban temperature studies.](https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1)" Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.



### `ROAD_DIRECTION_DISTRIBUTION_H0_Dxx_xx`

**Description**: 

**Method**: `xxxxxxx`



### `GROUND_LINEAR_ROAD_DENSITY`

**Description**: Road linear density, having a ZINDEX = 0, within the RSU.

**Method**: `xxxxxxx`



[back to top](#the-rsu_indicators-table)



### `GROUND_ROAD_FRACTION`

**Description**: Fraction of road areas, having a `ZINDEX` = 0, within the RSU. The road area is determined thanks to it’s `WIDTH` information.

**Method**: `xxxxxxx`



### `OVERGROUND_ROAD_FRACTION`

**Description**: Fraction of road areas, having a `ZINDEX` > 0, within the RSU. The road area is determined thanks to it’s `WIDTH` information.

**Method**: `xxxxxxx`



### `WATER_FRACTION`

**Description**: Fraction of water areas within the RSU.

**Method**: `SUM(water areas) / RSU area `



### `LOW_VEGETATION_FRACTION`

**Description**: Fraction of "low" vegetation areas within the RSU

**Method**: `SUM(low vegetation areas) / RSU area`

**References**:

- Stewart, Ian D., and Tim R. Oke. "[Local climate zones for urban temperature studies.](https://journals.ametsoc.org/doi/full/10.1175/BAMS-D-11-00019.1)" Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.



### `HIGH_VEGETATION_FRACTION`

**Description**: Fraction of "high" vegetation areas within the RSU.

**Method**: `SUM(high vegetation areas) / RSU area`

 

### `ALL_VEGETATION_FRACTION`

**Description**: Fraction of “low and high” vegetation areas within the RSU.

**Method**: `SUM(vegetation areas) / RSU area`



### `PERVIOUS_FRACTION`

**Description**: Fraction of pervious areas within the RSU

**Method**: `SUM(pervious areas) / RSU area`




### `IMPERVIOUS_FRACTION`

**Description**: Fraction of impervious areas within the RSU

**Method**: `SUM(impervious areas) / RSU area`



[back to top](#the-rsu_indicators-table)