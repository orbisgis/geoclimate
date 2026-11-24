# Urban Typology Model (UTRF)

Based on (`Tornay *et al*. 2017 <https://doi.org/10.1016/j.uclim.2017.03.002>`_) method, GeoClimate classifies the RSU areas 
according to 10 urban classes. It uses a Random Forest model that classifies each building, the building types being 
then aggregated at the RSU scale. 
The indicators used by the model describe the building and its neighborhood at the block and RSU scales.

Two training data, and therefore two models, are created: one with the BDTopo and another one with OSM. 
The training data contains 20 761 buildings described by 63 indicators.

## 1. Urban Classes

The variable to predict is `I_TYPO`.

| `I_TYPO` | UTRF code |
|---|---|
| 1 | `ba` (Industrial building) |
| 2 | `bgh` (High-rise building) |
| 3 | `icif` (Block of buildings on closed urban islet) |
| 4 | `icio` (Block of buildings on open urban islet) |
| 5 | `id` (Detached building) |
| 6 | `local` (Informal building) |
| 7 | `pcif` (Residential on closed islet) |
| 8 | `pcio` (Residential on open islet) |
| 9 | `pd` (Detached house) |
| 10 | `psc` (Semi-detached house) |
| | **Total** |

## 2. Training data

Two training data files are available in the `models` folder : 
- one for BDTopo data (e.g TRAINING_DATA_UTRF_OSM_RF_2_2.geojson.gz)
- the another for OSM (e.g TRAINING_DATA_UTRF_BDTOPO_V2_RF_2_2.geojson.gz).



## 3. Indicators

Bellow the list of the 63 variables (indicators) of the training data and stored in the RF model. 

### Building scale (20)

| # | Variable | Type | Description |
|---|---|---|---|
| 10 | `BUILD_AREA` | double | Footprint area (m²) |
| 11 | `BUILD_AREA_CONCAVITY` | double | Footprint area / convex hull area |
| 12 | `BUILD_COMMON_WALL_FRACTION` | double | Wall length shared with other buildings / total wall length |
| 13 | `BUILD_CONTIGUITY` | double | Fraction of the wall area shared with other buildings |
| 14 | `BUILD_FLOOR_AREA` | double | Floor area (footprint area × number of levels) |
| 15 | `BUILD_FORM_FACTOR` | double | Area / perimeter² |
| 16 | `BUILD_HEIGHT_ROOF` | integer | Roof height (m) |
| 17 | `BUILD_HEIGHT_WALL` | integer | Wall height (m) |
| 18 | `BUILD_LIKELIHOOD_LARGE_BUILDING` | double | Logistic function of the largest side of the minimum rectangle (horizontal extent > 50 m) |
| 19 | `BUILD_MAIN_USE` | nominal | Main use. Always empty in the training data |
| 20 | `BUILD_MINIMUM_BUILDING_SPACING` | double | Distance to the closest building (m) |
| 21 | `BUILD_NB_LEV` | integer | Number of levels |
| 22 | `BUILD_NUMBER_BUILDING_NEIGHBOR` | integer | Number of buildings in contact |
| 23 | `BUILD_PERIMETER` | double | Perimeter (m) |
| 24 | `BUILD_PERIMETER_CONVEXITY` | double | Convex hull perimeter / perimeter |
| 25 | `BUILD_RAW_COMPACTNESS` | double | Wall and roof area / volume^(2/3) |
| 26 | `BUILD_ROAD_DISTANCE` | double | Distance to the closest road (m) |
| 27 | `BUILD_TOTAL_FACADE_LENGTH` | double | Building perimeter + courtyard perimeters (m) |
| 28 | `BUILD_TYPE` | nominal | Building type, 18 levels |
| 29 | `BUILD_VOLUME` | double | Volume (m³) |

### Block scale (10)

| # | Variable | Type | Description |
|---|---|---|---|
| 0 | `BLOCK_AREA` | double | Block area (m²) |
| 1 | `BLOCK_AVG_HEIGHT_ROOF_AREA_WEIGHTED` | double | Mean roof height weighted by the building area |
| 2 | `BLOCK_BUILDING_DIRECTION_EQUALITY` | double | Equality of the distribution of the building directions |
| 3 | `BLOCK_BUILDING_DIRECTION_UNIQUENESS` | double | Uniqueness of the main building direction |
| 4 | `BLOCK_CLOSINGNESS` | double | Block hole area − sum of the building hole areas (identifies closed blocks) |
| 5 | `BLOCK_FLOOR_AREA` | double | Block floor area (m²) |
| 6 | `BLOCK_HOLE_AREA_DENSITY` | double | Area of the holes (courtyards) / block area |
| 7 | `BLOCK_NET_COMPACTNESS` | double | Free external building area / (building volume)^(2/3) |
| 8 | `BLOCK_STD_HEIGHT_ROOF_AREA_WEIGHTED` | double | Standard deviation of the roof height weighted by the building area |
| 9 | `BLOCK_VOLUME` | double | Block volume (m³) |

### RSU scale (33)

| # | Variable | Type | Description |
|---|---|---|---|
| 30 | `RSU_AREA` | double | RSU area (m²) |
| 31 | `RSU_ASPECT_RATIO` | double | Aspect ratio |
| 32 | `RSU_AVG_HEIGHT_ROOF_AREA_WEIGHTED` | double | Mean roof height weighted by the building area |
| 33 | `RSU_AVG_MINIMUM_BUILDING_SPACING` | double | Mean of `BUILD_MINIMUM_BUILDING_SPACING` |
| 34 | `RSU_AVG_NUMBER_BUILDING_NEIGHBOR` | double | Mean of `BUILD_NUMBER_BUILDING_NEIGHBOR` |
| 35 | `RSU_AVG_VOLUME` | double | Mean building volume |
| 36 | `RSU_BUILDING_DIRECTION_EQUALITY` | double | Equality of the distribution of the building directions |
| 37 | `RSU_BUILDING_DIRECTION_UNIQUENESS` | double | Uniqueness of the main building direction |
| 38 | `RSU_BUILDING_FLOOR_AREA_DENSITY` | double | Building floor area / RSU area |
| 39 | `RSU_BUILDING_FRACTION` | double | Building surface fraction |
| 40 | `RSU_BUILDING_NUMBER_DENSITY` | double | Number of buildings / RSU area |
| 41 | `RSU_BUILDING_TOTAL_FRACTION` | double | Building surface fraction, including buildings under high vegetation |
| 42 | `RSU_BUILDING_VOLUME_DENSITY` | double | Building volume / RSU area |
| 43 | `RSU_FREE_EXTERNAL_FACADE_DENSITY` | double | Free external facade area / RSU area |
| 44 | `RSU_GEOM_AVG_HEIGHT_ROOF` | double | Geometric mean of the roof height |
| 45 | `RSU_GROUND_LINEAR_ROAD_DENSITY` | double | Ground road length / RSU area |
| 46 | `RSU_HIGH_VEGETATION_BUILDING_FRACTION` | double | High vegetation over buildings |
| 47 | `RSU_HIGH_VEGETATION_FRACTION` | double | High vegetation over nothing else |
| 48 | `RSU_HIGH_VEGETATION_IMPERVIOUS_FRACTION` | double | High vegetation over impervious surfaces |
| 49 | `RSU_HIGH_VEGETATION_IMPERVIOUS_FRACTION_UTRF` | double | See section 3.1 |
| 50 | `RSU_HIGH_VEGETATION_LOW_VEGETATION_FRACTION` | double | High vegetation over low vegetation |
| 51 | `RSU_HIGH_VEGETATION_PERVIOUS_FRACTION_UTRF` | double | See section 3.1 |
| 52 | `RSU_HIGH_VEGETATION_ROAD_FRACTION` | double | High vegetation over roads |
| 53 | `RSU_HIGH_VEGETATION_WATER_FRACTION` | double | High vegetation over water. Recreated at application, see section 5 |
| 54 | `RSU_IMPERVIOUS_FRACTION` | double | Impervious surfaces |
| 55 | `RSU_IMPERVIOUS_FRACTION_UTRF` | double | See section 3.1 |
| 56 | `RSU_LOW_VEGETATION_FRACTION` | double | Low vegetation |
| 57 | `RSU_LOW_VEGETATION_FRACTION_UTRF` | double | See section 3.1 |
| 58 | `RSU_ROAD_FRACTION` | double | Roads |
| 59 | `RSU_ROAD_FRACTION_UTRF` | double | See section 3.1 |
| 60 | `RSU_STD_HEIGHT_ROOF_AREA_WEIGHTED` | double | Standard deviation of the roof height weighted by the building area |
| 61 | `RSU_VEGETATION_FRACTION_UTRF` | double | See section 3.1 |
| 62 | `RSU_WATER_FRACTION` | double | Water. Recreated at application, see section 5 |

`RSU_HIGH_VEGETATION_LOW_VEGETATION_FRACTION` is in the training data but is not used by any split
(`RSU_HIGH_VEGETATION_WATER_FRACTION` and the low vegetation fractions are almost never used).

### 3.1 UTRF surface fractions

The `_UTRF` fractions are sums of the elementary RSU surface fractions (`utrfSurfFraction` in
`WorkflowGeoIndicators`):

| Variable | Sum of |
|---|---|
| `VEGETATION_FRACTION_UTRF` | `high_vegetation`, `low_vegetation`, `high_vegetation_low_vegetation`, `high_vegetation_road`, `high_vegetation_impervious`, `high_vegetation_water_permanent`, `high_vegetation_water_intermittent`, `high_vegetation_building` |
| `LOW_VEGETATION_FRACTION_UTRF` | `low_vegetation` |
| `HIGH_VEGETATION_IMPERVIOUS_FRACTION_UTRF` | `high_vegetation_road`, `high_vegetation_impervious` |
| `HIGH_VEGETATION_PERVIOUS_FRACTION_UTRF` | `high_vegetation`, `high_vegetation_low_vegetation`, `high_vegetation_water_permanent`, `high_vegetation_water_intermittent` |
| `ROAD_FRACTION_UTRF` | `road`, `high_vegetation_road` |
| `IMPERVIOUS_FRACTION_UTRF` | `road`, `high_vegetation_road`, `impervious`, `high_vegetation_impervious` |

## 4. Model construction

The RF model is built with `TypologyClassification.createRandomForestModel` function from the disabled test
`createRandomForestModelUTRF_BDTOPO_V2_RF`.

The steps are : 

1. The training GeoJSON is loaded with `GEOJSONREAD`.
2. The buildings with `I_TYPO = 1 AND BUILD_TYPE = 'residential'` are removed. There is no `residential` type in
   the training file, so this filter has no effect.
3. The geometry column is dropped and `I_TYPO` is cast to `INTEGER`.
4. The rows with a `NULL` value are removed (`omitNullRows`).
5. The text columns are converted to nominal variables with `DataFrame.factorize`: the distinct values are sorted
   and numbered from 0. The resulting scales are stored in the model schema:
   - `BUILD_TYPE`: `building`=0, `castle`=1, `chapel`=2, `church`=3, `commercial`=4, `farm_auxiliary`=5,
     `government`=6, `grandstand`=7, `greenhouse`=8, `historic`=9, `light_industry`=10, `military`=11,
     `monument`=12, `religious`=13, `silo`=14, `sports_centre`=15, `terminal`=16, `townhall`=17
   - `BUILD_MAIN_USE`: a single level, the empty string (=0)
6. The Random Forest is trained with the following parameters:

| Parameter | Value | Meaning |
|---|---|---|
| `ntrees` | 500 | Number of trees |
| `mtry` | 15 | Number of variables drawn at random at each node (63 variables) |
| `rule` | `GINI` | Split rule |
| `maxDepth` | 20 | Maximum depth of a tree |
| `maxNodes` | 400 | Maximum number of leaf nodes of a tree |
| `nodeSize` | 1 | Minimum number of instances in a node to split it |
| `subsample` | 1.0 | Sampling with replacement (bootstrap) |

7. The model is serialized with XStream and gzipped.


## 5. Application

The model is applied in `WorkflowGeoIndicators.computeTypologyIndicators` when `indicatorUse` contains `UTRF`.

It follows these steps : 

1. **Load the RF model file**: `modelCheck` looks for the file locally, otherwise in `~/.geoclimate/`, otherwise downloads it
   from `https://github.com/orbisgis/geoclimate/raw/master/models/`.
2. **Collect the indicators**: `GenericIndicators.gatherScales` joins the building, block and RSU indicators at
   the building scale
3. **Format the indicators** (`formatBuildingBeforeEstimation`)
4. **Apply the model** (`TypologyClassification.applyRandomForestModel`)
5. **Save the prediction (typo classes) for each building**: `I_TYPO` is replaced by the UTRF code (`unknown` if no prediction) and joined to the
   building geometry (`building_utrf`).
6. **Aggregate the building classes at RSU scale**: for each RSU, the share of each UTRF type is computed by building area (`rsu_utrf_area`) and
   by building floor area (`rsu_utrf_floor_area`). `distributionCharacterization` gives the main type
   (`TYPO_MAJ`), the second type (`TYPO_SECOND`) and the uniqueness of the main type (`UNIQUENESS_VALUE`). These
   three values are `NULL` for the RSU without building.

---

```{eval-rst}
.. include:: _Footer.rst
```