# Urban Typology Model (UTRF)

Based on (`Tornay *et al*. 2017 <https://doi.org/10.1016/j.uclim.2017.03.002>`_) method, GeoClimate classifies the RSU areas 
according to 10 urban classes. It uses a Random Forest model that classifies each building, the building types being 
then aggregated at the RSU scale. 
The indicators used by the model describe the building and its neighborhood at the block and RSU scales.

Two training data, and therefore two models, are created: one with the BDTopo and another one with OSM. 
The training data contains 20 761 buildings described by 63 indicators.

## 1. Urban Classes

The variable to predict is `I_TYPO`.

| `I_TYPO` | UTRF code | Number of training buildings |
|---|---|---|
| 1 | `ba` (Industrial building) | 2 140 |
| 2 | `bgh` (High-rise building) | 504 |
| 3 | `icif` (Block of buildings on closed urban islet) | 3 159 |
| 4 | `icio` (Block of buildings on open urban islet) | 1 909 |
| 5 | `id` (Detached building) | 1 452 |
| 6 | `local` (Informal building) | 1 948 |
| 7 | `pcif` (Residential on closed islet) | 2 188 |
| 8 | `pcio` (Residential on open islet) | 2 838 |
| 9 | `pd` (Detached house) | 2 463 |
| 10 | `psc` (Semi-detached house) | 2 160 |
| | **Total** | **20 761** |

## 2. Training data

- 20 761 buildings, 64 attributes: `I_TYPO` and the 63 explanatory variables listed in section 3.
- The features have no geometry (`geometry` is `null`) and the file declares no CRS: the buildings cannot be
  located.
- The internal layer name is `TRAINING_DATA_URBAN_TYPOLOGY_BDTOPO_V2_RF_1_0`, although the file is named `RF_2_2`.
- The data were produced from the BDTopo V2 with the GeoClimate formatting of that time. Consequences:
  - `BUILD_TYPE` takes 18 values and is mostly `building` (91 %):

    | `BUILD_TYPE` | Count |
    |---|---|
    | `building` | 18 830 |
    | `light_industry` | 1 358 |
    | `commercial` | 197 |
    | `church` | 99 |
    | `sports_centre` | 78 |
    | `silo` | 50 |
    | `greenhouse` | 37 |
    | `grandstand` | 33 |
    | `castle` | 19 |
    | `townhall` | 12 |
    | `historic` | 9 |
    | `farm_auxiliary` | 9 |
    | `government` | 8 |
    | `religious` | 7 |
    | `chapel` | 5 |
    | `terminal` | 4 |
    | `military` | 3 |
    | `monument` | 3 |

  - `BUILD_MAIN_USE` is an empty string for all the buildings (the BDTopo V2 has no use attribute).
  - `BUILD_HEIGHT_ROOF`, `BUILD_HEIGHT_WALL`, `BUILD_NB_LEV` and `BUILD_NUMBER_BUILDING_NEIGHBOR` are integers.
- Data quality: `RSU_VEGETATION_FRACTION_UTRF` is greater than 1 for 23 buildings.

## 3. Explanatory variables

The 63 variables stored in the model schema, in the model order. The last column gives the share of the
199 433 tree splits that use the variable: it is an indication of the use of the variable by the model, not a
variable importance.

### Building scale (20)

| # | Variable | Type | Description | Splits |
|---|---|---|---|---|
| 10 | `BUILD_AREA` | double | Footprint area (m²) | 2.8 % |
| 11 | `BUILD_AREA_CONCAVITY` | double | Footprint area / convex hull area | 1.9 % |
| 12 | `BUILD_COMMON_WALL_FRACTION` | double | Wall length shared with other buildings / total wall length | 1.6 % |
| 13 | `BUILD_CONTIGUITY` | double | Fraction of the wall area shared with other buildings | 1.8 % |
| 14 | `BUILD_FLOOR_AREA` | double | Floor area (footprint area × number of levels) | 2.9 % |
| 15 | `BUILD_FORM_FACTOR` | double | Area / perimeter² | 2.4 % |
| 16 | `BUILD_HEIGHT_ROOF` | integer | Roof height (m) | 2.0 % |
| 17 | `BUILD_HEIGHT_WALL` | integer | Wall height (m) | 2.0 % |
| 18 | `BUILD_LIKELIHOOD_LARGE_BUILDING` | double | Logistic function of the largest side of the minimum rectangle (horizontal extent > 50 m) | 1.3 % |
| 19 | `BUILD_MAIN_USE` | nominal | Main use. Always empty in the training data | 0 % |
| 20 | `BUILD_MINIMUM_BUILDING_SPACING` | double | Distance to the closest building (m) | 0.9 % |
| 21 | `BUILD_NB_LEV` | integer | Number of levels | 1.6 % |
| 22 | `BUILD_NUMBER_BUILDING_NEIGHBOR` | integer | Number of buildings in contact | 0.6 % |
| 23 | `BUILD_PERIMETER` | double | Perimeter (m) | 2.5 % |
| 24 | `BUILD_PERIMETER_CONVEXITY` | double | Convex hull perimeter / perimeter | 1.9 % |
| 25 | `BUILD_RAW_COMPACTNESS` | double | Wall and roof area / volume^(2/3) | 2.2 % |
| 26 | `BUILD_ROAD_DISTANCE` | double | Distance to the closest road (m) | 2.3 % |
| 27 | `BUILD_TOTAL_FACADE_LENGTH` | double | Building perimeter + courtyard perimeters (m) | 2.4 % |
| 28 | `BUILD_TYPE` | nominal | Building type, 18 levels (see section 4) | 1.6 % |
| 29 | `BUILD_VOLUME` | double | Volume (m³) | 3.3 % |

### Block scale (10)

| # | Variable | Type | Description | Splits |
|---|---|---|---|---|
| 0 | `BLOCK_AREA` | double | Block area (m²) | 3.0 % |
| 1 | `BLOCK_AVG_HEIGHT_ROOF_AREA_WEIGHTED` | double | Mean roof height weighted by the building area | 2.1 % |
| 2 | `BLOCK_BUILDING_DIRECTION_EQUALITY` | double | Equality of the distribution of the building directions | 0.7 % |
| 3 | `BLOCK_BUILDING_DIRECTION_UNIQUENESS` | double | Uniqueness of the main building direction | 2.3 % |
| 4 | `BLOCK_CLOSINGNESS` | double | Block hole area − sum of the building hole areas (identifies closed blocks) | 0.4 % |
| 5 | `BLOCK_FLOOR_AREA` | double | Block floor area (m²) | 2.6 % |
| 6 | `BLOCK_HOLE_AREA_DENSITY` | double | Area of the holes (courtyards) / block area | 0.6 % |
| 7 | `BLOCK_NET_COMPACTNESS` | double | Free external building area / (building volume)^(2/3) | 2.1 % |
| 8 | `BLOCK_STD_HEIGHT_ROOF_AREA_WEIGHTED` | double | Standard deviation of the roof height weighted by the building area | 1.5 % |
| 9 | `BLOCK_VOLUME` | double | Block volume (m³) | 2.7 % |

### RSU scale (33)

| # | Variable | Type | Description | Splits |
|---|---|---|---|---|
| 30 | `RSU_AREA` | double | RSU area (m²) | 1.6 % |
| 31 | `RSU_ASPECT_RATIO` | double | Aspect ratio | 1.7 % |
| 32 | `RSU_AVG_HEIGHT_ROOF_AREA_WEIGHTED` | double | Mean roof height weighted by the building area | 2.0 % |
| 33 | `RSU_AVG_MINIMUM_BUILDING_SPACING` | double | Mean of `BUILD_MINIMUM_BUILDING_SPACING` | 1.7 % |
| 34 | `RSU_AVG_NUMBER_BUILDING_NEIGHBOR` | double | Mean of `BUILD_NUMBER_BUILDING_NEIGHBOR` | 1.9 % |
| 35 | `RSU_AVG_VOLUME` | double | Mean building volume | 2.4 % |
| 36 | `RSU_BUILDING_DIRECTION_EQUALITY` | double | Equality of the distribution of the building directions | 1.5 % |
| 37 | `RSU_BUILDING_DIRECTION_UNIQUENESS` | double | Uniqueness of the main building direction | 1.8 % |
| 38 | `RSU_BUILDING_FLOOR_AREA_DENSITY` | double | Building floor area / RSU area | 1.8 % |
| 39 | `RSU_BUILDING_FRACTION` | double | Building surface fraction | 1.9 % |
| 40 | `RSU_BUILDING_NUMBER_DENSITY` | double | Number of buildings / RSU area | 2.1 % |
| 41 | `RSU_BUILDING_TOTAL_FRACTION` | double | Building surface fraction, including buildings under high vegetation | 1.9 % |
| 42 | `RSU_BUILDING_VOLUME_DENSITY` | double | Building volume / RSU area | 1.8 % |
| 43 | `RSU_FREE_EXTERNAL_FACADE_DENSITY` | double | Free external facade area / RSU area | 1.7 % |
| 44 | `RSU_GEOM_AVG_HEIGHT_ROOF` | double | Geometric mean of the roof height | 1.9 % |
| 45 | `RSU_GROUND_LINEAR_ROAD_DENSITY` | double | Ground road length / RSU area | 1.7 % |
| 46 | `RSU_HIGH_VEGETATION_BUILDING_FRACTION` | double | High vegetation over buildings | 1.2 % |
| 47 | `RSU_HIGH_VEGETATION_FRACTION` | double | High vegetation over nothing else | 1.0 % |
| 48 | `RSU_HIGH_VEGETATION_IMPERVIOUS_FRACTION` | double | High vegetation over impervious surfaces | 0.3 % |
| 49 | `RSU_HIGH_VEGETATION_IMPERVIOUS_FRACTION_UTRF` | double | See section 3.1 | 1.2 % |
| 50 | `RSU_HIGH_VEGETATION_LOW_VEGETATION_FRACTION` | double | High vegetation over low vegetation | 0 % |
| 51 | `RSU_HIGH_VEGETATION_PERVIOUS_FRACTION_UTRF` | double | See section 3.1 | 1.0 % |
| 52 | `RSU_HIGH_VEGETATION_ROAD_FRACTION` | double | High vegetation over roads | 1.1 % |
| 53 | `RSU_HIGH_VEGETATION_WATER_FRACTION` | double | High vegetation over water. Recreated at application, see section 5 | 0.1 % |
| 54 | `RSU_IMPERVIOUS_FRACTION` | double | Impervious surfaces | 0.6 % |
| 55 | `RSU_IMPERVIOUS_FRACTION_UTRF` | double | See section 3.1 | 1.8 % |
| 56 | `RSU_LOW_VEGETATION_FRACTION` | double | Low vegetation | 0.1 % |
| 57 | `RSU_LOW_VEGETATION_FRACTION_UTRF` | double | See section 3.1 | 0.1 % |
| 58 | `RSU_ROAD_FRACTION` | double | Roads | 1.6 % |
| 59 | `RSU_ROAD_FRACTION_UTRF` | double | See section 3.1 | 1.6 % |
| 60 | `RSU_STD_HEIGHT_ROOF_AREA_WEIGHTED` | double | Standard deviation of the roof height weighted by the building area | 1.9 % |
| 61 | `RSU_VEGETATION_FRACTION_UTRF` | double | See section 3.1 | 1.1 % |
| 62 | `RSU_WATER_FRACTION` | double | Water. Recreated at application, see section 5 | 0.2 % |

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

The model is built with `TypologyClassification.createRandomForestModel` from the disabled test
`createRandomForestModelUTRF_BDTOPO_V2_RF`:

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

7. The model is serialised with XStream and gzipped.

The accuracy checked by the test is computed on the training data (resubstitution), and the assertion
`assertEquals 0.725, accuracy.round(3), 1.5` accepts any value between -0.775 and 2.225: the test does not
validate the model. No independent validation (cross-validation, out-of-bag error or test set) is documented.

## 5. Application

The model is applied in `WorkflowGeoIndicators.computeTypologyIndicators` when `indicatorUse` contains `UTRF`.

1. **Model file**: `modelCheck` looks for the file locally, otherwise in `~/.geoclimate/`, otherwise downloads it
   from `https://github.com/orbisgis/geoclimate/raw/master/models/`.
2. **Gathering of the scales**: `GenericIndicators.gatherScales` joins the building, block and RSU indicators at
   the building scale (`AVG` and `STD` operations).
3. **Formatting** (`formatBuildingBeforeEstimation`):
   - `BUILD_TYPE` and `BUILD_MAIN_USE` are converted with the mapping file
     `geoindicators/src/main/resources/.../types_uses_height_model.json`. A value is replaced by the key of the
     list that contains it, a key is kept as it is. If a value is unknown (neither a key nor in a list), an
     error is logged and the function returns `null`.
   - `RSU_HIGH_VEGETATION_WATER_FRACTION = RSU_HIGH_VEGETATION_WATER_PERMANENT_FRACTION + RSU_HIGH_VEGETATION_WATER_INTERMITTENT_FRACTION`
   - `RSU_WATER_FRACTION = RSU_WATER_PERMANENT_FRACTION` (the intermittent water is not counted)
4. **Prediction** (`TypologyClassification.applyRandomForestModel`):
   - The model is loaded with XStream (and kept in a cache).
   - The 63 columns of the model schema are read from the formatted table into a Smile `DataFrame`.
   - The text columns are converted with `DataFrame.factorize` (see known issue 1).
   - `Validation.test` predicts `I_TYPO` for each building. The result is written in a table
     (`ID_BUILD`, `I_TYPO`).
5. **Building output**: `I_TYPO` is replaced by the UTRF code (`unknown` if no prediction) and joined to the
   building geometry (`building_utrf`).
6. **RSU output**: for each RSU, the share of each UTRF type is computed by building area (`rsu_utrf_area`) and
   by building floor area (`rsu_utrf_floor_area`). `distributionCharacterization` gives the main type
   (`TYPO_MAJ`), the second type (`TYPO_SECOND`) and the uniqueness of the main type (`UNIQUENESS_VALUE`). These
   three values are `NULL` for the RSU without building.

## 6. Known issues

1. **Encoding of `BUILD_TYPE` at application (critical).** `applyRandomForestModel` calls `DataFrame.factorize`
   on the data to classify: the integer codes are recomputed from the values present in the processed zone and
   not taken from the scale stored in the model (section 4). As soon as the set of types of the zone differs from
   the 18 training types, the codes no longer mean the same thing (e.g. with the types `building`, `church`,
   `commercial`, `industrial`, `residential`, `residential` gets the code 4, i.e. `commercial` for the model).
   The model uses `BUILD_TYPE` in 3 108 splits.
2. **Building types produced today differ from the training types.** The current BDTopo formatting
   (`InputDataFormatting`) produces types that were not in the training data:

   | Current BDTopo value | Type after `formatBuildingBeforeEstimation` | Seen in training |
   |---|---|---|
   | Résidentiel | `residential` | no (`building`) |
   | Industriel, Bâtiment industriel | `industrial` | no (`light_industry`) |
   | Bâtiment (undefined) | `building` | yes |
   | greenhouse, historic, grandstand | `farm_auxiliary`, `monument`, `sports_centre` | no (kept as is in training) |

   15 of the 29 types of the mapping file never appear in the training data. `light_industry`, which is mostly
   classified as `ba` in the training data, is no longer produced.
3. **`BUILD_MAIN_USE` is not used by the model** (constant in the training data, no split) but it must exist in
   the input table, and an unknown use stops the classification in `formatBuildingBeforeEstimation`.
4. **`NULL` values in `formatBuildingBeforeEstimation`.** The test `row.BUILD_TYPE != "null"` compares with the
   string `"null"`: a SQL `NULL` is kept as an unknown value and stops the classification. The returned `null` is
   not checked by `computeTypologyIndicators`.
5. **Column check in `applyRandomForestModel`.** The condition `!allowedColumnNames && notSameColumnNames` only
   stops when no column is found; a missing column fails later in the SQL query.
6. **Model validation.** See the end of section 4.
7. **Versioning.** The internal name of the training layer (`..._RF_1_0`) does not match the file name
   (`..._RF_2_2`).

---

```{eval-rst}
.. include:: _Footer.rst
```