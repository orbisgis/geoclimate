# M1 - Input data preparation

This module consists of preparing the data chosen by the user for integration into the [Module 2](../data_formating/DATA_FORMATING.md).
In particular, it allows:

- the selection of the necessary input data, based on spatial and attribute filters,
- the compliance of the thematic descriptors.



## Spatial filtering

Spatial filtering consists in reducing the number of input objects based on areas deduced from the study area.

Below are presented:
- the different working areas,
- the conditions for filtering input data according to the working zones.



### Working areas

|       Name       | Description                                                  |
| :--------------: | ------------------------------------------------------------ |
|      `ZONE`      | The studied zone, which may be a city, an agglomeration, ... |
|  `ZONE_BUFFER`   | A buffer area around the `ZONE` . The size *(expressed in meter)* is defined with the `DIST_BUFFER` parameter. Default value = `500`m |
| `ZONE_EXTENDED`  | An extended area around the `ZONE` , produce with the [ST_Expand](http://www.h2gis.org/docs/dev/ST_Expand/) operator. The size *(expressed in meter)* is defined with the `EXPAND` parameter. Default value = `1000`m |
| `ZONE_NEIGHBORS` | The neighboring zones. A zone is considered to be a neighbor if it intersects the `ZONE_EXTENDED`. This zone is only used to determine the relationship between buildings and their belonging zones ([See](../data_formating/DATA_FORMATING.md#1--belonging-zone-id)). |

![](../images/zones.png)



**Remark**

- `ZONE` and `ZONE_NEIGHBORS` layers are expected in the [input data model](../input_data/INPUT_DATA_MODEL.md#zones).
- `ZONE_BUFFER`  and `ZONE_EXTENDED`  are temporary layers, just needed for Module 1.

### Input data *vs* working areas

Depending on the working zones, the input data are selected. The table below list the conditions of selection:

|       Theme        |              ZONE              |          ZONE_BUFFER           |         ZONE_EXTENDED          |
| :----------------: | :----------------------------: | :----------------------------: | :----------------------------: |
|     Buildings      |                                | ![](../images/icons/check.png) |                                |
|       Roads        |                                | ![](../images/icons/check.png) |                                |
|      Railways      | ![](../images/icons/check.png) |                                |                                |
|  Vegetation areas  |                                |                                | ![](../images/icons/check.png) |
| Hydrographic areas |                                |                                | ![](../images/icons/check.png) |
|  Impervious areas  | ![](../images/icons/check.png) |                                |                                |



## Attribute matching

The attribute matching consist in converting the thematic descriptors, in the entry data, in order to be consistent with the expected values in the [input data model](../input_data/INPUT_DATA_MODEL.md#zones).

For each of these values, the user has to determine conversion rules in order to update them.

It is therefore a step that is implemented on a case-by-case basis, since it depends on the input data sets.

Two examples are given with the OSM and [BD Topo](../use_case/bd_topo_v2_2.md#matching-tables) use cases.









