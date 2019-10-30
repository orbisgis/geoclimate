# M1 - Input data preparation

This module consists of preparing the data chosen by the user for integration into the [Module 2](../data_formating/DATA_FORMATING.md).
In particular, it allows the selection of the necessary input data, based on spatial and attribute filters



## Spatial filtering

Spatial filtering consists in reducing the number of input objects based on areas deduced from the study area.

Below are presented:
- the different working areas,
- the conditions for filtering input data according to the working zones.



### Working areas

| Name             | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| `ZONE`           | The studied zone, which may be a city, an agglomeration, ... |
| `ZONE_BUFFER`    | A buffer area around the `ZONE` . The size (expressed in meter) is defined with the `DIST_BUFFER` parameter. Default value = 500m |
| `ZONE_EXTENDED`  | An extended area around the `ZONE` , produce with the [ST_Expand](http://www.h2gis.org/docs/dev/ST_Expand/) operator. The size (expressed in meter) is defined with the `EXPAND` parameter. Default value = 1000m |
| `ZONE_NEIGHBORS` | The neighboring zones. A zone is considered to be a neighbor if it intersects the `ZONE_EXTENDED` |

![](/home/gpetit/Documents/Codes/geoclimate/docs/images/zones.png)





### Input data *vs* working areas

Depending on the working zones, the input data are selected. The table below list the conditions of selection:

|       Theme        |              ZONE              |          ZONE_BUFFER           |         ZONE_EXTENDED          |
| :----------------: | :----------------------------: | :----------------------------: | :----------------------------: |
|     Buildings      |                                | ![](../images/icons/check.png) |                                |
|       Roads        |                                | ![](../images/icons/check.png) |                                |
|      Railways      | ![](../images/icons/check.png) |                                |                                |
|  Vegetation areas  |                                |                                | ![](../images/icons/check.png) |
| Hydrographic areas |                                |                                | ![](../images/icons/check.png) |
|  Impervious areas  |                                |                                | ![](../images/icons/check.png) |

