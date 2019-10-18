# The `RSU_LCZ` table

This table stores all the *Local Climate Zones* (LCZ) types computed at the RSU's scale.

The LCZ, introduced by *Stewart* & *Oke* ([2012](http://journals.ametsoc.org/doi/abs/10.1175/BAMS-D-11-00019.1), [2014](http://onlinelibrary.wiley.com/doi/10.1002/joc.3746/abstract)), is a classification scheme used to segment the climate area's of cities( and other).

For each RSU, we calculate the main (`LCZ1`) and the secondary (`LCZ2`) LCZ type. The main type is the one that occupies the largest surface area in the RSU

## Table definition

| Field name   | Field type       | Definition         | Constraint  |
| :----------- | :--------------- | ------------------ | ----------- |
| ID_RSU       | integer          | RSU unique id      | Primary Key |
| LCZ1         | integer          | Main LCZ type      |             |
| LCZ2         | integer          | Secondary LCZ type |             |
| MIN_DISTANCE | double precision |                    |             |
| PSS          | double precision |                    |             |



## LCZ typology

Below are presented the different LCZ types. For each of them, we give the definition and the hexadecimal color code used to display those informations.

| Type |       Type definition       |             Hexa Color code              |
| :--: | :-------------------------: | :--------------------------------------: |
|  1   |  LCZ 1: Compact high-rise   | <font color='#8B0101'>**#8B0101**</font> |
|  2   |   LCZ 2: Compact mid-rise   | <font color='#cc0200'>**#cc0200**</font> |
|  3   |   LCZ 3: Compact low-rise   | <font color='#fc0001'>**#fc0001**</font> |
|  4   |    LCZ 4: Open high-rise    | <font color='#be4c03'>**#be4c03**</font> |
|  5   |    LCZ 5: Open mid-rise     | <font color='#ff6602'>**#ff6602**</font> |
|  6   |    LCZ 6: Open low-rise     | <font color='#ff9856'>**#ff9856**</font> |
|  7   | LCZ 7: Lightweight low-rise | <font color='#fbed08'>**#fbed08**</font> |
|  8   |    LCZ 8: Large low-rise    | <font color='#bcbcba'>**#bcbcba**</font> |
|  9   |    LCZ 9: Sparsely built    | <font color='#ffcca7'>**#ffcca7**</font> |
|  10  |   LCZ 10: Heavy industry    | <font color='#57555a'>**#57555a**</font> |
| 101  |     LCZ A: Dense trees      | <font color='#006700'>**#006700**</font> |
| 102  |   LCZ B: Scattered trees    | <font color='#05aa05'>**#05aa05**</font> |
| 103  |      LCZ C: Bush,scrub      | <font color='#648423'>**#648423**</font> |
| 104  |      LCZ D: Low plants      | <font color='#bbdb7a'>**#bbdb7a**</font> |
| 105  |  LCZ E: Bare rock or paved  | <font color='#010101'>**#010101**</font> |
| 106  |  LCZ F: Bare soil or sand   | <font color='#fdf6ae'>**#fdf6ae**</font> |
| 107  |        LCZ G: Water         | <font color='#6d67fd'>**#6d67fd**</font> |

### Representation

Two `.sld` style files, based on this classification, are provided in the `/processing_chain/src/main/resources/styles/` folder :
- `lcz1.sld` : the style ready to be applied to the field `LCZ1`
- `lcz2.sld` : the style ready to be applied to the field `LCZ2`

![](./images/lcz_legend.png)

