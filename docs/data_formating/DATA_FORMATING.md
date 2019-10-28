# Module 2 - Formating and quality control



The purpose of this module is to format the data from M1 in order to feed M3. As such, it allows the following two tasks in particular:
- enrich the data, based on pre-established rules,
- control data quality.



## Data enrichment



### Primary Key ![](../images/icons/pk.png)

All the `input_table` coming from M1 have to have an `id_source` column, that identify the input object. Since this *id* is stored as a text value (`VARCHAR`) we prefer to add a new unique and optimized *id* called `id_xxxx` (with `xxxx` the name of the layer - *e.g* `id_building`) and stored as a numeric (INTEGER /  SERIAL). In the same time, the PRIMARY KEY constraint is added in order to create an `INDEX` on this column (will be useful for the following processes).



### Building rules

#### HEIGHTs and Number of levels

In the layer `input_building` coming from M1, the fields `HEIGHT_WALL`, `HEIGHT_ROOF` and `NB_LEV` may have null values. To resolve this issue, we propose a set of logical rules in order to deduce probable values, using `type` and potential informations in the fields `HEIGHT_WALL`, `HEIGHT_ROOF` and `NB_LEV`.







### Road rules

#### Width

## Quality control

