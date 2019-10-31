# BD Topo v2.2 use case

The BD Topo is a topographic vector database, made by the french National Geographic Institute ([IGN](http://ign.fr/)). This database is "*a 3D vectorial description (structured in objects) of the elements of the territory and its infrastructures, of metric precision, usable at scales ranging from 1: 5 000 to 1: 50 000.*"

In this use case, we are working with the [version 2.2](http://professionnels.ign.fr/ancienne-bdtopo) (which was replaced in 2019 by the [version 3.0](http://professionnels.ign.fr/bdtopo) - a dedicated use case may be implemented in the coming months).

In practice, this database is:

- **not open**,
- free of charge only for research or eduction purposes,
- provided as shapefiles (.shp) or as PostGreSQL dumps, at the french department scale.



In this use case, we are executing the three following modules:

- M1 - [Input data preparation](#m1---input-data-preparation)
- M2 - [Formating and quality control](#m2---formating-and-quality-control)
- M3 - Geoindicators and LCZ's production



## M1 - Input data preparation

This part aims to illustrate the steps described in the [Module 1](../data_preparation/DATA_PREPARATION.md). 

Here, we are specifically presenting:

- the [input layers](#Input-layers)
- the [filtering of objects](#Filtering-of-objects)
- the [matching tables](#Matching-tables)
- the [initialization actions](#Initialization-actions)



### Input layers

#### Zone layer

As explained beforehand, we will process the BD Topo zone by zone. In this example, the `ZONE` will be a French city.

In the meantime, for the purposes of the [PAENDORA](https://www.ademe.fr/sites/default/files/assets/documents/aprademeprojetsretenusen2017.pdf#page=39) project in which this example is included, we need to combine Geoclimate results with population data. In France, this kind of information is commonly provided at the [IRIS](https://www.insee.fr/fr/metadonnees/definition/c1523) scale. An IRIS (Ilots Regroupés pour l’Information Statistique - grouped islet for the statistical information) is a basic spatial scale for the dissemination of local data such as population statistics, household consumption, professional social categories, ...

##### IRIS *vs* city

A city can consist of 1 to *n* IRIS and an IRIS belongs to only one city. So by merging IRIS geometries (in a temporary table called `TMP_IRIS`) that have a common city Id (nammed `CODE_INSEE` - `ID_ZONE`), we can generate the city (`ZONE`) shape.

![](../images/iris.png)



##### Input data

In this use case, we use the [`IRIS_GE`](http://professionnels.ign.fr/irisge) layer produced by the IGN, mainly because the geometries are consistent with the one from BD Topo.

|                 IRIS layer name                  | Input data model matching |
| :----------------------------------------------: | :-----------------------: |
| [`IRIS_GE`](http://professionnels.ign.fr/irisge) |           ZONE            |

#### Thematic layers

Below are listed the BD Topo layers used to feed the [input data model](../input_data/INPUT_DATA_MODEL.md).


| BD Topo layer name | Input data model matching |
| :--------: | :----: |
| [BATI_INDIFFERENCIE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=80) | input_building |
| [BATI_INDUSTRIEL](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=89) | input_building |
| [BATI_REMARQUABLE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=85) | input_building |
| [ROUTE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=13) | input_road |
| [TRONCON_VOIE_FERREE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=44) | input_rail |
| [ZONE_VEGETATION](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=116) | input_veget |
| [SURFACE_EAU](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=66) | input_hydro |
| [TERRAIN_SPORT](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=103) | input_impervious |
| [CONSTRUCTION_SURFACIQUE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=113) | input_impervious |
| [SURFACE_ROUTE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=35) | input_impervious |
| [SURFACE_ACTIVITE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=134) | input_impervious |

[back to top](#BD-Topo-v22-use-case)

### Filtering of objects

In some cases, it is necessary to filter the input data, in order to keep only needed informations. 

Below we are detailing for all layers from BD Topo wether a filter is applied or not (in this case, every objects are keeped).

| BD Topo layer name | Filter ? |
| :--------: | :----: |
| BATI_INDIFFERENCIE | no filter |
| BATI_INDUSTRIEL |                          no filter                           |
| BATI_REMARQUABLE | no filter |
| ROUTE | where [`POS_SOL`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=23) >= 0 |
| TRONCON_VOIE_FERREE | where [`POS_SOL`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=47) >= 0 |
| ZONE_VEGETATION | no filter |
| SURFACE_EAU| no filter |
| TERRAIN_SPORT | where [`NATURE`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=104) = ['Piste de sport' / 'Terrain de tennis'] |
| CONSTRUCTION_SURFACIQUE | where [`NATURE`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=114) = ['Barrage' / 'Ecluse' / 'Escalier'] |
| SURFACE_ROUTE | no filter |
| SURFACE_ACTIVITE | where [`CATEGORIE`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=135) = ['Administratif' / 'Enseignement' / 'Santé'] |

[back to top](#BD-Topo-v22-use-case)

### Matching tables

Below are listed the correspondence tables between the values from the BD Topo (stored in a column called `NATURE`) and those expected in the [input data model](../input_data/INPUT_DATA_MODEL.md).

The tables are grouped by theme and present the following informations:

- **Layer name** : name of the concerned layer from BD Topo v2.2
- **NATURE** : input value, in the table "Layer name" 
- **TYPE** : corresponding / expected value



#### For buildings

| Layer name | NATURE ([see](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=86)) | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-building-use-and-type)) |
| :--------: | :----: | :--: |
| BATI_INDIFFERENCIE |        | building |
| BATI_INDUSTRIEL | Bâtiment agricole |farm_auxiliary|
| BATI_INDUSTRIEL | Bâtiment commercial |commercial|
| BATI_INDUSTRIEL | Bâtiment industriel |industrial|
| BATI_INDUSTRIEL | Serre |greenhouse|
| BATI_INDUSTRIEL | Silo |silo|
| BATI_REMARQUABLE | Aérogare |terminal|
| BATI_REMARQUABLE | Arc de triomphe |monument|
| BATI_REMARQUABLE | Arène ou théâtre antique |monument|
| BATI_REMARQUABLE | Bâtiment religieux divers |religious|
| BATI_REMARQUABLE | Bâtiment sportif |sports_centre|
| BATI_REMARQUABLE | Chapelle |chapel|
| BATI_REMARQUABLE | Château |castle|
| BATI_REMARQUABLE | Eglise |church|
| BATI_REMARQUABLE | Fort, blockhaus, casemate |military|
| BATI_REMARQUABLE | Gare |train_station|
| BATI_REMARQUABLE | Mairie |townhall|
| BATI_REMARQUABLE | Monument |monument|
| BATI_REMARQUABLE | Péage |toll_booth|
| BATI_REMARQUABLE | Préfecture |government|
| BATI_REMARQUABLE | Sous-préfecture |government|
| BATI_REMARQUABLE | Tour, donjon, moulin |historic|
| BATI_REMARQUABLE | Tribune |grandstand|

#### For roads

| Layer name | NATURE ([see](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=17)) | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-road-type)) |
| :--------: | :----: | :--: |
| ROUTE | Autoroute |motorway|
| ROUTE | Quasi-autoroute |trunk|
| ROUTE | Bretelle |highway_link|
| ROUTE | Route à 2 chaussées |primary|
| ROUTE | Route à 1 chaussée |unclassified|
| ROUTE | Route empierrée |track|
| ROUTE | Chemin |track|
| ROUTE | Bac auto |ferry|
| ROUTE | Bac piéton |ferry|
| ROUTE | Piste cyclable |cycleway|
| ROUTE | Sentier |path|
| ROUTE | Escalier |steps|


#### For railways

| Layer name | NATURE ([see](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=45)) | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-rail-type)) |
| :--------: | :----: | :--: |
| TRONCON_VOIE_FERREE | LGV |highspeed|
| TRONCON_VOIE_FERREE | Principale |rail|
| TRONCON_VOIE_FERREE | Voie de service |service_track|
| TRONCON_VOIE_FERREE | Voie non exploitée |disused|
| TRONCON_VOIE_FERREE | Transport urbain |tram|
| TRONCON_VOIE_FERREE | Funiculaire ou crémaillère |funicular|
| TRONCON_VOIE_FERREE | Metro |subway|
| TRONCON_VOIE_FERREE | Tramway |tram|


#### For vegetation areas

| Layer name | NATURE ([see](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=117)) | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-veget-type)) |
| :--------: | :----: | :--: |
| ZONE_VEGETATION | Zone arborée |wood|
| ZONE_VEGETATION | Forêt fermée de feuillus |forest|
| ZONE_VEGETATION | Forêt fermée mixte |forest|
| ZONE_VEGETATION | Forêt fermée de conifères |forest|
| ZONE_VEGETATION | Forêt ouverte |forest|
| ZONE_VEGETATION | Peupleraie |forest|
| ZONE_VEGETATION | Haie |hedge|
| ZONE_VEGETATION | Lande ligneuse |heath|
| ZONE_VEGETATION | Verger |orchard|
| ZONE_VEGETATION | Vigne |vineyard|
| ZONE_VEGETATION | Bois |forest|
| ZONE_VEGETATION | Bananeraie |banana_plants|
| ZONE_VEGETATION | Mangrove |mangrove|
| ZONE_VEGETATION | Canne à sucre |sugar_cane|

[back to top](#BD-Topo-v22-use-case)

### Initialization actions

Below we are listing all the initialization actions made on the input data


#### For buildings


- Objects from`BATI_INDIFFERENCIE`, `BATI_INDUSTRIEL` and `BATI_REMARQUABLE` layers are merged into a common table
- `ID` column is renamed `ID_SOURCE`
- `HAUTEUR` column is renamed `HEIGHT_WALL`
- Since there is no such informations in the BD Topo:
  - `HEIGHT_ROOF` is filled with the text value `''` (= null value)
  - `NB_LEV` is filled with the text value `''` (= null value)
  - `MAIN_USE` is filled with the text value `''` (= null value)
  - `ZINDEX` is forced to be equal to `0` 
- `TYPE` column is feeded with values coming from `NATURE` column, using the [matching rules](#Matching-tables). For the buildings from the `BATI_INDIFFERENCIE` layer, since there is no `NATURE` column, we forced the `TYPE` to `building`.
- geometries (coming from `BATI_INDIFFERENCIE`, `BATI_INDUSTRIEL` and `BATI_REMARQUABLE`) are [exploded](http://www.h2gis.org/docs/dev/ST_Explode/) in order to works only with simple geometries.


#### For roads


- Only objects having a `POS_SOL` (or `ZINDEX`) >= 0 are keeped
- `ID` column is renamed `ID_SOURCE`
- `LARGEUR` column is renamed `WIDTH`
- `POS_SOL` column is renamed `ZINDEX`
- Since there is no such informations in the BD Topo:
  - `SURFACE` is filled with the text value `''` (= null value)
  - `SIDEWALK` is filled with the text value `''` (= null value)
- `TYPE` column is feeded with values coming from `NATURE` column, using the [matching rules](#Matching-tables). 


#### For railways


- Only objects having a `POS_SOL` (or `ZINDEX`) >= 0 are keeped

- `ID` column is renamed `ID_SOURCE`

- `POS_SOL` column is renamed `ZINDEX`

- `TYPE` column is feeded with values coming from `NATURE` column, using the [matching rules](#Matching-tables). 



#### For vegetation areas


- `ID` column is renamed `ID_SOURCE`
- `TYPE` column is feeded with values coming from `NATURE` column, using the [matching rules](#Matching-tables). 


#### For hydrographic areas


- `ID` column is renamed `ID_SOURCE`



#### For impervious areas


- Objects from `TERRAIN_SPORT`, `CONSTRUCTION_SURFACIQUE`, `SURFACE_ROUTE` and `SURFACE_ACTIVITE` layers are merged into a common table
- Only objects having a `NATURE` or `CATEGORIE` listed [before](#Filtering-of-objects) are keeped
- `ID` column is renamed `ID_SOURCE`



[back to top](#BD-Topo-v22-use-case)

## M2 - Formating and quality control

The [M1](#m1---input-data-preparation) steps seen before leads to the creation of the 8 following layers: `input_building`, `input_road`, `input_rail`, `input_veget`, `input_hydro`, `input_impervious`, `zone` and `zone_neighbors`.

These layers are now formated and controled thanks to the [Module 2](../data_formating/DATA_FORMATING.md). To summarize we list below the main actions carried out:

1. On each input layers, a PRIMARY KEY is added and stored in a column called `id_xxxx`
2. Buildings are enriched with:
   1. the belonging zone id
   2. the update of wall / roof heights and number of levels
3.  Roads width is updated
4. Vegetation height class is deduced
5. Quality controls are executed 



Once finished, we obtain the 7 following layers `BUILDING`, `ROAD`, `RAIL`, `VEGET`, `HYDRO`, `IMPERVIOUS`, `ZONE`, ready to be used by the Module 3.