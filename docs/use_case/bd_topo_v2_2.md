# BD Topo v2.2 use case

The BD Topo is a topographic vector database, made by the french National Geographic Institute (IGN). This database is "*a 3D vectorial description (structured in objects) of the elements of the territory and its infrastructures, of metric precision, usable at scales ranging from 1: 5 000 to 1: 50 000.*"

In this use case, we are working with the [version 2.2](http://professionnels.ign.fr/ancienne-bdtopo) (which was replaced in 2019 by the [version 3.0](http://professionnels.ign.fr/bdtopo) - a dedicated use case may be implemented in the coming months).

In practice, this database is:

- **not open**,
- free of charge only for research or eduction purposes,
- provided as shapefiles (.shp) or as PostGreSQL dumps, at the french department scale.



In this use case, we are executing the three following modules:

- M1 - Input data preparation
- M2 - [Formating and quality control](../data_formating/DATA_FORMATING.md)
- M3 - Geoindicators and LCZ's production



## Input layers

Below are listed the BD Topo layers used to feed the [input data model](../input_data/INPUT_DATA_MODEL.md).


| BD Topo layer name | Input data model matching |
| :--------: | :----: |
| [BATI_INDIFFERENCIE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=80) | input_building / BUILDING |
| [BATI_INDUSTRIEL](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=89) | input_building / BUILDING |
| [BATI_REMARQUABLE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=85) | input_building / BUILDING |
| [ROUTE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=13) | input_road / ROAD |
| [TRONCON_VOIE_FERREE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=44) | input_rail / RAIL |
| [ZONE_VEGETATION](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=116) | input_veget/ VEGET |
| [SURFACE_EAU](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=66) | input_hydro / HYDRO |
| [TERRAIN_SPORT](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=103) | input_impervious / IMPERVIOUS |
| [CONSTRUCTION_SURFACIQUE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=113) | input_impervious / IMPERVIOUS |
| [SURFACE_ROUTE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=35) | input_impervious / IMPERVIOUS |
| [SURFACE_ACTIVITE](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=134) | input_impervious / IMPERVIOUS |



## Filtering of objects

In some cases, it is necessary to filter the input data, in order to keep only needed informations. 

Below we are detailing for all layers from BD Topo wether a filter is applied or not (in this case, every objects are keeped).

| BD Topo layer name | Filter ? |
| :--------: | :----: |
| BATI_INDIFFERENCIE | no filter |
| BATI_INDUSTRIEL |                          no filter                           |
| BATI_REMARQUABLE | no filter |
| ROUTE | no filter |
| TRONCON_VOIE_FERREE | no filter |
| ZONE_VEGETATION | no filter |
| SURFACE_EAU| no filter |
| TERRAIN_SPORT | where [`NATURE`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=104) = ['Piste de sport' / 'Terrain de tennis'] |
| CONSTRUCTION_SURFACIQUE | where [`NATURE`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=114) = ['Barrage' / 'Ecluse' / 'Escalier'] |
| SURFACE_ROUTE | no filter |
| SURFACE_ACTIVITE | where [`CATEGORIE`](http://professionnels.ign.fr/doc/DC_BDTOPO_2.pdf#page=135) = ['Administratif' / 'Enseignement' / 'Santé'] |



## Matching tables

Below are listed the correspondence tables between the values from the BD Topo (stored in a column called `NATURE`) and those expected in the [input data model](../input_data/INPUT_DATA_MODEL.md).

The tables are grouped by theme and present the following informations:

- **Layer name** : name of the concerned layer from BD Topo v2.2
- **NATURE** : input value, in the table "Layer name" 
- **TYPE** : corresponding / expected value



### For buildings

| Layer name | NATURE | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-building-use-and-type)) |
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

### For roads

| Layer name | NATURE | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-road-type)) |
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


### For railways

| Layer name | NATURE | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-rail-type)) |
| :--------: | :----: | :--: |
| TRONCON_VOIE_FERREE | LGV |highspeed|
| TRONCON_VOIE_FERREE | Principale |rail|
| TRONCON_VOIE_FERREE | Voie de service |service_track|
| TRONCON_VOIE_FERREE | Voie non exploitée |disused|
| TRONCON_VOIE_FERREE | Transport urbain |tram|
| TRONCON_VOIE_FERREE | Funiculaire ou crémaillère |funicular|
| TRONCON_VOIE_FERREE | Metro |subway|
| TRONCON_VOIE_FERREE | Tramway |tram|


### For vegetation areas

| Layer name | NATURE | TYPE ([see](../input_data/INPUT_DATA_MODEL.md#-veget-type)) |
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
