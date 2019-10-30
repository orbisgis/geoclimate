# BD Topo v2.2 use case









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
