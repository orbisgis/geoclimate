# OSM workflow

## Context

This workflow has been developed within the [URCLIM](http://www.urclim.eu/) research project, funded by the European Union.



## The OpenStreetMap database



- Présentation d'OSM
- Pourquoi on utilise cette base de données ?



## Workflow description

Les grandes phases

- extraction --> [Data preparation](./data_preparation.md)
- formatage --> [Data formating](./data_formating.md)
- couplage --> [Coupling with Geoclimate](./coupling_with_geoclimate.md)







## The configuration files



Four OSM workflow (.geojson) configuration files are available on the Geoclimate Github repository [here](https://github.com/orbisgis/geoclimate/tree/master/processingchain/src/test/resources/org/orbisgis/orbisprocess/geoclimate/processingchain/config):

1. [Using an envelop and exporting in a folder](https://github.com/orbisgis/geoclimate/blob/master/processingchain/src/test/resources/org/orbisgis/orbisprocess/geoclimate/processingchain/config/osm_workflow_envelope_folderoutput.json) 
2. [Using mixed filters and exporting in a folder](https://github.com/orbisgis/geoclimate/blob/master/processingchain/src/test/resources/org/orbisgis/orbisprocess/geoclimate/processingchain/config/osm_workflow_mixedfilter_folderoutput.json)
3. [Using a placename and exporting in a database](https://github.com/orbisgis/geoclimate/blob/master/processingchain/src/test/resources/org/orbisgis/orbisprocess/geoclimate/processingchain/config/osm_workflow_placename_dboutput.json)
4. [Using a placename and exporting in a folder](https://github.com/orbisgis/geoclimate/blob/master/processingchain/src/test/resources/org/orbisgis/orbisprocess/geoclimate/processingchain/config/osm_workflow_placename_folderoutput.json)

