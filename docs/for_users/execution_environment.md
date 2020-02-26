# Execute the Geoclimate chain


## How to execute the Geoclimate chain

Environnement pour exécuter la chaîne geoclimate. 


### Grab

On introduit la récupération des librairies avec @grab

http://docs.groovy-lang.org/latest/html/documentation/grape.html

## Requirements

To run the Geoclimate chain, you must have previously installed on your machine the two following free and open source applications:

- Java (from [Oracle](https://www.java.com/fr/download/) or [OpenJDK](https://openjdk.java.net/)) (at the least Version 8).
- [Groovy](https://groovy-lang.org/) version 3.0.1

**Tips**: You can install these two apps by your own, or choose to install them directly thanks to the [SDKMan](https://sdkman.io/) app following the instructions [here](https://sdkman.io/install).

## Execution tools

Geoclimate can, among other things, be executed using two tools presented below:

- [DBeaver](./execution_tools.md)
- The [Groovy Console](./execution_tools.md)




## Use cases

To see Geoclimate in action, you can consult the two following step-by-step documentations:

- With [OSM](./execution_examples/run_osm.md)
- With [BD Topo v2](./execution_examples/run_bd_topo_v2.md)



## Geoclimate and DB manager

Geoclimate uses a spatial database to store and process in a SQL way the data. 

The default database is [H2GIS](http://www.h2gis.org/) but the user can choose to set a [PostGreSQL](https://www.postgresql.org/) / [PostGIS](https://postgis.net/) connection instead. In this case:

 - The temporary tables should respect the pattern : `tableName_UUID` with `-` replaced by `_` if needed.
 - Index should be create using the PostgreSQL syntax : `CREATE INDEX IF EXISTS indexName ON table USING RTREE(columnName)`.
 - The processes should be documented with a description of the process followed by the inputs with `@param` and then the outputs with `@return`. As example :

``` java
/**
* Description of my process
* @param inputName Input description
* @param inputName Input description
* @return outputName Output description
*/
```