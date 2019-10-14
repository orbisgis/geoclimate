# geoclimate

Geoclimate chain is a collection of spatial processes to produce vector maps of cities. First, algorithms are used to extract and transform OpenStreetMap data to a set of GIS layers (GeoClimate abstract model). The GIS layers are then processed  to compute urban indicators at three geographic scales : RSU, block and building (Bocher et al, 2018). These indicators are used to feed the TEB model, classify the urban tissues and build the LCZ zones.

The algorithms of the Geoclimate chain are implemented on top of the open source library OrbisData (https://github.com/orbisgis/orbisdata). Orbisdata provides a unique access point to query, manage, retrieve data from a PostGIS or a H2GIS database. Orbisdata is based on lambda expressions and sugar programming methods introduced since JAVA 8. Orbisdata is closed to Groovy syntax and provide an elegant and fluent framework to easily manage geospatial data. The processing chain is packaged in the GeoClimate repository available as a set of  groovy scripts.

# Download

Geoclimate is avalaible as maven artifact from the repository http://nexus.orbisgis.org

To use the current snapshot add in the pom

```xml
<dependency>
  <groupId>org.orbisgis</groupId>
  <artifactId>geoclimate</artifactId>
  <version>1.0-SNAPSHOT</version>
  <type>pom</type>
</dependency>
```


# How to use

The simple way to use the Geoclimate chain is to run it in a Groovy console, using Grab annotation (http://groovy-lang.org/groovyconsole.html).

Put the following script and run it to extract OSM data from a place name and transform it to a set of GIS Layers.

```groovy
// Declaration of the maven repository
@GrabResolver(name='orbisgis', root='http://nexus-ng.orbisgis.org/repository/orbisgis/')
@Grab(group='org.orbisgis', module='prepare_data', version='1.0-SNAPSHOT')

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.PrepareData
import org.orbisgis.processmanagerapi.IProcess

//Create a local H2GIS database
def h2GIS = H2GIS.open('/tmp/osmdb;AUTO_SERVER=TRUE')

//Run the process to extract OSM data from a place name and transform it to a set of GIS layers needed by Geoclimate
IProcess process = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
         process.execute([
                datasource : h2GIS,
                placeName: "Vannes"])
 
 //Save the GIS layers in a shapeFile        
 process.getResults().each {it ->
        if(it.value!=null && !it.value.isInteger()){
                h2GIS.getTable(it.value).save("/tmp/${it.value}.shp")
            }
        }

```




### Notes

Geoclimate uses a spatial database to store and process in a SQL way the data. The default datase is H2GIS but the user can set a PostGIS connection.

 - The temporary tables should respect the pattern : `tableName_UUID` with `-` replaced by `_` if needed.
 - Index should be create using the Postgresql syntax : `CREATE INDEX IF EXISTS indexName ON table(columnName) USING RTREE`.
 - The processes should be documented with a description of the process followed by the inputs with `@param` and then the outputs with `@return`. As example :
    ``` java
    /**
    * Description of my process
    * @param inputName Input description
    * @param inputName Input description
    * @return outputName Output description
    */
    ```
