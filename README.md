# Geoclimate

Geoclimate chain is a collection of spatial processes to produce vector maps of cities. First, algorithms are used to extract and transform [OpenStreetMap](https://www.openstreetmap.org) (OSM) data to a set of GIS layers (GeoClimate abstract model). The GIS layers are then processed  to compute urban indicators at three geographic scales : RSU, block and building ([Bocher et al, 2018](http://dx.doi.org/10.1016/j.uclim.2018.01.008)). These indicators are used to feed the [TEB](http://www.umr-cnrm.fr/spip.php?article199) model, classify the urban tissues and build the *Local Climate Zones* ([LCZ](http://www.wudapt.org/lcz/)).

The algorithms of the Geoclimate chain are implemented on top of the open source library OrbisData (https://github.com/orbisgis/orbisdata). Orbisdata provides a unique access point to query, manage, retrieve data from a [PostGIS](https://postgis.net/) or a [H2GIS](http://www.h2gis.org/) database. Orbisdata is based on lambda expressions and sugar programming methods introduced since JAVA 8. Orbisdata is closed to Groovy syntax and provide an elegant and fluent framework to easily manage geospatial data. The processing chain is packaged in the GeoClimate repository available as a set of  Groovy scripts.

# Download

Geoclimate is avalaible as a Maven artifact from the repository http://nexus.orbisgis.org

To use the current snapshot add in the `pom`

```xml
<dependency>
  <groupId>org.orbisgis.orbisprocess</groupId>
  <artifactId>geoclimate</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```


# How to use

The simple way to use the Geoclimate chain is to run it in a Groovy console, using Grab annotation (http://groovy-lang.org/groovyconsole.html).

Put the following script and run it to extract OSM data from a place name and transform it to a set of GIS layers.

```groovy
// Declaration of the maven repository
@GrabResolver(name='orbisgis', root='http://nexus-ng.orbisgis.org/repository/orbisgis/')
@Grab(group='org.orbisgis.orbisprocess', module='geoclimate', version='1.0.0-SNAPSHOT')

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.orbisprocess.geoclimate.Geoclimate

//Uncomment next line to override the Geoclimate logger
//Geoclimate.logger = logger

//Create a local H2GIS database
def h2GIS = H2GIS.open('/tmp/osmdb;AUTO_SERVER=TRUE')

//Run the process to extract OSM data from a place name and transform it to a set of GIS layers needed by Geoclimate
def process = Geoclimate.OSMGISLayers.extractAndCreateGISLayers()
         process.execute([
                datasource : h2GIS,
                placeName: "Vannes"])
 
 //Save the GIS layers in a shapeFile        
 process.getResults().each {it ->
        if(it.value!=null && it.key!="epsg"){
                h2GIS.getTable(it.value).save("/tmp/${it.value}.shp")
            }
        }

```
The next script computes all geoindicators needed by the [TEB](http://www.umr-cnrm.fr/spip.php?article199) model. To run it, the user must set a place name and a connexion to a spatial database (H2GIS or PostGIS). As described above, the script extract the OSM data and transform it to a set of GIS layers requiered by the Geoclimate chain. Then a set of algorithms is executed to compute, the 3 geo-units (building, block and RSU). For each geo-units, geographical parameters like density, form, compactness, distance, *Sky View Factor* (SVF) are computed.

```groovy
@GrabResolver(name='orbisgis', root='http://nexus-ng.orbisgis.org/repository/orbisgis/')
@Grab(group='org.orbisgis.orbisprocess', module='geoclimate', version='1.0.0-SNAPSHOT')

//Uncomment next line to override the Geoclimate logger
//Geoclimate.logger = logger

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.orbisprocess.geoclimate.Geoclimate

//Folder to create the H2GIS database
def directory ="/tmp/geoclimate_chain"
def dirFile = new File(directory)
dirFile.delete()
dirFile.mkdir()
def datasource = H2GIS.open(dirFile.absolutePath+File.separator + "geoclimate_chain_db;AUTO_SERVER=TRUE")
def process = Geoclimate.Workflow.OSM()
if(process.execute(datasource: datasource, placeName: "Cliscouet,Vannes")){
    def saveTables = Geoclimate.DataUtils.saveTablesAsFiles()
    saveTables.execute( [inputTableNames: process.getResults().values()
                         , directory: directory, datasource: datasource])
}
```
The next script shows how to run the Geoclimate chain on the BDTopo data (version 2.0). The user must set to the script the path to an input folder that contains the BDTopo data plus the IRIS_GE as shapefiles. 
The results are stored in geojson files on a folder set by the user.

```groovy
@GrabResolver(name='orbisgis', root='http://nexus-ng.orbisgis.org/repository/orbisgis/')
@Grab(group='org.orbisgis.orbisprocess', module='geoclimate', version='1.0.0-SNAPSHOT')

//Uncomment next line to override the Geoclimate logger
//Geoclimate.logger = logger

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.orbisprocess.geoclimate.Geoclimate

//Folder to create the H2GIS database
def directory ="/tmp/geoclimate_chain"
def dirFile = new File(directory)
dirFile.delete()
dirFile.mkdir()
def datasource = H2GIS.open(dirFile.absolutePath+File.separator + "geoclimate_chain_db;AUTO_SERVER=TRUE")
def process = Geoclimate.Workflow.BBTOPO_V2()
process.execute(datasource: datasource,
                inputFolder: "../target/bdtopofolder/",
                outputFolder :"../target/geoclimate_chain/")

```

# Use Geoclimate in DBeaver

[DBeaver](https://dbeaver.io/) is an opensource multi-platform database tool to query, explore and manage data. Since the  6.2.2 version, DBeaver support the H2GIS-H2 database. User is able to create a H2GIS database, query and display spatial objects from a friendly user interface (see https://twitter.com/H2GIS/status/1181566934548176897).

To use Geoclimate scripts in DBeaver, user must install the Groovy Editor developed by the OrbisGIS team.  

In DBeaver, go to 

    1. Main menu Help -> Install New Software
    2. Paste extension P2 repository URL http://devs.orbisgis.org/eclipse-repo into Work with field,
    press Enter
    3. Select Groovy Editor item
    4. Click Next-> Finish. Restart DBeaver.

Once DBeaver has restarted, select the main menu Groovy Editor, clic on `Open editor`, then you will have a Groovy Console.
Copy-paste the previous script to use Geoclimate.
If you want to log the message of the processes into DBeaver, you must add `Geoclimate.logger = logger` just after the imports.


### Notes

Geoclimate uses a spatial database to store and process in a SQL way the data. The default datase is H2GIS but the user can set a PostGIS connection.

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
