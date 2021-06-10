# Resources for developers



## Maven architecture



Geoclimate is avalaible as a Maven artifact from the repository http://nexus.orbisgis.org

To use the current snapshot add the following lines in the `pom`

```xml
<dependency>
  <groupId>org.orbisgis.geoclimate</groupId>
  <artifactId>geoclimate</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```



## Integrate Geoclimate in your project




## Source code organisation



The source code of Geoclimate is stored on the following Github repository : [https://github.com/orbisgis/geoclimate/](https://github.com/orbisgis/geoclimate/)



The source code tree can be summarized as follow

- geoclimate
- geoindicators : groovy file for all the indicators and classifications
- preparedata
  - bdtopo : processes to prepare the BD Topo
  - common : common processes
  - osm : processes to prepare OSM
- processingchain : processing scripts, to execute some parts or the entire Geoclimate chain
