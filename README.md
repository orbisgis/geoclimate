# Geoclimate

Geoclimate chain is a collection of spatial processes to produce vector maps of any place in the world. 

First, algorithms are used to extract and transform data (such as [OpenStreetMap](https://www.openstreetmap.org) (OSM)) to a set of GIS layers feeding the "GeoClimate abstract model". These GIS layers are then processed  to compute urban indicators at three geographic scales : RSU, block and building ([Bocher et al, 2018](http://dx.doi.org/10.1016/j.uclim.2018.01.008)). Finally, these indicators are used to feed the [TEB](http://www.umr-cnrm.fr/spip.php?article199) model, classify the urban tissues and build the *Local Climate Zones* ([LCZ](http://www.wudapt.org/lcz/)).

![bandeau_geoclimate](./docs/resources/images/readme/bandeau_geoclimate.png)



The Geoclimate chain algorithms are implemented on top of the open source library [OrbisData](https://github.com/orbisgis/orbisdata). Orbisdata provides a unique access point to query, manage, retrieve data from a [PostGIS](https://postgis.net/) or a [H2GIS](http://www.h2gis.org/) database. Orbisdata is based on lambda expressions and sugar programming methods introduced since JAVA 8. Orbisdata is closed to Groovy syntax and provide an elegant and fluent framework to easily manage geospatial data. The processing chain is packaged in the GeoClimate repository available as a set of  Groovy scripts.


## Documentation

Want to know more ? Please have a look to the dedicated documentation [**here**](./docs/README.md).



## Authors

Geoclimate has been developed by researchers and engineers from the french [Lab-STICC](https://www.labsticc.fr/en/index/) laboratory (CNRS UMR 6285 - DECIDE team - GIS group). They are also authors of the [OrbisGIS](http://orbisgis.org/) and [H2GIS](http://h2gis.org/) applications.



## Funding

The Geoclimate library has been developed within the two following research projects:

- [URCLIM](http://www.urclim.eu/), part of ERA4CS, a project initiated by JPI Climate and co-funded by the European Union
- [PAENDORA](https://www.ademe.fr/sites/default/files/assets/documents/aprademeprojetsretenusen2017.pdf#page=39) , funded by [ADEME](