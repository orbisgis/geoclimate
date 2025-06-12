## Changelog for v1.0.1

- Upgrade dependencies H2, H2GIS, JTS and Groovy
- Fix some tests due to some rounding precision in JTS 1.20
- Fix bad OSM geometry representation #994
- Fix height value parsing with OSM #995
- Simplify database url in config file. Use only the name of the database. e.g : h2://, postgis://
- Add a test to check if the worldpop service is available
- Fix github actions
- Add TARGET landcover production
- Force TARGET ROOF fraction to 0.75 when BUILDING fraction is greater than 0.75
- Force TARGET W indicator to the grid resolution- 
- Add lanes informations on abstract model plus OSM and BDTopo drivers
- New grid indicator COUNT_WARM_X where X is the step of the sliding window
- Fix water extraction with OSM
