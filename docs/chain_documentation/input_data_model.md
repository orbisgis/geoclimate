# Input data model

This page describes the requirements specification of the GIS layers.
It determines which data should be available and how they should be organized.  
It consists in a set of data staged in a tabular way (rows, columns, also called “table”). 

A GIS layer is an abstraction of reality specified by a geographic data model (geometry + attributes).
A layer represents a single geographic subject. 7 layers are used by the Geoclimate algorithms.

- [1- Buildings](#1--Buildings)
- [2- Roads](#2--Roads)
- [3- Railways](#3--Railways)
- [4- Vegetation areas](#4--Vegetation-areas)
- [5- Hydrographic areas](#5--Hydrographic-areas)
- [6- Impervious areas](#6--Impervious-areas)
- [7- Zones](#7--Zones)

For each of them, we provide a dictionary of values (with the name and type of the columns) and the associated metadata tables, in which description and parameters are stored.

### The input table structure ![](../resources/images/common/icons/table.png)

- the expected **table name** (e.g `BUILDING`) 
- the **name**, the **type** and the **constraints** of each **columns**.

**Remarks**

- ![](../resources/images/common/icons/earth.png) The tables have to use a same [EPSG](https://spatialreference.org/ref/epsg/) code, pointing to a **metric** coordinate system.
- ![](../resources/images/common/icons/pk.png) indicates the Primary Keys 

### The expected values ![](../resources/images/common/icons/table_values.png)

When necessary, a list of values that a column must take, with :

- the expected **Term**,
- a **Definition**,
- a **Source**: most of the values are based on concepts from existing thesauri. In this case, we indicate their source.


## 1- Buildings

### ![](../resources/images/common/icons/table.png) BUILDING table structure

This table represents any kind of building.

![](../resources/images/common/icons/arrow.png) Table name : `BUILDING` 



|      Name       |  Type   |                        Constaints                        | Definition                                                   |
| :-------------: | :-----: | :------------------------------------------------------: | ------------------------------------------------------------ |
|    the_geom     | POLYGON |                     ST_DIMENSION()=2                     | Geometry                                                     |
| **id_building** | INTEGER | ![](../resources/images/common/icons/pk.png) Primary Key | Unique Identifier                                            |
|    id_source    | VARCHAR |                        *not null*                        | Identifier of the feature from the input datasource          |
|     id_zone     | VARCHAR |                        *not null*                        | Studied Zone Identifier                                      |
|   height_wall   |  FLOAT  |                        *not null*                        | The (corrected) height of the building in meters. Height of the building measured between the ground and the gutter (maximum altitude of the polyline describing the building). *(expressed in meters)* |
|   height_roof   |  FLOAT  |                        *not null*                        | The maximum height of a building is the distance between the top edge of the building (including the roof, but excluding antennas, spires and other equipment mounted on the roof) and the lowest point at the bottom where the building meets the ground. *(expressed in meters)* |
|     nb_lev      | INTEGER |                        *not null*                        | Number of levels (have to be greater than 0)                 |
|      type       | VARCHAR |                        *not null*                        | Value allowing to distinguish the type of building according to its architecture. These values are listed in the  [BUILDING_use_and_type](#BUILDING-use-and-type) section. |
|    main_use     | VARCHAR |                                                          | Main use of the building. The use of a building corresponds to a de facto element, relating to what it is used for. These values are listed in the  [BUILDING_use_and_type](#BUILDING-use-and-type) section. |
|     zindex      | INTEGER |                                                          | Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the objects above the ground surface. -4 to -1 value indicates that the object is underground. |


### ![](../resources/images/common/icons/table_values.png) BUILDING use and type

List of all possible values for the `type` and the `main_use` attributes, in the `BUILDING` table. We consider that a same value can be used to qualify a `type` or a `main_use`.

| Term | Definition | Source |
| :----------: | ------------------------------------------------------------ | :----------------------------------------------------------: |
| building                        | Used to qualify any kind of feature that is a building       |    [1](https://wiki.openstreetmap.org/wiki/Key:building)     |
| house                           | A single dwelling unit usually inhabited by one family       | [2](https://wiki.openstreetmap.org/wiki/Tag:building=house)  |
| detached                        | A free-standing residential building usually housing a single-family. | [3](https://wiki.openstreetmap.org/wiki/Tag:building=detached) |
| residential                     | A building used primarily for residential purposes           | [4](https://wiki.openstreetmap.org/wiki/Tag:building=residential) |
| apartments                      | A building arranged into individual dwellings, often on separate floors. May also have retail outlets on the ground floor. | [5](https://wiki.openstreetmap.org/wiki/Tag:building=apartments) |
| bungalow                        | A small, single-storey detached house in the form of a bungalow | [6](https://wiki.openstreetmap.org/wiki/Tag:building=bungalow) |
| historic                        | Any buildings of historical interest                         |    [7](https://wiki.openstreetmap.org/wiki/Key:historic)     |
| monument                        | A memorial object, which is especially large, built to remember, show respect to a person or group of people or to commemorate an event. | [8](https://wiki.openstreetmap.org/wiki/Tag:historic=monument) |
| ruins                           | House that is an abandoned (but still a building)            | [9](https://wiki.openstreetmap.org/wiki/Tag:building=ruins)  |
| castle                          | Various kinds of structures, most of which were originally built as fortified residences of a lord or noble | [10](https://wiki.openstreetmap.org/wiki/Tag:historic=castle) |
| agricultural                    | A building, machinery, facilities, related to agricultural production. |                              11                              |
| farm                            | A farmhouse is the main building of a farm                   | [12](https://wiki.openstreetmap.org/wiki/Tag:building=farm)  |
| farm_auxiliary                  | A building on a farm that is not a dwelling                  | [13](https://wiki.openstreetmap.org/wiki/Tag:building=farm_auxiliary) |
| barn                            | An agricultural building used for storage and as a covered workplace | [14](https://wiki.openstreetmap.org/wiki/Tag:building=barn)  |
| greenhouse                      | A greenhouse (also called a glasshouse) is a building in which plants are grown. It typically has a roof and walls made of clear glass or plastic to allow sunlight to enter. | [15](https://wiki.openstreetmap.org/wiki/Tag:building=greenhouse) |
| silo                            | A storage container for bulk material, often grains such as corn or wheat | [16](https://wiki.openstreetmap.org/wiki/Tag:man_made=silo)  |
| commercial                      | A building where non-specific commercial activities take place | [17](https://wiki.openstreetmap.org/wiki/Tag:building=commercial) |
| industrial                      | A building where some industrial process takes place         | [18](https://wiki.openstreetmap.org/wiki/Tag:building=industrial) |
| sport                           | Buildings, constructions, installations, organized areas and equipment for indoor and outdoor sport activities. | [19](https://wiki.openstreetmap.org/wiki/Tag:building=sport) |
| sports_centre                   | Building that is designed for sports, e.g. for school sports, university or club sports | [20](https://wiki.openstreetmap.org/wiki/Tag:building=sports_centre) |
| grandstand                      | Building for the main stand, usually roofed, commanding the best view for spectators at racecourses or sports grounds | [21](https://wiki.openstreetmap.org/wiki/Tag:building=grandstand) |
| transportation                  | Buildings, constructions, installations, organized areas and equipment for transportation | [22](https://wiki.openstreetmap.org/wiki/Tag:building=transportation) |
| train_station                   | A train station building                                     | [23](https://wiki.openstreetmap.org/wiki/Tag:building=train_station) |
| toll_booth                      | Toll roads charge money for some or all traffic              | [24](https://wiki.openstreetmap.org/wiki/Tag:barrier=toll_booth) |
| terminal                        | An airport passenger building                                | [25](https://wiki.openstreetmap.org/wiki/Tag:aeroway=terminal) |
| healthcare                      | All places that provide healthcare                           |   [26](https://wiki.openstreetmap.org/wiki/Key:healthcare)   |
| education                       | All places that provide education                            | [27](https://wiki.openstreetmap.org/wiki/Map_Features#Education) |
| entertainment, arts and culture | All places that provide entertainment, arts and culture      | [28](https://wiki.openstreetmap.org/wiki/Key:amenity#Entertainment.2C_Arts_.26_Culture) |
| sustenance                      | Buildings, constructions, installations, organized areas and equipment of any food commodity or related food products. ex : bar, pub... | [29](https://wiki.openstreetmap.org/wiki/Map_Features#Sustenance) , [30](https://www.eionet.europa.eu/gemet/en/concept/3362) |
| military                        | Buildings, constructions, installations necessary to the performance of military activities, either combat or noncombat. | [31](https://www.eionet.europa.eu/gemet/en/concept/13220) , [32](https://wiki.openstreetmap.org/wiki/Map_Features#Military) , [33](https://wiki.openstreetmap.org/wiki/Key:military) |
| religious                       | Unspecific religious building                                | [34](https://wiki.openstreetmap.org/wiki/Key:building#Religious) |
| chapel                          | Religious building, often pretty small. One can enter in it to pray or meditate | [35](https://wiki.openstreetmap.org/wiki/Tag:building=chapel) |
| church                          | A building that was built as a church                        | [36](https://wiki.openstreetmap.org/wiki/Tag:building=church) |
| government                      | Building built to house government offices                   | [37](https://wiki.openstreetmap.org/wiki/Tag:building=government) |
| townhall                        | Building that may serve as an administrative center, or may be merely a community meeting place | [38](https://wiki.openstreetmap.org/wiki/Tag:amenity=townhall) |
| office                          | Office block typically houses companies, but offices may be also rented by any other kind of organization like charities, government, any NGO etc. | [39](https://wiki.openstreetmap.org/wiki/Tag:building=office) |

[back to top](#Input-data-model)

## 2- Roads

### ![](../resources/images/common/icons/table.png) ROAD table structure

This table represents any kind of road network.

![](../resources/images/common/icons/arrow.png) Table name:  `ROAD`



|    Name     |    Type    |                       Constraints                        | Definition                                                   |
| :---------: | :--------: | :------------------------------------------------------: | ------------------------------------------------------------ |
|  the_geom   | LINESTRING |                    ST_DIMENSION() =1                     | Geometry                                                     |
| **id_road** |  INTEGER   | ![](../resources/images/common/icons/pk.png) Primary Key | Unique Identifier                                            |
|  id_source  |  VARCHAR   |                        *not null*                        | Identifier of the feature from the input datasource          |
|    width    |   FLOAT    |                        *not null*                        | Width of the road *(expressed in meters)*                    |
|    type     |  VARCHAR   |                        *not null*                        | Type of road                                                 |
|   surface   |  VARCHAR   |                                                          | The surface value is used to provide additional information about the physical surface of roads/footpaths and some other features, particularly regarding material composition and/or structure. |
|  sidewalk   |  VARCHAR   |                                                          | Specify if the road has two, one or no sidewalk(s) - values=[two, one, no] |
|   zindex    |  VARCHAR   |                                                          | Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the object is above the ground surface. -4 to -1 value indicates that the object is underground. |
|  crossing   |  VARCHAR   |                                                          | Indicates whether the road is located on a `bridge`, in a `tunnel` or neither (value=`crossing`). |



### ![](../resources/images/common/icons/table_values.png) ROAD type

List of all possible values for the `type` attribute in the `ROAD` table.

| Term         | Definition                                                   | Source                                                       |
| :----------: | ------------------------------------------------------------ | :----------------------------------------------------------: |
| residential  | Highway generally used for local traffic within settlement. Usually highway accessing or around residential areas. | [1](https://wiki.openstreetmap.org/wiki/Tag:highway=residential) |
| track        | Highway for mostly agricultural use, forest tracks etc.; usually unpaved (unsealed) but may apply to paved tracks as well, that are suitable for two-track vehicles, such as tractors or jeeps. | [2](https://wiki.openstreetmap.org/wiki/Tag:highway=track)   |
| unclassified | Minor public highway typically at the lowest level of the interconnecting grid network. Have lower importance in the highway network than tertiary and are not residential streets or agricultural tracks. | [3](https://wiki.openstreetmap.org/wiki/FR:Tag:highway=unclassified) |
| footway      | For designated footpaths, i.e. mainly/exclusively for pedestrians. | [4](https://wiki.openstreetmap.org/wiki/Tag:highway=footway) |
| path         | A generic multi-use path open to non-motorized vehicles.     | [5](https://wiki.openstreetmap.org/wiki/Tag:highway=path)    |
| tertiary     | Highway linking small settlements, or the local centers of a large town or city. | [6](https://wiki.openstreetmap.org/wiki/Tag:highway=tertiary) |
| secondary    | Highway linking large towns. Usually have two lanes but not separated by a central barrier. | [7](https://wiki.openstreetmap.org/wiki/FR:Tag:highway=secondary) |
| primary      | Important highway linking large towns. Usually have two lanes but not separated by a central barrier. | [8](https://wiki.openstreetmap.org/wiki/FR:Tag:highway=primary) |
| cycleway     | Separated way for the use of cyclists.                       | [9](https://wiki.openstreetmap.org/wiki/Tag:highway=cycleway) |
| trunk        | Important high-performance highway that are not motorways. Deserving main towns. | [10](https://wiki.openstreetmap.org/wiki/FR:Tag:highway=trunk) |
| steps        | For flights of steps on footways and paths.                  | [11](https://wiki.openstreetmap.org/wiki/Tag:highway=steps)  |
| motorway     | Highest-performance highway within a territory that deserve main towns. Usually have a reglemented access. | [12](https://wiki.openstreetmap.org/wiki/FR:Tag:highway=motorway) |
| highway_link | Connecting ramp to/from a highway.                           | [13](https://wiki.openstreetmap.org/wiki/Key:highway)        |
| roundabout   | Generally a circular (self-intersecting) highway junction where the traffic on the roundabout has right of way. | [14](https://wiki.openstreetmap.org/wiki/Tag:junction=roundabout) |
| highway      | Any kind of street or way.                                   | [15](https://wiki.openstreetmap.org/wiki/Key:highway)        |
| ferry        | A ferry route used to transport things or people from one bank of a watercourse or inlet to the other, or as a permanent or seasonal local maritime link, and a link to a foreign country. | [16](http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf) |



### ![](../resources/images/common/icons/table_values.png) ROAD surface

List of all possible values for the `surface` attribute in the `ROAD` table.

|    Term     | Definition                                                   |                        Source                        |
| :---------: | ------------------------------------------------------------ | :--------------------------------------------------: |
|   unpaved   | Generic term to qualify the surface of a highway that is predominantly unsealed along its length; i.e., it has a loose covering ranging from compacted stone chippings to ground. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|    paved    | Surface with coating. Generic term for a highway with a stabilized and hard surface. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|   ground    | Surface of the ground itself with no specific fraction of rock. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|   gravel    | Surface composed of broken/crushed rock larger than sand grains and thinner than pebblestone. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|  concrete   | Cement based concrete surface.                               | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|    grass    | Grass covered ground.                                        | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|  compacted  | A mixture of larger (e.g., gravel) and smaller (e.g., sand) parts, compacted. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|    sand     | Small to very small fractions of rock as findable alongside body of water. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| cobblestone | Any cobbled surface.                                         | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|    wood     | Highway made of wooden surface.                              | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| pebblestone | Surface made of rounded rock as pebblestone findable alongside body of water. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|     mud     | Wet unpaved surface.                                         | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|    metal    | Metallic surface.                                            | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
|    water    | Used to qualify the surface of ferry route that uses water (waterbodies, watercourses, seas,...) as a traffic surface. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |



### ![](../resources/images/common/icons/table_values.png) ROAD crossing

Lists of all possible values for the `crossing` attribute in the `ROAD` table.

|  Term  | Definition                                                   |                       Source                        |
| :----: | ------------------------------------------------------------ | :-------------------------------------------------: |
| bridge | Artificial construction that spans features such as roads, railways,  waterways or valleys and carries a road, railway or other feature | [1](https://wiki.openstreetmap.org/wiki/Key:bridge) |
| tunnel | Underground passage for roads, railways or similar           | [2](https://wiki.openstreetmap.org/wiki/Key:tunnel) |
| *null* | Everything but a bridge or a tunnel                          |                                                     |

[back to top](#Input-data-model)



## 3- Railways

### ![](../resources/images/common/icons/table.png) RAIL table structure

This table represents any kind of railways network.

![](../resources/images/common/icons/arrow.png) Table name : `RAIL`



|    Name     |    Type    |                       Constraints                        | Definition                                                   |
| :---------: | :--------: | :------------------------------------------------------: | ------------------------------------------------------------ |
|  the_geom   | LINESTRING |                    ST_DIMENSION() =1                     | Geometry                                                     |
| **id_rail** |  INTEGER   | ![](../resources/images/common/icons/pk.png) Primary Key | Unique Identifier                                            |
|  id_source  |  VARCHAR   |                        *not null*                        | Identifier of the feature from the input datasource          |
|    type     |  VARCHAR   |                        *not null*                        | Type of rail                                                 |
|   zindex    |  INTEGER   |                                                          | Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the object is above the ground surface. -4 to -1 value indicates that the object is underground. |
|  crossing   |  VARCHAR   |                                                          | Indicates whether the rail section is located on a `bridge`, in a `tunnel` or neither (value=`crossing`). |


### ![](../resources/images/common/icons/table_values.png) RAIL type

List of all possible values for the `type` attribute in the `RAIL` table.

| Term          | Definition                                                   |                            Source                            |
| :-----------: | ------------------------------------------------------------ | :----------------------------------------------------------: |
| highspeed     | Railway track for highspeed rail.                            |    [1](https://wiki.openstreetmap.org/wiki/Key:highspeed)    |
| rail          | Railway track for full sized passenger or freight trains in the standard gauge for the country or state. |  [2](https://wiki.openstreetmap.org/wiki/Tag:railway=rail)   |
| service track | Railway track mainly used for sorting or temporary parking of freight trains. | [3](http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf)  |
| disused       | A section of railway which is no longer used but where the track and infrastructure remain in place. | [4](https://wiki.openstreetmap.org/wiki/Tag:railway=disused) |
| funicular     | Cable railway in which a cable attached to a pair of tram-like vehicles on rails moves them up and down a steep slope, the ascending and descending vehicles counterbalancing each other. | [5](https://wiki.openstreetmap.org/wiki/Tag:railway=funicular) |
| subway        | Rails used for city public transport that are always completely separated from other traffic, often underground | [6](https://wiki.openstreetmap.org/wiki/Tag:railway=subway)  |
| tram          | Railway track which is mainly or exclusively used for trams, or where tram tracks are laid within a normal road open to all traffic, often called street running. |  [7](https://wiki.openstreetmap.org/wiki/Tag:railway=tram)   |



### ![](../resources/images/common/icons/table_values.png) RAIL crossing

List of all possible values for the `crossing` attribute in the `RAIL` table.

|  Term  | Definition                                                   |                       Source                        |
| :----: | ------------------------------------------------------------ | :-------------------------------------------------: |
| bridge | Artificial construction that spans features such as roads, railways,  waterways or valleys and carries a road, railway or other feature | [1](https://wiki.openstreetmap.org/wiki/Key:bridge) |
| tunnel | Underground passage for roads, railways or similar           | [2](https://wiki.openstreetmap.org/wiki/Key:tunnel) |
| *null* | Everything but a bridge or a tunnel                          |                                                     |

[back to top](#Input-data-model)




## 4- Vegetation areas

### ![](../resources/images/common/icons/table.png) VEGET table structure

This table represents any kind of vegetation area.

![](../resources/images/common/icons/arrow.png) Table name: `VEGET`

|     Name     |  Type   |                       Constraints                        | Definition                                          |
| :----------: | :-----: | :------------------------------------------------------: | --------------------------------------------------- |
|   the_geom   | POLYGON |                    ST_DIMENSION() =2                     | Geometry                                            |
| **id_veget** | INTEGER | ![](../resources/images/common/icons/pk.png) Primary Key | Unique Identifier                                   |
|  id_source   | VARCHAR |                        *not null*                        | Identifier of the feature from the input datasource |
|     type     | VARCHAR |                        *not null*                        | Type of vegetation.                                 |
| height_class | VARCHAR |                        *not null*                        | Height class (`low` or `high`)                      |

### ![](../resources/images/common/icons/table_values.png) VEGET type

List of all possible values for `type` attribute in the `VEGET` table.


| Term | Definition | Source |
| :-----------: | ------------- | :-------------: |
| tree | A single tree |  [1](https://wiki.openstreetmap.org/wiki/Tag:natural=tree)   |
| wood          | Tree-covered area (a 'forest' or 'wood') not managed for economic purposes |  [2](https://wiki.openstreetmap.org/wiki/Tag:natural=wood)   |
| forest        | Managed woodland or woodland plantation. Wooded area maintained by human to obtain forest products | [3](https://wiki.openstreetmap.org/wiki/Tag:landuse=forest)  |
| scrub         | Uncultivated land covered with bushes or stunted trees       |  [4](https://wiki.openstreetmap.org/wiki/Tag:natural=scrub)  |
| grassland     | Natural areas where the vegetation is dominated by grasses (Poaceae) and other herbaceous (non-woody) plants | [5](https://wiki.openstreetmap.org/wiki/Tag:natural=grassland) |
| heath         | A dwarf-shrub habitat, characterized by open, low growing woody vegetation, often dominated by plants of the Ericaceae |  [6](https://wiki.openstreetmap.org/wiki/Tag:natural=heath)  |
| tree_row      | A line of trees                                              | [7](https://wiki.openstreetmap.org/wiki/Tag:natural=tree_row) |
| hedge         | A line of closely spaced shrubs and tree species, which form a barrier or mark the boundary of an area |  [8](https://wiki.openstreetmap.org/wiki/Tag:barrier=hedge)  |
| mangrove      | It is formed by forests of salt tolerant mangrove trees in the tidal zone of tropical coasts with water temperatures above 20° C | [9](https://wiki.openstreetmap.org/wiki/Tag:wetland=mangrove) |
| orchard       | Intentional planting of trees or shrubs maintained for food production | [10](https://wiki.openstreetmap.org/wiki/Tag:landuse=orchard) |
| vineyard      | A piece of land where grapes are grown                       | [11](https://wiki.openstreetmap.org/wiki/Tag:landuse=vineyard) |
| banana_plants | A banana plantation                                          |     [12](https://wiki.openstreetmap.org/wiki/Key:trees)      |
| sugar_cane    | A piece of land where sugar cane are grown                   | [13](https://wiki.openstreetmap.org/wiki/Tag:landuse=farmland) |



[back to top](#Input-data-model)

## 5- Hydrographic areas

### ![](../resources/images/common/icons/table.png) HYDRO table structure

This table represents any kind of hydrographic area (river, sea, ...).

![](../resources/images/common/icons/arrow.png) Table name: `HYDRO`



|     Name     |  Type   |                       Constraints                        | Definition                                          |
| :----------: | :-----: | :------------------------------------------------------: | --------------------------------------------------- |
|   the_geom   | POLYGON |                    ST_DIMENSION() =2                     | Geometry                                            |
| **id_hydro** | INTEGER | ![](../resources/images/common/icons/pk.png) Primary Key | Unique Identifier                                   |
|  id_source   | VARCHAR |                        *not null*                        | Identifier of the feature from the input datasource |

[back to top](#Input-data-model)

## 6- Impervious areas

### ![](../resources/images/common/icons/table.png) IMPERVIOUS table structure

This table represents any impervious surface, in addition to buildings and roads already present in the layers `BUILDING` and `ROAD`.

![](../resources/images/common/icons/arrow.png) Table name: `IMPERVIOUS`




|       Name        |  Type   |                       Constraints                        | Definition                                          |
| :---------------: | :-----: | :------------------------------------------------------: | --------------------------------------------------- |
|     the_geom      | POLYGON |                    ST_DIMENSION() =2                     | Geometry                                            |
| **id_impervious** | INTEGER | ![](../resources/images/common/icons/pk.png) Primary Key | Unique Identifier                                   |
|     id_source     | VARCHAR |                        *not null*                        | Identifier of the feature from the input datasource |



[back to top](#Input-data-model)

## 7- Zones

### ![](../resources/images/common/icons/table.png) ZONE table structure

This table represents the studied zone *(so, only one geometry expected)*.

![](../resources/images/common/icons/arrow.png) Table name: `ZONE`

|   Name   |  Type   |    Constraints    | Definition                  |
| :------: | :-----: | :---------------: | --------------------------- |
| the_geom | POLYGON | ST_DIMENSION() =2 | Geometry                    |
| id_zone  | VARCHAR |    *not null*     | Identifier of the zone area |
