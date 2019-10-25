# Input data model



This page presents the data dictionary used to describe the geographic data that feed the GeoClimate chain. The data are organized by topics. For each of them, we provide the input tables dictionary (with the name and type of the fields – prefixed with `input_`) and their associated metadata, in which description and parameters are presented:
- [Building](#Buildings):  `input_BUILDING`, BUILDING Abstract use and type, BUILDING Abstract level
- [Roads](#Roads): `input_ROAD`, ROAD Abstract type, ROAD Abstract surface, ROAD Abstract width
- [Rail ways](#Rail-ways): `input_RAIL`, RAIL Abstract type
- [Vegetation areas](#Vegetation-areas): `input_VEGET`, VEGET Abstract type, VEGET Abstract height
- [Hydrographic areas](#Hydrographic-areas): `input_HYDRO`
- [Zones](#Zones): `ZONE`, `ZONE_NEIGHBORS`

**Source code**

![](../images/icons/github.png) The input model source code is available [here](https://github.com/gpetit/geoclimate/blob/add_docs/prepare_data/src/main/groovy/org/orbisgis/common/AbstractTablesInitialization.groovy).


## Buildings



### BUILDING input table dictionary

- Table name : `input_building`
- EPSG Code : Any metric coordinate system

**Abstract dictionary table used to define building data:**

|    Name     |  Type   |    Constaints    | Definition                                                   |
| :---------: | :-----: | :--------------: | ------------------------------------------------------------ |
|  the_geom   | POLYGON | ST_DIMENSION()=2 | Geometry                                                     |
|  id_source  | VARCHAR |                  | Identifier of the feature from the input datasource          |
| height_wall |  FLOAT  |                  | The (corrected) height of the building in meters. Height of the building measured between the ground and the gutter (maximum altitude of the polyline describing the building). *(exprimed in meters)* |
| height_roof |  FLOAT  |                  | The maximum height of a building is the distance between the top edge of the building (including the roof, but excluding antennas, spires and other equipment mounted on the roof) and the lowest point at the bottom where the building meets the ground. *(exprimed in meters)* |
|   nb_lev    | INTEGER |                  | Number of levels (have to be greater than 0)                 |
|    type     | VARCHAR |                  | Value allowing to distinguish the type of building according to its architecture. These values are listed in the  [BUILDING_use_and_type](#BUILDING-use-and-type) section. |
|  main_use   | VARCHAR |                  | Main use of the building. The use of a building corresponds to a de facto element, relating to what it is used for. These values are listed in the  [BUILDING_use_and_type](#BUILDING-use-and-type) section. |
|   zindex    | INTEGER |                  | Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the objects above the ground surface. -4 to -1 value indicates that the object is underground. |

### BUILDING use and type

- Concerned table : `input_building`
- Concerned fields : `type` and `main_use`

List the values that can be used to describe the `type` and the `main_use` of a building feature in the `input_building` table. We consider that a same value can be used to qualify a `type` or a use (`main_use`).

- Term: value used to describe the type and the main use of a building

- Definition: provides a definition of the type or use

- Source: most of the values are based on concepts from existing thesauri. In this case, we indicate their source.  

| Term | Definition | Source |
| :--: | ---------- | :----: |
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



### BUILDING level

- Concerned table : `input_building`

- Concerned field : `nb_lev`

For each individual value concerning the `type` or `use` of a building (values listed in the [BUILDING_use and type](#BUILDING-use-and-type) metadata section), this list specifies the rules for calculating the number of levels of a building in order to feed the `nb_lvl` field in the `input_building` table.

- Term: value used to describe the `type` and the `main_use` of a building

- Nb_lev: Specifies whether or not the building type is taken into account when calculating the number of levels (`0` = not taken into account (in this case, the number of levels will be forced to 1) / `1`= taken into account (in this case, a formula will be used to deduct the number) / `2` = other situtation (rule).

| Term                            | Nb_lev |
| :-----------------------------: | :----: |
| building                        |   1    |
| house                           |   1    |
| detached                        |   1    |
| residential                     |   1    |
| apartments                      |   1    |
| bungalow                        |   0    |
| historic                        |   0    |
| monument                        |   0    |
| ruins                           |   0    |
| castle                          |   0    |
| agricultural                    |   0    |
| farm                            |   0    |
| farm_auxiliary                  |   0    |
| barn                            |   0    |
| greenhouse                      |   0    |
| silo                            |   0    |
| commercial                      |   2    |
| industrial                      |   0    |
| sport                           |   0    |
| sports_centre                   |   0    |
| grandstand                      |   0    |
| transportation                  |   0    |
| train_station                   |   0    |
| toll_booth                      |   0    |
| terminal                        |   0    |
| healthcare                      |   1    |
| education                       |   1    |
| entertainment, arts and culture |   0    |
| sustenance                      |   1    |
| military                        |   0    |
| religious                       |   0    |
| chapel                          |   0    |
| church                          |   0    |
| government                      |   1    |
| townhall                        |   1    |
| office                          |   1    |

[back to top](#Input-data-model)

## Roads

### ROAD input table dictionary

- Table name: `input_road`
- EPSG Code: Any metric coordinate system



**Abstract dictionary table used to define the road data:**

|   Name    |    Type    |    Constraints    | Definition                                                   |
| :-------: | :--------: | :---------------: | ------------------------------------------------------------ |
| the_geom  | LINESTRING | ST_DIMENSION() =1 | Geometry                                                     |
| id_source |  VARCHAR   |                   | Identifier of the feature from the input datasource          |
|   width   |   FLOAT    |                   | Width of the road *(exprimed in meters)*                     |
|   type    |  VARCHAR   |                   | Type of road                                                 |
|  surface  |  VARCHAR   |                   | The surface value is used to provide additional information about the physical surface of roads/footpaths and some other features, particularly regarding material composition and/or structure. |
| sidewalk  |  VARCHAR   |                   | Specify if the road has two, one or no sidewalk(s) - values=[two, one, no] |
|  zindex   |  VARCHAR   |                   | Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the object is above the ground surface. -4 to -1 value indicates that the object is underground. |
| crossing  |  VARCHAR   |                   | Indicates whether the road is located on a `bridge`, in a `tunnel` or neither (value=`crossing`). |



### ROAD type

- Concerned table : `input_road`
- Concerned fields: `type`

Below are listed the possible values for the `type` attributes in the `input_road` table.

- Term: value used to qualify the type of the road
- Definition: provides a definition of the type attribute
- Source: most of the values are based on concepts from existing thesauri. In this case, we indicate their sources.

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



### ROAD surface

- Concerned table : `input_road`
- Concerned field : `surface`

Lists of all possible values for the `surface` attributes in the `input_road` table.

- Term: value used to qualify the material on the road surface
- Definition: provides a definition of the surface attribute
- Source: most of the values are based on concepts from existing thesauri. In this case, we indicate their
  sources.

| Term        | Definition                                                   |                        Source                        |
| :---------: | ------------------------------------------------------------ | :--------------------------------------------------: |
| unpaved     | "Generic term to qualify the surface of a highway that is predominantly unsealed along its length; i.e., it has a loose covering ranging from compacted stone chippings to ground." | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| paved       | Surface with coating. Generic term for a highway with a stabilized and hard surface. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| ground      | Surface of the ground itself with no specific fraction of rock. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| gravel      | Surface composed of broken/crushed rock larger than sand grains and thinner than pebblestone. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| concrete    | Cement based concrete surface.                               | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| grass       | Grass covered ground.                                        | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| compacted   | "A mixture of larger (e.g., gravel) and smaller (e.g., sand) parts, compacted." | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| sand        | Small to very small fractions of rock as findable alongside body of water. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| cobblestone | Any cobbled surface.                                         | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| wood        | Highway made of wooden surface.                              | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| pebblestone | Surface made of rounded rock as pebblestone findable alongside body of water. | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| mud         | Wet unpaved surface.                                         | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| metal       | Metallic surface.                                            | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |
| water       | "Used to qualify the surface of ferry route that uses water (waterbodies, watercourses, seas,...) as a traffic surface." | [1](https://wiki.openstreetmap.org/wiki/Key:surface) |



### ROAD width

- Concerned table : `input_road`
- Concerned field : `width`

For each individual value concerning the `type` of a roads (values listed in the [ROAD type](#ROAD-type) metadata section), this list specifies the minimum road width in order to feed the `width` field of the
`input_road` table when no information are provided.

- Term: value used to qualify the type of the road
- Min_width: minimum road width *(in meter)* to apply

| Term         | Min_width |
| :----------: | :-------: |
| residential  |     8     |
| track        |     2     |
| unclassified |     3     |
| footway      |     1     |
| path         |     1     |
| tertiary     |     8     |
| secondary    |    10     |
| primary      |    10     |
| cycleway     |     1     |
| trunk        |    16     |
| steps        |     1     |
| motorway     |    24     |
| highway_link |     8     |
| roundabout   |     4     |
| highway      |     8     |
| ferry        |     0     |



### ROAD crossing

- Concerned table : `input_road`
- Concerned field : `crossing`

Lists of all possible values for `crossing` attribute in the `input_road table.

- Term: value used to qualify the crossing type of the road
- Definition: provides a definition of the crossing attribute
- Source: Most of the values are based on concepts from existing thesauri. In this case, we indicate their sources.

|   Term   | Definition                                                   |                       Source                        |
| :------: | ------------------------------------------------------------ | :-------------------------------------------------: |
|  bridge  | Artificial construction that spans features such as roads, railways,  waterways or valleys and carries a road, railway or other feature | [1](https://wiki.openstreetmap.org/wiki/Key:bridge) |
|  tunnel  | Underground passage for roads, railways or similar           | [2](https://wiki.openstreetmap.org/wiki/Key:tunnel) |
| crossing | Everything but a bridge or a tunnel                          |                                                     |

[back to top](#Input-data-model)



## Rail ways

### RAIL input table dictionary

* Table name : `input_rail`
* EPSG Code : Any metric coordinate system

**Abstract dictionary table used to define the rail data:**

|   Name    |    Type    |    Constraints    | Definition                                                   |
| :-------: | :--------: | :---------------: | ------------------------------------------------------------ |
| the_geom  | LINESTRING | ST_DIMENSION() =1 | Geometry                                                     |
| id_source |  VARCHAR   |                   | Identifier of the feature from the input datasource          |
|   type    |  VARCHAR   |                   | Type of rail                                                 |
|  zindex   |  INTEGER   |                   | Defines the position with respect to the ground. 0 indicates that the object is on the ground. 1 to 4 indicates that the object is above the ground surface. -4 to -1 value indicates that the object is underground. |
| crossing  |  VARCHAR   |                   | Indicates whether the rail section is located on a `bridge`, in a `tunnel` or neither (value=`crossing`). |


### RAIL type

- Concerned table : `input_rail`
- Concerned field : `type`

Lists of all possible values for `type` attribute in the `input_rail` table.

- Term: value used to qualify the type of the rail
- Definition: provides a definition of the type attribute
- Source: Most of the values are based on concepts from existing thesauri. In this case, we indicate their
  sources.

| Term          | Definition                                                   |                            Source                            |
| :-----------: | ------------------------------------------------------------ | :----------------------------------------------------------: |
| highspeed     | Railway track for highspeed rail.                            |    [1](https://wiki.openstreetmap.org/wiki/Key:highspeed)    |
| rail          | Railway track for full sized passenger or freight trains in the standard gauge for the country or state. |  [2](https://wiki.openstreetmap.org/wiki/Tag:railway=rail)   |
| service track | Railway track mainly used for sorting or temporary parking of freight trains. | [3](http://professionnels.ign.fr/doc/DC_BDTOPO_3-0Beta.pdf)  |
| disused       | A section of railway which is no longer used but where the track and infrastructure remain in place. | [4](https://wiki.openstreetmap.org/wiki/Tag:railway=disused) |
| funicular     | Cable railway in which a cable attached to a pair of tram-like vehicles on rails moves them up and down a steep slope, the ascending and descending vehicles counterbalancing each other. | [5](https://wiki.openstreetmap.org/wiki/Tag:railway=funicular) |
| subway        | Rails used for city public transport that are always completely separated from other traffic, often underground | [6](https://wiki.openstreetmap.org/wiki/Tag:railway=subway)  |
| tram          | Railway track which is mainly or exclusively used for trams, or where tram tracks are laid within a normal road open to all traffic, often called street running. |  [7](https://wiki.openstreetmap.org/wiki/Tag:railway=tram)   |



### RAIL crossing

- Concerned table : `input_rail`
- Concerned field : `crossing`

Lists of all possible values for `crossing` attribute in the `input_rail` table.

- Term: value used to qualify the crossingtype of the rail
- Definition: provides a definition of the crossing attribute
- Source: Most of the values are based on concepts from existing thesauri. In this case, we indicate their sources.

|   Term   | Definition                                                   |                       Source                        |
| :------: | ------------------------------------------------------------ | :-------------------------------------------------: |
|  bridge  | Artificial construction that spans features such as roads, railways,  waterways or valleys and carries a road, railway or other feature | [1](https://wiki.openstreetmap.org/wiki/Key:bridge) |
|  tunnel  | Underground passage for roads, railways or similar           | [2](https://wiki.openstreetmap.org/wiki/Key:tunnel) |
| crossing | Everything but a bridge or a tunnel                          |                                                     |

[back to top](#Input-data-model)




## Vegetation areas

### VEGET input table dictionary

- Table name: `input_veget`
- EPSG Code: Any metric coordinate system

**Abstract table used to store the vegetation data:**

| Name         | Type    | Constraints       | Definition                                          |
| :----------: | :-----: | :---------------: | --------------------------------------------------- |
| the_geom     | POLYGON | ST_DIMENSION() =2 | Geometry                                            |
| id_source    | VARCHAR |                   | Identifier of the feature from the input datasource |
| type         | VARCHAR |                   | Type of vegetation.                                 |
| height_class | VARCHAR |                   | Height class (`low` or `high`)                      |

### VEGET type

- Concerned table : `input_veget`
- Concerned field : `type`

Lists of all possible values for `type` attribute in the `input_veget` table.

- Term: value used to qualify the `type` of the vegetation
- Definition: provides a definition of the `type` attribute
- Source: most of the values are based on concepts from existing thesauri. In this case, we indicate their sources.


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



### VEGET height

- Concerned table : `input_veget`
- Concerned field : `height_class`

Lists of all possible values for the `height_class` attribute in the `input_veget` table.

- Term: value used to qualify the `height_class` of the vegetation
- Height_class: vegetation height class (`low` or `high`)

  

| Term | Height_class |
| :-----------: | :-----------: |
| tree | high |
| wood          | high |
| forest        | high |
| scrub         | low     |
| grassland     | low |
| heath         | low |
| tree_row      | high                                          |
| hedge         | high |
| mangrove      | high |
| orchard       | high |
| vineyard      | low                    |
| banana_plants | high                                      |
| sugar_cane    | low                |

[back to top](#Input-data-model)

## Hydrographic areas

### HYDRO input table dictionary
- Table name: `input_hydro`
- EPSG Code: Any metric coordinate system

**Abstract table used to store the hydrographic data:**

| Name      | Type    | Constraints       | Definition                                          |
| :-------: | :-----: | :---------------: | --------------------------------------------------- |
| the_geom  | POLYGON | ST_DIMENSION() =2 | Geometry                                            |
| id_source | VARCHAR |                   | Identifier of the feature from the input datasource |

[back to top](#Input-data-model)

## Zones

### ZONE input table dictionary
- Table name: `zone`
- EPSG Code: Any metric coordinate system

**Abstract table used to store the studied zone area:**

| Name     | Type    | Constraints       | Definition                  |
| :------: | :-----: | :---------------: | --------------------------- |
| the_geom | POLYGON | ST_DIMENSION() =2 | Geometry                    |
| id_zone  | VARCHAR |                   | Identifier of the zone area |

### ZONE NEIGHBORS input table dictionary
- Table name: `zone_neighbors`
- EPSG Code: Any metric coordinate system

**Abstract table used to store every neighboring zones of the studied zone:**

| Name     | Type    | Constraints       | Definition                  |
| :------: | :-----: | :---------------: | --------------------------- |
| the_geom | POLYGON | ST_DIMENSION() =2 | Geometry                    |
| id_zone  | VARCHAR |                   | Identifier of the zone area |

[back to top](#Input-data-model)