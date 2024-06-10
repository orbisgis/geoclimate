/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.bdtopo

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.orbisgis.data.H2GIS
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators


@BaseScript BDTopo bdTopo

/**
 * This process is used to format the BDTopo buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param building The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @return The name of the final buildings table
 */
String formatBuildingLayer(JdbcDataSource datasource, String building, String zone = "",
                           String urban_areas = "", float h_lev_min = 3) throws Exception {
    if (!h_lev_min) {
        h_lev_min = 3
    }
    def outputTableName = postfix "BUILDING"
    debug 'Formating building layer'

    //Create the final building table

    datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM geometry, ID_BUILD integer, ID_SOURCE varchar(24), 
            HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX integer, ROOF_SHAPE VARCHAR);""".toString())

    if (building) {
        if (datasource.hasTable(building)) {
            def queryMapper = "SELECT "
            if (zone) {
                datasource.createSpatialIndex(building, "the_geom")
                datasource.createSpatialIndex(zone, "the_geom")
                queryMapper += " a.*  FROM $building as a,  $zone as b WHERE a.the_geom && b.the_geom and st_intersects(a.the_geom " +
                        ",b.the_geom) "
            } else {
                queryMapper += "* FROM $building as a  "
            }

            def types_uses_dictionnary =
                    ["Bâtiment agricole"                 : ["farm_auxiliary": "agricultural"],
                     "Bâtiment commercial"               : ["commercial": "commercial"],
                     "Bâtiment industriel"               : ["industrial": "industrial"],
                     "Serre"                             : ["greenhouse": "agricultural"],
                     "Silo"                              : ["silo": "agricultural"],
                     "Aérogare"                          : ["aeroway": "transport"],
                     "Arc de triomphe"                   : ["monument": "heritage"],
                     "Arène ou théâtre antique"          : ["monument": "monument"],
                     "Bâtiment religieux divers"         : ["religious": "religious"],
                     "Bâtiment sportif"                  : ["sports_centre": "sport"],
                     "Chapelle"                          : ["chapel": "religious"],
                     "Château"                           : ["castle": "heritage"],
                     "Eglise"                            : ["church": "religious"],
                     "Fort, blockhaus, casemate"         : ["military": "military"],
                     "Gare"                              : ["train_station": "transport"],
                     "Mairie"                            : ["townhall": "government"],
                     "Monument"                          : ["monument": "monument"],
                     "Péage"                             : ["toll_booth": "transport"],
                     "Préfecture"                        : ["government": "government"],
                     "Sous-préfecture"                   : ["government": "government"],
                     "Tour, donjon, moulin"              : ["historic": "heritage"],
                     "Moulin à vent"                     : ["historic": "heritage"],
                     "Tour, donjon"                      : ["historic": "heritage"],
                     "Tribune"                           : ["grandstand": "entertainment_arts_culture"],
                     "Résidentiel"                       : ["residential": "residential"],
                     "Agricole"                          : ["agricultural": "agricultural"],
                     "Commercial et services"            : ["commercial": "commercial"],
                     "Industriel"                        : ["industrial": "industrial"],
                     "Religieux"                         : ["religious": "religious"],
                     "Sportif"                           : ["sport": "sport"],
                     "Annexe"                            : ["annex": "building"],
                     "Industriel, agricole ou commercial": ["commercial": "commercial"],
                     "Bâtiment"                          : ["building": "building"],
                     "Industrie lourde"                  : ["industrial": "industrial"]
                    ]

            def building_type_level = [
                    "building"                  : 1,
                    "house"                     : 1,
                    "detached"                  : 1,
                    "residential"               : 1,
                    "apartments"                : 1,
                    "bungalow"                  : 0,
                    "historic"                  : 0,
                    "monument"                  : 0,
                    "ruins"                     : 0,
                    "castle"                    : 1,
                    "agricultural"              : 0,
                    "farm"                      : 0,
                    "farm_auxiliary"            : 0,
                    "barn"                      : 0,
                    "greenhouse"                : 0,
                    "silo"                      : 0,
                    "commercial"                : 2,
                    "industrial"                : 0,
                    "sport"                     : 0,
                    "sports_centre"             : 0,
                    "grandstand"                : 0,
                    "transport"                 : 0,
                    "train_station"             : 0,
                    "toll_booth"                : 0,
                    "toll"                      : 0,
                    "terminal"                  : 0,
                    "airport_terminal"          : 0,
                    "healthcare"                : 1,
                    "education"                 : 1,
                    "entertainment_arts_culture": 0,
                    "sustenance"                : 1,
                    "military"                  : 0,
                    "religious"                 : 0,
                    "chapel"                    : 0,
                    "church"                    : 0,
                    "government"                : 1,
                    "townhall"                  : 1,
                    "office"                    : 1,
                    "emergency"                 : 0,
                    "hotel"                     : 2,
                    "hospital"                  : 2,
                    "parking"                   : 1,
                    "slight_construction"       : 0,
                    "water_tower"               : 0,
                    "fortress"                  : 0,
                    "abbey"                     : 0,
                    "cathedral"                 : 0,
                    "mosque"                    : 0,
                    "musalla"                   : 0,
                    "temple"                    : 0,
                    "synagogue"                 : 0,
                    "shrine"                    : 0,
                    "place_of_worship"          : 0,
                    "wayside_shrine"            : 0,
                    "swimming_pool"             : 0,
                    "fitness_centre"            : 1,
                    "horse_riding"              : 0,
                    "ice_rink"                  : 0,
                    "pitch"                     : 0,
                    "stadium"                   : 0,
                    "track"                     : 0,
                    "sports_hall"               : 0,
                    "ammunition"                : 0,
                    "bunker"                    : 0,
                    "barracks"                  : 1,
                    "casemate"                  : 0,
                    "station"                   : 0,
                    "government_office"         : 1,
                    "stable"                    : 0,
                    "sty"                       : 0,
                    "cowshed"                   : 0,
                    "digester"                  : 0,
                    "farmhouse"                 : 0,
                    "bank"                      : 1,
                    "bureau_de_change"          : 0,
                    "boat_rental"               : 0,
                    "car_rental"                : 0,
                    "internet_cafe"             : 0,
                    "kiosk"                     : 0,
                    "money_transfer"            : 0,
                    "market"                    : 0,
                    "marketplace"               : 0,
                    "pharmacy"                  : 0,
                    "post_office"               : 1,
                    "retail"                    : 0,
                    "shop"                      : 0,
                    "store"                     : 0,
                    "supermarket"               : 0,
                    "warehouse"                 : 0,
                    "factory"                   : 0,
                    "gas"                       : 0,
                    "heating_station"           : 0,
                    "oil_mill"                  : 0,
                    "oil"                       : 0,
                    "wellsite"                  : 0,
                    "well_cluster"              : 0,
                    "grain_silo"                : 0,
                    "villa"                     : 1,
                    "dormitory"                 : 1,
                    "condominium"               : 1,
                    "sheltered_housing"         : 0,
                    "workers_dormitory"         : 0,
                    "terrace"                   : 1,
                    "transportation"            : 0,
                    "hangar"                    : 0,
                    "tower"                     : 1,
                    "control_tower"             : 0,
                    "aeroway"                   : 1,
                    "roundhouse"                : 0,
                    "social_facility"           : 1,
                    "college"                   : 1,
                    "kindergarten"              : 0,
                    "school"                    : 0,
                    "university"                : 1,
                    "cinema"                    : 1,
                    "arts_centre"               : 0,
                    "brothel"                   : 1,
                    "casino"                    : 0,
                    "community_centre"          : 0,
                    "conference_centre"         : 1,
                    "events_venue"              : 1,
                    "exhibition_centre"         : 0,
                    "gambling"                  : 0,
                    "music_venue"               : 0,
                    "nightclub"                 : 0,
                    "planetarium"               : 0,
                    "social_centre"             : 0,
                    "studio"                    : 0,
                    "theatre"                   : 0,
                    "library"                   : 1,
                    "museum"                    : 0,
                    "aquarium"                  : 0,
                    "gallery"                   : 0,
                    "information"               : 0,
                    "restaurant"                : 0,
                    "bar"                       : 0,
                    "cafe"                      : 0,
                    "fast_food"                 : 0,
                    "ice_cream"                 : 0,
                    "pub"                       : 0,
                    "attraction"                : 0
            ]


            //Formating building table
            def id_build = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper.toString()) { row ->
                    def values = row.toRowResult()
                    def id_source = values.ID_SOURCE
                    def type_use = getTypeAndUse(values.TYPE, values.MAIN_USE, types_uses_dictionnary)
                    def feature_type = type_use[0]
                    def feature_main_use = type_use[1]
                    def height_wall = values.HEIGHT_WALL
                    def height_roof = values.HEIGHT_ROOF
                    def nb_lev = 0
                    //Update height_wall
                    if (!height_wall) {
                        height_wall = 0
                    }
                    //Update height_roof
                    if (!height_roof && height_wall) {
                        height_roof = height_wall
                    }
                    //Update NB_LEV, HEIGHT_WALL and HEIGHT_ROOF
                    def formatedHeight = Geoindicators.WorkflowGeoIndicators.formatHeightsAndNbLevels(height_wall, height_roof, nb_lev, h_lev_min,
                            feature_type, building_type_level)

                    def zIndex = 0
                    if (formatedHeight.nbLevels > 0) {
                        Geometry geom = values.the_geom
                        if (!geom.isEmpty()) {
                            def srid = geom.getSRID()
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                    stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$srid), 
                                                    $id_build, 
                                                    '$id_source',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    '${feature_type}',
                                                    '${feature_main_use}',
                                                    ${zIndex}, null)
                                            """.toString()

                                    id_build++
                                }
                            }
                        }
                    }
                }
            }
            //Let's use the urban areas table to improve building qualification
            if (urban_areas) {
                datasource.createSpatialIndex(outputTableName, "the_geom")
                datasource.createIndex(outputTableName, "id_build")
                datasource.createIndex(outputTableName, "type")
                datasource.createSpatialIndex(urban_areas, "the_geom")
                def buildinType = postfix("BUILDING_TYPE")

                datasource.execute """create table $buildinType as SELECT 
                        max(b.type) as type, 
                        max(b.type) as main_use, a.id_build FROM $outputTableName a, $urban_areas b 
                        WHERE ST_POINTONSURFACE(a.the_geom) && b.the_geom and st_intersects(ST_POINTONSURFACE(a.the_geom), b.the_geom) 
                        AND  a.TYPE ='building' AND b.TYPE != 'unknown'
                         group by a.id_build""".toString()

                datasource.createIndex(buildinType, "id_build")

                def newBuildingWithType = postfix("NEW_BUILDING_TYPE")

                datasource.execute("""DROP TABLE IF EXISTS $newBuildingWithType;
                                           CREATE TABLE $newBuildingWithType as
                                            SELECT  a.THE_GEOM, a.ID_BUILD,a.ID_SOURCE,
                                            a.HEIGHT_WALL,
                                            a.HEIGHT_ROOF,
                                               a.NB_LEV, 
                                               COALESCE(b.TYPE, a.TYPE) AS TYPE ,
                                               COALESCE(b.MAIN_USE, a.MAIN_USE) AS MAIN_USE
                                               , a.ZINDEX, a.ROOF_SHAPE from $outputTableName
                                        a LEFT JOIN $buildinType b on a.id_build=b.id_build""")

                datasource.execute("""DROP TABLE IF EXISTS $buildinType, $outputTableName;
                        ALTER TABLE $newBuildingWithType RENAME TO $outputTableName;
                        DROP TABLE IF EXISTS $newBuildingWithType;""")
            }
        }
    }
    info "Building formatted"
    return outputTableName
}

/**
 * Return the type and the use by looking in a dictionary of values
 * @param main_type the original type
 * @param main_use the original main use
 * @param types_and_uses the dictionnary of values
 * @return the type and the use. Default is [building, building]
 */
static String[] getTypeAndUse(def main_type, def main_use, def types_and_uses) {
    def feature_type = "building"
    def feature_main_use = "building"
    if (main_type && main_use) {
        def tmp_type_use_from_main_type = types_and_uses.get(main_type.trim())
        def tmp_type_use_from_main_use = types_and_uses.get(main_use.trim())
        feature_type = tmp_type_use_from_main_type.grep()[0].key
        feature_main_use = tmp_type_use_from_main_use.grep()[0].key
    } else if (!main_type && main_use) {
        def tmp_type_use_from_main_use = types_and_uses.get(main_use.trim())
        feature_type = tmp_type_use_from_main_use.grep()[0].key
        feature_main_use = feature_type
    } else if (main_type && !main_use) {
        def tmp_type_use_from_main_type = types_and_uses.get(main_type.trim())
        def type_main = tmp_type_use_from_main_type.grep()[0]
        feature_type = type_main.key
        feature_main_use = type_main.value
    }
    return [feature_type, feature_main_use]
}


/**
 * This process is used to transform the BDTopo roads table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw roads table
 * @param road The name of the raw roads table in the DB
 * @return The name of the final roads table
 */
String formatRoadLayer(JdbcDataSource datasource, String road, String zone = "") throws Exception {
    debug('Formating road layer')
    def outputTableName = postfix "ROAD"
    datasource.execute("""
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_road serial, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, CROSSING VARCHAR(30),
                SURFACE VARCHAR, SIDEWALK VARCHAR, MAXSPEED INTEGER, DIRECTION INTEGER, ZINDEX INTEGER);
        """)
    if (road) {
        def road_types_width =
                ["highway"     : 8,
                 "motorway"    : 24,
                 "trunk"       : 16,
                 "primary"     : 10,
                 "secondary"   : 10,
                 "tertiary"    : 8,
                 "residential" : 8,
                 "unclassified": 3,
                 "track"       : 2,
                 "path"        : 2,
                 "footway"     : 2,
                 "cycleway"    : 2,
                 "steps"       : 2,
                 "highway_link": 8,
                 "roundabout"  : 4,
                 "ferry"       : 0,
                 "pedestrian"  : 3,
                 "service"     : 3]
        if (datasource.hasTable(road)) {
            def queryMapper = "SELECT a.ID_SOURCE, a.WIDTH, a.TYPE, a.ZINDEX, a.CROSSING, a.DIRECTION, a.RANK, a.ADMIN_SCALE"
            if (zone) {
                datasource.createSpatialIndex(road, "the_geom")
                queryMapper += ", ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom), 2) as the_geom " +
                        "FROM " +
                        "$road AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom "
            } else {
                queryMapper += ", a.the_geom FROM $road  as a"
            }
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    def qualified_road_maxspeed = 50
                    def qualified_road_type = 'unclassified'
                    def qualified_road_surface = 'asphalt'
                    def qualified_sidewalk = 'no'
                    def qualified_road_width = 3
                    def qualified_crossing = null
                    def qualified_road_zindex = row.ZINDEX
                    def road_rank = row.RANK
                    //def road_admin_scale = row.ADMIN_SCALE
                    switch (row.TYPE) {
                        case 'Autoroute':
                            qualified_road_maxspeed = 130
                            qualified_road_type = 'motorway'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Type autoroutier':
                            qualified_road_maxspeed = 130
                            qualified_road_type = 'motorway'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Quasi-autoroute':
                            qualified_road_maxspeed = 130
                            qualified_road_type = 'trunk'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Bretelle':
                            qualified_road_maxspeed = 50
                            qualified_road_type = 'highway_link'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Route à 2 chaussées':
                            qualified_road_maxspeed = 80
                            qualified_road_type = 'primary'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            if (road_rank == 6) {
                                qualified_road_maxspeed = 0
                            } else if (road_rank == 5) {
                                qualified_road_maxspeed = 30
                            }
                            break
                        case 'Route à 1 chaussée':
                            if (road_rank == 6) {
                                qualified_road_maxspeed = 0
                            } else if (road_rank == 5) {
                                qualified_road_maxspeed = 30
                            } else if (road_rank <= 4) {
                                qualified_road_maxspeed = 80
                            }
                            qualified_road_type = 'unclassified'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Route empierrée':
                            qualified_road_surface = 'paved'
                            qualified_road_maxspeed = 10
                            qualified_road_type = 'track'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Chemin':
                            qualified_road_surface = 'ground'
                            qualified_road_maxspeed = 0
                            qualified_road_type = 'track'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Sentier':
                            qualified_road_surface = 'ground'
                            qualified_road_maxspeed = 0
                            qualified_road_type = 'path'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Pont':
                            qualified_road_maxspeed = 0
                            qualified_road_type = 'bridge'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'NC':
                            qualified_road_maxspeed = 0
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Rond-point':
                            qualified_road_maxspeed = 30
                            qualified_road_type = 'roundabout'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        case 'Piste cyclable':
                            qualified_road_maxspeed = 0
                            qualified_road_type = 'cycleway'
                            qualified_road_width = road_types_width.get(qualified_road_type)
                            break
                        default:
                            break
                    }

                    //Use the original width
                    def width = row.WIDTH
                    if (width || width > 0) {
                        qualified_road_width = width
                    }
                    def road_crossing = row.CROSSING
                    if (road_crossing == 'Gué ou radier') {
                        qualified_crossing = 'crossing'
                        qualified_road_zindex = 0
                    } else if (road_crossing == 'Pont') {
                        qualified_crossing = 'bridge'
                        if (!qualified_road_zindex) {
                            qualified_road_zindex = 1
                        }
                    } else if (road_crossing == 'NC') {
                        qualified_crossing = null
                    }
                    def road_sens = row.DIRECTION

                    if (road_sens == "Double") {
                        road_sens = 3
                    } else if (road_sens == "Direct") {
                        road_sens = 1
                    } else if (road_sens == "Inverse") {
                        road_sens = 2
                    } else {
                        road_sens = -1
                    }

                    def ID_SOURCE = row.ID_SOURCE
                    if (qualified_road_zindex >= 0 && qualified_road_type != 'path') {
                        Geometry geom = row.the_geom
                        if (!geom.isEmpty()) {
                            def epsg = geom.getSRID()
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (!subGeom.isEmpty()) {
                                    stmt.addBatch """
                                        INSERT INTO $outputTableName VALUES(ST_GEOMFROMTEXT(
                                        '${subGeom}',$epsg), 
                                        ${rowcount++}, 
                                        '${ID_SOURCE}', 
                                        ${qualified_road_width},
                                        '${qualified_road_type}',
                                        ${qualified_crossing ? "'${qualified_crossing}'" : qualified_crossing}, 
                                        '${qualified_road_surface}',
                                        '${qualified_sidewalk}',
                                        ${qualified_road_maxspeed},
                                        ${road_sens},
                                        ${qualified_road_zindex})
                                        """.toString()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    info "Road formatted"
    return outputTableName
}

/**
 * This process is used to transform the raw hydro table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw hydro table
 * @param water The name of the raw hydro table in the DB
 * @return The name of the final hydro table
 */
String formatHydroLayer(JdbcDataSource datasource, String water, String zone = "") throws Exception {
    debug('Hydro transformation starts')
    def outputTableName = postfix("HYDRO")
    datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, ID_WATER serial, ID_SOURCE VARCHAR, TYPE VARCHAR, ZINDEX INTEGER);""".toString()

    if (water) {
        if (datasource.hasTable(water)) {
            String query
            if (zone) {
                datasource.createSpatialIndex(water, "the_geom")
                query = "select st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        ", a.ZINDEX, a.ID_SOURCE, a.TYPE " +
                        " FROM " +
                        "$water AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom "
            } else {
                query = "select * FROM $water "

            }
            def water_types =
                    ["Aqueduc"                   : "aqueduct",
                     "Canal"                     : "canal",
                     "Delta"                     : "bay",
                     "Ecoulement canalisé"       : "canal",
                     "Ecoulement endoréique"     : "water",
                     "Ecoulement hyporhéique"    : "water",
                     "Ecoulement karstique"      : "water",
                     "Ecoulement naturel"        : "water",
                     "Ecoulement phréatique"     : "water",
                     "Estuaire"                  : "bay",
                     "Inconnue"                  : "water",
                     "Lac"                       : "lake",
                     "Lagune"                    : "lagoon", "Mangrove": "mangrove",
                     "Mare"                      : "pond",
                     "Plan d'eau de gravière"    : "pond",
                     "Plan d'eau de mine"        : "basin", "Ravine": "water",
                     "Réservoir-bassin"          : "basin",
                     "Réservoir-bassin d'orage"  : "basin",
                     "Réservoir-bassin piscicole": "basin",
                     "Retenue"                   : "basin",
                     "Retenuebarrage"            : "basin",
                     "Retenue-bassin portuaire"  : "basin",
                     "Retenue-digue"             : "basin",
                     "Surface d'eau"             : "water",
                     "Bassin"                    : "basin"
                    ]

            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(query) { row ->
                    def water_type = water_types.get(row.TYPE)
                    def water_zindex = 0
                    if (water_type) {
                        Geometry geom = row.the_geom
                        if (!geom.isEmpty()) {
                            def epsg = geom.getSRID()
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.ID_SOURCE}', '${water_type}', ${water_zindex})".toString()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    info "Water formatted"
    return outputTableName
}

/**
 * This process is used to transform the raw rails table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw rails table
 * @param rail The name of the raw rails table in the DB
 * @return The name of the final rails table
 */
String formatRailsLayer(JdbcDataSource datasource, String rail, String zone = "") throws Exception {
    debug('Rails transformation starts')
    def outputTableName = postfix("RAILS")
    datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_rail serial,
                ID_SOURCE VARCHAR, TYPE VARCHAR,CROSSING VARCHAR(30), ZINDEX INTEGER, WIDTH FLOAT, USAGE VARCHAR(30));""".toString()

    if (rail) {
        if (datasource.hasTable(rail)) {
            def queryMapper = "SELECT a.ID_SOURCE, a.TYPE, a.ZINDEX, a.CROSSING, a.WIDTH"
            if (zone) {
                datasource.createSpatialIndex(rail, "the_geom")
                queryMapper += ", st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        "FROM " +
                        "$rail AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom "
            } else {
                queryMapper += ", a.the_geom FROM $rail  as a"

            }

            def rail_types = ['LGV'                       : 'highspeed',
                              'Principale'                : 'rail',
                              'Voie ferrée principale'    : 'rail',
                              'Voie de service'           : 'service_track',
                              'Voie non exploitée'        : 'disused',
                              'Transport urbain'          : 'tram',
                              'Funiculaire ou crémaillère': 'funicular',
                              'Metro'                     : 'subway',
                              'Métro'                     : 'subway',
                              'Tramway'                   : 'tram',
                              'Pont'                      : 'bridge',
                              'Tunnel'                    : 'tunnel',
                              'Sans objet'                : null,
                              'NC'                        : null]
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper.toString()) { row ->
                    def rail_type = row.TYPE
                    if (rail_type) {
                        rail_type = rail_types.get(rail_type)
                    } else {
                        rail_type = "unclassified"
                    }
                    def rail_usage = null
                    if (rail_type in ["highspeed", "rail", "tram", "bridge"]) {
                        rail_usage = "main"
                    }
                    def rail_zindex = row.ZINDEX
                    if (!rail_zindex) {
                        rail_zindex = 0
                    }

                    //1.435 default value for standard gauge
                    //1 constant for balasting
                    def rail_width = !row.WIDTH ? 1.435 + 1 : row.WIDTH + 1
                    def rail_crossing = rail_types.get(row.CROSSING)

                    if (rail_zindex >= 0 && rail_type) {
                        Geometry geom = row.the_geom
                        if (!geom.isEmpty()) {
                            def epsg = geom.getSRID()
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (!subGeom.isEmpty()) {
                                    stmt.addBatch """
                                    INSERT INTO $outputTableName values(ST_GEOMFROMTEXT(
                                    '${subGeom}',$epsg), 
                                    ${rowcount++}, 
                                    '${row.ID_SOURCE}',
                                    '${rail_type}',
                                    ${rail_crossing ? "'${rail_crossing}'" : rail_crossing},
                                    ${rail_zindex},
                                    ${rail_width},
                                    ${rail_usage ? "'${rail_usage}'" : rail_usage})
                                """
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    info "Rail formatted"
    return outputTableName
}


/**
 * This process is used to transform the raw vegetation table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw vegetation table
 * @param vegetation The name of the raw vegetation table in the DB
 * @return The name of the final vegetation table
 */
String formatVegetationLayer(JdbcDataSource datasource, String vegetation, String zone = "") throws Exception {
    debug('Vegetation transformation starts')
    def outputTableName = postfix "VEGET"
    datasource.execute(""" 
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_veget serial, ID_SOURCE VARCHAR, TYPE VARCHAR, HEIGHT_CLASS VARCHAR(4), ZINDEX INTEGER);""")
    if (vegetation) {
        if (datasource.hasTable(vegetation)) {
            def queryMapper = "SELECT a.ID_SOURCE, a.TYPE, a.ZINDEX"
            if (zone) {
                datasource.createSpatialIndex(vegetation, "the_geom")
                queryMapper += ", st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        "FROM " +
                        "$vegetation AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom "
            } else {
                queryMapper += ", a.the_geom FROM $vegetation  as a"
            }
            def vegetation_types = ['Zone arborée'             : 'wood',
                                    'Forêt fermée de feuillus' : 'forest',
                                    'Forêt fermée mixte'       : 'forest',
                                    'Forêt fermée de conifères': 'forest',
                                    'Forêt ouverte'            : 'forest',
                                    'Peupleraie'               : 'forest',
                                    'Haie'                     : 'hedge',
                                    'Lande ligneuse'           : 'heath',
                                    'Verger'                   : 'orchard',
                                    'Vigne'                    : 'vineyard',
                                    'Bois'                     : 'forest',
                                    'Bananeraie'               : 'banana_plants',
                                    'Mangrove'                 : 'mangrove',
                                    'Canne à sucre'            : 'sugar_cane',
                                    'Houblonnière'             : 'hops',
                                    'Rizière'                  : 'rice_field',
                                    'Piste en herbe'           : 'grass',
                                    'Terrain de football'      : 'grass',
                                    'Terrain de rugby'         : 'grass',
                                    'Marais'                   : 'marsh']

            def vegetation_classes = [
                    'tree'         : 'high',
                    'wood'         : 'high',
                    'forest'       : 'high',
                    'scrub'        : 'low',
                    'grassland'    : 'low',
                    'heath'        : 'low',
                    'tree_row'     : 'high',
                    'hedge'        : 'high',
                    'mangrove'     : 'high',
                    'orchard'      : 'high',
                    'vineyard'     : 'low',
                    'banana_plants': 'high',
                    'sugar_cane'   : 'low',
                    'unclassified' : 'low',
                    'hops'         : 'low',
                    'rice_field'   : 'low',
                    'grass'        : 'low',
                    'marsh'        : 'low'
            ]

            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    def vegetation_type = row.TYPE
                    if (vegetation_type) {
                        vegetation_type = vegetation_types.get(vegetation_type)
                    } else {
                        vegetation_type = "unclassified"
                    }
                    def height_class = vegetation_classes.get(vegetation_type)
                    def vegetation_zindex = row.ZINDEX
                    if (!vegetation_zindex) {
                        vegetation_zindex = 0
                    }
                    Geometry geom = row.the_geom
                    def epsg = geom.getSRID()
                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                        Geometry subGeom = geom.getGeometryN(i)
                        if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                            stmt.addBatch """
                                            INSERT INTO $outputTableName VALUES(
                                                ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                ${rowcount++}, 
                                                '${row.ID_SOURCE}',
                                                '${vegetation_type}', 
                                                '${height_class}', ${vegetation_zindex})
                                    """.toString()
                        }
                    }
                }

            }
        }
    }
    info "Vegetation formatted"
    return outputTableName
}

/**
 * This process is used to transform the BDTopo impervious table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the impervious table
 * @param impervious The name of the impervious table in the DB
 * @return outputTableName The name of the final impervious table
 */
String formatImperviousLayer(H2GIS datasource, String impervious) throws Exception {
    debug('Impervious layer')
    def outputTableName = postfix("IMPERVIOUS")
    datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_impervious serial,TYPE VARCHAR);""".toString()

    // A map of weigths to select a tag when several geometries overlap
    def weight_values = [
            "government": 5, "entertainment_arts_culture": 10, "education": 10, "military": 20,
            "industrial": 20, "commercial": 20, "healthcare": 10, "transport": 15, "building": 10,
            "sport"     : 10, "cemetery": 10, "religious": 10]

    //We must remove all overlapping geometries and then choose the attribute TYPE to set according some priorities
    def polygonizedTable = postfix("impervious_polygonized")
    datasource.execute(""" DROP TABLE IF EXISTS $polygonizedTable;
            CREATE TABLE $polygonizedTable as
            SELECT * from ST_EXPLODE('(select st_polygonize(st_union(st_accum(ST_ToMultiLine( the_geom)))) as the_geom from $impervious)')
            """.toString())

    datasource.createSpatialIndex(impervious, "the_geom")

    datasource.execute(""" CREATE SPATIAL INDEX ON $polygonizedTable(THE_GEOM);
                        CREATE INDEX ON $polygonizedTable(EXPLOD_ID);""".toString())

    def query = """SELECT LISTAGG(a.ID_IMPERVIOUS, ',') AS ids_impervious, LISTAGG(a.TYPE, ',') AS types, b.EXPLOD_ID as id, b.the_geom  FROM $impervious AS a, $polygonizedTable AS b
            WHERE a.the_geom && b.the_geom AND st_intersects(st_pointonsurface(b.the_geom), a.the_geom) GROUP BY b.explod_id;""".toString()
    int rowcount = 1
    datasource.withBatch(100) { stmt ->
        datasource.eachRow(query) { row ->
            def types = row.types
            //Choose the best impervious type
            def listTypes = types.split(",") as Set
            def type = weight_values.subMap(listTypes).max { it.key }.key
            if (type) {
                Geometry geom = row.the_geom
                def epsg = geom.getSRID()
                for (int i = 0; i < geom.getNumGeometries(); i++) {
                    Geometry subGeom = geom.getGeometryN(i)
                    if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                        stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${type}')".toString()
                    }
                }
            }
        }
    }
    datasource.execute("DROP TABLE IF EXISTS $polygonizedTable")
    info "Impervious areas formatted"
    return outputTableName
}

/**
 *
 * @return
 */
String setAliasOnColumns(String tableName) {
    return columnNames.inject([]) { result, iter ->
        result += "a.$iter"
    }.join(",")
}


