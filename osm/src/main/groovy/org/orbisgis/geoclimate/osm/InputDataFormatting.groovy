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
package org.orbisgis.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.geom.prep.PreparedGeometry
import org.locationtech.jts.geom.prep.PreparedGeometryFactory
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.util.regex.Pattern

@BaseScript OSM OSM


/**
 * This process is used to format the OSM buildings table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param building The name of the raw buildings table in the DB
 * @param zone an envelope to reduce the study area
 * @param urban_areas used to improved the building type
 * @param hLevMin Minimum building level height
 * @param jsonFilename Name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final buildings table
 * @return outputEstimatedTableName The name of the table containing the state of estimation for each building
 */
Map formatBuildingLayer(JdbcDataSource datasource, String building, String zone = "",
                        String urban_areas = "", int h_lev_min = 3, String jsonFilename = "") throws Exception {
    if (!h_lev_min) {
        h_lev_min = 3
    }
    def outputTableName = postfix "INPUT_BUILDING"
    debug 'Formating building layer'
    def outputEstimateTableName = "EST_${outputTableName}"
    datasource.execute("""
                    DROP TABLE if exists ${outputEstimateTableName};
                    CREATE TABLE ${outputEstimateTableName} (
                        id_build INTEGER,
                        ID_SOURCE VARCHAR)
                """)

    datasource.execute(""" 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY, id_build INTEGER, ID_SOURCE VARCHAR, 
                    HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER, ROOF_SHAPE VARCHAR);
            """)
    if (building) {
        def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def mappingTypeAndUse = parametersMap.type
        def typeAndLevel = parametersMap.level
        def queryMapper = "SELECT "
        def columnToMap = parametersMap.columns
        if (datasource.getRowCount(building) > 0) {
            def heightPattern = Pattern.compile("((?:\\d+\\/|(?:\\d+|^|\\s)\\.)?\\d+)\\s*([^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?(?:\\/[^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?)*)?", Pattern.CASE_INSENSITIVE)
            def columnNames = datasource.getColumnNames(building)
            columnNames.remove("THE_GEOM")
            queryMapper += columnsMapper(columnNames, columnToMap)
            queryMapper += " , st_force2D(a.the_geom) as the_geom FROM $building as a "
            if (zone) {
                Geometry geomZone = datasource.firstRow("select st_union(st_accum(the_geom)) as the_geom from $zone").the_geom
                PreparedGeometry pZone = PreparedGeometryFactory.prepare(geomZone)
                def id_build = 1;
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow(queryMapper) { row ->
                        Geometry geom = row.the_geom
                        if (pZone.intersects(geom)) {
                        def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                        def use = typeAndUseValues[1]
                        def type = typeAndUseValues[0]
                        if (type) {
                            String height = row.height
                            String roof_height = row.'roof:height'
                            String b_lev = row.'building:levels'
                            String roof_lev = row.'roof:levels'
                            def heightRoof = getHeightRoof(height, heightPattern)
                            def heightWall = getHeightWall(heightRoof, roof_height)
                            def nbLevels = getNbLevels(b_lev, roof_lev)
                            def formatedHeight = Geoindicators.WorkflowGeoIndicators.formatHeightsAndNbLevels(heightWall, heightRoof, nbLevels, h_lev_min, type, typeAndLevel)
                            def zIndex = getZIndex(row.'layer')
                            String roof_shape = row.'roof:shape'
                            if (formatedHeight.nbLevels > 0 && zIndex >= 0 && type) {
                                    def srid = geom.getSRID()
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        Geometry subGeom = geom.getGeometryN(i)
                                        if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                            subGeom.normalize()
                                            stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$srid), 
                                                    $id_build, 
                                                    '${row.id}',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    ${singleQuote(type)},
                                                    ${singleQuote(use)},
                                                    ${zIndex},
                                                    ${roof_shape ? "'" + roof_shape + "'" : null})
                                            """.toString()

                                            if (formatedHeight.estimated) {
                                                stmt.addBatch """
                                                INSERT INTO ${outputEstimateTableName} values(
                                                    $id_build, 
                                                    '${row.id}')
                                                """.toString()
                                            }
                                            id_build++
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                def id_build = 1;
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow(queryMapper) { row ->
                        Geometry geom = row.the_geom
                        if(geom) {
                            def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                            def use = typeAndUseValues[1]
                            def type = typeAndUseValues[0]
                            if (type) {
                                String height = row.height
                                String roof_height = row.'roof:height'
                                String b_lev = row.'building:levels'
                                String roof_lev = row.'roof:levels'
                                def heightRoof = getHeightRoof(height, heightPattern)
                                def heightWall = getHeightWall(heightRoof, roof_height)
                                def nbLevels = getNbLevels(b_lev, roof_lev)
                                def formatedHeight = Geoindicators.WorkflowGeoIndicators.formatHeightsAndNbLevels(heightWall, heightRoof, nbLevels, h_lev_min, type, typeAndLevel)
                                def zIndex = getZIndex(row.'layer')
                                String roof_shape = row.'roof:shape'
                                if (formatedHeight.nbLevels > 0 && zIndex >= 0 && type) {
                                    def srid = geom.getSRID()
                                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                                        Geometry subGeom = geom.getGeometryN(i)
                                        if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                            subGeom.normalize()
                                            stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${subGeom}',$srid), 
                                                    $id_build, 
                                                    '${row.id}',
                                                    ${formatedHeight.heightWall},
                                                    ${formatedHeight.heightRoof},
                                                    ${formatedHeight.nbLevels},
                                                    ${singleQuote(type)},
                                                    ${singleQuote(use)},
                                                    ${zIndex},
                                                    ${roof_shape ? "'" + roof_shape + "'" : null})
                                            """.toString()

                                            if (formatedHeight.estimated) {
                                                stmt.addBatch """
                                                INSERT INTO ${outputEstimateTableName} values(
                                                    $id_build, 
                                                    '${row.id}')
                                                """.toString()
                                            }
                                            id_build++
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            //Improve building type using the urban areas table
            if (urban_areas) {
                datasource.createSpatialIndex(outputTableName, "the_geom")
                datasource.createIndex(outputTableName, "id_build")
                datasource.createIndex(outputTableName, "type")
                def urbanAreasPart = postfix("urbanAreasPart")
                def buildinType = postfix("BUILDING_TYPE")
                String triangles = postfix("triangles")

                //Tessellate the urban_areas to perform the intersection on large geometry
                datasource.execute("""DROP TABLE IF EXISTS $triangles;
                CREATE TABLE $triangles as 
                SELECT * FROM st_explode('(SELECT CASE WHEN ST_NPoints(THE_GEOM) > 2500 
                      THEN st_subdivide(the_geom, 2500) ELSE THE_GEOM END AS THE_GEOM,
                      type FROM $urban_areas)')""".toString())
                datasource.createSpatialIndex(triangles)

                datasource.execute """DROP TABLE IF EXISTS $urbanAreasPart, $buildinType ;
            CREATE TABLE $urbanAreasPart as SELECT b.type, a.id_build, st_area(st_intersection(a.the_geom,b.the_geom))/st_area(a.the_geom) as part 
                        FROM $outputTableName a, $triangles b 
                        WHERE a.the_geom && b.the_geom and st_intersects(st_buffer(a.the_geom, 0), b.the_geom) AND  a.TYPE ='building';   
            CREATE INDEX ON $urbanAreasPart(id_build);                     
            create table $buildinType as select 
            MAX(part) FILTER (WHERE type = 'residential') as "residential",
            MAX(part) FILTER (WHERE  type = 'education')  as "education",
            MAX(part) FILTER (WHERE  type = 'school')  as "school",
            MAX(part) FILTER (WHERE  type = 'university')  as "university",
            MAX(part) FILTER (WHERE  type = 'research_institute')  as "research_institute",
            MAX(part) FILTER (WHERE  type = 'construction')  as "construction",
            MAX(part) FILTER (WHERE  type = 'commercial')  as "commercial",
            MAX(part) FILTER (WHERE  type = 'retail')  as "retail",
            MAX(part) FILTER (WHERE  type = 'industrial')  as "industrial",
            MAX(part) FILTER (WHERE  type = 'port')  as "port",
            MAX(part) FILTER (WHERE  type = 'refinery')  as "refinery",
            MAX(part) FILTER (WHERE  type = 'government')  as "government",
            MAX(part) FILTER (WHERE  type = 'community_centre')  as "community_centre",
            MAX(part) FILTER (WHERE  type = 'military')  as "military",
            MAX(part) FILTER (WHERE  type = 'railway')  as "railway",
            MAX(part) FILTER (WHERE  type = 'farmyard')  as "farmyard",
            id_build FROM $urbanAreasPart where part > 0.9 group by id_build """.toString()

                datasource.withBatch(100) { stmt ->
                    datasource.eachRow("SELECT * FROM $buildinType".toString()) { row ->
                        def new_type = "building"
                        def id_build_ = row.id_build
                        def mapTypes = row.toRowResult()
                        mapTypes.remove("ID_BUILD")
                        mapTypes.removeAll { it.value == null }
                        //Only one value type, no overlaps
                        if (mapTypes.size() == 1) {
                            new_type = mapTypes.keySet().stream().findFirst().get()
                        } else {
                            def maxType = mapTypes.max { it.value }
                            if (maxType) {
                                String maxKeyType = maxType.key
                                def sortValues = mapTypes.sort { -it.value }.keySet()
                                sortValues.remove(maxKeyType)
                                switch (maxKeyType) {
                                    case "residential":
                                        if (sortValues.contains("education")) {
                                            new_type = "education"
                                        } else {
                                            new_type = sortValues.take(1)[0]
                                        }
                                        break
                                    case "education":
                                        new_type = "education"
                                        break
                                    default:
                                        new_type = sortValues.take(1)[0]
                                        break
                                }
                            }

                        }
                        stmt.addBatch("UPDATE $outputTableName set type = '${new_type}' , main_use = '${new_type}'where id_build=${id_build_}")
                    }
                }
                datasource.execute("""DROP TABLE IF EXISTS $urbanAreasPart, $buildinType, $triangles""".toString())

            }
        }
    }
    debug 'Buildings transformation finishes'
    [building: outputTableName, building_estimated: outputEstimateTableName]
}

/**
 * This process is used to transform the OSM roads table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw roads table
 * @param road The name of the raw roads table in the DB
 * @param zone an envelope to reduce the study area
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final roads table
 */
String formatRoadLayer(
        JdbcDataSource datasource, String road, String zone = "", String jsonFilename = "") throws Exception {
    debug('Formating road layer')
    def outputTableName = postfix "INPUT_ROAD"
    datasource """
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_road serial, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, CROSSING VARCHAR(30),
                SURFACE VARCHAR, SIDEWALK VARCHAR, MAXSPEED INTEGER, DIRECTION INTEGER, LANES INTEGER, ZINDEX INTEGER);
        """.toString()
    if (road) {
        //Define the mapping between the values in OSM and those used in the abstract model
        def paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def mappingForRoadType = parametersMap.type
        def mappingForSurface = parametersMap.surface
        def typeAndWidth = parametersMap.width
        def crossingValues = parametersMap.crossing
        def queryMapper = "SELECT "
        def columnToMap = parametersMap.columns
        if (datasource.getRowCount(road) > 0) {
            def columnNames = datasource.getColumnNames(road)
            columnNames.remove("THE_GEOM")
            queryMapper += columnsMapper(columnNames, columnToMap)
            if (zone) {
                datasource.createSpatialIndex(road)
                queryMapper += ", st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        "FROM " +
                        "$road AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom"
            } else {
                queryMapper += ", a.the_geom as the_geom FROM $road  as a"
            }
            int rowcount = 1
            def speedPattern = Pattern.compile("([0-9]+)( ([a-zA-Z]+))?", Pattern.CASE_INSENSITIVE)
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    def processRow = true
                    def zIndex = getZIndex(row.'layer')
                    def road_access = row.'access'
                    def road_area = row.'area'
                    if (road_area in ['yes']) {
                        processRow = false
                    }
                    def road_service = row.'service'
                    if (road_service && road_service in ["parking_aisle", "alley", "slipway", "drive-through", "driveway"]) {
                        processRow = false
                    }
                    if (road_access && road_access in ["agricultural", "forestry"]) {
                        processRow = false
                    }

                    if (processRow) {
                        String type = getTypeValue(row, columnNames, mappingForRoadType)
                        def width = getWidth(row.'width')
                        if (!type) {
                            type = 'unclassified'
                        }
                        def widthFromType = typeAndWidth[type]
                        if (width <= 0 && widthFromType) {
                            width = widthFromType
                        }
                        def crossing = row.'bridge'
                        if (crossing) {
                            crossing = crossingValues.bridge.contains(crossing) ? "'bridge'" : null
                            if (!zIndex && crossing) {
                                zIndex = 1
                            }
                        }

                        String surface = getTypeValue(row, columnNames, mappingForSurface)

                        if (!surface) {
                            def tracktype = row.'tracktype'
                            //tracktype is use to defined the surface of the track
                            switch (tracktype) {
                                case 'grade1':
                                    surface = 'compacted'
                                    break
                                case 'grade2':
                                    surface = 'gravel'
                                    break
                                case 'grade3':
                                    surface = 'gravel'
                                    break
                                case 'grade4':
                                    surface = 'ground'
                                    break
                                case 'grade5':
                                    surface = 'ground'
                                    break
                                default:
                                    break
                            }
                        }
                        String sidewalk = getSidewalk(row.'sidewalk')

                        //maxspeed value
                        int maxspeed_value = getSpeedInKmh(speedPattern, row."maxspeed")

                        String onewayValue = row."oneway"
                        int direction = 3;
                        if (onewayValue && onewayValue == "yes") {
                            direction = 1
                        }

                        if (zIndex >= 0 && type) {
                            boolean addGeom = false
                            if (type == 'track' && surface in ['unpaved', 'asphalt', 'paved', 'cobblestone', 'metal', 'concrete', 'compacted']) {
                                addGeom = true
                            } else if (type != "track") {
                                addGeom = true
                            }
                            if (addGeom) {
                                Geometry geom = row.the_geom
                                int epsg = geom.getSRID()
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    stmt.addBatch """
                                    INSERT INTO $outputTableName VALUES(ST_GEOMFROMTEXT(
                                        '${geom.getGeometryN(i)}',$epsg), 
                                        ${rowcount++}, 
                                        '${row.id}', 
                                        ${width},
                                        ${singleQuote(type)},
                                        ${crossing}, 
                                        ${singleQuote(surface)},
                                        ${singleQuote(sidewalk)},
                                        ${maxspeed_value},
                                        ${direction},
                                        ${row.'lanes'},
                                        ${zIndex})
                                """.toString()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    debug('Roads transformation finishes')
    return outputTableName
}

/**
 * This process is used to transform the raw rails table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw rails table
 * @param rail The name of the raw rails table in the DB
 * @param zone an envelope to reduce the study area
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final rails table
 */
String formatRailsLayer(JdbcDataSource datasource, String rail, String zone = "", String jsonFilename = "") throws Exception {
    debug('Rails transformation starts')
    def outputTableName = "INPUT_RAILS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_rail serial,ID_SOURCE VARCHAR, TYPE VARCHAR,CROSSING VARCHAR(30), ZINDEX INTEGER, WIDTH FLOAT, USAGE VARCHAR(30));""".toString()

    if (rail != null) {
        def paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def mappingType = parametersMap.get("type")
        def crossingValues = parametersMap.get("crossing")
        def queryMapper = "SELECT "
        def columnToMap = parametersMap.get("columns")

        if (datasource.getRowCount(rail) > 0) {
            def columnNames = datasource.getColumnNames(rail)
            columnNames.remove("THE_GEOM")
            queryMapper += columnsMapper(columnNames, columnToMap)
            if (zone) {
                datasource.createSpatialIndex(rail)
                queryMapper += ", st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        "FROM " +
                        "$rail AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom "
            } else {
                queryMapper += ", a.the_geom as the_geom FROM $rail  as a"

            }
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper.toString()) { row ->
                    def type = getTypeValue(row, columnNames, mappingType)
                    def zIndex = getZIndex(row.'layer')
                    //special treatment if type is subway
                    if (type == "subway") {
                        if (!((row.tunnel != null && row.tunnel == "no" && zIndex >= 0)
                                || (row.bridge != null && (row.bridge == "yes" || row.bridge == "viaduct")))) {
                            type = null
                        }
                    }
                    def crossing = row.'bridge'
                    if (crossing) {
                        crossing = crossingValues.bridge.contains(crossing) ? "bridge" : null
                        if (!zIndex && crossing) {
                            zIndex = 1
                        }
                    }
                    def gauge = row.'gauge'
                    //1.435 default value for standard gauge
                    //1 constant for balasting
                    def rail_width = 1.435 + 1
                    if (gauge && gauge.isFloat()) {
                        rail_width = (gauge.toFloat() / 1000) + 1
                    }

                    if (zIndex >= 0 && type) {
                        Geometry geom = row.the_geom
                        int epsg = geom.getSRID()
                        for (int i = 0; i < geom.getNumGeometries(); i++) {
                            stmt.addBatch """
                                    INSERT INTO $outputTableName values(ST_GEOMFROMTEXT(
                                    '${geom.getGeometryN(i)}',$epsg), 
                                    ${rowcount++}, 
                                    '${row.id}',
                                    ${singleQuote(type)},
                                    ${singleQuote(crossing)},
                                    ${zIndex}, $rail_width, '${row.'usage'}')
                                """
                        }
                    }
                }
            }
        }
    }
    debug('Rails transformation finishes')
    return outputTableName
}

/**
 * This process is used to transform the raw vegetation table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw vegetation table
 * @param vegetation The name of the raw vegetation table in the DB
 * @param zone an envelope to reduce the study area
 * @param jsonFilename name of the json formatted file containing the filtering parameters
 * @return outputTableName The name of the final vegetation table
 */
String formatVegetationLayer(JdbcDataSource datasource, String vegetation, String zone = "", String jsonFilename = "") throws Exception {
    debug('Vegetation transformation starts')
    def outputTableName = postfix "INPUT_VEGET"
    datasource """ 
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_veget serial, ID_SOURCE VARCHAR, TYPE VARCHAR, HEIGHT_CLASS VARCHAR(4), ZINDEX INTEGER);""".toString()
    if (vegetation) {
        def paramsDefaultFile = this.class.getResourceAsStream("vegetParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def mappingType = parametersMap.get("type")
        def typeAndVegClass = parametersMap.get("class")
        def queryMapper = "SELECT "
        def columnToMap = parametersMap.get("columns")
        if (datasource.getRowCount(vegetation) > 0) {
            def columnNames = datasource.getColumnNames(vegetation)
            columnNames.remove("THE_GEOM")
            queryMapper += columnsMapper(columnNames, columnToMap)
            if (zone) {
                datasource.createSpatialIndex(vegetation)
                queryMapper += ", st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        "FROM " +
                        "$vegetation AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom  "
            } else {
                queryMapper += ", a.the_geom as the_geom FROM $vegetation  as a"
            }
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    def type = getTypeValue(row, columnNames, mappingType)
                    if (type) {
                        //Check surface
                        boolean addGeom = true
                        if (row."surface" && row."surface" != "grass") {
                            addGeom = false
                        }
                        if (addGeom) {
                            def height_class = typeAndVegClass[type]
                            def zindex = getZIndex(row."layer")

                            Geometry geom = row.the_geom
                            int epsg = geom.getSRID()
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                    stmt.addBatch """
                                            INSERT INTO $outputTableName VALUES(
                                                ST_GEOMFROMTEXT('${subGeom}',$epsg), 
                                                ${rowcount++}, 
                                                '${row.id}',
                                                ${singleQuote(type)}, 
                                                ${singleQuote(height_class)}, ${zindex})
                                    """.toString()
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    debug('Vegetation transformation finishes')
    return outputTableName
}


/**
 * This process is used to transform the raw hydro table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw hydro table *
 * @param water The name of the raw hydro table in the DB
 * @param zone an envelope to reduce the study area
 * @return outputTableName The name of the final hydro table
 */
String formatWaterLayer(JdbcDataSource datasource, String water, String zone = "") throws Exception {
    debug('Hydro transformation starts')
    def outputTableName = "INPUT_HYDRO_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_water serial, ID_SOURCE VARCHAR, TYPE VARCHAR, INTERMITTENT BOOLEAN DEFAULT FALSE, ZINDEX INTEGER);""".toString()

    if (water) {
        if (datasource.getRowCount(water) > 0) {
            String query
            if (zone) {
                datasource.createSpatialIndex(water, "the_geom")
                query = """select id ,  st_intersection(a.the_geom, b.the_geom) as the_geom
                        , a.\"natural\", a.\"layer\",  a.\"intermittent\"
                         FROM 
                        $water AS a, $zone AS b 
                        WHERE 
                        a.the_geom && b.the_geom """
                if (datasource.getColumnNames(water).contains("seamark:type")) {
                    query += " and (a.\"seamark:type\" is null or a.\"seamark:type\" in ('harbour_basin', 'harbour'))"
                }

            } else {
                query = "select id,  the_geom, \"natural\", \"layer\", \"intermittent\" FROM $water "
                if (datasource.getColumnNames(water).contains("seamark:type")) {
                    query += " where \"seamark:type\" is null"
                }
            }
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(query) { row ->
                    def water_type = row.natural in ['bay', 'strait'] ? 'sea' : 'water'
                    def zIndex = getZIndex(row.'layer')
                    def intermittent = row.'intermittent'
                    Geometry geom = row.the_geom
                    int epsg = geom.getSRID()
                    for (int i = 0; i < geom.getNumGeometries(); i++) {
                        Geometry subGeom = geom.getGeometryN(i)
                        if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                            stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.id}',  '${water_type}', ${(intermittent && intermittent ==  "yes")} ,${zIndex})".toString()
                        }
                    }
                }
            }
        }
    }
    debug('Hydro transformation finishes')
    return outputTableName
}

/**
 * This process is used to transform the raw impervious table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw impervious table
 * @param impervious The name of the raw impervious table in the DB
 * @param zone an envelope to reduce the study area
 * @return outputTableName The name of the final impervious table
 */
String formatImperviousLayer(JdbcDataSource datasource, String impervious, String zone = "", String jsonFilename = "") throws Exception {
    debug('Impervious transformation starts')
    def outputTableName = "INPUT_IMPERVIOUS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    debug(impervious)
    if (impervious) {
        def paramsDefaultFile = this.class.getResourceAsStream("imperviousParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def mappingTypeAndUse = parametersMap.type
        def queryMapper = "SELECT "
        def columnToMap = parametersMap.get("columns")
        if (datasource.getRowCount(impervious) > 0) {
            datasource.createSpatialIndex(impervious)
            def columnNames = datasource.getColumnNames(impervious)
            columnNames.remove("THE_GEOM")
            queryMapper += columnsMapper(columnNames, columnToMap)
            if (zone) {
                queryMapper += ", st_intersection(a.the_geom, b.the_geom) as the_geom " +
                        "FROM " +
                        "$impervious AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom"
            } else {
                queryMapper += ",  a.the_geom FROM $impervious  as a"
            }

            def polygonizedTable = postfix("polygonized")
            def polygonizedExploded = postfix("polygonized_exploded")
            datasource.execute("""  
                    DROP TABLE IF EXISTS $polygonizedTable;
                    CREATE TABLE $polygonizedTable as select st_polygonize(st_union(st_accum(ST_ToMultiLine( the_geom)))) as the_geom from 
                    ($queryMapper) as foo where "surface" not in('grass') or "parking" not in ('underground') or "building" is null;
                    DROP TABLE IF EXISTS $polygonizedExploded;
                    CREATE TABLE $polygonizedExploded as select * from st_explode('$polygonizedTable');
                    """.toString())

            datasource.createSpatialIndex(impervious)
            datasource.createSpatialIndex(polygonizedExploded)
            def filtered_area = postfix("filtered_area")
            datasource.execute("""DROP TABLE IF EXISTS $filtered_area;
                    CREATE TABLE  $filtered_area AS 
                    SELECT a.EXPLOD_ID , a.the_geom, st_area(b.the_geom) as area_imp, b.* EXCEPT(the_geom) from $polygonizedExploded as a , 
                    $impervious as b where a.the_geom && b.the_geom AND st_intersects(st_pointonsurface(a.the_geom), b.the_geom) ;
                    CREATE INDEX ON $filtered_area(EXPLOD_ID);""".toString())

            def impervious_prepared = postfix("impervious_prepared")

            datasource.execute """Drop table if exists $impervious_prepared;
                    CREATE TABLE $impervious_prepared (THE_GEOM GEOMETRY, id_impervious serial, type varchar);""".toString()

            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow("""SELECT 
                                g1.*
                                 FROM $filtered_area g1
                                 LEFT JOIN $filtered_area g2 ON 
                                    g1.EXPLOD_ID = g2.EXPLOD_ID AND g1.AREA_IMP  > g2.AREA_IMP 
                                 WHERE g2.EXPLOD_ID  IS NULL
                                 ORDER BY g1.EXPLOD_ID  ASC""".toString()) { row ->

                    def typeAndUseValues = getTypeAndUse(row, columnNames, mappingTypeAndUse)
                    def use = typeAndUseValues[1]
                    def type = typeAndUseValues[0]
                    if (type) {
                        Geometry geom = row.the_geom
                        int epsg = geom.getSRID()
                        if (!geom.isEmpty()) {
                            for (int i = 0; i < geom.getNumGeometries(); i++) {
                                Geometry subGeom = geom.getGeometryN(i)
                                if (!subGeom.isEmpty()) {
                                    if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                        stmt.addBatch "insert into $impervious_prepared values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${type}')".toString()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            //Do the union of all type
            datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName as SELECT EXPLOD_ID AS id_impervious, THE_GEOM, TYPE
                    FROM ST_EXPLODE('(
                    SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, TYPE from $impervious_prepared group by type
                    )');""".toString())
            datasource.execute("DROP TABLE IF EXISTS  $polygonizedTable, $filtered_area, $polygonizedExploded, $impervious_prepared".toString())

            return outputTableName
        }
    }
    datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_impervious serial, type varchar);""".toString()

    debug('Impervious transformation finishes')
    return outputTableName
}

/**
 * This function defines the input values for both columns type and use to follow the constraints
 * of the geoClimate Input Model
 * @param row The row of the raw table to examine
 * @param columnNames the names of the column in the raw table
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String[] getTypeAndUse(def row, def columnNames, def myMap) {
    String strType = null
    String strUse = null
    myMap.each { finalVal ->
        def type_use = finalVal.key.split(":")
        def type
        def use
        if (type_use.size() == 2) {
            type = type_use[0]
            use = type_use[1]
        } else {
            type = finalVal.key;
            use = type
        }
        finalVal.value.each { osmVals ->
            if (columnNames.contains(osmVals.key)) {
                def columnValue = row.getString(osmVals.key)
                if (columnValue != null) {
                    osmVals.value.each { osmVal ->
                        if (osmVal.startsWith("!")) {
                            osmVal = osmVal.replace("! ", "")
                            if ((columnValue != osmVal) && (columnValue != null)) {
                                if (strType == null) {
                                    strType = type
                                    strUse = use
                                }
                            }
                        } else {
                            if (columnValue == osmVal) {
                                if (strType == null) {
                                    strType = type
                                    strUse = use
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if (strUse == null) {
        strUse = strType
    }
    return [strType, strUse]
}

/**
 * This function defines the value of the column height_wall according to the values of height and r_height
 * @param height value of the building height
 * @param r_height value of the roof height
 * @return The calculated value of height_wall (default value : 0)
 */

static float getHeightWall(height, r_height) {
    float result = 0
    if (r_height != null && r_height.isFloat()) {
        if (r_height.toFloat() < height) {
            result = height - r_height.toFloat()
        }
    } else {
        result = height
    }
    return result
}

/**
 * This function defines the value of the column height_roof according to the values of height and b_height
 * @param row The row of the raw table to examine
 * @return The calculated value of height_roof (default value : 0)
 */
static float getHeightRoof(height, heightPattern) {
    if (!height) return 0
    def matcher = heightPattern.matcher(height)
    if (!matcher.find()) {
        return 0
    }
    def new_h = 0
    def match1_group1 = matcher.group(1)
    def match1_group2 = matcher.group(2)
    //next match for feet, inch
    if (matcher.find()) {
        def match2_group1 = matcher.group(1)
        def match2_group2 = matcher.group(2)
        if (match1_group1) {
            new_h = parseFloat(match1_group1) * 12
        }
        if (match2_group2 == "''") {
            new_h += parseFloat(match2_group1)
        }
        return new_h * 0.0254

    } else {
        if (match1_group1 && match1_group2 == null) {
               return parseFloat(match1_group1)
        }
        //next mach for feet, inch matcher.find();
        else {
            def type = match1_group2.toLowerCase()
            switch (type) {
                case "m":
                    return parseFloat(match1_group1)
                case "foot":
                    return parseFloat(match1_group1) * 0.3048
                case "'":
                    return parseFloat(match1_group1) * 12 * 0.0254
                case "''":
                    return parseFloat(match1_group1) * 0.0254
                default:
                    return 0
            }
        }
    }
}

/**
 * Parse to float otherwise return 0
 * @param value to parse
 * @return a float value
 */
static Float parseFloat(def value){
    try {
        return Float.parseFloat(value)
    }catch (NumberFormatException ex){
        return 0
    }
}

/**
 * This function defines the value of the column nb_lev according to the values of b_lev and r_lev
 * @param row The row of the raw table to examine
 * @return The calculated value of nb_lev (default value : 0)
 */
static int getNbLevels(b_lev, r_lev) {
    int result = 0
    if (b_lev != null && b_lev.isFloat()) {
        if (r_lev != null && r_lev.isFloat()) {
            result = b_lev.toFloat() + r_lev.toFloat()
        } else {
            result = b_lev.toFloat()
        }
    }
    return result
}

/**
 * This function defines the value of the column width according to the value of width from OSM
 * @param width The original width value
 * @return the calculated value of width (default value : null)
 */
static Float getWidth(String width) {
    return (width != null && width.isFloat()) ? width.toFloat() : 0
}

/**
 * This function defines the value of the column zindex according to the value of zindex from OSM
 * @param width The original zindex value
 * @return The calculated value of zindex (default value : null)
 */
static int getZIndex(String zindex) {
    return (zindex != null && zindex.isInteger()) ? zindex.toInteger() : 0
}

/**
 * This function defines the input value for a column of the geoClimate Input Model according to a given mapping between the expected value and the set of key/values tag in OSM
 * @param row The row of the raw table to examine
 * @param columnNames the names of the column in the raw table
 * @param myMap A map between the target values in the model and the associated key/value tags retrieved from OSM
 * @return A list of Strings : first value is for "type" and second for "use"
 */
static String getTypeValue(def row, def columnNames, def myMap) {
    String strType = null
    myMap.each { finalVal ->
        def finalKey = finalVal.key
        finalVal.value.each { osmVals ->
            if (columnNames.contains(osmVals.key)) {
                def columnValue = row.getString(osmVals.key)
                if (columnValue != null) {
                    osmVals.value.each { osmVal ->
                        if (osmVal.startsWith("!")) {
                            osmVal = osmVal.replace("! ", "")
                            if ((columnValue != osmVal) && (columnValue != null)) {
                                if (strType == null) {
                                    strType = finalKey
                                }
                            }
                        } else {
                            if (columnValue == osmVal) {
                                if (strType == null) {
                                    strType = finalKey
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return strType
}

static String singleQuote(String value) {
    return value ? "'" + value + "'" : value
}

/**
 * This function defines the value of the column sidewalk according to the values of sidewalk from OSM
 * @param width The original sidewalk value
 * @return The calculated value of sidewalk (default value : null)
 */
static String getSidewalk(String sidewalk) {
    String result
    switch (sidewalk) {
        case 'both':
            result = 'two'
            break
        case 'right':
        case 'left':
        case 'yes':
            result = 'one'
            break
        default:
            result = 'no'
            break
    }
    return result
}

/**
 * Method to find the difference between two list
 * right - left
 *
 * @param inputColumns left list
 * @param columnsToMap right list
 * @return a flat list with escaped elements
 */
static String columnsMapper(def inputColumns, def columnsToMap) {
    def flatList = inputColumns.inject([]) { result, iter ->
        result += "a.\"$iter\""
    }.join(",")

    columnsToMap.each { it ->
        if (!inputColumns*.toLowerCase().contains(it)) {
            flatList += ", null as \"${it}\""
        }
    }
    return flatList
}

/**
 * Get a set of parameters stored in a json file
 *
 * @param file
 * @param altResourceStream
 * @return
 */
static Map parametersMapping(def file, def altResourceStream) {
    def paramStream
    def jsonSlurper = new JsonSlurper()
    if (file) {
        if (new File(file).isFile()) {
            paramStream = new FileInputStream(file)
        } else {
            warn("No file named ${file} found. Taking default instead")
            paramStream = altResourceStream
        }
    } else {
        paramStream = altResourceStream
    }
    return jsonSlurper.parse(paramStream)
}


/**
 * This process is used to transform the urban areas  table into a table that matches the constraints
 * of the geoClimate Input Model
 * @param datasource A connexion to a DB containing the raw urban areas table
 * @param urban_areas The name of the urban areas table
 * @param zone an envelope to reduce the study area
 * @return outputTableName The name of the final urban areas table
 */
String formatUrbanAreas(JdbcDataSource datasource, String urban_areas, String zone = "", String jsonFilename = "") throws Exception {
    debug('Urban areas transformation starts')
    def outputTableName = "INPUT_URBAN_AREAS_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_urban serial, ID_SOURCE VARCHAR, TYPE VARCHAR);""".toString()
    if (urban_areas != null) {
        def paramsDefaultFile = this.class.getResourceAsStream("urbanAreasParams.json")
        def parametersMap = parametersMapping(jsonFilename, paramsDefaultFile)
        def mappingType = parametersMap.type
        def queryMapper = "SELECT "
        def columnToMap = parametersMap.columns
        if (datasource.getRowCount(urban_areas) > 0) {
            def columnNames = datasource.getColumnNames(urban_areas)
            columnNames.remove("THE_GEOM")
            queryMapper += columnsMapper(columnNames, columnToMap)
            if (zone) {
                datasource.createSpatialIndex(urban_areas)
                queryMapper += ", st_snaptoself(st_intersection(a.the_geom, b.the_geom), -0.001) as the_geom " +
                        "FROM " +
                        "$urban_areas AS a, $zone AS b " +
                        "WHERE " +
                        "a.the_geom && b.the_geom "
            } else {
                queryMapper += ",  st_snaptoself(a.the_geom, -0.001) as the_geom FROM $urban_areas  as a"

            }
            def constructions = ["industrial", "commercial", "residential"]
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow(queryMapper) { row ->
                    Geometry geom = row.the_geom
                    if(geom) {
                        if (!row.building) {
                            def typeAndUseValues = getTypeAndUse(row, columnNames, mappingType)
                            def type = typeAndUseValues[0]
                            //Check if the urban areas is under construction
                            if (type == "construction") {
                                def construction = row."construction"
                                if (construction && construction in constructions) {
                                    type = construction
                                }
                            }
                            if (type) {
                                int epsg = geom.getSRID()
                                for (int i = 0; i < geom.getNumGeometries(); i++) {
                                    Geometry subGeom = geom.getGeometryN(i)
                                    if (subGeom instanceof Polygon && subGeom.getArea() > 1) {
                                        stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${subGeom}',$epsg), ${rowcount++}, '${row.id}', ${singleQuote(type)})".toString()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            //Merging urban_areas
            def mergingUrbanAreas = postfix("merging_urban_areas")
            datasource.execute("""
            CREATE TABLE $mergingUrbanAreas as select CAST((row_number() over()) as Integer) as id_urban,the_geom, type from 
            ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(the_geom)) as the_geom, type from $outputTableName group by type)');
            DROP TABLE IF EXISTS $outputTableName;
            ALTER TABLE  $mergingUrbanAreas RENAME to $outputTableName;
            """.toString())

        }
    }
    debug('Urban areas transformation finishes')
    return outputTableName
}

/**
 * This process is used to build a sea-land mask layer from the coastline and zone table
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param coastline The name of the coastlines table in the DB
 * @param zone The name of the zone table to limit the area
 * @param water The name of the input water table to improve sea extraction
 * @return outputTableName The name of the final buildings table
 */
String formatSeaLandMask(JdbcDataSource datasource, String coastline, String zone = "", String water = "") throws Exception {
    String outputTableName = postfix "INPUT_SEA_LAND_MASK_"
    datasource.execute """Drop table if exists $outputTableName;
                    CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id serial, type varchar);""".toString()
    if (!zone) {
        debug "A zone table must be provided to compute the sea/land mask"
    } else if (coastline) {
        if (datasource.hasTable(coastline) && datasource.getRowCount(coastline) > 0) {
            debug 'Computing sea/land mask table'
            datasource.createSpatialIndex(coastline, "the_geom")
            def mergingDataTable = "coatline_merged${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def coastLinesIntersects = "coatline_intersect_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def islands_mark = "islands_mark_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def coastLinesIntersectsPoints = "coatline_intersect_points_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def coastLinesPoints = "coatline_points_zone${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def sea_land_mask = "sea_land_mask${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def water_to_be_filtered = "water_to_be_filtered${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def water_filtered_exploded = "water_filtered_exploded${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def sea_land_triangles = "sea_land_triangles${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def sea_id_triangles = "sea_id_triangles${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def water_id_triangles = "water_id_triangles${UUID.randomUUID().toString().replaceAll("-", "_")}"

            datasource.createSpatialIndex(coastline, "the_geom")
            datasource.execute """DROP TABLE IF EXISTS $coastLinesIntersects, 
                        $islands_mark, $mergingDataTable,  $coastLinesIntersectsPoints, $coastLinesPoints,$sea_land_mask,
                        $water_filtered_exploded,$water_to_be_filtered, $sea_land_triangles, $sea_id_triangles, $water_id_triangles;
                        CREATE TABLE $coastLinesIntersects AS SELECT ST_intersection(a.the_geom, b.the_geom) as the_geom
                        from $coastline  AS  a,  $zone  AS b WHERE
                        a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom) and "natural"= 'coastline';
                        """.toString()

            if (water) {
                //Sometimes there is no coastlines
                if (datasource.getRowCount(coastLinesIntersects) > 0) {
                    datasource.createSpatialIndex(water, "the_geom")
                    datasource.execute """
                        CREATE TABLE $islands_mark (the_geom GEOMETRY, ID SERIAL) AS 
                       SELECT the_geom, EXPLOD_ID  FROM st_explode('(  
                       SELECT ST_LINEMERGE(st_accum(THE_GEOM)) AS the_geom, NULL FROM $coastLinesIntersects)');""".toString()

                    //Reduce the zone to limit intersection rounding with JTS
                    String reducedZone = postfix("reduced_zone")
                    datasource.execute("""DROP TABLE IF EXISTS $reducedZone;
                    CREATE TABLE $reducedZone as select st_buffer(the_geom, -0.01,2) as the_geom from $zone;""")

                    datasource.execute("""  
                        CREATE TABLE $mergingDataTable  AS
                        SELECT  THE_GEOM FROM $coastLinesIntersects 
                        UNION ALL
                        SELECT the_geom
                        from $reducedZone 
                        UNION ALL
                        SELECT st_tomultiline(the_geom)
                        from $water ;
                        CREATE TABLE $sea_land_mask (THE_GEOM GEOMETRY,ID serial, TYPE VARCHAR, ZINDEX INTEGER) AS SELECT THE_GEOM, EXPLOD_ID, 'land', 0 AS ZINDEX FROM
                        st_explode('(SELECT st_polygonize(st_union(ST_NODE(st_accum(the_geom)))) AS the_geom FROM $mergingDataTable)')
                        as foo where ST_DIMENSION(the_geom) = 2 AND st_area(the_geom) >0;  """)

                    datasource.execute("""
                        CREATE SPATIAL INDEX IF NOT EXISTS ${sea_land_mask}_the_geom_idx ON $sea_land_mask (THE_GEOM);
                       
                        CREATE SPATIAL INDEX IF NOT EXISTS ${islands_mark}_the_geom_idx ON $islands_mark (THE_GEOM);

                        CREATE TABLE $coastLinesPoints as  SELECT ST_LocateAlong(the_geom, 0.5, -0.01) AS the_geom FROM 
                        st_explode('(select ST_GeometryN(ST_ToMultiSegments(st_intersection(a.the_geom, b.the_geom)), 1) as the_geom from $islands_mark as a,
                        $reducedZone as b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom))');
    
                        CREATE TABLE $coastLinesIntersectsPoints as  SELECT the_geom FROM st_explode('$coastLinesPoints'); 

                        CREATE SPATIAL INDEX IF NOT EXISTS ${coastLinesIntersectsPoints}_the_geom_idx ON $coastLinesIntersectsPoints (THE_GEOM);
                        DROP TABLE IF EXISTS $reducedZone;""")

                    //Perform triangulation to tag the areas as sea or water
                    datasource.execute """
                         DROP TABLE IF EXISTS $sea_land_triangles;
                       CREATE TABLE $sea_land_triangles AS 
                       SELECT * FROM 
                        st_explode('(SELECT CASE WHEN ST_AREA(THE_GEOM) > 100000 THEN ST_Tessellate(the_geom) ELSE THE_GEOM END AS THE_GEOM,
                      ID, TYPE, ZINDEX FROM $sea_land_mask)');

                    CREATE SPATIAL INDEX IF NOT EXISTS ${sea_land_triangles}_the_geom_idx ON $sea_land_triangles (THE_GEOM);

                    DROP TABLE IF EXISTS $sea_id_triangles;
                    CREATE TABLE $sea_id_triangles AS SELECT DISTINCT a.id FROM $sea_land_triangles a, 
                    $coastLinesIntersectsPoints b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_intersects(a.THE_GEOM, b.THE_GEOM);  
                    CREATE INDEX ON  $sea_id_triangles (id);""".toString()

                    //Set the triangles to sea
                    datasource.execute """                                     
                    UPDATE ${sea_land_triangles} SET TYPE='sea' WHERE ID IN(SELECT ID FROM $sea_id_triangles);   
                    """.toString()

                    //Set the triangles to water
                    datasource.execute """
                    DROP TABLE IF EXISTS $water_id_triangles;
                    CREATE TABLE $water_id_triangles AS SELECT a.ID
                                FROM ${sea_land_triangles} a, $water b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_intersects( a.THE_GEOM, st_pointonsurface(b.the_geom)) and b.type='water';
                               
                    CREATE INDEX ON  $water_id_triangles (id);
                         
                    --Update water triangles            
                    UPDATE $sea_land_triangles SET TYPE='water' WHERE ID IN(SELECT ID FROM $water_id_triangles);                         
                         """.toString()

                    //Unioning all geometries
                    datasource.execute("""DROP TABLE if exists ${outputTableName};
                    create table $outputTableName as select id , 
                    st_union(st_accum(the_geom)) the_geom, type from $sea_land_triangles a group by id, type;
                    """.toString())
                } else {
                    //We look for the type of water
                    def waterTypes = datasource.firstRow("SELECT COUNT(*) as count, type from $water group by type".toString())
                    //There is only sea geometries then we then decided to put the entire study area in a sea zone
                    //As a result, the water layer is replaced by the entire
                    if (!waterTypes.containsValue("water")) {
                        datasource.execute("""
                            DROP TABLE IF EXISTS $water;
                            CREATE TABLE $water as select CAST(1 AS INTEGER) AS ID_WATER, NULL AS ID_SOURCE , CAST(0 AS INTEGER) AS ZINDEX, the_geom, 'sea' as type from $zone  ;                     
                            """.toString())
                        return outputTableName
                    }
                }
            } else {
                datasource.execute """
                        CREATE TABLE $islands_mark (the_geom GEOMETRY, ID SERIAL) AS 
                       SELECT the_geom, EXPLOD_ID  FROM st_explode('(  
                       SELECT ST_LINEMERGE(st_accum(THE_GEOM)) AS the_geom, NULL FROM $coastLinesIntersects)');""".toString()

                datasource.execute """  
                        CREATE TABLE $mergingDataTable  AS
                        SELECT  THE_GEOM FROM $coastLinesIntersects 
                        UNION ALL
                        SELECT st_tomultiline(st_buffer(the_geom, -0.01,2))
                        from $zone ;

                        CREATE TABLE $sea_land_mask (THE_GEOM GEOMETRY,ID serial, TYPE VARCHAR, ZINDEX INTEGER) AS SELECT THE_GEOM, EXPLOD_ID, 'land', 0 AS ZINDEX FROM
                        st_explode('(SELECT st_polygonize(st_union(ST_NODE(st_accum(the_geom)))) AS the_geom FROM $mergingDataTable)') as foo where ST_DIMENSION(the_geom) = 2 AND st_area(the_geom) >0;                
                        
                        CREATE SPATIAL INDEX IF NOT EXISTS ${sea_land_mask}_the_geom_idx ON $sea_land_mask (THE_GEOM);

                        CREATE SPATIAL INDEX IF NOT EXISTS ${islands_mark}_the_geom_idx ON $islands_mark (THE_GEOM);

                        CREATE TABLE $coastLinesPoints as  SELECT ST_LocateAlong(the_geom, 0.5, -0.01) AS the_geom FROM 
                        st_explode('(select ST_GeometryN(ST_ToMultiSegments(st_intersection(a.the_geom, b.the_geom)), 1) as the_geom from $islands_mark as a,
                        $zone as b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom))');
    
                        CREATE TABLE $coastLinesIntersectsPoints as  SELECT the_geom FROM st_explode('$coastLinesPoints'); 

                        CREATE SPATIAL INDEX IF NOT EXISTS ${coastLinesIntersectsPoints}_the_geom_idx ON $coastLinesIntersectsPoints (THE_GEOM);
                         """.toString()

                //Perform triangulation to tag the areas as sea or water
                datasource.execute """
                         DROP TABLE IF EXISTS $sea_land_triangles;
                       CREATE TABLE $sea_land_triangles AS 
                       SELECT * FROM 
                        st_explode('(SELECT CASE WHEN ST_AREA(THE_GEOM) > 100000 THEN ST_Tessellate(the_geom) ELSE THE_GEOM END AS THE_GEOM,
                      ID, TYPE, ZINDEX FROM $sea_land_mask)');

                    CREATE SPATIAL INDEX IF NOT EXISTS ${sea_land_triangles}_the_geom_idx ON $sea_land_triangles (THE_GEOM);

                    DROP TABLE IF EXISTS $sea_id_triangles;
                    CREATE TABLE $sea_id_triangles AS SELECT DISTINCT a.id FROM $sea_land_triangles a, 
                    $coastLinesIntersectsPoints b WHERE a.THE_GEOM && b.THE_GEOM AND
                                st_contains(a.THE_GEOM, b.THE_GEOM);  
                    CREATE INDEX ON  $sea_id_triangles (id);
                    
                    --Update sea triangles                    
                    UPDATE ${sea_land_triangles} SET TYPE='sea' WHERE ID IN(SELECT ID FROM $sea_id_triangles);   
                     """.toString()

                //Unioning all geometries
                datasource.execute("""DROP TABLE if exists ${outputTableName};
                    create table $outputTableName as select id, 
                    st_union(st_accum(the_geom)) the_geom, type from $sea_land_triangles a group by id, type;
                    """.toString())
            }

            datasource.execute("""DROP TABLE IF EXISTS $coastLinesIntersects,
                        $islands_mark, $mergingDataTable,  $coastLinesIntersectsPoints, $coastLinesPoints,$sea_land_mask,
                        $water_filtered_exploded,$water_to_be_filtered, $sea_land_triangles, $sea_id_triangles, $water_id_triangles
                        """.toString())
            debug 'The sea/land mask has been computed'
            return outputTableName
        } else {
            //There is no coatline geometries, we check the water table.
            if (water) {
                def waterTypes = datasource.firstRow("SELECT COUNT(*) as count, type from $water group by type".toString())
                //There is only sea geometries then we then decided to put the entire study area in a sea zone
                //As a result, the water layer is replaced by the entire
                if (waterTypes && !waterTypes.containsValue("water")) {
                    datasource.execute("""
                            DROP TABLE IF EXISTS $water;
                            CREATE TABLE $water as select CAST(1 AS INTEGER) AS ID_WATER, NULL AS ID_SOURCE , CAST(0 AS INTEGER) AS ZINDEX, the_geom, 'sea' as type from $zone  ;                        
                            """.toString())
                    return outputTableName
                }
            }
        }
    }
    return outputTableName
}


/**
 * Return a maxspeed value expressed in kmh
 * @param maxspeedValue from OSM
 * @return
 */
static int getSpeedInKmh(def speedPattern, String maxspeedValue) {
    if (!maxspeedValue) return -1
    def matcher = speedPattern.matcher(maxspeedValue)
    if (!matcher.matches()) return -1

    def speed = Integer.parseInt(matcher.group(1))

    if (!(matcher.group(3))) return speed

    def type = matcher.group(3).toLowerCase()
    switch (type) {
        case "kmh":
            return speed
        case "mph":
            return Math.round(speed * 1.609)
        default:
            return -1
    }
}




