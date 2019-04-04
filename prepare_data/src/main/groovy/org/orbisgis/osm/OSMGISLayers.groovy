package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

/**
 * OSMGISLayers is the main script to build a set of OSM GIS layers based on OSM data.
 * It uses the overpass api to download data
 * It builds a sql script file to create the layers table with the geometries and the attributes given as parameters
 * It produces a shapefile for each layer
 * Produced layers : buildings, roads, rails, vegetation, hydro
 **/


import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.PrepareData

@BaseScript PrepareData prepareData

//TODO : use stream api instead of file
/**
 * This process is used to create the buildings table thank to the osm data tables
 * @return the name of the buildings table
 */
static IProcess prepareBuildings() {
    return processFactory.create(
            "Prepare the building layer with OSM data",
            [datasource: JdbcDataSource,
             osmTablesPrefix: String,
             outputColumnNames: Map,
             tagKeys: String[],
             tagValues: String[],
             buildingTablePrefix: String,
             filteringZoneTableName: String],
            [buildingTableName: String],
            { datasource, osmTablesPrefix, outputColumnNames, tagKeys, tagValues,
              buildingTablePrefix, filteringZoneTableName ->
                logger.info('Buildings preparation starts')
                def tableName
                if (buildingTablePrefix != null && buildingTablePrefix.endsWith("_")) {
                    tableName = buildingTablePrefix+'INPUT_BUILDING'
                } else {
                    tableName = buildingTablePrefix+'_INPUT_BUILDING'
                }
                def scriptFile = File.createTempFile("createBuildingTable", ".sql")
                defineBuildingScript(osmTablesPrefix, outputColumnNames, tagKeys, tagValues,
                        scriptFile, tableName, filteringZoneTableName)
                datasource.executeScript(scriptFile.getAbsolutePath())
                scriptFile.delete()
                logger.info('Buildings preparation finishes')
                [buildingTableName: tableName]
            }
    )
}

/**
 * This process is used to create the roads table thank to the osm data tables
 * @return the name of the roads table
 */
static IProcess prepareRoads() {
    return processFactory.create(
            "Prepare the roads layer with OSM data",
            [datasource: JdbcDataSource,
             osmTablesPrefix: String,
             outputColumnNames: Map,
             tagKeys: String[],
             tagValues: String[],
             roadTablePrefix: String,
             filteringZoneTableName: String],
            [roadTableName: String],
            { datasource, osmTablesPrefix, outputColumnNames, tagKeys, tagValues,
              roadTablePrefix, filteringZoneTableName ->
                logger.info('Roads preparation starts')
                String tableName
                if (roadTablePrefix != null && roadTablePrefix.endsWith("_")){
                    tableName = roadTablePrefix+'INPUT_ROAD'
                } else {
                    tableName = roadTablePrefix+'_INPUT_ROAD'
                }
                def scriptFile = File.createTempFile("createRoadTable", ".sql")
                defineRoadScript(osmTablesPrefix, outputColumnNames, tagKeys, tagValues,
                        scriptFile, tableName, filteringZoneTableName)
                datasource.executeScript(scriptFile.getAbsolutePath())
                scriptFile.delete()
                logger.info('Roads preparation finishes')
                [roadTableName: tableName]
            }
    )
}

/**
 * This process is used to create the rails table thank to the osm data tables
 * @param datasource A h2GIS db containing the 11 OSM tables
 * @return railTableName The name of the rails table in the db
 */
static IProcess prepareRails() {
    return processFactory.create(
            "Prepare the rails layer with OSM data",
            [datasource: JdbcDataSource,
             osmTablesPrefix: String,
             outputColumnNames: Map,
             tagKeys: String[],
             tagValues: String[],
             railTablePrefix: String,
             filteringZoneTableName: String],
            [railTableName: String],
            { datasource, osmTablesPrefix, outputColumnNames, tagKeys, tagValues,
              railTablePrefix, filteringZoneTableName ->
                logger.info('Rails preparation starts')
                String tableName
                if (railTablePrefix != null && railTablePrefix.endsWith("_")){
                    tableName = railTablePrefix+'INPUT_RAIL'
                } else {
                    tableName = railTablePrefix+'_INPUT_RAIL'
                }
                def scriptFile = File.createTempFile("createRailTable", ".sql")
                defineRailScript osmTablesPrefix, outputColumnNames, tagKeys, tagValues,
                        scriptFile, tableName, filteringZoneTableName
                datasource.executeScript scriptFile.getAbsolutePath()
                scriptFile.delete()
                logger.info('Rails preparation finishes')
                [railTableName: tableName]
            }
    )
}

//List of parameters
// Zone to search in - must correspond to a administrative level 8 value
String zoneCode = '56243' // Vannes 56260 - Séné 56243
// Size of the extended zone to compute - in meters
int extZoneSize = 1000
// Size of the zone buffer to compute - in meters
int bufferZoneSize = 500
def prefix = "zoneExt" //prefix of the tables name in the h2DB

/*
 * Information to retrieve for the rails layer
 */
// Tags that should be retrieved to compute the input_rail table (and the names they'll have in the table)
def railOptions = ['highspeed':'highspeed','railway':'railway','service':'service','tunnel':'tunnel','layer':'layer','bridge':'bridge']
// Tag keys in which to search for the rails
def railTagKeys = ['railway']
// Corresponding tag values to search for the rails if any specific
def railTagValues = null

/*
 * Information to retrieve for the vegetation layer
 */
// TODO : reorganise keys and values as for the hydro layer

// Tags that should be retrieved to compute the input_veget table (and the names they'll have in the table)
def vegetOptions = ['natural':'natural','landuse':'landuse','landcover':'landcover','vegetation':'vegetation','barrier':'barrier','fence_type':'fence_type',
                    'hedge':'hedge','wetland':'wetland','vineyard':'vineyard','trees':'trees','crop':'crop','produce':'produce']
// Tag keys in which to search for the vegetation
def vegetTagKeys = ['natural', 'landuse','landcover']

// Corresponding tag values to search for the vegetation if any specific
def vegetTagValues = ['fell', 'heath', 'scrub', 'tree', 'tree_row', 'trees', 'wood','farmland','forest','grass','grassland','greenfield',
                      'meadow','orchard','plant_nursery','vineyard','hedge','hedge_bank','mangrove','banana_plants','banana','sugar_cane']

/*
 * Information to retrieve for the hydro layer
 */
// Tags that should be retrieved to compute the input_hydro table (and the names they'll have in the table)
def hydroOptions = ['natural':'natural','water':'water','waterway':'waterway']
// Tag keys/values in which to search for the hydro
def hydroTags = ['natural':['water','waterway','bay'],'water':[],'waterway':[]]


//The sql script to create the buildings from osm model
// ELS : for the moment, I've been putting everything in the tmp dir so that it can be both used on Windows and Linux without changing a single line
def tmpPath = System.getProperty("java.io.tmpdir")

// working folder
def outputFolder = tmpPath + File.separator + "osm_gis_final" // working folder
def outputGarbageFolder = tmpPath + File.separator + "osm_gis_final" + File.separator + "tmp" // garbage folder

//filenames of temporary files to store the layers tables creation sql scripts
def buildingsScriptPath = outputGarbageFolder + File.separator + 'createOSMBuildingTable.sql'
def roadsScriptPath = outputGarbageFolder + File.separator + 'createOSMRoadTable.sql'
def railsScriptPath = outputGarbageFolder + File.separator + 'createOSMRailTable.sql'
def vegetScriptPath = outputGarbageFolder + File.separator + 'createOSMVegetTable.sql'
def hydroScriptPath = outputGarbageFolder + File.separator + 'createOSMHydroTable.sql'
def dbName = outputFolder + File.separator + 'osmGisDb'

// filenames of the files to store the layers tables in a geojson format
def zoneFilePath = outputFolder + File.separator + 'zone.geojson'
def extZoneFilePath = outputFolder + File.separator + 'zone_extended.geojson'
def bufferZoneFilePath = outputFolder + File.separator + 'zone_buffer.geojson'
def zoneNeighborsFilePath = outputFolder + File.separator + 'zone_neighbors.geojson'
def buildingsFilePath = outputFolder + File.separator + 'input_building.geojson'
def roadsFilePath = outputFolder + File.separator + 'input_road.geojson'
def railsFilePath = outputFolder + File.separator + 'input_rail.geojson'
def vegetFilePath = outputFolder + File.separator + 'input_veget.geojson'
def hydroFilePath = outputFolder + File.separator + 'input_hydro.geojson'

//create the folders
def tmpFolder = new File(outputFolder)
tmpFolder.deleteDir()
tmpFolder.mkdir()
def tmpGarbFolder = new File(outputGarbageFolder)
tmpGarbFolder.mkdir()

//Temporary file to store the osm data retrieved at the zone level
def outputOSMFile = new File(outputGarbageFolder + File.separator + "zone.osm")

//zone download : relation, ways and nodes corresponding to the targeted zone limit
def initQuery="[timeout:900];(relation[\"ref:INSEE\"=\"$zoneCode\"][\"boundary\"=\"administrative\"][\"admin_level\"=\"8\"];>;);out;"
//Download the corresponding OSM data
def startTime = System.currentTimeMillis()
executeOverPassQuery(initQuery, outputOSMFile)

if (outputOSMFile.exists()) {
    def h2GIS =H2GIS.open([databaseName: dbName])
    h2GIS.load(outputOSMFile.absolutePath, 'zoneOsm', true)
    // Create the polygon corresponding to the zone limit and its extended area
    h2GIS.execute zoneSQLScript('zoneOsm',zoneCode,extZoneSize, bufferZoneSize)

    //define the coordinates of the extended zone bbox
    h2GIS.select('''
        ST_XMin(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as minLon, 
        ST_XMax(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as maxLon,
        ST_YMin(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as minLat, 
        ST_YMax(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as maxLat
    ''').from('ZONE_EXTENDED').getTable().eachRow{ row -> bbox = "$row.minLat, $row.minLon, $row.maxLat, $row.maxLon"}
    h2GIS.save('ZONE', zoneFilePath)
    h2GIS.save('ZONE_EXTENDED', extZoneFilePath)
    h2GIS.save('ZONE_BUFFER', bufferZoneFilePath)

    h2GIS.execute(dropOSMTables('zoneOsm'))

    logger.info('Zone, Zone Buffer and Zone Extended OK')

    //Download all the osm data concerning the extended zone bbox
    def outputOSMFile2 = new File(outputGarbageFolder + File.separator + "zoneExtended.osm")
    def queryURLExtendedZone  = "[bbox:$bbox];((node;way;relation;);>;);out;"
    executeOverPassQuery(queryURLExtendedZone, outputOSMFile2)

    //If the osm file is downloaded do the job
    if (outputOSMFile2.exists()) {

        //Import the OSM file
        h2GIS.load(outputOSMFile2.absolutePath, prefix, true)
        h2GIS.execute createIndexesOnOSMTables(prefix)

        //Create the zone_neighbors table and save it in the targeted shapefile
        h2GIS.execute zoneNeighborsSQLScript(prefix)
        h2GIS.save('ZONE_NEIGHBORS',zoneNeighborsFilePath)
        logger.info('Zone neighbors OK')

        //Create the buildings table
        IProcess process = prepareBuildings()
        process.execute([
                datasource   : h2GIS,
                tablesPrefix : "ext",
                ouputColumnNames: ['height':'height','building:height':'b_height','roof:height':'r_height','building:roof:height':'b_r_height',
                                   'building:levels':'b_lev','roof:levels':'r_lev','building:roof:levels':'b_r_lev','building':'building',
                                   'amenity':'amenity','layer':'zindex','aeroway':'aeroway','historic':'historic','leisure':'leisure','monument':'monument',
                                   'place_of_worship':'place_of_worship','military':'military','railway':'railway','public_transport':'public_transport',
                                   'barrier':'barrier','government':'government','historic:building':'historic_building','grandstand':'grandstand',
                                   'house':'house','shop':'shop','industrial':'industrial','man_made':'man_made', 'residential':'residential',
                                   'apartments':'apartments','ruins':'ruins','agricultural':'agricultural','barn':'barn', 'healthcare':'healthcare',
                                   'education':'education','restaurant':'restaurant','sustenance':'sustenance','office':'office'],
                tagKeys: ['building'],
                tagValues: null,
                buildingTableName: "RAW_INPUT_BUILDING",
                filteringZoneTableName: "ZONE_BUFFER"])

        //Create the roads table
        process = prepareRoads()
        process.execute([
                datasource   : h2GIS,
                tablesPrefix : "ext",
                ouputColumnNames: ['width':'width','highway':'highway', 'surface':'surface', 'sidewalk':'sidewalk',
                                   'lane':'lane','layer':'zindex','maxspeed':'maxspeed','oneway':'oneway',
                                   'h_ref':'h_ref','route':'route','cycleway':'cycleway',
                                   'biclycle_road':'biclycle_road','cyclestreet':'cyclestreet','junction':'junction'],
                tagKeys: ['highway','cycleway','biclycle_road','cyclestreet','route','junction'],
                tagValues: null,
                roadTableName: "RAW_INPUT_ROAD2",
                filteringZoneTableName: "ZONE_BUFFER"])


        //Create the rails table and save it in the targeted file
        defineRailScript(prefix, railOptions, railTagKeys, railTagValues, new File(railsScriptPath), "RAW_INPUT_RAIL","ZONE_BUFFER")
        h2GIS.executeScript(railsScriptPath)
        h2GIS.save('INPUT_RAIL', railsFilePath)
        logger.info('Rails OK')

        //Create the vegetation table and save it in the targeted file
        defineVegetationScript(prefix, vegetOptions, vegetTagKeys, vegetTagValues, new File(vegetScriptPath), "RAW_INPUT_VEGET", "ZONE_EXTENDED")
        h2GIS.executeScript(vegetScriptPath)
        h2GIS.save('INPUT_VEGET', vegetFilePath)
        logger.info('Vegetation OK')

        //Create the hydro table and save it in the targeted file
        defineHydroScript(prefix, hydroOptions, hydroTags, hydroScriptPath, "RAW_INPUT_HYDRO", "ZONE_EXTENDED")
        h2GIS.executeScript(hydroScriptPath)
        h2GIS.save('INPUT_HYDRO', hydroFilePath)
        logger.info('Hydro OK')

        logger.info "DB and geojson files are ready in folder $outputFolder"
        //tmpGarbFolder.deleteDir()
        h2GIS.execute(dropOSMTables(prefix))


        def endTime = System.currentTimeMillis()
        logger.info 'Process took : ' + (endTime - startTime)/1000 +' seconds'

    } else {
        println "Cannot find OSM data on the requested area"
    }

} else {
    println "Cannot find OSM data on the requested area"
}//Download the osm data according to an insee code



/**
 ** Method to execute the Overpass query
 ** TODO replace with overpass4J in a future release
 **/
def executeOverPassQuery(def query, def outputOSMFile) {
    if (outputOSMFile.exists()) {
        outputOSMFile.delete()
    }
    apiUrl = "https://lz4.overpass-api.de/api/interpreter?data="
    def connection = new URL(apiUrl + URLEncoder.encode(query, java.nio.charset.StandardCharsets.UTF_8.toString())).openConnection() as HttpURLConnection

    println(apiUrl + URLEncoder.encode(query, java.nio.charset.StandardCharsets.UTF_8.toString()))

    connection.setRequestMethod("GET")

    logger.info "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        logger.info "Downloading the OSM data from overpass api"
        outputOSMFile << connection.inputStream
        return true
    } else {
        println "Cannot execute the query"
        return false
    }
}

/**
 ** Function to drop the temp tables coming from the OSM extraction
 **/

String dropOSMTables (String prefix) {
    script = '        DROP TABLE IF EXISTS '+prefix+'_NODE;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_NODE_MEMBER;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_NODE_TAG;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_RELATION;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_RELATION_MEMBER;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_RELATION_TAG;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_TAG;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_WAY;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_WAY_MEMBER;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_WAY_NODE;\n' +
            '        DROP TABLE IF EXISTS '+prefix+'_WAY_TAG;'
    return script
}
/**
 ** Function to prepare the script to index the tables from OSM
 **/
static String createIndexesOnOSMTables(def prefix){
    def script = ''
    script = '''
            CREATE INDEX IF NOT EXISTS map_node_index on map_node(id_node);
            CREATE INDEX IF NOT EXISTS map_way_node_index on map_way_node(id_node);
            CREATE INDEX IF NOT EXISTS map_way_node_index2 on map_way_node(node_order);
            CREATE INDEX IF NOT EXISTS map_way_node_index3 ON map_way_node(id_way);
            CREATE INDEX IF NOT EXISTS map_way_index on map_way(id_way);
            CREATE INDEX IF NOT EXISTS map_way_tag_id_index on map_way_tag(id_tag);
            CREATE INDEX IF NOT EXISTS map_way_tag_va_index on map_way_tag(id_way);
            CREATE INDEX IF NOT EXISTS map_tag_id_index on map_tag(id_tag);
            CREATE INDEX IF NOT EXISTS map_tag_key_index on map_tag(tag_key);
            CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(id_tag);
            CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index2 ON map_relation_tag(id_relation);
            CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(tag_value);
            CREATE INDEX IF NOT EXISTS map_relation_tag_rel_index ON map_relation(id_relation);
            CREATE INDEX IF NOT EXISTS map_way_member_index ON map_way_member(id_relation);
            '''
    return script.replaceAll('map',prefix)
}

/**
 ** Function to create the script for the selected zone
 **/
static String zoneSQLScript(def prefix, def zoneCode, def bboxSize, def bufferSize) {
    def script = 'DROP TABLE IF EXISTS ZONE;\n' +
            'CREATE TABLE ZONE AS\n' +
            '        SELECT '+ zoneCode + ', ST_Polygonize(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation'
    script += '''
        FROM (
                SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, id_way
                FROM (
                        SELECT (
                                SELECT ST_ACCUM(the_geom) the_geom
                                FROM (
                                        SELECT n.id_node, n.the_geom, wn.id_way idway
                                        FROM map_node n, map_way_node wn
                                        WHERE n.id_node = wn.id_node ORDER BY wn.node_order
                                )
                                WHERE  idway = w.id_way) the_geom
                        , w.id_way, br.id_relation
                        FROM map_way w JOIN (
                        SELECT rt.id_relation, wm.id_way
                        FROM map_relation_tag rt
                        JOIN map_tag t ON (rt.id_tag = t.id_tag)
                        JOIN map_way_member wm ON(rt.id_relation = wm.id_relation)
                        WHERE tag_key = 'admin_level'
                        AND tag_value = '8'
                ) br ON (w.id_way = br.id_way)) geom_table
                WHERE st_numgeometries(the_geom)>=2)
        GROUP BY id_relation;
    
        -- Generation of a rectangular area (bbox) around the studied zone
        DROP TABLE IF EXISTS ZONE_EXTENDED;
        CREATE TABLE ZONE_EXTENDED AS
        SELECT ST_EXPAND( the_geom , '''
    script += bboxSize+') as the_geom'
    script += '''
        FROM zone;
        CREATE SPATIAL INDEX ON ZONE_EXTENDED(the_geom) ;

        -- Generation of a buffer around the studied zone
        DROP TABLE IF EXISTS ZONE_BUFFER;
        CREATE TABLE ZONE_BUFFER AS
        SELECT ST_BUFFER( the_geom , '''
    script += bboxSize+') as the_geom'
    script += '''
        FROM zone;
        CREATE SPATIAL INDEX ON ZONE_BUFFER(the_geom) ;
    '''
    return script.replaceAll('map',prefix)
}

/**
 ** Function to create the script for the zone neighbors
 **/
def zoneNeighborsSQLScript(def prefix){
    def script = '''
        DROP TABLE IF EXISTS ZONE_NEIGHBORS;
        CREATE TABLE ZONE_NEIGHBORS AS
        SELECT a.id_zone, st_polygonize(st_union(b.the_geom)) the_geom
        from (
                select tag_value as id_zone, a.id_relation
                from map_relation_tag a 
                    join (select id_relation
                            from map_relation_tag rt 
                                join map_tag t ON (rt.id_tag = t.id_tag)
                            WHERE tag_key = 'admin_level\'
                            AND tag_value = '8') b on (a.id_relation=b.id_relation)
                    join map_tag T on (a.id_tag = T.id_tag)
                WHERE tag_key='ref:INSEE') a
            join (
                SELECT ST_LINEMERGE(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation
                FROM (
                        SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, id_way
                        FROM (
                                SELECT (
                                        SELECT ST_ACCUM(the_geom) the_geom
                                        FROM (
                                                SELECT n.id_node, n.the_geom, wn.id_way idway
                                                FROM map_node n, map_way_node wn
                                                WHERE n.id_node = wn.id_node ORDER BY wn.node_order
                                        )
                                        WHERE  idway = w.id_way) the_geom
                                , w.id_way, br.id_relation
                                FROM map_way w JOIN (
                                SELECT rt.id_relation, wm.id_way
                                FROM map_relation_tag rt
                                JOIN map_tag t ON (rt.id_tag = t.id_tag)
                                JOIN map_way_member wm ON(rt.id_relation = wm.id_relation)
                                WHERE tag_key = 'admin_level\'
                                AND tag_value = '8\'
                        ) br ON (w.id_way = br.id_way)) geom_table
                        WHERE st_numgeometries(the_geom)>=2)
                GROUP BY id_relation) b on a.id_relation = b.id_relation;
        CREATE SPATIAL INDEX ON ZONE_NEIGHBORS( the_geom) ;  
    '''
    return script.replaceAll('map',prefix)
}

/**
 ** Function to create the script for the buildings
 **/
static void defineBuildingScript(prefix, options, tagKeys, tagValues, scriptFile,
                                 buildingTableName, bufferTableName){
    def uid = UUID.randomUUID().toString().replaceAll("-","")
    def script = ''
    script += "DROP TABLE IF EXISTS buildings_simp_raw_$uid; \n" +
            "CREATE TABLE buildings_simp_raw_$uid AS \n" +
            'SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), 2154) the_geom \n' +
            'FROM \n' +
            '    (SELECT \n' +
            '        (SELECT ST_ACCUM(the_geom) the_geom \n' +
            '        FROM \n' +
            '            (SELECT n.id_node, n.the_geom, wn.id_way idway \n' +
            '            FROM map_node n, map_way_node wn \n' +
            '            WHERE n.id_node = wn.id_node \n' +
            '            ORDER BY wn.node_order) \n' +
            '        WHERE  idway = w.id_way) the_geom ,w.id_way \n' +
            '    FROM map_way w, \n' +
            '        (SELECT DISTINCT id_way \n' +
            '        FROM map_way_tag wt, map_tag t \n' +
            '        WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\''
    tagKeys.eachWithIndex { it, i ->
        if (i==0) {
            script += it
        } else {
            script += '\',\'' + it
        }
    }
    script +='\')\n'
    if (tagValues != null) {
        script += 'AND value in (\''
        tagValues.eachWithIndex { it, i ->
            if (i == 0) {
                script += it
            } else {
                script += '\',\'' + it
            }
        }
        script += '\')'
    }
    script +=') b \n' +
            '    WHERE w.id_way = b.id_way) geom_table \n' +
            'WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) \n' +
            'AND ST_NUMGEOMETRIES(the_geom) > 3;\n' +
            'CREATE INDEX IF NOT EXISTS buildings_simp_raw_'+uid+'_index ON buildings_simp_raw_'+uid+'(id_way);\n'
    script += 'DROP TABLE IF EXISTS buildings_simp_'+uid+'; \n' +
            'CREATE TABLE buildings_simp_'+uid+' (the_geom polygon, id_way varchar'
    options.each {
        script += ', ' + it.value + ' varchar'
    }
    script += ') AS \n SELECT a.the_geom, a.id_way'
    options.eachWithIndex { it, i ->
        script += ', t' + i + '.' + it.value
    }
    script += '\nFROM buildings_simp_raw_'+uid+' a \n'
    options.eachWithIndex { it, i ->
        script += 'LEFT JOIN     \n' +
                '    (SELECT DISTINCT br.id_way, VALUE ' + it.value + ' \n' +
                '    FROM map_way_tag wt, map_tag t, buildings_simp_raw_'+uid+' br \n' +
                '    WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'' + it.key + '\') \n' +
                '    AND br.id_way = wt.id_way ) t' + i + ' \n' +
                'ON a.id_way = t' + i + '.id_way \n'
    }
    script += ';\nCREATE INDEX IF NOT EXISTS buildings_simp_'+uid+'_index ON buildings_simp_'+uid+' (id_way);\n' +
            'CREATE SPATIAL INDEX ON buildings_simp_'+uid+'(the_geom);\n' +
            'DROP TABLE IF EXISTS buildings_simp_raw_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_rel_way_'+uid+'; \n' +
            'CREATE TABLE buildings_rel_way_'+uid+' (id_relation varchar, id_way varchar, role varchar) AS \n' +
            'SELECT rt.id_relation, wm.id_way, wm.role \n' +
            'FROM map_relation_tag rt, map_tag t, map_way_member wm \n' +
            'WHERE rt.id_tag = t.id_tag AND t.tag_key IN (\''
    tagKeys.eachWithIndex { it, i ->
        if (i==0) {
            script += it
        } else {
            script += '\',\'' + it
        }
    }
    script +='\')\n'
    if (tagValues != null) {
        script += 'AND value in (\''
        tagValues.eachWithIndex { it, i ->
            if (i == 0) {
                script += it
            } else {
                script += '\',\'' + it
            }
        }
        script += '\')'
    }
    script += ' AND rt.id_relation = wm.id_relation; \n' +
            'CREATE INDEX IF NOT EXISTS buildings_rel_way_'+uid+'_index on buildings_rel_way_'+uid+' (id_way);\n' +
            'CREATE INDEX IF NOT EXISTS buildings_rel_way_'+uid+'_index2 on buildings_rel_way_'+uid+' (id_relation);\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_raw_'+uid+'; \n' +
            'CREATE TABLE buildings_rel_raw_'+uid+' AS \n' +
            'SELECT ST_LINEMERGE(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation, role\n' +
            '    FROM \n' +
            '        (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, role, id_way \n' +
            '        FROM      \n' +
            '            (SELECT \n' +
            '                (SELECT ST_ACCUM(the_geom) the_geom \n' +
            '                FROM \n' +
            '                    (SELECT n.id_node, n.the_geom, wn.id_way idway \n' +
            '                    FROM map_node n, map_way_node wn \n' +
            '                    WHERE n.id_node = wn.id_node ORDER BY wn.node_order) \n' +
            '                WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role \n' +
            '            FROM map_way w, buildings_rel_way_'+uid+' br \n' +
            '            WHERE w.id_way = br.id_way) geom_table where st_numgeometries(the_geom)>=2)\n' +
            '    GROUP BY id_relation, role;\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_raw2_'+uid+'; \n' +
            'CREATE TABLE buildings_rel_raw2_'+uid+' AS \n' +
            'SELECT * FROM ST_Explode(\'buildings_rel_raw_'+uid+'\');\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_raw3_'+uid+'; \n' +
            'CREATE TABLE buildings_rel_raw3_'+uid+' AS \n' +
            'SELECT ST_MAKEPOLYGON(the_geom) the_geom, id_relation, role\n' +
            'FROM buildings_rel_raw2_'+uid+'\n' +
            'WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom);\n'
    script += 'UPDATE buildings_rel_raw3_'+uid+' SET role =\n' +
            'CASE WHEN role = \'outline\' THEN \'outer\'\n' +
            '    ELSE role\n' +
            'END;\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_tot_'+uid+'; \n' +
            'CREATE TABLE buildings_rel_tot_'+uid+' AS \n' +
            'SELECT ST_difference(st_union(st_accum(to.the_geom)),st_union(st_accum(ti.the_geom))) as the_geom, to.id_relation \n' +
            'FROM      \n' +
            '    (SELECT the_geom, id_relation, role \n' +
            '    FROM buildings_rel_raw3_'+uid+' \n' +
            '    WHERE role = \'outer\') to,     \n' +
            '    (SELECT the_geom, id_relation, role \n' +
            '    FROM buildings_rel_raw3_'+uid+' \n' +
            '    WHERE role = \'inner\') ti \n' +
            'where ti.id_relation = to.id_relation\n' +
            'GROUP BY to.id_relation\n' +
            'UNION \n' +
            'SELECT a.the_geom, a.id_relation \n' +
            'FROM \n' +
            '    (SELECT the_geom, id_relation, role\n' +
            '    FROM buildings_rel_raw3_'+uid+'\n' +
            '    WHERE role = \'outer\') a\n' +
            'LEFT JOIN \n' +
            '    (SELECT the_geom, id_relation, role\n' +
            '    FROM buildings_rel_raw3_'+uid+'\n' +
            '    where role = \'inner\') b\n' +
            'ON a.id_relation = b.id_relation\n' +
            'WHERE b.id_relation IS NULL;\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_'+uid+'; \n' +
            'CREATE TABLE buildings_rel_'+uid+' AS \n' +
            'SELECT a.the_geom, a.id_relation'
    options.eachWithIndex { it, i ->
        script += ', t' + i + '.' + it.value
    }
    script += '\nFROM buildings_rel_tot_'+uid+' a \n'
    options.eachWithIndex { it, i ->
        script += 'LEFT JOIN     \n' +
                '    (SELECT DISTINCT br.id_relation, tag_value ' + it.value + ' \n' +
                '    FROM map_relation_tag rt, map_tag t, buildings_rel_tot_'+uid+' br \n' +
                '    WHERE rt.id_tag = t.id_tag AND t.tag_key IN (\'' + it.key + '\') \n' +
                '    AND br.id_relation = rt.id_relation ) t' + i + ' \n' +
                '    ON a.id_relation =  t' + i +'.id_relation \n'
    }
    script += """;
            CREATE SPATIAL INDEX ON buildings_rel_tot_$uid (the_geom);
            DROP TABLE IF EXISTS $buildingTableName;
            CREATE TABLE $buildingTableName AS
            SELECT ST_MAKEVALID(a.the_geom) the_geom, id_way"""
    options.each {
        script += ', ' + it.value
    }
    script += '\nFROM buildings_simp_'+uid+' a, '+bufferTableName+' b\n' +
            'WHERE ST_INTERSECTS(a.the_geom, b.the_geom) and a.the_geom && b.the_geom \n' +
            'UNION \n' +
            'SELECT ST_MAKEVALID(a.the_geom), id_relation'
    options.each {
        script += ', ' + it.value
    }
    script += '\nFROM buildings_rel_'+uid+' a, '+bufferTableName+' b\n' +
            'WHERE ST_INTERSECTS(a.the_geom, b.the_geom) and a.the_geom && b.the_geom;\n' +
            'DROP TABLE IF EXISTS buildings_rel_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_rel_raw_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_rel_raw2_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_rel_raw3_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_rel_tot_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_rel_way_'+uid+';\n' +
            'DROP TABLE IF EXISTS buildings_simp_'+uid+';\n' +
            'CREATE SPATIAL INDEX ON '+buildingTableName+'(the_geom);\n'
    scriptFile << script.replaceAll('map',prefix)
}

// TODO : add a new parameter to take into account the list of desired 'highway' tag values
static void defineRoadScript(prefix, options, tagKeys, tagValues, scriptFile
                      , roadTableName, filteringZoneTableName) {
    def script
    def uid = UUID.randomUUID().toString().replaceAll("-","")
    script = """
            DROP TABLE IF EXISTS roads_raw_$uid;
            CREATE TABLE roads_raw_$uid AS
            SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_way
            FROM
                (SELECT w.id_way,
                    (SELECT ST_ACCUM(the_geom) the_geom
                    FROM
                        (SELECT n.id_node, n.the_geom, wn.id_way idway
                        FROM map_node n, map_way_node wn
                        WHERE n.id_node = wn.id_node
                        ORDER BY wn.node_order)
                    WHERE  idway = w.id_way) the_geom
                FROM map_way w,
                    (SELECT DISTINCT id_way
                    FROM map_way_tag wt, map_tag t
                    WHERE wt.id_tag = t.id_tag
            """
    tagKeys.eachWithIndex { it, i ->
        if (i==0) {
            script += 'AND t.tag_key IN (\'' + it
        } else {
            script += '\',\'' + it
        }
    }
    script +='\')\n'
    if (tagValues != null) {
        script += ' AND value in (\''
        tagValues.eachWithIndex { it, i ->
            if (i == 0) {
                script += it
            } else {
                script += '\',\'' + it
            }
        }
        script += '\')'
    }
    script +=""") b
                WHERE w.id_way = b.id_way) geom_table
            WHERE ST_NUMGEOMETRIES(the_geom) >= 2;
            
            CREATE INDEX IF NOT EXISTS roads_raw_${uid}_index ON roads_raw_${uid}(id_way);
            CREATE SPATIAL INDEX ON roads_raw_${uid}(the_geom);
            """
    script += """
            CREATE TABLE roads_raw_filtered_${uid} as
            select st_intersection(a.the_geom, b.the_geom) the_geom, a.id_way
            from roads_raw_${uid} a, ${filteringZoneTableName} b
            where ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.the_geom && b.the_geom;
            CREATE INDEX IF NOT EXISTS roads_raw_filtered_${uid}_index ON roads_raw_filtered_${uid}(id_way);
            DROP TABLE IF EXISTS roads_raw_${uid};
            """
    script +='DROP TABLE IF EXISTS '+roadTableName+';\n' +
            'CREATE TABLE '+roadTableName+' AS\n    SELECT a.the_geom, a.id_way'
    options.eachWithIndex{ it, i ->
        script +=', t'+i+'."'+it.value + '"'
    }
    script +='\n    FROM roads_raw_filtered_'+uid+' a\n'
    options.eachWithIndex { it, i ->
        script +='        LEFT JOIN\n' +
                '            (SELECT DISTINCT wt.id_way, VALUE "'+it.value+'"\n' +
                '            FROM map_way_tag wt, map_tag t\n' +
                '            WHERE wt.id_tag = t.id_tag\n' +
                '            AND t.tag_key IN (\''+it.key+'\')\n' +
                '            ) t'+i+'\n' +
                '        ON a.id_way = t'+i+'.id_way\n'
    }
    script+= ';\nDROP TABLE IF EXISTS roads_raw_filtered_'+uid+';\n'+
            'CREATE SPATIAL INDEX ON '+roadTableName+'(the_geom);'
    scriptFile << script.replaceAll('map',prefix)

}

static void defineRailScript(prefix, options, tagKeys, tagValues, scriptFile,
                      railTableName, filteringZoneTableName) {
    def script
    def uid = UUID.randomUUID().toString().replaceAll("-","")
    script = """
            DROP TABLE IF EXISTS rails_raw_${uid};
            CREATE TABLE rails_raw_${uid} AS
            SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom
            FROM
                (SELECT w.id_way, val,
                    (SELECT ST_ACCUM(the_geom) the_geom
                    FROM
                        (SELECT n.id_node, n.the_geom, wn.id_way idway
                        FROM map_node n, map_way_node wn
                        WHERE n.id_node = wn.id_node
                        ORDER BY wn.node_order)
                    WHERE  idway = w.id_way) the_geom
                FROM map_way w,
                    (SELECT DISTINCT id_way, value as val
                    FROM map_way_tag wt, map_tag t
                    WHERE wt.id_tag = t.id_tag
            """
    tagKeys.eachWithIndex { it, i ->
        if (i==0) {
            script += 'AND t.tag_key IN (\''+ it
        } else {
            script += '\',\'' + it
        }
    }
    script +='\')\n'
    if (tagValues != null) {
        script += ' AND value in (\''
        tagValues.eachWithIndex { it, i ->
            if (i == 0) {
                script += it
            } else {
                script += '\',\'' + it
            }
        }
        script += '\')'
    }

    script +=""") b
                WHERE w.id_way = b.id_way) geom_table
            WHERE ST_NUMGEOMETRIES(the_geom) >= 2;
            CREATE INDEX IF NOT EXISTS rails_raw_${uid}_index ON rails_raw_${uid}(id_way);
            
            DROP TABLE IF EXISTS rails_raw_filtered_${uid};
            CREATE TABLE rails_raw_filtered_${uid} as
            select a.*
            from rails_raw_${uid} a, ${filteringZoneTableName} b
            where ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.the_geom && b.the_geom;
            CREATE INDEX IF NOT EXISTS rails_raw_filtered_${uid}_index ON rails_raw_filtered_${uid}(id_way);            
            """
    script +="DROP TABLE IF EXISTS ${railTableName};\n" +
            "CREATE TABLE ${railTableName} AS\n    SELECT a.id_way, a.the_geom"
    options.eachWithIndex{ it, i ->
        script +=', t'+i+'."'+it.value +'"'
    }
    script +="\n    FROM rails_raw_filtered_${uid} a\n"
    options.eachWithIndex { it, i ->
        script +='        LEFT JOIN\n' +
                '            (SELECT DISTINCT wt.id_way, VALUE "'+it.value+'"\n' +
                '            FROM map_way_tag wt, map_tag t\n' +
                '            WHERE wt.id_tag = t.id_tag\n' +
                '            AND t.tag_key IN (\''+it.key+'\')\n' +
                '            ) t'+i+'\n' +
                '        ON a.id_way = t'+i+'.id_way\n'
    }
    script+= ";\nDROP TABLE IF EXISTS rails_raw_${uid};"
    script+= "\nDROP TABLE IF EXISTS rails_raw_filtered_${uid};"
    scriptFile << script.replaceAll('map',prefix)

}

static void defineVegetationScript(String prefix, def options, def tagKeys, def tagValues, def scriptPath,
                            def vegetTableName, def filteringZoneTableName) {
    def script = ''
    script += """
            DROP TABLE IF EXISTS veget_simp_raw;
            CREATE TABLE veget_simp_raw AS
            SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), 2154) the_geom, val as type
            FROM
            (SELECT
                    (SELECT ST_ACCUM(the_geom) the_geom
                            FROM
                                    (SELECT n.id_node, n.the_geom, wn.id_way idway
                                            FROM map_node n, map_way_node wn
                                            WHERE n.id_node = wn.id_node
                                            ORDER BY wn.node_order)
                            WHERE  idway = w.id_way) the_geom ,w.id_way, val
            FROM map_way w,
            (SELECT DISTINCT id_way, value as val
            FROM map_way_tag wt, map_tag t
            WHERE wt.id_tag = t.id_tag
            """
    tagKeys.eachWithIndex { it, i ->
        if (i==0) {
            script += 'AND t.tag_key IN (\'' + it
        } else {
            script += '\',\'' + it
        }
    }
    script +='\')\n'
    if (tagValues != null) {
        script += 'AND value in (\''
        tagValues.eachWithIndex { it, i ->
            if (i == 0) {
                script += it
            } else {
                script += '\',\'' + it
            }
        }
        script += '\')'
    }

    script += """) b
            WHERE w.id_way = b.id_way) geom_table
            WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom))
            AND ST_NUMGEOMETRIES(the_geom) > 3;
            CREATE INDEX IF NOT EXISTS veget_simp_raw_index ON veget_simp_raw(id_way);
            
            DROP TABLE IF EXISTS veget_simp;
            """
    script += 'CREATE TABLE veget_simp AS \n SELECT a.the_geom, a.id_way'
    options.eachWithIndex { it, i ->
        script += ', t' + i + '."' + it.value+'"'
    }
    script += '\nFROM veget_simp_raw a \n'
    options.eachWithIndex { it, i ->
        script += 'LEFT JOIN     \n' +
                '    (SELECT DISTINCT br.id_way, VALUE AS "' + it.value + '"\n' +
                '    FROM map_way_tag wt, map_tag t, veget_simp_raw br \n' +
                '    WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'' + it.key + '\') \n' +
                '    AND br.id_way = wt.id_way ) t' + i + ' \n' +
                'ON a.id_way = t' + i + '.id_way\n'
    }
    script += ''';\n\n
            CREATE INDEX IF NOT EXISTS veget_simp_index ON veget_simp (id_way);
            DROP TABLE IF EXISTS veget_simp_raw;
            DROP TABLE IF EXISTS veget_rel_way;
            CREATE TABLE veget_rel_way (id_relation varchar, id_way varchar, role varchar) AS
            SELECT rt.id_relation, wm.id_way, wm.role
            FROM map_relation_tag rt, map_tag t, map_way_member wm
            WHERE rt.id_tag = t.id_tag
            '''
    tagKeys.eachWithIndex { it, i ->
        if (i==0) {
            script += 'AND t.tag_key IN (\'' + it
        } else {
            script += '\',\'' + it
        }
    }
    script +='\')\n'
    if (tagValues != null) {
        script += '            AND tag_value in (\''
        tagValues.eachWithIndex { it, i ->
            if (i == 0) {
                script += it
            } else {
                script += '\',\'' + it
            }
        }
        script += '\')'
    }

    script += '''
            AND rt.id_relation = wm.id_relation;
            CREATE INDEX IF NOT EXISTS veget_rel_way_index on veget_rel_way (id_way);
            CREATE INDEX IF NOT EXISTS veget_rel_way_index2 on veget_rel_way (id_relation);
            '''
    script += '''
            DROP TABLE IF EXISTS veget_rel_raw;
            CREATE TABLE veget_rel_raw AS
            SELECT ST_LINEMERGE(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation, role
                FROM
                    (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, role, id_way
                    FROM
                        (SELECT
                            (SELECT ST_ACCUM(the_geom) the_geom
                            FROM
                                (SELECT n.id_node, n.the_geom, wn.id_way idway
                                FROM map_node n, map_way_node wn
                                WHERE n.id_node = wn.id_node ORDER BY wn.node_order)
                            WHERE  idway = w.id_way) the_geom, w.id_way, br.id_relation, br.role
                        FROM map_way w, veget_rel_way br
                        WHERE w.id_way = br.id_way) geom_table where st_numgeometries(the_geom)>=2)
                GROUP BY id_relation, role;
            
            DROP TABLE IF EXISTS veget_rel_raw2;
            CREATE TABLE veget_rel_raw2 AS
            SELECT * FROM ST_Explode(\'veget_rel_raw\');
            
            DROP TABLE IF EXISTS veget_rel_raw3;
            CREATE TABLE veget_rel_raw3 AS
            SELECT ST_MAKEPOLYGON(the_geom) the_geom, id_relation, role
            FROM veget_rel_raw2
            WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom);
            
            UPDATE veget_rel_raw3 SET role =
            CASE WHEN role = 'outline' THEN 'outer'
                ELSE role
            END;
            
            DROP TABLE IF EXISTS veget_rel_tot;
            CREATE TABLE veget_rel_tot AS 
            SELECT ST_difference(st_union(st_accum(to.the_geom)),st_union(st_accum(ti.the_geom))) as the_geom, to.id_relation
            FROM 
                (SELECT the_geom, id_relation, role
                FROM veget_rel_raw3
                WHERE role = 'outer') to,
                (SELECT the_geom, id_relation, role
                FROM veget_rel_raw3
                WHERE role = \'inner\') ti
            where ti.id_relation = to.id_relation
            GROUP BY to.id_relation
            UNION
            SELECT a.the_geom, a.id_relation
            FROM
                (SELECT the_geom, id_relation, role
                FROM veget_rel_raw3
                WHERE role = \'outer\') a
            LEFT JOIN
                (SELECT the_geom, id_relation, role
                FROM veget_rel_raw3
                where role = \'inner\') b
            ON a.id_relation = b.id_relation
            WHERE b.id_relation IS NULL;
            
            DROP TABLE IF EXISTS veget_rel;
            '''
    script += 'CREATE TABLE veget_rel AS \n' +
            'SELECT a.the_geom, a.id_relation'
    options.eachWithIndex { it, i ->
        script += ', t' + i + '."' + it.value + '"'
    }
    script += '\nFROM veget_rel_tot a \n'
    options.eachWithIndex { it, i ->
        script += 'LEFT JOIN     \n' +
                '    (SELECT DISTINCT br.id_relation, tag_value as "' + it.value + '"\n' +
                '    FROM map_relation_tag rt, map_tag t, veget_rel_tot br \n' +
                '    WHERE rt.id_tag = t.id_tag AND t.tag_key IN (\'' + it.key + '\') \n' +
                '    AND br.id_relation = rt.id_relation ) t' + i + ' \n' +
                '    ON a.id_relation =  t' + i +'.id_relation \n'
    }
    script += ';\nDROP TABLE IF EXISTS INPUT_VEGET; \n' +
            'CREATE TABLE INPUT_VEGET\n'
    script += ' AS\nSELECT id_way as id_source, st_intersection(ST_MAKEVALID(a.the_geom),b.the_geom) the_geom'
    options.each {
        script += ', "' + it.value + '"'
    }
    script += '\nFROM veget_simp a, zone_extended b \nUNION \nSELECT id_relation, st_intersection(ST_MAKEVALID(a.the_geom),b.the_geom)'
    options.each {
        script += ', "' + it.value + '"'
    }
    script += '''
            FROM veget_rel a, zone_extended b;
            DROP TABLE IF EXISTS veget_rel;
            DROP TABLE IF EXISTS veget_rel_raw;
            DROP TABLE IF EXISTS veget_rel_raw2;
            DROP TABLE IF EXISTS veget_rel_raw3;
            DROP TABLE IF EXISTS veget_rel_tot;
            DROP TABLE IF EXISTS veget_rel_way;
            DROP TABLE IF EXISTS veget_simp;
            '''
    def scriptFile = new File(scriptPath)
    scriptFile << script.replaceAll('map',prefix)

}

static void defineHydroScript (def prefix, def options, def tags, def scriptPath,
                       def hydroTableName, def filteringZoneTableName) {
    def script = ''
    script += '''
            DROP TABLE IF EXISTS hydro_simp_raw;
            CREATE TABLE hydro_simp_raw AS
            SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKEPOLYGON(ST_MAKELINE(the_geom)), 4326), 2154) the_geom, val as type
            FROM
            (SELECT
                    (SELECT ST_ACCUM(the_geom) the_geom
                        FROM
                            (SELECT n.id_node, n.the_geom, wn.id_way idway
                                FROM map_node n, map_way_node wn
                                WHERE n.id_node = wn.id_node
                                ORDER BY wn.node_order)
                        WHERE  idway = w.id_way) the_geom ,w.id_way, val
            FROM map_way w JOIN
                (SELECT DISTINCT id_way, value as val
                    FROM map_way_tag wt 
                        JOIN map_tag t ON (wt.id_tag = t.id_tag)
            '''
    tags.eachWithIndex { it, i ->
        if (i == 0) {
            script += "                    WHERE (t.tag_key = '" + it.key + "'"
        } else {
            script += "\n                    OR (t.tag_key = '" + it.key + "'"
        }
        if (it.value == []) {
            script += ")"
        } else {
            it.value.eachWithIndex { token, j ->
                if (j == 0) {
                    script += "\n                    AND value in ('" + token
                } else {
                    script += "','" + token
                }
            }
            script += "'))"
        }
    }

    script +='''
            ) b
            WHERE w.id_way = b.id_way) geom_table
            WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom))
            AND ST_NUMGEOMETRIES(the_geom) > 3;
            CREATE INDEX IF NOT EXISTS hydro_simp_raw_index ON hydro_simp_raw(id_way);
            
            DROP TABLE IF EXISTS hydro_simp_raw_filtered;
            CREATE TABLE hydro_simp_raw_filtered as
            select a.*
            from hydro_simp_raw a, zone_extended b
            where ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.the_geom && b.the_geom; 
                       
            DROP TABLE IF EXISTS hydro_simp;
            CREATE TABLE hydro_simp AS
            SELECT a.the_geom, a.id_way'''
    options.eachWithIndex {it, i ->
        script += ', t' + i + '."' + it.value + '"'
    }
    script += '\n             FROM hydro_simp_raw_filtered a'
    options.eachWithIndex {it, i ->
        script += '\n            LEFT JOIN     \n' +
                '                (SELECT DISTINCT br.id_way, VALUE AS "' + it.value + '"\n' +
                '                FROM map_way_tag wt, map_tag t, hydro_simp_raw br \n' +
                '                WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'' + it.key + '\') \n' +
                '                AND br.id_way = wt.id_way ) t'+i+' \n' +
                '            ON a.id_way = t'+i+'.id_way'
    }
    script += '''
            ;
            CREATE INDEX IF NOT EXISTS hydro_simp_index ON hydro_simp (id_way);
            DROP TABLE IF EXISTS hydro_simp_raw;

            DROP TABLE IF EXISTS hydro_rel_way;
            CREATE TABLE hydro_rel_way (id_relation varchar, id_way varchar, role varchar) AS
            SELECT rt.id_relation, wm.id_way, wm.role
            FROM map_relation_tag rt
                JOIN map_tag t ON (rt.id_tag = t.id_tag)
                JOIN map_way_member wm ON (rt.id_relation = wm.id_relation)
            '''
    tags.eachWithIndex { it, i ->
        if (i == 0) {
            script += "         WHERE (t.tag_key = '" + it.key + "'"
        } else {
            script += "\n                    OR (t.tag_key = '" + it.key + "'"
        }
        if (it.value == []) {
            script += ")"
        } else {
            it.value.eachWithIndex { token, j ->
                if (j == 0) {
                    script += "\n                    AND tag_value in ('" + token
                } else {
                    script += "','" + token
                }
            }
            script += "'))"
        }
    }

    script +='''
            ;
            CREATE INDEX IF NOT EXISTS hydro_rel_way_index on hydro_rel_way (id_way);
            CREATE INDEX IF NOT EXISTS hydro_rel_way_index2 on hydro_rel_way (id_relation);
            
            DROP TABLE IF EXISTS hydro_rel_raw;
            CREATE TABLE hydro_rel_raw AS
            SELECT ST_LINEMERGE(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation, role
                FROM
                    (SELECT ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom, id_relation, role, id_way
                    FROM
                        (SELECT
                            (SELECT ST_ACCUM(the_geom) the_geom
                            FROM
                                (SELECT n.id_node, n.the_geom, wn.id_way idway
                                FROM map_node n
                                    JOIN map_way_node wn ON (n.id_node = wn.id_node)
                                 ORDER BY wn.node_order)
                            WHERE  idway = w.id_way) the_geom, 
                            w.id_way, br.id_relation, br.role
                        FROM map_way w
                            JOIN hydro_rel_way br ON (w.id_way = br.id_way)) geom_table 
                    WHERE st_numgeometries(the_geom)>=2)
                GROUP BY id_relation, role;
            
            DROP TABLE IF EXISTS hydro_rel_raw2;
            CREATE TABLE hydro_rel_raw2 AS
            SELECT * FROM ST_Explode('hydro_rel_raw');

            DROP TABLE IF EXISTS hydro_rel_raw2_filtered;
            CREATE TABLE hydro_rel_raw2_filtered as
            select a.*
            from hydro_rel_raw2 a, zone_extended b
            where ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.the_geom && b.the_geom; 
            
            DROP TABLE IF EXISTS hydro_rel_raw3;
            CREATE TABLE hydro_rel_raw3 AS
            SELECT ST_MAKEPOLYGON(the_geom) the_geom, id_relation, role
            FROM hydro_rel_raw2_filtered
            WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom);
            
            UPDATE hydro_rel_raw3 SET role =
            CASE WHEN role = 'outline' THEN 'outer\'
                ELSE role
            END;
            
            DROP TABLE IF EXISTS hydro_rel_tot;
            CREATE TABLE hydro_rel_tot AS 
            SELECT ST_difference(st_union(st_accum(to.the_geom)),st_union(st_accum(ti.the_geom))) as the_geom, to.id_relation
            FROM 
                (SELECT the_geom, id_relation, role
                FROM hydro_rel_raw3
                WHERE role = 'outer') to,
                (SELECT the_geom, id_relation, role
                FROM hydro_rel_raw3
                WHERE role = 'inner') ti
            where ti.id_relation = to.id_relation
            GROUP BY to.id_relation
            UNION
            SELECT a.the_geom, a.id_relation
            FROM
                (SELECT the_geom, id_relation, role
                FROM hydro_rel_raw3
                WHERE role = 'outer') a
            LEFT JOIN
                (SELECT the_geom, id_relation, role
                FROM hydro_rel_raw3
                where role = 'inner') b
            ON a.id_relation = b.id_relation
            WHERE b.id_relation IS NULL;
            
            DROP TABLE IF EXISTS hydro_rel;
            CREATE TABLE hydro_rel AS 
            SELECT a.the_geom, a.id_relation 
            '''
    options.eachWithIndex {it, i ->
        script += ', t' + i + '."' + it.value + '"'
    }
    script += '             FROM hydro_rel_tot a'
    options.eachWithIndex {it, i ->
        script += '            LEFT JOIN     \n' +
                '                (SELECT DISTINCT br.id_relation, tag_value AS "' + it.value + '"\n' +
                '                FROM map_relation_tag rt, map_tag t, hydro_rel_tot br \n' +
                '                WHERE rt.id_tag = t.id_tag AND t.tag_key IN (\'' + it.key + '\') \n' +
                '                AND br.id_relation = rt.id_relation ) t'+i+' \n' +
                '            ON a.id_relation = t'+i+'.id_relation'
    }
    script += ';\nDROP TABLE IF EXISTS INPUT_HYDRO; \n' +
            'CREATE TABLE INPUT_HYDRO\n'
    script += ' AS\nSELECT id_way as id_source, st_intersection(ST_MAKEVALID(a.the_geom), b.the_geom)  the_geom'
    options.each {
        script += ', "' + it.value + '"'
    }
    script += '\nFROM hydro_simp a, zone_extended b \nUNION \nSELECT id_relation, st_intersection(ST_MAKEVALID(a.the_geom), b.the_geom)'
    options.each {
        script += ', "' + it.value + '"'
    }
    script += '''
            FROM hydro_rel a,zone_extended b;
            DROP TABLE IF EXISTS hydro_rel;
            DROP TABLE IF EXISTS hydro_rel_raw;
            DROP TABLE IF EXISTS hydro_rel_raw2;
            DROP TABLE IF EXISTS hydro_rel_raw2_filtered;
            DROP TABLE IF EXISTS hydro_rel_raw3;
            DROP TABLE IF EXISTS hydro_rel_tot;
            DROP TABLE IF EXISTS hydro_rel_way;
            DROP TABLE IF EXISTS hydro_simp;
            DROP TABLE IF EXISTS HYDRO_SIMP_RAW_FILTERED;
            '''
    def scriptFile = new File(scriptPath)
    scriptFile << script.replaceAll('map',prefix)
}
