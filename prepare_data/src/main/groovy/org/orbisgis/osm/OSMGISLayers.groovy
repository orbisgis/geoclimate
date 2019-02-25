package org.orbisgis.osm

/**
 * OSMGISLayers is the main script to build a set of OSM GIS layers based on OSM data.
 * It uses the overpass api to download data
 * It builds a sql script file to create the layers table with the geometries and the attributes given as parameters
 * It produces a shapefile for each layer
 * Produced layers : buildings, roads
**/

import org.orbisgis.datamanager.h2gis.H2GIS

//Parametrized variables
def inseeCode = '56243'
def buildingOptions = ['height','building:levels','building']
def roadOptions = ['width','highway', 'surface', 'sidewalk']
def railOptions = ['width','category']

//The sql script to create the buildings from osm model
// ELS : for the moment, I've been putting everything in the tmp dir so that it can be both used on Windows and Linux without changing a single line
def tmpPath = System.getProperty("java.io.tmpdir")

def outputFolder = tmpPath + File.separator + "osm_gis_" + System.currentTimeMillis()
def buildingsScriptPath = outputFolder + File.separator + 'createOSMBuildingTable.sql'
def roadsScriptPath = outputFolder + File.separator + 'createOSMRoadTable.sql'
def railsScriptPath = outputFolder + File.separator + 'createOSMRailTable.sql'

def cityFilePath = outputFolder + File.separator + 'city.shp'
def extCityFilePath = outputFolder + File.separator + 'extended_city.shp'
def cityNeighborsFilePath = outputFolder + File.separator + 'city_neighbors.shp'
def buildingsFilePath = outputFolder + File.separator + 'buildings.shp'
def roadsFilePath = outputFolder + File.separator + 'roads.shp'
def railsFilePath = outputFolder + File.separator + 'rails.shp'

//Create a new outputFolder each time
//This folder is used to store the osm file and the H2GIS database
def tmpFolder = new File(outputFolder)
tmpFolder.deleteDir()
tmpFolder.mkdir()


//The tmp file that stores the osm data for a commune
def outputOSMFile = new File(outputFolder + File.separator + "city.osm")
//the tmp file that stores the script to build the layers tables
def buildingsScriptFile = new File(buildingsScriptPath)
def roadsScriptFile = new File(roadsScriptPath)
def railsScriptFile = new File(railsScriptPath)

//City download : relation, ways and nodes corresponding to the targeted city limit
def initQuery="[timeout:900];(relation[\"ref:INSEE\"=\"$inseeCode\"][\"boundary\"=\"administrative\"][\"admin_level\"=\"8\"];>;);out;"
//Download the corresponding OSM data
executeOverPassQuery(initQuery, outputOSMFile)

if (outputOSMFile.exists()) {
    def h2GIS =H2GIS.open([databaseName: outputFolder])
    h2GIS.load(outputOSMFile.absolutePath, 'city', true)
    // Create the polygon corresponding to the city limit and its extended area
    h2GIS.execute citySQLScript('city',inseeCode)

    //define the coordinates of the extended city bbox
    h2GIS.select('''
        ST_XMin(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as minLon, 
        ST_XMax(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as maxLon,
        ST_YMin(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as minLat, 
        ST_YMax(ST_TRANSFORM(ST_SETSRID(the_geom,2154), 4326)) as maxLat
    ''').from('EXTENDED_CITY').getTable().eachRow{ row -> bbox = "$row.minLat, $row.minLon, $row.maxLat, $row.maxLon"}
    h2GIS.save('CITY', cityFilePath)
    h2GIS.save('EXTENDED_CITY', extCityFilePath)

    println('City and Extended City OK')

    //Download all the osm data concerning the extended city bbox
    def outputOSMFile2 = new File(outputFolder + File.separator + "extendedCity.osm")
    def queryURLExtendedCity  = "[bbox:$bbox];(node;way;relation;);out;"
    executeOverPassQuery(queryURLExtendedCity, outputOSMFile2)

    //If the osm file is downloaded do the job
    if (outputOSMFile2.exists()) {

        //Import the OSM file
        h2GIS.load(outputOSMFile2.absolutePath, "ext_city", true)

        //Create the city_neighbors table and save it in the targeted shapefile
        //Build the overpass query to retrieve all the information about the neighbours cities limits
        def queryNeighbours ='(('
        def res = h2GIS.select ('id_relation').from('ext_city_relation_tag rt JOIN ext_city_tag t ON (rt.id_tag = t.id_tag)').where ("tag_key = 'admin_level' AND tag_value = '8'")
        res.getTable().eachRow{row -> queryNeighbours += "relation($row.id_relation);"}
        queryNeighbours += ");>;);out;"
        // Retrieve the corresponding informations and store it in the h2GIS DB
        def outputOSMFile3 = new File(outputFolder + File.separator + "cityNeighbours.osm")
        executeOverPassQuery(queryNeighbours, outputOSMFile3)
        if (outputOSMFile3.exists()) {
            h2GIS.load(outputOSMFile3.absolutePath, "neighbor", true)
            h2GIS.execute cityNeighborsSQLScript("neighbor")
            h2GIS.save('CITY_NEIGHBORS',cityNeighborsFilePath)
        } else {
            println "Cannot find OSM data for the city neighbours"
        }

        //Create the buildings table and save it in the targeted shapefile
        defineBuildingScript("ext_city",buildingOptions, buildingsScriptFile)
        h2GIS.executeScript(buildingsScriptPath)
        h2GIS.save('BUILDINGS', buildingsFilePath)

        //Create the roads table and save it in the targeted shapefile
        defineRoadScript("ext_city",roadOptions, roadsScriptFile)
        h2GIS.executeScript(roadsScriptPath)
        h2GIS.save('ROADS', roadsFilePath)

        //Create the rails table and save it in the targeted shapefile
        defineRailScript("ext_city",railOptions, railsScriptFile)
        h2GIS.executeScript(railsScriptPath)
        h2GIS.save('RAILS', railsFilePath)

        println "The shape files are ready in folder $outputFolder"

    } else {
        println "Cannot find OSM data on the requested area"
    }

} else {
    println "Cannot find OSM data on the requested area"
}//Download the osm data according to an insee code



//tmpFolder.deleteDir()
//println("Folder deleted")



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

    println "Executing query... $query"
    //Save the result in a file
    if (connection.responseCode == 200) {
        println "Downloading the OSM data from overpass api"
        outputOSMFile << connection.inputStream
        return true
    } else {
        println "Cannot execute the query"
        return false
    }
}

/**
 ** Function to create the script for the city
 **/
def citySQLScript(def prefix, def inseeCode) {
    def script = 'DROP TABLE IF EXISTS CITY;\n' +
            'CREATE TABLE CITY AS\n' +
            '        SELECT '+ inseeCode + ', ST_Polygonize(ST_UNION(ST_ACCUM(the_geom))) the_geom, id_relation'
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
    
        -- Generation of a rectangular area (bbox) around the studied city
        DROP TABLE IF EXISTS EXTENDED_CITY;
        CREATE TABLE EXTENDED_CITY AS
        SELECT ST_EXPAND( the_geom , 1000) as the_geom
        FROM city;
        CREATE SPATIAL INDEX ON EXTENDED_CITY( the_geom) ;
    '''
    return script.replaceAll('map',prefix)
}

/**
 ** Function to create the script for the city neighbors
 **/
def cityNeighborsSQLScript(def prefix){
    def script = '''
        DROP TABLE IF EXISTS CITY_NEIGHBORS;
        CREATE TABLE CITY_NEIGHBORS AS
        SELECT a.id_city, st_polygonize(st_union(b.the_geom)) the_geom
        from (
                select tag_value as id_city, a.id_relation
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
        CREATE SPATIAL INDEX ON CITY_neighbors( the_geom) ;  
    '''
    return script.replaceAll('map',prefix)
}

/**
 ** Function to create the script for the buildings
 **/
def defineBuildingScript(def prefix, def options, def buildings_script){
    def script = ''
    def tags =[]
    options.each {
        tags.add(it.toString().replace(':','_'))
    }
    script = 'CREATE INDEX IF NOT EXISTS map_node_index on map_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index on map_way_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index2 on map_way_node(node_order);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index3 ON map_way_node(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_index on map_way(id_way); \n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_id_index on map_way_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_va_index on map_way_tag(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_id_index on map_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_key_index on map_tag(tag_key); \n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index2 ON map_relation_tag(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(tag_value);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_rel_index ON map_relation(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_member_index ON map_way_member(id_relation);\n'
    script += 'DROP TABLE IF EXISTS buildings_simp_raw; \n' +
            'CREATE TABLE buildings_simp_raw AS \n' +
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
            '        WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'building\')) b \n' +
            '    WHERE w.id_way = b.id_way) geom_table \n' +
            'WHERE ST_GEOMETRYN(the_geom, 1) = ST_GEOMETRYN(the_geom, ST_NUMGEOMETRIES(the_geom)) \n' +
            'AND ST_NUMGEOMETRIES(the_geom) > 3;\n' +
            'CREATE INDEX IF NOT EXISTS buildings_simp_raw_index ON buildings_simp_raw(id_way);\n'
    script += 'DROP TABLE IF EXISTS buildings_simp; \n' +
            'CREATE TABLE buildings_simp (the_geom polygon, id_way varchar'
    options.eachWithIndex {it, i ->
        script += ', ' + tags[i] + ' varchar'
    }
    script += ') AS \n SELECT a.the_geom, a.id_way'
     options.eachWithIndex { it, i ->
         script += ', t' + i + '.' + tags[i]
     }
    script += '\nFROM buildings_simp_raw a \n'
    options.eachWithIndex { it, i ->
        script += 'LEFT JOIN     \n' +
                '    (SELECT DISTINCT br.id_way, VALUE ' + tags[i] + ' \n' +
                '    FROM map_way_tag wt, map_tag t, buildings_simp_raw br \n' +
                '    WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'' + it + '\') \n' +
                '    AND br.id_way = wt.id_way ) t' + i + ' \n' +
                'ON a.id_way = t' + i + '.id_way \n'
    }
    script += ';\nCREATE INDEX IF NOT EXISTS buildings_simp_index ON buildings_simp (id_way);\n' +
            'DROP TABLE IF EXISTS buildings_simp_raw;\n' +
            'DROP TABLE IF EXISTS buildings_rel_way; \n' +
            'CREATE TABLE buildings_rel_way (id_relation varchar, id_way varchar, role varchar) AS \n' +
            'SELECT rt.id_relation, wm.id_way, wm.role \n' +
            'FROM map_relation_tag rt, map_tag t, map_way_member wm \n' +
            'WHERE rt.id_tag = t.id_tag AND t.tag_key IN (\'building\') AND rt.id_relation = wm.id_relation; \n' +
            'CREATE INDEX IF NOT EXISTS buildings_rel_way_index on buildings_rel_way (id_way);\n' +
            'CREATE INDEX IF NOT EXISTS buildings_rel_way_index2 on buildings_rel_way (id_relation);\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_raw; \n' +
            'CREATE TABLE buildings_rel_raw AS \n' +
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
            '            FROM map_way w, buildings_rel_way br \n' +
            '            WHERE w.id_way = br.id_way) geom_table where st_numgeometries(the_geom)>=2)\n' +
            '    GROUP BY id_relation, role;\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_raw2; \n' +
            'CREATE TABLE buildings_rel_raw2 AS \n' +
            'SELECT * FROM ST_Explode(\'buildings_rel_raw\');\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_raw3; \n' +
            'CREATE TABLE buildings_rel_raw3 AS \n' +
            'SELECT ST_MAKEPOLYGON(the_geom) the_geom, id_relation, role\n' +
            'FROM buildings_rel_raw2\n' +
            'WHERE ST_STARTPOINT(the_geom) = ST_ENDPOINT(the_geom);\n'
    script += 'UPDATE buildings_rel_raw3 SET role =\n' +
            'CASE WHEN role = \'outline\' THEN \'outer\'\n' +
            '    ELSE role\n' +
            'END;\n'
    script += 'DROP TABLE IF EXISTS buildings_rel_tot; \n' +
            'CREATE TABLE buildings_rel_tot AS \n' +
            'SELECT ST_difference(st_union(st_accum(to.the_geom)),st_union(st_accum(ti.the_geom))) as the_geom, to.id_relation \n' +
            'FROM      \n' +
            '    (SELECT the_geom, id_relation, role \n' +
            '    FROM buildings_rel_raw3 \n' +
            '    WHERE role = \'outer\') to,     \n' +
            '    (SELECT the_geom, id_relation, role \n' +
            '    FROM buildings_rel_raw3 \n' +
            '    WHERE role = \'inner\') ti \n' +
            'where ti.id_relation = to.id_relation\n' +
            'GROUP BY to.id_relation\n' +
            'UNION \n' +
            'SELECT a.the_geom, a.id_relation \n' +
            'FROM \n' +
            '    (SELECT the_geom, id_relation, role\n' +
            '    FROM buildings_rel_raw3\n' +
            '    WHERE role = \'outer\') a\n' +
            'LEFT JOIN \n' +
            '    (SELECT the_geom, id_relation, role\n' +
            '    FROM buildings_rel_raw3\n' +
            '    where role = \'inner\') b\n' +
            'ON a.id_relation = b.id_relation\n' +
            'WHERE b.id_relation IS NULL;\n'
    script += 'DROP TABLE IF EXISTS buildings_rel; \n' +
            'CREATE TABLE buildings_rel AS \n' +
            'SELECT a.the_geom, a.id_relation'
    options.eachWithIndex { it, i ->
        script += ', t' + i + '.' + tags[i]
    }
    script += '\nFROM buildings_rel_tot a \n'
    options.eachWithIndex { it, i ->
        script += 'LEFT JOIN     \n' +
                '    (SELECT DISTINCT br.id_relation, tag_value ' + tags[i] + ' \n' +
                '    FROM map_relation_tag rt, map_tag t, buildings_rel_tot br \n' +
                '    WHERE rt.id_tag = t.id_tag AND t.tag_key IN (\'' + it + '\') \n' +
                '    AND br.id_relation = rt.id_relation ) t' + i + ' \n' +
                '    ON a.id_relation =  t' + i +'.id_relation \n'
    }
    script += ';\nDROP TABLE IF EXISTS buildings; \n' +
            'CREATE TABLE buildings (id SERIAL, id_source varchar, the_geom GEOMETRY'
    tags.each {
        script += ', ' + it + ' VARCHAR'
    }
    script += ') AS\nSELECT NULL, id_way, ST_MAKEVALID(the_geom)'
    tags.each {
        script += ', ' + it
    }
     script += '\nFROM buildings_simp \nUNION \nSELECT NULL, id_relation, ST_MAKEVALID(the_geom)'
    tags.each {
        script += ', ' + it
    }
    script += '\nFROM buildings_rel;\n' +
            'DROP TABLE IF EXISTS buildings_rel;\n' +
            'DROP TABLE IF EXISTS buildings_rel_raw;\n' +
            'DROP TABLE IF EXISTS buildings_rel_raw2;\n' +
            'DROP TABLE IF EXISTS buildings_rel_raw3;\n' +
            'DROP TABLE IF EXISTS buildings_rel_tot;\n' +
            'DROP TABLE IF EXISTS buildings_rel_way;\n' +
            'DROP TABLE IF EXISTS buildings_simp;\n'
    buildings_script << script.replaceAll('map',prefix)
}

// TODO : add a new parameter to take into account the list of desired 'highway' tag values
def defineRoadScript(def prefix, def options, def roads_script) {
    def script = ''
    def tags =[]
    options.each {
        tags.add(it.toString().replace(':','_'))
    }
    script += 'CREATE INDEX IF NOT EXISTS map_node_index on map_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index on map_way_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index2 on map_way_node(node_order);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index3 ON map_way_node(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_index on map_way(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_id_index on map_way_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_va_index on map_way_tag(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_id_index on map_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_key_index on map_tag(tag_key);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index2 ON map_relation_tag(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(tag_value);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_rel_index ON map_relation(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_member_index ON map_way_member(id_relation);\n'
    script += 'DROP TABLE IF EXISTS roads_raw;\n' +
            '\n' +
            'CREATE TABLE roads_raw AS\n' +
            'SELECT id_way, val highway, ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom\n' +
            'FROM\n' +
            '    (SELECT w.id_way, val,\n' +
            '        (SELECT ST_ACCUM(the_geom) the_geom\n' +
            '        FROM\n' +
            '            (SELECT n.id_node, n.the_geom, wn.id_way idway\n' +
            '            FROM map_node n, map_way_node wn\n' +
            '            WHERE n.id_node = wn.id_node\n' +
            '            ORDER BY wn.node_order)\n' +
            '        WHERE  idway = w.id_way) the_geom\n' +
            '    FROM map_way w,\n' +
            '        (SELECT DISTINCT id_way, value as val\n' +
            '        FROM map_way_tag wt, map_tag t\n' +
            '        WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'highway\')) b\n' +
            '    WHERE w.id_way = b.id_way) geom_table\n' +
            'WHERE ST_NUMGEOMETRIES(the_geom) >= 2\n' +
            'and val in (\'motorway\', \'trunk\', \'residential\', \'secondary\', \'tertiary\',\'primary\',\'motorway_link\', \'trunk_link\', \'secondary_link\', \'tertiary_link\',\'primary_link\',\'unclassified\');\n' +
            'CREATE INDEX IF NOT EXISTS roads_raw_index ON roads_raw(id_way);\n'
    script +='DROP TABLE IF EXISTS roads;\n' +
            '\n' +
            'CREATE TABLE roads (id serial, id_source varchar, the_geom linestring'
    tags.each {
        script += ', '+it+' varchar'
    }
    script += ') AS\n    SELECT null, a.id_way, a.the_geom'
    tags.eachWithIndex{ it, i ->
        script +=', t'+i+'.'+it
    }
    script +='\n    FROM roads_raw a\n'
    tags.eachWithIndex { it, i ->
        script +='        LEFT JOIN\n' +
            '            (SELECT DISTINCT wt.id_way, VALUE '+it+'\n' +
            '            FROM map_way_tag wt, map_tag t\n' +
            '            WHERE wt.id_tag = t.id_tag\n' +
            '            AND t.tag_key IN (\''+options[i]+'\')\n' +
            '            ) t'+i+'\n' +
            '        ON a.id_way = t'+i+'.id_way\n'
    }
    script+= ';\nDROP TABLE IF EXISTS roads_raw;'
    roads_script << script.replaceAll('map',prefix)

}

def defineRailScript(def prefix, def options, def rails_script) {
    def script = ''
    def tags =[]
    options.each {
        tags.add(it.toString().replace(':','_'))
    }
    script += 'CREATE INDEX IF NOT EXISTS map_node_index on map_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index on map_way_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index2 on map_way_node(node_order);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index3 ON map_way_node(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_index on map_way(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_id_index on map_way_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_va_index on map_way_tag(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_id_index on map_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_key_index on map_tag(tag_key);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index2 ON map_relation_tag(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(tag_value);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_rel_index ON map_relation(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_member_index ON map_way_member(id_relation);\n'
    script += 'DROP TABLE IF EXISTS rails_raw;\n' +
            '\n' +
            'CREATE TABLE rails_raw AS\n' +
            'SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom\n' +
            'FROM\n' +
            '    (SELECT w.id_way, val,\n' +
            '        (SELECT ST_ACCUM(the_geom) the_geom\n' +
            '        FROM\n' +
            '            (SELECT n.id_node, n.the_geom, wn.id_way idway\n' +
            '            FROM map_node n, map_way_node wn\n' +
            '            WHERE n.id_node = wn.id_node\n' +
            '            ORDER BY wn.node_order)\n' +
            '        WHERE  idway = w.id_way) the_geom\n' +
            '    FROM map_way w,\n' +
            '        (SELECT DISTINCT id_way, value as val\n' +
            '        FROM map_way_tag wt, map_tag t\n' +
            '        WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'railway\')) b\n' +
            '    WHERE w.id_way = b.id_way) geom_table\n' +
            'WHERE ST_NUMGEOMETRIES(the_geom) >= 2\n' +
            'and val in (\'rail\', \'disused\');\n' +
            'CREATE INDEX IF NOT EXISTS rails_raw_index ON rails_raw(id_way);\n'
    script +='DROP TABLE IF EXISTS rails;\n' +
            '\n' +
            'CREATE TABLE rails (id serial, id_source varchar, the_geom linestring'
    tags.each {
        script += ', '+it+' varchar'
    }
    script += ') AS\n    SELECT null, a.id_way, a.the_geom'
    tags.eachWithIndex{ it, i ->
        script +=', t'+i+'.'+it
    }
    script +='\n    FROM rails_raw a\n'
    tags.eachWithIndex { it, i ->
        script +='        LEFT JOIN\n' +
                '            (SELECT DISTINCT wt.id_way, VALUE '+it+'\n' +
                '            FROM map_way_tag wt, map_tag t\n' +
                '            WHERE wt.id_tag = t.id_tag\n' +
                '            AND t.tag_key IN (\''+options[i]+'\')\n' +
                '            ) t'+i+'\n' +
                '        ON a.id_way = t'+i+'.id_way\n'
    }
    script+= ';\nDROP TABLE IF EXISTS rails_raw;'
    rails_script << script.replaceAll('map',prefix)

}

def defineVegetationScript(def prefix, def options, def veget_script) {
    def script = ''
    def tags =[]
    options.each {
        tags.add(it.toString().replace(':','_'))
    }
    script += 'CREATE INDEX IF NOT EXISTS map_node_index on map_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index on map_way_node(id_node);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index2 on map_way_node(node_order);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_node_index3 ON map_way_node(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_index on map_way(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_id_index on map_way_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_tag_va_index on map_way_tag(id_way);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_id_index on map_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_tag_key_index on map_tag(tag_key);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(id_tag);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index2 ON map_relation_tag(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_tag_index ON map_relation_tag(tag_value);\n' +
            'CREATE INDEX IF NOT EXISTS map_relation_tag_rel_index ON map_relation(id_relation);\n' +
            'CREATE INDEX IF NOT EXISTS map_way_member_index ON map_way_member(id_relation);\n'
    script += 'DROP TABLE IF EXISTS vegetation_raw;\n' +
            '\n' +
            'CREATE TABLE vegetation_raw AS\n' +
            'SELECT id_way, ST_TRANSFORM(ST_SETSRID(ST_MAKELINE(the_geom), 4326), 2154) the_geom\n' +
            'FROM\n' +
            '    (SELECT w.id_way, val,\n' +
            '        (SELECT ST_ACCUM(the_geom) the_geom\n' +
            '        FROM\n' +
            '            (SELECT n.id_node, n.the_geom, wn.id_way idway\n' +
            '            FROM map_node n, map_way_node wn\n' +
            '            WHERE n.id_node = wn.id_node\n' +
            '            ORDER BY wn.node_order)\n' +
            '        WHERE  idway = w.id_way) the_geom\n' +
            '    FROM map_way w,\n' +
            '        (SELECT DISTINCT id_way, value as val\n' +
            '        FROM map_way_tag wt, map_tag t\n' +
            '        WHERE wt.id_tag = t.id_tag AND t.tag_key IN (\'natural\', \'landuse\')) b\n' +
            '    WHERE w.id_way = b.id_way) geom_table\n' +
            'WHERE ST_NUMGEOMETRIES(the_geom) >= 2\n' +
            'and val in (\'fell\', \'heath\', \'scrub\', \'tree\', \'wood\',\'farmland\',\'forest\',\'grass\',\'greenfield\',\'meadow\',\'orchard\',\'plant_nursery\',\'vineyard\');\n' +
            'CREATE INDEX IF NOT EXISTS vegetation_raw_index ON vegetation_raw(id_way);\n'
    script +='DROP TABLE IF EXISTS vegetation;\n' +
            '\n' +
            'CREATE TABLE vegetation (id serial, id_source varchar, the_geom linestring'
    tags.each {
        script += ', '+it+' varchar'
    }
    script += ') AS\n    SELECT null, a.id_way, a.the_geom'
    tags.eachWithIndex{ it, i ->
        script +=', t'+i+'.'+it
    }
    script +='\n    FROM vegetation_raw a\n'
    tags.eachWithIndex { it, i ->
        script +='        LEFT JOIN\n' +
                '            (SELECT DISTINCT wt.id_way, VALUE '+it+'\n' +
                '            FROM map_way_tag wt, map_tag t\n' +
                '            WHERE wt.id_tag = t.id_tag\n' +
                '            AND t.tag_key IN (\''+options[i]+'\')\n' +
                '            ) t'+i+'\n' +
                '        ON a.id_way = t'+i+'.id_way\n'
    }
    script+= ';\nDROP TABLE IF EXISTS vegetation_raw;'
    rails_script << script.replaceAll('map',prefix)

}