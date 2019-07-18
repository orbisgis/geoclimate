package org.orbisgis.osm

import groovy.transform.BaseScript
import groovyjarjarantlr.collections.List
import org.orbisgis.datamanager.JdbcDataSource

/**
 * OSMGISLayers is the main script to build a set of OSM GIS layers based on OSM data.
 * It uses the overpass api to download data
 * It builds a sql script file to create the layers table with the geometries and the attributes given as parameters
 * Produced layers : buildings, roads, rails, vegetation, hydro
 * Data credit : www.openstreetmap.org/copyright
 **/


import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.PrepareData
import org.orbisgis.processmanagerapi.IProcess

@BaseScript PrepareData prepareData


/**
 * This process is used to create the buildings table thanks to the osm data tables
 * @param datasource A connexion to a DB containing the 11 OSM tables
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables
 * @param outputColumnNames A map of all the columns to keep in the resulting table (tagKey : columnName)
 * @param tagKeys The tag keys corresponding to buildings
 * @param tagValues The selection of admitted tag values corresponding to the given keys (null if no filter)
 * @param buildingTablePrefix Prefix to give to the resulting table (null if none)
 * @param filteringZoneTableName Zone on which the buildings will be kept if they intersect
 * @return buildingTableName The name of the resulting buildings table
 */
 IProcess prepareBuildings() {
    return create({
        title "Prepare the building layer with OSM data"
        inputs datasource: JdbcDataSource, osmTablesPrefix: String,
                buildingTableColumnsNames: Map,
                buildingTagKeys: String[],
                buildingTagValues: String[],
                tablesPrefix: String,
                buildingFilter: String
        outputs buildingTableName: String
        run { JdbcDataSource datasource, osmTablesPrefix, buildingTableColumnsNames, buildingTagKeys, buildingTagValues,
              tablesPrefix, buildingFilter ->
            logger.info('Buildings preparation starts')
            def tableName
            if (tablesPrefix != null && tablesPrefix.endsWith("_")) {
                tableName = tablesPrefix + 'INPUT_BUILDING'
            } else {
                tableName = tablesPrefix + '_INPUT_BUILDING'
            }
            def scriptFile = File.createTempFile("createBuildingTable", ".sql")
            defineBuildingScript(osmTablesPrefix, buildingTableColumnsNames, buildingTagKeys, buildingTagValues,
                    scriptFile, tableName, buildingFilter)
            datasource.executeScript(scriptFile.getAbsolutePath())
            scriptFile.delete()
            logger.info('Buildings preparation finished')
            [buildingTableName: tableName]
        }
    }
    )
}

/**
 * This process is used to create the roads table thanks to the osm data tables
 * @param datasource A connexion to a DB containing the 11 OSM tables
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables
 * @param outputColumnNames A map of all the columns to keep in the resulting table (tagKey : columnName)
 * @param tagKeys The tag keys corresponding to roads
 * @param tagValues The selection of admitted tag values corresponding to the given keys (null if no filter)
 * @param roadTablePrefix Prefix to give to the resulting table (null if none)
 * @param filteringZoneTableName Zone on which the roads will be kept if they intersect
 * @return roadTableName The name of the resulting roads table
 */
static IProcess prepareRoads() {
    return create({
        title "Prepare the roads layer with OSM data"
        inputs datasource           : JdbcDataSource,
         osmTablesPrefix      : String,
         roadTableColumnsNames: Map,
         roadTagKeys          : String[],
         roadTagValues        : String[],
         tablesPrefix         : String,
         roadFilter           : String
        ouptputs roadTableName: String ,
        run{ datasource, osmTablesPrefix, roadTableColumnsNames, roadTagKeys, roadTagValues,
          tablesPrefix, roadFilter ->
            logger.info('Roads preparation starts')
            String tableName
            if (tablesPrefix != null && tablesPrefix.endsWith("_")) {
                tableName = tablesPrefix + 'INPUT_ROAD'
            } else {
                tableName = tablesPrefix + '_INPUT_ROAD'
            }
            def scriptFile = File.createTempFile("createRoadTable", ".sql")
            defineRoadScript(osmTablesPrefix, roadTableColumnsNames, roadTagKeys, roadTagValues,
                    scriptFile, tableName, roadFilter)
            datasource.executeScript(scriptFile.getAbsolutePath())
            scriptFile.delete()
            logger.info('Roads preparation finishes')
            [roadTableName: tableName]
        }
    }
    )
}

/**
 * This process is used to create the rails table thanks to the osm data tables
 * @param datasource A connexion to a DB containing the 11 OSM tables
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables
 * @param outputColumnNames A map of all the columns to keep in the resulting table (tagKey : columnName)
 * @param tagKeys The tag keys corresponding to rails
 * @param tagValues The selection of admitted tag values corresponding to the given keys (null if no filter)
 * @param railTablePrefix Prefix to give to the resulting table (null if none)
 * @param filteringZoneTableName Zone on which the rails will be kept if they intersect
 * @return railTableName The name of the resulting rails table
 */
static IProcess prepareRails() {
    return create({
        title "Prepare the rails layer with OSM data"
        inputs datasource           : JdbcDataSource,
         osmTablesPrefix      : String,
         railTableColumnsNames: Map,
         railTagKeys          : String[],
         railTagValues        : String[],
         tablesPrefix         : String,
         railFilter           : String
        outputs railTableName: String
        run{ datasource, osmTablesPrefix, railTableColumnsNames, tagKeys, tagValues,
          railTablePrefix, filteringZoneTableName ->
            logger.info('Rails preparation starts')
            String tableName
            if (railTablePrefix != null && railTablePrefix.endsWith("_")) {
                tableName = railTablePrefix + 'INPUT_RAIL'
            } else {
                tableName = railTablePrefix + '_INPUT_RAIL'
            }
            def scriptFile = File.createTempFile("createRailTable", ".sql")
            defineRailScript osmTablesPrefix, railTableColumnsNames, tagKeys, tagValues,
                    scriptFile, tableName, filteringZoneTableName
            datasource.executeScript scriptFile.getAbsolutePath()
            scriptFile.delete()
            logger.info('Rails preparation finishes')
            [railTableName: tableName]
        }
    }
    )
}

/**
 * This process is used to create the vegetation table thanks to the osm data tables
 * @param datasource A connexion to a DB containing the 11 OSM tables
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables
 * @param outputColumnNames A map of all the columns to keep in the resulting table (tagKey : columnName)
 * @param tagKeys The tag keys corresponding to vegetation
 * @param tagValues The selection of admitted tag values corresponding to the given keys (null if no filter)
 * @param vegetTablePrefix Prefix to give to the resulting table (null if none)
 * @param filteringZoneTableName Zone on which the vegetation will be kept if they intersect
 * @return vegetTableName The name of the resulting vegetation table
 */
static IProcess prepareVeget() {
    return create({
        title "Prepare the vegetation layer with OSM data"
        inputs  datasource            : JdbcDataSource,
         osmTablesPrefix       : String,
         vegetTableColumnsNames: Map,
         vegetTagKeys          : String[],
         vegetTagValues        : String[],
         tablesPrefix          : String,
         vegetFilter           : String
        outputs vegetTableName: String
        run{ datasource, osmTablesPrefix, vegetTableColumnsNames, vegetTagKeys, vegetTagValues,
          tablesPrefix, vegetFilter ->
            logger.info('Veget preparation starts')
            String tableName
            if (tablesPrefix == null || tablesPrefix.endsWith("_")) {
                tableName = tablesPrefix + 'INPUT_VEGET'
            } else {
                tableName = tablesPrefix + '_INPUT_VEGET'
            }
            def scriptFile = File.createTempFile("createVegetTable", ".sql")
            defineVegetationScript osmTablesPrefix, vegetTableColumnsNames, vegetTagKeys, vegetTagValues,
                    scriptFile, tableName, vegetFilter
            datasource.executeScript scriptFile.getAbsolutePath()
            scriptFile.delete()
            logger.info('Veget preparation finishes')
            [vegetTableName: tableName]
        }
    }
    )
}

/**
 * This process is used to create the hydro table thanks to the osm data tables
 * @param datasource A connexion to a DB containing the 11 OSM tables
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables
 * @param outputColumnNames A map of all the columns to keep in the resulting table (tagKey : columnName)
 * @param tagKeys The tag keys corresponding to hydro
 * @param tagValues The selection of admitted tag values corresponding to the given keys (null if no filter)
 * @param hydroTablePrefix Prefix to give to the resulting table (null if none)
 * @param filteringZoneTableName Zone on which the hydro will be kept if they intersect
 * @return hydroTableName The name of the resulting vegetation table
 */
static IProcess prepareHydro() {
    return create({
        title "Prepare the hydrological layer with OSM data"
        inputs datasource            : JdbcDataSource,
         osmTablesPrefix       : String,
         hydroTableColumnsNames: Map,
         hydroTags             : Map,
         tablesPrefix          : String,
         hydroFilter           : String
        outputs hydroTableName: String
        run{ datasource, osmTablesPrefix, hydroTableColumnsNames, hydroTags,
          tablesPrefix, hydroFilter ->
            logger.info('Hydro preparation starts')
            String tableName
            if (tablesPrefix == null || tablesPrefix.endsWith("_")) {
                tableName = tablesPrefix + 'INPUT_HYDRO'
            } else {
                tableName = tablesPrefix + '_INPUT_HYDRO'
            }
            def scriptFile = File.createTempFile("createHydroTable", ".sql")
            defineHydroScript osmTablesPrefix, hydroTableColumnsNames, hydroTags,
                    scriptFile, tableName, hydroFilter
            datasource.executeScript scriptFile.getAbsolutePath()
            scriptFile.delete()
            logger.info('Hydro preparation finishes')
            [hydroTableName: tableName]
        }
    }
    )
}

/**
 ** Function to drop the temp tables coming from the OSM extraction
 * @param prefix prefix of the OSM tables
 **/
static String dropOSMTables (String prefix) {
    def script = """DROP TABLE IF EXISTS ${prefix}_NODE, ${prefix}_NODE_MEMBER, ${prefix}_NODE_TAG,
    ${prefix}_RELATION,${prefix}_RELATION_MEMBER,${prefix}_RELATION_TAG,${prefix}_TAG, ${prefix}_WAY,
    ${prefix}_WAY_MEMBER,${prefix}_WAY_NODE,${prefix}_WAY_TAG"""
    return script.toString()
}





