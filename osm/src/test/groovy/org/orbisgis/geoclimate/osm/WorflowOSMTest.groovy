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

import groovy.json.JsonOutput
import org.apache.commons.io.FileUtils
import org.h2gis.utilities.GeographyUtilities
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.geoclimate.osmtools.OSMTools

import static org.junit.jupiter.api.Assertions.*

class WorflowOSMTest extends WorkflowAbstractTest {


    /**
     * This method is used to copy resources from the jar the tmp folder in order to limit
     * the overpass access
     */
    @BeforeAll
    static void copyOSMFiles() {
        //Here we copy osm files stored in resources to avoid overpass query
        def overpass_file_pont_de_veyle = new File(WorflowOSMTest.class.getResource("overpass_pont_de_veyle.osm").toURI())
        //The sha encoding used to set a name to the file by OSMTools
        def pont_de_veyle_query_sha = "4e8e5b748c6b5b571ebac46714aaaeba112045e541facef8623a1fc233004255"
        def copy_pont_de_veyle = new File(System.getProperty("java.io.tmpdir") + File.separator + pont_de_veyle_query_sha + ".osm")
        FileUtils.copyFile(overpass_file_pont_de_veyle, copy_pont_de_veyle)
        def overpass_file_bbox = new File(WorflowOSMTest.class.getResource("overpass_bbox.osm").toURI())
        //The sha encoding used to set a name to the file by OSMTools
        def bbox_query_sha = "5c4099c626089c4dd6f612b77ce6c4c0b7f4ddbd81d1dd2e36598601c972e5da"
        def copy_bbox = new File(System.getProperty("java.io.tmpdir") + File.separator + bbox_query_sha + ".osm")
        FileUtils.copyFile(overpass_file_bbox, copy_bbox)
        def overpass_file_bbox_logger = new File(WorflowOSMTest.class.getResource("overpass_bbox_logger.osm").toURI())
        //The sha encoding used to set a name to the file by OSMTools
        def bbox_logger_query_sha = "7da2d1cd63290068a75fb5f94510c8461e65f5790f9f8c6b97aab8204ef4699e"
        def copy_bbox_logger = new File(System.getProperty("java.io.tmpdir") + File.separator + bbox_logger_query_sha + ".osm")
        FileUtils.copyFile(overpass_file_bbox_logger, copy_bbox_logger)

    }

    @Test
    void osmGeoIndicatorsFromTestFiles(@TempDir File folder) {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad = new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        //TODO enable it for debug purpose
        boolean saveResults = false
        String directory = folder.absolutePath + File.separator + "osm_processchain_geoindicators_redon"
        def prefixName = ""
        def indicatorUse = ["UTRF", "LCZ", "TEB"]
        def svfSimplified = true

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")

        String zone = "zone"
        String buildingTableName = "building"
        String roadTableName = "road"
        String railTableName = "rails"
        String vegetationTableName = "veget"
        String hydrographicTableName = "hydro"


        datasource.load(urlBuilding, buildingTableName, true)
        datasource.load(urlRoad, roadTableName, true)
        datasource.load(urlRail, railTableName, true)
        datasource.load(urlVeget, vegetationTableName, true)
        datasource.load(urlHydro, hydrographicTableName, true)
        datasource.load(urlZone, zone, true)
        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, datasource, zone, buildingTableName, roadTableName,
                railTableName, vegetationTableName, hydrographicTableName, "", "", "",
                saveResults, svfSimplified, indicatorUse, prefixName, false)
    }

    @Test
    void osmGeoIndicatorsFromTestFilesWhenOnlySea(@TempDir File folder) {
        String urlZone = new File(getClass().getResource("ZONE_ONLY_SEA.geojson").toURI()).absolutePath

        //TODO enable it for debug purpose
        boolean saveResults = false
        String directory = folder.absolutePath + File.separator + "osm_processchain_geoindicators_onlysea"
        def prefixName = ""
        def indicatorUse = ["UTRF", "LCZ", "TEB"]
        def svfSimplified = true

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")

        String zone = "zone"
        String buildingTableName = "buildings"
        String roadTableName = "road"
        String railTableName = "rails"
        String vegetationTableName = "veget"
        String hydrographicTableName = "hydro"
        String urbanTableName = "urban"
        String sealandTableName = "sealand"
        String imperviousTableName = "impervious"


        datasource.load(urlZone, zone, true)
        datasource """DROP TABLE IF EXISTS $buildingTableName;
                    CREATE TABLE $buildingTableName(THE_GEOM GEOMETRY,
                                                    ID_BUILD INTEGER,
                                                    ID_SOURCE VARCHAR,
                                                    HEIGHT_WALL DOUBLE,
                                                    HEIGHT_ROOF DOUBLE,
                                                    NB_LEV INTEGER,
                                                    TYPE VARCHAR,
                                                    MAIN_USE VARCHAR,
                                                    ZINDEX INTEGER,
                                                    ROOF_SHAPE VARCHAR)"""
        datasource """DROP TABLE IF EXISTS $roadTableName;
                    CREATE TABLE $roadTableName(THE_GEOM GEOMETRY,
                                                ID_ROAD INTEGER,
                                                ID_SOURCE VARCHAR,
                                                WIDTH DOUBLE,
                                                TYPE VARCHAR,
                                                CROSSING VARCHAR,
                                                SURFACE VARCHAR,
                                                SIDE_WALK VARCHAR,
                                                MAXSPEED INTEGER,
                                                DIRECTION INTEGER,
                                                ZINDEX INTEGER,
                                                TUNNEL INTEGER)"""
        datasource """DROP TABLE IF EXISTS $railTableName;
                    CREATE TABLE $railTableName(THE_GEOM GEOMETRY,
                                                ID_RAIL INTEGER,
                                                ID_SOURCE VARCHAR,
                                                WIDTH DOUBLE,
                                                TYPE VARCHAR,
                                                CROSSING VARCHAR,
                                                USAGE VARCHAR,
                                                ZINDEX INTEGER)"""
        datasource """DROP TABLE IF EXISTS $vegetationTableName;
                    CREATE TABLE $vegetationTableName(THE_GEOM GEOMETRY,
                                                    ID_VEGET INTEGER,
                                                    ID_SOURCE VARCHAR,
                                                    HEIGHT_CLASS VARCHAR,
                                                    ZINDEX INTEGER)"""
        datasource """DROP TABLE IF EXISTS $hydrographicTableName;
                    CREATE TABLE $hydrographicTableName(THE_GEOM GEOMETRY,
                                                    ID_WATER INTEGER,
                                                    ID_SOURCE VARCHAR,
                                                    TYPE VARCHAR,
                                                    ZINDEX INTEGER)"""
        datasource """DROP TABLE IF EXISTS $urbanTableName;
                    CREATE TABLE $urbanTableName(THE_GEOM GEOMETRY,
                                                ID_URBAN INTEGER,
                                                ID_SOURCE VARCHAR,
                                                TYPE VARCHAR,
                                                MAIN_USE VARCHAR)"""
        datasource """DROP TABLE IF EXISTS $sealandTableName;
                    CREATE TABLE $sealandTableName(THE_GEOM GEOMETRY,
                                                ID INTEGER,
                                                TYPE VARCHAR)"""
        datasource """DROP TABLE IF EXISTS $imperviousTableName;
                    CREATE TABLE $imperviousTableName(THE_GEOM GEOMETRY,
                                                ID_IMPERVIOUS INTEGER,
                                                TYPE VARCHAR)"""

        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, datasource, zone, buildingTableName, roadTableName,
                railTableName, vegetationTableName, hydrographicTableName, imperviousTableName, sealandTableName, "",
                saveResults, svfSimplified, indicatorUse, prefixName, true)
    }

    /**
     * Save the geoclimate result to an H2GIS database
     */
    @Test
    void osmWorkflowToH2Database(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "osmWorkflowToH2Database"
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result into a database",
                "geoclimatedb": [
                        "folder": directory,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "database":
                                ["user"    : "sa",
                                 "password": "",
                                 "url"     : "h2://" + directory + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE",
                                 "tables"  : [
                                         "rsu_indicators": "rsu_indicators",
                                         "rsu_lcz"       : "rsu_lcz",
                                         "building_updated": "building_updated"]]],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        OSM.workflow(osm_parmeters)
        H2GIS outputdb = H2GIS.open(directory + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE")
        def rsu_indicatorsTable = outputdb.getTable("rsu_indicators")
        assertNotNull(rsu_indicatorsTable)
        assertTrue(rsu_indicatorsTable.getRowCount() > 0)
        def rsu_lczTable = outputdb.getTable("rsu_lcz")
        assertNotNull(rsu_lczTable)
        assertTrue(rsu_lczTable.getRowCount() > 0)
        def building_updated = outputdb.getTable("building_updated")
        assertNotNull(building_updated)
        assertTrue(building_updated.getRowCount() > 0)

    }

    /**
     * Save the geoclimate result to a PostGIS database
     */
    @Test
    void osmWorkflowToPostGISDatabase(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "osmWorkflowToPostGISDatabase"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "database":
                                ["user"    : "orbisgis",
                                 "password": "orbisgis",
                                 "url"     : "postgis://localhost:5432/orbisgis_db",
                                 "tables"  : [
                                         "rsu_indicators"         : "rsu_indicators",
                                         "rsu_lcz"                : "rsu_lcz",
                                         "zone"                   : "zone",
                                         "grid_indicators"        : "grid_indicators",
                                         "building_updated": "building_updated"]]],
                "parameters"  :
                        ["distance"       : 0,
                         rsu_indicators   : ["indicatorUse" : ["LCZ"],
                                             "svfSimplified": true],
                         "grid_indicators": [
                                 "x_size"    : 1000,
                                 "y_size"    : 1000,
                                 "indicators": ["ROAD_FRACTION"]
                         ]
                        ]
        ]
        OSM.workflow(osm_parmeters)
        def postgis_dbProperties = [databaseName: 'orbisgis_db',
                                    user        : 'orbisgis',
                                    password    : 'orbisgis',
                                    url         : 'jdbc:postgresql://localhost:5432/'
        ]
        POSTGIS postgis = POSTGIS.open(postgis_dbProperties);
        if (postgis) {
            def rsu_indicatorsTable = postgis.getTable("rsu_indicators")
            assertNotNull(rsu_indicatorsTable)
            assertTrue(rsu_indicatorsTable.getRowCount() > 0)
            def rsu_lczTable = postgis.getTable("rsu_lcz")
            assertNotNull(rsu_lczTable)
            assertTrue(rsu_lczTable.getRowCount() > 0)
            def zonesTable = postgis.getTable("zone")
            assertNotNull(zonesTable)
            assertTrue(zonesTable.getRowCount() > 0)
            def gridTable = postgis.getTable("grid_indicators")
            assertNotNull(gridTable)
            assertTrue(gridTable.getRowCount() > 0)
            def building_updated = postgis.getTable("building_updated")
            assertNotNull(building_updated)
            assertTrue(building_updated.getRowCount() > 0)
        }
    }

    @Test
    void testOSMWorkflowFromPlaceNameWithSrid(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testOSMWorkflowFromPlaceNameWithSrid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": directory,
                        "srid"  : 4326],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        OSM.workflow(osm_parmeters)
        //Test the SRID of all output files
        def geoFiles = []
        def output_folder = new File("${directory + File.separator}osm_Pont-de-Veyle")
        output_folder.eachFileRecurse groovy.io.FileType.FILES, { file ->
            if (file.name.toLowerCase().endsWith(".fgb")) {
                geoFiles << file.getAbsolutePath()
            }
        }
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        geoFiles.eachWithIndex { geoFile, index ->
            def tableName = h2gis.load(geoFile, true)
            if(h2gis.getRowCount(tableName)>0) {
                assertEquals(4326, h2gis.getSpatialTable(tableName).srid)
            }
        }
    }

    @Test
    void testOSMWorkflowFromBbox(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testOSMWorkflowFromBbox"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def bbox = [38.89557963573336, -77.03930318355559, 38.89944983078282, -77.03364372253417]
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": [bbox]],
                "output"      : [
                        "folder": directory]
        ]
        OSM.WorkflowOSM.workflow(osm_parmeters)
        def output_folder = new File(directory + File.separator + "osm_" + bbox.join("_"))
        def countFiles = 0;
        output_folder.eachFileRecurse groovy.io.FileType.FILES, { file ->
            if (file.name.toLowerCase().endsWith(".fgb")) {
                countFiles++
            }
        }
        assertEquals(10, countFiles)
    }

    @Disabled
    @Test
    void testOSMWorkflowFromPoint(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testOSMWorkflowFromPoint"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def bbox = OSM.bbox(48.78146, -3.01115, 100)
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": [bbox]],
                "output"      : [
                        "folder": directory]
        ]
        OSM.WorkflowOSM.workflow(osm_parmeters)
        def output_folder = new File(directory + File.separator + "osm_" + bbox.join("_"))
        def resultFiles = []
        output_folder.eachFileRecurse groovy.io.FileType.FILES, { file ->
            if (file.name.toLowerCase().endsWith(".fgb")) {
                resultFiles << file.getAbsolutePath()
            }
        }
        assertTrue(resultFiles.size() == 1)
        assertTrue(resultFiles.get(0) == folder.absolutePath + File.separator + "zone.fgb")
    }

    @Test
    void testOSMWorkflowBadOSMFilters(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testOSMWorkflowBadOSMFilters"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": ["", [-3.0961382389068604, -3.1055688858032227,]]],
                "output"      : [
                        "folder": directory]
        ]
        OSM.workflow(osm_parmeters)
    }

    @Test
    void workflowWrongMapOfWeights(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "workflowWrongMapOfWeights"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": directory],
                "parameters"  :
                        ["distance"    : 100,
                         "hLevMin"     : 3,
                         rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true,
                                          "mapOfWeights" :
                                                  ["sky_view_factor"             : 1,
                                                   "aspect_ratio"                : 1,
                                                   "building_surface_fraction"   : 1,
                                                   "impervious_surface_fraction" : 1,
                                                   "pervious_surface_fraction"   : 1,
                                                   "height_of_roughness_elements": 1,
                                                   "terrain_roughness_length"    : 1,
                                                   "terrain_roughness_class"     : 1]]
                        ]
        ]
        assertThrows(Exception.class, () -> OSM.workflow(osm_parmeters))
    }

    @Test
    void testGrid_Indicators(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testGrid_Indicators"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": ["path"  : directory,
                                   "tables": ["grid_indicators", "zone"]]],
                "parameters"  :
                        ["distance"       : 0,
                         "grid_indicators": [
                                 "x_size"    : 1000,
                                 "y_size"    : 1000,
                                 "indicators": ["WATER_FRACTION", "LCZ_PRIMARY"],
                                 "output"    : "asc"
                         ]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def gridTable = tableNames.grid_indicators[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $gridTable where water_permanent_fraction + water_intermittent_fraction>0").count > 0
        def grid_file = new File("${directory + File.separator}osm_Pont-de-Veyle${File.separator}grid_indicators_water_permanent_fraction.asc")
        h2gis.execute("DROP TABLE IF EXISTS water_grid; CALL ASCREAD('${grid_file.getAbsolutePath()}', 'water_grid')")
        assertTrue h2gis.firstRow("select count(*) as count from water_grid").count == 6
        assertEquals(6, h2gis.firstRow("select count(*) as count from $gridTable where LCZ_PRIMARY is not null").count)
     }

    @Test
    void testLoggerZones(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testLoggerZones"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[48.49749, 5.25349, 48.58082, 5.33682]]],
                "output"      : [
                        "folder": ["path"  : directory,
                                   "tables": ["grid_indicators", "zone"]]],
                "parameters"  :
                        ["distance"       : 0,
                         "grid_indicators": [
                                 "x_size"    : 10,
                                 "y_size"    : 10,
                                 rowCol      : true,
                                 "indicators": ["WATER_FRACTION"],
                                 "output"    : "asc"
                         ]
                        ]
        ]
        OSM.workflow(osm_parmeters)
    }

    @Test
    void osmWrongAreaSize(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "osmWrongAreaSize"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result into a database",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"],
                        "area"     : -1],
                "output"      : [
                        "database":
                                ["user"    : "sa",
                                 "password": "",
                                 "url"     : "h2://" + dirFile.absolutePath + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE",
                                 "tables"  : [
                                         "rsu_indicators": "rsu_indicators",
                                         "rsu_lcz"       : "rsu_lcz"]]],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        assertThrows(Exception.class, () -> OSM.workflow(osm_parmeters))
    }


    @Test
    void testOSMTEB(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testOSMTEB"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": directory],
                "parameters"  :
                        [
                                rsu_indicators: [
                                        "indicatorUse" : ["TEB"],
                                        "svfSimplified": true
                                ]
                        ]
        ]
        OSM.workflow(createOSMConfigFile(osm_parmeters, directory))
    }

    @Test
    void testTarget(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testTarget"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Compute the Target land input",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]
                        ],
                "output"      : [
                        "folder": directory],
                "parameters"  :
                        [
                                rsu_indicators: [
                                        "indicatorUse" : ["TARGET"]
                                ]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def targetGrid = tableNames.grid_target[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db")
        assertEquals(h2gis.getRowCount(targetGrid), h2gis.firstRow("""select count(*) as count from $targetGrid 
        where \"roof\"+ \"road\"+ \"watr\"+\"conc\"+\"Veg\" + \"dry\" + \"irr\" >=1""").count)
    }

    @Test
    void testTargetGridSize(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testTargetGridSize"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Compute the target land input",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": directory],
                "parameters"  :
                        [
                                rsu_indicators: ["indicatorUse" : ["TARGET", "LCZ"]
                                ],"grid_indicators"   : [
                                "x_size"    : 200,
                                "y_size"    : 200,
                                "indicators": [
                                        "LCZ_PRIMARY"]
                        ]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def targetGrid = tableNames.grid_target[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db")
        assertEquals(h2gis.getRowCount(targetGrid), h2gis.firstRow("""select count(*) as count from $targetGrid 
        where \"roof\"+ \"road\"+ \"watr\"+\"conc\"+\"Veg\" + \"dry\" + \"irr\" >=1""").count)
        def gridIndicators = tableNames.grid_indicators[0]
        assertTrue(h2gis.getColumnNames(gridIndicators).contains("LCZ_PRIMARY"))
    }

    @Test
    void testRoadTrafficAndNoiseIndicators(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testRoadTrafficAndNoiseIndicators"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run only the road traffic estimation",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": ["path"  : directory,
                                   "tables": ["road_traffic", "ground_acoustic"]]],
                "parameters"  :
                        ["road_traffic"    : true,
                         "noise_indicators": [
                                 "ground_acoustic": true
                         ]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def roadTableName = process["Pont-de-Veyle"]["road_traffic"]
        assertNotNull(roadTableName)
        def ground_acoustic = process["Pont-de-Veyle"]["ground_acoustic"]
        assertNotNull(ground_acoustic)
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $roadTableName where road_type is not null".toString()).count > 0
        assertTrue h2gis.firstRow("select count(*) as count from $ground_acoustic where layer in ('road', 'building')".toString()).count == 0
        assertTrue h2gis.rows("select distinct(g) as g from $ground_acoustic where layer = 'water'".toString()).size() == 1
    }

    @Disabled
    //Use it to test a list of bboxs collect from various user feedback
    @Test
    void testLCZBBOXFeedback() {
        String directory = "/tmp/geoclimate"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def location= [15.004311, 108.55263, 15.094142, 108.64575] //visual validation in a GIS
        location= [15.094142,108.73887,15.183973,108.83199] //visual validation in a GIS
        location = [62.027935,129.76294,62.045902,129.80127] //visual validation in a GIS
        location =[40.70075,-74.03082,40.709732,-74.01897] //visual validation in a GIS
        location=[40.70075,-74.01897,40.709732,-74.00712] //visual validation in a GIS
        location=[53.242824,-9.103203,53.299902,-8.915749] //visual validation in a GIS
        //location=[48.882799,2.221194,48.899165,2.259474]

        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_test_bbox;",
                        "delete": false
                ],
                "input"       : [
                        "locations": [location],
                        "area"     : 2800],
                "output"      : [
                        "folder": directory]
                ,
                "parameters"  :
                        [
                         "rsu_indicators"       : [
                                 //"surface_urban_areas":0,
                                 //"surface_vegetation":1500,
                                 "indicatorUse": ["LCZ"]
                         ]
                        ]
        ]
        OSM.workflow(osm_parmeters)
    }

    @Disabled
    //Use it for debug
    @Test
    void testIntegration() {
        String directory = "/tmp/geoclimate"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def location = "Redon"
        location = [43.4, 1.4, 43.6, 1.6]
        //def nominatim = OSMTools.Utilities.getNominatimData("Redon")
        def grid_size = 100
        //location =[47.214976592711274,-1.6425595375815742,47.25814872718718,-1.5659501122281323]
        //location=[47.215334,-1.558058,47.216646,-1.556185]
        //location = [nominatim.bbox]
        def location1= [47.642695,-2.777953,47.648651,-2.769413]
        def location2= [47.642723,-2.769456,47.648622,-2.761259]

        //Farm land pb
        location1  = [50.957075,1.946297,50.988314,2.027321]

        //location = [[47.504374,-0.479279,47.516621,-0.454495]]

        /* location =[ 48.84017284026897,
                    2.3061887733275785,
                    48.878115442982086,
                    2.36742047202511]*/

        // Disable this if you want to compute a grid of domains
        // location = computeDomains(location1, 500)
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_test_integration;",
                        "delete": false
                ],
                "input"       : [
                        "locations": [location],//["Pont-de-Veyle"],//[nominatim["bbox"]],//["Lorient"],
                        "area":100000,
                        "timeout":1800,
                        "maxsize":1036870912
                        //"date":"2017-12-31T19:20:00Z",
                        /*"timeout":182,
                        "maxsize": 536870918,
                        "endpoint":"https://lz4.overpass-api.de/api"*/],
                "output"      : [
                        "folder": directory]
                        /*[
                        "database":
                                ["user"    : "orbisgis",
                                 "password": "orbisgis",
                                 "url"     : "postgis://localhost:5432/orbisgis_db",
                                 "tables"  : [
                                         "rsu_indicators"         : "rsu_indicators",
                                         "rsu_lcz"                : "rsu_lcz",
                                         "zone"                   : "zone"]]]*/
                ,
                "parameters"  :
                        [//"distance"             : 200,
                         "rsu_indicators"       : [
                                 "indicatorUse": ["LCZ", "TEB"] //, "UTRF"]

                         ]/*,
                          "grid_indicators"   : [
                                "x_size"    : grid_size,
                                "y_size"    : grid_size,
                                "rowCol": true,
                                "output" : "geojson",
                                "indicators": [
                                        "BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_POP",
                                        "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                                        "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                                        "BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY",
                                        "SEA_LAND_FRACTION", "ASPECT_RATIO", "SVF",
                                        "HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS",
                                        "PROJECTED_FACADE_DENSITY_DIR", "BUILDING_DIRECTION",
                                        "UTRF_AREA_FRACTION", "UTRF_FLOOR_AREA_FRACTION",
                                        "LCZ_PRIMARY", "BUILDING_HEIGHT_DISTRIBUTION", "STREET_WIDTH",
                                        "BUILDING_NUMBER"]
                                //"lcz_lod":1
                        ],
                         /*

                         "road_traffic"                                         : true,
                         "noise_indicators"                                     : [
                                 "ground_acoustic": true
                         ]*/
                        ]
        ]
         OSM.workflow(osm_parmeters)
    }

    /**
     * Method to compute a grid of domains
     * @param osm_zone
     * @param distance
     * @return
     */
    List computeDomains(def osm_zone, float distance){
        H2GIS db = H2GIS.open("/tmp/mydb")
       Geometry geom = OSMTools.Utilities.getArea(osm_zone)

       def lat_lon_bbox_extended = geom.getFactory().toGeometry(GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance))

       db.execute("""
           DROP TABLE IF EXISTS domains;
           CREATE TABLE domains as select * from st_makegrid(ST_GEOMFROMTEXT('$lat_lon_bbox_extended', 4326), 500,500)
       """)
       db.save("domains", "/tmp/domains.geojson", true)

       def location =[]
       db.eachRow("SELECT THE_GEOM from DOMAINS"){
           def env = it.the_geom.getEnvelopeInternal()
           location<<[ env.getMinY() as float,env.getMinX() as float,env.getMaxY() as float,env.getMaxX() as float]
       }
        return location
    }

    @Disabled
    //Use it for debug
    @Test
    void testwrf() {
        String directory = "/tmp/geoclimate"
        String geoclimate_init_dir = "/home/decide/Data/WRF/Data/output/osm_57.696809_11.918449_57.711667_11.947289"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def test = "test"
        def wrf_indicators = ["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_HEIGHT_WEIGHTED",
                              "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                              "ROAD_FRACTION", "IMPERVIOUS_FRACTION",
                              "FREE_EXTullERNAL_FACADE_DENSITY", "BUILDING_SURFACE_DENSITY",
                              "BUILDING_HEIGHT_DISTRIBUTION", "FRONTAL_AREA_INDEX", "SEA_LAND_FRACTION"]
        def databasePath = '/tmp/geoclimate_chain_dbtest;AUTO_SERVER=TRUE'
        def h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
        def datasource = H2GIS.open(h2gis_properties)
        def srid_calc = 3007
        def x_size = 100
        def y_size = 100
        def prefixName = "UPDATED_HEIGHTTEST"
        def buildingUpdated = prefixName + "_BUILDING"

        def id_zone = [57.696809, 11.918449, 57.711667, 11.947289]
        def roadFile = "road"
        def vegetationFile = "vegetation"
        def waterFile = "water"
        def railFile = "rail"
        def imperviousFile = "impervious"
        def zoneFile = "zone"
        def sea_land_maskFile = "sea_land_mask"
        def filesToLoad = ["$roadFile"         : roadFile + test, "$vegetationFile": vegetationFile + test,
                           "$waterFile"        : waterFile + test, "$railFile": railFile + test,
                           "$imperviousFile"   : imperviousFile + test, "$zoneFile": zoneFile + test,
                           "$sea_land_maskFile": sea_land_maskFile + test]
        filesToLoad.each { file, tab ->
            datasource.load("$geoclimate_init_dir/${file}.geojson", tab, true)
        }

        def geomEnv = datasource.firstRow("SELECT ST_TRANSFORM(ST_EXTENT(the_geom), $srid_calc) AS geom FROM $zoneFile$test".toString()).geom
        def rasterizedIndicators
        def rsuLczUpdated = "UPDATED_HEIGHTTEST_RSU_LCZ"
        println(""" Grid indicators are calculated """)
        def gridProcess = Geoindicators.WorkflowGeoIndicators.createGrid(datasource, geomEnv,
                x_size, y_size, srid_calc, false)
        if (gridProcess) {
            def computeRasterizedIndicators = Geoindicators.WorkflowGeoIndicators.rasterizeIndicators(datasource,
                    gridProcess,
                    wrf_indicators,
                    null,
                    buildingUpdated,
                    roadFile + test,
                    vegetationFile + test,
                    waterFile + test,
                    imperviousFile + test,
                    rsuLczUpdated,
                    null,
                    null,
                    sea_land_maskFile + test,
                    prefixName)
            if (!computeRasterizedIndicators) {
                println("Could not rasterized indicators")
            }
            def reproject = false
            def deleteOutputData = true
            def outputFolder = new File('/home/decide/Data/WRF/Data/output/updated')
            def subFolder = new File(outputFolder.getAbsolutePath() + File.separator + "osm_" + id_zone)
            Geoindicators.WorkflowUtilities.saveToAscGrid(computeRasterizedIndicators, subFolder, "grid_indicators", datasource, 3007, reproject, deleteOutputData)
        }
    }

    @Test
    //Integration tests
    @Disabled
    void testOSMConfigurationFile() {
        def configFile = getClass().getResource("config/osm_workflow_placename_folderoutput.json").toURI()
        //configFile =getClass().getResource("config/osm_workflow_envelope_folderoutput.json").toURI()
        OSM.WorkflowOSM.workflow(configFile)
    }

    @Test
    void testEstimateBuildingWithAllInputHeight(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testEstimateBuildingWithAllInputHeight"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[43.726898, 7.298452, 43.727677, 7.299632]]],
                "parameters"  :
                        ["distance"    : 100,
                         rsu_indicators: ["indicatorUse": ["LCZ"]]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def building = tableNames.building[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
    }

    @Test
    void testEstimateBuildingWithAllInputHeightFromPoint(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testEstimateBuildingWithAllInputHeightFromPoint"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[43.726898, 7.298452, 100]]],
                "output"      : [
                        "folder": ["path"  : directory,
                                   "tables": ["building", "zone"]]],
                "parameters"  :
                        [
                                rsu_indicators: ["indicatorUse": ["LCZ"]]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def building = tableNames.building[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
    }


    @Test
    void testCreateGISLayers(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testCreateGISLayers"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[43.726898, 7.298452, 100]]],
                "output"      : [
                        "folder": ["path"  : directory,
                                   "tables": ["building", "zone"]]]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def building = tableNames.building[0]
        def zone = tableNames.zone[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
        assertEquals(1, h2gis.firstRow("select count(*) as count from $zone").count)
    }

    @Test
    void testCreateGISLayersNoOutput(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testCreateGISLayersNoOutput"
        directory = "/tmp/db"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[43.726898, 7.298452, 100]]]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db")
        //All tables must exist in the database
        tableNames.each { it ->
            assertTrue(h2gis.hasTable(it.value))
        }
    }


    @Disabled
    //Because it takes some time to build the OSM query
    @Test
    void testEstimateBuildingWithAllInputHeightDate(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testEstimateBuildingWithAllInputHeightDate"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[43.726898, 7.298452, 43.727677, 7.299632]],
                        "date"     : "2015-12-31T19:20:00Z"],
                "output"      : [
                        "folder": ["path"  : directory,
                                   "tables": ["building", "zone"]]],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ"]]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def building = tableNames.building[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
    }

    @Disabled
    @Test
    //TODO : A fix on OSM must be done because Golfe de Gascogne is tagged as a river.
    //This geometry must be redesigned because according the International Hydrographic Organization
    // Golfe de Gascogne must be considered as bay where some main rivers  empty into it
    void testOneSeaLCZ() {
        String directory = folder.absolutePath + File.separator + "testOneSeaLCZ"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "sea_lcz_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [[47.4, -4.8, 47.6, -4.6]]],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ"]]
                        ]
        ]
        Map process = OSM.WorkflowOSM.workflow(osm_parmeters)
        def tableNames = process.values()
        def lcz = tableNames.rsu_lcz[0]
        H2GIS h2gis = H2GIS.open("${directory + File.separator}sea_lcz_db;AUTO_SERVER=TRUE")
        h2gis.save(lcz, "/tmp/sea.geojson", true)
        def lcz_group = h2gis.firstRow("select  lcz_primary, count(*) as count from $lcz group by lcz_primary".toString())
        assertTrue(lcz_group.size() == 2)
        assertTrue(lcz_group.lcz_primary == 107)
        assertTrue(lcz_group.count == 1)
    }


    /**
     * Save the geoclimate result to a PostGIS database
     */
    @Test
    void osmWorkflowToPostGISExcludeColumns(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "osmWorkflowToPostGISExcludeColumns"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "locations": ["Pont-de-Veyle"]],
                "output"      : [
                        "database":
                                ["user"    : "orbisgis",
                                 "password": "orbisgis",
                                 "url"     : "postgis://localhost:5432/orbisgis_db",
                                 "tables"  : [
                                         "rsu_indicators"         : "rsu_indicators",
                                         "rsu_lcz"                : "rsu_lcz",
                                         "zone"                   : "zone",
                                         "building":"building"],
                                 "excluded_columns"  : [
                                         "rsu_indicators" : ["the_geom"],
                                         "rsu_lcz"         : ["the_geom"],
                                         "building_indicators"  : ["the_geom"]]]],
                "parameters"  :
                        ["distance"       : 0,
                         rsu_indicators   : ["indicatorUse" : ["LCZ"]]
                        ]
        ]
        OSM.workflow(osm_parmeters)
        def postgis_dbProperties = [databaseName: 'orbisgis_db',
                                    user        : 'orbisgis',
                                    password    : 'orbisgis',
                                    url         : 'jdbc:postgresql://localhost:5432/'
        ]
        POSTGIS postgis = POSTGIS.open(postgis_dbProperties);
        if (postgis) {
            def rsu_indicatorsTable = postgis.getTable("rsu_indicators")
            assertNotNull(rsu_indicatorsTable)
            assertTrue(rsu_indicatorsTable.getRowCount() > 0)
            def rsu_lczTable = postgis.getTable("rsu_lcz")
            assertNotNull(rsu_lczTable)
            assertTrue(rsu_lczTable.getRowCount() > 0)
            def zonesTable = postgis.getTable("zone")
            assertNotNull(zonesTable)
            assertTrue(zonesTable.getRowCount() > 0)
            postgis.dropTable("rsu_indicators" , "rsu_lcz" ,"zone" ,"building")
        }
    }

    /**
     * Create a configuration file
     * @param osmParameters
     * @param directory
     * @return
     */
    def createOSMConfigFile(def osmParameters, def directory) {
        def json = JsonOutput.toJson(osmParameters)
        def configFilePath = directory + File.separator + "osmConfigFile.json"
        File configFile = new File(configFilePath)
        if (configFile.exists()) {
            configFile.delete()
        }
        configFile.write(json)
        return configFile.absolutePath
    }

    @Disabled
    //Use it for debug
    @Test
    void testMergeRsu() {
        H2GIS h2GIS  = H2GIS.open("/tmp/database")

        List domains =["/tmp/geoclimate/osm_47.642695_-2.777953_47.648651_-2.769413/",
                       "/tmp/geoclimate/osm_47.642723_-2.769456_47.648622_-2.761259/"]

        h2GIS.load("/tmp/geoclimate/osm_47.642695_-2.777953_47.648651_-2.769413/zone.fgb", "zone_a", true)
        h2GIS.load("/tmp/geoclimate/osm_47.642695_-2.777953_47.648651_-2.769413/rsu_lcz.fgb", "rsu_a_in", true)
        h2GIS.load("/tmp/geoclimate/osm_47.642723_-2.769456_47.648622_-2.761259/zone.fgb", "zone_b", true)
        h2GIS.load("/tmp/geoclimate/osm_47.642723_-2.769456_47.648622_-2.761259/rsu_lcz.fgb", "rsu_b_in", true)

        h2GIS.createSpatialIndex("rsu_a_in")
        h2GIS.createSpatialIndex("rsu_b_in")

        h2GIS.execute("""
                DROP TABLE IF EXISTS rsu_a;
                create table rsu_a as 
                SELECT a.* FROM rsu_a_in as a, zone_a as b 
                where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);
                DROP TABLE IF EXISTS rsu_b;
                create table rsu_b as 
                SELECT a.* FROM rsu_b_in as a, zone_b as b 
                where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom);""")

        h2GIS.createSpatialIndex("rsu_a")
        h2GIS.createSpatialIndex("rsu_b")

        h2GIS.execute("""
                DROP TABLE IF EXISTS diff_rsu;
                create table diff_rsu as 
                SELECT a.id_rsu as a_id_rsu, b.id_rsu as b_id_rsu, b.the_geom, b.lcz_primary FROM rsu_a as a, rsu_b as b 
                where a.the_geom && b.the_geom and st_intersects(st_pointonsurface(a.the_geom), b.the_geom);
                DELETE FROM RSU_A WHERE ID_RSU IN (SELECT a_id_rsu from diff_rsu );
                DELETE FROM RSU_B WHERE ID_RSU IN (SELECT b_id_rsu from diff_rsu );
                DROP TABLE IF EXISTS final_rsu;
                CREATE TABLE final_rsu as (select id_rsu, the_geom, lcz_primary from rsu_a) 
                union all (select id_rsu, the_geom, lcz_primary from rsu_b)
                union all (select b_id_rsu, the_geom, lcz_primary from diff_rsu)
                """)

        h2GIS.save("diff_rsu","/tmp/diff_rsu.fgb", true)
        h2GIS.save("final_rsu","/tmp/final_rsu.fgb", true)
    }

    @Test
    void testClip(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testClip"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def bbox = [43.726898, 7.298452, 43.727677, 7.299632]
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [bbox]],
                "output"      : [
                        "folder": directory,
                        "domain":"zone",
                        "srid"  : 4326],
                "parameters"  :
                        ["distance"    : 100,
                         rsu_indicators: ["indicatorUse": ["LCZ"]],
                         "grid_indicators": [
                                 "domain": "zone_extended", //Compute the grid on the extended zone
                                "x_size"    : 100,
                                "y_size"    : 100,
                                "indicators": ["LCZ_PRIMARY"]
                        ]
                        ]
        ]
        OSM.WorkflowOSM.workflow(osm_parmeters)
        def folder_out = directory + File.separator + "osm_" + bbox.join("_")
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")

        def building = "building"
        def zone = "zone"
        h2gis.load(folder_out+File.separator+"building.fgb", building, true)
        h2gis.load(folder_out+File.separator+"zone.fgb", zone, true)
        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
        h2gis.execute("""DROP TABLE IF EXISTS building_out;
        CREATE TABLE building_out as SELECT a.* FROM  $building a LEFT JOIN $zone b
                ON a.the_geom && b.the_geom and ST_INTERSECTS(a.the_geom, b.the_geom)
                WHERE b.the_geom IS NULL;""")
        assertEquals(0, h2gis.getRowCount("building_out"))
        def grid_indicators = "grid_indicators"
        h2gis.load(folder_out+File.separator+"grid_indicators.fgb", grid_indicators, true)
        h2gis.execute("""DROP TABLE IF EXISTS grid_out;
        CREATE TABLE grid_out as SELECT a.* FROM  $grid_indicators a LEFT JOIN $zone b
                ON a.the_geom && b.the_geom and ST_INTERSECTS(st_centroid(a.the_geom), b.the_geom)
                WHERE b.the_geom IS NULL;""")
        assertEquals(10, h2gis.getRowCount("grid_out"))

        h2gis.dropTable("building_out", "grid_out")
    }

    @Test
    void testClip2(@TempDir File folder) {
        String directory = folder.absolutePath + File.separator + "testClip2"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def bbox = [43.726898, 7.298452, 43.727677, 7.299632]
        def osm_parmeters = [
                "geoclimatedb": [
                        "folder": dirFile.absolutePath,
                        "name"  : "geoclimate_chain_db",
                        "delete": false
                ],
                "input"       : [
                        "locations": [bbox]],
                "output"      : [
                        "folder": directory,
                        "domain":"zone_extended",
                        "srid"  : 4326],
                "parameters"  :
                        ["distance"    : 100,
                         rsu_indicators: ["indicatorUse": ["LCZ"]],
                         "grid_indicators": [
                                 "x_size"    : 100,
                                 "y_size"    : 100,
                                 "indicators": ["LCZ_PRIMARY"]
                         ]
                        ]
        ]
        OSM.WorkflowOSM.workflow(osm_parmeters)
        def folder_out = directory + File.separator + "osm_" + bbox.join("_")
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")

        def building = "building"
        def zone = "zone"
        h2gis.load(folder_out+File.separator+"building.fgb", building, true)
        h2gis.load(folder_out+File.separator+"zone.fgb", zone, true)
        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
        h2gis.execute("""DROP TABLE IF EXISTS building_out;
        CREATE TABLE building_out as SELECT a.* FROM  $building a LEFT JOIN $zone b
                ON a.the_geom && b.the_geom and ST_INTERSECTS(a.the_geom, b.the_geom)
                WHERE b.the_geom IS NULL;""")
        assertEquals(14, h2gis.getRowCount("building_out"))
    }
}
