package org.orbisgis.geoclimate.osm

import groovy.json.JsonOutput
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess

import static org.junit.jupiter.api.Assertions.*

class WorflowOSMTest extends WorkflowAbstractTest {


    @BeforeAll
    static void copyOSMFiles(){
        //Here we copy osm files stored in resources to avoid overpass query
        def  overpass_file_pont_de_veyle = new File(WorflowOSMTest.class.getResource("overpass_pont_de_veyle.osm").toURI())
        //The sha encoding used to set a name to the file by OSMTools
        def pont_de_veyle_query_sha = "4e8e5b748c6b5b571ebac46714aaaeba112045e541facef8623a1fc233004255"
        def copy_pont_de_veyle = new File(System.getProperty("java.io.tmpdir")+File.separator+pont_de_veyle_query_sha+".osm")
        FileUtils.copyFile(overpass_file_pont_de_veyle, copy_pont_de_veyle)
        def  overpass_file_bbox = new File(WorflowOSMTest.class.getResource("overpass_bbox.osm").toURI())
        //The sha encoding used to set a name to the file by OSMTools
        def bbox_query_sha = "5c4099c626089c4dd6f612b77ce6c4c0b7f4ddbd81d1dd2e36598601c972e5da"
        def copy_bbox = new File(System.getProperty("java.io.tmpdir")+File.separator+bbox_query_sha+".osm")
        FileUtils.copyFile(overpass_file_bbox, copy_bbox)
        def  overpass_file_bbox_logger = new File(WorflowOSMTest.class.getResource("overpass_bbox_logger.osm").toURI())
        //The sha encoding used to set a name to the file by OSMTools
        def bbox_logger_query_sha = "7da2d1cd63290068a75fb5f94510c8461e65f5790f9f8c6b97aab8204ef4699e"
        def copy_bbox_logger = new File(System.getProperty("java.io.tmpdir")+File.separator+bbox_logger_query_sha+".osm")
        FileUtils.copyFile(overpass_file_bbox_logger, copy_bbox_logger)

    }

    @Test
    void osmGeoIndicatorsFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        //TODO enable it for debug purpose
        boolean saveResults = false
        String directory ="./target/osm_processchain_geoindicators_redon"
        def prefixName = ""
        def indicatorUse = ["UTRF", "LCZ", "TEB"]
        def svfSimplified = true

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osm_chain_db;AUTO_SERVER=TRUE")

        String zoneTableName="zone"
        String buildingTableName="building"
        String roadTableName="road"
        String railTableName="rails"
        String vegetationTableName="veget"
        String hydrographicTableName="hydro"


        datasource.load(urlBuilding, buildingTableName, true)
        datasource.load(urlRoad, roadTableName, true)
        datasource.load(urlRail, railTableName, true)
        datasource.load(urlVeget, vegetationTableName, true)
        datasource.load(urlHydro, hydrographicTableName, true)
        datasource.load(urlZone, zoneTableName, true)
        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath+File, datasource, zoneTableName, buildingTableName,roadTableName,
                null,vegetationTableName, hydrographicTableName,saveResults, svfSimplified, indicatorUse, prefixName)
    }


    @Disabled
    @Test
    void osmLczFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        //TODO enable it for debug purpose
        boolean saveResults = false
        String directory ="./target/osm_processchain_lcz"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osmchain_lcz;AUTO_SERVER=TRUE")

        String zoneTableName="zone"
        String buildingTableName="building"
        String roadTableName="road"
        String railTableName="rails"
        String vegetationTableName="veget"
        String hydrographicTableName="hydro"

        datasource.load(urlBuilding, buildingTableName, true)
        datasource.load(urlRoad, roadTableName, true)
        datasource.load(urlRail, railTableName, true)
        datasource.load(urlVeget, vegetationTableName, true)
        datasource.load(urlHydro, hydrographicTableName, true)
        datasource.load(urlZone, zoneTableName, true)

        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        IProcess geodindicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators()
        assertTrue geodindicators.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: railTableName, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights,svfSimplified: true )
        assertTrue(datasource.getTable(geodindicators.results.outputTableBuildingIndicators).rowCount>0)
        assertNotNull(geodindicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuIndicators).rowCount>0)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuLcz).rowCount>0)
    }

    @Test
    void osmWorkflowToH2Database() {
        String directory ="./target/geoclimate_chain_db"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the result into a database",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "database" :
                                ["user" : "sa",
                                "password":"",
                                 "url": "jdbc:h2://"+dirFile.absolutePath+File.separator+"geoclimate_chain_db_output;AUTO_SERVER=TRUE",
                                 "tables": [
                                          "rsu_indicators":"rsu_indicators",
                                          "rsu_lcz":"rsu_lcz" ]]],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: osm_parmeters))
        H2GIS outputdb = H2GIS.open(dirFile.absolutePath+File.separator+"geoclimate_chain_db_output;AUTO_SERVER=TRUE")
        def rsu_indicatorsTable = outputdb.getTable("rsu_indicators")
        assertNotNull(rsu_indicatorsTable)
        assertTrue(rsu_indicatorsTable.getRowCount()>0)
        def rsu_lczTable = outputdb.getTable("rsu_lcz")
        assertNotNull(rsu_lczTable)
        assertTrue(rsu_lczTable.getRowCount()>0)
    }

    @Test
    void osmWorkflowToPostGISDatabase() {
        String directory ="./target/geoclimate_chain_postgis"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "database" :
                                ["user" : "orbisgis",
                                 "password":"orbisgis",
                                 "url": "jdbc:postgresql://localhost:5432/orbisgis_db",
                                 "tables": [
                                            "rsu_indicators":"rsu_indicators",
                                            "rsu_lcz":"rsu_lcz",
                                            "zones":"zones" ,
                                            "grid_indicators":"grid_indicators",
                                            "building_height_missing":"building_height_missing"]]],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ"],
                                          "svfSimplified": true] ,
                        "grid_indicators": [
                        "x_size": 1000,
                        "y_size": 1000,
                        "indicators": ["ROAD_FRACTION"]
                        ]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: osm_parmeters))
        def postgis_dbProperties = [databaseName: 'orbisgis_db',
                                           user        : 'orbisgis',
                                           password    : 'orbisgis',
                                           url         : 'jdbc:postgresql://localhost:5432/'
        ]
        POSTGIS postgis = POSTGIS.open(postgis_dbProperties);
        if(postgis){
            def rsu_indicatorsTable = postgis.getTable("rsu_indicators")
            assertNotNull(rsu_indicatorsTable)
            assertTrue(rsu_indicatorsTable.getRowCount()>0)
            def rsu_lczTable = postgis.getTable("rsu_lcz")
            assertNotNull(rsu_lczTable)
            assertTrue(rsu_lczTable.getRowCount()>0)
            def zonesTable = postgis.getTable("zones")
            assertNotNull(zonesTable)
            assertTrue(zonesTable.getRowCount()>0)
            def gridTable = postgis.getTable("grid_indicators")
            assertNotNull(gridTable)
            assertTrue(gridTable.getRowCount()>0)
            def building_height_missing = postgis.getTable("building_height_missing")
            assertNotNull(building_height_missing)
            assertTrue(building_height_missing.getRowCount()>0)
        }
    }

    @Test
    void testOSMWorkflowFromPlaceNameWithSrid() {
        String directory ="./target/geoclimate_chain_srid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the results in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : directory,
                        "srid":4326],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: osm_parmeters))
        //Test the SRID of all output files
        def geoFiles = []
        def  folder = new File("${directory+File.separator}osm_Pont-de-Veyle")
        folder.eachFileRecurse groovy.io.FileType.FILES,  { file ->
            if (file.name.toLowerCase().endsWith(".geojson")) {
                geoFiles << file.getAbsolutePath()
            }
        }
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        geoFiles.eachWithIndex { geoFile , index->
            def tableName = h2gis.load(geoFile, true)
            assertEquals(4326, h2gis.getSpatialTable(tableName).srid)
        }
    }

    @Test
    void testOSMWorkflowFromPlaceName() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : directory],
                "parameters":
                        [rsu_indicators: ["indicatorUse": ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }


    @Test
    void testOSMWorkflowFromBbox() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "locations" : [[38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417]]],
                "output" :[
                        "folder" : directory]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOSMWorkflowBadOSMFilters() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "locations" : ["", [-3.0961382389068604, -3.1055688858032227,48.77155634881654,]]],
                "output" :[
                        "folder" : directory]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: osm_parmeters))
    }

    @Test
    void workflowWrongMapOfWeights() {
        String directory ="./target/bdtopo_workflow"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : directory],
                "parameters":
                        ["distance" : 100,
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10,
                         rsu_indicators: ["indicatorUse": ["LCZ"],
                                          "svfSimplified": true,
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_length": 1 ,
                                  "terrain_roughness_class": 1 ]]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertFalse(process.execute(input: osm_parmeters))
    }

    @Disabled
    @Test
    void testOSMEstimatedHeight() {
        String directory ="./target/geoclimate_chain_estimated_height"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the results in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : directory],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ", "UTRF"],
                         "svfSimplified": true,
                         "estimateHeight":true]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOnlyEstimateHeight() {
        String directory ="./target/geoclimate_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run only the estimated height model",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : ["path": directory,
                                    "tables": ["building"]]],
                "parameters":
                        ["distance" : 0,
                         "rsu_indicators": [
                                 "svfSimplified": true,
                                 "estimateHeight":true
                         ]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count>0
    }

    @Test
    void testGrid_Indicators() {
        String directory ="./target/geoclimate_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                "folder" : ["path": directory,
                    "tables": ["grid_indicators", "zones"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                             "x_size": 1000,
                             "y_size": 1000,
                             "indicators": ["WATER_FRACTION"],
                             "output":"asc"
                         ]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count>0
        def  grid_file = new File("${directory+File.separator}osm_Pont-de-Veyle${File.separator}grid_indicators_water_fraction.asc")
        h2gis.execute("DROP TABLE IF EXISTS water_grid; CALL ASCREAD('${grid_file.getAbsolutePath()}', 'water_grid')")
        assertTrue h2gis.firstRow("select count(*) as count from water_grid").count==6
    }

    @Test
    void testGrid_LCZ_PRIMARY() {
        String directory ="./target/geoclimate_chain_grid_lcz_primary"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : ["path": directory,
                                    "tables": ["grid_indicators", "zones", "rsu_lcz"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                                 "x_size": 1000,
                                 "y_size": 1000,
                                 "indicators": ["LCZ_PRIMARY"]
                         ]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertEquals(5, h2gis.firstRow("select count(*) as count from grid_indicators where LCZ_PRIMARY is not null").count)
        assertEquals(1,h2gis.firstRow("select count(*) as count from grid_indicators where LCZ_PRIMARY is null").count)
    }

    @Test
    void testLoggerZones() {
        String directory ="./target/geoclimate_chain_grid_logger"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : [[48.49749,5.25349,48.58082,5.33682]]],
                "output" :[
                        "folder" : ["path": directory,
                                    "tables": ["grid_indicators", "zones"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                                 "x_size": 10,
                                 "y_size": 10,
                                  rowCol: true,
                                 "indicators": ["WATER_FRACTION"],
                                 "output":"asc"
                         ]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Disabled //Use it for debug
    @Test
    void testIntegration() {
        String directory ="/tmp/geoclimate"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_test_integration;AUTO_SERVER=TRUE;",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Redon"],
                "timeout":3600,
                "maxsize": 536870918,
                "endpoint":"https://lz4.overpass-api.de/api"],
                "output" :[
                        "folder" :directory]
                ,
                "parameters":
                        ["distance" : 0,
                         "rsu_indicators":[
                                 "indicatorUse": ["LCZ"],
                                 "svfSimplified": true,
                                 "estimateHeight":true
                         ],"grid_indicators": [
                                "x_size": 100,
                                "y_size": 100,
                                //"rowCol": true,
                                "indicators":  ["BUILDING_FRACTION","BUILDING_HEIGHT", "BUILDING_POP",
                                                "BUILDING_TYPE_FRACTION","WATER_FRACTION","VEGETATION_FRACTION",
                                                "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "LCZ_FRACTION", "LCZ_PRIMARY",
                                                "FREE_EXTERNAL_FACADE_DENSITY"]
                        ],    "worldpop_indicators" : true
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }


    @Disabled //Use it for debug SLIM copernicus
    @Test
    void testIntegrationSlim() {
        String directory ="./target/geoclimate_slim_integration"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" :
                                   [[46.257330,4.870033,46.269970,4.905224]]
                                 ],
                "output" :["folder" : directory,srid: 4326]
                ,
                "parameters":
                        ["distance" : 0,
                         "rsu_indicators":[
                                 "indicatorUse": ["LCZ"],
                                 "svfSimplified": true,
                                 "estimateHeight":true
                         ],
                         "grid_indicators": [
                                 "x_size": 10000,
                                 "y_size": 10000,
                                 "rowCol": true,
                                 "output" : "asc",
                                 "indicators": ["BUILDING_FRACTION","BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION","WATER_FRACTION","VEGETATION_FRACTION",
                                                "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "LCZ_FRACTION"]
                         ]
                        ]
        ]

        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }


    @Test
    void testOSMTEB() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : directory],
                "parameters":
                        [
                                rsu_indicators:[
                                        "unit" :  "GRID",
                                        "indicatorUse": ["TEB"],
                                        "svfSimplified": true
                                ]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
    }


    @Test
    void testRoad_traffic() {
        String directory ="./target/geoclimate_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run only the road traffic estimation",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : ["path": directory,
                                    "tables": ["road_traffic"]]],
                "parameters":
                        ["road_traffic" : true]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: createOSMConfigFile(osm_parmeters, directory)))
        assertEquals(3,  process.getResults().output["Pont-de-Veyle"].size())
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from road_traffic where road_type is not null").count>0
    }


    @Disabled
    @Test
    void testPopulation_Indicators() {
        String directory ="./target/geoclimate_population"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run only the road traffic estimation",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "osm" : ["Pont-de-Veyle"]],
                "output" :[
                        "folder" : ["path": directory,
                                    "tables": ["building", "population"]]],
                "parameters":
                        ["worldpop_indicators" : true]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: osm_parmeters))
        assertEquals(10,  process.getResults().output["Pont-de-Veyle"].size())
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from ${process.results.output["Pont-de-Veyle"].populationTableName} where pop is not null").count>0
    }


    @Test
    void osmWrongAreaSize() {
        String directory ="./target/geoclimate_chain_db"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the result into a database",
                "geoclimatedb" : [
                        "folder" : dirFile.absolutePath,
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "locations" : ["Pont-de-Veyle"],
                        "area" : -1],
                "output" :[
                        "database" :
                                ["user" : "sa",
                                 "password":"",
                                 "url": "jdbc:h2://"+dirFile.absolutePath+File.separator+"geoclimate_chain_db_output;AUTO_SERVER=TRUE",
                                 "tables": [
                                         "rsu_indicators":"rsu_indicators",
                                         "rsu_lcz":"rsu_lcz" ]]],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: ["indicatorUse": ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        IProcess process = OSM.WorkflowOSM.workflow()
        assertFalse(process.execute(input: osm_parmeters))
    }

    /**
     * Create a configuration file
     * @param osmParameters
     * @param directory
     * @return
     */
    def createOSMConfigFile(def osmParameters, def directory){
        def json = JsonOutput.toJson(osmParameters)
        def configFilePath =  directory+File.separator+"osmConfigFile.json"
        File configFile = new File(configFilePath)
        if(configFile.exists()){
            configFile.delete()
        }
        configFile.write(json)
        return configFile.absolutePath
    }

    @Test //Integration tests
    @Disabled
    void testOSMConfigurationFile() {
        def configFile = getClass().getResource("config/osm_workflow_placename_folderoutput.json").toURI()
        //configFile =getClass().getResource("config/osm_workflow_envelope_folderoutput.json").toURI()
        IProcess process = OSM.WorkflowOSM.workflow()
        assertTrue(process.execute(input: configFile))
    }
}
