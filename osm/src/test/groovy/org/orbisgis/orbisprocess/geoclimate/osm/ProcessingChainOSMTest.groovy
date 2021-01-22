package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonOutput
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain

import static org.junit.jupiter.api.Assertions.*

class ProcessingChainOSMTest extends ChainProcessAbstractTest {

    @Test
    void osmToRSU() {
        String directory = "./target/osm_processchain_geoindicators_rsu"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def h2GIS = H2GIS.open(dirFile.absolutePath + File.separator + 'osm_chain_db;AUTO_SERVER=TRUE')
        def zoneToExtract = "Pont-de-Veyle"
        IProcess process = OSM.buildGeoclimateLayers

        process.execute([datasource: h2GIS, zoneToExtract: zoneToExtract, distance: 0])

        def prefixName = zoneToExtract.trim().split("\\s*(,|\\s)\\s*").join("_");

        // Create the RSU
        def prepareRSUData = Geoindicators.SpatialUnits.prepareRSUData()
        def createRSU = Geoindicators.SpatialUnits.createRSU()
        if (prepareRSUData([datasource       : h2GIS,
                            zoneTable        : process.getResults().outputZone,
                            roadTable        : process.getResults().outputRoad,
                            railTable        : process.getResults().outputRail,
                            vegetationTable  : process.getResults().outputVeget,
                            hydrographicTable: process.getResults().outputHydro,
                            prefixName       : prefixName])) {
            def saveTables = Geoindicators.DataUtils.saveTablesAsFiles()

            saveTables.execute([inputTableNames: process.getResults().values(), delete: true
                                , directory    : directory, datasource: h2GIS])

            if (createRSU([datasource        : h2GIS,
                           inputTableName    : prepareRSUData.results.outputTableName,
                           inputZoneTableName: process.getResults().outputZone,
                           prefixName        : prefixName])) {
                //TODO enable it for debug purpose
                // h2GIS.getTable(createRSU.results.outputTableName).save(dirFile.absolutePath+File.separator+"${prefixName}.geojson", true)
                assertTrue(h2GIS.getTable(createRSU.results.outputTableName).getRowCount() > 0)
            }
        }
    }

    @Test
    void osmGeoIndicatorsFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad = new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        //TODO enable it for debug purpose
        boolean saveResults = false
        String directory = "./target/osm_processchain_geoindicators_redon"
        def prefixName = ""
        def indicatorUse = ["URBAN_TYPOLOGY", "LCZ", "TEB"]
        def svfSimplified = true

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")

        String zoneTableName = "zone"
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
        datasource.load(urlZone, zoneTableName, true)

        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath + File, datasource, zoneTableName, buildingTableName, roadTableName,
                null, vegetationTableName, hydrographicTableName, saveResults, svfSimplified, indicatorUse, prefixName)

    }

    @Test
    void osmGeoIndicatorsFromApi() {
        String directory = "./target/osm_processchain_indicators"
        //TODO enable it for debug purpose
        boolean saveResults = false
        def prefixName = ""
        def svfSimplified = true
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")

        //Extract and transform OSM data
        def zoneToExtract = "Pont-de-Veyle"

        IProcess prepareOSMData = OSM.buildGeoclimateLayers

        prepareOSMData.execute([datasource: datasource, zoneToExtract: zoneToExtract, distance: 0])

        String buildingTableName = prepareOSMData.getResults().outputBuilding

        String roadTableName = prepareOSMData.getResults().outputRoad

        String railTableName = prepareOSMData.getResults().outputRail

        String hydrographicTableName = prepareOSMData.getResults().outputHydro

        String vegetationTableName = prepareOSMData.getResults().outputVeget

        String zoneTableName = prepareOSMData.getResults().outputZone

        if (saveResults) {
            println("Saving OSM GIS layers")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles
            saveTables.execute([inputTableNames: [buildingTableName, roadTableName, railTableName, hydrographicTableName,
                                                  vegetationTableName, zoneTableName]
                                , directory    : dirFile.absolutePath, datasource: datasource])
        }

        def indicatorUse = ["TEB", "URBAN_TYPOLOGY", "LCZ"]

        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, datasource, zoneTableName, buildingTableName, roadTableName, railTableName, vegetationTableName,
                hydrographicTableName, saveResults, svfSimplified, indicatorUse, prefixName)

    }


    @Disabled
    @Test
    void osmLczFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad = new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        //TODO enable it for debug purpose
        boolean saveResults = false
        String directory = "./target/osm_processchain_lcz"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath + File.separator + "osmchain_lcz;AUTO_SERVER=TRUE")

        String zoneTableName = "zone"
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
        datasource.load(urlZone, zoneTableName, true)

        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        IProcess geodindicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue geodindicators.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: railTableName, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights, svfSimplified: true)
        assertTrue(datasource.getTable(geodindicators.results.outputTableBuildingIndicators).rowCount > 0)
        assertNotNull(geodindicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuIndicators).rowCount > 0)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuLcz).rowCount > 0)
    }

    @Test
    void osmWorkflowToH2Database() {
        String directory = "./target/geoclimate_chain_db"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result into a database",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "database":
                                ["user"    : "sa",
                                 "password": "",
                                 "url"     : "jdbc:h2://${dirFile.absolutePath + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE"}",
                                 "tables"  : [
                                         "rsu_indicators": "rsu_indicators",
                                         "rsu_lcz"       : "rsu_lcz"]]],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true,
                                          "mapOfWeights" :
                                                  ["sky_view_factor"             : 1,
                                                   "aspect_ratio"                : 1,
                                                   "building_surface_fraction"   : 1,
                                                   "impervious_surface_fraction" : 1,
                                                   "pervious_surface_fraction"   : 1,
                                                   "height_of_roughness_elements": 1,
                                                   "terrain_roughness_length"    : 1]]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
        H2GIS outputdb = H2GIS.open(dirFile.absolutePath + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE")
        def rsu_indicatorsTable = outputdb.getTable("rsu_indicators")
        assertNotNull(rsu_indicatorsTable)
        assertTrue(rsu_indicatorsTable.getRowCount() > 0)
        def rsu_lczTable = outputdb.getTable("rsu_lcz")
        assertNotNull(rsu_lczTable)
        assertTrue(rsu_lczTable.getRowCount() > 0)
    }

    @Test
    void osmWorkflowToPostGISDatabase() {
        String directory = "./target/geoclimate_chain_postgis"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "database":
                                ["user"    : "orbisgis",
                                 "password": "orbisgis",
                                 "url"     : "jdbc:postgresql://localhost:5432/orbisgis_db",
                                 "tables"  : [
                                         "rsu_indicators" : "rsu_indicators",
                                         "rsu_lcz"        : "rsu_lcz",
                                         "zones"          : "zones",
                                         "grid_indicators": "grid_indicators"]]],
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
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
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
            def zonesTable = postgis.getTable("zones")
            assertNotNull(zonesTable)
            assertTrue(zonesTable.getRowCount() > 0)
            def gridTable = postgis.getTable("grid_indicators")
            assertNotNull(gridTable)
            assertTrue(gridTable.getRowCount() > 0)
        }
    }

    @Test
    void testOSMWorkflowFromPlaceNameWithSrid() {
        String directory = "./target/geoclimate_chain_srid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": "${directory}",
                        "srid"  : "4326"],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
        //Test the SRID of all output files
        def geoFiles = []
        def folder = new File("${directory + File.separator}osm_Pont-de-Veyle")
        folder.eachFileRecurse groovy.io.FileType.FILES, { file ->
            if (file.name.toLowerCase().endsWith(".geojson")) {
                geoFiles << file.getAbsolutePath()
            }
        }
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        geoFiles.eachWithIndex { geoFile, index ->
            def tableName = h2gis.load(geoFile, true)
            assertEquals(4326, h2gis.getSpatialTable(tableName).srid)
        }
    }

    @Test
    void testOSMWorkflowFromPlaceName() {
        String directory = "./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": "$directory"],
                "parameters"  :
                        [rsu_indicators: ["indicatorUse" : ["LCZ"],
                                          "svfSimplified": true]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))

    }

    @Disabled
    @Test
    void testOSMWorkflowFromBboxDeleteDBFalse() {
        String directory = "./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": [[38.89557963573336, -77.03930318355559, 38.89944983078282, -77.03364372253417]]],
                "output"      : [
                        "folder": "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOSMWorkflowFromBbox() {
        String directory = "./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": [[38.89557963573336, -77.03930318355559, 38.89944983078282, -77.03364372253417]]],
                "output"      : [
                        "folder": "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOSMWorkflowBadOSMFilters() {
        String directory = "./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": ["", [-3.0961382389068604, -3.1055688858032227, 48.77155634881654,]]],
                "output"      : [
                        "folder": "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void workflowWrongMapOfWeights() {
        String directory = "./target/bdtopo_workflow"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": "$directory"],
                "parameters"  :
                        ["distance"      : 100,
                         "hLevMin"       : 3,
                         "hLevMax"       : 15,
                         "hThresholdLev2": 10,
                         rsu_indicators  : ["indicatorUse" : ["LCZ"],
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
        IProcess process = OSM.workflow
        assertFalse(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Disabled
    @Test
    void testOSMEstimatedHeight() {
        String directory = "./target/geoclimate_chain_estimated_height"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": "$directory"],
                "parameters"  :
                        ["distance"    : 0,
                         rsu_indicators: ["indicatorUse"  : ["LCZ", "URBAN_TYPOLOGY"],
                                          "svfSimplified" : true,
                                          "estimateHeight": true]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOnlyEstimateHeight() {
        String directory = "./target/geoclimate_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run only the estimated height model",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": ["path"  : "$directory",
                                   "tables": ["building"]]],
                "parameters"  :
                        ["distance"      : 0,
                         "rsu_indicators": [
                                 "svfSimplified" : true,
                                 "estimateHeight": true
                         ]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count > 0
    }


    @Test
    void testGrid_Indicators() {
        String directory = "./target/geoclimate_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": ["path"  : "$directory",
                                   "tables": ["zones","rsu_indicators", "rsu_lcz","grid_indicators"]]],
                "parameters"  :
                        ["distance"       : 0,
                         "grid_indicators": [
                                 "x_size"    : 1000,
                                 "y_size"    : 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count > 0
    }

    @Disabled
    //Use it for debug
    @Test
    void testIntegration() {
        String directory = "./target/geoclimate_chain_integration"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "osm": ["Nantes"]],
                "output"      : [
                        "folder": [path: "$directory"], "tables": [
                        "building_indicators": "building_indicators"
                ]],
                "parameters"  :
                        ["distance"      : 0,
                         "rsu_indicators": [
                                 "indicatorUse"  : ["LCZ", "URBAN_TYPOLOGY"],
                                 "svfSimplified" : true,
                                 "estimateHeight": true
                         ]
                        ]
        ]

        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
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
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" : [
                        "osm" : [[
                                         71.16667,
                                         25.58318,
                                         71.25,
                                         25.66652
                                 ]]],
                "output" :["folder" : "$directory"]
                ,
                "parameters":
                        ["distance" : 0,
                         "rsu_indicators":[
                                 "indicatorUse": ["LCZ"],
                                 "svfSimplified": true,
                                 "estimateHeight":true
                         ],
                         "grid_indicators": [
                                 "x_size": 1000,
                                 "y_size": 1000,
                                 "indicators": ["BUILDING_FRACTION","BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION","WATER_FRACTION","VEGETATION_FRACTION",
                                                "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "LCZ_FRACTION"]
                         ]
                        ]
        ]

        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOSMTEB() {
        String directory = "./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" : "Example of configuration file to run the OSM workflow and store the result in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "osm": ["Pont-de-Veyle"]],
                "output"      : [
                        "folder": "$directory"],
                "parameters"  :
                        [
                                rsu_indicators: [
                                        "indicatorUse" : ["TEB"],
                                        "svfSimplified": true
                                ]
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
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

    @Test
    //Integration tests
    @Disabled
    void testOSMConfigurationFile() {
        def configFile = getClass().getResource("config/osm_workflow_placename_folderoutput.json").toURI()
        //configFile =getClass().getResource("config/osm_workflow_envelope_folderoutput.json").toURI()
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: configFile))
    }

    @Disabled
    //Enable this test to test some specific indicators
    @Test
    void testIndicators() {
        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = false

        H2GIS datasource = H2GIS.open("./target/rsuindicatorsdb;AUTO_SERVER=TRUE")

        //Extract and transform OSM data
        def zoneToExtract = "Rennes"

        IProcess prepareOSMData = OSM.buildGeoclimateLayers

        prepareOSMData.execute([datasource: datasource, zoneToExtract: zoneToExtract, distance: 0])

        String buildingTableName = prepareOSMData.getResults().outputBuilding

        String roadTableName = prepareOSMData.getResults().outputRoad

        String railTableName = prepareOSMData.getResults().outputRail

        String hydrographicTableName = prepareOSMData.getResults().outputHydro

        String vegetationTableName = prepareOSMData.getResults().outputVeget

        String zoneTableName = prepareOSMData.getResults().outputZone

        def prepareData = Geoindicators.SpatialUnits.prepareRSUData()
        assertTrue prepareData.execute([zoneTable        : zoneTableName, roadTable: roadTableName, railTable: '',
                                        vegetationTable  : vegetationTableName,
                                        hydrographicTable: hydrographicTableName,
                                        prefixName       : "prepare_rsu", datasource: datasource])

        def outputTableGeoms = prepareData.results.outputTableName

        assertNotNull datasource.getTable(outputTableGeoms)

        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: datasource])
        def outputTable = rsu.results.outputTableName
        assertTrue datasource.save(outputTable, './target/rsu.shp', true)

        def p = Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                rsuTable  : outputTable, buildingTable: buildingTableName, roadTable: roadTableName, vegetationTable: vegetationTableName, waterTable: hydrographicTableName,
                prefixName: "test", datasource: datasource])
        def outputTableStats = p.results.outputTableName

        datasource.execute """DROP TABLE IF EXISTS stats_rsu;
                    CREATE INDEX ON $outputTableStats (ID_RSU);
                   CREATE TABLE stats_rsu AS SELECT b.the_geom,
                round(sum(CASE WHEN a.low_vegetation=1 THEN a.area ELSE 0 END),1) AS low_VEGETATION_sum,
                round(sum(CASE WHEN a.high_vegetation=1 THEN a.area ELSE 0 END),1) AS high_VEGETATION_sum,
                round(sum(CASE WHEN a.water=1 THEN a.area ELSE 0 END),1) AS water_sum,
                round(sum(CASE WHEN a.road=1 THEN a.area ELSE 0 END),1) AS road_sum,
                round(sum(CASE WHEN a.building=1 THEN a.area ELSE 0 END),1) AS building_sum,
                FROM $outputTableStats AS a, $outputTable b WHERE a.id_rsu=b.id_rsu GROUP BY b.id_rsu"""

        datasource.save("stats_rsu", './target/stats_rsu.shp', true)

    }

    @Test
    void TestAsciiGrid() {

        H2GIS h2gis_datasource = H2GIS.open("D:/Users/le_sauxe/Documents/IUT/Recherche/Labsticc/SLIM/geoclimate_chain/slim_1610967941742;AUTO_SERVER=TRUE")
        def output_files_generic_name = "D:/Users/le_sauxe/Documents/IUT/Recherche/Labsticc/SLIM/geoclimate_chain/"
        def h2gis_table_to_save = "GRID_INDICATORS"
        def query = "select max(id_col) as cmax, max(id_row) as rmax from GRID_INDICATORS"
        def nbcols
        def nbrows
        h2gis_datasource.eachRow(query){row ->
            nbcols = row.cmax
            nbrows = row.rmax
        }
        def env = h2gis_datasource.getSpatialTable(h2gis_table_to_save).getExtent().getEnvelopeInternal();
        def xmin =env.getMinX()
        def ymin =env.getMinY()

        def IndicsTable = h2gis_datasource."$h2gis_table_to_save"
        List columnNames = IndicsTable.columns
        columnNames.remove("THE_GEOM")
        columnNames.remove("ID")
        columnNames.remove("ID_COL")
        columnNames.remove("ID_ROW")
        columnNames.each { it ->
            new File("${output_files_generic_name}${it}.asc").withOutputStream { stream ->
                stream.write "ncols $nbcols\nnrows $nbrows\nxllcorner $xmin\nyllcorner $ymin\ncellsize ???\nnodata_value -9999\n".getBytes()
                query = "select id_row, id_col, $it from $h2gis_table_to_save order by id_row, id_col"
                String fileContent = ""
                h2gis_datasource.eachRow(query) { row ->
                    fileContent += row.getString(it) + " "
                    if (row.getInt("id_col") == nbcols) {
                        fileContent += "\n"
                        stream.write fileContent.getBytes()
                        fileContent=""
                    }
                }
            }
        }
    }
}