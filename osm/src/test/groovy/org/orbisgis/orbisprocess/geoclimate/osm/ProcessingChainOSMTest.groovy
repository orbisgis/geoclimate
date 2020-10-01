package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonOutput
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain

import static org.junit.jupiter.api.Assertions.*

class ProcessingChainOSMTest extends ChainProcessAbstractTest {

    @Disabled
    @Test
    void osmToRSU() {
        String directory ="./target/osm_processchain_geoindicators"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def h2GIS = H2GIS.open(dirFile.absolutePath+File.separator+'osm_chain_db;AUTO_SERVER=TRUE')
        def zoneToExtract = "Pont de veyle"
        IProcess process = OSM.buildGeoclimateLayers

        process.execute([datasource: h2GIS, zoneToExtract :zoneToExtract, distance: 0])

        def prefixName  = zoneToExtract.trim().split("\\s*(,|\\s)\\s*").join("_");

        // Create the RSU
        def prepareRSUData = Geoindicators.SpatialUnits.prepareRSUData()
        def createRSU = Geoindicators.SpatialUnits.createRSU()
        if (prepareRSUData([datasource        : h2GIS,
                             zoneTable         : process.getResults().outputZone,
                             roadTable         : process.getResults().outputRoad,
                             railTable         : process.getResults().outputRail,
                             vegetationTable   : process.getResults().outputVeget,
                             hydrographicTable : process.getResults().outputHydro,
                             prefixName        : prefixName])) {
            def saveTables = Geoindicators.DataUtils.saveTablesAsFiles()

            saveTables.execute( [inputTableNames: process.getResults().values(), delete:true
                                 , directory: directory, datasource: h2GIS])

            if (createRSU([datasource    : h2GIS,
                            inputTableName: prepareRSUData.results.outputTableName,
                            inputZoneTableName :process.getResults().outputZone,
                            prefixName    : prefixName])) {
                h2GIS.getTable(createRSU.results.outputTableName).save(dirFile.absolutePath+File.separator+"${prefixName}.geojson")
            }
        }
    }

    @Test
    void osmGeoIndicatorsFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        boolean saveResults = true
        String directory ="./target/osm_processchain_geoindicators"
        def prefixName = ""
        def indicatorUse = ["URBAN_TYPOLOGY", "LCZ", "TEB"]
        def svfSimplified = false

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
    void osmGeoIndicatorsFromApi() {
        String directory ="./target/osm_processchain_indicators"
        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = false

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osm_chain_db;AUTO_SERVER=TRUE")

        //Extract and transform OSM data
        def zoneToExtract = "Plessis-l'Évêque"

        IProcess prepareOSMData = OSM.buildGeoclimateLayers

        prepareOSMData.execute([datasource: datasource, zoneToExtract :zoneToExtract, distance: 0])

        String buildingTableName = prepareOSMData.getResults().outputBuilding

        String roadTableName = prepareOSMData.getResults().outputRoad

        String railTableName = prepareOSMData.getResults().outputRail

        String hydrographicTableName = prepareOSMData.getResults().outputHydro

        String vegetationTableName = prepareOSMData.getResults().outputVeget

        String zoneTableName = prepareOSMData.getResults().outputZone

        if(saveResults){
            println("Saving OSM GIS layers")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles
            saveTables.execute( [inputTableNames: [buildingTableName,roadTableName,railTableName,hydrographicTableName,
                                                   vegetationTableName,zoneTableName]
                                 , directory: dirFile.absolutePath, datasource: datasource])
        }

        def indicatorUse = ["TEB", "URBAN_TYPOLOGY", "LCZ"]

        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, datasource, zoneTableName, buildingTableName,roadTableName,railTableName,vegetationTableName,
                hydrographicTableName,saveResults, svfSimplified,indicatorUse,  prefixName)

    }

    @Test
    void osmLczFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        boolean saveResults = true
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

        IProcess geodindicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue geodindicators.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: railTableName, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights,lczRandomForest: true )
        assertTrue(datasource.getTable(geodindicators.results.outputTableBuildingIndicators).rowCount>0)
        assertNotNull(geodindicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuIndicators).rowCount>0)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuLcz).rowCount>0)
    }

    @Disabled
    @Test
    void osmWorkflowToH2Database() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["romainville"]],
                "output" :[
                        "database" :
                                ["user" : "sa",
                                "password":"sa",
                                 "url": "jdbc:h2://${dirFile.absolutePath+File.separator+"geoclimate_chain_db_output;AUTO_SERVER=TRUE"}",
                                 "tables": ["building_indicators":"building_indicators",
                                          "block_indicators":"block_indicators",
                                          "rsu_indicators":"rsu_indicators",
                                          "rsu_lcz":"rsu_lcz",
                                          "zones":"zones" ]]],
                "parameters":
                        ["distance" : 0,
                         "indicatorUse": ["LCZ", "TEB", "URBAN_TYPOLOGY"],
                         "svfSimplified": true,
                         "prefixName": "",
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_length": 1],
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Disabled
    @Test
    void osmWorkflowToPostGISDatabase() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["romainville"]],
                "output" :[
                        "database" :
                                ["user" : "orbisgis",
                                 "password":"orbisgis",
                                 "url": "jdbc:postgresql://localhost:5432/orbisgis_db",
                                 "tables": ["building_indicators":"building_indicators",
                                            "block_indicators":"block_indicators",
                                            "rsu_indicators":"rsu_indicators",
                                            "rsu_lcz":"rsu_lcz",
                                            "zones":"zones" ]]],
                "parameters":
                        ["distance" : 0,
                         "indicatorUse": ["LCZ", "TEB", "URBAN_TYPOLOGY"],
                         "svfSimplified": true,
                         "prefixName": "",
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_length": 1  ],
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOSMWorkflowFromPlaceNameWithSrid() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["romainville"]],
                "output" :[
                        "folder" : "${directory}",
                        "srid":"4326"],
                "parameters":
                        ["distance" : 0,
                         "indicatorUse": ["LCZ"],
                         "svfSimplified": true,
                         "prefixName": "",
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
        //Test the SRID of all output files
        def geoFiles = []
        def  folder = new File("./target/geoclimate_chain/osm_romainville")
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

    @Disabled
    @Test
    void testOSMWorkflowFromPlaceName() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["Redon"]],
                "output" :[
                        "folder" : "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))

    }

    @Disabled
    @Test
    void testOSMWorkflowFromBboxDeleteDBFalse() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :"false"
                ],
                "input" : [
                        "osm" : [[38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417]]],
                "output" :[
                        "folder" : "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Disabled
    @Test
    void testOSMWorkflowFromBbox() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : [[38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417]]],
                "output" :[
                        "folder" : "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
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
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["", [-3.0961382389068604, -3.1055688858032227,48.77155634881654,]]],
                "output" :[
                        "folder" : "$directory"]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
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
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["LE PONTET"]],
                "output" :[
                        "folder" : "$directory"],
                "parameters":
                        ["distance" : 100,
                         "indicatorUse": ["LCZ"],
                         "svfSimplified": true,
                         "prefixName": "",
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_length": 1 ,
                                  "terrain_roughness_class": 1 ],
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10
                        ]
        ]
        IProcess process = OSM.workflow
        assertFalse(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Disabled
    @Test
    void testOSMLCZ() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
            "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
            "geoclimatedb" : [
                "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                "delete" :true
            ],
            "input" : [
                "osm" : ["LE PONTET"]],
            "output" :[
                "folder" : "$directory"],
            "parameters":
            ["distance" : 100,
                "indicatorUse": ["LCZ"],
                "svfSimplified": true,
                "prefixName": "",
                "mapOfWeights":
                ["sky_view_factor": 1,
                    "aspect_ratio": 1,
                    "building_surface_fraction": 1,
                    "impervious_surface_fraction" : 1,
                    "pervious_surface_fraction": 1,
                    "height_of_roughness_elements": 1,
                    "terrain_roughness_length": 1  ],
                "hLevMin": 3,
                "hLevMax": 15,
                "hThresholdLev2": 10
            ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }


    @Disabled
    @Test
    void testOSMConfigurationFileWithoutIndicUse() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["romainville"]],
                "output" :[
                        "folder" : "$directory"],
                "parameters":
                        ["distance" : 1000,
                         "svfSimplified": false,
                         "prefixName": "",
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_length": 1  ],
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10
                        ]
        ]
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: createOSMConfigFile(osm_parmeters, directory)))
    }

    @Test
    void testOSMTEB() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["NOVES"]],
                "output" :[
                        "folder" : "$directory"],
                "parameters":
                        [
                         "indicatorUse": ["TEB"],
                         "svfSimplified": true,
                         "prefixName": "",
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_length": 1  ],
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10
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
        configFile =getClass().getResource("config/osm_workflow_envelope_folderoutput.json").toURI()
        IProcess process = OSM.workflow
        assertTrue(process.execute(configurationFile: configFile))
    }

    @Disabled //Enable this test to test some specific indicators
    @Test
    void testIndicators() {
        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = false

        H2GIS datasource = H2GIS.open("/home/ebocher/Autres/codes/geoclimate/geoindicators/target/rsuindicatorsdb;AUTO_SERVER=TRUE")

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

        def  prepareData = Geoindicators.SpatialUnits.prepareRSUData()
        assertTrue prepareData.execute([zoneTable: zoneTableName, roadTable: roadTableName,  railTable: '',
                                        vegetationTable : vegetationTableName,
                                        hydrographicTable :hydrographicTableName,
                                        prefixName: "prepare_rsu", datasource: datasource])

        def outputTableGeoms = prepareData.results.outputTableName

        assertNotNull datasource.getTable(outputTableGeoms)

        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: datasource])
        def outputTable = rsu.results.outputTableName
        assertTrue datasource.save(outputTable,'./target/rsu.shp', true)

        def  p =  Geoindicators.RsuIndicators.smallestCommunGeometry()
        assertTrue p.execute([
                rsuTable: outputTable,buildingTable: buildingTableName, roadTable:roadTableName,vegetationTable: vegetationTableName,waterTable: hydrographicTableName,
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
    void osmGridFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad = new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

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

        IProcess geoIndicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue geoIndicators.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: railTableName, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights,svfSimplified:true)

        assertTrue(datasource.getTable(geoIndicators.results.outputTableBuildingIndicators).rowCount > 0)
        assertNull(geoIndicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geoIndicators.results.outputTableRsuIndicators).rowCount > 0)
        assertTrue(datasource.getTable(geoIndicators.results.outputTableRsuLcz).rowCount > 0)

        // Define constant attributes
        def attribute = "lcz1"
        def gridTableName = 'gridTest'
        def GEOMETRIC_FIELD = 'the_geom'

        // Apply grid process on local geometry of the rsu_lcz zone
        datasource.execute("DROP TABLE IF EXISTS $gridTableName")
        def geometry = datasource.getSpatialTable(zoneTableName).getExtent(GEOMETRIC_FIELD)
        def sourceTableName = geoIndicators.results.outputTableRsuLcz
        geometry.setSRID(datasource.getSpatialTable(sourceTableName).srid)
        def gridProcess = Geoindicators.SpatialUnits.createGrid()
        assert gridProcess.execute(geometry: geometry,
                                   deltaX: 100,
                                   deltaY: 100,
                                   gridTableName: gridTableName,
                                   datasource: datasource)
        def targetTableName = gridProcess.results.outputTableName
        assert datasource.getSpatialTable(targetTableName)

        // Make Spatial Join between grid and rsu_lcz tables
        def idColumnTarget = "id"
        def spatialJoinTable = "gridSpatialJoinTest"
        datasource."$sourceTableName".the_geom.createSpatialIndex()
        datasource."$targetTableName".the_geom.createSpatialIndex()

        def spatialJoin = """  
                          DROP TABLE IF EXISTS $spatialJoinTable;
                          CREATE TABLE $spatialJoinTable 
                          AS SELECT   b.$idColumnTarget, a.$attribute,
                                      ST_AREA(ST_INTERSECTION(st_force2d(st_makevalid(a.$GEOMETRIC_FIELD)), 
                                      st_force2d(st_makevalid(b.$GEOMETRIC_FIELD)))) AS AREA
                          FROM $sourceTableName a, $targetTableName b
                          WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD AND 
                                ST_INTERSECTS(st_force2d(a.$GEOMETRIC_FIELD), st_force2d(b.$GEOMETRIC_FIELD));
                          """
        datasource.execute(spatialJoin)

        // Save distinct attribute values
        def queryValues = """
                          SELECT DISTINCT $attribute
                          AS val FROM $spatialJoinTable
                          """
        // Make pivot
        def gridAttributeTableName = "gridAttributeTest"
        def list = datasource.rows(queryValues)
        def query = """
                    DROP TABLE IF EXISTS $gridAttributeTableName;
                    CREATE TABLE $gridAttributeTableName
                    AS SELECT $idColumnTarget
                    """
        list.each {query += ", SUM(lcz_${it.val}) AS lcz_${it.val}"}
        query += " FROM ( SELECT $idColumnTarget"
        list.each {
            query += """
                     , CASE WHEN $attribute=$it.val 
                     THEN SUM(area) ELSE 0 END 
                     AS lcz_${it.val}
                     """}
        query += """ 
                 FROM $spatialJoinTable 
                 GROUP BY $idColumnTarget, $attribute) 
                 GROUP BY $idColumnTarget;
                 """
        datasource.execute(query)

        // Join tables
        def gridAreaTableName = "gridAreaTest"
        def qjoin = """
                    DROP TABLE IF EXISTS $gridAreaTableName; 
                    CREATE TABLE $gridAreaTableName
                    AS SELECT b.$idColumnTarget, b.$GEOMETRIC_FIELD
                    """
        list.each {qjoin += ", NVL(lcz_${it.val}, 0) AS lcz_${it.val}"}
        qjoin += """
                 FROM $targetTableName b 
                 LEFT JOIN $gridAttributeTableName a 
                 ON (a.$idColumnTarget = b.$idColumnTarget);
                 """
        datasource.execute(qjoin)

        // Save tables
        datasource.getTable(gridAreaTableName).save("./target"+File.separator+"$gridAreaTableName"+".geojson")
        datasource.getSpatialTable(targetTableName).save("./target"+File.separator+"$targetTableName"+".geojson")
    }
}
