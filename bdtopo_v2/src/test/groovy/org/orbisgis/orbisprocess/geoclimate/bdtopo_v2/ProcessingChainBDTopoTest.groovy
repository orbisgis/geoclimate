package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import groovy.json.JsonOutput
import org.h2gis.utilities.FileUtilities
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain
import static org.junit.jupiter.api.Assertions.*

class ProcessingChainBDTopoTest extends ChainProcessAbstractTest{

    def static postgis_dbProperties = [databaseName: 'orbisgis_db',
                               user        : 'orbisgis',
                               password    : 'orbisgis',
                               url         : 'jdbc:postgresql://localhost:5432/'
    ]
    static POSTGIS postGIS;


    private static communeToTest = "12174"

    private static def h2db = "./target/bdtopo_chain"
    private static def bdtopoFoldName = "sample_$communeToTest"
    private static def listTables = ["IRIS_GE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
    "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "TRONCON_VOIE_FERREE", "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
    "SURFACE_ROUTE", "SURFACE_ACTIVITE"]
    private static def paramTables = ["BUILDING_ABSTRACT_PARAMETERS", "BUILDING_ABSTRACT_USE_TYPE", "BUILDING_BD_TOPO_USE_TYPE",
                                     "RAIL_ABSTRACT_TYPE", "RAIL_BD_TOPO_TYPE", "RAIL_ABSTRACT_CROSSING",
                                     "RAIL_BD_TOPO_CROSSING", "ROAD_ABSTRACT_PARAMETERS", "ROAD_ABSTRACT_SURFACE",
                                     "ROAD_ABSTRACT_CROSSING", "ROAD_BD_TOPO_CROSSING", "ROAD_ABSTRACT_TYPE",
                                     "ROAD_BD_TOPO_TYPE", "VEGET_ABSTRACT_PARAMETERS", "VEGET_ABSTRACT_TYPE",
                                     "VEGET_BD_TOPO_TYPE"]

    @BeforeAll
    static void beforeAll(){
        postGIS = POSTGIS.open(postgis_dbProperties)
        System.setProperty("test.postgis", Boolean.toString(postGIS != null))
    }

    /**
     * Return the path of the data sample
     * @return
     */
    private String getDataFolderPath(){
        if( new File(getClass().getResource(bdtopoFoldName).toURI()).isDirectory()){
                return new File(getClass().getResource(bdtopoFoldName).toURI()).absolutePath
        }
        return null;
    }

    private def loadFiles(String dbPath){
        H2GIS h2GISDatabase = H2GIS.open(dbPath)
        // Load parameter files
        paramTables.each{
            h2GISDatabase.load(getClass().getResource(it+".csv"), it, true)
        }

        def relativePath = bdtopoFoldName

        // Test whether there is a folder containing .shp files for the corresponding INSEE code
        if(getClass().getResource(relativePath)){
            // Test is the URL is a folder
            if(new File(getClass().getResource(relativePath).toURI()).isDirectory()){
                listTables.each {
                    def filePath = getClass().getResource(relativePath + File.separator + it + ".shp")
                    // If some layers are missing, do not try to load them...
                    if (filePath) {
                        h2GISDatabase.load(filePath, it, true)
                    }
                }
            }
            else{
                fail("There is no folder containing shapefiles for commune insee $inseeCode")
            }
        }
        else{
            fail("There is no folder containing shapefiles for commune insee $inseeCode")
        }
        return h2GISDatabase
    }

    @Test
    void prepareBDTopoTest(){
        String directory ="./target/bdtopo_chain_prepare"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        def process = BDTopo_V2.prepareData
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    tablePiste_AerodromeName : 'PISTE_AERODROME',
                                    tableReservoirName :'RESERVOIR',
                                    distBuffer: 0, distance: 0, idZone: communeToTest,
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        process.getResults().each {entry ->
            assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }
    }

    @Test
    void createUnitsOfAnalysisTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchainscales;AUTO_SERVER=TRUE")
        String sqlString = new File(getClass().getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT id_road, the_geom, zindex, crossing, type FROM road_test WHERE id_road < 5 OR id_road > 6;" +
                "CREATE TABLE tempo_zone AS SELECT * FROM zone_test;" +
                "CREATE TABLE tempo_veget AS SELECT id_veget, the_geom FROM veget_test WHERE id_veget < 4;" +
                "CREATE TABLE tempo_hydro AS SELECT id_hydro, the_geom FROM hydro_test WHERE id_hydro < 2;")
        IProcess pm =  ProcessingChain.GeoIndicatorsChain.createUnitsOfAnalysis()
        pm.execute([datasource          : h2GIS,        zoneTable               : "tempo_zone",     roadTable           : "tempo_road",
                    railTable           : "", vegetationTable         : "tempo_veget",    hydrographicTable   : "tempo_hydro",
                    surface_vegetation  : null,         surface_hydro           : null,             buildingTable       : "tempo_build",
                    distance            : 0.0,          prefixName              : "test",           indicatorUse        :["LCZ",
                                                                                                                          "URBAN_TYPOLOGY",
                                                                                                                          "TEB"]])

        // Test the number of blocks within RSU ID 2, whether id_build 4 and 8 belongs to the same block and are both
        // within id_rsu = 2
        def row_nb = h2GIS.firstRow(("SELECT COUNT(*) AS nb_blocks FROM ${pm.results.outputTableBlockName} " +
                "WHERE id_rsu = 2").toString())
        def row_bu4 = h2GIS.firstRow(("SELECT id_block AS id_block FROM ${pm.results.outputTableBuildingName} " +
                "WHERE id_build = 4 AND id_rsu = 2").toString())
        def row_bu8 = h2GIS.firstRow(("SELECT id_block AS id_block FROM ${pm.results.outputTableBuildingName} " +
                "WHERE id_build = 8 AND id_rsu = 2").toString())
    }



    @Test
    void bdtopoLczFromTestFiles() {
        String directory ="./target/bdtopo_chain_lcz"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS datasource = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        def process = BDTopo_V2.prepareData
        assertTrue process.execute([datasource: datasource,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    tablePiste_AerodromeName : 'PISTE_AERODROME',
                                    tableReservoirName :'RESERVOIR',
                                    distBuffer: 500, distance: 1000, idZone: communeToTest,
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        def abstractTables = process.getResults()

        // Define the weights of each indicator in the LCZ creation
        def mapOfWeights =  ["sky_view_factor"                : 4,
                            "aspect_ratio"                   : 3,
                            "building_surface_fraction"      : 8,
                            "impervious_surface_fraction"    : 0,
                            "pervious_surface_fraction"      : 0,
                            "height_of_roughness_elements"   : 6,
                            "terrain_roughness_length"       : 0.5]
        def svfSimplified = true
        IProcess geodindicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue geodindicators.execute(datasource: datasource, zoneTable: abstractTables.outputZone,
                buildingTable: abstractTables.outputBuilding, roadTable: abstractTables.outputRoad,
                railTable: abstractTables.outputRail, vegetationTable: abstractTables.outputVeget,
                hydrographicTable: abstractTables.outputHydro, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights, svfSimplified:svfSimplified)

        assertTrue(datasource.getTable(geodindicators.results.outputTableBuildingIndicators).rowCount>0)
        assertNull(geodindicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuIndicators).rowCount>0)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuLcz).rowCount>0)
    }


    @Test
    void bdtopoGeoIndicatorsFromTestFiles() {
        String directory ="./target/bdtopo_chain_geoindicators"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        def process = BDTopo_V2.prepareData
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    tablePiste_AerodromeName : 'PISTE_AERODROME',
                                    tableReservoirName :'RESERVOIR',
                                    distBuffer: 500, distance: 1000, idZone: communeToTest,
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        def abstractTables = process.getResults()

        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = true //Fast test
        def indicatorUse = ["TEB", "URBAN_TYPOLOGY", "LCZ"]
        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, h2GISDatabase, abstractTables.outputZone, abstractTables.outputBuilding,
                abstractTables.outputRoad, abstractTables.outputRail, abstractTables.outputVeget,
                abstractTables.outputHydro, saveResults, svfSimplified, indicatorUse,  prefixName)
    }

    // Test the workflow on the commune INSEE 01306 only for TEB in order to verify that only RSU_INDICATORS and BUILDING_INDICATORS are saved
    @Test
    @Disabled
    void testBDTOPO_V2Workflow() {
        String directory ="./target/geoclimate_chain/bdtopo_config/"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        IProcess processBDTopo = BDTopo_V2.workflow
        assertTrue(processBDTopo.execute(configurationFile: getClass().getResource("config/bdtopo_workflow_folderinput_folderoutput_id_zones.json").toURI()))
        assertNotNull(processBDTopo.getResults().outputFolder)
        def baseNamePathAndFileOut = processBDTopo.getResults().outputFolder + File.separator + "zone_" + communeToTest + "_"
        assertTrue(new File(baseNamePathAndFileOut + "rsu_indicators.geojson").exists())
        assertFalse(new File(baseNamePathAndFileOut + "rsu_lcz.geojson").exists())
        assertFalse(new File(baseNamePathAndFileOut + "block_indicators.geojson").exists())
        assertTrue(new File(baseNamePathAndFileOut + "building_indicators.geojson").exists())
    }

    @Test //Integration tests
    @Disabled
    void testBDTopoConfigurationFile() {
        def configFile = getClass().getResource("config/bdtopo_workflow_folderinput_folderoutput.json").toURI()
        //configFile =getClass().getResource("config/bdtopo_workflow_folderinput_folderoutput_id_zones.json").toURI()
        //configFile =getClass().getResource("config/bdtopo_workflow_folderinput_dboutput.json").toURI()
        //configFile =getClass().getResource("config/bdtopo_workflow_dbinput_dboutput.json").toURI()
        IProcess process = BDTopo_V2.workflow
        process.execute(configurationFile:configFile)
    }

    @Test
    void workflowWrongMapOfWeights() {
        String directory ="./target/bdtopo_workflow"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "folder": ["path" :".../processingChain",
                                   "id_zones":["56350"]]],
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
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10,
                          rsu_indicators: [
                         "indicatorUse": ["LCZ", "TEB", "URBAN_TYPOLOGY"],
                         "svfSimplified": true,
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_class": 1  ]]
                        ]
        ]
        IProcess process = BDTopo_V2.workflow
        assertFalse(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
    }

    @Disabled
    @Test
    void workflowFolderToDatabase() {
        String directory ="./target/bdtopo_workflow"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "folder": ["path" :".../processingChain",
                            "id_zones":["-", "-"]]],
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
                         "hLevMin": 3,
                         "hLevMax": 15,
                         "hThresholdLev2": 10,
                         rsu_indicators: [
                         "indicatorUse": ["LCZ", "TEB", "URBAN_TYPOLOGY"],
                         "svfSimplified": true,
                         "mapOfWeights":
                                 ["sky_view_factor": 1,
                                  "aspect_ratio": 1,
                                  "building_surface_fraction": 1,
                                  "impervious_surface_fraction" : 1,
                                  "pervious_surface_fraction": 1,
                                  "height_of_roughness_elements": 1,
                                  "terrain_roughness_class": 1  ]]
                        ]
        ]
        IProcess process = BDTopo_V2.workflow
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
    }

    @Test
    void runBDTopoWorkflow(){
        def inseeCode = communeToTest
        def defaultParameters = [distance: 1000,distance_buffer:500,prefixName: "",
                                 "hLevMin": 3,
                                 "hLevMax": 15,
                                 "hThresholdLev2": 10,
                                 rsu_indicators: [ indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"],
                                 svfSimplified:true,
                                 mapOfWeights : ["sky_view_factor" : 2, "aspect_ratio": 1, "building_surface_fraction": 4,
                                                 "impervious_surface_fraction" : 0, "pervious_surface_fraction": 0,
                                                 "height_of_roughness_elements": 3, "terrain_roughness_length": 1]
                                 ]]
        String directory ="./target/bdtopo_chain_workflow"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        def process = new WorkflowBDTopo_V2().bdtopo_processing(h2GISDatabase, defaultParameters, inseeCode, null, null, null, null, 0);
        checkSpatialTable(h2GISDatabase, "block_indicators")
        checkSpatialTable(h2GISDatabase, "building_indicators")
        checkSpatialTable(h2GISDatabase, "rsu_indicators")
        checkSpatialTable(h2GISDatabase, "rsu_lcz")
    }

    @Test
    void runBDTopoWorkflowWithSRID(){
        def inseeCode = communeToTest
        def defaultParameters = [distance: 1000,distance_buffer:500,  prefixName: "",
                                 rsu_indicators: [
                                 indicatorUse: ["LCZ"],
                                 svfSimplified:true,
                                 mapOfWeights : ["sky_view_factor" : 2, "aspect_ratio": 1, "building_surface_fraction": 4,
                                                 "impervious_surface_fraction" : 0, "pervious_surface_fraction": 0,
                                                 "height_of_roughness_elements": 3, "terrain_roughness_length": 1]]]
        String directory ="./target/bdtopo_chain_workflow"
        File dirFile = new File(directory)
        if(dirFile.exists()){
            FileUtilities.deleteFiles(dirFile, true)
        }
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        dirFile.mkdir()
        def tablesToSave = ["building_indicators",
                            "block_indicators",
                            "rsu_indicators",
                            "rsu_lcz",
                            "zones",
                            "building",
                            "road",
                            "rail" ,
                            "water",
                            "vegetation",
                            "impervious"]
        def process = new WorkflowBDTopo_V2().bdtopo_processing(h2GISDatabase, defaultParameters, inseeCode, dirFile, tablesToSave, null, null, 4326);
        checkSpatialTable(h2GISDatabase, "block_indicators")
        checkSpatialTable(h2GISDatabase, "building_indicators")
        checkSpatialTable(h2GISDatabase, "rsu_indicators")
        checkSpatialTable(h2GISDatabase, "rsu_lcz")
        def geoFiles = []
        def  folder = new File("./target/bd_topo_workflow_srid/bdtopo_v2_12174")
        folder.eachFileRecurse groovy.io.FileType.FILES,  { file ->
            if (file.name.toLowerCase().endsWith(".geojson")) {
                geoFiles << file.getAbsolutePath()
            }
        }
        geoFiles.eachWithIndex { geoFile , index->
            def tableName = h2GISDatabase.load(geoFile, true)
            assertEquals(4326, h2GISDatabase.getSpatialTable(tableName).srid)
        }
    }

    @EnabledIfSystemProperty(named = "test.postgis", matches = "true")
    @Test
    void workflowPostGIS() {
        def outputTables =["building_indicators":"building_indicators",
                           "block_indicators":"block_indicators",
                           "rsu_indicators":"rsu_indicators",
                           "rsu_lcz":"rsu_lcz",
                           "zones":"zones" ]
        //Drop all output tables if exist
        postGIS.execute("DROP TABLE IF EXISTS ${outputTables.values().join(",")};");
        String directory ="./target/bdtopo_workflow_postgis"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete" :true
                ],
                "input" : [
                        "folder": ["path" :dataFolder,
                                   "id_zones":[communeToTest]]],
                "output" :[
                        "database" :
                                ["user" : postgis_dbProperties.user,
                                 "password":postgis_dbProperties.password,
                                 "url": postgis_dbProperties.url+postgis_dbProperties.databaseName,
                                 "tables": outputTables]],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: [
                         "indicatorUse": ["LCZ", "URBAN_TYPOLOGY"],
                         "svfSimplified": true
                          ]
                        ]
        ]
        IProcess process = BDTopo_V2.workflow
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        //Check if the tables exist and contains at least one row
        outputTables.values().each {it->
            def spatialTable = postGIS.getSpatialTable(it)
            assertNotNull(spatialTable)
            assertTrue(spatialTable.getRowCount()>0)
        }

    }


    /**
     * Check if the table exist and contains at least one row
     * @param datasource
     * @param tableName
     * @return
     */
    private static def checkSpatialTable(JdbcDataSource datasource, def tableName){
        assertTrue(datasource.hasTable(tableName))
        assertTrue(datasource.getSpatialTable(tableName).getRowCount()>0)
    }
    /**
     * Create a configuration file
     * @param bdTopoParameters
     * @param directory
     * @return
     */
    private static def createConfigFile(def bdTopoParameters, def directory){
        def json = JsonOutput.toJson(bdTopoParameters)
        def configFilePath =  directory+File.separator+"bdTopoConfigFile.json"
        File configFile = new File(configFilePath)
        if(configFile.exists()){
            configFile.delete()
        }
        configFile.write(json)
        return configFile.absolutePath
    }
}