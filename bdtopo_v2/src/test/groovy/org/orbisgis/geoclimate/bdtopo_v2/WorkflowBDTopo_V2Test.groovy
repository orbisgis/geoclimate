package org.orbisgis.geoclimate.bdtopo_v2

import groovy.json.JsonOutput
import org.h2gis.utilities.FileUtilities
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.geoclimate.Geoindicators
import static org.junit.jupiter.api.Assertions.*

class WorkflowBDTopo_V2Test extends WorkflowAbstractTest{

    def static postgis_dbProperties = [databaseName: 'orbisgis_db',
                               user        : 'orbisgis',
                               password    : 'orbisgis',
                               url         : 'jdbc:postgresql://localhost:5432/'
    ]
    static POSTGIS postGIS;


    private static communeToTest = "12174"

    private static def h2db = "./target/bdtopo_chain"
    private static def bdtopoFoldName = "sample_$communeToTest"
    private static def listTables = ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
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
    void loadAndFormatData(){
        String directory ="./target/bdtopo_chain_prepare"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        def process = BDTopo_V2.WorkflowBDTopo_V2.loadAndFormatData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        IProcess pm =  Geoindicators.WorkflowGeoIndicators.createUnitsOfAnalysis()
        pm.execute([datasource          : h2GIS,        zoneTable               : "tempo_zone",     roadTable           : "tempo_road",
                    railTable           : "", vegetationTable         : "tempo_veget",    hydrographicTable   : "tempo_hydro",
                    surface_vegetation  : null,         surface_hydro           : null,             buildingTable       : "tempo_build",
                    distance            : 0.0,          prefixName              : "test",           indicatorUse        :["LCZ",
                                                                                                                          "UTRF",
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
        def process = BDTopo_V2.WorkflowBDTopo_V2.loadAndFormatData()
        assertTrue process.execute([datasource: datasource,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        IProcess geodindicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators()
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
        def process = BDTopo_V2.WorkflowBDTopo_V2.loadAndFormatData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        def indicatorUse = ["TEB", "UTRF", "LCZ"]
        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, h2GISDatabase, abstractTables.outputZone, abstractTables.outputBuilding,
                abstractTables.outputRoad, abstractTables.outputRail, abstractTables.outputVeget,
                abstractTables.outputHydro, saveResults, svfSimplified, indicatorUse,  prefixName)
    }


    @Test
    void roadTrafficFromBDTopoTest(){
        String directory ="./target/bdtopo_chain_prepare"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        def process = BDTopo_V2.WorkflowBDTopo_V2.loadAndFormatData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE',
                                    tableRoadName: 'ROUTE',
                                    distBuffer: 0, distance: 0, idZone: communeToTest
        ])
        def road_traffic =  Geoindicators.RoadIndicators.build_road_traffic()
        road_traffic.execute([
                datasource : h2GISDatabase,
                inputTableName: process.getResults().outputRoad,
                epsg: 2154])

        assertEquals 813 , h2GISDatabase.getTable(road_traffic.results.outputTableName).rowCount
        assertTrue h2GISDatabase.firstRow("select count(*) as count from ${road_traffic.results.outputTableName} where road_type is not null").count==813
    }

    // Test the workflow on the commune INSEE 01306 only for TEB in order to verify that only RSU_INDICATORS and BUILDING_INDICATORS are saved
    @Test
    @Disabled
    void testBDTOPO_V2Workflow() {
        String directory ="./target/geoclimate_chain/bdtopo_config/"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        IProcess processBDTopo = BDTopo_V2.WorkflowBDTopo_V2.workflow()
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
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
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
                "input" : ["bdtopo_v2": [
                        "folder": ["path" :".../processingChain",
                                   "id_zones":["56350"]]]],
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
                         "indicatorUse": ["LCZ", "TEB", "UTRF"],
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
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
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
                "input" :["bdtopo_v2":  [
                        "folder": ["path" :".../processingChain",
                            "id_zones":["-", "-"]]]],
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
                         "indicatorUse": ["LCZ", "TEB", "UTRF"],
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
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
    }


    @Test
    void runBDTopoWorkflowWithSRID(){
        def inseeCode = communeToTest
        def defaultParameters = [distance: 0,distance_buffer:0,  prefixName: "",
                                 rsu_indicators: [
                                 indicatorUse: ["LCZ", "UTRF"],
                                 svfSimplified:true,
                                 mapOfWeights : ["sky_view_factor" : 2, "aspect_ratio": 1, "building_surface_fraction": 4,
                                                 "impervious_surface_fraction" : 0, "pervious_surface_fraction": 0,
                                                 "height_of_roughness_elements": 3, "terrain_roughness_length": 1]]]
        String directory ="./target/bdtopo_chain_workflow_srid"
        File dirFile = new File(directory)
        if(dirFile.exists()){
            FileUtilities.deleteFiles(dirFile, true)
        }
        dirFile.mkdir()
        H2GIS h2GISDatabase = loadFiles(dirFile.absolutePath+File.separator+"bdtopo_db;AUTO_SERVER=TRUE")
        dirFile.mkdir()
        def tablesToSave = [
                            "rsu_lcz",]
        def process = BDTopo_V2.WorkflowBDTopo_V2.bdtopo_processing(h2GISDatabase, defaultParameters, inseeCode, dirFile, tablesToSave, null, null, 4326);
        checkSpatialTable(h2GISDatabase, "block_indicators")
        checkSpatialTable(h2GISDatabase, "building_indicators")
        checkSpatialTable(h2GISDatabase, "rsu_indicators")
        checkSpatialTable(h2GISDatabase, "rsu_lcz")
        def geoFiles = []
        def  folder = new File(directory+File.separator+"bdtopo_v2_12174")
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
                           "zones":"zones", "grid_indicators":"grid_indicators" ]
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
                "input" : ["bdtopo_v2": [
                        "folder": ["path" :dataFolder,
                                   "id_zones":[communeToTest]]]],
                "output" :[
                        "database" :
                                ["user" : postgis_dbProperties.user,
                                 "password":postgis_dbProperties.password,
                                 "url": postgis_dbProperties.url+postgis_dbProperties.databaseName,
                                 "tables": outputTables]],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: [
                         "indicatorUse": ["LCZ", "UTRF"],
                         "svfSimplified": true
                          ],
                         "grid_indicators": [
                                 "x_size": 1000,
                                 "y_size": 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        //Check if the tables exist and contains at least one row
        outputTables.values().each {it->
            def spatialTable = postGIS.getSpatialTable(it)
            assertNotNull(spatialTable)
            assertTrue(spatialTable.getRowCount()>0)
        }
    }

    @EnabledIfSystemProperty(named = "test.postgis", matches = "true")
    @Test
    void workflowPostGISBBox() {
        WKTReader wktReader = new WKTReader()
        Geometry geom = wktReader.read("POLYGON ((664540 6359900, 665430 6359900, 665430 6359110, 664540 6359110, 664540 6359900))")
        Envelope env = geom.getEnvelopeInternal()

        def outputTables =["building_indicators":"building_indicators",
                           "block_indicators":"block_indicators",
                           "rsu_indicators":"rsu_indicators",
                           "rsu_lcz":"rsu_lcz",
                           "zones":"zones", "grid_indicators":"grid_indicators" ]
        //Drop all output tables if exist
        postGIS.execute("DROP TABLE IF EXISTS ${outputTables.values().join(",")};");
        String directory ="./target/bdtopo_workflow_postgis_bbox"
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
                "input" : ["bdtopo_v2": [
                        "folder": ["path" :dataFolder,
                                   "id_zones":[[env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]]]]],
                "output" :[
                        "database" :
                                ["user" : postgis_dbProperties.user,
                                 "password":postgis_dbProperties.password,
                                 "url": postgis_dbProperties.url+postgis_dbProperties.databaseName,
                                 "tables": outputTables]],
                "parameters":
                        ["distance" : 0,
                         rsu_indicators: [
                                 "indicatorUse": ["LCZ", "UTRF"],
                                 "svfSimplified": true
                         ],
                         "grid_indicators": [
                                 "x_size": 10,
                                 "y_size": 10,
                                 "rowCol": true,
                                 "indicators": ["BUILDING_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        //Check if the tables exist and contains at least one row
        outputTables.values().each {it->
            def spatialTable = postGIS.getSpatialTable(it)
            assertNotNull(spatialTable)
            assertTrue(spatialTable.getRowCount()>0)
        }
    }

    @Test
    void testWorkFlow() {
        String directory ="./target/bdtopo_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" :["bdtopo_v2":  [
                        "folder": ["path" :dataFolder,
                                   "id_zones":[communeToTest]]]],
                "output" :[
                        "folder" : ["path": "$directory",
                                    "tables": ["grid_indicators"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                                 "x_size": 1000,
                                 "y_size": 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ],
                         "rsu_indicators":[
                                 "indicatorUse": ["LCZ", "UTRF", "TEB"],
                                 "svfSimplified": false,
                         ],
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count>0
    }

    @Test
    void testWorkFlowWithoutIdZone() {
        String directory ="./target/bdtopo_withoutzone"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" :["bdtopo_v2":  [
                        "folder": ["path" :dataFolder]]],
                "output" :[
                        "folder" : ["path": "$directory",
                                    "tables": ["grid_indicators"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                                 "x_size": 1000,
                                 "y_size": 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ],
                         "rsu_indicators":[
                                 "indicatorUse": ["LCZ", "UTRF", "TEB"],
                                 "svfSimplified": true,
                         ],
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count>0
    }

    @Test
    void testWorkFlowGridWithName() {
        String directory ="./target/bdtopo_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" :["bdtopo_v2":  [
                        "folder": ["path" :dataFolder,
                                   "id_zones":["Olemps"]]]],
                "output" :[
                        "folder" : ["path": "$directory",
                                    "tables": ["grid_indicators"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                                 "x_size": 1000,
                                 "y_size": 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where water_fraction>0").count>0
    }

    @Test
    void testWorkFlowGridWithBbox() {
        String directory ="./target/bdtopo_chain_grid_bbox"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        WKTReader wktReader = new WKTReader()
        Geometry geom = wktReader.read("POLYGON ((664540 6359900, 665430 6359900, 665430 6359110, 664540 6359110, 664540 6359900))")
        Envelope env = geom.getEnvelopeInternal()
        def bdTopoParameters = [
                "description" :"Example of configuration file to run the grid indicators",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" :["bdtopo_v2":  [
                        "folder": ["path" :dataFolder,
                                   "id_zones":[[env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]]]]],
                "output" :[
                        "folder" : ["path": "$directory",
                                    "tables": ["grid_indicators"]]],
                "parameters":
                        ["distance" : 0,
                         "grid_indicators": [
                                 "x_size": 10,
                                 "y_size": 10,
                                 "rowCol": true,
                                 "indicators": ["BUILDING_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from grid_indicators where BUILDING_FRACTION>0").count>0
    }


    @Test
    void testWorkFlowRoadTraffic() {
        String directory ="./target/bdtopo_roadtraffic"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" :"Example of configuration file to build the road traffic",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete" :false
                ],
                "input" :["bdtopo_v2":  [
                        "folder": ["path" :dataFolder,
                                   "id_zones":[communeToTest]]]],
                "output" :[
                        "folder" : ["path": "$directory",
                                    "tables": ["road_traffic"]]],
                "parameters":
                        ["distance" : 0,
                         "road_traffic": true
                        ]
        ]
        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(bdTopoParameters, directory)))
        H2GIS h2gis = H2GIS.open("${directory+File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from road_traffic where road_type is null").count==0
    }

    @Disabled //Use it for integration test with a postgis database
    @org.junit.jupiter.api.Test
    void testIntegrationPostGIS() {
        String directory ="./target/geoclimate_postgis_integration"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def user = ""
        def password = ""
        def url = ""
        def id_zones = ["56223", "44185"]
        def local_database_name="paendora_${System.currentTimeMillis()}"

        /*================================================================================
        * Input database and tables
        */
        def  input = ["bdtopo_v2": [
                "database": [
                        "user":user,
                        "password": password,
                        "url": url,
                        "id_zones":id_zones,
                        "tables": ["commune":"ign_bdtopo_2017.commune",
                                   "bati_indifferencie":"ign_bdtopo_2017.bati_indifferencie",
                                   "bati_industriel":"ign_bdtopo_2017.bati_industriel",
                                   "bati_remarquable":"ign_bdtopo_2017.bati_remarquable",
                                   "route":"ign_bdtopo_2017.route",
                                   "troncon_voie_ferree":"ign_bdtopo_2017.troncon_voie_ferree",
                                   "surface_eau":"ign_bdtopo_2017.surface_eau",
                                   "zone_vegetation":"ign_bdtopo_2017.zone_vegetation",
                                   "terrain_sport":"ign_bdtopo_2017.terrain_sport",
                                   "construction_surfacique":"ign_bdtopo_2017.construction_surfacique",
                                   "surface_route":"ign_bdtopo_2017.surface_route",
                                   "surface_activite":"ign_bdtopo_2017.surface_activite",
                                   "piste_aerodrome":"ign_bdtopo_2017.piste_aerodrome"]
                ]]]


        /*================================================================================
        * output tables in the database
        */
        def  output  = [
                "database": [
                        "user":user,
                        "password": password,
                        "url": url,
                        "tables": [
                                "building_indicators":"paendora.building_indicators_2154",
                                "block_indicators":"paendora.block_indicators_2154",
                                "rsu_indicators":"paendora.rsu_indicators_2154",
                                "rsu_lcz":"paendora.rsu_lcz_2154",
                                "zones":"paendora.zones_2154",
                                "building_urban_typo":"paendora.building_urban_typo_2154",
                                "rsu_urban_typo_area":"paendora.rsu_urban_typo_area_2154",
                                "rsu_urban_typo_floor_area":"paendora.rsu_urban_typo_floor_area_2154",
                                "grid_indicators":"paendora.grid_indicators_2154",
                                "road_traffic" : "paendora.road_traffic_2154"]
                ]
        ]


        /*================================================================================
        * WORKFLOW PARAMETERS
        */
        def workflow_parameters = [
                "description" :"Run the Geoclimate chain with BDTopo data imported and exported to a POSTGIS database",
                "geoclimatedb" : [
                        "folder" :directory,
                        "name" : "${local_database_name};AUTO_SERVER=TRUE",
                        "delete" : true
                ],
                "input" :input,
                "output" : output,
                "parameters": [ "distance" : 1000   ,
                                "rsu_indicators":[
                                        "indicatorUse": ["LCZ", "UTRF", "TEB"],
                                        "svfSimplified": false,
                                ],
                                /*"grid_indicators": [
                                        "x_size": 100,
                                        "y_size": 100,
                                        "indicators": ["BUILDING_FRACTION","BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION","WATER_FRACTION","VEGETATION_FRACTION",
                                                       "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "URBAN_TYPO_AREA_FRACTION", "LCZ_FRACTION"]
                                ],*/
                                "road_traffic": true
                ]
        ]

        IProcess process = BDTopo_V2.WorkflowBDTopo_V2.workflow()
        assertTrue(process.execute(configurationFile: createConfigFile(workflow_parameters, directory)))
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