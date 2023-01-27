package org.orbisgis.geoclimate.bdtopo

import groovy.json.JsonOutput
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess
import org.orbisgis.geoclimate.Geoindicators
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.fail
import static org.junit.jupiter.api.Assertions.fail

abstract class WorkflowAbstractTest {

    @TempDir
    static File folder

    public static Logger logger = LoggerFactory.getLogger(WorkflowAbstractTest.class)

    def static postgis_dbProperties = [databaseName: 'orbisgis_db',
                                       user        : 'orbisgis',
                                       password    : 'orbisgis',
                                       url         : 'jdbc:postgresql://localhost:5432/'
    ]
    static POSTGIS postGIS

    @BeforeAll
    static void beforeAll() {
        postGIS = POSTGIS.open(postgis_dbProperties)
        System.setProperty("test.postgis", Boolean.toString(postGIS != null))
    }

    /**
     * Load the files to run the test
     * @param dbPath
     * @return
     */
    private def loadFiles(def dbPath) {
        H2GIS h2GISDatabase = H2GIS.open(dbPath)
        // Load parameter files
        paramTables.each {
            h2GISDatabase.load(getClass().getResource(it + ".csv"), it, true)
        }

        def relativePath = bdtopoFoldName

        // Test whether there is a folder containing .shp files for the corresponding INSEE code
        if (getClass().getResource(relativePath)) {
            // Test is the URL is a folder
            if (new File(getClass().getResource(relativePath).toURI()).isDirectory()) {
                listTables.each {
                    def filePath = getClass().getResource(relativePath + File.separator + it + ".shp")
                    // If some layers are missing, do not try to load them...
                    if (filePath) {
                        h2GISDatabase.link(filePath, it, true)
                    }
                }
            } else {
                fail("There is no folder containing shapefiles for commune insee $inseeCode")
            }
        } else {
            fail("There is no folder containing shapefiles for commune insee $inseeCode")
        }
        return h2GISDatabase
    }


    /**
     * Get the version of the workflow
     * @return
     */
    abstract int getVersion()


/**
 * The folder that contains the BDTopo test data
 * @return
 */
    abstract String getFolderName();

    abstract String getInseeCode();


    /**
     * Create a configuration file
     * @param bdTopoParameters
     * @param directory
     * @return
     */
    String createConfigFile(def bdTopoParameters, def directory) {
        def json = JsonOutput.toJson(bdTopoParameters)
        def configFilePath = directory + File.separator + "bdTopoConfigFile.json"
        File configFile = new File(configFilePath)
        if (configFile.exists()) {
            configFile.delete()
        }
        configFile.write(json)
        return configFile.absolutePath
    }


    /**
     * Check if the table exist and contains at least one row
     * @param datasource
     * @param tableName
     * @return
     */
    def checkSpatialTable(JdbcDataSource datasource, def tableName) {
        assertTrue(datasource.hasTable(tableName))
        assertTrue(datasource.getSpatialTable(tableName).getRowCount() > 0)
    }

    /**
     * Return the path of the data sample
     * @return
     */
    String getDataFolderPath() {
        if (new File(getClass().getResource(bdtopoFoldName).toURI()).isDirectory()) {
            return new File(getClass().getResource(bdtopoFoldName).toURI()).absolutePath
        }
        return null;
    }

    /**
     * A method to compute geomorphological indicators
     * @param directory
     * @param datasource
     * @param zone
     * @param buildingTableName
     * @param roadTableName
     * @param railTableName
     * @param vegetationTableName
     * @param hydrographicTableName
     * @param saveResults
     * @param indicatorUse
     */
    void geoIndicatorsCalc(String directory, def datasource, String zone, String buildingTableName,
                           String roadTableName, String railTableName, String vegetationTableName,
                           String hydrographicTableName, boolean saveResults, boolean svfSimplified = false, def indicatorUse,
                           String prefixName = "") {
        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = Geoindicators.WorkflowGeoIndicators.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource       : datasource, zoneTable: zone, buildingTable: buildingTableName,
                                         roadTable        : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro    : 2500, distance: 0.01, prefixName: prefixName])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationBlocks = spatialUnits.getResults().outputTableBlockName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            logger.debug("Saving spatial units")
            IProcess saveTables = Geoindicators.DataUtils.saveTablesAsFiles()
            saveTables.execute([inputTableNames: spatialUnits.getResults().values(), delete: true
                                , directory    : directory, datasource: datasource])
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        assertEquals(countBlocks.count, maxBlocks.max)


        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count, maxRSUBlocks.max)

        //Compute building indicators
        def computeBuildingsIndicators = Geoindicators.WorkflowGeoIndicators.computeBuildingsIndicators()
        assertTrue computeBuildingsIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: relationBuildings,
                                                       inputRoadTableName    : roadTableName,
                                                       indicatorUse          : indicatorUse,
                                                       prefixName            : prefixName])
        String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName
        if (saveResults) {
            logger.debug("Saving building indicators")
            datasource.save(buildingIndicators, directory + File.separator + "${buildingIndicators}.geojson", true)
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count, countBuildingIndicators.count)

        //Compute block indicators
        if (indicatorUse.contains("UTRF")) {
            def computeBlockIndicators = Geoindicators.WorkflowGeoIndicators.computeBlockIndicators()
            assertTrue computeBlockIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: buildingIndicators,
                                                       inputBlockTableName   : relationBlocks,
                                                       prefixName            : prefixName])
            String blockIndicators = computeBlockIndicators.getResults().outputTableName
            if (saveResults) {
                logger.debug("Saving block indicators")
                datasource.save(blockIndicators, directory + File.separator + "${blockIndicators}.geojson", true)
            }
            //Check if we have the same number of blocks
            def countRelationBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count, countBlocksIndicators.count)
        }

        //Compute RSU indicators
        def computeRSUIndicators = Geoindicators.WorkflowGeoIndicators.computeRSUIndicators()
        assertTrue computeRSUIndicators.execute([datasource       : datasource,
                                                 buildingTable    : buildingIndicators,
                                                 rsuTable         : relationRSU,
                                                 vegetationTable  : vegetationTableName,
                                                 roadTable        : roadTableName,
                                                 hydrographicTable: hydrographicTableName,
                                                 indicatorUse     : indicatorUse,
                                                 prefixName       : prefixName,
                                                 svfSimplified    : svfSimplified])
        String rsuIndicators = computeRSUIndicators.getResults().outputTableName
        if (saveResults) {
            logger.debug("Saving RSU indicators")
            datasource.save(rsuIndicators, directory + File.separator + "${rsuIndicators}.geojson", true)
        }

        //Check if we have the same number of RSU
        def countRelationRSU = datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count, countRSUIndicators.count)
    }

    @Disabled
    @EnabledIfSystemProperty(named = "test.postgis", matches = "true")
    @Test
    void workflowPostGIS() {
        def outputTables = ["building_indicators": "building_indicators",
                            "block_indicators"   : "block_indicators",
                            "rsu_indicators"     : "rsu_indicators",
                            "rsu_lcz"            : "rsu_lcz",
                            "zone"               : "zone", "grid_indicators": "grid_indicators"]
        //Drop all output tables if exist
        postGIS.execute("DROP TABLE IF EXISTS ${outputTables.values().join(",")};");
        String directory = "./target/bdtopo_workflow_postgis"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": folder.getAbsolutePath(),
                        "name"  : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder": ["path"     : folder.getAbsolutePath(),
                                   "locations": [communeToTest]]],
                "output"      : [
                        "database":
                                ["user"    : postgis_dbProperties.user,
                                 "password": postgis_dbProperties.password,
                                 "url"     : postgis_dbProperties.url + postgis_dbProperties.databaseName,
                                 "tables"  : outputTables]],
                "parameters"  :
                        ["distance"       : 0,
                         rsu_indicators   : [
                                 "indicatorUse": ["LCZ", "UTRF"]
                         ],
                         "grid_indicators": [
                                 "x_size"    : 1000,
                                 "y_size"    : 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo.Workflow.workflow()
        assertTrue(process.execute(input: bdTopoParameters, version: getVersion()))
        //Check if the tables exist and contains at least one row
        outputTables.values().each { it ->
            def spatialTable = postGIS.getSpatialTable(it)
            assertNotNull(spatialTable)
            assertTrue(spatialTable.getRowCount() > 0)
        }
    }

    @EnabledIfSystemProperty(named = "test.postgis", matches = "true")
    @Test
    void workflowPostGISBBox() {
        WKTReader wktReader = new WKTReader()
        Geometry geom = wktReader.read("POLYGON ((664540 6359900, 665430 6359900, 665430 6359110, 664540 6359110, 664540 6359900))")
        Envelope env = geom.getEnvelopeInternal()

        def outputTables = ["building_indicators": "building_indicators",
                            "block_indicators"   : "block_indicators",
                            "rsu_indicators"     : "rsu_indicators",
                            "rsu_lcz"            : "rsu_lcz",
                            "zone"               : "zone", "grid_indicators": "grid_indicators"]
        //Drop all output tables if exist
        postGIS.execute("DROP TABLE IF EXISTS ${outputTables.values().join(",")};".toString());
        String directory = "./target/bdtopo_workflow_postgis_bbox"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [[env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]]],
                "output"      : [
                        "database":
                                ["user"    : postgis_dbProperties.user,
                                 "password": postgis_dbProperties.password,
                                 "url"     : postgis_dbProperties.url + postgis_dbProperties.databaseName,
                                 "tables"  : outputTables]],
                "parameters"  :
                        ["distance"       : 0,
                         rsu_indicators   : [
                                 "indicatorUse": ["LCZ", "UTRF"]
                         ],
                         "grid_indicators": [
                                 "x_size"    : 10,
                                 "y_size"    : 10,
                                 "rowCol"    : true,
                                 "indicators": ["BUILDING_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo.Workflow.workflow()
        assertTrue(process.execute(input: bdTopoParameters, version: getVersion()))
        //Check if the tables exist and contains at least one row
        outputTables.values().each { it ->
            def spatialTable = postGIS.getSpatialTable(it)
            assertNotNull(spatialTable)
            assertTrue(spatialTable.getRowCount() > 0)
        }
    }

    @Test
    void testWorkFlowListCodes() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": folder.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder": ["path"     : dataFolder,
                                   "locations": [2000, 2001, 2002]]],
                "output"      : [
                        "folder": ["path"  : folder.absolutePath,
                                   "tables": ["grid_indicators"]]],
                "parameters"  :
                        ["distance"       : 0,
                         "grid_indicators": [
                                 "x_size"    : 1000,
                                 "y_size"    : 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ]
                        ]
        ]
        IProcess process = BDTopo.Workflow.workflow()
        assertFalse(process.execute(input: createConfigFile(bdTopoParameters, folder.absolutePath)))
    }

    @Disabled
    @Test
    void workflowFolderToDatabase() {
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": folder.absolutePath,
                        "name"  : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder": ["path"     : ".../processingChain",
                                   "locations": ["-", "-"]]],
                "output"      : [
                        "database":
                                ["user"    : "sa",
                                 "password": "sa",
                                 "url"     : "jdbc:h2://${folder.absolutePath + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE"}",
                                 "tables"  : ["building_indicators": "building_indicators",
                                              "block_indicators"   : "block_indicators",
                                              "rsu_indicators"     : "rsu_indicators",
                                              "rsu_lcz"            : "rsu_lcz",
                                              "zone"               : "zone"]]],
                "parameters"  :
                        ["distance"    : 0,
                         "hLevMin"     : 3,
                         rsu_indicators: [
                                 "indicatorUse" : ["LCZ", "TEB", "UTRF"],
                                 "svfSimplified": true,
                                 "mapOfWeights" :
                                         ["sky_view_factor"             : 1,
                                          "aspect_ratio"                : 1,
                                          "building_surface_fraction"   : 1,
                                          "impervious_surface_fraction" : 1,
                                          "pervious_surface_fraction"   : 1,
                                          "height_of_roughness_elements": 1,
                                          "terrain_roughness_class"     : 1]]
                        ]
        ]
        IProcess process = BDTopo.Workflow.workflow()
        assertTrue(process.execute(input: bdTopoParameters, version: getVersion()))
    }

    @Test
    void testFullWorkFlow() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description"     : "Example of configuration file to build the road traffic",
                "geoclimatedb"    : [
                        "folder": folder.absolutePath,
                        "name"  : "testWorkFlowRoadTraffic;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"           : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"          : [
                        "folder": ["path"  : folder.absolutePath,
                                   "tables": ["road_traffic", "ground_acoustic", "impervious"]]],
                "parameters"      :
                        ["distance"    : 0,
                         rsu_indicators: [
                                 "indicatorUse": ["LCZ", "TEB", "UTRF"]]
                        ],
                "grid_indicators" : [
                        "x_size"    : 1000,
                        "y_size"    : 1000,
                        "indicators": ["WATER_FRACTION"]
                ],
                "road_traffic"    : true,
                "noise_indicators": [
                        "ground_acoustic": true
                ]
        ]

        IProcess process = BDTopo.Workflow.workflow()
        assertTrue(process.execute(input: bdTopoParameters, version: getVersion()))
        def tableNames = process.results.output.values()
        def roadTableName = process.getResults().output[getInseeCode()]["road_traffic"]
        assertNotNull(roadTableName)
        def ground_acoustic = process.getResults().output[getInseeCode()]["ground_acoustic"]
        assertNotNull(ground_acoustic)
        H2GIS h2gis = H2GIS.open(folder.absolutePath + File.separator + "testWorkFlowRoadTraffic;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.grid_indicators[0]} where water_fraction>0").count > 0
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.roadTableName[0]} where road_type is null".toString()).count == 0
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.ground_acoustic[0]} where layer in ('road', 'building')".toString()).count == 0
        assertTrue h2gis.rows("select distinct(g) as g from ${tableNames.ground_acoustic[0]} where type = 'water'".toString()).size() == 1

    }

}