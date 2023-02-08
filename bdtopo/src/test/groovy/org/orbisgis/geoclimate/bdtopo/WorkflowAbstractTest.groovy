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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.fail

abstract class WorkflowAbstractTest {

    //@TempDir
    static File folder = new File("/tmp/")

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
    def loadFiles(def dbPath) {
        H2GIS h2GISDatabase = H2GIS.open(dbPath)

        if (getClass().getResource(getFolderName())) {
            // Test is the URL is a folder
            if (new File(getClass().getResource(getFolderName()).toURI()).isDirectory()) {
                getFileNames().each {
                    def filePath = getClass().getResource(getFolderName() + File.separator + it + ".shp")
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
    abstract String getFolderName()

    /**
     * Get the insee code of the tested area
     * @return
     */
    abstract String getInseeCode()



    /**
     * The names of file used for the test
     * @return
     */
    abstract ArrayList getFileNames()


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
        if (new File(getClass().getResource(getFolderName()).toURI()).isDirectory()) {
            return new File(getClass().getResource(getFolderName()).toURI()).absolutePath
        }
        return null;
    }

    @Test
    void testFormatData() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description"     : "Full workflow configuration file",
                "geoclimatedb"    : [
                        "folder": folder.absolutePath,
                        "name"  : "testFullWorflow;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"           : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"          : [
                        "folder": ["path": folder.absolutePath]],
                "parameters"      :
                        ["distance"    : 0
                        ]
        ]

        Map process = BDTopo.workflow(input: bdTopoParameters, version: getVersion())
        assertNotNull(process)

        def tableNames = process.output[getInseeCode()]
        assertTrue(tableNames.size()>0)
        H2GIS h2gis = H2GIS.open(folder.absolutePath + File.separator + "testFullWorflow;AUTO_SERVER=TRUE")

        //Test zone
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.zone} where the_geom is not null").count > 0

        //Test building
        String building_table = tableNames.building
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where TYPE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where MAIN_USE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where NB_LEV is not null or NB_LEV>0 ;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where HEIGHT_WALL is not null or HEIGHT_WALL>0 ;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where HEIGHT_ROOF is not null or HEIGHT_ROOF>0 ;""".toString()).count > 0)

        //Test water
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.water} where TYPE is not null;""".toString()).count > 0)

        //Test vegetation
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.vegetation} where TYPE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.vegetation} where HEIGHT_CLASS is not null;""".toString()).count > 0)

        //Test road
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.road} where TYPE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.road} where WIDTH is not null or WIDTH>0 ;""".toString()).count > 0)
  }

    @Test
    void testFullWorkFlow() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description"     : "Full workflow configuration file",
                "geoclimatedb"    : [
                        "folder": folder.absolutePath,
                        "name"  : "testFullWorflow;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"           : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"          : [
                        "folder": ["path": folder.absolutePath]],
                "parameters"      :
                        ["distance"    : 0,
                         rsu_indicators: [
                                 "indicatorUse": ["LCZ", "TEB", "UTRF"]]
                        ,
                        "grid_indicators" : [
                        "x_size"    : 1000,
                        "y_size"    : 1000,
                        "indicators": ["WATER_FRACTION"]
                        ],
                "road_traffic"    : true,
                "noise_indicators": [
                        "ground_acoustic": true
                ]]
        ]

        Map process = BDTopo.workflow(input: bdTopoParameters, version: getVersion())
        assertNotNull(process)

        def tableNames = process.output[getInseeCode()]
        assertTrue(tableNames.size()>0)
        H2GIS h2gis = H2GIS.open(folder.absolutePath + File.separator + "testFullWorflow;AUTO_SERVER=TRUE")

        //Test zone
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.zone} where the_geom is not null").count > 0

        //Test building
        String building_table = tableNames.building
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where TYPE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where MAIN_USE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where NB_LEV is not null or NB_LEV>0 ;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where HEIGHT_WALL is not null or HEIGHT_WALL>0 ;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from $building_table where HEIGHT_ROOF is not null or HEIGHT_ROOF>0 ;""".toString()).count > 0)

        //Test building / block
        assertEquals(h2gis.firstRow("""SELECT count(distinct id_block) as building_count from $building_table;""".toString()).building_count,
                h2gis.firstRow("""SELECT count(*) as block_count from ${tableNames.block_indicators};""".toString()).block_count)

        //Test water
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.water} where TYPE is not null;""".toString()).count > 0)

        //Test vegetation
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.vegetation} where TYPE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.vegetation} where HEIGHT_CLASS is not null;""".toString()).count > 0)

        //Test road
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.road} where TYPE is not null;""".toString()).count > 0)
        assertTrue(h2gis.firstRow("""SELECT count(*) as count from ${tableNames.road} where WIDTH is not null or WIDTH>0 ;""".toString()).count > 0)

        //Test road_traffic
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.road_traffic} where road_type is null".toString()).count == 0
        assertEquals(h2gis.firstRow("""SELECT sum(ST_Length(the_geom)) as road_length from ${tableNames.road};""".toString()).road_length,
                h2gis.firstRow("""SELECT sum(ST_Length(the_geom)) as traffic_Length from ${tableNames.road_traffic};""".toString()).traffic_Length)


        //Test ground acoustic
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.ground_acoustic} where layer in ('road', 'building')".toString()).count == 0
        assertTrue h2gis.rows("select distinct(g) as g from ${tableNames.ground_acoustic} where type = 'water'".toString()).size() == 1

        //Test grid_indicators
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.grid_indicators} where water_fraction>0").count > 0
    }

    @Test
    void runWorkflowSRIDAndBBox() {
        String dataFolder = getDataFolderPath()
        def filePath = getClass().getResource(getFolderName() + File.separator  + "COMMUNE.shp")
        // If some layers are missing, do not try to load them...
        if (filePath) {
            H2GIS  h2GIS = H2GIS.open(folder.absolutePath+File.separator+"tmpdb")
            h2GIS.link(filePath, "COMMUNE", true)
            Geometry geom = h2GIS.firstRow("""SELECT ST_BUFFER(ST_POINTONSURFACE(the_geom), 100) AS the_geom from commune""".toString()).the_geom
            h2GIS.close()
            Envelope env = geom.getEnvelopeInternal()
            def location = [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]

            def bdTopoParameters = [
                    "description"     : "Example of configuration file to build the road traffic",
                    "geoclimatedb"    : [
                            "folder": folder.absolutePath,
                            "name"  : "testFullWorflowSRID;AUTO_SERVER=TRUE",
                            "delete": false
                    ],
                    "input"           : [
                            "folder"   : dataFolder,
                            "locations": [location]],
                    "output"          : [
                            "folder": ["path": folder.absolutePath]],
                    "srid": 4326,
                    "parameters"      :
                            ["distance"    : 0,
                             rsu_indicators: [
                                     "indicatorUse": ["LCZ"]],"grid_indicators"    : [
                                    "x_size"    : 100,
                                    "y_size"    : 100,
                                    "rowCol"    : true,
                                    "indicators": ["BUILDING_FRACTION"]
                            ]
                            ]
            ]

            Map process = BDTopo.workflow(input: bdTopoParameters, version: getVersion())
            assertTrue(process)

            List outputTables = ["zone", "road", "rail", "water", "vegetation",
                                 "impervious", "urban_areas", "building",
                                 "building_indicators", "block_indicators",
                                 "rsu_indicators", "rsu_lcz",
                                 "rsu_utrf_area", "rsu_utrf_floor_area",
                                 "building_utrf", "grid_indicators",
                                 "ground_acoustic"]

            def tableNames = outputTables.intersect(process.values())
            assertTrue(tableNames)
            def grid_table = tableNames.grid_indicators

            H2GIS h2gis = H2GIS.open("${folderName.absolutePath + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
            assertTrue h2gis.firstRow("select count(*) as count from $grid_table".toString()).count == 100
            assertTrue h2gis.firstRow("select count(*) as count from $grid_table where WATER_FRACTION>0".toString()).count == 0
            assertTrue h2gis.firstRow("select count(*) as count from $grid_table where HIGH_VEGETATION_FRACTION>0".toString()).count > 0
            assertTrue h2gis.firstRow("select count(*) as count from $grid_table where LOW_VEGETATION_FRACTION>0".toString()).count == 0
        }
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
                                   "locations": [getInseeCode()]]],
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
        assertNull(BDTopo.workflow(input:bdTopoParameters, version :getVersion()))
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


}