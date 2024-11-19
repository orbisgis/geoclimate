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
package org.orbisgis.geoclimate.bdtopo

import org.junit.jupiter.api.Test
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS

import static org.junit.jupiter.api.Assertions.*

abstract class WorkflowAbstractTest {


    /**
     * Get the version of the workflow
     * @return
     */
    abstract int getVersion()

    /**
     * The path to store the db
     * @return
     */
    abstract String getDBFolderPath()

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

    abstract void checkFormatData();


    /**
     * The names of file used for the test
     * @return
     */
    abstract ArrayList getFileNames()


    Map getResultFiles(String dataFolder) {
        if (dataFolder) {
            def files = [:]
            new File(dataFolder).eachFileRecurse groovy.io.FileType.FILES, { file ->
                if (file.name.toLowerCase().endsWith(".fgb")) {
                    files.put((file.name.take(file.name.lastIndexOf('.'))), file.getAbsolutePath())
                }
            }
            return files
        }
    }

    /**
     * Return the path of the data sample
     * @return
     */
    String getDataFolderPath() {
        if (new File(getClass().getResource(getFolderName()).toURI()).isDirectory()) {
            return new File(getClass().getResource(getFolderName()).toURI()).absolutePath
        }
        return null
    }

    @Test
    void testFullWorkFlow() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Full workflow configuration file",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "testFullWorflow;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"      : [
                        "folder": ["path": getDBFolderPath()]],
                "parameters"  :
                        ["distance"        : 0,
                         rsu_indicators    : [
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

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)

        def tableNames = process[getInseeCode()]
        assertTrue(tableNames.size() > 0)
        H2GIS h2gis = H2GIS.open(getDBFolderPath() + File.separator + "testFullWorflow;AUTO_SERVER=TRUE")

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
        assertEquals(h2gis.firstRow("""SELECT sum(ST_Length(the_geom)) as road_length from ${tableNames.road} where type not in ('track', 'path', 'cycleway', 'steps');""".toString()).road_length,
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
        def filePath = getClass().getResource(getFolderName() + File.separator + "COMMUNE.shp")
        // If some layers are missing, do not try to load them...
        if (filePath) {
            H2GIS h2GIS = H2GIS.open(getDBFolderPath() + File.separator + "tmpdb")
            h2GIS.link(filePath, "COMMUNE", true)
            Geometry geom = h2GIS.firstRow("""SELECT ST_BUFFER(ST_POINTONSURFACE(the_geom), 200) AS the_geom from commune""".toString()).the_geom
            h2GIS.close()
            Envelope env = geom.getEnvelopeInternal()
            def location = [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]

            def bdTopoParameters = [
                    "description" : "Example of configuration file to set the SRID",
                    "geoclimatedb": [
                            "folder": getDBFolderPath(),
                            "name"  : "testFullWorflowSRID;AUTO_SERVER=TRUE",
                            "delete": false
                    ],
                    "input"       : [
                            "folder"   : dataFolder,
                            "locations": [location]],
                    "output"      : [
                            "folder": ["path": getDBFolderPath(), "tables": ["grid_indicators"]],
                            "srid"  : 4326],
                    "parameters"  :
                            ["distance"       : 0,
                             rsu_indicators   : [
                                     "indicatorUse": ["LCZ"]],
                             "grid_indicators": [
                                     "x_size"    : 10,
                                     "y_size"    : 10,
                                     "rowCol"    : true,
                                     "indicators": ["BUILDING_FRACTION"]
                             ]
                            ]
            ]

            Map process = BDTopo.workflow(bdTopoParameters, getVersion())
            assertNotNull(process)
            def tableNames = process.values()
            def grid_table = tableNames.grid_indicators[0]
            assertNotNull(grid_table)
            H2GIS h2gis = H2GIS.open("${getDBFolderPath() + File.separator}testFullWorflowSRID;AUTO_SERVER=TRUE")
            assertTrue h2gis.firstRow("select count(*) as count from $grid_table".toString()).count == 100
            assertTrue h2gis.firstRow("select count(*) as count from $grid_table where BUILDING_FRACTION>0".toString()).count > 0
            File grid_file = new File(getDBFolderPath() + File.separator + "bdtopo_" + getVersion() + "_" + location.join("_") + File.separator + "grid_indicators.fgb")
            assertTrue(grid_file.exists())
            h2gis.load(grid_file.absolutePath, "grid_indicators_file", true)
            assertEquals(4326, h2gis.getSpatialTable("grid_indicators_file").srid)
        }
    }

    @Test
    void workflowExternalDB() {
        def externaldb_dbProperties = [databaseName: "${getDBFolderPath() + File.separator}external_db",
                                       user        : 'sa',
                                       password    : 'sa'
        ]
        def outputTables = ["building_indicators": "building_indicators",
                            "rsu_indicators"     : "rsu_indicators",
                            "rsu_lcz"            : "rsu_lcz",
                            "zone"               : "zone",
                            "grid_indicators"    : "grid_indicators"]
        //Drop all output tables if exist
        H2GIS externalDB = H2GIS.open(getDBFolderPath() + File.separator + externaldb_dbProperties.databaseName,
                externaldb_dbProperties.user, externaldb_dbProperties.password)
        externalDB.execute("DROP TABLE IF EXISTS ${outputTables.values().join(",")};".toString())
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder"   : getDataFolderPath(),
                        "locations": [getInseeCode()]],
                "output"      : [
                        "database":
                                ["user"        : externaldb_dbProperties.user,
                                 "password"    : externaldb_dbProperties.password,
                                 "databaseName": getDBFolderPath() + File.separator + externaldb_dbProperties.databaseName,
                                 "tables"      : outputTables]],
                "parameters"  :
                        ["distance"       : 0,
                         rsu_indicators   : [
                                 "indicatorUse": ["LCZ"]
                         ],
                         "grid_indicators": [
                                 "x_size"    : 1000,
                                 "y_size"    : 1000,
                                 "indicators": ["WATER_FRACTION"]
                         ]
                        ]
        ]

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)
        //Check if the tables exist and contains at least one row
        outputTables.values().each { it ->
            def spatialTable = externalDB.getSpatialTable(it)
            assertNotNull(spatialTable)
            assertEquals(2154, spatialTable.srid)
            assertTrue(spatialTable.getRowCount() > 0)
        }
        externalDB.close()
    }

    @Test
    void workflowExternalDBBBox() {
        String dataFolder = getDataFolderPath()
        def filePath = getClass().getResource(getFolderName() + File.separator + "COMMUNE.shp")
        // If some layers are missing, do not try to load them...
        if (filePath) {
            def externaldb_dbProperties = [databaseName: "${getDBFolderPath() + File.separator}external_db_bbox",
                                           user        : 'sa',
                                           password    : 'sa'
            ]
            H2GIS externalDB = H2GIS.open(getDBFolderPath() + File.separator + externaldb_dbProperties.databaseName, externaldb_dbProperties.user, externaldb_dbProperties.password)
            externalDB.link(filePath, "COMMUNE", true)
            Geometry geom = externalDB.firstRow("""SELECT ST_BUFFER(ST_POINTONSURFACE(the_geom), 200) AS the_geom from commune""".toString()).the_geom

            Envelope env = geom.getEnvelopeInternal()
            def location = [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]

            def outputTables = ["building_indicators": "building_indicators",
                                "rsu_indicators"     : "rsu_indicators",
                                "rsu_lcz"            : "rsu_lcz",
                                "zone"               : "zone", "grid_indicators": "grid_indicators"]
            //Drop all output tables if exist
            externalDB.execute("DROP TABLE IF EXISTS ${outputTables.values().join(",")};".toString())
            def bdTopoParameters = [
                    "description" : "Example of configuration file to run the BDTopo workflow and store the results in a folder",
                    "geoclimatedb": [
                            "folder": getDBFolderPath(),
                            "name"  : "bdtopo_workflow_db_bbox;AUTO_SERVER=TRUE",
                            "delete": true
                    ],
                    "input"       : [
                            "folder"   : getDataFolderPath(),
                            "locations": [location]],
                    "output"      : [
                            "database":
                                    ["user"        : externaldb_dbProperties.user,
                                     "password"    : externaldb_dbProperties.password,
                                     "databaseName": getDBFolderPath() + File.separator + externaldb_dbProperties.databaseName,
                                     "tables"      : outputTables]],
                    "parameters"  :
                            ["distance"       : 0,
                             rsu_indicators   : [
                                     "indicatorUse": ["LCZ"]
                             ],
                             "grid_indicators": [
                                     "x_size"    : 10,
                                     "y_size"    : 10,
                                     "rowCol"    : true,
                                     "indicators": ["BUILDING_FRACTION"]
                             ]
                            ]
            ]
            Map process = BDTopo.workflow(bdTopoParameters, getVersion())
            assertNotNull(process)
            //Check if the tables exist and contains at least one row
            outputTables.values().each { it ->
                assertTrue(externalDB.hasTable(it))
                assertEquals(2154, externalDB.getSrid(it))
                assertTrue(externalDB.getRowCount(it) > 0)
            }
            externalDB.close()
        }
    }

    @Test
    void testWorkFlowListCodes() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [2000, 2001, 2002]],
                "output"      : [
                        "folder": ["path"  : getDBFolderPath(),
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
        assertThrows(Exception.class, () -> BDTopo.workflow(bdTopoParameters, getVersion()))
    }

    @Test
    void testFormatData() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Full workflow configuration file",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "testFormatedData;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"      : [
                        "folder": ["path": getDBFolderPath()]],
                "parameters"  :
                        ["distance": 0]
        ]

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)
        checkFormatData()
    }

    @Test
    void testOnlyFormatData() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Workflow format data",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "testFormat;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"      : [
                        "folder": ["path": getDBFolderPath()]]
        ]

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)

        def tableNames = process[getInseeCode()]
        assertTrue(tableNames.size() > 0)
        H2GIS h2gis = H2GIS.open(getDBFolderPath() + File.separator + "testFormat;AUTO_SERVER=TRUE")

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
    void testTarguet() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Full workflow configuration file",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "testFormatedData",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"      : [
                        "folder": ["path": getDBFolderPath()]],
                "parameters"  :
                        ["distance": 0,
                         rsu_indicators: [
                                         "indicatorUse" : ["TARGET"]
                                         ]
                        ]
                ]

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)
        def tableNames = process.values()
        def targetGrid = tableNames.grid_target[0]
        H2GIS h2gis = H2GIS.open("${getDBFolderPath() + File.separator}testFormatedData")
        assertEquals(h2gis.getRowCount(targetGrid), h2gis.firstRow("""select count(*) as count from $targetGrid 
        where \"roof\"+ \"road\"+ \"watr\"+\"conc\"+\"Veg\" + \"dry\" + \"irr\" >=1""").count)
    }

    @Test
    void testTargetGridSize() {
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Full workflow configuration file",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "testFormatedData",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"      : [
                        "folder": ["path": getDBFolderPath()]],
                "parameters"  :
                        ["distance": 0,
                         rsu_indicators: [
                                 "indicatorUse" : ["TARGET", "LCZ"]
                         ],
                         "grid_indicators"   : [
                                "x_size"    : 200,
                                "y_size"    : 200,
                                "indicators": ["LCZ_PRIMARY"]
                        ]
                        ]
        ]

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)
        def tableNames = process.values()
        def targetGrid = tableNames.grid_target[0]
        H2GIS h2gis = H2GIS.open("${getDBFolderPath() + File.separator}testFormatedData")
        assertEquals(h2gis.getRowCount(targetGrid), h2gis.firstRow("""select count(*) as count from $targetGrid 
        where \"roof\"+ \"road\"+ \"watr\"+\"conc\"+\"Veg\" + \"dry\" + \"irr\" >=1""").count)
        def gridIndicators = tableNames.grid_indicators[0]
        assertTrue(h2gis.getColumnNames(gridIndicators).contains("LCZ_PRIMARY"))
    }
}