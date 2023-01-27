package org.orbisgis.geoclimate.bdtopo.v2

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.geoclimate.bdtopo.BDTopo
import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest
import org.orbisgis.process.api.IProcess
import org.orbisgis.geoclimate.Geoindicators
import static org.junit.jupiter.api.Assertions.*

class WorkflowBDTopoV2Test extends WorkflowAbstractTest {


    private static def listTables = ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                                     "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "TRONCON_VOIE_FERREE", "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
                                     "SURFACE_ROUTE", "SURFACE_ACTIVITE"]
    private static def paramTables = ["BUILDING_ABSTRACT_PARAMETERS", "BUILDING_ABSTRACT_USE_TYPE", "BUILDING_BD_TOPO_USE_TYPE",
                                      "RAIL_ABSTRACT_TYPE", "RAIL_BD_TOPO_TYPE", "RAIL_ABSTRACT_CROSSING",
                                      "RAIL_BD_TOPO_CROSSING", "ROAD_ABSTRACT_PARAMETERS", "ROAD_ABSTRACT_SURFACE",
                                      "ROAD_ABSTRACT_CROSSING", "ROAD_BD_TOPO_CROSSING", "ROAD_ABSTRACT_TYPE",
                                      "ROAD_BD_TOPO_TYPE", "VEGET_ABSTRACT_PARAMETERS", "VEGET_ABSTRACT_TYPE",
                                      "VEGET_BD_TOPO_TYPE"]



    //TODO move it
    /*@Test
    void loadAndFormatData() {
        H2GIS h2GISDatabase = loadFiles(folder.absolutePath + File.separator + "loadAndFormatData;AUTO_SERVER=TRUE")
        def process = BDTopo.Workflow.loadAndFormatData()
        assertTrue process.execute([datasource                 : h2GISDatabase,
                                    tableCommuneName           : 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName        : 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName              : 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName             : 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName   : 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    tablePiste_AerodromeName   : 'PISTE_AERODROME',
                                    tableReservoirName         : 'RESERVOIR',
                                    distance                   : 0, hLevMin: 3
        ])
        process.getResults().each { entry ->
            assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }
    }*/


    @Test
    void bdtopoGeoIndicatorsFromTestFiles() {
        H2GIS h2GISDatabase = loadFiles(folder.absolutePath + File.separator + "bdtopoGeoIndicatorsFromTestFiles;AUTO_SERVER=TRUE")
        def process = BDTopo.Workflow.loadAndFormatData()
        assertTrue process.execute([datasource                 : h2GISDatabase,
                                    tableCommuneName           : 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName        : 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName              : 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName             : 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName   : 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    tablePiste_AerodromeName   : 'PISTE_AERODROME',
                                    tableReservoirName         : 'RESERVOIR', distance: 1000,
                                    hLevMin                    : 3
        ])
        def abstractTables = process.getResults()

        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = true //Fast test
        def indicatorUse = ["TEB", "UTRF", "LCZ"]
        //Run tests
        geoIndicatorsCalc(folder.absolutePath, h2GISDatabase, abstractTables.outputZone, abstractTables.outputBuilding,
                abstractTables.outputRoad, abstractTables.outputRail, abstractTables.outputVeget,
                abstractTables.outputHydro, saveResults, svfSimplified, indicatorUse, prefixName)
    }


    @Test
    void roadTrafficFromBDTopoTest() {
        H2GIS h2GISDatabase = loadFiles(folder.absolutePath + File.separator + "roadTrafficFromBDTopoTest;AUTO_SERVER=TRUE")
        def process = BDTopo.Workflow.loadAndFormatData()
        assertTrue process.execute([datasource      : h2GISDatabase,
                                    tableCommuneName: 'COMMUNE',
                                    tableRoadName   : 'ROUTE', distance: 0
        ])
        def road_traffic = Geoindicators.RoadIndicators.build_road_traffic()
        road_traffic.execute([
                datasource    : h2GISDatabase,
                inputTableName: process.getResults().outputRoad,
                epsg          : 2154])

        assertEquals 2360, h2GISDatabase.getTable(road_traffic.results.outputTableName).rowCount
        assertTrue h2GISDatabase.firstRow("select count(*) as count from ${road_traffic.results.outputTableName} where road_type is not null").count == 2360
    }

    @Test
    void runBDTopoWorkflowWithSRID() {
        def inseeCode = communeToTest
        def defaultParameters = BDTopo.Workflow.extractProcessingParameters()

        // Only download the data if no "indicator_use"
        defaultParameters["rsu_indicators"]["indicatorUse"] = ["UTRF", "LCZ"]                                                       

        H2GIS h2GISDatabase = loadFiles(folder.absolutePath + File.separator + "roadTrafficFromBDTopoTest;AUTO_SERVER=TRUE")
        def tablesToSave = [
                "rsu_lcz",]
        def process = BDTopo.Workflow.bdtopo_processing(inseeCode, h2GISDatabase, defaultParameters, folder.absolutePath, tablesToSave, null, null, 4326, 2154);
        def tableNames = process.values()

        checkSpatialTable(h2GISDatabase, tableNames["block_indicators"])
        checkSpatialTable(h2GISDatabase, tableNames["building_indicators"])
        checkSpatialTable(h2GISDatabase, tableNames["rsu_indicators"])
        checkSpatialTable(h2GISDatabase, tableNames["rsu_lcz"])
        def geoFiles = []
        folder.eachFileRecurse groovy.io.FileType.FILES, { file ->
            if (file.name.toLowerCase().endsWith(".geojson")) {
                geoFiles << file.getAbsolutePath()
            }
        }
        geoFiles.eachWithIndex { geoFile, index ->
            def tableName = h2GISDatabase.load(geoFile, true)
            assertEquals(4326, h2GISDatabase.getSpatialTable(tableName).srid)
        }
    }

    @Test
    void testWorkFlowWithoutDBConfig() {
        String dataFolder = getDataFolderPath()
        WKTReader wktReader = new WKTReader()
        Geometry geom = wktReader.read("POLYGON ((664540 6359900, 665430 6359900, 665430 6359110, 664540 6359110, 664540 6359900))")
        Envelope env = geom.getEnvelopeInternal()
        def envCoords = [env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": folderName.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [envCoords]],
                "output"      : [
                        "folder": ["path"  : folderName.absolutePath,
                                   "tables": ["grid_indicators"]
                        ]],
                "parameters"  :
                        ["distance"       : 0,
                         "grid_indicators": [
                                 "x_size"    : 10,
                                 "y_size"    : 10,
                                 "rowCol"    : true,
                                 "indicators": ["WATER_FRACTION", "VEGETATION_FRACTION"]
                         ],
                         "rsu_indicators" : [
                                 "indicatorUse": ["LCZ"]
                         ],
                        ]
        ]
        IProcess process = BDTopo.Workflow.workflow()
        assertTrue(process.execute(input: bdTopoParameters, version:getVersion()))
        def tableNames = process.results.output.values();
        def grid_table = tableNames.grid_indicators[0]
        H2GIS h2gis = H2GIS.open("${folderName.absolutePath + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from $grid_table".toString()).count == 100
        assertTrue h2gis.firstRow("select count(*) as count from $grid_table where WATER_FRACTION>0".toString()).count == 0
        assertTrue h2gis.firstRow("select count(*) as count from $grid_table where HIGH_VEGETATION_FRACTION>0".toString()).count > 0
        assertTrue h2gis.firstRow("select count(*) as count from $grid_table where LOW_VEGETATION_FRACTION>0".toString()).count == 0
    }

    @Test
    void testWorkFlowGridWithName() {
        String directory = "./target/bdtopo_chain_grid"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": ["Olemps"]],
                "output"      : [
                        "folder": ["path"  : "$directory",
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
        assertTrue(process.execute(input: createConfigFile(bdTopoParameters, directory)))
        def tableNames = process.results.output.values();
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_chain_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.grid_indicators[0]} where water_fraction>0").count > 0
    }


    @Test
    void testWorkFlowGridWithBbox() {
        String directory = "./target/bdtopo_chain_bbox"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        String dataFolder = getDataFolderPath()
        WKTReader wktReader = new WKTReader()
        Geometry geom = wktReader.read("POLYGON ((664540 6359900, 665430 6359900, 665430 6359110, 664540 6359110, 664540 6359900))")
        Envelope env = geom.getEnvelopeInternal()
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the grid indicators",
                "geoclimatedb": [
                        "folder": "${dirFile.absolutePath}",
                        "name"  : "geoclimate_db;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [[env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()]]],
                "output"      : [
                        "folder": ["path"  : "$directory",
                                   "tables": ["grid_indicators", "population"]]],
                "parameters"  :
                        ["distance"           : 0,
                         "grid_indicators"    : [
                                 "x_size"    : 100,
                                 "y_size"    : 100,
                                 "rowCol"    : true,
                                 "indicators": ["BUILDING_FRACTION", "BUILDING_POP"]
                         ],
                         "worldpop_indicators": true
                        ]
        ]
        IProcess process = BDTopo.Workflow.workflow()
        assertTrue(process.execute(input: createConfigFile(bdTopoParameters, directory)))
        def tableNames = process.results.output.values();
        H2GIS h2gis = H2GIS.open("${directory + File.separator}geoclimate_db;AUTO_SERVER=TRUE")
        assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.grid_indicators[0]} where BUILDING_FRACTION>0").count > 0

        if (!h2gis.getTable(tableNames.population[0]).isEmpty()) {
            assertTrue h2gis.firstRow("select count(*) as count from ${tableNames.population[0]} where pop is not null").count > 0
        }
    }

    @Override
    int getVersion() {
        return 2
    }

    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}"
    }

    @Override
    String getInseeCode() {
        return "12174"
    }
}