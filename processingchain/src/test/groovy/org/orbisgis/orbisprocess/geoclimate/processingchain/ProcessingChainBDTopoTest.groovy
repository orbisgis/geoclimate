package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

class ProcessingChainBDTopoTest extends ChainProcessAbstractTest{

    public static Logger logger = LoggerFactory.getLogger(ProcessingChainBDTopoTest.class)
    public static def h2db = "./target/myh2gisbdtopodb"
    public static def bdtopoFoldName = "bdtopofolder"
    public static def listTables = ["IRIS_GE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
    "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "TRONCON_VOIE_FERREE", "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
    "SURFACE_ROUTE", "SURFACE_ACTIVITE"]
    public static def paramTables = ["BUILDING_ABSTRACT_PARAMETERS", "BUILDING_ABSTRACT_USE_TYPE", "BUILDING_BD_TOPO_USE_TYPE",
                                     "RAIL_ABSTRACT_TYPE", "RAIL_BD_TOPO_TYPE", "RAIL_ABSTRACT_CROSSING",
                                     "RAIL_BD_TOPO_CROSSING", "ROAD_ABSTRACT_PARAMETERS", "ROAD_ABSTRACT_SURFACE",
                                     "ROAD_ABSTRACT_CROSSING", "ROAD_BD_TOPO_CROSSING", "ROAD_ABSTRACT_TYPE",
                                     "ROAD_BD_TOPO_TYPE", "VEGET_ABSTRACT_PARAMETERS", "VEGET_ABSTRACT_TYPE",
                                     "VEGET_BD_TOPO_TYPE"]

    @BeforeAll
    static void init(){
        if(ProcessingChainBDTopoTest.class.getResource(bdtopoFoldName) != null &&
                new File(ProcessingChainBDTopoTest.class.getResource(bdtopoFoldName).toURI()).exists()) {
            System.properties.setProperty("data.bd.topo", "true")
        }
        else{
            System.properties.setProperty("data.bd.topo", "false")
        }
    }

    def loadFiles(String inseeCode, String dbSuffixName){
        H2GIS h2GISDatabase = H2GIS.open(h2db+dbSuffixName+";AUTO_SERVER=TRUE", "sa", "")

        // Load parameter files
        paramTables.each{
            h2GISDatabase.load(ProcessingChain.class.getResource(it+".csv"), it, true)
        }

        def relativePath = bdtopoFoldName + File.separator + inseeCode

        // Test whether there is a folder containing .shp files for the corresponding INSEE code
        if(ProcessingChain.class.getResource(relativePath)){
            // Test is the URL is a folder
            if(new File(ProcessingChain.class.getResource(relativePath).toURI()).isDirectory()){
                listTables.each {
                    def filePath = ProcessingChain.class.getResource(relativePath + File.separator + it + ".shp")
                    // If some layers are missing, do not try to load them...
                    if (filePath) {
                        h2GISDatabase.load(filePath, it, true)
                    }
                }
            }
            else{
                logger.error  "There is no folder containing shapefiles for commune insee $inseeCode"
                fail()
            }
        }
        else{
            logger.error  "There is no folder containing shapefiles for commune insee $inseeCode"
            fail()
        }


        return h2GISDatabase
    }

    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void prepareBDTopoTest(){
        def dbSuffixName = "_prepare"
        def inseeCode = "01306"
        H2GIS h2GISDatabase = loadFiles(inseeCode, dbSuffixName)
        def process = ProcessingChain.PrepareBDTopo.prepareBDTopo()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        process.getResults().each {entry ->
            if(entry.key == 'outputStats') {
                entry.value.each{tab -> assertNotNull(h2GISDatabase.getTable(tab))}
            }
            else{assertNotNull(h2GISDatabase.getTable(entry.getValue()))}
        }
    }

    @Test
    void CreateUnitsOfAnalysisTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchainscales;AUTO_SERVER=TRUE")
        String sqlString = new File(getClass().getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT id_road, the_geom, zindex, crossing FROM road_test WHERE id_road < 5 OR id_road > 6;" +
                "CREATE TABLE tempo_zone AS SELECT * FROM zone_test;" +
                "CREATE TABLE tempo_veget AS SELECT id_veget, the_geom FROM veget_test WHERE id_veget < 4;" +
                "CREATE TABLE tempo_hydro AS SELECT id_hydro, the_geom FROM hydro_test WHERE id_hydro < 2;")
        IProcess pm =  ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        pm.execute([datasource          : h2GIS,        zoneTable               : "tempo_zone",     roadTable           : "tempo_road",
                    railTable           : "tempo_road", vegetationTable         : "tempo_veget",    hydrographicTable   : "tempo_hydro",
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
//        assertTrue(4 == row_nb.nb_blocks)
//        assertTrue(row_bu4.id_block ==row_bu8.id_block)
    }



    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void bdtopoLczFromTestFiles() {
        def dbSuffixName = "_lcz"
        String inseeCode = "01306"
        H2GIS datasource = loadFiles(inseeCode, dbSuffixName)
        def process = ProcessingChain.PrepareBDTopo.prepareBDTopo()
        assertTrue process.execute([datasource: datasource,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: inseeCode,
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        def abstractTables = process.getResults()

        String directory ="./target/bdtopo_processchain_lcz"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        // Define the weights of each indicator in the LCZ creation
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        IProcess geodindicators = ProcessingChain.Workflow.GeoIndicators()
        assertTrue geodindicators.execute(datasource: datasource, zoneTable: abstractTables.outputZone,
                buildingTable: abstractTables.outputBuilding, roadTable: abstractTables.outputRoad,
                railTable: abstractTables.outputRail, vegetationTable: abstractTables.outputVeget,
                hydrographicTable: abstractTables.outputHydro, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights)

        assertTrue(datasource.getTable(geodindicators.results.outputTableBuildingIndicators).rowCount>0)
        assertNull(geodindicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuIndicators).rowCount>0)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuLcz).rowCount>0)
    }


    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void bdtopoGeoIndicatorsFromTestFiles() {
        def dbSuffixName = "_geoIndicators"
        String inseeCode = "01306"
        H2GIS h2GISDatabase = loadFiles(inseeCode, dbSuffixName)
        def process = ProcessingChain.PrepareBDTopo.prepareBDTopo()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: inseeCode,
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        def abstractTables = process.getResults()

        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = false
        def indicatorUse = ["TEB", "URBAN_TYPOLOGY", "LCZ"]
        String directory ="./target/bdtopo_processchain_lcz"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

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
        def inseeCode = "01306"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        IProcess processBDTopo = ProcessingChain.Workflow.BDTOPO_V2()
        assertTrue(processBDTopo.execute(configurationFile: getClass().getResource("config/bdtopo_workflow_folderinput_folderoutput_id_zones.json").toURI()))mm
        assertNotNull(processBDTopo.getResults().outputFolder)
        def baseNamePathAndFileOut = processBDTopo.getResults().outputFolder + File.separator + "zone_" + inseeCode + "_"
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
        IProcess process = ProcessingChain.Workflow.BDTOPO_V2()
        assertTrue(process.execute(configurationFile: configFile))
    }
}