package org.orbisgis.processingchain


import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.assertEquals

class ProcessingChainBDTopoTest extends ChainProcessMainTest{

    public static Logger logger = LoggerFactory.getLogger(ProcessingChainBDTopoTest.class)

    @BeforeAll
    static void init(){
        if(ProcessingChainBDTopoTest.class.getResource("bdtopofolder") != null &&
                new File(ProcessingChainBDTopoTest.class.getResource("bdtopofolder").toURI()).exists()) {
            System.properties.setProperty("data.bd.topo", "true")
            H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb;AUTO_SERVER=TRUE", "sa", "")
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/IRIS_GE.shp"), "IRIS_GE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/BATI_INDIFFERENCIE.shp"), "BATI_INDIFFERENCIE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/BATI_INDUSTRIEL.shp"), "BATI_INDUSTRIEL", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/BATI_REMARQUABLE.shp"), "BATI_REMARQUABLE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/ROUTE.shp"), "ROUTE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/SURFACE_EAU.shp"), "SURFACE_EAU", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/ZONE_VEGETATION.shp"), "ZONE_VEGETATION", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("bdtopofolder/TRONCON_VOIE_FERREE.shp"), "TRONCON_VOIE_FERREE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("BUILDING_ABSTRACT_PARAMETERS.csv"), "BUILDING_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("BUILDING_ABSTRACT_USE_TYPE.csv"), "BUILDING_ABSTRACT_USE_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("BUILDING_BD_TOPO_USE_TYPE.csv"), "BUILDING_BD_TOPO_USE_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("RAIL_ABSTRACT_TYPE.csv"), "RAIL_ABSTRACT_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("RAIL_BD_TOPO_TYPE.csv"), "RAIL_BD_TOPO_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("ROAD_ABSTRACT_PARAMETERS.csv"), "ROAD_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("ROAD_ABSTRACT_SURFACE.csv"), "ROAD_ABSTRACT_SURFACE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("ROAD_ABSTRACT_TYPE.csv"), "ROAD_ABSTRACT_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("RAIL_ABSTRACT_TYPE.csv"), "RAIL_ABSTRACT_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("ROAD_BD_TOPO_TYPE.csv"), "ROAD_BD_TOPO_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("VEGET_ABSTRACT_PARAMETERS.csv"), "VEGET_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("VEGET_ABSTRACT_TYPE.csv"), "VEGET_ABSTRACT_TYPE", true)
            h2GISDatabase.load(ProcessingChain.class.getResource("VEGET_BD_TOPO_TYPE.csv"), "VEGET_BD_TOPO_TYPE", true)
        }
        else{
            System.properties.setProperty("data.bd.topo", "false")
        }
    }

    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void prepareBDTopoTest(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
        def process = ProcessingChain.PrepareBDTopo.prepareBDTopo()
        assertTrue process.execute([datasource: h2GISDatabase, tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
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
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void CreateUnitsOfAnalysisTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchainscales")
        String sqlString = new File(getClass().getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT id_road, the_geom, zindex FROM road_test WHERE id_road < 5 OR id_road > 6;" +
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
    void createLCZTest(){
        H2GIS h2GIS = H2GIS.open("./target/processinglcz")
        String sqlString = new File(getClass().getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT id_build, the_geom, height_wall, height_roof," +
                "nb_lev FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT * FROM road_test WHERE id_road < 5 OR id_road > 6;" +
                "CREATE TABLE tempo_zone AS SELECT * FROM zone_test;" +
                "CREATE TABLE tempo_veget AS SELECT * FROM veget_test WHERE id_veget < 5;" +
                "CREATE TABLE tempo_hydro AS SELECT * FROM hydro_test WHERE id_hydro < 2;")


        // First create the scales
        IProcess pm_units = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        pm_units.execute([datasource: h2GIS, zoneTable : "tempo_zone", buildingTable:"tempo_build", roadTable : "tempo_road", railTable : "tempo_road",
                          vegetationTable: "tempo_veget", hydrographicTable: "tempo_hydro", surface_vegetation: null,
                          surface_hydro: null, distance: 0.0, prefixName: "test", indicatorUse :["LCZ"]])

        IProcess pm_lcz =  ProcessingChain.BuildLCZ.createLCZ()
        pm_lcz.execute([datasource: h2GIS, prefixName: "test", buildingTable: pm_units.results.outputTableBuildingName,
                        rsuTable: pm_units.results.outputTableRsuName, roadTable: "tempo_road", vegetationTable: "tempo_veget",
                        hydrographicTable: "tempo_hydro", facadeDensListLayersBottom: [0, 50, 200], facadeDensNumberOfDirection: 8,
                        svfPointDensity: 0.008, svfRayLength: 100, svfNumberOfDirection: 60,
                        heightColumnName: "height_roof",
                        mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                                       "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                                       "height_of_roughness_elements": 1, "terrain_roughness_class": 1],

                        fractionTypePervious: ["low_vegetation", "water"],
                        fractionTypeImpervious: ["road"], inputFields: ["id_build"], levelForRoads: [0]])

        h2GIS.eachRow("SELECT * FROM ${pm_lcz.results.outputTableName}".toString()){row ->
            assertTrue(row.id_rsu != null)
            println(row.lcz1)
            assertEquals("LCZ", row.lcz1[0..2])
            assertEquals("LCZ", row.lcz2[0..2])
//            assertTrue(row.min_distance != null)
//            assertTrue(row.pss <= 1)
        }
//        def nb_rsu = h2GIS.firstRow("SELECT COUNT(*) AS nb FROM ${pm_lcz.results.outputTableName}".toString())
//        assertEquals(14, nb_rsu.nb)
    }



    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void bdtopoLczFromTestFiles() {
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
        def process = ProcessingChain.PrepareBDTopo.prepareBDTopo()
        assertTrue process.execute([datasource: h2GISDatabase, tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
                                    hLevMin: 3, hLevMax : 15, hThresholdLev2 : 10
        ])
        def abstractTables = process.getResults()

        boolean saveResults = true
        String directory ="./target/bdtopo_processchain_lcz"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        //Run tests
        calcLcz(directory, h2GISDatabase, abstractTables.outputZone, abstractTables.outputBuilding,abstractTables.outputRoad,
                abstractTables.outputRoad, abstractTables.outputVeget, abstractTables.outputHydro,saveResults)
    }
}