package org.orbisgis.geoclimate.bdtopo_v2

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.data.H2GIS

import static org.junit.jupiter.api.Assertions.*

class InputDataFormattingTest {

    H2GIS h2GISDatabase

    public static communeToTest = "abcde"

    @BeforeEach
    void beforeEach(){
        def dataFolderInseeCode = "bd_topo_unit_test"
        def listFilesBDTopo = ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                               "ROUTE", "TRONCON_VOIE_FERREE", "SURFACE_EAU", "ZONE_VEGETATION"
                              ,"TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE","SURFACE_ROUTE", "SURFACE_ACTIVITE"]

        h2GISDatabase = H2GIS.open("./target/h2gis_input_data_formating;AUTO_SERVER=TRUE;", "sa", "")
        // Load data files
        listFilesBDTopo.each{
            h2GISDatabase.load(getClass().getResource("$dataFolderInseeCode${File.separator+it.toLowerCase()}.shp"), it, true)
        }
    }

    @Test
    void formattingBuildingTest(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                          tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                          distance: 1000
        ])
        def resultsImport=processImport.results
        def processFormatting = BDTopo_V2.InputDataFormatting.formatBuildingLayer()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputTableName: resultsImport.outputBuildingName,
                                              inputZoneEnvelopeTableName: resultsImport.outputZoneName])
        def tableOutput = processFormatting.results.outputTableName
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where TYPE is not null;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where MAIN_USE is not null;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where NB_LEV is not null or NB_LEV>0 ;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where HEIGHT_WALL is not null or HEIGHT_WALL>0 ;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where HEIGHT_ROOF is not null or HEIGHT_ROOF>0 ;""".toString()).count>0)
    }

    @Test
    void formattingBuildingWithInputImperviousTest(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                          tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                          distance: 1000
        ])
        def resultsImport=processImport.results
        def processFormatting = BDTopo_V2.InputDataFormatting.formatBuildingLayer()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputTableName: resultsImport.outputBuildingName,
                                              inputZoneEnvelopeTableName: resultsImport.outputZoneName,
                                              inputImpervious: resultsImport.outputImperviousName])
        def tableOutput = processFormatting.results.outputTableName
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where TYPE is not null;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where MAIN_USE is not null;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where NB_LEV is not null or NB_LEV>0 ;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where HEIGHT_WALL is not null or HEIGHT_WALL>0 ;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where HEIGHT_ROOF is not null or HEIGHT_ROOF>0 ;""".toString()).count>0)
    }

    @Test
    void formattingRoadTest(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableRoadName: 'ROUTE',
                                          distance: 1000
        ])
        def resultsImport=processImport.results
        def processFormatting = BDTopo_V2.InputDataFormatting.formatRoadLayer()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputTableName: resultsImport.outputRoadName,
                                              inputZoneEnvelopeTableName: resultsImport.outputZoneName])
        def tableOutput = processFormatting.results.outputTableName
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where TYPE is not null;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where WIDTH is not null or WIDTH>0 ;""".toString()).count>0)
    }

    @Test
    void formattingRailTest(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableRoadName: 'TRONCON_VOIE_FERREE',
                                          distance: 1000
        ])
        def resultsImport=processImport.results
        def processFormatting = BDTopo_V2.InputDataFormatting.formatRailsLayer()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputTableName: resultsImport.outputRailName,
                                              inputZoneEnvelopeTableName: resultsImport.outputZoneName])
        def tableOutput = processFormatting.results.outputTableName
        assertTrue(h2GISDatabase.getTable(tableOutput).isEmpty())
    }

    @Test
    void formattingVegetationTest(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableVegetName: 'ZONE_VEGETATION',
                                          distance: 1000
        ])
        def resultsImport=processImport.results
        def processFormatting = BDTopo_V2.InputDataFormatting.formatVegetationLayer()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputTableName: resultsImport.outputVegetName,
                                              inputZoneEnvelopeTableName: resultsImport.outputZoneName])
        def tableOutput = processFormatting.results.outputTableName
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where TYPE is not null;""".toString()).count>0)
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where HEIGHT_CLASS is not null;""".toString()).count>0)
    }

    @Test
    void formattingWaterTest(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableHydroName: 'ZONE_VEGETATION',
                                          distance: 1000
        ])
        def resultsImport=processImport.results
        def processFormatting = BDTopo_V2.InputDataFormatting.formatHydroLayer()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputTableName: resultsImport.outputHydroName,
                                              inputZoneEnvelopeTableName: resultsImport.outputZoneName])
        def tableOutput = processFormatting.results.outputTableName
        assertTrue(h2GISDatabase.firstRow("""SELECT count(*) as count from $tableOutput where TYPE is not null;""".toString()).count>0)
    }
}
