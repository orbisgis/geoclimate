package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager

import static org.junit.jupiter.api.Assertions.*

class InputDataFormattingTest {

    def h2GISDatabase

    public static communeToTest = "abcde"

    @BeforeEach
    void beforeEach(){
        def dataFolderInseeCode = "bd_topo_unit_test"
        def listFilesBDTopo = ["IRIS_GE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                               "ROUTE", "TRONCON_VOIE_FERREE", "SURFACE_EAU", "ZONE_VEGETATION"
                              ,"TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE","SURFACE_ROUTE", "SURFACE_ACTIVITE"]

        def paramTables = ["BUILDING_ABSTRACT_PARAMETERS", "BUILDING_ABSTRACT_USE_TYPE", "BUILDING_BD_TOPO_USE_TYPE",
                                         "RAIL_ABSTRACT_TYPE", "RAIL_BD_TOPO_TYPE", "RAIL_ABSTRACT_CROSSING",
                                         "RAIL_BD_TOPO_CROSSING", "ROAD_ABSTRACT_PARAMETERS", "ROAD_ABSTRACT_SURFACE",
                                         "ROAD_ABSTRACT_CROSSING", "ROAD_BD_TOPO_CROSSING", "ROAD_ABSTRACT_TYPE",
                                         "ROAD_BD_TOPO_TYPE", "VEGET_ABSTRACT_PARAMETERS", "VEGET_ABSTRACT_TYPE",
                                         "VEGET_BD_TOPO_TYPE"]

        h2GISDatabase = H2GIS.open("./target/h2gis_input_data_formating;AUTO_SERVER=TRUE", "sa", "")

        // Load parameter files
        paramTables.each{
            h2GISDatabase.load(getClass().getResource(it+".csv"), it, true)
        }
        // Load data files
        listFilesBDTopo.each{
            println(it)
            h2GISDatabase.load(getClass().getResource("$dataFolderInseeCode/${it.toLowerCase()}.shp"), it, true)
        }
    }

    @Test
    void inputDataFormatting(){
        def processImport = BDTopo_V2.importPreprocess
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                          tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                          tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                          tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                          tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                          tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                          distBuffer: 500, expand: 1000, idZone: communeToTest,
                                          building_bd_topo_use_type: 'BUILDING_BD_TOPO_USE_TYPE', building_abstract_use_type: 'BUILDING_ABSTRACT_USE_TYPE',
                                          road_bd_topo_type: 'ROAD_BD_TOPO_TYPE', road_abstract_type: 'ROAD_ABSTRACT_TYPE',
                                          road_bd_topo_crossing: 'ROAD_BD_TOPO_CROSSING', road_abstract_crossing: 'ROAD_ABSTRACT_CROSSING',
                                          rail_bd_topo_type: 'RAIL_BD_TOPO_TYPE', rail_abstract_type: 'RAIL_ABSTRACT_TYPE',
                                          rail_bd_topo_crossing: 'RAIL_BD_TOPO_CROSSING', rail_abstract_crossing: 'RAIL_ABSTRACT_CROSSING',
                                          veget_bd_topo_type: 'VEGET_BD_TOPO_TYPE', veget_abstract_type: 'VEGET_ABSTRACT_TYPE'
        ])
        def resultsImport=processImport.results

        def processFormatting = BDTopo_V2.formatInputData
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                         inputBuilding: resultsImport.outputBuildingName, inputRoad: resultsImport.outputRoadName,
                         inputRail: resultsImport.outputRailName, inputHydro: resultsImport.outputHydroName,
                         inputVeget: resultsImport.outputVegetName, inputImpervious: resultsImport.outputImperviousName,
                         inputZone: resultsImport.outputZoneName, //inputZoneNeighbors: resultsImport.outputZoneNeighborsName,

                         hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, idZone: communeToTest, expand: 1000,

                         buildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE', buildingAbstractParameters: 'BUILDING_ABSTRACT_PARAMETERS',
                         roadAbstractType: 'ROAD_ABSTRACT_TYPE', roadAbstractParameters: 'ROAD_ABSTRACT_PARAMETERS', roadAbstractCrossing: 'ROAD_ABSTRACT_CROSSING',
                         railAbstractType: 'RAIL_ABSTRACT_TYPE', railAbstractCrossing: 'RAIL_ABSTRACT_CROSSING',
                         vegetAbstractType: 'VEGET_ABSTRACT_TYPE', vegetAbstractParameters: 'VEGET_ABSTRACT_PARAMETERS'])
        processFormatting.results.each {
            entry -> assertNotNull h2GISDatabase.getTable(entry.getValue())
        }

        // -----------------------------------------------------------------------------------
        // For BUILDINGS
        // -----------------------------------------------------------------------------------

        // Check if the BUILDING table has the correct number of columns and rows
        def tableName = processFormatting.results.outputBuilding
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(10, table.columnCount)
        assertEquals(19, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_BUILD'))
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('INTEGER', table.columnType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.columnType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.columnType('NB_LEV'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        assertEquals('VARCHAR', table.columnType('MAIN_USE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))

        // For each rows, check if the fields contains the expected values
        table.eachRow(){ row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_BUILD)
            assertNotEquals('', row.ID_BUILD)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            // Check that the HEIGHT_WALL is smaller than 1000m high
            assertNotNull(row.HEIGHT_WALL)
            assertNotEquals('', row.HEIGHT_WALL)
            assertTrue(row.HEIGHT_WALL >= 0)
            assertTrue(row.HEIGHT_WALL <= 1000)
            // Check that there is no rows with a HEIGHT_ROOF value (will be updated in the following process)
            assertNotEquals('', row.HEIGHT_ROOF)
            assertNotEquals('', row.HEIGHT_ROOF)
            assertTrue(row.HEIGHT_ROOF >= 0)
            assertTrue(row.HEIGHT_ROOF <= 1000)
            // Check that there is no rows with a NB_LEV value (will be updated in the following process)
            assertNotEquals('', row.NB_LEV)
            assertNotEquals('', row.NB_LEV)
            assertTrue(row.NB_LEV >= 0)
            assertTrue(row.NB_LEV <= 1000)
            assertNotNull(row.TYPE)
            assertEquals('', row.MAIN_USE)
            assertNotNull(row.ZINDEX)
            assertNotEquals('', row.ZINDEX)
            assertEquals(0, row.ZINDEX)
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
        }

        //Check value for  specific features
        //TODO: to be fixed nb_level is wrong
        /*def res =  h2GISDatabase.firstRow("select type,  nb_lev, height_wall, height_roof from ${tableName} where ID_SOURCE='BATIMENT0000000257021459'")
        assertEquals("building", res.TYPE)
        assertEquals(1, res.NB_LEV)
        assertEquals(6, res.HEIGHT_WALL)
        assertEquals(6, res.HEIGHT_ROOF)*/

        
        // Specific cases
        // -------------------------------
        // ... with the building 'BREMAR0001' : HAUTEUR = 0 / TYPE = 'Bâtiment sportif'
        assertEquals(3, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0001';")["HEIGHT_WALL"])
        assertEquals(3, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0001';")["HEIGHT_ROOF"])
        assertEquals(1, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0001';")["NB_LEV"])
        assertEquals('sports_centre', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0001';")["TYPE"])

        //... with the building 'BREMAR0002' : HAUTEUR = 0 / NATURE = 'Tour, donjon, moulin'
        assertEquals(3, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0002';")["HEIGHT_WALL"])
        assertEquals(3, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0002';")["HEIGHT_ROOF"])
        assertEquals(1, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0002';")["NB_LEV"])
        assertEquals('historic', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0002';")["TYPE"])

        //... with the building 'BREMAR0003' : HAUTEUR = 12 / NATURE = 'Chapelle'
        assertEquals(12, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0003';")["HEIGHT_WALL"])
        assertEquals(12, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0003';")["HEIGHT_ROOF"])
        assertEquals(1, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0003';")["NB_LEV"])
        assertEquals('chapel', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0003';")["TYPE"])

        //... with the building 'BREMAR0004' : HAUTEUR = 20 / NATURE = 'Mairie'
        assertEquals(20, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0004';")["HEIGHT_WALL"])
        assertEquals(20, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0004';")["HEIGHT_ROOF"])
        assertEquals(6, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0004';")["NB_LEV"])
        assertEquals('townhall', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0004';")["TYPE"])

        //... with the building 'BINDUS0001' : HAUTEUR = 13 / NATURE = 'Bâtiment commercial'
        assertEquals(13, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0001';")["HEIGHT_WALL"])
        assertEquals(13, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0001';")["HEIGHT_ROOF"])
        assertEquals(4, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0001';")["NB_LEV"])
        assertEquals('commercial', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0001';")["TYPE"])

        //... with the building 'BINDUS0002' : HAUTEUR = 10 / NATURE = 'Bâtiment industriel'
        assertEquals(10, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0002';")["HEIGHT_WALL"])
        assertEquals(10, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0002';")["HEIGHT_ROOF"])
        assertEquals(1, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0002';")["NB_LEV"])
        assertEquals('industrial', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0002';")["TYPE"])

        //... with the building 'BINDIF0001' : HAUTEUR = 0 / Bati indif so no NATURE
        assertEquals(3, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0001';")["HEIGHT_WALL"])
        assertEquals(3, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0001';")["HEIGHT_ROOF"])
        assertEquals(1, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0001';")["NB_LEV"])
        assertEquals('building', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0001';")["TYPE"])

        //... with the building 'BINDIF0002' : HAUTEUR = 17 / Bati indif so no NATURE
        assertEquals(17, h2GISDatabase.firstRow("SELECT HEIGHT_WALL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0002';")["HEIGHT_WALL"])
        assertEquals(17, h2GISDatabase.firstRow("SELECT HEIGHT_ROOF FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0002';")["HEIGHT_ROOF"])
        assertEquals(5, h2GISDatabase.firstRow("SELECT NB_LEV FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0002';")["NB_LEV"])
        assertEquals('building', h2GISDatabase.firstRow("SELECT TYPE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0002';")["TYPE"])

        // Check if building are well selected or not ...
        // ... with the building (INDIF) 'BINDIF0003' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0003';")["TOTAL"])
        // ... with the building (INDIF) 'BINDIF0004' which is inside the buffer zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0004';")["TOTAL"])
        // ... with the building (INDIF) 'BINDIF0005' which is outside the buffer zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0005';")["TOTAL"])
        // ... with the building (INDUS) 'BINDUS0003' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0003';")["TOTAL"])
        // ... with the building (INDUS) 'BINDUS0005' which is inside the buffer zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0005';")["TOTAL"])
        // ... with the building (INDUS) 'BINDUS0006' which is outside the buffer zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BINDUS0006';")["TOTAL"])
        // ... with the building (REMARQ) 'BREMAR0005' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0005';")["TOTAL"])
        // ... with the building (REMARQ) 'BREMAR0006' which is inside the buffer zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0006';")["TOTAL"])
        // ... with the building (REMARQ) 'BREMAR0007' which is outside the buffer zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM BUILDING " +
                "WHERE ID_SOURCE='BREMAR0007';")["TOTAL"])

        // Check if building are associated to the appropriate city (ZONE_ID) ...
        // ... with the building (INDIF) 'BINDIF0006' which in Gwened (abcde)
        assertEquals('abcde', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0006';")["ID_ZONE"])
        // ... with the building (INDIF) 'BINDIF0007' which in Saint-Avé (56206 - so 'outside' expected)
        assertEquals('outside', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0007';")["ID_ZONE"])
        // ... with the building (INDIF) 'BINDIF0008' which in Arradon (56003 - so 'outside' expected)
        assertEquals('outside', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0008';")["ID_ZONE"])

        // Verifies that a building that straddles two communes is assigned to the right area
        // ... with the building (INDIF) 'BINDIF0009' which main part is in Séné (56243 - so 'outside' expected)
        assertEquals('outside', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0009';")["ID_ZONE"])
        // ... with the building (INDIF) 'BINDIF0010' which main part is in Gwened (abcde)
        assertEquals('abcde', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
                "WHERE ID_SOURCE='BINDIF0010';")["ID_ZONE"])
        

        // ------------------
        // Check if the BUILDING_STATS_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputBuildingStatZone
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(16, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_BUILD'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('FC_H_ZERO'))
        assertEquals('BIGINT', table.columnType('FC_H_NULL'))
        assertEquals('BIGINT', table.columnType('FC_H_RANGE'))
        assertEquals('BIGINT', table.columnType('H_NULL'))
        assertEquals('BIGINT', table.columnType('H_RANGE'))
        assertEquals('BIGINT', table.columnType('H_ROOF_MIN_WALL'))
        assertEquals('BIGINT', table.columnType('LEV_NULL'))
        assertEquals('BIGINT', table.columnType('LEV_RANGE'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_BUILD)
            assertNotEquals('', row.NB_BUILD)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.FC_H_ZERO)
            assertNotEquals('', row.FC_H_ZERO)
            assertNotNull(row.FC_H_NULL)
            assertNotEquals('', row.FC_H_NULL)
            assertNotNull(row.FC_H_RANGE)
            assertNotEquals('', row.FC_H_RANGE)
            assertNotNull(row.H_NULL)
            assertNotEquals('', row.H_NULL)
            assertNotNull(row.H_RANGE)
            assertNotEquals('', row.H_RANGE)
            assertNotNull(row.H_ROOF_MIN_WALL)
            assertNotEquals('', row.H_ROOF_MIN_WALL)
            assertNotNull(row.LEV_NULL)
            assertNotEquals('', row.LEV_NULL)
            assertNotNull(row.LEV_RANGE)
            assertNotEquals('', row.LEV_RANGE)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // ------------------
        // Check if the BUILDING_STATS_EXT_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputBuildingStatZoneBuff
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(16, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_BUILD'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('FC_H_ZERO'))
        assertEquals('BIGINT', table.columnType('FC_H_NULL'))
        assertEquals('BIGINT', table.columnType('FC_H_RANGE'))
        assertEquals('BIGINT', table.columnType('H_NULL'))
        assertEquals('BIGINT', table.columnType('H_RANGE'))
        assertEquals('BIGINT', table.columnType('H_ROOF_MIN_WALL'))
        assertEquals('BIGINT', table.columnType('LEV_NULL'))
        assertEquals('BIGINT', table.columnType('LEV_RANGE'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_BUILD)
            assertNotEquals('', row.NB_BUILD)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.FC_H_ZERO)
            assertNotEquals('', row.FC_H_ZERO)
            assertNotNull(row.FC_H_NULL)
            assertNotEquals('', row.FC_H_NULL)
            assertNotNull(row.FC_H_RANGE)
            assertNotEquals('', row.FC_H_RANGE)
            assertNotNull(row.H_NULL)
            assertNotEquals('', row.H_NULL)
            assertNotNull(row.H_RANGE)
            assertNotEquals('', row.H_RANGE)
            assertNotNull(row.H_ROOF_MIN_WALL)
            assertNotEquals('', row.H_ROOF_MIN_WALL)
            assertNotNull(row.LEV_NULL)
            assertNotEquals('', row.LEV_NULL)
            assertNotNull(row.LEV_RANGE)
            assertNotEquals('', row.LEV_RANGE)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // -----------------------------------------------------------------------------------
        // For ROADS
        // -----------------------------------------------------------------------------------

        // Check if the ROAD table has the correct number of columns and rows
        tableName = processFormatting.results.outputRoad
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(9, table.columnCount)
        assertEquals(9, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_ROAD'))
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('DOUBLE', table.columnType('WIDTH'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        assertEquals('VARCHAR', table.columnType('SURFACE'))
        assertEquals('VARCHAR', table.columnType('SIDEWALK'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('VARCHAR', table.columnType('CROSSING'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_ROAD)
            assertNotEquals('', row.ID_ROAD)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            // Check that the WIDTH is smaller than 100m
            assertNotNull(row.WIDTH)
            assertNotEquals('', row.WIDTH)
            assertTrue(row.WIDTH >= 0)
            assertTrue(row.WIDTH <= 100)
            assertNotNull(row.TYPE)
            assertNotEquals('', row.TYPE)
            assertNotNull(row.SURFACE)
            assertNotNull(row.SIDEWALK)
            assertNotNull(row.ZINDEX)
            assertNotEquals('', row.ZINDEX)
            assertNotNull(row.CROSSING)
            assertNotEquals('', row.CROSSING)
        }
        
        // Specific cases
        // -------------------------------
        //... with the road 'ROUTE0001' : LARGEUR = 0 / NATURE = 'Sentier'
        assertEquals(1, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0001';")["WIDTH"])
        assertEquals('path', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0001';")["TYPE"])

        //... with the road 'ROUTE0002' : LARGEUR = 5,5 / NATURE = 'Route à 1 chaussée'
        assertEquals(5.5, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0002';")["WIDTH"])
        assertEquals('unclassified', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0002';")["TYPE"])

        //... with the road 'ROUTE0003' : LARGEUR = 0 / NATURE = 'Route empierrée'
        assertEquals(2, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0003';")["WIDTH"])
        assertEquals('track', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0003';")["TYPE"])

        //... with the road 'ROUTE0004' : LARGEUR = 10 / NATURE = 'Quasi-autoroute'
        assertEquals(10, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0004';")["WIDTH"])
        assertEquals('trunk', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0004';")["TYPE"])

        //... with the road 'ROUTE0005' : LARGEUR = 4 / NATURE = 'Bretelle'
        assertEquals(4, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0005';")["WIDTH"])
        assertEquals('highway_link', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0005';")["TYPE"])

        //... with the road 'ROUTE0006' : LARGEUR = 0 / NATURE = 'Route empierrée'
        assertEquals(2, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0006';")["WIDTH"])
        assertEquals('track', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0006';")["TYPE"])

        //... with the road 'ROUTE0007' : LARGEUR = 0 / NATURE = 'Piste cyclable'
        assertEquals(1, h2GISDatabase.firstRow("SELECT WIDTH FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0007';")["WIDTH"])
        assertEquals('cycleway', h2GISDatabase.firstRow("SELECT TYPE FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0007';")["TYPE"])

        // Check if roads are well selected or not ...
        // ... with the road 'ROUTE0008' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0008';")["TOTAL"])
        // ... with the road 'ROUTE0009' which is inside the buffer zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0009';")["TOTAL"])
        // ... with the road 'ROUTE0010' which is outside the buffer zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM ROAD " +
                "WHERE ID_SOURCE='ROUTE0010';")["TOTAL"])
        

        // ------------------
        // Check if the ROAD_STATS_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputRoadStatZone
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(13, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_ROAD'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('FC_W_ZERO'))
        assertEquals('BIGINT', table.columnType('FC_W_NULL'))
        assertEquals('BIGINT', table.columnType('FC_W_RANGE'))
        assertEquals('BIGINT', table.columnType('W_NULL'))
        assertEquals('BIGINT', table.columnType('W_RANGE'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_ROAD)
            assertNotEquals('', row.NB_ROAD)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.FC_W_ZERO)
            assertNotEquals('', row.FC_W_ZERO)
            assertNotNull(row.FC_W_NULL)
            assertNotEquals('', row.FC_W_NULL)
            assertNotNull(row.FC_W_RANGE)
            assertNotEquals('', row.FC_W_RANGE)
            assertNotNull(row.W_NULL)
            assertNotEquals('', row.W_NULL)
            assertNotNull(row.W_RANGE)
            assertNotEquals('', row.W_RANGE)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // ------------------
        // Check if the ROAD_STATS_EXT_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputRoadStatZoneBuff
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(13, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_ROAD'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('FC_W_ZERO'))
        assertEquals('BIGINT', table.columnType('FC_W_NULL'))
        assertEquals('BIGINT', table.columnType('FC_W_RANGE'))
        assertEquals('BIGINT', table.columnType('W_NULL'))
        assertEquals('BIGINT', table.columnType('W_RANGE'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_ROAD)
            assertNotEquals('', row.NB_ROAD)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.FC_W_ZERO)
            assertNotEquals('', row.FC_W_ZERO)
            assertNotNull(row.FC_W_NULL)
            assertNotEquals('', row.FC_W_NULL)
            assertNotNull(row.FC_W_RANGE)
            assertNotEquals('', row.FC_W_RANGE)
            assertNotNull(row.W_NULL)
            assertNotEquals('', row.W_NULL)
            assertNotNull(row.W_RANGE)
            assertNotEquals('', row.W_RANGE)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // -----------------------------------------------------------------------------------
        // For RAILS
        // -----------------------------------------------------------------------------------

        // Check if the RAIL table has the correct number of columns and rows
        tableName = processFormatting.results.outputRail
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(6, table.columnCount)
        assertEquals(3, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_RAIL'))
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('VARCHAR', table.columnType('CROSSING'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_RAIL)
            assertNotEquals('', row.ID_RAIL)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            assertNotNull(row.TYPE)
            assertNotEquals('', row.TYPE)
            assertNotNull(row.ZINDEX)
            assertNotEquals('', row.ZINDEX)
            assertNotNull(row.CROSSING)
            assertNotEquals('', row.CROSSING)
        }
        
        // Specific cases
        // -------------------------------
        //... with the rail 'RAIL0001' : NATURE = 'Voie de service'
        assertEquals('service_track', h2GISDatabase.firstRow("SELECT TYPE FROM RAIL " +
                "WHERE ID_SOURCE='RAIL0001';")["TYPE"])

        //... with the rail 'RAIL0002' : NATURE = 'Principale'
        assertEquals('rail', h2GISDatabase.firstRow("SELECT TYPE FROM RAIL " +
                "WHERE ID_SOURCE='RAIL0002';")["TYPE"])

        // Check if rails are well selected or not ...
        // ... with the rail 'RAIL0002' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM RAIL " +
                "WHERE ID_SOURCE='RAIL0002';")["TOTAL"])
        // ... with the rail 'RAIL0003' which is intersecting the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM RAIL " +
                "WHERE ID_SOURCE='RAIL0003';")["TOTAL"])
        // ... with the rail 'RAIL0004' which is not intersecting the zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM RAIL " +
                "WHERE ID_SOURCE='RAIL0004';")["TOTAL"])
        

        // ------------------
        // Check if the RAIL_STATS_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputRailStatZone
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_RAIL'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_RAIL)
            assertNotEquals('', row.NB_RAIL)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // -----------------------------------------------------------------------------------
        // For HYDROGRAPHIC AREAS
        // -----------------------------------------------------------------------------------

        // Check if the HYDRO table has the correct number of columns and rows
        tableName = processFormatting.results.outputHydro
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(3, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_HYDRO'))
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_HYDRO)
            assertNotEquals('', row.ID_HYDRO)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }
        
        // Specific cases
        // -------------------------------
        // Check if hydrographic area are well selected or not ...
        // ... with the hydro area 'SURFEAU0001' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM HYDRO " +
                "WHERE ID_SOURCE='SURFEAU0001';")["TOTAL"])
        // ... with the hydro area 'SURFEAU0002' which is inside the extended zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM HYDRO " +
                "WHERE ID_SOURCE='SURFEAU0002';")["TOTAL"])
        // ... with the hydro area 'SURFEAU0003' which is outside the extended zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM HYDRO " +
                "WHERE ID_SOURCE='SURFEAU0003';")["TOTAL"])



        // ------------------
        // Check if the HYDRO_STATS_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputHydroStatZone
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(6, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_HYDRO'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_HYDRO)
            assertNotEquals('', row.NB_HYDRO)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
        }

        // ------------------
        // Check if the HYDRO_STATS_EXT_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputHydroStatZoneExt
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(6, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_HYDRO'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_HYDRO)
            assertNotEquals('', row.NB_HYDRO)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
        }

        // -----------------------------------------------------------------------------------
        // For VEGETATION AREAS
        // -----------------------------------------------------------------------------------

        // Check if the VEGET table has the correct number of columns and rows
        tableName = processFormatting.results.outputVeget
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(5, table.columnCount)
        assertEquals(7, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_VEGET'))
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        assertEquals('VARCHAR', table.columnType('HEIGHT_CLASS'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_VEGET)
            assertNotEquals('', row.ID_VEGET)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            assertNotNull(row.TYPE)
            assertNotEquals('', row.TYPE)
            assertNotNull(row.HEIGHT_CLASS)
            assertNotEquals('', row.HEIGHT_CLASS)
        }
        
        // Specific cases
        // -------------------------------
        //... with the vegetation area 'VEGET0001' : NATURE = 'Forêt fermée de feuillus'
        assertEquals('forest', h2GISDatabase.firstRow("SELECT TYPE FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0001';")["TYPE"])
        assertEquals('high', h2GISDatabase.firstRow("SELECT HEIGHT_CLASS FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0001';")["HEIGHT_CLASS"])

        //... with the vegetation area 'VEGET0002' : NATURE = 'Forêt fermée mixte'
        assertEquals('forest', h2GISDatabase.firstRow("SELECT TYPE FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0002';")["TYPE"])
        assertEquals('high', h2GISDatabase.firstRow("SELECT HEIGHT_CLASS FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0002';")["HEIGHT_CLASS"])

        //... with the vegetation area 'VEGET0003' : NATURE = 'Bois'
        assertEquals('forest', h2GISDatabase.firstRow("SELECT TYPE FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0003';")["TYPE"])
        assertEquals('high', h2GISDatabase.firstRow("SELECT HEIGHT_CLASS FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0003';")["HEIGHT_CLASS"])

        //... with the vegetation area 'VEGET0004' : NATURE = 'Haie'
        assertEquals('hedge', h2GISDatabase.firstRow("SELECT TYPE FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0004';")["TYPE"])
        assertEquals('high', h2GISDatabase.firstRow("SELECT HEIGHT_CLASS FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0004';")["HEIGHT_CLASS"])

        // Check if vegetation area are well selected or not ...
        // ... with the veget area 'VEGET0005' which is inside the zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0005';")["TOTAL"])
        // ... with the veget area 'VEGET0006' which is inside the extended zone --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0006';")["TOTAL"])
        // ... with the veget area 'VEGET0007' which is intersecting the extended zone (having a part outside) --> so expected 1
        assertEquals(1, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0007';")["TOTAL"])
        // ... with the veget area 'VEGET0008' which is outside the extended zone --> so expected 0
        assertEquals(0, h2GISDatabase.firstRow("SELECT COUNT(*) as TOTAL FROM VEGET " +
                "WHERE ID_SOURCE='VEGET0008';")["TOTAL"])
        

        // ------------------
        // Check if the VEGET_STATS_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputVegetStatZone
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_VEGET'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_VEGET)
            assertNotEquals('', row.NB_VEGET)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // ------------------
        // Check if the VEGET_STATS_ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputVegetStatZoneExt
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertEquals('BIGINT', table.columnType('NB_VEGET'))
        assertEquals('BIGINT', table.columnType('NOT_VALID'))
        assertEquals('BIGINT', table.columnType('IS_EMPTY'))
        assertEquals('BIGINT', table.columnType('IS_EQUALS'))
        assertEquals('BIGINT', table.columnType('OVERLAP'))
        assertEquals('BIGINT', table.columnType('NO_TYPE'))
        assertEquals('BIGINT', table.columnType('TYPE_RANGE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.NB_VEGET)
            assertNotEquals('', row.NB_VEGET)
            assertNotNull(row.NOT_VALID)
            assertNotEquals('', row.NOT_VALID)
            assertNotNull(row.IS_EMPTY)
            assertNotEquals('', row.IS_EMPTY)
            assertNotNull(row.IS_EQUALS)
            assertNotEquals('', row.IS_EQUALS)
            assertNotNull(row.OVERLAP)
            assertNotEquals('', row.OVERLAP)
            assertNotNull(row.NO_TYPE)
            assertNotEquals('', row.NO_TYPE)
            assertNotNull(row.TYPE_RANGE)
            assertNotEquals('', row.TYPE_RANGE)
        }

        // -----------------------------------------------------------------------------------
        // For IMPERVIOUS AREAS
        // -----------------------------------------------------------------------------------

        // Check if the IMPERVIOUS table has the correct number of columns and rows
        tableName = processFormatting.results.outputImpervious
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(4, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_IMPERVIOUS'))
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_IMPERVIOUS)
            assertNotEquals('', row.ID_IMPERVIOUS)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // -----------------------------------------------------------------------------------
        // For ZONE
        // -----------------------------------------------------------------------------------

        // Check if the ZONE table has the correct number of columns and rows
        tableName = processFormatting.results.outputZone
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertTrue(table.the_geom.spatial)
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
        }
    }
}
