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

    public static communeToTest = "12174"

    @BeforeEach
    void beforeEach(){
        def dataFolderInseeCode = "sample_$communeToTest"
        def listFilesBDTopo = ["IRIS_GE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                                "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "ZONE_VEGETATION",
                                "TRONCON_VOIE_FERREE", "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
                                "SURFACE_ROUTE", "SURFACE_ACTIVITE"]

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
            h2GISDatabase.load(getClass().getResource("$dataFolderInseeCode/${it}.shp"), it, true)
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
        assertEquals(23, table.rowCount)
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
        // ... with the building (INDIF) 'BINDIF0006' which in Vannes (56260)
        assertEquals('56260', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
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
        // ... with the building (INDIF) 'BINDIF0010' which main part is in Vannes (56260)
        assertEquals('56260', h2GISDatabase.firstRow("SELECT ID_ZONE FROM BUILDING " +
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
        assertEquals(10, table.rowCount)
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
        assertEquals(92, table.rowCount)
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


        // Check that the intersection between the Hydro geom (ID_SOURCE = SURFEAU0004) and the RSU is possible
        assertEquals('POLYGON ((265847.762403373 6746613.707730148, 265852.5 6746616.3, 265854.3 6746616.7, ' +
                '265857.3 6746616.6, 265859.5 6746615.3, 265861.6 6746612.2, 265863.1 6746602.4, 265865 6746588.1, ' +
                '265864 6746585.8, 265862.9 6746584.1, 265861.9 6746583.1, 265859.2 6746581.3, 265843.7 6746572.6, ' +
                '265836.4 6746569, 265833.2 6746567.2, 265831.5 6746566.4, 265828.6 6746565.6, 265826.6 6746565.8, ' +
                '265825.1 6746566.7, 265824.1 6746568.3, 265823.7 6746569.3, 265823.3 6746570.8, 265822.8007604562 ' +
                '6746574.333079848, 265824.1 6746573.9, 265827.1 6746573.9, 265827.1 6746576.9, 265826.1 6746579.9, ' +
                '265825.6 6746582.9, 265829.2 6746596.9, 265831.8 6746603.3, 265841.3 6746600.8, 265844.3172413799 ' +
                '6746600.558620689, 265843.2 6746600, 265841.8 6746599.1, 265841.1 6746597.6, 265841 6746596.1, ' +
                '265839.4 6746589.6, 265840.3 6746586.6, 265841.6 6746585.1, 265843.2 6746584.6, 265844.5 6746584.4, ' +
                '265846 6746584.3, 265848.2 6746584.5, 265849.5 6746584.9, 265851.9 6746586.4, 265852.9 6746588.6, ' +
                '265853.1 6746590.9, 265852.6 6746592.9, 265851.7 6746596.4, 265851.3 6746597.8, 265850.9 6746599, ' +
                '265849.9 6746599.6, 265848.7 6746600.2, 265848.6577235773 6746600.211382114, 265848.8 6746600.2, ' +
                '265851.9 6746607.4, 265856 6746609.2, 265853.6 6746612.4, 265848.3 6746613.2, 265847.762403373 6746613.707730148))'.toString(),
                h2GISDatabase.firstRow("SELECT ST_INTERSECTION(THE_GEOM, " +
                "'POLYGON ((265652.709 6746320.319, 265652.5 6746320.3, 265650.2 6746320, 265649.6 6746320, " +
                "265648.7 6746323, 265645.2 6746322.6, 265642.2 6746322.1, 265637.7 6746323.1, 265636.6 6746320.1, " +
                "265633.1 6746320.7, 265618.2 6746323.1, 265617.5 6746331.4, 265624.8 6746333.2, 265625.7 6746328.2, " +
                "265629.2 6746326.7, 265634.2 6746325.6, 265639.2 6746326.6, 265647.2 6746328.5, 265651.2 6746330.5, " +
                "265652.7 6746333, 265651.8 6746337, 265662.3 6746341.4, 265667.3 6746344.4, 265672.3 6746345.3, " +
                "265678.3 6746343.3, 265683.3 6746340.2, 265684.2 6746337.2, 265679.2 6746334.3, 265683.2 6746333.2, " +
                "265687.2 6746334.2, 265692.2 6746337.2, 265688.3 6746343.2, 265687.3 6746346.2, 265688.3 6746349.2, " +
                "265691.4 6746352.2, 265694.9 6746353.6, 265696.4 6746359.1, 265698.4 6746363.1, 265701.5 6746366.1, " +
                "265708 6746368.5, 265711.5 6746376, 265716.6 6746384.9, 265724.7 6746392.9, 265725.2 6746400.3, " +
                "265721.2 6746402.4, 265718.3 6746407.4, 265720.8 6746410.9, 265722.3 6746414.4, 265726.4 6746416.3, " +
                "265728.9 6746422.8, 265732 6746431.8, 265736 6746439.7, 265739.6 6746445.2, 265738.1 6746450.7, " +
                "265739.2 6746454.7, 265744.2 6746457.7, 265749.5 6746459.4, 265750.6 6746449.1, 265755.6 6746449.1, " +
                "265757.5 6746455.5, 265756.2 6746459.6, 265757.7 6746463, 265767.2 6746465.8, 265775.7 6746464.9, " +
                "265777.7 6746468.9, 265783.3 6746472.3, 265787.8 6746474.8, 265789.8 6746478.8, 265794.3 6746481.2, " +
                "265797.8 6746482.7, 265799.9 6746486.7, 265804.4 6746489.1, 265805.9 6746491.6, 265805 6746497.6, " +
                "265811 6746499.6, 265816.4 6746497, 265820.5 6746503, 265823 6746507.5, 265826 6746508.5, " +
                "265827.6 6746519.9, 265830.6 6746522.9, 265835.7 6746529.9, 265837.2 6746533.3, 265842.7 6746534.8, " +
                "265840.3 6746539, 265837.8 6746541.8, 265835.8 6746545.8, 265832.9 6746548.9, 265828.9 6746549.9, " +
                "265824.9 6746548.9, 265817.8 6746546, 265813.9 6746548, 265807.9 6746550.1, 265802.4 6746548.6, " +
                "265800.718 6746549.787, 265803 6746551.4, 265805.9 6746554.6, 265807.6 6746558.7, 265808.9 6746565.1, " +
                "265809.5 6746577, 265809.6 6746578.504, 265809.6 6746578.5, 265821.1 6746574.9, 265824.1 6746573.9, " +
                "265827.1 6746573.9, 265827.1 6746576.9, 265826.1 6746579.9, 265825.6 6746582.9, 265829.2 6746596.9, " +
                "265831.8 6746603.3, 265841.3 6746600.8, 265848.8 6746600.2, 265851.9 6746607.4, 265856 6746609.2, " +
                "265853.6 6746612.4, 265848.3 6746613.2, 265842.9 6746618.3, 265842.9 6746621.155, 265851.4 6746625.2, " +
                "265863.4 6746630.1, 265873.3 6746573.8, 265879.4 6746555.9, 265888.159 6746538.63, 265888.1 6746538.7, " +
                "265886.7 6746540.4, 265883.3 6746542, 265882.3 6746545, 265876.8 6746544.5, 265873.3 6746543, " +
                "265873.5 6746538.6, 265877.2 6746528, 265880.1 6746527, 265878.6 6746520.5, 265875.6 6746521.5, " +
                "265872.6 6746518.6, 265868.6 6746518.2, 265861.1 6746517.2, 265858.5 6746512.7, 265854.5 6746506.7, " +
                "265849.4 6746497.8, 265839.4 6746487.9, 265837.3 6746481.9, 265834.8 6746477.4, 265830.3 6746474.9, " +
                "265822.7 6746468.5, 265819.2 6746470, 265816.2 6746469.1, 265811.2 6746466.1, 265809.2 6746472.1, " +
                "265804.3 6746475.2, 265801.7 6746470.7, 265796.2 6746469.2, 265784.7 6746465.8, 265784.7 6746462.8, " +
                "265787.7 6746461.8, 265787.6 6746454.8, 265784.6 6746453.8, 265783.5 6746442.8, 265780 6746441.4, " +
                "265783.5 6746439.8, 265782 6746435.4, 265777.9 6746429.4, 265774.9 6746430.4, 265765.9 6746421.5, " +
                "265762.4 6746420, 265761.3 6746417, 265753.8 6746415.6, 265749.8 6746411.6, 265746.3 6746410.2, " +
                "265745.2 6746397.2, 265741.7 6746395.7, 265737.2 6746393.3, 265735.1 6746389.3, 265731.6 6746387.8, " +
                "265721.5 6746377.9, 265718 6746376.4, 265718.8 6746371.9, 265717.5 6746366.9, 265718.9 6746362.4, " +
                "265722.9 6746361.4, 265724.4 6746364.9, 265726 6746372.4, 265730 6746371.3, 265731.5 6746367.8, " +
                "265730.4 6746364.8, 265729.8 6746357.8, 265733.4 6746355.8, 265738.3 6746352.8, 265728.2 6746340.9, " +
                "265727.7 6746337.4, 265731.2 6746337.8, 265749.2 6746338.7, 265768.2 6746337.2, 265780.2 6746338.4, " +
                "265809.6 6746334.7, 265816.7 6746339.7, 265823.1 6746337.1, 265827.1 6746335.1, 265833.1 6746333, " +
                "265852.6 6746331.4, 265854 6746325.9, 265854.5 6746322.4, 265833 6746325, 265825.1 6746328.1, " +
                "265821.1 6746326.1, 265818.1 6746327.4, 265813.1 6746326.2, 265806.1 6746328.2, 265800.1 6746326.8, " +
                "265784.1 6746329.7, 265780.1 6746328.5, 265774.1 6746330.5, 265770.1 6746329.2, 265766.1 6746330.6, " +
                "265762.1 6746329.3, 265753.1 6746330.7, 265745.1 6746329.7, 265740.1 6746328.8, 265730.2 6746330.9, " +
                "265723.1 6746328.9, 265719.2 6746330.9, 265715.2 6746329, 265712.7 6746330.5, 265710.2 6746329, " +
                "265707.7 6746330.5, 265682.2 6746327.3, 265675.8 6746328.6, 265676.7 6746332.8, 265671.7 6746332.8, " +
                "265670.7 6746329.8, 265666.2 6746327.9, 265660.2 6746328.9, 265654.2 6746323.5, 265652.709 " +
                "6746320.319))'::geometry) as THE_GEOM FROM HYDRO WHERE ID_SOURCE='SURFEAU0004';")["THE_GEOM"].toString())

        

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
        assertEquals(2325, table.rowCount)
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

        // Check if the VEGET table has the correct number of columns and rows
        tableName = processFormatting.results.outputImpervious
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(7, table.rowCount)
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
