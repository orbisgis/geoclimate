package org.orbisgis.geoclimate.bdtopo_v2

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.*

class InputDataFormattingTest {

    def h2GISDatabase

    public static communeToTest = "abcde"

    @BeforeEach
    void beforeEach(){
        def dataFolderInseeCode = "bd_topo_unit_test"
        def listFilesBDTopo = ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                               "ROUTE", "TRONCON_VOIE_FERREE", "SURFACE_EAU", "ZONE_VEGETATION"
                              ,"TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE","SURFACE_ROUTE", "SURFACE_ACTIVITE"]

        def paramTables = ["BUILDING_ABSTRACT_PARAMETERS", "BUILDING_ABSTRACT_USE_TYPE", "BUILDING_BD_TOPO_USE_TYPE",
                                         "RAIL_ABSTRACT_TYPE", "RAIL_BD_TOPO_TYPE", "RAIL_ABSTRACT_CROSSING",
                                         "RAIL_BD_TOPO_CROSSING", "ROAD_ABSTRACT_PARAMETERS", "ROAD_ABSTRACT_SURFACE",
                                         "ROAD_ABSTRACT_CROSSING", "ROAD_BD_TOPO_CROSSING", "ROAD_ABSTRACT_TYPE",
                                         "ROAD_BD_TOPO_TYPE", "VEGET_ABSTRACT_PARAMETERS", "VEGET_ABSTRACT_TYPE",
                                         "VEGET_BD_TOPO_TYPE"]

        h2GISDatabase = H2GIS.open("./target/h2gis_input_data_formating;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE", "sa", "")

        // Load parameter files
        paramTables.each{
            h2GISDatabase.load(getClass().getResource(it+".csv"), it, true)
        }
        // Load data files
        listFilesBDTopo.each{
            h2GISDatabase.load(getClass().getResource("$dataFolderInseeCode/${it.toLowerCase()}.shp"), it, true)
        }
    }

    @Test
    void inputDataFormatting(){
        def processImport = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue processImport.execute([datasource: h2GISDatabase,
                                          tableCommuneName:'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                          tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                          tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                          tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                          tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                          tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                          distBuffer: 500, distance: 1000, idZone: communeToTest,
                                          building_bd_topo_use_type: 'BUILDING_BD_TOPO_USE_TYPE', building_abstract_use_type: 'BUILDING_ABSTRACT_USE_TYPE',
                                          road_bd_topo_type: 'ROAD_BD_TOPO_TYPE', road_abstract_type: 'ROAD_ABSTRACT_TYPE',
                                          road_bd_topo_crossing: 'ROAD_BD_TOPO_CROSSING', road_abstract_crossing: 'ROAD_ABSTRACT_CROSSING',
                                          rail_bd_topo_type: 'RAIL_BD_TOPO_TYPE', rail_abstract_type: 'RAIL_ABSTRACT_TYPE',
                                          rail_bd_topo_crossing: 'RAIL_BD_TOPO_CROSSING', rail_abstract_crossing: 'RAIL_ABSTRACT_CROSSING',
                                          veget_bd_topo_type: 'VEGET_BD_TOPO_TYPE', veget_abstract_type: 'VEGET_ABSTRACT_TYPE'
        ])
        def resultsImport=processImport.results

        def processFormatting = BDTopo_V2.InputDataFormatting.formatData()
        assertTrue processFormatting.execute([datasource: h2GISDatabase,
                                              inputBuilding: resultsImport.outputBuildingName, inputRoad: resultsImport.outputRoadName,
                                              inputRail: resultsImport.outputRailName, inputHydro: resultsImport.outputHydroName,
                                              inputVeget: resultsImport.outputVegetName, inputImpervious: resultsImport.outputImperviousName,
                                              inputZone: resultsImport.outputZoneName, //inputZoneNeighbors: resultsImport.outputZoneNeighborsName,
                                              hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, idZone: communeToTest, distance: 1000,
                                              buildingAbstractParameters: 'BUILDING_ABSTRACT_PARAMETERS', roadAbstractParameters: 'ROAD_ABSTRACT_PARAMETERS',
                                              vegetAbstractParameters: 'VEGET_ABSTRACT_PARAMETERS'])
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
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('INTEGER', table.columnType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.columnType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.columnType('NB_LEV'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
        assertEquals('CHARACTER VARYING', table.columnType('MAIN_USE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('CHARACTER VARYING', table.columnType('ID_ZONE'))

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

        // Specific cases
        // -------------------------------
        // ... with the building 'BREMAR0001' : HAUTEUR = 0 / TYPE = 'Bâtiment sportif'
        def row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BREMAR0001';")
            assertEquals(3, row_test["HEIGHT_WALL"])
            assertEquals(3, row_test["HEIGHT_ROOF"])
            assertEquals(1, row_test["NB_LEV"])
            assertEquals('sports_centre', row_test["TYPE"])

        //... with the building 'BREMAR0002' : HAUTEUR = 0 / NATURE = 'Tour, donjon, moulin'
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BREMAR0002';")
            assertEquals(3, row_test["HEIGHT_WALL"])
            assertEquals(3, row_test["HEIGHT_ROOF"])
            assertEquals(1, row_test["NB_LEV"])
            assertEquals('historic', row_test["TYPE"])

        //... with the building 'BREMAR0003' : HAUTEUR = 12 / NATURE = 'Chapelle'
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BREMAR0003';")
            assertEquals(12, row_test["HEIGHT_WALL"])
            assertEquals(12, row_test["HEIGHT_ROOF"])
            assertEquals(1, row_test["NB_LEV"])
            assertEquals('chapel', row_test["TYPE"])

        //... with the building 'BREMAR0004' : HAUTEUR = 20 / NATURE = 'Mairie'
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BREMAR0004';")
            assertEquals(20, row_test["HEIGHT_WALL"])
            assertEquals(20, row_test["HEIGHT_ROOF"])
            assertEquals(6, row_test["NB_LEV"])
            assertEquals('townhall', row_test["TYPE"])

        //... with the building 'BINDUS0001' : HAUTEUR = 13 / NATURE = 'Bâtiment commercial'
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BINDUS0001';")
            assertEquals(13, row_test["HEIGHT_WALL"])
            assertEquals(13, row_test["HEIGHT_ROOF"])
            assertEquals(4, row_test["NB_LEV"])
            assertEquals('commercial', row_test["TYPE"])

        //... with the building 'BINDUS0002' : HAUTEUR = 10 / NATURE = 'Bâtiment industriel'
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BINDUS0002';")
            assertEquals(10, row_test["HEIGHT_WALL"])
            assertEquals(10, row_test["HEIGHT_ROOF"])
            assertEquals(1, row_test["NB_LEV"])
            assertEquals('heavy_industry', row_test["TYPE"])

        //... with the building 'BINDIF0001' : HAUTEUR = 0 / Bati indif so no NATURE
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BINDIF0001';")
            assertEquals(3, row_test["HEIGHT_WALL"])
            assertEquals(3, row_test["HEIGHT_ROOF"])
            assertEquals(1, row_test["NB_LEV"])
            assertEquals('building', row_test["TYPE"])

        //... with the building 'BINDIF0002' : HAUTEUR = 17 / Bati indif so no NATURE
        row_test = h2GISDatabase.firstRow("SELECT HEIGHT_WALL, HEIGHT_ROOF, NB_LEV, TYPE FROM BUILDING WHERE ID_SOURCE='BINDIF0002';")
            assertEquals(17, row_test["HEIGHT_WALL"])
            assertEquals(17, row_test["HEIGHT_ROOF"])
            assertEquals(5, row_test["NB_LEV"])
            assertEquals('building', row_test["TYPE"])


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

        // -----------------------------------------------------------------------------------
        // For ROADS
        // -----------------------------------------------------------------------------------

        // Check if the ROAD table has the correct number of columns and rows
        tableName = processFormatting.results.outputRoad
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(11, table.columnCount)
        assertEquals(9, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_ROAD'))
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('DOUBLE PRECISION', table.columnType('WIDTH'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
        assertEquals('CHARACTER VARYING', table.columnType('SURFACE'))
        assertEquals('CHARACTER VARYING', table.columnType('SIDEWALK'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('CHARACTER VARYING', table.columnType('CROSSING'))
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
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0001';")
        assertEquals(1, row_test["WIDTH"])
        assertEquals('path', row_test["TYPE"])

        //... with the road 'ROUTE0002' : LARGEUR = 5,5 / NATURE = 'Route à 1 chaussée'
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0002';")
        assertEquals(5.5, row_test["WIDTH"])
        assertEquals('unclassified', row_test["TYPE"])

        //... with the road 'ROUTE0003' : LARGEUR = 0 / NATURE = 'Route empierrée'
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0003';")
        assertEquals(2, row_test["WIDTH"])
        assertEquals('track', row_test["TYPE"])

        //... with the road 'ROUTE0004' : LARGEUR = 10 / NATURE = 'Quasi-autoroute'
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0004';")
        assertEquals(10, row_test["WIDTH"])
        assertEquals('trunk', row_test["TYPE"])

        //... with the road 'ROUTE0005' : LARGEUR = 4 / NATURE = 'Bretelle'
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0005';")
        assertEquals(4, row_test["WIDTH"])
        assertEquals('highway_link', row_test["TYPE"])

        //... with the road 'ROUTE0006' : LARGEUR = 0 / NATURE = 'Route empierrée'
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0006';")
        assertEquals(2, row_test["WIDTH"])
        assertEquals('track', row_test["TYPE"])

        //... with the road 'ROUTE0007' : LARGEUR = 0 / NATURE = 'Piste cyclable'
        row_test = h2GISDatabase.firstRow("SELECT WIDTH, TYPE FROM ROAD WHERE ID_SOURCE='ROUTE0007';")
        assertEquals(1, row_test["WIDTH"])
        assertEquals('cycleway', row_test["TYPE"])


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
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('CHARACTER VARYING', table.columnType('CROSSING'))
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


        // -----------------------------------------------------------------------------------
        // For HYDROGRAPHIC AREAS
        // -----------------------------------------------------------------------------------

        // Check if the HYDRO table has the correct number of columns and rows
        tableName = processFormatting.results.outputHydro
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(5, table.columnCount)
        assertEquals(3, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_HYDRO'))
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_HYDRO)
            assertNotEquals('', row.ID_HYDRO)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            assertEquals(0, row.ZINDEX as int)
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


        // -----------------------------------------------------------------------------------
        // For VEGETATION AREAS
        // -----------------------------------------------------------------------------------

        // Check if the VEGET table has the correct number of columns and rows
        tableName = processFormatting.results.outputVeget
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(6, table.columnCount)
        assertEquals(7, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.spatial)
        assertEquals('INTEGER', table.columnType('ID_VEGET'))
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
        assertEquals('CHARACTER VARYING', table.columnType('HEIGHT_CLASS'))
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
            assertEquals(0, row.ZINDEX as int)
        }
        
        // Specific cases
        // -------------------------------
        //... with the vegetation area 'VEGET0001' : NATURE = 'Forêt fermée de feuillus'
        row_test = h2GISDatabase.firstRow("SELECT TYPE, HEIGHT_CLASS FROM VEGET WHERE ID_SOURCE='VEGET0001';")
        assertEquals('forest', row_test["TYPE"])
        assertEquals('high', row_test["HEIGHT_CLASS"])

        //... with the vegetation area 'VEGET0002' : NATURE = 'Forêt fermée mixte'
        row_test = h2GISDatabase.firstRow("SELECT TYPE, HEIGHT_CLASS FROM VEGET WHERE ID_SOURCE='VEGET0002';")
        assertEquals('forest', row_test["TYPE"])
        assertEquals('high', row_test["HEIGHT_CLASS"])

        //... with the vegetation area 'VEGET0003' : NATURE = 'Bois'
        row_test = h2GISDatabase.firstRow("SELECT TYPE, HEIGHT_CLASS FROM VEGET WHERE ID_SOURCE='VEGET0003';")
        assertEquals('forest', row_test["TYPE"])
        assertEquals('high', row_test["HEIGHT_CLASS"])

        //... with the vegetation area 'VEGET0004' : NATURE = 'Haie'
        row_test = h2GISDatabase.firstRow("SELECT TYPE, HEIGHT_CLASS FROM VEGET WHERE ID_SOURCE='VEGET0004';")
        assertEquals('hedge', row_test["TYPE"])
        assertEquals('high', row_test["HEIGHT_CLASS"])


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
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals('CHARACTER VARYING', table.columnType('ID_ZONE'))
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
