package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.*

class BDTopoGISLayersTest {
    
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
    void importPreprocessTest() {
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(3219, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('INTEGER', table.columnType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.columnType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.columnType('NB_LEV'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        assertEquals('VARCHAR', table.columnType('MAIN_USE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            // Check that the HEIGHT_WALL is smaller than 1000m high
            assertNotNull(row.HEIGHT_WALL)
            assertNotEquals('', row.HEIGHT_WALL)
            assertTrue(row.HEIGHT_WALL >= 0)
            assertTrue(row.HEIGHT_WALL <= 1000)
            // Check that there is no rows with a HEIGHT_ROOF value (will be updated in the following process)
            assertNull(row.HEIGHT_ROOF)
            // Check that there is no rows with a NB_LEV value (will be updated in the following process)
            assertNull(row.NB_LEV)
            assertNotNull(row.TYPE)
            assertEquals('', row.MAIN_USE)
            assertNotNull(row.ZINDEX)
            assertNotEquals('', row.ZINDEX)
            assertEquals(0, row.ZINDEX)
        }


        // Check if the INPUT_ROAD table has the correct number of columns and rows
        tableName = process.getResults().outputRoadName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(1779, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
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
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            // Check that the WIDTH is smaller than 100m high
            assertNotNull(row.WIDTH)
            assertNotEquals('', row.WIDTH)
            assertTrue(row.WIDTH >= 0)
            assertTrue(row.WIDTH <= 100)
            assertNotNull(row.TYPE)
            assertNotEquals('', row.TYPE)
            assertNotNull(row.SURFACE)
            assertEquals('', row.SURFACE)
            assertNotNull(row.SIDEWALK)
            assertEquals('', row.SURFACE)
            assertNotNull(row.ZINDEX)
            assertNotEquals('', row.ZINDEX)
            assertTrue(row.ZINDEX <= 10)
            assertTrue(row.ZINDEX >= -10)
        }

        // Check if the INPUT_RAIL table has the correct number of columns and rows
        tableName = process.getResults().outputRailName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(5, table.columnCount)
        assertEquals(5, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('VARCHAR', table.columnType('CROSSING'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            assertNotNull(row.TYPE)
            assertNotEquals('', row.TYPE)
            assertNotNull(row.ZINDEX)
            assertNotEquals('', row.ZINDEX)
            assertTrue(row.ZINDEX <= 10)
            assertTrue(row.ZINDEX >= -10)
        }

        // Check if the INPUT_HYDRO table has the correct number of columns and rows
        tableName = process.getResults().outputHydroName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(92, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the INPUT_VEGET table has the correct number of columns and rows
        tableName = process.getResults().outputVegetName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(2325, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        assertEquals('VARCHAR', table.columnType('TYPE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
            assertNotNull(row.TYPE)
            assertNotEquals('', row.TYPE)
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(7, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the ZONE table has the correct number of columns and rows
        tableName = process.getResults().outputZoneName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(1, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.columnType('ID_ZONE'))
        assertTrue(table.THE_GEOM.spatial)
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals(communeToTest, row.ID_ZONE)
        }

    }

    // Check whether the INPUT_BUILDING table is well produced, despite the absence of the BATI_INDIFFERENCIE table
    @Test
    void importPreprocessBuildIndifTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS BATI_INDIFFERENCIE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(274, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.getColumnType('NB_LEV'))
        assertEquals('VARCHAR', table.getColumnType('TYPE'))
        assertEquals('VARCHAR', table.getColumnType('MAIN_USE'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))

        // Check if the BATI_INDIFFERENCIE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("BATI_INDIFFERENCIE")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
        assertEquals('INTEGER', table.getColumnType('HAUTEUR'))
    }

    // Check whether the INPUT_BUILDING table is well produced, despite the absence of the BATI_INDUSTRIEL table
    @Test
    void importPreprocessBuildIndusTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS BATI_INDUSTRIEL;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(2963, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.getColumnType('NB_LEV'))
        assertEquals('VARCHAR', table.getColumnType('TYPE'))
        assertEquals('VARCHAR', table.getColumnType('MAIN_USE'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))

        // Check if the BATI_INDUSTRIEL table has the correct number of columns and rows
        table = h2GISDatabase.getTable("BATI_INDUSTRIEL")
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
        assertEquals('INTEGER', table.getColumnType('HAUTEUR'))
        assertEquals('VARCHAR', table.getColumnType('NATURE'))
    }

    // Check whether the INPUT_BUILDING table is well produced, despite the absence of the BATI_REMARQUABLE table
    @Test
    void importPreprocessBuildRemarqTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS BATI_REMARQUABLE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(3201, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.getColumnType('NB_LEV'))
        assertEquals('VARCHAR', table.getColumnType('TYPE'))
        assertEquals('VARCHAR', table.getColumnType('MAIN_USE'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))

        // Check if the BATI_REMARQUABLE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("BATI_REMARQUABLE")
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
        assertEquals('INTEGER', table.getColumnType('HAUTEUR'))
        assertEquals('VARCHAR', table.getColumnType('NATURE'))
    }

    // Check whether the INPUT_ROAD table is well produced, despite the absence of the ROUTE table
    @Test
    void importPreprocessRoadTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS ROUTE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_ROAD table has the correct number of columns and rows
        def tableName = process.getResults().outputRoadName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(8, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))
        assertEquals('DOUBLE', table.getColumnType('WIDTH'))
        assertEquals('VARCHAR', table.getColumnType('TYPE'))
        assertEquals('VARCHAR', table.getColumnType('SURFACE'))
        assertEquals('VARCHAR', table.getColumnType('SIDEWALK'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))
        assertEquals('VARCHAR', table.getColumnType('CROSSING'))

        // Check if the ROUTE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("ROUTE")
        assertNotNull(table)
        assertEquals(6, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
        assertEquals('DOUBLE', table.getColumnType('LARGEUR'))
        assertEquals('VARCHAR', table.getColumnType('NATURE'))
        assertEquals('INTEGER', table.getColumnType('POS_SOL'))
        assertEquals('VARCHAR', table.getColumnType('FRANCHISST'))
    }

    // Check whether the INPUT_RAIL table is well produced, despite the absence of the TRONCON_VOIE_FERREE table
    @Test
    void importPreprocessRailTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS TRONCON_VOIE_FERREE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_RAIL table has the correct number of columns and rows
        def tableName = process.getResults().outputRailName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(5, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))
        assertEquals('VARCHAR', table.getColumnType('TYPE'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))
        assertEquals('VARCHAR', table.getColumnType('CROSSING'))

        // Check if the TRONCON_VOIE_FERREE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("TRONCON_VOIE_FERREE")
        assertNotNull(table)
        assertEquals(5, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
        assertEquals('VARCHAR', table.getColumnType('NATURE'))
        assertEquals('INTEGER', table.getColumnType('POS_SOL'))
        assertEquals('VARCHAR', table.getColumnType('FRANCHISST'))
    }

    // Check whether the INPUT_HYDRO table is well produced, despite the absence of the SURFACE_EAU table
    @Test
    void importPreprocessHydroTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_EAU;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_HYDRO table has the correct number of columns and rows
        def tableName = process.getResults().outputHydroName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))

        // Check if the SURFACE_EAU table has the correct number of columns and rows
        table = h2GISDatabase.getTable("SURFACE_EAU")
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
    }

    // Check whether the INPUT_VEGET table is well produced, despite the absence of the ZONE_VEGETATION table
    @Test
    void importPreprocessVegetTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS ZONE_VEGETATION;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_VEGET table has the correct number of columns and rows
        def tableName = process.getResults().outputVegetName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID_SOURCE'))
        assertEquals('VARCHAR', table.getColumnType('TYPE'))

        // Check if the ZONE_VEGETATION table has the correct number of columns and rows
        table = h2GISDatabase.getTable("ZONE_VEGETATION")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.getColumnType('ID'))
        assertEquals('VARCHAR', table.getColumnType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the SURFACE_ACTIVITE table
    @Test
    void importPreprocessImperviousSurfActTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_ACTIVITE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(2, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the SURFACE_ACTIVITE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("SURFACE_ACTIVITE")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID'))
        assertEquals('VARCHAR', table.columnType('CATEGORIE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the TERRAIN_SPORT table
    @Test
    void importPreprocessImperviousSportTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS TERRAIN_SPORT;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(7, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the TERRAIN_SPORT table has the correct number of columns and rows
        table = h2GISDatabase.getTable("TERRAIN_SPORT")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID'))
        assertEquals('VARCHAR', table.columnType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the CONSTRUCTION_SURFACIQUE table
    @Test
    void importPreprocessImperviousConstrSurfTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS CONSTRUCTION_SURFACIQUE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(7, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the CONSTRUCTION_SURFACIQUE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("CONSTRUCTION_SURFACIQUE")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('VARCHAR', table.columnType('ID'))
        assertEquals('VARCHAR', table.columnType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the SURFACE_ROUTE table
    @Test
    void importPreprocessImperviousSurfRoadTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_ROUTE;")
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(5, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.isSpatial())
        assertEquals('VARCHAR', table.columnType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the SURFACE_ROUTE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("SURFACE_ROUTE")
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.isSpatial())
        assertEquals('VARCHAR', table.columnType('ID'))
    }


    // Check that the conversion (to valid) of an invalid building (ID = BATIMENT0000000290122667 from BATI_INDIFFERENCIE) is well done
    @Test
    void checkMakeValidBuildingIndif() {
        def process = BDTopo_V2.importPreprocess
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
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
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        //assertEquals('true', h2GISDatabase.firstRow("SELECT ST_IsValid(THE_GEOM) as valid FROM INPUT_BUILDING WHERE ID_SOURCE='BATIMENT0000000290122667';")["valid"].toString())
    }

    @Test
    void initTypes() {
        def process = BDTopo_V2.initTypes
        assertTrue process.execute([datasource       : h2GISDatabase, buildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE',
                                    roadAbstractType: 'ROAD_ABSTRACT_TYPE', roadAbstractCrossing: 'ROAD_ABSTRACT_CROSSING',
                                    railAbstractType: 'RAIL_ABSTRACT_TYPE', railAbstractCrossing: 'RAIL_ABSTRACT_CROSSING',
                                    vegetAbstractType: 'VEGET_ABSTRACT_TYPE'])
        process.getResults().each {
            entry -> assertNotNull h2GISDatabase.getTable(entry.getValue())
        }

        // Check if the BUILDING_BD_TOPO_USE_TYPE table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingBDTopoUseType
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(23, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_NATURE'))
        assertEquals('VARCHAR', table.columnType('NATURE'))
        assertEquals('VARCHAR', table.columnType('TABLE_NAME'))
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_NATURE)
            assertNotEquals('', row.ID_NATURE)
            assertNotNull(row.NATURE)
            assertNotNull(row.TABLE_NAME)
            assertNotEquals('', row.TABLE_NAME)
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
        }

        // Check if the ROAD_BD_TOPO_TYPE table has the correct number of columns and rows
        tableName = process.getResults().outputroadBDTopoType
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(12, table.rowCount)
        assertEquals('INTEGER', table.columnType('ID_NATURE'))
        assertEquals('VARCHAR', table.columnType('NATURE'))
        assertEquals('VARCHAR', table.columnType('TABLE_NAME'))
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        table.eachRow { row ->
            assertNotNull(row.ID_NATURE)
            assertNotEquals('', row.ID_NATURE)
            assertNotNull(row.NATURE)
            assertNotEquals('', row.NATURE)
            assertNotNull(row.TABLE_NAME)
            assertNotEquals('', row.TABLE_NAME)
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
        }

        // Check if the ROAD_BD_TOPO_CROSSING table has the correct number of columns and rows
        tableName = process.getResults().outputroadBDTopoCrossing
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(4, table.rowCount)
        assertEquals('INTEGER', table.columnType('ID_FRANCHISST'))
        assertEquals('VARCHAR', table.columnType('FRANCHISST'))
        assertEquals('VARCHAR', table.columnType('TABLE_NAME'))
        assertEquals('INTEGER', table.columnType('ID_CROSSING'))
        table.eachRow { row ->
            assertNotNull(row.ID_FRANCHISST)
            assertNotEquals('', row.ID_FRANCHISST)
            assertNotNull(row.FRANCHISST)
            assertNotEquals('', row.FRANCHISST)
            assertNotNull(row.TABLE_NAME)
            assertNotEquals('', row.TABLE_NAME)
            assertNotNull(row.ID_CROSSING)
            assertNotEquals('', row.ID_CROSSING)
        }

        // Check if the RAIL_BD_TOPO_TYPE table has the correct number of columns and rows
        tableName = process.getResults().outputrailBDTopoType
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(8, table.rowCount)
        assertEquals('INTEGER', table.columnType('ID_NATURE'))
        assertEquals('VARCHAR', table.columnType('NATURE'))
        assertEquals('VARCHAR', table.columnType('TABLE_NAME'))
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        table.eachRow { row ->
            assertNotNull(row.ID_NATURE)
            assertNotEquals('', row.ID_NATURE)
            assertNotNull(row.NATURE)
            assertNotEquals('', row.NATURE)
            assertNotNull(row.TABLE_NAME)
            assertNotEquals('', row.TABLE_NAME)
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
        }

        // Check if the RAIL_BD_TOPO_CROSSING table has the correct number of columns and rows
        tableName = process.getResults().outputrailBDTopoCrossing
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(3, table.rowCount)
        assertEquals('INTEGER', table.columnType('ID_FRANCHISST'))
        assertEquals('VARCHAR', table.columnType('FRANCHISST'))
        assertEquals('VARCHAR', table.columnType('TABLE_NAME'))
        assertEquals('INTEGER', table.columnType('ID_CROSSING'))
        table.eachRow { row ->
            assertNotNull(row.ID_FRANCHISST)
            assertNotEquals('', row.ID_FRANCHISST)
            assertNotNull(row.FRANCHISST)
            assertNotEquals('', row.FRANCHISST)
            assertNotNull(row.TABLE_NAME)
            assertNotEquals('', row.TABLE_NAME)
            assertNotNull(row.ID_CROSSING)
            assertNotEquals('', row.ID_CROSSING)
        }

        // Check if the VEGET_BD_TOPO_TYPE table has the correct number of columns and rows
        tableName = process.getResults().outputvegetBDTopoType
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(14, table.rowCount)
        assertEquals('INTEGER', table.columnType('ID_NATURE'))
        assertEquals('VARCHAR', table.columnType('NATURE'))
        assertEquals('VARCHAR', table.columnType('TABLE_NAME'))
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        table.eachRow { row ->
            assertNotNull(row.ID_NATURE)
            assertNotEquals('', row.ID_NATURE)
            assertNotNull(row.NATURE)
            assertNotEquals('', row.NATURE)
            assertNotNull(row.TABLE_NAME)
            assertNotEquals('', row.TABLE_NAME)
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
        }
    }

}