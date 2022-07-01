package org.orbisgis.geoclimate.bdtopo_v2

import org.junit.jupiter.api.Test
import org.orbisgis.data.H2GIS

import static org.junit.jupiter.api.Assertions.*

class InputDataLoadingTest {


    public static communeToTest = "12174"


    H2GIS createH2GIS(String dbPath){
        def dataFolderInseeCode = "sample_$communeToTest"
        def listFilesBDTopo = ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                               "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "ZONE_VEGETATION",
                               "TRONCON_VOIE_FERREE", "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
                               "SURFACE_ROUTE", "SURFACE_ACTIVITE"]

        def paramTables = ["BUILDING_ABSTRACT_PARAMETERS", "BUILDING_ABSTRACT_USE_TYPE", "BUILDING_BD_TOPO_USE_TYPE",
                           "RAIL_ABSTRACT_TYPE", "RAIL_BD_TOPO_TYPE", "RAIL_ABSTRACT_CROSSING",
                           "RAIL_BD_TOPO_CROSSING", "ROAD_ABSTRACT_PARAMETERS", "ROAD_ABSTRACT_SURFACE",
                           "ROAD_ABSTRACT_CROSSING", "ROAD_BD_TOPO_CROSSING", "ROAD_ABSTRACT_TYPE",
                           "ROAD_BD_TOPO_TYPE", "VEGET_ABSTRACT_PARAMETERS", "VEGET_ABSTRACT_TYPE",
                           "VEGET_BD_TOPO_TYPE"]

        H2GIS h2GISDatabase = H2GIS.open("./target/${dbPath};AUTO_SERVER=TRUE", "sa", "")

        // Load parameter files
        paramTables.each{
            h2GISDatabase.load(getClass().getResource(it+".csv"), it, true)
        }
        // Load data files
        listFilesBDTopo.each{
            h2GISDatabase.load(getClass().getResource("$dataFolderInseeCode/${it}.shp"), it, true)
        }
        return h2GISDatabase
    }

    @Test
    void prepareBDTopoDataTest() {
        def h2GISDatabase =  createH2GIS("prepareBDTopoDataTest")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000, idZone: communeToTest
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(11202, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('INTEGER', table.columnType('HEIGHT_WALL'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
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
        }


        // Check if the INPUT_ROAD table has the correct number of columns and rows
        tableName = process.getResults().outputRoadName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(9, table.columnCount)
        assertEquals(4973, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
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
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
        assertEquals('INTEGER', table.columnType('ZINDEX'))
        assertEquals('CHARACTER VARYING', table.columnType('CROSSING'))
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
        assertEquals(3, table.columnCount)
        assertEquals(92, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals(4, table.columnCount)
        assertEquals(2325, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
        assertEquals('CHARACTER VARYING', table.columnType('TYPE'))
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
        assertEquals(4, table.columnCount)
        assertEquals(13, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals('CHARACTER VARYING', table.columnType('ID_ZONE'))
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
        def h2GISDatabase =  createH2GIS("importPreprocessBuildIndifTest")
        h2GISDatabase.execute ("""DROP TABLE IF EXISTS BATI_INDIFFERENCIE""")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000, idZone: communeToTest
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(721, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_WALL'))
        assertEquals('CHARACTER VARYING', table.getColumnType('TYPE'))

        // Check if the BATI_INDIFFERENCIE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("BATI_INDIFFERENCIE")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
        assertEquals('INTEGER', table.getColumnType('HAUTEUR'))
    }

    // Check whether the INPUT_BUILDING table is well produced, despite the absence of the BATI_INDUSTRIEL table
    @Test
    void importPreprocessBuildIndusTest() {
        def h2GISDatabase =  createH2GIS("importPreprocessBuildIndusTest")
        h2GISDatabase.execute ("""DROP TABLE IF EXISTS BATI_INDUSTRIEL""")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE',tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000, idZone: communeToTest
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(10547, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_WALL'))
        assertEquals('CHARACTER VARYING', table.getColumnType('TYPE'))

        // Check if the BATI_INDUSTRIEL table has the correct number of columns and rows
        table = h2GISDatabase.getTable("BATI_INDUSTRIEL")
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
        assertEquals('INTEGER', table.getColumnType('HAUTEUR'))
        assertEquals('CHARACTER VARYING', table.getColumnType('NATURE'))
    }

    // Check whether the INPUT_BUILDING table is well produced, despite the absence of the BATI_REMARQUABLE table
    @Test
    void importPreprocessBuildRemarqTest() {
        def h2GISDatabase = createH2GIS("importPreprocessBuildRemarqTest")
        h2GISDatabase.execute ("""DROP TABLE IF EXISTS BATI_REMARQUABLE """)
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 0
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(6697, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnType('HEIGHT_WALL'))
        assertEquals('CHARACTER VARYING', table.getColumnType('TYPE'))

        // Check if the BATI_REMARQUABLE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("BATI_REMARQUABLE")
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
        assertEquals('INTEGER', table.getColumnType('HAUTEUR'))
        assertEquals('CHARACTER VARYING', table.getColumnType('NATURE'))
    }

    // Check whether the INPUT_ROAD table is well produced, despite the absence of the ROUTE table
    @Test
    void importPreprocessRoadTest() {
        def h2GISDatabase = createH2GIS("importPreprocessRoadTest")
        h2GISDatabase.execute ("""DROP TABLE IF EXISTS ROUTE""")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_ROAD table has the correct number of columns and rows
        def tableName = process.getResults().outputRoadName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(9, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))
        assertEquals('DOUBLE PRECISION', table.getColumnType('WIDTH'))
        assertEquals('CHARACTER VARYING', table.getColumnType('TYPE'))
        assertEquals('CHARACTER VARYING', table.getColumnType('SURFACE'))
        assertEquals('CHARACTER VARYING', table.getColumnType('SIDEWALK'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))
        assertEquals('CHARACTER VARYING', table.getColumnType('CROSSING'))

        // Check if the ROUTE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("ROUTE")
        assertNotNull(table)
        assertEquals(7, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
        assertEquals('DOUBLE PRECISION', table.getColumnType('LARGEUR'))
        assertEquals('CHARACTER VARYING', table.getColumnType('NATURE'))
        assertEquals('INTEGER', table.getColumnType('POS_SOL'))
        assertEquals('CHARACTER VARYING', table.getColumnType('FRANCHISST'))
    }

    // Check whether the INPUT_RAIL table is well produced, despite the absence of the TRONCON_VOIE_FERREE table
    @Test
    void importPreprocessRailTest() {
        def h2GISDatabase = createH2GIS("importPreprocessRailTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS TRONCON_VOIE_FERREE;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
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
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))
        assertEquals('CHARACTER VARYING', table.getColumnType('TYPE'))
        assertEquals('INTEGER', table.getColumnType('ZINDEX'))
        assertEquals('CHARACTER VARYING', table.getColumnType('CROSSING'))

        // Check if the TRONCON_VOIE_FERREE table has the correct number of columns and rows
        table = h2GISDatabase.getTable("TRONCON_VOIE_FERREE")
        assertNotNull(table)
        assertEquals(5, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
        assertEquals('CHARACTER VARYING', table.getColumnType('NATURE'))
        assertEquals('INTEGER', table.getColumnType('POS_SOL'))
        assertEquals('CHARACTER VARYING', table.getColumnType('FRANCHISST'))
    }

    // Check whether the INPUT_HYDRO table is well produced, despite the absence of the SURFACE_EAU table
    @Test
    void importPreprocessHydroTest() {
        def h2GISDatabase = createH2GIS("importPreprocessHydroTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_EAU;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_HYDRO table has the correct number of columns and rows
        def tableName = process.getResults().outputHydroName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))

        // Check if the SURFACE_EAU table has the correct number of columns and rows
        table = h2GISDatabase.getTable("SURFACE_EAU")
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
    }

    // Check whether the INPUT_VEGET table is well produced, despite the absence of the ZONE_VEGETATION table
    @Test
    void importPreprocessVegetTest() {
        def h2GISDatabase = createH2GIS("importPreprocessVegetTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS ZONE_VEGETATION;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_VEGET table has the correct number of columns and rows
        def tableName = process.getResults().outputVegetName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID_SOURCE'))
        assertEquals('CHARACTER VARYING', table.getColumnType('TYPE'))

        // Check if the ZONE_VEGETATION table has the correct number of columns and rows
        table = h2GISDatabase.getTable("ZONE_VEGETATION")
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.getColumnType('ID'))
        assertEquals('CHARACTER VARYING', table.getColumnType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the SURFACE_ACTIVITE table
    @Test
    void importPreprocessImperviousSurfActTest() {
        def h2GISDatabase =  createH2GIS("importPreprocessImperviousSurfActTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_ACTIVITE;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(2, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals('CHARACTER VARYING', table.columnType('ID'))
        assertEquals('CHARACTER VARYING', table.columnType('CATEGORIE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the TERRAIN_SPORT table
    @Test
    void importPreprocessImperviousSportTest() {
        def h2GISDatabase =  createH2GIS("importPreprocessImperviousSportTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS TERRAIN_SPORT;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(13, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals('CHARACTER VARYING', table.columnType('ID'))
        assertEquals('CHARACTER VARYING', table.columnType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the CONSTRUCTION_SURFACIQUE table
    @Test
    void importPreprocessImperviousConstrSurfTest() {
        def h2GISDatabase = createH2GIS("importPreprocessImperviousConstrSurfTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS CONSTRUCTION_SURFACIQUE;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(13, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.THE_GEOM.spatial)
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals('CHARACTER VARYING', table.columnType('ID'))
        assertEquals('CHARACTER VARYING', table.columnType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the SURFACE_ROUTE table
    @Test
    void importPreprocessImperviousSurfRoadTest() {
        def h2GISDatabase =  createH2GIS("importPreprocessImperviousSurfRoadTest")
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_ROUTE;")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 1000
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        // Check if the INPUT_IMPERVIOUS table has the correct number of columns and rows
        def tableName = process.getResults().outputImperviousName
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(11, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.isSpatial())
        assertEquals('CHARACTER VARYING', table.columnType('ID_SOURCE'))
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
        assertEquals(3, table.columnCount)
        assertEquals(0, table.rowCount)
        // Check if the column types are correct
        assertTrue(table.the_geom.isSpatial())
        assertEquals('CHARACTER VARYING', table.columnType('ID'))
    }


    // Check that the conversion (to valid) of an invalid building (ID = BATIMENT0000000290122667 from BATI_INDIFFERENCIE) is well done
    @Test
    void checkMakeValidBuildingIndif() {
        def h2GISDatabase =  createH2GIS("checkMakeValidBuildingIndif")
        def process = BDTopo_V2.InputDataLoading.prepareBDTopoData()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableCommuneName: 'COMMUNE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distance: 0
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }

        //assertEquals('true', h2GISDatabase.firstRow("SELECT ST_IsValid(THE_GEOM) as valid FROM INPUT_BUILDING WHERE ID_SOURCE='BATIMENT0000000290122667';")["valid"].toString())
    }

}