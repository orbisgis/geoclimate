package org.orbisgis.orbisprocess.geoclimate.preparedata.bdtopo

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.*

class BDTopoGISLayersTest {
    def h2GISDatabase

    @BeforeAll
    static void beforeAll(){
        if(BDTopoGISLayersTest.class.getResource("bdtopofolder") != null &&
                new File(BDTopoGISLayersTest.class.getResource("bdtopofolder").toURI()).exists()) {
            System.properties.setProperty("data.bd.topo", "true")
        }
        else {
            System.properties.setProperty("data.bd.topo", "false")
        }
    }

    @BeforeEach
    void beforeEach() {
        if(System.properties.containsKey("data.bd.topo") && System.properties.getProperty("data.bd.topo") == "true") {
            h2GISDatabase = H2GIS.open("./target/h2gis_bd_topo_gis_layer_${UUID.randomUUID()};AUTO_SERVER=TRUE", "sa", "")
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/IRIS_GE.shp"), "IRIS_GE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/BATI_INDIFFERENCIE.shp"), "BATI_INDIFFERENCIE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/BATI_INDUSTRIEL.shp"), "BATI_INDUSTRIEL", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/BATI_REMARQUABLE.shp"), "BATI_REMARQUABLE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/ROUTE.shp"), "ROUTE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/SURFACE_EAU.shp"), "SURFACE_EAU", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/ZONE_VEGETATION.shp"), "ZONE_VEGETATION", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/TRONCON_VOIE_FERREE.shp"), "TRONCON_VOIE_FERREE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/TERRAIN_SPORT.shp"), "TERRAIN_SPORT", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/CONSTRUCTION_SURFACIQUE.shp"), "CONSTRUCTION_SURFACIQUE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/SURFACE_ROUTE.shp"), "SURFACE_ROUTE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/SURFACE_ACTIVITE.shp"), "SURFACE_ACTIVITE", true)

            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("BUILDING_ABSTRACT_PARAMETERS.csv"), "BUILDING_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("BUILDING_ABSTRACT_USE_TYPE.csv"), "BUILDING_ABSTRACT_USE_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("BUILDING_BD_TOPO_USE_TYPE.csv"), "BUILDING_BD_TOPO_USE_TYPE", true)

            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_ABSTRACT_TYPE.csv"), "RAIL_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_BD_TOPO_TYPE.csv"), "RAIL_BD_TOPO_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_ABSTRACT_CROSSING.csv"), "RAIL_ABSTRACT_CROSSING", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_BD_TOPO_CROSSING.csv"), "RAIL_BD_TOPO_CROSSING", true)

            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_PARAMETERS.csv"), "ROAD_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_SURFACE.csv"), "ROAD_ABSTRACT_SURFACE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_CROSSING.csv"), "ROAD_ABSTRACT_CROSSING", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_BD_TOPO_CROSSING.csv"), "ROAD_BD_TOPO_CROSSING", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_TYPE.csv"), "ROAD_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_BD_TOPO_TYPE.csv"), "ROAD_BD_TOPO_TYPE", true)

            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("VEGET_ABSTRACT_PARAMETERS.csv"), "VEGET_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("VEGET_ABSTRACT_TYPE.csv"), "VEGET_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("VEGET_BD_TOPO_TYPE.csv"), "VEGET_BD_TOPO_TYPE", true)
        }
    }

    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void importPreprocessTest() {
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
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
        assertEquals(20568, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        assertEquals('INTEGER', table.getColumnsType('HEIGHT_WALL'))
        assertEquals('INTEGER', table.getColumnsType('HEIGHT_ROOF'))
        assertEquals('INTEGER', table.getColumnsType('NB_LEV'))
        assertEquals('VARCHAR', table.getColumnsType('TYPE'))
        assertEquals('VARCHAR', table.getColumnsType('MAIN_USE'))
        assertEquals('INTEGER', table.getColumnsType('ZINDEX'))
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
        assertEquals(9762, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        assertEquals('DOUBLE', table.getColumnsType('WIDTH'))
        assertEquals('VARCHAR', table.getColumnsType('TYPE'))
        assertEquals('VARCHAR', table.getColumnsType('SURFACE'))
        assertEquals('VARCHAR', table.getColumnsType('SIDEWALK'))
        assertEquals('INTEGER', table.getColumnsType('ZINDEX'))
        assertEquals('VARCHAR', table.getColumnsType('CROSSING'))
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
        assertEquals(20, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        assertEquals('VARCHAR', table.getColumnsType('TYPE'))
        assertEquals('INTEGER', table.getColumnsType('ZINDEX'))
        assertEquals('VARCHAR', table.getColumnsType('CROSSING'))
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
        assertEquals(385, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
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
        assertEquals(7756, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        assertEquals('VARCHAR', table.getColumnsType('TYPE'))
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
        assertEquals(71, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
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
        assertEquals('VARCHAR', table.getColumnsType('ID_ZONE'))
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
            assertEquals('56260', row.ID_ZONE)
        }

        // Check if the ZONE_NEIGHBORS table has the correct number of columns and rows
        tableName = process.getResults().outputZoneNeighborsName
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(2, table.columnCount)
        assertEquals(11, table.rowCount)
        // Check if the column types are correct
        assertEquals('VARCHAR', table.getColumnsType('ID_ZONE'))
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_ZONE)
            assertNotEquals('', row.ID_ZONE)
        }

    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the SURFACE_ACTIVITE table
    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void importPreprocessImperviousSurfActTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_ACTIVITE;")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
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
        assertEquals(41, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the SURFACE_ACTIVITE table has the correct number of columns and rows
        tableName = h2GISDatabase.getTable("SURFACE_ACTIVITE")
        assertNotNull(tableName)
        assertEquals(3, tableName.columnCount)
        assertEquals(0, tableName.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', tableName.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', tableName.getColumnsType('ID'))
        assertEquals('VARCHAR', tableName.getColumnsType('CATEGORIE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the TERRAIN_SPORT table
    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void importPreprocessImperviousSportTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS TERRAIN_SPORT;")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
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
        assertEquals(69, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the TERRAIN_SPORT table has the correct number of columns and rows
        tableName = h2GISDatabase.getTable("TERRAIN_SPORT")
        assertNotNull(tableName)
        assertEquals(3, tableName.columnCount)
        assertEquals(0, tableName.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', tableName.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', tableName.getColumnsType('ID'))
        assertEquals('VARCHAR', tableName.getColumnsType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the CONSTRUCTION_SURFACIQUE table
    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void importPreprocessImperviousConstrSurfTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS CONSTRUCTION_SURFACIQUE;")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
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
        assertEquals(71, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the CONSTRUCTION_SURFACIQUE table has the correct number of columns and rows
        tableName = h2GISDatabase.getTable("CONSTRUCTION_SURFACIQUE")
        assertNotNull(tableName)
        assertEquals(3, tableName.columnCount)
        assertEquals(0, tableName.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', tableName.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', tableName.getColumnsType('ID'))
        assertEquals('VARCHAR', tableName.getColumnsType('NATURE'))
    }

    // Check whether the INPUT_IMPERVIOUS table is well produced, despite the absence of the SURFACE_ROUTE table
    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void importPreprocessImperviousSurfRoadTest() {
        h2GISDatabase.execute ("DROP TABLE IF EXISTS SURFACE_ROUTE;")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    tableImperviousSportName: 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                    tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                    distBuffer: 500, expand: 1000, idZone: '56260',
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
        assertEquals(32, table.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', table.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', table.getColumnsType('ID_SOURCE'))
        // For each rows, check if the fields contains the expected values
        table.eachRow { row ->
            assertNotNull(row.THE_GEOM)
            assertNotEquals('', row.THE_GEOM)
            assertNotNull(row.ID_SOURCE)
            assertNotEquals('', row.ID_SOURCE)
        }

        // Check if the SURFACE_ROUTE table has the correct number of columns and rows
        tableName = h2GISDatabase.getTable("SURFACE_ROUTE")
        assertNotNull(tableName)
        assertEquals(2, tableName.columnCount)
        assertEquals(0, tableName.rowCount)
        // Check if the column types are correct
        assertEquals('GEOMETRY', tableName.getColumnsType('THE_GEOM'))
        assertEquals('VARCHAR', tableName.getColumnsType('ID'))
    }




    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void initTypes() {
        def process = PrepareData.BDTopoGISLayers.initTypes()
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
        assertEquals('INTEGER', table.getColumnsType('ID_NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('TABLE_NAME'))
        assertEquals('INTEGER', table.getColumnsType('ID_TYPE'))
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
        assertEquals('INTEGER', table.getColumnsType('ID_NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('TABLE_NAME'))
        assertEquals('INTEGER', table.getColumnsType('ID_TYPE'))
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
        assertEquals('INTEGER', table.getColumnsType('ID_FRANCHISST'))
        assertEquals('VARCHAR', table.getColumnsType('FRANCHISST'))
        assertEquals('VARCHAR', table.getColumnsType('TABLE_NAME'))
        assertEquals('INTEGER', table.getColumnsType('ID_CROSSING'))
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
        assertEquals('INTEGER', table.getColumnsType('ID_NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('TABLE_NAME'))
        assertEquals('INTEGER', table.getColumnsType('ID_TYPE'))
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
        assertEquals('INTEGER', table.getColumnsType('ID_FRANCHISST'))
        assertEquals('VARCHAR', table.getColumnsType('FRANCHISST'))
        assertEquals('VARCHAR', table.getColumnsType('TABLE_NAME'))
        assertEquals('INTEGER', table.getColumnsType('ID_CROSSING'))
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
        assertEquals('INTEGER', table.getColumnsType('ID_NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('NATURE'))
        assertEquals('VARCHAR', table.getColumnsType('TABLE_NAME'))
        assertEquals('INTEGER', table.getColumnsType('ID_TYPE'))
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