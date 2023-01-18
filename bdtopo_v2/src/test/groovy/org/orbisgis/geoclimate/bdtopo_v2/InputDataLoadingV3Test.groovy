package org.orbisgis.geoclimate.bdtopo_v2

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS

import static org.junit.jupiter.api.Assertions.*

class InputDataLoadingV3Test {

    @TempDir
    static File folder
    public static communeToTest = "35236"
    public static layers = []

    H2GIS createH2GIS() {
        def debugpath = "/tmp/"
        def dataFolderInseeCode = "sample_v3_$communeToTest"
        def listFilesBDTopo = ["COMMUNE", "BATIMENT", "ZONE_D_ACTIVITE_OU_D_INTERET",
                               "TERRAIN_DE_SPORT", "PISTE_D_AERODROME", "RESERVOIR", "CONSTRUCTION_SURFACIQUE", "EQUIPEMENT_DE_TRANSPORT",
                               "SURFACE_HYDROGRAPHIQUE", "ZONE_DE_VEGETATION", "CIMETIERE", "AERODROME",
                               "TRONCON_DE_VOIE_FERREE",  "TRONCON_DE_ROUTE", "TERRAIN_DE_SPORT"]

        //H2GIS h2GISDatabase = H2GIS.open(folder.getAbsolutePath()+File.separator+"bdtopo_v3_inputDataLoadingTest;AUTO_SERVER=TRUE")
        H2GIS h2GISDatabase = H2GIS.open(debugpath + File.separator + "bdtopo_v3_inputDataLoadingTest;AUTO_SERVER=TRUE")

        // Load data files
        listFilesBDTopo.each {
            def data = getClass().getResource("$dataFolderInseeCode/${it}.shp")
            if (data) {
                layers<<it
                h2GISDatabase.link(data, it, true)
            }
        }
        return h2GISDatabase
    }

    @Test
    void prepareBDTopoDataTest() {
        def h2GISDatabase = createH2GIS()
        def process = BDTopo_V2.InputDataLoadingBDTopo3.prepareBDTopo3Data()
        assertTrue process.execute([datasource: h2GISDatabase,
                                    layers : layers, idZone: communeToTest
        ])
        /*process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }*/

        // Check if the INPUT_BUILDING table has the correct number of columns and rows
        def tableName = process.getResults().building
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(29, table.columnCount)
        assertEquals(6342, table.rowCount)
        // Check the datamodel
        assertEquals(29, table.columns.intersect(["THE_GEOM", "PK", "ID", "NATURE", "USAGE1", "USAGE2", "LEGER", "ETAT", "DATE_CREAT", "DATE_MAJ",
                                                  "DATE_APP", "DATE_CONF", "SOURCE", "ID_SOURCE", "ACQU_PLANI", "PREC_PLANI", "ACQU_ALTI", "PREC_ALTI",
                                                  "NB_LOGTS", "NB_ETAGES", "MAT_MURS", "MAT_TOITS", "HAUTEUR", "Z_MIN_SOL", "Z_MIN_TOIT", "Z_MAX_TOIT",
                                                  "Z_MAX_SOL", "ORIGIN_BAT", "APP_FF"]).size())

        // Check if the INPUT_ROAD table has the correct number of columns and rows
        /*tableName = process.getResults().outputRoadName
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
        assertEquals(16, table.rowCount)
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
        }*/
    }
}