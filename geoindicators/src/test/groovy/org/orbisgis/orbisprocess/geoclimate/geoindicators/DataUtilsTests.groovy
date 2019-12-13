package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class DataUtilsTests {


    @Test
    void joinTest() {
        def h2GIS = H2GIS.open( './target/datautils;AUTO_SERVER=TRUE')

        h2GIS.execute "DROP TABLE IF EXISTS tablea, tableb, tablec; CREATE TABLE tablea (ida integer, name varchar); insert into tablea values(1,'orbisgis');" +
                "CREATE TABLE tableb (idb integer, lab varchar); insert into tableb values(1,'CNRS');" +
                "CREATE TABLE tablec (idc integer, location varchar); insert into tablec values(1,'Vannes');"

        IProcess joinProcess = Geoindicators.DataUtils.joinTables()
        assertTrue joinProcess.execute([inputTableNamesWithId: [tablea:"ida", tableb:"idb", tablec:"idc"]
                                        , outputTableName: "test", datasource: h2GIS])

        def table = h2GIS.getTable(joinProcess.getResults().outputTableName)
        assertEquals"IDA,NAME,LAB,LOCATION", table.columns.join(",")
        assertEquals(1, table.rowCount)

        table.eachRow { row ->
            assertTrue(row.lab.equals('CNRS'))
            assertTrue(row.location.equals('Vannes'))
        }
    }

    @Test
    void joinTest2() {
        def h2GIS = H2GIS.open( './target/datautils;AUTO_SERVER=TRUE')

        h2GIS.execute "DROP TABLE IF EXISTS tablea, tableb, tablec; CREATE TABLE tablea (ida integer, name varchar); insert into tablea values(1,'orbisgis');" +
                "CREATE TABLE tableb (idb integer, lab varchar); insert into tableb values(1,'CNRS');" +
                "CREATE TABLE tablec (idc integer, location varchar); insert into tablec values(1,'Vannes');"

        IProcess joinProcess = Geoindicators.DataUtils.joinTables()
        assertTrue joinProcess.execute([inputTableNamesWithId: [tablea:"ida", tableb:"idb", tablec:"idc"]
                                        , outputTableName: "test", datasource: h2GIS, prefixWithTabName: true])

        def table = h2GIS.getTable(joinProcess.getResults().outputTableName)
        assertEquals"TABLEA_IDA,TABLEA_NAME,TABLEB_LAB,TABLEC_LOCATION", table.columns.join(",")
        assertEquals(1, table.rowCount)

        table.eachRow { row ->
            assertTrue(row.tableb_lab.equals('CNRS'))
            assertTrue(row.tablec_location.equals('Vannes'))
        }
    }

    @Test
    void saveTablesAsFiles() {
        def directory = "./target/savedFiles"
        def h2GIS = H2GIS.open( './target/datautils;AUTO_SERVER=TRUE')

        h2GIS.execute "DROP TABLE IF EXISTS tablea, tablegeom; " +
                "CREATE TABLE tablea (ida integer, name varchar); " +
                "INSERT INTO tablea values(1,'orbisgis'),(2,'vannes');" +
                "CREATE TABLE tablegeom (idb integer, the_geom geometry); " +
                "INSERT INTO tablegeom values(1,'POINT(10 10)'::GEOMETRY);"

        IProcess saveTablesAsFiles = Geoindicators.DataUtils.saveTablesAsFiles()
        assertTrue saveTablesAsFiles.execute([inputTableNames: ["tablea","tablegeom"], directory: directory,
                                              datasource: h2GIS])

        assertEquals 1, h2GIS.load(directory+File.separator+"tablegeom.geojson").rowCount
        assertEquals 2, h2GIS.load(directory+File.separator+"tablea.csv").rowCount
    }
}
