package org.orbisgis

import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DataUtilsTests {


    @Test
    void joinTest() {
        def h2GIS = H2GIS.open([databaseName: './target/datautils'])

        h2GIS.execute("DROP TABLE IF EXISTS tablea, tableb, tablec; CREATE TABLE tablea (ida integer, name varchar); insert into tablea values(1,'orbisgis');" +
                "CREATE TABLE tableb (idb integer, lab varchar); insert into tableb values(1,'CNRS');" +
                "CREATE TABLE tablec (idc integer, location varchar); insert into tablec values(1,'Vannes');"
        )

        IProcess joinProcess = Geoclimate.DataUtils.joinTables()
        assertTrue joinProcess.execute([inputTableNamesWithId: [tablea:"ida", tableb:"idb", tablec:"idc"]
                                        , outputTableName: "test", datasource: h2GIS])

        assertTrue("IDA,NAME,LAB,LOCATION".equals(h2GIS.getTable(joinProcess.getResults().outputTableName).columnNames.join(",")))
        assertEquals(1,h2GIS.getTable(joinProcess.getResults().outputTableName.toString().toUpperCase()).rowCount)

        h2GIS.getTable(joinProcess.getResults().outputTableName.toString().toUpperCase()).eachRow { row ->
            assertTrue(row.lab.equals('CNRS'))
            assertTrue(row.location.equals('Vannes'))
        }
    }

    @Test
    void saveTablesAsFiles() {

        def directory = "./target/savedFiles"
        def h2GIS = H2GIS.open([databaseName: './target/datautils'])

        h2GIS.execute("DROP TABLE IF EXISTS tablea, tablegeom; CREATE TABLE tablea (ida integer, name varchar); insert into tablea values(1,'orbisgis'),(2,'vannes');" +
                "CREATE TABLE tablegeom (idb integer, the_geom geometry); insert into tablegeom values(1,'POINT(10 10)'::GEOMETRY);"
        )

        IProcess saveTablesAsFiles = Geoclimate.DataUtils.saveTablesAsFiles()
        saveTablesAsFiles.execute([inputTableNames: ["tablea","tablegeom"], directory: directory, datasource: h2GIS])

        assertTrue h2GIS.load(directory+File.separator+"tablegeom.geojson").rowCount ==1
        assertTrue h2GIS.load(directory+File.separator+"tablea.csv").rowCount ==2
    }
}
