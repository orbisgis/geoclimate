package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.geoclimate.Geoindicators
import static org.junit.jupiter.api.Assertions.assertNotNull

import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open

class DataUtilsTests {

    private static def h2GIS
    private static def randomDbName() {"${DataUtilsTests.simpleName}_${UUID.randomUUID().toString().replaceAll"-", "_"}"}

    @BeforeAll
    static void beforeAll(){
        h2GIS = open"./target/${randomDbName()};AUTO_SERVER=TRUE"
    }

    @BeforeEach
    void beforeEach() {
        h2GIS """
                DROP TABLE IF EXISTS tablea, tableb, tablec, tablegeom; 
                CREATE TABLE tablea (ida INTEGER, name VARCHAR); 
                INSERT INTO tablea VALUES(1,'orbisgis');
                CREATE TABLE tableb (idb INTEGER, lab VARCHAR); 
                INSERT INTO tableb VALUES(1,'CNRS');
                CREATE TABLE tablec (idc INTEGER, location VARCHAR); 
                INSERT INTO tablec VALUES(1,'Vannes');
                CREATE TABLE tablegeom (idb integer, the_geom geometry);
                INSERT INTO tablegeom values(1,'POINT(10 10)'::GEOMETRY);
        """
    }

    @Test
    void joinTest() {
        def result =  Geoindicators.DataUtils.joinTables(
                [tablea:"ida", tableb:"idb", tablec:"idc"], "test", h2GIS)
        assertNotNull result
        def table = h2GIS."${result.outputTableName}"
        assert "IDA,NAME,LAB,LOCATION" == table.columns.join(",")
        assert 1 == table.rowCount
        table.eachRow { assert it.lab.equals('CNRS') && it.location.equals('Vannes') }
    }

    @Test
    void joinTest2() {
        def result =   Geoindicators.DataUtils.joinTables(
               [tablea:"ida", tableb:"idb", tablec:"idc"],"test",
               h2GIS,true)
        assertNotNull result
        def table = h2GIS."${result.outputTableName}"
        assert "TABLEA_IDA,TABLEA_NAME,TABLEB_LAB,TABLEC_LOCATION" == table.columns.join(",")
        assert 1 == table.rowCount
        table.eachRow { assert it.tableb_lab.equals('CNRS') && it.tablec_location.equals('Vannes') }
    }

    @Test
    void saveTablesAsFiles() {
        def directory = "./target/savedFiles"
        assertNotNull Geoindicators.DataUtils.saveTablesAsFiles(["tablea","tablegeom"],true, directory, h2GIS)

        assert 1 == h2GIS.table(h2GIS.load(directory+File.separator+"tablegeom.geojson", true)).rowCount
        assert 1 == h2GIS.table(h2GIS.load(directory+File.separator+"tablea.csv", true)).rowCount
    }
}
