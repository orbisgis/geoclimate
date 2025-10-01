/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class DataUtilsTests {

    @TempDir
    static File folder
    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "dataUtilsTests;AUTO_SERVER=TRUE")
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
        """.toString()
    }

    @Test
    void joinTest() {
        def p = Geoindicators.DataUtils.joinTables(h2GIS,
                [tablea: "ida", tableb: "idb", tablec: "idc"],
                "test")
        assert "IDA,NAME,LAB,LOCATION" == h2GIS.getColumnNames(p).join(",")
        assert 1 == h2GIS.getRowCount(p)

        h2GIS.getTable(p).eachRow { assert it.get("LAB").equals('CNRS') && it.get("LOCATION").equals('Vannes') }
    }

    @Test
    void joinTest2() {
        def p = Geoindicators.DataUtils.joinTables(h2GIS,
                [tablea: "ida", tableb: "idb", tablec: "idc"],
                "test", true)
        assert p
        def table = h2GIS.getTable(p)
        assert "TABLEA_IDA,TABLEA_NAME,TABLEB_LAB,TABLEC_LOCATION" == table.getColumnNames().join(",")
        assert 1 == table.getRowCount()

        table.eachRow { assert it.tableb_lab.equals('CNRS') && it.tablec_location.equals('Vannes') }
    }

    @Test
    void saveTablesAsFiles() {
        def directory = "./target/savedFiles"
        def p = Geoindicators.DataUtils.saveTablesAsFiles(h2GIS,
                ["tablea", "tablegeom"], true,
                directory)
        assert p

        assert 1 == h2GIS.getRowCount(h2GIS.load(directory + File.separator + "tablegeom.fgb", true))
        assert 1 == h2GIS.getRowCount(h2GIS.load(directory + File.separator + "tablea.csv", true))
    }

    @Test
    void unionTest() {
        def p = Geoindicators.DataUtils.unionTables(h2GIS,
                "tablea","tableb",
                "test")
        assert "IDA,IDB,LAB,NAME" == h2GIS.getColumnNames(p).join(",")
        assert 2 == h2GIS.getRowCount(p)
    }

    @Test
    void withinToHolesTest() {
        h2GIS.execute("""
        DROP TABLE IF EXISTS overlaps;
        CREATE TABLE overlaps as 
        SELECT 1 as id, ST_BUFFER(ST_MAKEPOINT(0, 0), 100) AS THE_GEOM, 'geoclimate' as name
        UNION ALL
        SELECT CAST((row_number() over()) as Integer) + 1 as  id, THE_GEOM, CONCAT('geoclimate', X) as name FROM ST_EXPLODE('(
        SELECT ST_BUFFER(ST_MAKEPOINT(0, 0), 10 * X) AS THE_GEOM, X FROM GENERATE_SERIES(1, 4))')
        """)
        Geoindicators.DataUtils.withinToHoles(h2GIS, "overlaps", "id", "result")
        assertEquals(5, h2GIS.getRowCount("result"))
        h2GIS.dropTable("result")
    }

    @Test
    void withinToHolesTest2() {
        h2GIS.execute("""
        DROP TABLE IF EXISTS overlaps;
        CREATE TABLE overlaps as 
        SELECT 1 as id, ST_BUFFER(ST_MAKEPOINT(0, 0), 100) AS THE_GEOM, 'geoclimate' as name
        UNION ALL
        SELECT CAST((row_number() over()) as Integer) + 1 as  id, THE_GEOM, CONCAT('geoclimate', X) as name FROM ST_EXPLODE('(
        SELECT ST_BUFFER(ST_MAKEPOINT(0, 0), 10 * X) AS THE_GEOM, X FROM GENERATE_SERIES(1, 4))')
        union all
        SELECT 10 as id, ST_BUFFER(ST_MAKEPOINT(60, 70), 5) AS THE_GEOM, 'geoclimate_ring' as name
        """)
        Geoindicators.DataUtils.withinToHoles(h2GIS, "overlaps", "id", "result")
        assertEquals(6, h2GIS.getRowCount("result"))
        h2GIS.dropTable("result")
    }

    @Test
    void removeOverlapsTest() {
        h2GIS.execute("""
        DROP TABLE IF EXISTS overlaps;
        CREATE TABLE overlaps as 
        SELECT 1 as id, 'POLYGON ((0 200, 200 200, 200 0, 0 0, 0 200))'::GEOMETRY AS THE_GEOM
        UNION ALL
        SELECT  2 as  id, 'POLYGON ((-100 100, 200 100, 200 0, -100 0, -100 100))'::GEOMETRY THE_GEOM
        """)
        Geoindicators.DataUtils.removeOverlaps(h2GIS, "overlaps", "id", "result")
        assertEquals(2, h2GIS.getRowCount("result"))
        assertTrue( h2GIS.firstRow("select st_area(the_geom) as area_1 from result where id =1").area_1-h2GIS.firstRow("select st_area(the_geom) as area_2 from result where id =2").area_2<0)
        h2GIS.dropTable("result")
    }

    @Test
    void removeOverlapsTest2() {
        h2GIS.execute("""
        DROP TABLE IF EXISTS overlaps;
        CREATE TABLE overlaps as 
        SELECT 1 as id, 'POLYGON ((0 200, 200 200, 200 0, 0 0, 0 200))'::GEOMETRY AS THE_GEOM
        UNION ALL
        SELECT  2 as  id, 'POLYGON ((-100 100, 200 100, 200 0, -100 0, -100 100))'::GEOMETRY THE_GEOM
        UNION ALL
        SELECT  3 as  id, 'POLYGON ((99 161, 324 161, 324 58, 99 58, 99 161))'::GEOMETRY THE_GEOM
        """)
        Geoindicators.DataUtils.removeOverlaps(h2GIS, "overlaps", "id", "result")
        assertEquals(3, h2GIS.getRowCount("result"))
        assertTrue( h2GIS.firstRow("select st_area(the_geom) as area_1 from result where id =1").area_1-h2GIS.firstRow("select st_area(the_geom) as area_2 from result where id =2").area_2<0)
        h2GIS.dropTable("result")
    }
}
