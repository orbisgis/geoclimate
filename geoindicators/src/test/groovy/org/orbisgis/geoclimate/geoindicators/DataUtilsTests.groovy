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
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

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
}
