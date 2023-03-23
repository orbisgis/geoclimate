package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.io.WKTReader
import org.orbisgis.data.POSTGIS
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.orbisgis.data.H2GIS.open

class SpatialUnitsTests {

    @TempDir
    static File folder

    def static dbProperties = [databaseName: 'orbisgis_db',
                               user        : 'orbisgis',
                               password    : 'orbisgis',
                               url         : 'jdbc:postgresql://localhost:5432/'
    ]
    static POSTGIS postGIS;

    private static def h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "spatialUnitsTests;AUTO_SERVER=TRUE")
        postGIS = POSTGIS.open(dbProperties)
        System.setProperty("test.postgis", Boolean.toString(postGIS != null));
    }

    @BeforeEach
    void beforeEach() {
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void createTSUTest() {
        h2GIS """
                DROP TABLE IF EXISTS roads_tsu;
                CREATE TABLE roads_tsu AS SELECT * FROM road_test WHERE id_road <5
        """.toString()
        def outputTable = Geoindicators.SpatialUnits.createTSU(h2GIS, "roads_tsu", "", "")
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 9 == countRows.numberOfRows
    }

    @Test
    void prepareGeometriesForTSUTest() {
        h2GIS.load(SpatialUnitsTests.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("rail_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("zone_test.geojson"), true)

        def outputTableGeoms = Geoindicators.SpatialUnits.prepareTSUData(h2GIS,
                'zone_test', 'road_test', 'rail_test',
                'veget_test', 'hydro_test', "",
                10000, 2500, "block")

        assertNotNull(outputTableGeoms)

        assert h2GIS."$outputTableGeoms"

        def outputTable = Geoindicators.SpatialUnits.createTSU(h2GIS, outputTableGeoms, "", "tsu")
        assert h2GIS.getSpatialTable(outputTable).save(new File(folder, "tsu.shp").getAbsolutePath(), true)
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 237 == countRows.numberOfRows
    }

    @Test
    void createRsuTest() {
        h2GIS.load(SpatialUnitsTests.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("rail_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("zone_test.geojson"), true)

        def createRSU = Geoindicators.SpatialUnits.createTSU(h2GIS, "zone_test",
                'road_test', 'rail_test',
                'veget_test', 'hydro_test',
                "", 10000, 2500, "block")
        assert createRSU

        assert h2GIS.getSpatialTable(createRSU).save(new File(folder, "rsu.shp").getAbsolutePath(), true)
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $createRSU"
        assert 237 == countRows.numberOfRows
    }


    @Test
    void createBlocksTest() {
        h2GIS """
                DROP TABLE IF EXISTS build_tempo; 
                CREATE TABLE build_tempo AS SELECT * FROM building_test WHERE id_build <27
        """.toString()
        def outputTable = Geoindicators.SpatialUnits.createBlocks(h2GIS, "build_tempo", 0.01, "block")
        assertNotNull(outputTable)
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 12 == countRows.numberOfRows
    }

    @Test
    void spatialJoinTest() {
        h2GIS """
                DROP TABLE IF EXISTS build_tempo; 
                CREATE TABLE build_tempo AS 
                    SELECT id_build, the_geom FROM building_test 
                    WHERE id_build < 9 OR id_build > 28 AND id_build < 30
        """
        String idColumnTarget = "id_rsu"
        def pRsu = Geoindicators.SpatialUnits.spatialJoin(h2GIS,
                "build_tempo", "rsu_test", idColumnTarget,
                false, 1, "test")
        assertNotNull(pRsu)
        h2GIS.eachRow("SELECT * FROM ${pRsu}") {
            row ->
                def expected = h2GIS.firstRow("SELECT ${idColumnTarget} FROM rsu_build_corr WHERE id_build = ${row.id_build}".toString())
                assert row[idColumnTarget] == expected[idColumnTarget]
        }
        idColumnTarget = "id_block"
        def pBlock = Geoindicators.SpatialUnits.spatialJoin(h2GIS, "build_tempo",
                "block_test", "id_block", false, 1, "test")

        h2GIS.eachRow("SELECT * FROM ${pBlock}".toString()) {
            row ->
                def expected = h2GIS.firstRow "SELECT ${idColumnTarget} FROM block_build_corr WHERE id_build = ${row.id_build}".toString()
                assert row[idColumnTarget] == expected[idColumnTarget]
        }
    }

    @Test
    void spatialJoinTest2() {
        h2GIS """
                DROP TABLE IF EXISTS tab1, tab2;
                CREATE TABLE tab1(id1 int, the_geom geometry);
                CREATE TABLE tab2(id2 int, the_geom geometry);
                INSERT INTO tab1 VALUES (1, 'POLYGON((0 0, 10 0, 10 20, 0 20, 0 0))'),
                                        (2, 'POLYGON((10 5, 20 5, 20 20, 10 20, 10 5))'),
                                        (3, 'POLYGON((10 -10, 25 -10, 25 5, 10 5, 10 -10))');
                INSERT INTO tab2 VALUES (1, 'POLYGON((0 0, 20 0, 20 20, 0 20, 0 0))'),
                                        (2, 'POLYGON((20 0, 30 0, 30 20, 20 20, 20 0))');
        """
        def pNbRelationsAll = Geoindicators.SpatialUnits.spatialJoin(h2GIS, "tab1", "tab2",
                "id2", false, null, "test")
        assertEquals 1, h2GIS.firstRow("SELECT count(*) as count FROM ${pNbRelationsAll} WHERE ID1 = 1 AND ID2=1").count
        assertEquals 1, h2GIS.firstRow("SELECT count(*) as count  FROM ${pNbRelationsAll} WHERE ID1 = 2 AND ID2=1").count
        assertEquals 2, h2GIS.firstRow("SELECT count(*) as count  FROM ${pNbRelationsAll} WHERE (ID1 = 3 AND ID2=1) or (ID1 = 3 AND ID2=2)").count


        def pPointOnSurface = Geoindicators.SpatialUnits.spatialJoin(h2GIS, "tab1",
                "tab2", "id2", true, null, "test")
        assert h2GIS.getTable(pPointOnSurface).getRowCount() == 2
    }

    @Test
    void prepareGeometriesForRSUWithFilterTest() {
        h2GIS.load(SpatialUnitsTests.class.class.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("rail_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("zone_test.geojson"), true)

        def outputTableGeoms = Geoindicators.SpatialUnits.prepareTSUData(h2GIS,
                'zone_test', 'road_test', 'rail_test', 'veget_test',
                'hydro_test', "", 10000, 2500, "block")


        assertNotNull(outputTableGeoms)

        assert h2GIS."$outputTableGeoms"
        def outputTable = Geoindicators.SpatialUnits.createTSU(h2GIS, outputTableGeoms, "", "tsu")
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"

        assert 237 == countRows.numberOfRows
    }

    @EnabledIfSystemProperty(named = "test.h2gis", matches = "false")
    @Test
    void regularGridTestH2GIS() {

        def wktReader = new WKTReader()
        def box = wktReader.read('POLYGON((-180 -80, 180 -80, 180 80, -180 80, -180 -80))')
        def gridP = Geoindicators.SpatialUnits.createGrid(h2GIS, box, 1, 1)
        assertNotNull(gridP)
        assert h2GIS."$gridP"
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $gridP".toString()
        assert 57600 == countRows.numberOfRows
    }

    @EnabledIfSystemProperty(named = "test.postgis", matches = "true")
    @Test
    void regularGridTestPOSTGIS() {
        postGIS.execute("DROP TABLE IF EXISTS grid")

        def wktReader = new WKTReader()
        def box = wktReader.read('POLYGON((-5 -5, 5 -5, 5 5, -5 5, -5 -5))')
        def outputTable = Geoindicators.SpatialUnits.createGrid(postGIS, box, 1, 1)
        assert outputTable
        assert postGIS."$outputTable"
        def countRows = postGIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 100 == countRows.numberOfRows
    }
}