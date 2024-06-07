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
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.*

class SpatialUnitsTests {

    @TempDir
    static File folder

    def static dbProperties = [databaseName: 'orbisgis_db',
                               user        : 'orbisgis',
                               password    : 'orbisgis',
                               url         : 'jdbc:postgresql://localhost:5432/'
    ]
    static POSTGIS postGIS;

    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "spatialUnitsTests;AUTO_SERVER=TRUE")
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
                'veget_test', 'hydro_test', "", "",
                10000, 2500, 10000, "block")

        assertNotNull(outputTableGeoms)

        assert h2GIS.hasTable(outputTableGeoms)

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
                "", "", 10000, 2500, 10000, "block")
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
        h2GIS.load(SpatialUnitsTests.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("rail_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("zone_test.geojson"), true)

        def outputTableGeoms = Geoindicators.SpatialUnits.prepareTSUData(h2GIS,
                'zone_test', 'road_test', 'rail_test', 'veget_test',
                'hydro_test', "", "", 10000, 2500, 10000, "block")


        assertNotNull(outputTableGeoms)

        assert h2GIS.hasTable(outputTableGeoms)
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
        assert postGIS.hasTable(outputTable)
        def countRows = postGIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 100 == countRows.numberOfRows
    }

    @Test
    void sprawlAreasTest1() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid, 104 AS LCZ_PRIMARY FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);
        --Center cell urban
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 5 AND id_col = 5;
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 6 AND id_col = 4; 
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 6 AND id_col = 5;
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 6 AND id_col = 6; 
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 5 AND id_col = 6; 
        """.toString())
        String sprawl_areas = Geoindicators.SpatialUnits.computeSprawlAreas(h2GIS, "grid", 0)
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $sprawl_areas".toString()).count)
        assertEquals(5, h2GIS.firstRow("select st_area(the_geom) as area from $sprawl_areas".toString()).area, 0.0001)
    }

    @Test
    void sprawlAreasTest2() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid, 104 AS LCZ_PRIMARY FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);
        """.toString())
        String sprawl_areas = Geoindicators.SpatialUnits.computeSprawlAreas(h2GIS, "grid", 0)
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $sprawl_areas".toString()).count)
        assertTrue(h2GIS.firstRow("select st_union(st_accum(the_geom)) as the_geom from $sprawl_areas".toString()).the_geom.isEmpty())
    }

    @Test
    void sprawlAreasTest3() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid,104 AS LCZ_PRIMARY FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);        
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 4 AND id_col = 4; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 4 AND id_col = 5; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 4 AND id_col = 6; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 5 AND id_col = 4; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 5 AND id_col = 6; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 6 AND id_col = 4;
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 6 AND id_col = 5;
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 6 AND id_col = 6;
        """.toString())
        String sprawl_areas = Geoindicators.SpatialUnits.computeSprawlAreas(h2GIS, "grid", 0)
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $sprawl_areas".toString()).count)
        assertEquals(9, h2GIS.firstRow("select st_area(the_geom) as area from $sprawl_areas".toString()).area, 0.0001)
    }

    @Test
    void sprawlAreasTest4() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid, 104 AS LCZ_PRIMARY FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);       
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 4 AND id_col = 4; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 4 AND id_col = 5; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 4 AND id_col = 6; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 5 AND id_col = 4; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 5 AND id_col = 6; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 6 AND id_col = 4;
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 6 AND id_col = 5;
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 6 AND id_col = 6;
        
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 9 AND id_col = 9; 
        UPDATE grid SET LCZ_PRIMARY= 1  WHERE id_row = 1 AND id_col = 1; 
        """.toString())
        String sprawl_areas = Geoindicators.SpatialUnits.computeSprawlAreas(h2GIS, "grid", 0)
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $sprawl_areas".toString()).count)
        assertEquals(9, h2GIS.firstRow("select st_area(st_accum(the_geom)) as area from $sprawl_areas".toString()).area, 0.0001)
    }

    @Test
    void inverseGeometriesTest1() {
        //Data for test
        h2GIS.execute("""
        DROP TABLE IF EXISTS polygons;
        CREATE TABLE polygons AS SELECT  'POLYGON ((160 290, 260 290, 260 190, 160 190, 160 290))'::GEOMETRY as the_geom;  
        """.toString())
        String inverse = Geoindicators.SpatialUnits.inversePolygonsLayer(h2GIS, "polygons")
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $inverse".toString()).count)
        assertTrue(h2GIS.firstRow("select st_accum(the_geom) as the_geom from $inverse".toString()).the_geom.isEmpty())
    }

    @Test
    void inverseGeometriesTest2() {
        def wktReader = new WKTReader()
        Geometry expectedGeom = wktReader.read("POLYGON ((160 190, 260 190, 260 290, 320 290, 320 150, 240 150, 240 80, 160 80, 160 190))")
        //Data for test
        h2GIS.execute("""
        DROP TABLE IF EXISTS polygons;
        CREATE TABLE polygons AS SELECT  'MULTIPOLYGON (((160 290, 260 290, 260 190, 160 190, 160 290)), 
        ((240 150, 320 150, 320 80, 240 80, 240 150)))'::GEOMETRY as the_geom;  
        """.toString())
        String inverse = Geoindicators.SpatialUnits.inversePolygonsLayer(h2GIS, "polygons")
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $inverse".toString()).count)
        assertTrue(expectedGeom.equals(h2GIS.firstRow("select the_geom  from $inverse".toString()).the_geom))
    }

    @Test
    void inverseGeometriesTest3() {
        def wktReader = new WKTReader()
        Geometry expectedGeom = wktReader.read("MULTIPOLYGON (((160 190, 260 190, 260 290, 320 290, 320 150, 240 150, 240 80, 160 80, 160 190)), ((230 265, 230 230, 189 230, 189 265, 230 265)))")
        //Data for test
        h2GIS.execute("""
        DROP TABLE IF EXISTS polygons;
        CREATE TABLE polygons AS SELECT  'MULTIPOLYGON (((160 290, 260 290, 260 190, 160 190, 160 290),  
        (189 265, 230 265, 230 230, 189 230, 189 265)), ((240 150, 320 150, 320 80, 240 80, 240 150)))'::GEOMETRY as the_geom;  
        """.toString())
        String inverse = Geoindicators.SpatialUnits.inversePolygonsLayer(h2GIS, "polygons")
        assertEquals(2, h2GIS.firstRow("select count(*) as count from $inverse".toString()).count)
        assertTrue(expectedGeom.equals(h2GIS.firstRow("select st_accum(the_geom) as the_geom  from $inverse".toString()).the_geom))
    }

    @Test
    @Disabled
    void sprawlAreasTestIntegration() {
        //Data for test
        String path = "/home/ebocher/Autres/data/geoclimate/uhi_lcz/Angers/"
        String grid_scales = h2GIS.load("${path}grid_indicators.geojson")
        String sprawl_areas = Geoindicators.SpatialUnits.computeSprawlAreas(h2GIS, grid_scales, 100)
        h2GIS.save(sprawl_areas, "/tmp/sprawl_areas_indic.fgb", true)
        h2GIS.save(grid_scales, "/tmp/grid_indicators.fgb", true)
        String distances = Geoindicators.GridIndicators.gridDistances(h2GIS, sprawl_areas, grid_scales, "id_grid")
        h2GIS.save(distances, "/tmp/distances.fgb", true)

        //Method to compute the cool areas distances
        String cool_areas = Geoindicators.SpatialUnits.extractCoolAreas(h2GIS, grid_scales)
        h2GIS.save(cool_areas, "/tmp/cool_areas.fgb", true)
        String inverse_cool_areas = Geoindicators.SpatialUnits.inversePolygonsLayer(h2GIS, cool_areas)
        h2GIS.save(inverse_cool_areas, "/tmp/inverse_cool_areas.fgb", true)
        distances = Geoindicators.GridIndicators.gridDistances(h2GIS, inverse_cool_areas, grid_scales, "id_grid")
        h2GIS.save(distances, "/tmp/cool_inverse_distances.fgb", true)
    }

    /**
     * A test to debug test creation with some inputs data
     */
    @Disabled
    @Test
    void debugTSUTest() {
        String path = "/tmp/geoclimate"
        String zone = h2GIS.load(path + File.separator + "zone.fgb")
        String road = h2GIS.load(path + File.separator + "road.fgb")
        String rail = h2GIS.load(path + File.separator + "rail.fgb")
        String vegetation = h2GIS.load(path + File.separator + "vegetation.fgb")
        String water = h2GIS.load(path + File.separator + "water.fgb")
        String sea_land_mask = h2GIS.load(path + File.separator + "sea_land_mask.fgb")
        String urban_areas = h2GIS.load(path + File.separator + "urban_areas.fgb")
        double surface_vegetation = 10000
        double surface_hydro = 2500
        double surface_urban_areas = 10000
        double area = 1
        String rsu = Geoindicators.SpatialUnits.createTSU(h2GIS, zone,
                area, road, rail, vegetation,
                water, sea_land_mask, urban_areas,
                surface_vegetation, surface_hydro, surface_urban_areas, "rsu")
        if (rsu) {
            h2GIS.save(rsu, "/tmp/rsu.fgb", true)
        }
    }
}