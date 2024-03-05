package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class GridIndicatorsTests {

    @TempDir
    static File folder

    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath() + File.separator + "gridIndicatorsTests")
    }

    @BeforeEach
    void beforeEach() {
    }

    @Test
    void multiscaleLCZGridTest() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid, 104 AS lcz_primary FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);

        --First cell lod_0
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 2 AND id_col = 2;

        --Center cell lod_0
        UPDATE grid SET lcz_primary= 102 WHERE id_row = 5 AND id_col = 5;
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 6 AND id_col = 4; 
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 6 AND id_col = 5;
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 6 AND id_col = 6; 
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 5 AND id_col = 6; 

        --Last cell lod_0
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 8 AND id_col = 7;
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 8 AND id_col = 9; 
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 7 AND id_col = 7;
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 7 AND id_col = 8; 
        UPDATE grid SET lcz_primary= 2 WHERE id_row = 7 AND id_col = 9; 
        """.toString())
        String grid_scale = Geoindicators.GridIndicators.multiscaleLCZGrid(h2GIS, "grid", 2)

        def values = h2GIS.firstRow("SELECT * EXCEPT(THE_GEOM) FROM $grid_scale  WHERE id_row = 2 AND id_col = 2 ".toString())

        def expectedValues = [ID_COL: 2, ID_ROW: 2, ID_GRID: 10, LCZ_PRIMARY: 2, LCZ_PRIMARY_N: 104, LCZ_PRIMARY_NE: 104, LCZ_PRIMARY_E: 104, LCZ_PRIMARY_SE: 104, LCZ_PRIMARY_S: 104, LCZ_PRIMARY_SW: 104, LCZ_PRIMARY_W: 104, LCZ_PRIMARY_NW: 104, ID_ROW_LOD_1: 1, ID_COL_LOD_1: 1, LCZ_WARM_LOD_1: 1, LCZ_COOL_LOD_1: 8, LCZ_PRIMARY_LOD_1: 104, LCZ_PRIMARY_N_LOD_1: 104, LCZ_PRIMARY_NE_LOD_1: 2, LCZ_PRIMARY_E_LOD_1: 104, LCZ_PRIMARY_SE_LOD_1: null, LCZ_PRIMARY_S_LOD_1: null, LCZ_PRIMARY_SW_LOD_1: null, LCZ_PRIMARY_W_LOD_1: null, LCZ_PRIMARY_NW_LOD_1: null, LCZ_WARM_N_LOD_1: null, LCZ_WARN_NE_LOD_1: 4, LCZ_WARN_E_LOD_1: null, LCZ_WARN_SE_LOD_1: null, LCZ_WARN_S_LOD_1: null, LCZ_WARN_SW_LOD_1: null, LCZ_WARN_W_LOD_1: null, LCZ_WARN_NW_LOD_1: null]

        assertTrue(values == expectedValues)

        values = h2GIS.firstRow("SELECT * EXCEPT(THE_GEOM) FROM $grid_scale  WHERE id_row = 5 AND id_col = 5 ".toString())

        expectedValues = [ID_COL: 5, ID_ROW: 5, ID_GRID: 40, LCZ_PRIMARY: 102, LCZ_PRIMARY_N: 2, LCZ_PRIMARY_NE: 2, LCZ_PRIMARY_E: 2, LCZ_PRIMARY_SE: 104, LCZ_PRIMARY_S: 104, LCZ_PRIMARY_SW: 104, LCZ_PRIMARY_W: 104, LCZ_PRIMARY_NW: 2, ID_ROW_LOD_1: 2, ID_COL_LOD_1: 2, LCZ_WARM_LOD_1: 4, LCZ_COOL_LOD_1: 5, LCZ_PRIMARY_LOD_1: 2, LCZ_PRIMARY_N_LOD_1: 104, LCZ_PRIMARY_NE_LOD_1: 2, LCZ_PRIMARY_E_LOD_1: 104, LCZ_PRIMARY_SE_LOD_1: 104, LCZ_PRIMARY_S_LOD_1: 104, LCZ_PRIMARY_SW_LOD_1: 104, LCZ_PRIMARY_W_LOD_1: 104, LCZ_PRIMARY_NW_LOD_1: 104, LCZ_WARM_N_LOD_1: null, LCZ_WARN_NE_LOD_1: 5, LCZ_WARN_E_LOD_1: null, LCZ_WARN_SE_LOD_1: null, LCZ_WARN_S_LOD_1: null, LCZ_WARN_SW_LOD_1: 1, LCZ_WARN_W_LOD_1: null, LCZ_WARN_NW_LOD_1: null]

        assertTrue(values == expectedValues)

        h2GIS.dropTable("grid")
    }

    @Test
    @Disabled
    //Todo a test that shows how to create the a geom layer for each lod
    void multiscaleLCZGridGeomTest() {
        String grid_indicators = h2GIS.load("/home/ebocher/Autres/data/geoclimate/uhi_lcz/Dijon/grid_indicators.geojson", true)
        int nb_levels= 3
        String grid_scale = Geoindicators.GridIndicators.multiscaleLCZGrid(h2GIS, grid_indicators, nb_levels)
        for (int i in 1..nb_levels) {
            def grid_lod = "grid_lod_$i"
            h2GIS.execute("""
            create index on $grid_scale(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
            DROP TABLE IF EXIsTS $grid_lod;
            CREATE TABLE $grid_lod AS 
            SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM, MAX(LCZ_PRIMARY_LOD_${i}) AS LCZ_PRIMARY
            from $grid_scale
            group by ID_ROW_LOD_${i}, ID_COL_LOD_${i};
            """.toString())
            h2GIS.save(grid_lod, "/tmp/grid_lod_${i}.geojson", true)
            h2GIS.dropTable(grid_lod)
        }
    }

    @Test
    void gridDistancesTest1() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid, polygons;
        CREATE TABLE grid AS SELECT  *   FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);
        CREATE TABLE polygons AS SELECT  'POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))'::GEOMETRY AS THE_GEOM ;
        """.toString())

        String grid_distances = Geoindicators.GridIndicators.gridDistances(h2GIS, "polygons","grid", "id")
        assertEquals(4, h2GIS.firstRow("select count(*) as count from $grid_distances where distance =0.5".toString()).count)
    }

    @Test
    void gridDistancesTest2() {
        //Data for test
        h2GIS.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid, polygons;
        CREATE TABLE grid AS SELECT  *   FROM 
        ST_MakeGrid('POLYGON((0 0, 9 0, 9 9, 0 0))'::GEOMETRY, 1, 1);
        CREATE TABLE polygons AS SELECT  'POLYGON ((2 2, 6 2, 6 6, 2 6, 2 2), (3 5, 5 5, 5 3, 3 3, 3 5))'::GEOMETRY AS THE_GEOM ;
        """.toString())
        String grid_distances = Geoindicators.GridIndicators.gridDistances(h2GIS, "polygons","grid", "id")
        assertEquals(12, h2GIS.firstRow("select count(*) as count from $grid_distances where distance =0.5".toString()).count)
    }
}
