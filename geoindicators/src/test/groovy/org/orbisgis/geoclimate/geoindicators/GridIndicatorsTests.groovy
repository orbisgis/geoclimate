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
        String grid_scale = Geoindicators.GridIndicators.multiscaleLCZGrid(h2GIS, "grid", 1)

        def values =  h2GIS.firstRow("SELECT * EXCEPT(THE_GEOM) FROM $grid_scale  WHERE id_row = 2 AND id_col = 2 ".toString())

        def expectedValues=[ID_GRID:10, ID_ROW:2, ID_COL:2, LCZ_PRIMARY:2, LCZ_PRIMARY_N:104, LCZ_PRIMARY_NE:104, LCZ_PRIMARY_E:104, LCZ_PRIMARY_SE:104, LCZ_PRIMARY_S:104, LCZ_PRIMARY_SW:104, LCZ_PRIMARY_W:104, LCZ_PRIMARY_NW:104, ID_ROW_LOD_1:1, ID_COL_LOD_1:0, LCZ_PRIMARY_URBAN_LOD_1:1, LCZ_PRIMARY_COOL_LOD_1:8, LCZ_PRIMARY_LOD_1:104, LCZ_PRIMARY_N_LOD_1:104, LCZ_PRIMARY_NE_LOD_1:null, LCZ_PRIMARY_E_LOD_1:104, LCZ_PRIMARY_SE_LOD_1:null, LCZ_PRIMARY_S_LOD_1:null, LCZ_PRIMARY_SW_LOD_1:null, LCZ_PRIMARY_W_LOD_1:null, LCZ_PRIMARY_NW_LOD_1:null]

        assertTrue(values == expectedValues)

        values =  h2GIS.firstRow("SELECT * EXCEPT(THE_GEOM) FROM $grid_scale  WHERE id_row = 5 AND id_col = 5 ".toString())

        expectedValues=[ID_GRID:40, ID_ROW:5, ID_COL:5, LCZ_PRIMARY:102, LCZ_PRIMARY_N:2, LCZ_PRIMARY_NE:2, LCZ_PRIMARY_E:2, LCZ_PRIMARY_SE:104, LCZ_PRIMARY_S:104, LCZ_PRIMARY_SW:104, LCZ_PRIMARY_W:104, LCZ_PRIMARY_NW:2, ID_ROW_LOD_1:2, ID_COL_LOD_1:1, LCZ_PRIMARY_URBAN_LOD_1:4, LCZ_PRIMARY_COOL_LOD_1:5, LCZ_PRIMARY_LOD_1:104, LCZ_PRIMARY_N_LOD_1:104, LCZ_PRIMARY_NE_LOD_1:104, LCZ_PRIMARY_E_LOD_1:104, LCZ_PRIMARY_SE_LOD_1:104, LCZ_PRIMARY_S_LOD_1:104, LCZ_PRIMARY_SW_LOD_1:104, LCZ_PRIMARY_W_LOD_1:104, LCZ_PRIMARY_NW_LOD_1:104]

        assertTrue(values == expectedValues)

        h2GIS.dropTable("grid")
    }
}
