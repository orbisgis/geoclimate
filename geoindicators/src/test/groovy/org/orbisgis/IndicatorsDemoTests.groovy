package org.orbisgis

import org.orbisgis.datamanager.h2gis.H2GIS
import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class IndicatorsDemoTests {

    /**
     * A basic test just for demo... must be removed
     */
    @Test
    void loadH2GIS() {
        def h2GIS = H2GIS.open([databaseName: './target/loadH2GIS'])
        h2GIS.execute("""
                DROP TABLE IF EXISTS spatial_table;
                CREATE TABLE spatial_table (id int, the_geom point);
                INSERT INTO spatial_table VALUES (1, 'POINT(10 10)'::GEOMETRY), (2, 'POINT(1 1)'::GEOMETRY);
        """)
        assertNotNull(h2GIS)

        Geoclimate.IndicatorDemo.demoProcess.execute([inputA : h2GIS.getSpatialTable("spatial_table")])
        assertTrue(Geoclimate.IndicatorDemo.demoProcess.results.outputA.equals(["ID", "THE_GEOM"]))

    }
}
