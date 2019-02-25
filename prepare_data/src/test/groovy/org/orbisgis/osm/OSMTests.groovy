package org.orbisgis.osm

import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class OSMTests {

    /**
     * A basic test just for demo... must be removed
     */
    @Test
    void loadH2GIS() {
        def h2GIS = H2GIS.open([databaseName: './target/loadH2GIS'])
        assertNotNull(h2GIS)
    }

}