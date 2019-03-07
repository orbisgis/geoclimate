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

        IndicatorDemo  indicatorDemo =  new IndicatorDemo()

        indicatorDemo.demoProcess.execute([inputA : h2GIS.getSpatialTable("spatial_table")])
        assertTrue(indicatorDemo.demoProcess.results.outputA.equals(["ID", "THE_GEOM"]))
    }

    @Test
    void calculateBuildingArea() {
        def h2GIS = H2GIS.open([databaseName: './target/loadH2GIS'])
        String sqlFilePath = System.getProperty("user.dir")+"/target/TestDatasetCreation.sql"
        String sqlString = new File(sqlFilePath).text
        h2GIS.execute(sqlString)

        h2GIS.execute("DROP TABLE IF EXISTS tempo_build; CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build = 1 OR id_build = 2 OR id_build = 3")

        IndicatorDemo  indicatorDemo =  new IndicatorDemo()

        indicatorDemo.buildingArea.execute([inputA: "tempo_build", inputB: "building_area", inputC: "building_area", inputD: "the_geom", inputE: h2GIS])

        def concat = ""
        h2GIS.eachRow("SELECT * FROM BUILDING_AREA")
                {row -> concat += "$row.BUILDING_AREA\n"}
        assertEquals("156.0\n40.0\n100.0\n", concat)
    }
}
