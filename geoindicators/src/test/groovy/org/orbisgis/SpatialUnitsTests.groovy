package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class SpatialUnitsTests {

    @Test
    void testCreateRSU() {
        def h2GIS = H2GIS.open([databaseName: './target/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        def  rsu =  Geoclimate.SpatialUnits.createRSU()
        rsu.execute([inputTableName: "road_test",
                      outputTableName: "rsu", datasource: h2GIS])

        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from rsu")
        assert 10 == countRows.numberOfRows

    }


}