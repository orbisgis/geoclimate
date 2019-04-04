package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class SpatialUnitsTests {

    @Test
    void testCreateRSU() {
        def h2GIS = H2GIS.open([databaseName: './target/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        h2GIS.execute("drop table if exists roads_rsu; " +
                "create table roads_rsu as select * from road_test where id <5")
        def  rsu =  Geoclimate.SpatialUnits.createRSU()
        rsu.execute([inputTableName: "roads_rsu",
                      outputTableName: "rsu", datasource: h2GIS])

        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from rsu")
        assert 10 == countRows.numberOfRows

    }


}