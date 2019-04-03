package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

class SpatialUnitsTests {

    @Test
    void testCreateRSU() {
        def h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
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

    @Test
    void testPrepareGeometriesForRSU() {
        def h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        def  prepareData =  Geoclimate.SpatialUnits.prepareRSUData()
        prepareData.execute([zoneTable: 'zone_test', roadTable: 'road_test',  railTable: 'rail_test', vegetationTable : 'veg_test',
                             hydrographicTable :'hydro_test',surface_vegetation : null, surface_hydro : null,
                     outputTableName: "unified_geometries", datasource: h2GIS])

        String outputTableGeoms = prepareData.results.outputTableName

        assert h2GIS.getTable(outputTableGeoms)!=null

        def  rsu =  Geoclimate.SpatialUnits.createRSU()
        rsu.execute([inputTableName: "outputTableGeoms",
                     outputTableName: "rsu", datasource: h2GIS])

        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from rsu")
        assert 10 == countRows.numberOfRows
    }


}