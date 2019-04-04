package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull

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
                     prefixName: "rsu", datasource: h2GIS])
        String outputTable = rsu.results.outputTableName
        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from $outputTable".toString())
        assertEquals 10, countRows.numberOfRows
    }

    @Test
    void testPrepareGeometriesForRSU() {
        H2GIS h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
        h2GIS.load(this.class.getResource("road_test.shp").toString(), true)
        h2GIS.load(this.class.getResource("rail_test.shp").toString(), true)
        h2GIS.load(this.class.getResource("veget_test.shp").toString(), true)
        h2GIS.load(this.class.getResource("hydro_test.shp").toString(), true)
        h2GIS.load(this.class.getResource("zone_test.shp").toString(),true)

        def  prepareData =  Geoclimate.SpatialUnits.prepareRSUData()
        prepareData.execute([zoneTable: 'zone_test', roadTable: 'road_test',  railTable: 'rail_test',
                             vegetationTable : 'veget_test',
                             hydrographicTable :'hydro_test',surface_vegetation : null, surface_hydro : null,
                             prefixName: "block", datasource: h2GIS])

        String outputTableGeoms = prepareData.results.outputTableName

        assertNotNull h2GIS.getTable(outputTableGeoms)

        def  rsu =  Geoclimate.SpatialUnits.createRSU()
        rsu.execute([inputTableName: outputTableGeoms,
                     prefixName: "rsu", datasource: h2GIS])
        String outputTable = rsu.results.outputTableName
        h2GIS.save("rsu",'/tmp/rsu.shp')
        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from $outputTable".toString())

        assertEquals 213 , countRows.numberOfRows
    }


    @Test
    void testCreateBlocks() {
        def h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        def  blockP =  Geoclimate.SpatialUnits.createBlocks()
        blockP.execute([inputTableName: "building_test",distance:0.01,
                     prefixName: "block", datasource: h2GIS])
        String outputTable = blockP.results.outputTableName
        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from $outputTable".toString())
        assertEquals 12 , countRows.numberOfRows
    }


}