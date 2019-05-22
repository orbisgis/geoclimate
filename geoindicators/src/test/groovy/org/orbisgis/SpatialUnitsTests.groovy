package org.orbisgis

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull

class SpatialUnitsTests {

    @Test
    void createRSUTest() {
        def h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        h2GIS.execute("drop table if exists roads_rsu; " +
                "create table roads_rsu as select * from road_test where id_road <5")
        def  rsu =  Geoclimate.SpatialUnits.createRSU()
        rsu.execute([inputTableName: "roads_rsu",
                     prefixName: "rsu", datasource: h2GIS])
        String outputTable = rsu.results.outputTableName
        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from $outputTable".toString())
        assertEquals 10, countRows.numberOfRows
    }

    @Test
    void prepareGeometriesForRSUTest() {
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
        h2GIS.save(outputTable,'/tmp/rsu.shp')
        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from $outputTable".toString())

        assertEquals 213 , countRows.numberOfRows
    }


    @Test
    void createBlocksTest() {
        def h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        h2GIS.execute("drop table if exists build_tempo; " +
                "create table build_tempo as select * from building_test where id_build <27")
        def  blockP =  Geoclimate.SpatialUnits.createBlocks()
        blockP.execute([inputTableName: "build_tempo",distance:0.01,
                     prefixName: "block", datasource: h2GIS])
        String outputTable = blockP.results.outputTableName
        def countRows =  h2GIS.firstRow("select count(*) as numberOfRows from $outputTable".toString())
        assertEquals 12 , countRows.numberOfRows
    }

    @Test
    void createScalesRelationsTest() {
        def h2GIS = H2GIS.open([databaseName: '/tmp/spatialunitsdb'])
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)
        h2GIS.execute("DROP TABLE IF EXISTS build_tempo; " +
                "CREATE TABLE build_tempo AS SELECT id_build, the_geom FROM building_test WHERE id_build < 9 "+
                "OR id_build > 28 AND id_build < 30")
        def pRsu =  Geoclimate.SpatialUnits.createScalesRelations()
        pRsu.execute([inputLowerScaleTableName: "build_tempo", inputUpperScaleTableName : "rsu_test",
                      idColumnUp: "id_rsu", prefixName: "test", datasource: h2GIS])
        h2GIS.eachRow("SELECT * FROM ${pRsu.results.outputTableName}".toString()){
            row ->
                def expected = h2GIS.firstRow("SELECT ${pRsu.results.outputIdColumnUp} FROM rsu_build_corr WHERE id_build = ${row.id_build}".toString())
                assertEquals(row[pRsu.results.outputIdColumnUp], expected[pRsu.results.outputIdColumnUp])
        }
        def pBlock =  Geoclimate.SpatialUnits.createScalesRelations()
        pBlock.execute([inputLowerScaleTableName: "build_tempo", inputUpperScaleTableName : "block_test",
                        idColumnUp: "id_block", prefixName: "test", datasource: h2GIS])

        h2GIS.eachRow("SELECT * FROM ${pBlock.results.outputTableName}".toString()){
            row ->
                def expected = h2GIS.firstRow("SELECT ${pBlock.results.outputIdColumnUp} FROM block_build_corr WHERE id_build = ${row.id_build}".toString())
                assertEquals(row[pBlock.results.outputIdColumnUp], expected[pBlock.results.outputIdColumnUp])
        }
    }

}