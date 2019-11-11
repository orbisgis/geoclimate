package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class SpatialUnitsTests {

    private static H2GIS h2GIS

    @BeforeAll
    static void init(){
        h2GIS = H2GIS.open('./target/spatialunitsdb;AUTO_SERVER=TRUE')
    }

    @BeforeEach
    void initData(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void createRSUTest() {
        h2GIS.execute "drop table if exists roads_rsu; " +
                "create table roads_rsu as select * from road_test where id_road <5"
        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: "roads_rsu", prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assertEquals 10, countRows.numberOfRows
    }

    @Test
    void prepareGeometriesForRSUTest() {
        H2GIS h2GIS = H2GIS.open('./target/spatialunitsdb2;AUTO_SERVER=TRUE')
        h2GIS.load(SpatialUnitsTests.class.getResource("road_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("rail_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("veget_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("hydro_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.getResource("zone_test.shp"),true)

        def  prepareData = Geoindicators.SpatialUnits.prepareRSUData()
        assertTrue prepareData.execute([zoneTable: 'zone_test', roadTable: 'road_test',  railTable: 'rail_test',
                             vegetationTable : 'veget_test',
                             hydrographicTable :'hydro_test',surface_vegetation : null, surface_hydro : null,
                             prefixName: "block", datasource: h2GIS])

        def outputTableGeoms = prepareData.results.outputTableName

        assertNotNull h2GIS.getTable(outputTableGeoms)

        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName
        assertTrue h2GIS.save(outputTable,'./target/rsu.shp')
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"

        assertEquals 246 , countRows.numberOfRows
    }


    @Test
    void createBlocksTest() {
        h2GIS.execute("drop table if exists build_tempo; " +
                "create table build_tempo as select * from building_test where id_build <27")
        def  blockP = Geoindicators.SpatialUnits.createBlocks()
        assertTrue blockP.execute([inputTableName: "build_tempo",distance:0.01,
                     prefixName: "block", datasource: h2GIS])
        def outputTable = blockP.results.outputTableName
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assertEquals 12 , countRows.numberOfRows
    }

    @Test
    void createScalesRelationsTest() {
        h2GIS.execute "DROP TABLE IF EXISTS build_tempo; " +
                "CREATE TABLE build_tempo AS SELECT id_build, the_geom FROM building_test WHERE id_build < 9 "+
                "OR id_build > 28 AND id_build < 30"
        def pRsu =  Geoindicators.SpatialUnits.createScalesRelations()
        assertTrue pRsu.execute([inputLowerScaleTableName: "build_tempo", inputUpperScaleTableName : "rsu_test",
                      idColumnUp: "id_rsu", prefixName: "test", datasource: h2GIS])
        h2GIS.eachRow("SELECT * FROM ${pRsu.results.outputTableName}".toString()){
            row ->
                def expected = h2GIS.firstRow("SELECT ${pRsu.results.outputIdColumnUp} FROM rsu_build_corr WHERE id_build = ${row.id_build}".toString())
                assertEquals(row[pRsu.results.outputIdColumnUp], expected[pRsu.results.outputIdColumnUp])
        }
        def pBlock =  Geoindicators.SpatialUnits.createScalesRelations()
        assertTrue pBlock.execute([inputLowerScaleTableName: "build_tempo", inputUpperScaleTableName : "block_test",
                        idColumnUp: "id_block", prefixName: "test", datasource: h2GIS])

        h2GIS.eachRow("SELECT * FROM ${pBlock.results.outputTableName}".toString()){
            row ->
                def expected = h2GIS.firstRow("SELECT ${pBlock.results.outputIdColumnUp} FROM block_build_corr WHERE id_build = ${row.id_build}".toString())
                assertEquals(row[pBlock.results.outputIdColumnUp], expected[pBlock.results.outputIdColumnUp])
        }
    }

    @Test
    void prepareGeometriesForRSUWithFilterTest() {
        H2GIS h2GIS = H2GIS.open('./target/spatialunitsdb2;AUTO_SERVER=TRUE')
        h2GIS.load(SpatialUnitsTests.class.class.getResource("road_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("rail_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("veget_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("hydro_test.shp"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("zone_test.shp"),true)

        def  prepareData = Geoindicators.SpatialUnits.prepareRSUData()

        assertTrue prepareData.execute([zoneTable               : 'zone_test', roadTable            : 'road_test',
                                        railTable               : 'rail_test', vegetationTable      : 'veget_test',
                                        hydrographicTable       :'hydro_test', surface_vegetation   : null,
                                        surface_hydro           : null,        prefixName           : "block",
                                        datasource              : h2GIS])


        def outputTableGeoms = prepareData.results.outputTableName

        assertNotNull h2GIS.getTable(outputTableGeoms)
        def rsu = Geoindicators.SpatialUnits.createRSU()
        assertTrue rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName
        assertTrue h2GIS.save(outputTable,'./target/rsu.shp')
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"

        assertEquals 246 , countRows.numberOfRows
    }

}