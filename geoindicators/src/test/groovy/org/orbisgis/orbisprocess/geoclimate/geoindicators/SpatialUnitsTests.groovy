package org.orbisgis.orbisprocess.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open
import static org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager.load

class SpatialUnitsTests {

    private static def h2GIS
    private static def randomDbName() {"${SpatialUnitsTests.simpleName}_${UUID.randomUUID().toString().replaceAll"-", "_"}"}

    @BeforeAll
    static void beforeAll(){
        h2GIS = open"./target/${randomDbName()};AUTO_SERVER=TRUE"
    }

    @BeforeEach
    void beforeEach(){
        h2GIS.executeScript(getClass().getResourceAsStream("data_for_tests.sql"))
    }

    @Test
    void createRSUTest() {
        h2GIS """
                DROP TABLE IF EXISTS roads_rsu;
                CREATE TABLE roads_rsu AS SELECT * FROM road_test WHERE id_road <5
        """
        def rsu = Geoindicators.SpatialUnits.createRSU()
        assert rsu([
                inputTableName  : "roads_rsu",
                prefixName      : "rsu",
                datasource      : h2GIS])
        def outputTable = rsu.results.outputTableName
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 9 == countRows.numberOfRows
    }

    @Test
    void prepareGeometriesForRSUTest() {
        h2GIS.load(SpatialUnitsTests.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("rail_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.getResource("zone_test.geojson"),true)

        def prepareData = Geoindicators.SpatialUnits.prepareRSUData()
        assert prepareData([
                zoneTable           : 'zone_test',
                roadTable           : 'road_test',
                railTable           : 'rail_test',
                vegetationTable     : 'veget_test',
                hydrographicTable   :'hydro_test',
                surface_vegetation  : null,
                surface_hydro       : null,
                prefixName          : "block",
                datasource          : h2GIS])

        def outputTableGeoms = prepareData.results.outputTableName

        assert h2GIS."$outputTableGeoms"

        def rsu = Geoindicators.SpatialUnits.createRSU()
        assert rsu([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName
        assert h2GIS.save(outputTable,'./target/rsu.shp')
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"

        assert 246 == countRows.numberOfRows
    }


    @Test
    void createBlocksTest() {
        h2GIS """
                DROP TABLE IF EXISTS build_tempo; 
                CREATE TABLE build_tempo AS SELECT * FROM building_test WHERE id_build <27
        """
        def blockP = Geoindicators.SpatialUnits.createBlocks()
        assert blockP([inputTableName: "build_tempo",distance:0.01,prefixName: "block", datasource: h2GIS])
        def outputTable = blockP.results.outputTableName
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"
        assert 12 == countRows.numberOfRows
    }

    @Test
    void createScalesRelationsTest() {
        h2GIS """
                DROP TABLE IF EXISTS build_tempo; 
                CREATE TABLE build_tempo AS 
                    SELECT id_build, the_geom FROM building_test 
                    WHERE id_build < 9 OR id_build > 28 AND id_build < 30
        """
        def pRsu =  Geoindicators.SpatialUnits.createScalesRelations()
        assert pRsu.execute([
                inputLowerScaleTableName    : "build_tempo",
                inputUpperScaleTableName    : "rsu_test",
                idColumnUp                  : "id_rsu",
                prefixName                  : "test",
                datasource                  : h2GIS])
        h2GIS.eachRow("SELECT * FROM ${pRsu.results.outputTableName}"){
            row ->
                def expected = h2GIS.firstRow("SELECT ${pRsu.results.outputIdColumnUp} FROM rsu_build_corr WHERE id_build = ${row.id_build}".toString())
                assert row[pRsu.results.outputIdColumnUp] == expected[pRsu.results.outputIdColumnUp]
        }
        def pBlock =  Geoindicators.SpatialUnits.createScalesRelations()
        assert pBlock([
                inputLowerScaleTableName    : "build_tempo",
                inputUpperScaleTableName    : "block_test",
                idColumnUp                  : "id_block",
                prefixName                  : "test",
                datasource                  : h2GIS])

        h2GIS.eachRow("SELECT * FROM ${pBlock.results.outputTableName}".toString()){
            row ->
                def expected = h2GIS.firstRow "SELECT ${pBlock.results.outputIdColumnUp} FROM block_build_corr WHERE id_build = ${row.id_build}".toString()
                assert row[pBlock.results.outputIdColumnUp] == expected[pBlock.results.outputIdColumnUp]
        }
    }

    @Test
    void prepareGeometriesForRSUWithFilterTest() {
        h2GIS.load(SpatialUnitsTests.class.class.getResource("road_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("rail_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("veget_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("hydro_test.geojson"), true)
        h2GIS.load(SpatialUnitsTests.class.class.getResource("zone_test.geojson"),true)

        def  prepareData = Geoindicators.SpatialUnits.prepareRSUData()

        assert prepareData([
                zoneTable               : 'zone_test',
                roadTable               : 'road_test',
                railTable               : 'rail_test',
                vegetationTable         : 'veget_test',
                hydrographicTable       : 'hydro_test',
                surface_vegetation      : null,
                surface_hydro           : null,
                prefixName              : "block",
                datasource              : h2GIS])


        def outputTableGeoms = prepareData.results.outputTableName

        assert h2GIS."$outputTableGeoms"
        def rsu = Geoindicators.SpatialUnits.createRSU()
        assert rsu.execute([inputTableName: outputTableGeoms, prefixName: "rsu", datasource: h2GIS])
        def outputTable = rsu.results.outputTableName
        assert h2GIS.save(outputTable,'./target/rsu.shp')
        def countRows = h2GIS.firstRow "select count(*) as numberOfRows from $outputTable"

        assert 246 == countRows.numberOfRows
    }

}