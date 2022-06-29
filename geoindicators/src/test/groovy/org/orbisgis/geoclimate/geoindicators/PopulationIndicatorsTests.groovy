package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.orbisgis.data.H2GIS
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.process.api.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class PopulationIndicatorsTests {


    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        //h2GIS = open "./target/population_db;AUTO_SERVER=TRUE"
        h2GIS = open "/tmp/population_db;AUTO_SERVER=TRUE"
    }

    @Test
    void formatPopulationTableTest() {
        h2GIS.execute("""DROP TABLE IF EXISTS population;
        CREATE TABLE population (id serial, the_geom GEOMETRY(POLYGON), pop float, pop_old float);
        INSERT INTO population VALUES (1,'POLYGON ((105 312, 230 312, 230 200, 105 200, 105 312))'::GEOMETRY, 12, 10 ),
        (1,'POLYGON((280 170, 390 170, 390 70, 280 70, 280 170))'::GEOMETRY, 1, 200 );""")
        IProcess process = Geoindicators.PopulationIndicators.formatPopulationTable()
        assertTrue(process.execute(populationTable: "population", populationColumns:["pop"], datasource:h2GIS))
        def populationTable = process.results.populationTable
        assertEquals(2, h2GIS.firstRow("select count(*) as count from $populationTable".toString()).count)
    }

    @Test
    void formatPopulationTableWithZoneTest() {
        h2GIS.execute("""DROP TABLE IF EXISTS population, zone;
        CREATE TABLE population (id serial, the_geom GEOMETRY(POLYGON), pop float, pop_old float);
        INSERT INTO population VALUES (1,'POLYGON ((105 312, 230 312, 230 200, 105 200, 105 312))'::GEOMETRY, 12, 10 ),
        (1,'POLYGON((280 170, 390 170, 390 70, 280 70, 280 170))'::GEOMETRY, 1, 200 );
        CREATE TABLE zone as select 'POLYGON ((70 390, 290 390, 290 270, 70 270, 70 390))'::GEOMETRY as the_geom;""")
        IProcess process = Geoindicators.PopulationIndicators.formatPopulationTable()
        assertTrue(process.execute(populationTable: "population", populationColumns:["pop"],
                zoneTable : "zone", datasource:h2GIS))
        def populationTable = process.results.populationTable
        assertEquals(1, h2GIS.firstRow("select count(*) as count from $populationTable".toString()).count)
    }

    @Test
    void multiScalePopulationTest() {
        h2GIS.execute("""DROP TABLE IF EXISTS building, population, rsu, grid;
        CREATE TABLE population (id_pop serial, the_geom GEOMETRY(POLYGON), pop float);
        INSERT INTO population VALUES (1,'POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'::GEOMETRY, 10 ),
        (2,'POLYGON((0 10, 0 20, 10 20, 10 10, 0 10))'::GEOMETRY, 10 ),
        (3,'POLYGON((10 0, 10 10, 20 10, 20 0, 10 0))'::GEOMETRY, 10 ),
        (4,'POLYGON((10 10, 10 20, 20 20, 20 10, 10 10))'::GEOMETRY, 10 );
        
        CREATE TABLE building (id_build serial, the_geom GEOMETRY(POLYGON), nb_lev integer, main_use varchar, type varchar, id_rsu integer);
        INSERT INTO building VALUES (1,'POLYGON ((2.4 16.15, 5.75 16.15, 5.75 13.85, 2.4 13.85, 2.4 16.15))'::GEOMETRY,1,
        'residential', 'residential', 1 ),
        (2,'POLYGON ((9 2, 11 2, 11 0, 9 0, 9 2))'::GEOMETRY,1, 'residential', 'residential',2 );
        
        CREATE TABLE rsu(id_rsu serial, the_geom GEOMETRY(POLYGON));
        INSERT INTO rsu VALUES (1, 'POLYGON ((-4.2 17.5, 9 17.5, 9 7.1, -4.2 7.1, -4.2 17.5))'::GEOMETRY),
        (2, 'POLYGON ((8 4, 16.5 4, 16.5 -1.8, 8 -1.8, 8 4))'::GEOMETRY), 
        (3, 'POLYGON ((-12 4.3, -1.8 4.3, -1.8 -1.2, -12 -1.2, -12 4.3))'::GEOMETRY);
        
        CREATE TABLE grid (id_grid serial, the_geom GEOMETRY(POLYGON));
        INSERT INTO grid VALUES (1,'POLYGON ((0 -5, 0 6, 10 6, 10 -5, 0 -5))'::GEOMETRY ),
        (2,'POLYGON((0 6, 0 20, 10 20, 10 6, 0 6))'::GEOMETRY ),
        (3,'POLYGON((10 -5, 10 6, 20 6, 20 -5, 10 -5))'::GEOMETRY ),
        (4,'POLYGON((10 6, 10 20, 20 20, 20 6, 10 6))'::GEOMETRY );""".toString())

        IProcess process = Geoindicators.PopulationIndicators.multiScalePopulation()
        assertTrue process.execute(populationTable : "population" , populationColumns: ["pop"],
                buildingTable: "building", rsuTable:"rsu", gridPopTable: "grid",
                datasource: h2GIS)
        def results = process.results

        results.each {it->
            h2GIS.save(it.value, "/tmp/${it.value}.geojson", true)
        }

        def rows = h2GIS.rows("SELECT id_build, pop from ${results.buildingTable} order by id_build".toString())
        assertEquals(10, rows[0].POP, 0.1)
        assertEquals(20, rows[1].POP, 0.1)

        rows = h2GIS.rows("SELECT * from ${results.rsuTable} order by id_rsu".toString())
        assertEquals(10, rows[0].SUM_POP, 0.1)
        assertEquals(20, rows[1].SUM_POP, 0.1)
        assertEquals(0, rows[2].SUM_POP, 0.1)

        rows = h2GIS.rows("SELECT * from ${results.gridTable} order by id_grid".toString())
        println(rows)
        assertEquals(10, rows[0].SUM_POP, 0.1)
        assertEquals(10, rows[1].SUM_POP, 0.1)
        assertEquals(10, rows[2].SUM_POP, 0.1)
        assertNull( rows[3].SUM_POP)
    }
    }
