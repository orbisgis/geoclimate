/*
 * Bundle OSMTools is part of the GeoClimate tool
 *
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies .
 * GeoClimate is developed by the GIS group of the DECIDE team of the
 * Lab-STICC CNRS laboratory, see <http://www.lab-sticc.fr/>.
 *
 * The GIS group of the DECIDE team is located at :
 *
 * Laboratoire Lab-STICC – CNRS UMR 6285
 * Equipe DECIDE
 * UNIVERSITÉ DE BRETAGNE-SUD
 * Institut Universitaire de Technologie de Vannes
 * 8, Rue Montaigne - BP 561 56017 Vannes Cedex
 *
 * OSMTools is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019-2021 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSMTools is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSMTools is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSMTools. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <https://github.com/orbisgis/geoclimate>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.geoclimate.osmtools

import org.junit.jupiter.api.*
import org.locationtech.jts.geom.*
import org.orbisgis.data.H2GIS
import org.orbisgis.process.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.orbisgis.geoclimate.osmtools.utils.OSMElement
import org.orbisgis.geoclimate.osmtools.utils.Utilities

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for the processes in {@link Transform}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class TransformTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformTest)

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
        super.beforeEach()
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        super.afterEach()
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPoints} process with bad data.
     */
    @Test
    void badToPointsTest(){
        def toPoints = OSMTools.Transform.toPoints()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse toPoints(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertTrue toPoints.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPoints} process.
     */
    @Test
    void toPointsTest(){
        def toPoints = OSMTools.Transform.toPoints()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse toPoints.results.isEmpty()
        def table = ds.getTable(toPoints.results.outputTableName)
        assertEquals 2, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals 1, it.id_node
                    assertTrue it.the_geom instanceof Point
                    assertEquals "house", it.building
                    assertEquals null, it.water
                    break
                case 2:
                    assertEquals 4, it.id_node
                    assertTrue it.the_geom instanceof Point
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test column to keep absent
        toPoints = OSMTools.Transform.toPoints()
        assertTrue toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landcover"])
        assertTrue ds.getTable(toPoints.results.outputTableName).isEmpty()

        //Test no points
        toPoints = OSMTools.Transform.toPoints()
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        assertFalse toPoints(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPoints.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toLines} process with bad data.
     */
    @Test
    void badToLinesTest(){
        def toLines = OSMTools.Transform.toLines()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse toLines(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertTrue toLines.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toLines} process.
     */
    @Test
    void toLinesTest(){
        def toLines = OSMTools.Transform.toLines()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue toLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse toLines.results.isEmpty()
        def table = ds.getTable(toLines.results.outputTableName)
        assertEquals 2, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
                case 2:
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test column to keep absent
        toLines = OSMTools.Transform.toLines()
        assertTrue toLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landcover"])
        assertTrue ds.getTable(toLines.results.outputTableName).isEmpty()

        //Test no lines
        toLines = OSMTools.Transform.toLines()
        ds.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
        DROP TABLE ${prefix}_relation_tag;
        CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar);""".toString()
        assertTrue toLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue ds.getTable(toLines.results.outputTableName).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPolygons} process with bad data.
     */
    @Test
    void badToPolygonsTest(){
        def toPolygons = OSMTools.Transform.toPolygons()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse toPolygons(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue toPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse toPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertTrue toPolygons.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPolygons} process.
     */
    @Test
    void toPolygonsTest(){
        def toPolygons = OSMTools.Transform.toPolygons()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue toPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse toPolygons.results.isEmpty()
        def table = ds.getSpatialTable(toPolygons.results.outputTableName)
        assertEquals 2, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "w1", it.id
                    assertEquals 2 ,  it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
                case 2:
                    assertEquals "r1", it.id
                    assertEquals 2 ,  it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test column to keep absent
        toPolygons = OSMTools.Transform.toPolygons()
        assertTrue toPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landcover"])
        assertTrue ds.getTable(toPolygons.results.outputTableName).isEmpty()

        //Test no polygons
        toPolygons = OSMTools.Transform.toPolygons()
        ds.execute """DROP TABLE ${prefix}_relation;
        CREATE TABLE ${prefix}_relation(id_relation int);
        DROP TABLE ${prefix}_way;
        CREATE TABLE ${prefix}_way(id_way int);""".toString()
        assertTrue toPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue ds.getTable(toPolygons.results.outputTableName).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process with bad data.
     */
    @Test
    void badExtractWaysAsPolygonsTest(){
        def extractWaysAsPolygons = OSMTools.Transform.extractWaysAsPolygons()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse extractWaysAsPolygons(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractWaysAsPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractWaysAsPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractWaysAsPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertFalse extractWaysAsPolygons.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process.
     */
    @Test
    void extractWaysAsPolygonsTest(){
        def extractWaysAsPolygons = OSMTools.Transform.extractWaysAsPolygons()
        H2GIS ds = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractWaysAsPolygons.results.isEmpty()
        def table = ds.getTable(extractWaysAsPolygons.results.outputTableName)
        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "w1", it.id
                    assertEquals 2 ,  it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        extractWaysAsPolygons = OSMTools.Transform.extractWaysAsPolygons()
        assertTrue extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[toto:"tata"], columnsToKeep:[])
        assertTrue ds.getTable(extractWaysAsPolygons.results.outputTableName).isEmpty()

        //Test no tags
        extractWaysAsPolygons = OSMTools.Transform.extractWaysAsPolygons()
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        assertTrue extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[], columnsToKeep:[])
        table = ds.getTable(extractWaysAsPolygons.results.outputTableName)
        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "w1", it.id
                    assertEquals 2 ,  it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }
        
        //Test column to keep absent
        extractWaysAsPolygons = OSMTools.Transform.extractWaysAsPolygons()
        assertTrue extractWaysAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landscape"])
        assertTrue ds.getTable(extractWaysAsPolygons.results.outputTableName).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsPolygons} process with bad data.
     */
    @Test
    void badExtractRelationsAsPolygonsTest(){
        def extractRelationsAsPolygons = OSMTools.Transform.extractRelationsAsPolygons()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse extractRelationsAsPolygons(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractRelationsAsPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractRelationsAsPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractRelationsAsPolygons.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertFalse extractRelationsAsPolygons.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsPolygons)} process.
     */
    @Test
    void extractRelationsAsPolygonsTest(){
        def extractRelationsAsPolygons = OSMTools.Transform.extractRelationsAsPolygons()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractRelationsAsPolygons.results.isEmpty()
        def table = ds.getTable(extractRelationsAsPolygons.results.outputTableName)
        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "r1", it.id
                    assertEquals 2 ,  it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        extractRelationsAsPolygons = OSMTools.Transform.extractRelationsAsPolygons()
        assertTrue extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[toto:"tata"], columnsToKeep:[])
        assertTrue ds.getTable(extractRelationsAsPolygons.results.outputTableName).isEmpty()

        //Test no tags
        extractRelationsAsPolygons = OSMTools.Transform.extractRelationsAsPolygons()
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        assertTrue extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[], columnsToKeep:[])
        table = ds.getTable(extractRelationsAsPolygons.results.outputTableName)
        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "r1", it.id
                    assertEquals 2 ,  it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }
        
        //Test column to keep absent
        extractRelationsAsPolygons = OSMTools.Transform.extractRelationsAsPolygons()
        assertTrue extractRelationsAsPolygons(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landscape"])
        assertTrue ds.getTable(extractRelationsAsPolygons.results.outputTableName).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsLines} process with bad data.
     */
    @Test
    void badExtractWaysAsLinesTest(){
        def extractWaysAsLines = OSMTools.Transform.extractWaysAsLines()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse extractWaysAsLines(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractWaysAsLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractWaysAsLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractWaysAsLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertFalse extractWaysAsLines.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsLines} process.
     */
    @Test
    void extractWaysAsLinesTest(){
        def extractWaysAsLines = OSMTools.Transform.extractWaysAsLines()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractWaysAsLines.results.isEmpty()
        def table = ds.getTable(extractWaysAsLines.results.outputTableName)

        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        extractWaysAsLines = OSMTools.Transform.extractWaysAsLines()
        assertTrue extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[toto:"tata"], columnsToKeep:[])
        assertTrue ds.getTable(extractWaysAsLines.results.outputTableName).isEmpty()

        //Test column to keep absent
        extractWaysAsLines = OSMTools.Transform.extractWaysAsLines()
        assertTrue extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landscape"])
        assertTrue ds.getTable(extractWaysAsLines.results.outputTableName).isEmpty()

        //Test no tags
        extractWaysAsLines = OSMTools.Transform.extractWaysAsLines()
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        assertTrue extractWaysAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[], columnsToKeep:[])
        table = ds.getTable(extractWaysAsLines.results.outputTableName)
        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsLines} process with bad data.
     */
    @Test
    void badExtractRelationsAsLinesTest(){
        def extractRelationsAsLines = OSMTools.Transform.extractRelationsAsLines()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertFalse extractRelationsAsLines(datasource: null, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractRelationsAsLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertFalse extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:-1, tags:tags, columnsToKeep:columnsToKeep)
        assertTrue extractRelationsAsLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractRelationsAsLines.results.isEmpty()

        LOGGER.warn("An error will be thrown next")
        assertTrue extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:null, columnsToKeep:null)
        assertFalse extractRelationsAsLines.results.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsLines)} process.
     */
    @Test
    void extractRelationsAsLinesTest(){
        def extractRelationsAsLines = OSMTools.Transform.extractRelationsAsLines()
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2453
        def tags = [building:"house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        assertTrue extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:columnsToKeep)
        assertFalse extractRelationsAsLines.results.isEmpty()
        def table = ds.getTable(extractRelationsAsLines.results.outputTableName)

        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        extractRelationsAsLines = OSMTools.Transform.extractRelationsAsLines()
        assertTrue extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[toto:"tata"], columnsToKeep:[])
        assertTrue ds.getTable(extractRelationsAsLines.results.outputTableName).isEmpty()

        //Test no tags
        extractRelationsAsLines = OSMTools.Transform.extractRelationsAsLines()
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        assertTrue extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:[], columnsToKeep:[])
        table = ds.getTable(extractRelationsAsLines.results.outputTableName)
        assertEquals 1, table.rowCount
        table.each{
            switch(it.row){
                case 1:
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }
        
        //Test column to keep absent
        extractRelationsAsLines = OSMTools.Transform.extractRelationsAsLines()
        assertTrue extractRelationsAsLines(datasource: ds, osmTablesPrefix: prefix, epsgCode:epsgCode, tags:tags, columnsToKeep:["landscape"])
        assertTrue ds.getTable(extractRelationsAsLines.results.outputTableName).isEmpty()
    }

    /**
     * Use to test the online transform
     */
    @Disabled
    @Test
    void transformOnLine() {
        H2GIS h2GIS = RANDOM_DS()
        Geometry geom = Utilities.getNominatimData("Saint Jean La Poterie");
        def query = Utilities.buildOSMQuery(geom.getEnvelopeInternal(), [], OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
        def extract = OSMTools.Loader.extract()
        if (!query.isEmpty()) {
            if (extract.execute(overpassQuery: query)) {
                def prefix = "OSM_FILE_${OSMTools.Utilities.uuid()}"
                def load = OSMTools.Loader.load()
                if (load(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath:extract.results.outputFilePath)) {
                    def tags = ['building']
                    def transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags)
                    assertNotNull(transform.results.outputTableName)
                    assertTrue(h2GIS.getTable(transform.results.outputTableName).getRowCount()>0)
                }
            }
        }
    }


    @Test
    void buildGISLayersTest () {
        def h2GIS = H2GIS.open('./target/osmtools_read_file;AUTO_SERVER=TRUE')
        IProcess loader = OSMTools.Loader.load()
        def prefix = "OSM_REDON"
        assertTrue loader.execute(datasource : h2GIS, osmTablesPrefix : prefix,
                osmFilePath : new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())
        //Create building layer
        def tags = ["building"]
        IProcess transform = OSMTools.Transform.toPolygons()
        transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags)
        def outputTableName = transform.results.outputTableName
        assertEquals 6, h2GIS.firstRow("select count(*) as count from ${outputTableName} where ST_NumInteriorRings(the_geom)  > 0").count as int
        assertEquals 1032, h2GIS.firstRow("select count(*) as count from ${outputTableName} where ST_NumInteriorRings(the_geom)  = 0").count as int

        //Create landuse layer
        tags = ["landuse":["farmland", "forest", "grass", "meadow", "orchard", "vineyard", "village_green", "allotments"],]
        transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags)
        outputTableName = transform.results.outputTableName
        assertEquals 131, h2GIS.firstRow("select count(*) as count from ${outputTableName}").count as int
        assertEquals 123, h2GIS.firstRow("select count(*) as count from ${outputTableName} where \"landuse\"='grass'").count as int

    }



    /**
     * It uses for test purpose
     */
    @Disabled
    @Test
    void testIntegrationForAPlaceName() {
        H2GIS h2GIS = H2GIS.open("/tmp/geoclimate;AUTO_SERVER=TRUE")

        def  keysValues = ["building", "railway", "amenity",
                           "leisure", "highway", "natural",
                           "landuse", "landcover",
                           "vegetation","waterway"]

        Geometry geom = Utilities.getNominatimData("Redon");

        def query =  "[maxsize:1073741824]"+ Utilities.buildOSMQueryWithAllData(geom.getEnvelopeInternal(), keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)


        def extract = OSMTools.Loader.extract()
        if (!query.isEmpty()) {
            if (extract.execute(overpassQuery: query)) {
                def prefix = "OSM_FILE_${OSMTools.Utilities.uuid()}"
                def load = OSMTools.Loader.load()
                if (load(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath:extract.results.outputFilePath)) {
                    def transform
                    //Extract water
                    def tags = ["natural":["water","waterway","bay", "strait"],
                    "water":[],
                    "waterway":[],
                    "landuse":["basin", " salt_pond"]]
                    def columns = ["natural", "layer"]
                    transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags, columnsToKeep:columns)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/water.shp", true)

                    //Extract buildings
                    tags = ["building"]
                    transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/building.shp", true)

                    //Extract impervious areas
                    tags = ["highway": [
                            "residential",
                            "pedestrian"
                    ],
                    "amenity": [
                            "parking",
                            "bicycle_parking",
                            "car_sharing",
                            "parking_place"
                    ],
                    "leisure": ["pitch"],
                    "railway": ["platform"],
                    "landuse": ["railway","construction"]]
                    columns = [
                            "highway",
                            "surface",
                            "amenity",
                            "area",
                            "leisure",
                            "parking",
                            "landuse"
                    ]
                    transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags, columnsToKeep: columns)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/impervious.shp", true)

                }
            }
        }
    }

    /**
     * It uses for test purpose
     */
    @Disabled
    @Test
    void testIntegrationForOSMFile() {
        H2GIS h2GIS = H2GIS.open("/tmp/geoclimate;AUTO_SERVER=TRUE")
                def prefix = "OSM_FILE_${OSMTools.Utilities.uuid()}"
                def load = OSMTools.Loader.load()
                if (load(datasource: h2GIS, osmTablesPrefix: prefix, osmFilePath:"/tmp/map.osm")) {
                    //Extract building
                    /*def tags = ["natural":["water","waterway","bay", "strait"],
                                "water":[],
                                "waterway":[],
                                "landuse":["basin", " salt_pond"]]
                    def columns = ["natural", "layer"]
                    def transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags, columnsToKeep:columns)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/water.shp", true)*/

                    //Extract buildings
                    def tags = ["building"]
                    def transform = OSMTools.Transform.toPolygons()
                    transform.execute(datasource: h2GIS, osmTablesPrefix: prefix, tags: tags)
                    assertNotNull(transform.results.outputTableName)
                    h2GIS.getTable(transform.results.outputTableName).save("/tmp/building.shp", true)
                }
    }
}
