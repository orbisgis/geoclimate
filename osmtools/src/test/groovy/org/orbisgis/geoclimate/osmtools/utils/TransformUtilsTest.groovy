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
package org.orbisgis.geoclimate.osmtools.utils

import org.h2.jdbc.JdbcSQLSyntaxErrorException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.locationtech.jts.geom.LineString
import org.locationtech.jts.geom.MultiLineString
import org.locationtech.jts.geom.Polygon
import org.orbisgis.geoclimate.osmtools.AbstractOSMTest
import org.orbisgis.data.H2GIS
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for the processes in {@link org.orbisgis.orbisanalysis.osm.Transform}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class TransformUtilsTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformUtilsTest)

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
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#createWhereFilter(java.lang.Object)}
     * method.
     */
    @Test
    void createWhereFilterTest(){
        def tags = new HashMap<>()
        tags["material"] = ["concrete"]
        assertGStringEquals "(tag_key = 'material' AND tag_value IN ('concrete'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = ["tata", "tutu"]
        assertGStringEquals "(tag_value IN ('tata','tutu'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = "toto"
        assertGStringEquals "(tag_value IN ('toto'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["null"] = null
        assertGStringEquals "(tag_key = 'null')",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["empty"] = []
        assertGStringEquals "(tag_key = 'empty')",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["road"] = [null, "highway"]
        assertGStringEquals "(tag_key = 'road' AND tag_value IN ('highway'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["water"] = "pound"
        assertGStringEquals "(tag_key = 'water' AND tag_value IN ('pound'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["pound"] = ["emilie", "big", "large"]
        assertGStringEquals "(tag_key = 'pound' AND tag_value IN ('emilie','big','large'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[["river", "song"]] = ["big", "large"]
        assertGStringEquals "(tag_key IN ('river','song') AND tag_value IN ('big','large'))",
                TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["material"] = ["concrete"]
        tags[null] = ["tata", "tutu"]
        tags[null] = "toto"
        tags["null"] = null
        tags["empty"] = []
        tags["road"] = [null, "highway"]
        tags["water"] = "pound"
        tags["pound"] = ["emilie", "big", "large"]
        tags[["river", "song"]] = ["big", "large"]
        assertGStringEquals "(tag_value IN ('toto')) OR " +
                "(tag_key = 'pound' AND tag_value IN ('emilie','big','large')) OR " +
                "(tag_key = 'material' AND tag_value IN ('concrete')) OR " +
                "(tag_key = 'null') OR " +
                "(tag_key = 'road' AND tag_value IN ('highway')) OR " +
                "(tag_key IN ('river','song') AND tag_value IN ('big','large')) OR " +
                "(tag_key = 'water' AND tag_value IN ('pound')) OR " +
                "(tag_key = 'empty')", TransformUtils.createWhereFilter(tags)

        assertGStringEquals "tag_key IN ('emilie','big','large')", TransformUtils.createWhereFilter(["emilie", "big", "large", null])
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#createWhereFilter(java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badCreateWhereFilterTest(){
        assertGStringEquals "", TransformUtils.createWhereFilter(null)
        assertGStringEquals "", TransformUtils.createWhereFilter(new HashMap())
        assertGStringEquals "", TransformUtils.createWhereFilter([])
        assertGStringEquals "", TransformUtils.createWhereFilter([null])
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#getColumnSelector(java.lang.Object, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badGetColumnSelectorTest(){
        def validTableName = "tutu"
        def validTags = [toto:"tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertNull TransformUtils.getColumnSelector(null, validTags, columnsToKeep)
        assertNull TransformUtils.getColumnSelector("", validTags, columnsToKeep)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#getColumnSelector(java.lang.Object, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void getColumnSelectorTest(){
        def validTableName = "tutu"
        def validTags = [toto:"tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','col1','col2','col5')",
                TransformUtils.getColumnSelector(validTableName, validTags, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getColumnSelector(validTableName, validTags, null)
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getColumnSelector(validTableName, validTags, [])
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getColumnSelector(validTableName, validTags, [null, null])
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','tutu')",
                TransformUtils.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('toto','tutu')",
                TransformUtils.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu WHERE tag_key IN ('col1','col2','col5')",
                TransformUtils.getColumnSelector(validTableName, null, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu", TransformUtils.getColumnSelector(validTableName, null, null)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#getCountTagsQuery(java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void getCountTagQueryTest(){
        def osmTable = "tutu"
        assertGStringEquals "SELECT count(*) AS count FROM tutu WHERE tag_key IN ('titi','tata')",
                TransformUtils.getCountTagsQuery(osmTable, ["titi", "tata"])
        assertGStringEquals "SELECT count(*) AS count FROM tutu",
                TransformUtils.getCountTagsQuery(osmTable, null)
        assertGStringEquals "SELECT count(*) AS count FROM tutu",
                TransformUtils.getCountTagsQuery(osmTable, [])
        assertGStringEquals "SELECT count(*) AS count FROM tutu",
                TransformUtils.getCountTagsQuery(osmTable, [null])
        assertGStringEquals "SELECT count(*) AS count FROM tutu WHERE tag_key IN ('toto')",
                TransformUtils.getCountTagsQuery(osmTable, "toto")
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#getCountTagsQuery(java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badGetCountTagQueryTest(){
        assertNull TransformUtils.getCountTagsQuery(null, ["titi", "tata"])
        assertNull TransformUtils.getCountTagsQuery("", ["titi", "tata"])
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#createTagList(java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void createTagListTest(){
        def h2gis = RANDOM_DS()
        def osmTable = "toto"

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (0, 'material', ('concrete', 'brick'))")
        assertEquals(", MAX(CASE WHEN b.tag_key = 'material' THEN b.tag_value END) AS \"material\"",
                TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable").toString())
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (1, 'water', null)")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'water' THEN b.tag_value END) AS \"water\"",
                TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (2, 'road', '{}')")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'road' THEN b.tag_value END) AS \"road\"",
                TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (0, 'material', ('concrete', 'brick'))")
        assertNull  TransformUtils.createTagList(null, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#createTagList(java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badCreateTagListTest(){
        def h2gis = RANDOM_DS()
        def osmTable = "toto"

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (3, null, ('lake', 'pound'))")
        assertGStringEquals "", TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (4, null, null)")
        assertGStringEquals "", TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#buildIndexes(org.orbisgis.datamanager.JdbcDataSource, java.lang.String)}
     * method with bad data.
     */
    @Test
    void badBuildIndexesTest(){
        def h2gis = RANDOM_DS()
        def osmTable = "toto"

        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.buildIndexes(h2gis, null)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.buildIndexes(null, null)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.buildIndexes(null, osmTable)
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#buildIndexes(org.orbisgis.datamanager.JdbcDataSource, java.lang.String)}
     * method.
     */
    @Test
    void buildIndexesTest(){
        def h2gis = RANDOM_DS()
        def osmTablesPrefix = "toto"
        h2gis.execute """
            CREATE TABLE ${osmTablesPrefix}_node(id_node varchar);
            CREATE TABLE ${osmTablesPrefix}_way_node(id_node varchar, node_order varchar, id_way varchar);
            CREATE TABLE ${osmTablesPrefix}_way(id_way varchar, not_taken_into_account varchar);
            CREATE TABLE ${osmTablesPrefix}_way_tag(tag_key varchar,id_way varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_relation_tag(tag_key varchar,id_relation varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_relation(id_relation varchar);
            CREATE TABLE ${osmTablesPrefix}_way_member(id_relation varchar);
            CREATE TABLE ${osmTablesPrefix}_way_not_taken_into_account(id_way varchar);
            CREATE TABLE ${osmTablesPrefix}_relation_not_taken_into_account(id_relation varchar);
        """.toString()

        TransformUtils.buildIndexes(h2gis, osmTablesPrefix)

        assertNotNull h2gis.getTable("${osmTablesPrefix}_node")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_node")."id_node"
        assertTrue h2gis.getTable("${osmTablesPrefix}_node")."id_node".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")."id_node"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_node")."id_node".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")."node_order"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_node")."node_order".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_node")."id_way"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_node")."id_way".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way")."id_way"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way")."id_way".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way")."not_taken_into_account"
        assertFalse h2gis.getTable("${osmTablesPrefix}_way")."not_taken_into_account".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_key"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_key".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")."id_way"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_tag")."id_way".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_value"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_tag")."tag_value".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_key"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_key".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")."id_relation"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation_tag")."id_relation".indexed
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_value"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation_tag")."tag_value".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation")."id_relation"
        assertTrue h2gis.getTable("${osmTablesPrefix}_relation")."id_relation".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_member")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_member")."id_relation"
        assertTrue h2gis.getTable("${osmTablesPrefix}_way_member")."id_relation".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_not_taken_into_account")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_way_not_taken_into_account")."id_way"
        assertFalse h2gis.getTable("${osmTablesPrefix}_way_not_taken_into_account")."id_way".indexed

        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_not_taken_into_account")
        assertNotNull h2gis.getTable("${osmTablesPrefix}_relation_not_taken_into_account")."id_relation"
        assertFalse h2gis.getTable("${osmTablesPrefix}_relation_not_taken_into_account")."id_relation".indexed
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#arrayUnion(boolean, java.util.Collection[])}
     * method with bad data.
     */
    @Test
    void badArrayUnionTest(){
        assertNotNull TransformUtils.arrayUnion(true, null)
        assertTrue TransformUtils.arrayUnion(true, null).isEmpty()
        assertNotNull TransformUtils.arrayUnion(true, [])
        assertTrue TransformUtils.arrayUnion(true, []).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#arrayUnion(boolean, java.util.Collection[])}
     * method.
     */
    @Test
    void arrayUnionTest(){
        def unique = TransformUtils.arrayUnion(true, ["tata", "titi", "tutu"], ["titi", "toto", "toto"], [null, "value"])
        assertNotNull unique
        assertEquals 6, unique.size
        assertEquals null, unique[0]
        assertEquals "tata", unique[1]
        assertEquals "titi", unique[2]
        assertEquals "toto", unique[3]
        assertEquals "tutu", unique[4]
        assertEquals "value", unique[5]

        def notUnique = TransformUtils.arrayUnion(false, ["tata", "titi", "tutu"], ["titi", "toto", "toto"], [null, "value"])
        assertNotNull notUnique
        assertEquals 8, notUnique.size
        assertEquals null, notUnique[0]
        assertEquals "tata", notUnique[1]
        assertEquals "titi", notUnique[2]
        assertEquals "titi", notUnique[3]
        assertEquals "toto", notUnique[4]
        assertEquals "toto", notUnique[5]
        assertEquals "tutu", notUnique[6]
        assertEquals "value", notUnique[7]
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badExtractNodesAsPointsTest(){
        H2GIS ds = RANDOM_DS()
        def prefix = "prefix"+uuid()
        def epsgCode  = 2456
        def outTable = "output"
        def tags = [building:["toto", "house", null], material:["concrete"], road:null]
        tags.put(null, null)
        tags.put(null, ["value1", "value2"])
        tags.put('key', null)
        tags.put('key1', null)
        def columnsToKeep = []

        loadDataForNodeExtraction(ds, prefix)

        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(null, prefix, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, null, epsgCode, outTable, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, -1, outTable, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, null, tags, columnsToKeep)

        assertFalse TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, [house:"false", path:'false'], null)
    }

    private static loadDataForNodeExtraction(def ds, def prefix){
        ds.execute """CREATE TABLE ${prefix}_node (id_node int, the_geom geometry);
        INSERT INTO ${prefix}_node VALUES (1, 'POINT(0 0)'),
         (2, 'POINT(1 1)'),
         (3, 'POINT(2 2)'),
         (4, 'POINT(56.23 78.23)'),
         (5, 'POINT(-5.3 -45.23)'),
         (6, 'POINT(-5.3 -45.23)'),
         (7, 'POINT(-5.3 -45.23)'),
         (8, 'POINT(-5.3 -45.23)'),
         (9, 'POINT(-5.3 -45.23)'),
         (10, 'POINT(-5.3 -45.23)'),
         (11, 'POINT(-5.3 -45.23)'),
         (12, 'POINT(-5.3 -45.23)'),
         (13, 'POINT(-5.3 -45.23)'),
         (14, 'POINT(-5.3 -45.23)'),
         (15, 'POINT(-5.3 -45.23)');

        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);
        INSERT INTO ${prefix}_node_tag VALUES (1, 'building', 'house'),
        (1, 'house', 'true'),
        (1, 'material', 'concrete'),
        (2, 'water', 'pound'),
        (3, 'material', 'concrete'),
        (4, 'build', 'house'),
        (5, 'material', 'brick'),
        (6, 'material', null),
        (7, null, 'value1'),
        (8, 'key', null),
        (8, 'key1', null),
        (9, 'key2', null),
        (10, 'values', 'value1'),
        (11, 'key3', null),
        (12, 'key3', 'val1'),
        (13, 'road', 'service'),
        (14, 'key4', 'service'),
        (15, 'road', 'service'),
        (16, 'material', 'concrete');""".toString()
    }

    /**
     * Test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void extractNodesAsPointsTest(){
        H2GIS ds = RANDOM_DS()
        def prefix = "prefix"+uuid()
        def epsgCode  = 2456
        def outTable = "output"
        def tags = [building:["toto", "house", null], material:["concrete"], road:null]
        tags.put(null, null)
        tags.put(null, ["value1", "value2"])
        tags.put('key', null)
        tags.put('key1', null)
        tags.put('key3', null)
        tags.put('key4', ["value1", "value2"])
        def columnsToKeep = ["key1"]

        loadDataForNodeExtraction(ds, prefix)

        //With tags
        assertTrue TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, tags, columnsToKeep)
        def table = ds.getTable("output")
        assertNotNull table

        assertEquals 9, table.columnCount
        assertNotNull ds.getTable("output").id_node
        assertNotNull ds.getTable("output").the_geom
        assertNotNull ds.getTable("output").building
        assertNotNull ds.getTable("output").material
        assertNotNull ds.getTable("output").road
        assertNotNull ds.getTable("output").key
        assertNotNull ds.getTable("output").key1
        assertNotNull ds.getTable("output").key3
        assertNotNull ds.getTable("output").key4
        assertFalse table.columns.contains("WATER")
        assertFalse table.columns.contains("KEY2")
        assertFalse table.columns.contains("HOUSE")
        assertFalse table.columns.contains("VALUES")

        assertEquals 9, table.rowCount
        table.each {it ->
            switch(it.row){
                case 1:
                    assertEquals 1, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals "house", it."building"
                    assertEquals "concrete", it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 2:
                    assertEquals 3, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals "concrete", it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 3:
                    assertEquals 7, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 4:
                    assertEquals 8, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 5:
                    assertEquals 10, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 6:
                    assertEquals 11, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 7:
                    assertEquals 12, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals "val1", it."key3"
                    assertEquals null, it."key4"
                    break
                case 8:
                    assertEquals 13, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals "service", it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
                case 9:
                    assertEquals 15, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."material"
                    assertEquals "service", it."road"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    break
            }
        }

        //Without tags and with column to keep
        assertTrue TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, null, ["key1", "build"])
        table = ds.getTable("output")
        assertNotNull table

        assertEquals 4, table.columnCount
        assertNotNull ds.getTable("output").id_node
        assertNotNull ds.getTable("output").the_geom
        assertNotNull ds.getTable("output").build
        assertNotNull ds.getTable("output").key1
        assertFalse table.columns.contains("WATER")
        assertFalse table.columns.contains("MATERIAL")
        assertFalse table.columns.contains("ROAD")
        assertFalse table.columns.contains("KEY")
        assertFalse table.columns.contains("KEY2")
        assertFalse table.columns.contains("KEY3")
        assertFalse table.columns.contains("KEY4")
        assertFalse table.columns.contains("HOUSE")
        assertFalse table.columns.contains("BUILDING")
        assertFalse table.columns.contains("VALUES")

        //Without tags and columns to keep
        assertTrue TransformUtils.extractNodesAsPoints(ds, prefix, epsgCode, outTable, null, [])
        table = ds.getTable("output")
        assertNotNull table

        assertEquals 14, table.columnCount
        assertNotNull ds.getTable("output").id_node
        assertNotNull ds.getTable("output").the_geom
        assertNotNull ds.getTable("output").build
        assertNotNull ds.getTable("output").building
        assertNotNull ds.getTable("output").house
        assertNotNull ds.getTable("output").key
        assertNotNull ds.getTable("output").key1
        assertNotNull ds.getTable("output").key2
        assertNotNull ds.getTable("output").key3
        assertNotNull ds.getTable("output").key4
        assertNotNull ds.getTable("output").material
        assertNotNull ds.getTable("output").road
        assertNotNull ds.getTable("output").values
        assertNotNull ds.getTable("output").water

        assertEquals 15, table.rowCount
        table.each {it ->
            switch(it.row){
                case 1:
                    assertEquals 1, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals "house", it."building"
                    assertEquals null, it."build"
                    assertEquals "true", it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals "concrete", it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 2:
                    assertEquals 2, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals "pound", it."water"
                    break
                case 3:
                    assertEquals 3, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals "concrete", it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 4:
                    assertEquals 4, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals "house", it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 5:
                    assertEquals 5, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals "brick", it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 6:
                    assertEquals 6, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 7:
                    assertEquals 7, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 8:
                    assertEquals 8, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 9:
                    assertEquals 9, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 10:
                    assertEquals 10, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals "value1", it."values"
                    assertEquals null, it."water"
                    break
                case 11:
                    assertEquals 11, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 12:
                    assertEquals 12, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals "val1", it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 13:
                    assertEquals 13, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals "service", it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 14:
                    assertEquals 14, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals "service", it."key4"
                    assertEquals null, it."material"
                    assertEquals null, it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
                case 15:
                    assertEquals 15, it."id_node"
                    assertNotNull it."the_geom"
                    assertEquals null, it."building"
                    assertEquals null, it."build"
                    assertEquals null, it."house"
                    assertEquals null, it."key"
                    assertEquals null, it."key1"
                    assertEquals null, it."key2"
                    assertEquals null, it."key3"
                    assertEquals null, it."key4"
                    assertEquals null, it."material"
                    assertEquals "service", it."road"
                    assertEquals null, it."values"
                    assertEquals null, it."water"
                    break
            }
        }
    }

    /**
     * test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#toPolygonOrLine(java.lang.String, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badToPolygonOrLineTest(){
        def badType = "notAType"
        def lineType = TransformUtils.Types.LINES
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_"+uuid()
        def epsgCode = 2145
        def badEpsgCode = -1
        def tags = [:]
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull TransformUtils.toPolygonOrLine(null, ds, prefix, epsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull TransformUtils.toPolygonOrLine(lineType, null, prefix, epsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull TransformUtils.toPolygonOrLine(lineType, ds, null, epsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull TransformUtils.toPolygonOrLine(lineType, ds, prefix, badEpsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull TransformUtils.toPolygonOrLine(lineType, ds, prefix, null, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull TransformUtils.toPolygonOrLine(lineType, ds, prefix, epsgCode, null, null)
    }

    /**
     * test the {@link org.orbisgis.orbisanalysis.osm.utils.TransformUtils#toPolygonOrLine(java.lang.String, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void toPolygonOrLineTest() {
        def lineType = TransformUtils.Types.LINES
        def polygonType = TransformUtils.Types.POLYGONS
        H2GIS ds = RANDOM_DS()
        def prefix = "OSM_" + uuid()
        def epsgCode = 2145
        def tags = ["building": ["house"]]
        def columnsToKeep = ["water"]

        //Load data
        createData(ds, prefix)

        //Test line
        def result = TransformUtils.toPolygonOrLine(lineType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        assertTrue result.containsKey("outputTableName")
        assertNotNull result.outputTableName
        def table = ds.getTable(result.outputTableName)
        assertEquals 2, table.rowCount
        table.each {
            switch(it.row){
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "lake", it.water
                    break
                case 2:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test polygon
        result = TransformUtils.toPolygonOrLine(polygonType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        assertTrue result.containsKey("outputTableName")
        assertNotNull result.outputTableName
        table = ds.getTable(result.outputTableName)
        assertEquals 2, table.rowCount
        table.each {
            switch(it.row){
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof Polygon
                    assertEquals "lake", it.water
                    break
                case 2:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof Polygon
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test no way tags
        ds.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);""".toString()
        result = TransformUtils.toPolygonOrLine(polygonType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        assertTrue result.containsKey("outputTableName")
        assertNotNull result.outputTableName
        table = ds.getTable(result.outputTableName)
        assertEquals 1, table.rowCount
        table.each {
            switch(it.row){
                case 1:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof Polygon
                    assertEquals "lake", it.water
                    break
            }
        }
        result = TransformUtils.toPolygonOrLine(lineType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        assertTrue result.containsKey("outputTableName")
        assertNotNull result.outputTableName
        table = ds.getTable(result.outputTableName)
        assertEquals 1, table.rowCount
        table.each {
            switch(it.row){
                case 1:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test no relation tags
        ds.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
        INSERT INTO ${prefix}_way_tag VALUES(1, 'building', 'house'),
        (1, 'material', 'concrete'),(1, 'water', 'lake');
        DROP TABLE ${prefix}_relation_tag;
        CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar)""".toString()

        result = TransformUtils.toPolygonOrLine(polygonType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        assertTrue result.containsKey("outputTableName")
        assertNotNull result.outputTableName
        table = ds.getTable(result.outputTableName)
        assertEquals 1, table.rowCount
        table.each {
            switch(it.row){
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof Polygon
                    assertEquals "lake", it.water
                    break
            }
        }
        result = TransformUtils.toPolygonOrLine(lineType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        assertTrue result.containsKey("outputTableName")
        assertNotNull result.outputTableName
        table = ds.getTable(result.outputTableName)
        assertEquals 1, table.rowCount
        table.each {
            switch(it.row){
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test no tags
        ds.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
        DROP TABLE ${prefix}_relation_tag;
        CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar);""".toString()

        result = TransformUtils.toPolygonOrLine(polygonType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result

        result = TransformUtils.toPolygonOrLine(lineType, ds, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
    }
}
