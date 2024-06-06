/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.osmtools.utils

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.LineString
import org.locationtech.jts.geom.MultiLineString
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.osmtools.AbstractOSMToolsTest
import org.orbisgis.geoclimate.osmtools.OSMTools
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for the processes in {@link org.orbisgis.geoclimate.osmtools.Transform}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class TransformUtilsTest extends AbstractOSMToolsTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    static File folder

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformUtilsTest)

    static H2GIS h2gis

    @BeforeAll
    static void beforeAll() {
        h2gis = H2GIS.open(folder.getAbsolutePath() + File.separator + "TransformUtilsTest;AUTO_SERVER=TRUE;")
    }

    @BeforeEach
    final void beforeEach(TestInfo testInfo) {
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo) {
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#createWhereFilter(java.lang.Object)}
     * method.
     */
    @Test
    void createWhereFilterTest() {
        def tags = new HashMap<>()
        tags["material"] = ["concrete"]
        assertGStringEquals "(a.tag_key = 'material' AND a.tag_value IN ('concrete'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = ["tata", "tutu"]
        assertGStringEquals "(a.tag_value IN ('tata','tutu'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[null] = "toto"
        assertGStringEquals "(a.tag_value IN ('toto'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["null"] = null
        assertGStringEquals "(a.tag_key = 'null')",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["empty"] = []
        assertGStringEquals "(a.tag_key = 'empty')",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["road"] = [null, "highway"]
        assertGStringEquals "(a.tag_key = 'road' AND a.tag_value IN ('highway'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["water"] = "pound"
        assertGStringEquals "(a.tag_key = 'water' AND a.tag_value IN ('pound'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags["pound"] = ["emilie", "big", "large"]
        assertGStringEquals "(a.tag_key = 'pound' AND a.tag_value IN ('emilie','big','large'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

        tags = new HashMap<>()
        tags[["river", "song"]] = ["big", "large"]
        assertGStringEquals "(a.tag_key IN ('river','song') AND a.tag_value IN ('big','large'))",
                OSMTools.TransformUtils.createWhereFilter(tags)

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
        assertGStringEquals "(a.tag_value IN ('toto')) OR " +
                "(a.tag_key = 'pound' AND a.tag_value IN ('emilie','big','large')) OR " +
                "(a.tag_key = 'material' AND a.tag_value IN ('concrete')) OR " +
                "(a.tag_key = 'null') OR " +
                "(a.tag_key = 'road' AND a.tag_value IN ('highway')) OR " +
                "(a.tag_key IN ('river','song') AND a.tag_value IN ('big','large')) OR " +
                "(a.tag_key = 'water' AND a.tag_value IN ('pound')) OR " +
                "(a.tag_key = 'empty')", OSMTools.TransformUtils.createWhereFilter(tags)

        assertGStringEquals "a.tag_key IN ('emilie','big','large')", OSMTools.TransformUtils.createWhereFilter(["emilie", "big", "large", null])
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#createWhereFilter(java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badCreateWhereFilterTest() {
        assertGStringEquals "", OSMTools.TransformUtils.createWhereFilter(null)
        assertGStringEquals "", OSMTools.TransformUtils.createWhereFilter(new HashMap())
        assertGStringEquals "", OSMTools.TransformUtils.createWhereFilter([])
        assertGStringEquals "", OSMTools.TransformUtils.createWhereFilter([null])
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#getColumnSelector(java.lang.Object, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badGetColumnSelectorTest() {
        def validTags = [toto: "tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertNull OSMTools.TransformUtils.getColumnSelector(null, validTags, columnsToKeep)
        assertNull OSMTools.TransformUtils.getColumnSelector("", validTags, columnsToKeep)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#getColumnSelector(java.lang.Object, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void getColumnSelectorTest() {
        def validTableName = "tutu"
        def validTags = [toto: "tata"]
        def columnsToKeep = ["col1", "col2", "col5"]
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('toto','col1','col2','col5')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, validTags, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('toto')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, validTags, null)
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('toto')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, validTags, [])
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('toto')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, validTags, [null, null])
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('toto','tutu')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('toto','tutu')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, validTags, "tutu")
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a  WHERE tag_key IN ('col1','col2','col5')",
                OSMTools.TransformUtils.getColumnSelector(validTableName, null, columnsToKeep)
        assertGStringEquals "SELECT distinct tag_key FROM tutu as a ", OSMTools.TransformUtils.getColumnSelector(validTableName, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#getCountTagsQuery(java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void getCountTagQueryTest() {
        def osmTable = "tutu"
        assertGStringEquals "SELECT count(*) AS count FROM tutu as a WHERE a.tag_key IN ('titi','tata')",
                OSMTools.TransformUtils.getCountTagsQuery(osmTable, ["titi", "tata"])
        assertGStringEquals "SELECT count(*) AS count FROM tutu as a",
                OSMTools.TransformUtils.getCountTagsQuery(osmTable, null)
        assertGStringEquals "SELECT count(*) AS count FROM tutu as a",
                OSMTools.TransformUtils.getCountTagsQuery(osmTable, [])
        assertGStringEquals "SELECT count(*) AS count FROM tutu as a",
                OSMTools.TransformUtils.getCountTagsQuery(osmTable, [null])
        assertGStringEquals "SELECT count(*) AS count FROM tutu as a WHERE a.tag_key IN ('toto')",
                OSMTools.TransformUtils.getCountTagsQuery(osmTable, "toto")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#getCountTagsQuery(java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badGetCountTagQueryTest() {
        assertNull OSMTools.TransformUtils.getCountTagsQuery(null, ["titi", "tata"])
        assertNull OSMTools.TransformUtils.getCountTagsQuery("", ["titi", "tata"])
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#createTagList(java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void createTagListTest() {
        def osmTable = "toto"
        h2gis.execute("DROP TABLE IF EXISTS TOTO; CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (0, 'material', ('concrete', 'brick'))")
        assertEquals(", MAX(CASE WHEN b.tag_key = 'material' THEN b.tag_value END) AS \"material\"",
                OSMTools.TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable").toString())
        h2gis.execute("DROP TABLE IF EXISTS toto; CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (1, 'water', null)")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'water' THEN b.tag_value END) AS \"water\"",
                OSMTools.TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto;CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (2, 'road', '{}')")
        assertGStringEquals ", MAX(CASE WHEN b.tag_key = 'road' THEN b.tag_value END) AS \"road\"",
                OSMTools.TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto; CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (0, 'material', ('concrete', 'brick'))")
        assertNull OSMTools.TransformUtils.createTagList(null, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#createTagList(java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badCreateTagListTest() {
        def osmTable = "toto"
        h2gis.execute("DROP TABLE IF EXISTS TOTO; CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (3, null, ('lake', 'pound'))")
        assertGStringEquals "", OSMTools.TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")

        h2gis.execute("CREATE TABLE toto (id int, tag_key varchar, tag_value VARCHAR array[255])")
        h2gis.execute("INSERT INTO toto VALUES (4, null, null)")
        assertGStringEquals "", OSMTools.TransformUtils.createTagList(h2gis, "SELECT tag_key FROM $osmTable")
        h2gis.execute("DROP TABLE IF EXISTS toto")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#buildIndexes(org.orbisgis.datamanager.JdbcDataSource, java.lang.String)}
     * method with bad data.
     */
    @Test
    void badBuildIndexesTest() {
        def osmTable = "toto"
        assertThrows(Exception.class,()->OSMTools.TransformUtils.buildIndexes(h2gis, null))
        assertThrows(Exception.class, ()->OSMTools.TransformUtils.buildIndexes(null, null))
        assertThrows(Exception.class, ()->OSMTools.TransformUtils.buildIndexes(null, osmTable))
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#buildIndexes(org.orbisgis.datamanager.JdbcDataSource, java.lang.String)}
     * method.
     */
    @Test
    void buildIndexesTest() {
        def osmTablesPrefix = "toto"
        h2gis.execute """
            DROP TABLE IF EXISTS ${osmTablesPrefix}_node, ${osmTablesPrefix}_node_tag, ${osmTablesPrefix}_way_node,
        ${osmTablesPrefix}_way, ${osmTablesPrefix}_way_tag,  ${osmTablesPrefix}_relation_tag, ${osmTablesPrefix}_relation,
${osmTablesPrefix}_way_member, ${osmTablesPrefix}_way_not_taken_into_account, ${osmTablesPrefix}_relation_not_taken_into_account;
            CREATE TABLE ${osmTablesPrefix}_node(id_node varchar);
            CREATE TABLE ${osmTablesPrefix}_node_tag(id_node varchar,tag_key varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_way_node(id_node varchar, node_order varchar, id_way varchar);
            CREATE TABLE ${osmTablesPrefix}_way(id_way varchar, not_taken_into_account varchar);
            CREATE TABLE ${osmTablesPrefix}_way_tag(tag_key varchar,id_way varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_relation_tag(tag_key varchar,id_relation varchar,tag_value varchar);
            CREATE TABLE ${osmTablesPrefix}_relation(id_relation varchar);
            CREATE TABLE ${osmTablesPrefix}_way_member(id_relation varchar);
            CREATE TABLE ${osmTablesPrefix}_way_not_taken_into_account(id_way varchar);
            CREATE TABLE ${osmTablesPrefix}_relation_not_taken_into_account(id_relation varchar);
        """.toString()

        OSMTools.TransformUtils.buildIndexes(h2gis, osmTablesPrefix)

        assertTrue h2gis.hasTable("${osmTablesPrefix}_node")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_node","id_node")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_way_node")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way_node","id_node")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way_node","node_order")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_way")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way","id_way")
        assertFalse h2gis.isIndexed("${osmTablesPrefix}_way","not_taken_into_account")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_way_tag")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way_tag","tag_key")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way_tag","id_way")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way_tag","tag_value")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_relation_tag")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_relation_tag","tag_key")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_relation_tag","id_relation")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_relation_tag","tag_value")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_relation")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_relation","id_relation")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_way_member")
        assertTrue h2gis.isIndexed("${osmTablesPrefix}_way_member","id_relation")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_way_not_taken_into_account")
        assertFalse h2gis.isIndexed("${osmTablesPrefix}_way_not_taken_into_account","id_way")

        assertTrue h2gis.hasTable("${osmTablesPrefix}_relation_not_taken_into_account")
        assertFalse h2gis.isIndexed("${osmTablesPrefix}_relation_not_taken_into_account","id_relation")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#arrayUnion(boolean, java.util.Collection [ ])}
     * method with bad data.
     */
    @Test
    void badArrayUnionTest() {
        assertNotNull OSMTools.TransformUtils.arrayUnion(true, null)
        assertTrue OSMTools.TransformUtils.arrayUnion(true, null).isEmpty()
        assertNotNull OSMTools.TransformUtils.arrayUnion(true, [])
        assertTrue OSMTools.TransformUtils.arrayUnion(true, []).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#arrayUnion(boolean, java.util.Collection [ ])}
     * method.
     */
    @Test
    void arrayUnionTest() {
        def unique = OSMTools.TransformUtils.arrayUnion(true, ["tata", "titi", "tutu"], ["titi", "toto", "toto"], [null, "value"])
        assertNotNull unique
        assertEquals 6, unique.size()
        assertEquals null, unique[0]
        assertEquals "tata", unique[1]
        assertEquals "titi", unique[2]
        assertEquals "toto", unique[3]
        assertEquals "tutu", unique[4]
        assertEquals "value", unique[5]

        def notUnique = OSMTools.TransformUtils.arrayUnion(false, ["tata", "titi", "tutu"], ["titi", "toto", "toto"], [null, "value"])
        assertNotNull notUnique
        assertEquals 8, notUnique.size()
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
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badExtractNodesAsPointsTest() {
        def prefix = "prefix" + uuid()
        def epsgCode = 2456
        def outTable = "output"
        def tags = [building: ["toto", "house", null], material: ["concrete"], road: null]
        tags.put(null, null)
        tags.put(null, ["value1", "value2"])
        tags.put('key', null)
        tags.put('key1', null)
        def columnsToKeep = []

        loadDataForNodeExtraction(h2gis, prefix)

        assertThrows(Exception.class, ()->OSMTools.TransformUtils.extractNodesAsPoints(null, prefix, epsgCode, outTable, tags, columnsToKeep))
        assertThrows(Exception.class, ()-> OSMTools.TransformUtils.extractNodesAsPoints(h2gis, null, epsgCode, outTable, tags, columnsToKeep))
        assertThrows(Exception.class, ()->OSMTools.TransformUtils.extractNodesAsPoints(h2gis, prefix, -1, outTable, tags, columnsToKeep))
        assertThrows(Exception.class, ()->OSMTools.TransformUtils.extractNodesAsPoints(h2gis, prefix, epsgCode, null, tags, columnsToKeep))

        assertFalse OSMTools.TransformUtils.extractNodesAsPoints(h2gis, prefix, epsgCode, outTable, [house: "false", path: 'false'], null)
    }

    private loadDataForNodeExtraction(H2GIS ds, def prefix) {
        ds.execute """
        DROP TABLE IF EXISTS ${prefix}_node, ${prefix}_node_tag; 
        CREATE TABLE ${prefix}_node (id_node int, the_geom geometry);
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
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void extractNodesAsPointsTest() {
        def prefix = "prefix" + uuid()
        def epsgCode = 2456
        def outTable = "output"
        def tags = [building: ["toto", "house", null], material: ["concrete"], road: null]
        tags.put(null, null)
        tags.put(null, ["value1", "value2"])
        tags.put('key', null)
        tags.put('key1', null)
        tags.put('key3', null)
        tags.put('key4', ["value1", "value2"])
        def columnsToKeep = ["key1"]

        loadDataForNodeExtraction(h2gis, prefix)

        //With tags
        assertTrue OSMTools.TransformUtils.extractNodesAsPoints(h2gis, prefix, epsgCode, outTable, tags, columnsToKeep)
        def table = h2gis.getTable(outTable)
        assertNotNull table
        def columns = table.getColumnNames()
        assertEquals 3, columns.size()
        assertEquals 3, columns.intersect(["ID_NODE", "THE_GEOM", "key1"]).size()
        assertFalse columns.contains("WATER")
        assertFalse columns.contains("KEY2")
        assertFalse columns.contains("HOUSE")
        assertFalse columns.contains("VALUES")

        assertEquals 1, table.rowCount

        assertEquals 1, h2gis.firstRow("select count(*) as count from $outTable where \"key1\" is null").count
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#extractNodesAsPoints(org.orbisgis.datamanager.JdbcDataSource, java.lang.String, int, java.lang.String, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void extractNodesAsPointsTest2() {
        def prefix = "prefix" + uuid()
        def epsgCode = 2456
        def outTable = "output"
        loadDataForNodeExtraction(h2gis, prefix)

        //Without tags and with column to keep
        assertTrue OSMTools.TransformUtils.extractNodesAsPoints(h2gis, prefix, epsgCode, outTable, [], ["key1", "build"])
        def table = h2gis.getTable("output")
        assertNotNull table

        def columns = table.getColumnNames()
        assertEquals 4, columns.size()
        assertEquals 4, columns.intersect(["ID_NODE", "THE_GEOM", "build", "key1"]).size()
        assertFalse columns.contains("WATER")
        assertFalse columns.contains("MATERIAL")
        assertFalse columns.contains("ROAD")
        assertFalse columns.contains("KEY")
        assertFalse columns.contains("KEY2")
        assertFalse columns.contains("KEY3")
        assertFalse columns.contains("KEY4")
        assertFalse columns.contains("HOUSE")
        assertFalse columns.contains("BUILDING")
        assertFalse columns.contains("VALUES")

        assertEquals 2, table.rowCount

        assertEquals 2, h2gis.firstRow("select count(*) as count from $outTable where \"key1\" is null").count
        assertEquals 1, h2gis.firstRow("select count(*) as count from $outTable where \"build\"='house'").count
    }

    @Test
    void extractNodesAsPointsTest3() {
        def prefix = "prefix" + uuid()
        def epsgCode = 2456
        def outTable = "output"
        loadDataForNodeExtraction(h2gis, prefix)

        //Without tags and columns to keep
        assertTrue OSMTools.TransformUtils.extractNodesAsPoints(h2gis, prefix, epsgCode, outTable, [], [])
        def table = h2gis.getTable("output")
        assertNotNull table
        def columns = table.getColumnNames()
        assertEquals 14, columns.size()
        assertEquals 14, columns.intersect(["ID_NODE", "THE_GEOM", "build",
                                            "building", "material", "road", "key", "key1", "key2",
                                            "key3", "key4", "values", "water", "house"]).size()

        assertEquals 15, table.rowCount
        assertEquals 15, h2gis.firstRow("select count(*) as count from $outTable where \"key1\" is null").count
        assertEquals 1, h2gis.firstRow("select count(*) as count from $outTable where \"building\"='house'").count
    }

    /**
     * test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#toPolygonOrLine(java.lang.String, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * method with bad data.
     */
    @Test
    void badToPolygonOrLineTest() {
        def badType = "notAType"
        def lineType = GeometryTypes.LINES
        def prefix = "OSM_" + uuid()
        def epsgCode = 2145
        def badEpsgCode = -1
        def tags = [:]
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.TransformUtils.toPolygonOrLine(null, h2gis, prefix, epsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.TransformUtils.toPolygonOrLine(lineType, null, prefix, epsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, null, epsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, badEpsgCode, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, -1, tags, columnsToKeep)
        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, epsgCode, null, null)
    }

    /**
     * test the {@link org.orbisgis.geoclimate.osmtools.utils.TransformUtils#toPolygonOrLine(java.lang.String, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object, java.lang.Object)}
     * method.
     */
    @Test
    void toPolygonOrLineTest() {
        def lineType = GeometryTypes.LINES
        def polygonType = GeometryTypes.POLYGONS
        def prefix = "OSM_" + uuid()
        def epsgCode = 2145
        def tags = ["building": ["house"]]
        def columnsToKeep = ["building","water"]

        //Load data
        createData(h2gis, prefix)

        //Test line
        def result = OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        def table = h2gis.getTable(result)
        assertEquals 2, table.rowCount
        table.each {
            switch (it.row) {
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
        result = OSMTools.TransformUtils.toPolygonOrLine(polygonType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        table = h2gis.getTable(result)
        assertEquals 2, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "lake", it.water
                    break
                case 2:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test no way tags
        h2gis.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);""".toString()
        result = OSMTools.TransformUtils.toPolygonOrLine(polygonType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        table = h2gis.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "lake", it.water
                    break
            }
        }
        result = OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        table = h2gis.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "house", it.building
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test no relation tags
        h2gis.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
        INSERT INTO ${prefix}_way_tag VALUES(1, 'building', 'house'),
        (1, 'material', 'concrete'),(1, 'water', 'lake');
        DROP TABLE ${prefix}_relation_tag;
        CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar)""".toString()

        result = OSMTools.TransformUtils.toPolygonOrLine(polygonType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        table = h2gis.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "lake", it.water
                    break
            }
        }
        result = OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
        table = h2gis.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "house", it.building
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test no tags
        h2gis.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
        DROP TABLE ${prefix}_relation_tag;
        CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar);""".toString()

        result = OSMTools.TransformUtils.toPolygonOrLine(polygonType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result

        result = OSMTools.TransformUtils.toPolygonOrLine(lineType, h2gis, prefix, epsgCode, tags, columnsToKeep)
        assertNotNull result
    }
}
