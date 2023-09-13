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
package org.orbisgis.geoclimate.osmtools

import ch.qos.logback.classic.Logger
import org.h2gis.utilities.GeographyUtilities
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.*
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.osmtools.utils.OSMElement
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.geoclimate.utils.LoggerUtils

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for the processes in {@link Transform}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class TransformTest extends AbstractOSMToolsTest {

    @TempDir
    static File folder

    private static final Logger LOGGER = LoggerUtils.createLogger(TransformTest)

    static H2GIS ds

    @BeforeAll
    static void loadDb() {
        ds = H2GIS.open(folder.getAbsolutePath() + File.separator + "TransformTest;AUTO_SERVER=TRUE;")
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
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPoints} process with bad data.
     */
    @Test
    void badToPointsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPoints(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPoints(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("A default table is created")
        assertNotNull OSMTools.Transform.toPoints(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPoints(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPoints} process.
     */
    @Test
    void toPointsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = []

        createData(ds, prefix)

        String result = OSMTools.Transform.toPoints(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getTable(result)
        assertEquals 2, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals 1, it.id_node
                    assertTrue it.the_geom instanceof Point
                    assertEquals "house", it.building
                    break
                case 2:
                    assertEquals 4, it.id_node
                    assertTrue it.the_geom instanceof Point
                    assertEquals "house", it.building
                    break
            }
        }

        //Test column to keep absent
        result = OSMTools.Transform.toPoints(ds, prefix, epsgCode, tags, ["landcover"])
        assertTrue ds.getTable(result).isEmpty()

        //Test no points       
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        result = OSMTools.Transform.toPoints(ds, prefix, epsgCode, tags, columnsToKeep)
        assertNull result
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toLines} process with bad data.
     */
    @Test
    void badToLinesTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toLines(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toLines(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toLines(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toLines(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toLines} process.
     */
    @Test
    void toLinesTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = ["building", "water"]

        createData(ds, prefix)

        String result = OSMTools.Transform.toLines(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getTable(result)
        assertEquals 2, table.rowCount
        table.each {
            switch (it.row) {
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
        result = OSMTools.Transform.toLines(ds, prefix, epsgCode, tags, ["landcover"])
        assertTrue ds.getTable(result).isEmpty()

        //Test no lines
        ds.execute """DROP TABLE ${prefix}_way_tag;
        CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
        DROP TABLE ${prefix}_relation_tag;
        CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar);""".toString()
        result = OSMTools.Transform.toLines(ds, prefix, epsgCode, tags, columnsToKeep)
        assertTrue ds.getTable(result).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPolygons} process with bad data.
     */
    @Test
    void badToPolygonsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPolygons(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPolygons(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPolygons(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.toPolygons(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#toPolygons} process.
     */
    @Test
    void toPolygonsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = ["water"]

        createData(ds, prefix)

        String result = OSMTools.Transform.toPolygons(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getSpatialTable(result)
        assertEquals 2, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "w1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "lake", it.water
                    break
                case 2:
                    assertEquals "r1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test column to keep absent
        result = OSMTools.Transform.toPolygons(ds, prefix, epsgCode, tags, ["landcover"])
        assertEquals(0, ds.getRowCount(result))

        //Test no polygons
        ds.execute """DROP TABLE ${prefix}_relation;
        CREATE TABLE ${prefix}_relation(id_relation int);
        DROP TABLE ${prefix}_way;
        CREATE TABLE ${prefix}_way(id_way int);""".toString()
        result = OSMTools.Transform.toPolygons(ds, prefix, epsgCode, tags, columnsToKeep)
        assertEquals 0, ds.getRowCount(result)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process with bad data.
     */
    @Test
    void badExtractWaysAsPolygonsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractWaysAsPolygons(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractWaysAsPolygons(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("A default table is created")
        assertNotNull OSMTools.Transform.extractWaysAsPolygons(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractWaysAsPolygons(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process.
     */
    @Test
    void extractWaysAsPolygonsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = ["water", "building"]

        createData(ds, prefix)

        String result = OSMTools.Transform.extractWaysAsPolygons(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "w1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }
    }
    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process.
     */
    @Test
    void extractWaysAsPolygonsTest2() {
        def prefix = "OSM_" + uuid()
        createData(ds, prefix)
        def epsgCode = 2453
        //Test not existing tags
        def result = OSMTools.Transform.extractWaysAsPolygons(ds, prefix, epsgCode, [toto: "tata"], [])
        assertTrue ds.getTable(result).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process.
     */
    @Test
    void extractWaysAsPolygonsTest3() {
        def prefix = "OSM_" + uuid()
        createData(ds, prefix)
        def epsgCode = 2453
        //Test no tags
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        def result = OSMTools.Transform.extractWaysAsPolygons(ds, prefix, epsgCode, [], [])
        def table = ds.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "w1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsPolygons} process.
     */
    @Test
    void extractWaysAsPolygonsTest4() {
        def prefix = "OSM_" + uuid()
        createData(ds, prefix)
        def epsgCode = 2453
        def tags = [building: "house"]
        //Test column to keep absent
        def result = OSMTools.Transform.extractWaysAsPolygons(ds, prefix, epsgCode, tags, ["landscape"])
        assertFalse ds.getTable(result).columns.contains("landscape")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsPolygons} process with bad data.
     */
    @Test
    void badExtractRelationsAsPolygonsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractRelationsAsPolygons(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("A default table is created")
        assertNotNull OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsPolygons )} process.
     */
    @Test
    void extractRelationsAsPolygonsTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = ["building","water"]

        createData(ds, prefix)

        String result = OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "r1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        result = OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, epsgCode, [toto: "tata"], [])
        assertTrue ds.getTable(result).isEmpty()

        //Test no tags
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        result = OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, epsgCode, [], [])
        table = ds.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "r1", it.id
                    assertEquals 2, it.the_geom.getDimension()
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test column to keep absent
        result = OSMTools.Transform.extractRelationsAsPolygons(ds, prefix, epsgCode, tags, ["landscape"])
        assertTrue ds.getTable(result).isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsLines} process with bad data.
     */
    @Test
    void badExtractWaysAsLinesTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractWaysAsLines(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractWaysAsLines(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("A default table is created")
        assertNotNull OSMTools.Transform.extractWaysAsLines(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractWaysAsLines(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractWaysAsLines} process.
     */
    @Test
    void extractWaysAsLinesTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = ["building", "water"]

        createData(ds, prefix)

        String result = OSMTools.Transform.extractWaysAsLines(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getTable(result)

        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "w1", it.id
                    assertTrue it.the_geom instanceof LineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        result = OSMTools.Transform.extractWaysAsLines(ds, prefix, epsgCode, [toto: "tata"], [])
        assertTrue ds.getTable(result).isEmpty()

        //Test column to keep absent
        result = OSMTools.Transform.extractWaysAsLines(ds, prefix, epsgCode, tags, ["landscape"])
        assertTrue ds.getTable(result).isEmpty()

        //Test no tags
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        result = OSMTools.Transform.extractWaysAsLines(ds, prefix, epsgCode, [], [])
        table = ds.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
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
    void badExtractRelationsAsLinesTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = []
        def columnsToKeep = []

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractRelationsAsLines(null, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractRelationsAsLines(ds, prefix, -1, tags, columnsToKeep)

        LOGGER.warn("A default table is created")
        assertNotNull OSMTools.Transform.extractRelationsAsLines(ds, prefix, epsgCode, tags, columnsToKeep)

        LOGGER.warn("An error will be thrown next")
        assertNull OSMTools.Transform.extractRelationsAsLines(ds, prefix, epsgCode, null, null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.Transform#extractRelationsAsLines )} process.
     */
    @Test
    void extractRelationsAsLinesTest() {
        def prefix = "OSM_" + uuid()
        def epsgCode = 2453
        def tags = [building: "house"]
        def columnsToKeep = ["building", "water"]

        createData(ds, prefix)

        String result = OSMTools.Transform.extractRelationsAsLines(ds, prefix, epsgCode, tags, columnsToKeep)
        assertFalse result.isEmpty()
        def table = ds.getTable(result)

        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test not existing tags
        result = OSMTools.Transform.extractRelationsAsLines(ds, prefix, epsgCode, [toto: "tata"], [])
        assertTrue ds.getTable(result).isEmpty()

        //Test no tags
        ds.execute """DROP TABLE ${prefix}_node_tag;
        CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);""".toString()
        result = OSMTools.Transform.extractRelationsAsLines(ds, prefix, epsgCode, [], [])
        table = ds.getTable(result)
        assertEquals 1, table.rowCount
        table.each {
            switch (it.row) {
                case 1:
                    assertEquals "r1", it.id
                    assertTrue it.the_geom instanceof MultiLineString
                    assertEquals "house", it.building
                    assertEquals "lake", it.water
                    break
            }
        }

        //Test column to keep absent
        result = OSMTools.Transform.extractRelationsAsLines(ds, prefix, epsgCode, tags, ["landscape"])
        assertTrue ds.getTable(result).isEmpty()
    }

    /**
     * Use to test the online transform
     */
    @Disabled
    @Test
    void transformOnLine() {
        Geometry geom = Utilities.getNominatimData("Saint Jean La Poterie");
        def query = Utilities.buildOSMQuery(geom.getEnvelopeInternal(), [], OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
        if (!query.isEmpty()) {
            def extract = OSMTools.Loader.extract(query)
            if (extract) {
                def prefix = "OSM_FILE_${OSMTools.Utilities.uuid()}"
                if (OSMTools.Loader.load(h2GIS, prefix, extract)) {
                    def tags = ['building']
                    def transform = OSMTools.Transform.toPolygons(h2GIS, prefix, tags)
                    assertNotNull(transform)
                    assertTrue(h2GIS.getTable(transform).getRowCount() > 0)
                }
            }
        }
    }


    @Test
    void buildGISLayersTest() {
        def prefix = "OSM_REDON"
        assertTrue OSMTools.Loader.load(ds, prefix,
                new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())
        //Create building layer
        def tags = ["building"]
        String outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 4326, tags)
        assertEquals 6, ds.firstRow("select count(*) as count from ${outputTableName} where ST_NumInteriorRings(the_geom)  > 0").count as int
        assertEquals 1032, ds.firstRow("select count(*) as count from ${outputTableName} where ST_NumInteriorRings(the_geom)  = 0").count as int

        //Create landuse layer
        tags = ["landuse": ["farmland", "forest", "grass", "meadow", "orchard", "vineyard", "village_green", "allotments"],]
        outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 4326, tags)
        assertEquals 131, ds.firstRow("select count(*) as count from ${outputTableName}").count as int
        assertEquals 123, ds.firstRow("select count(*) as count from ${outputTableName} where \"landuse\"='grass'").count as int

        //Create urban areas layer
        tags = ["landuse": [
                "commercial",
                "residential",
                "retail",
                "industrial"
        ]]
         outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 4326, tags)

        assertEquals 6, ds.firstRow("select count(*) as count from ${outputTableName}").count as int
        assertEquals 4, ds.firstRow("select count(*) as count from ${outputTableName} where \"landuse\"='residential'").count as int

    }

    @Test
    void buildPolygonWithComplexHoles() {
        def prefix = "OSM_SAN_DIEGO"
        assertTrue OSMTools.Loader.load(ds, prefix,
                new File(this.class.getResource("san_diegeo_complex_polygon.osm").toURI()).getAbsolutePath())
        String outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 4326, ["leisure"], ["leisure"], true)
        assertEquals(0,ds.firstRow("SELECT COUNT(*) as count FROM $outputTableName where st_isvalid(the_geom)=false").count)
    }

    @Test
    void buildPolygonWithInvalidGeometries() {
        def prefix = "OSM_SAN_DIEGO"
        assertTrue OSMTools.Loader.load(ds, prefix,
                new File(this.class.getResource("san_diego_invalid_polygon.osm").toURI()).getAbsolutePath())
        String outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 4326, ["leisure"], ["leisure"], true)
        assertEquals(0,ds.firstRow("SELECT COUNT(*) as count FROM $outputTableName where st_isvalid(the_geom)=false").count)
    }

    @Test
    void buildGISPolygonsFilterGeometryTest() {
        def prefix = "OSM_REDON"
        assertTrue OSMTools.Loader.load(ds, prefix,
                new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())
        //Create building layer
        def tags = ["building"]
        String outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 2154, tags)
        assertEquals 6, ds.firstRow("select count(*) as count from ${outputTableName} where ST_NumInteriorRings(the_geom)  > 0").count as int

        Geometry geom = ds.firstRow("select st_extent(st_transform(the_geom, 2154)) as the_geom from ${outputTableName} ").the_geom;
        Geometry env = geom.getCentroid().buffer(20).getEnvelope()
        outputTableName = OSMTools.Transform.toPolygons(ds, prefix, 2154, tags, env)
        assertEquals(9, ds.getRowCount(outputTableName))
    }

    @Test
    void buildGISLinesFilterGeometryTest() {
        def prefix = "OSM_REDON"
        assertTrue OSMTools.Loader.load(ds, prefix,
                new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())
        //Create building layer
        def tags = ["highway"]
        String outputTableName = OSMTools.Transform.toLines(ds, prefix, 2154, tags)
        assertEquals 362, ds.firstRow("select count(*) as count from ${outputTableName}").count as int

        Geometry geom = ds.firstRow("select st_extent(st_transform(the_geom, 2154)) as the_geom from ${outputTableName} ").the_geom;
        Geometry env = geom.getCentroid().buffer(20).getEnvelope()
        outputTableName = OSMTools.Transform.toLines(ds, prefix, 2154, tags, env)

        assertEquals(1, ds.getRowCount(outputTableName))
    }


    @Test
    void buildGISPointsFilterGeometryTest() {
        def prefix = "OSM_REDON"
        assertTrue OSMTools.Loader.load(ds, prefix,
                new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath())
        //Create building layer
        def tags = ["amenity"]
        String outputTableName = OSMTools.Transform.toPoints(ds, prefix, 2154, tags)

        assertEquals 223, ds.firstRow("select count(*) as count from ${outputTableName}").count as int
        Geometry geom = ds.firstRow("select st_extent(st_transform(the_geom, 2154)) as the_geom from ${outputTableName} ").the_geom;
        Geometry env = geom.getCentroid().buffer(20).getEnvelope()
        outputTableName = OSMTools.Transform.toPoints(ds, prefix, 2154, tags, env)
        assertEquals(1, ds.getRowCount(outputTableName))
    }


    /**
     * It uses for test purpose
     */
    @Disabled
    @Test
    void testIntegrationForAPlaceName() {
        H2GIS h2GIS = H2GIS.open("/tmp/geoclimate;AUTO_SERVER=TRUE")

        def keysValues = ["building", "railway", "amenity",
                          "leisure", "highway", "natural",
                          "landuse", "landcover",
                          "vegetation", "waterway"]

        Geometry geom = Utilities.getNominatimData("Redon");

        def query = "[maxsize:1073741824]" + Utilities.buildOSMQueryWithAllData(geom.getEnvelopeInternal(), keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

        if (!query.isEmpty()) {
            def extract = OSMTools.Loader.extract(query)
            if (extract) {
                def prefix = "OSM_FILE_${OSMTools.Utilities.uuid()}"
                if (OSMTools.Loader.load(h2GIS, prefix, extract)) {
                    def transform
                    //Extract water
                    def tags = ["natural" : ["water", "waterway", "bay", "strait"],
                                "water"   : [],
                                "waterway": [],
                                "landuse" : ["basin", " salt_pond"]]
                    def columns = ["natural", "layer"]
                    String outputTableName = OSMTools.Transform.toPolygons(h2GIS, prefix, tags, columns)
                    assertNotNull(outputTableName)
                    h2GIS.getTable(outputTableName).save("/tmp/water.shp", true)

                    //Extract buildings
                    tags = ["building"]
                    outputTableName = OSMTools.Transform.toPolygons(h2GIS, prefix, tags)
                    assertNotNull(outputTableName)
                    h2GIS.getTable(outputTableName).save("/tmp/building.shp", true)

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
                            "landuse": ["railway", "construction"]]
                    columns = [
                            "highway",
                            "surface",
                            "amenity",
                            "area",
                            "leisure",
                            "parking",
                            "landuse"
                    ]
                    outputTableName = OSMTools.Transform.toPolygons(h2GIS, prefix, tags, columns)
                    assertNotNull(outputTableName)
                    h2GIS.getTable(outputTableName).save("/tmp/impervious.shp", true)

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
        if (OSMTools.Loader.load(h2GIS, prefix, osmFilePath: "/tmp/map.osm")) {
            //Extract building
            /*def tags = ["natural":["water","waterway","bay", "strait"],
                        "water":[],
                        "waterway":[],
                        "landuse":["basin", " salt_pond"]]
            def columns = ["natural", "layer"]
            def transform = OSMTools.Transform.toPolygons()
            transform.execute(h2GIS,  prefix,  tags, columns)
            assertNotNull(transform.results.outputTableName)
            h2GIS.getTable(transform.results.outputTableName).save("/tmp/water.shp", true)*/

            //Extract buildings
            def tags = ["building"]
            def outputTableName = OSMTools.Transform.toPolygons(h2GIS, prefix, tags)
            assertNotNull(outputTableName)
            h2GIS.getTable(outputTableName).save("/tmp/building.shp", true)
        }
    }

    /**
     * It uses for test purpose
     */
    @Disabled
    @Test
    void testIntegrationForDebug() {
        def location = "Redon"
        float distance = 1000
        def interiorPoint = true
        def nominatim = Utilities.getNominatimData(location)

        if (!nominatim) {
            println "Cannot find any OSM data for the location $location"
        }

        Geometry geom = nominatim["geom"]

        if (!geom) {
            println "Cannot find any geometry area for the location $location"
        }

        def env = geom.getEnvelopeInternal()
        if (interiorPoint) {
            Coordinate p = geom.getInteriorPoint().getCoordinate()
            env = GeographyUtilities.createEnvelope(new Coordinate(p.x, p.y), distance, distance)
        }

        def overpass_enpoint = "https://lz4.overpass-api.de/api"

        System.setProperty("OVERPASS_ENPOINT", overpass_enpoint)

        H2GIS h2GIS = H2GIS.open("/tmp/geoclimate;AUTO_SERVER=TRUE")

        def keysValues = ["building", "railway", "amenity",
                          "leisure", "highway", "natural",
                          "landuse", "landcover",
                          "vegetation", "waterway"]

        def query = "[maxsize:536870912]" + Utilities.buildOSMQueryWithAllData(env, keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)


        if (!query.isEmpty()) {
            def extract = OSMTools.Loader.extract(query)
            if (extract) {
                def prefix = "OSM_FILE_${OSMTools.Utilities.uuid()}"
                if (OSMTools.Loader.load(h2GIS, prefix, extract)) {
                    def transform
                    //Extract water
                    def tags = ["leisure": ["park"]]
                    def columns = ["leisure"]
                    String outputTableName = OSMTools.Transform.toPolygons(h2GIS, prefix, tags, columns)
                    assertNotNull(outputTableName)
                    h2GIS.getTable(outputTableName).save("/tmp/results.shp", true)

                }
            }
        }


    }
}
