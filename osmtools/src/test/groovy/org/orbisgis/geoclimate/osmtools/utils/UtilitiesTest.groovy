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
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.osmtools.AbstractOSMToolsTest
import org.orbisgis.geoclimate.osmtools.OSMTools
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for {@link org.orbisgis.geoclimate.osmtools.utils.Utilities}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019-2020)
 */
class UtilitiesTest extends AbstractOSMToolsTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    static File folder

    private static final Logger LOGGER = LoggerFactory.getLogger(UtilitiesTest)

    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeOverPassQuery
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def getNominatimData
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeNominatimQuery


    static H2GIS h2gis

    @BeforeAll
    static void beforeAll() {
        h2gis = H2GIS.open(folder.getAbsolutePath() + File.separator + "UtilitiesTest;AUTO_SERVER=TRUE;")
    }


    @BeforeEach
    final void beforeEach(TestInfo testInfo) {
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo) {
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    def RANDOM_PATH() { return folder.getAbsolutePath() + File.separator + uuid() }
    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#arrayToCoordinate(java.lang.Object)} method.
     */
    @Test
    void arrayToCoordinateTest() {
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        outer << [0.0, 0.0, 0.0]

        def coordinates = Utilities.arrayToCoordinate(outer)
        assertEquals 5, coordinates.size()
        assertEquals "(0.0, 0.0, 0.0)", coordinates[0].toString()
        assertEquals "(10.0, 0.0, NaN)", coordinates[1].toString()
        assertEquals "(10.0, 10.0, 0.0)", coordinates[2].toString()
        assertEquals "(0.0, 10.0, NaN)", coordinates[3].toString()
        assertEquals "(0.0, 0.0, 0.0)", coordinates[4].toString()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#arrayToCoordinate(java.lang.Object)} method with bad data.
     */
    @Test
    void badArrayToCoordinateTest() {
        def array1 = Utilities.arrayToCoordinate(null)
        assertNotNull array1
        assertEquals 0, array1.length

        def array2 = Utilities.arrayToCoordinate([])
        assertNotNull array2
        assertEquals 0, array2.length

        def array3 = Utilities.arrayToCoordinate([[0]])
        assertNotNull array3
        assertEquals 0, array3.length

        def array4 = Utilities.arrayToCoordinate([[0, 1, 2, 3]])
        assertNotNull array4
        assertEquals 0, array4.length
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#parsePolygon(java.lang.Object, org.locationtech.jts.geom.GeometryFactory)}
     * method.
     */
    @Test
    void parsePolygonTest() {
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        outer << [0.0, 0.0, 0.0]

        def hole1 = []
        hole1 << [2.0, 2.0, 0.0]
        hole1 << [8.0, 2.0]
        hole1 << [8.0, 3.0]
        hole1 << [2.0, 2.0, 0.0]
        def hole2 = []
        hole2 << [2.0, 5.0, 0.0]
        hole2 << [8.0, 5.0]
        hole2 << [8.0, 7.0]
        hole2 << [2.0, 5.0, 0.0]

        def poly1 = []
        poly1 << outer

        def poly2 = []
        poly2 << outer
        poly2 << hole1
        poly2 << hole2

        assertEquals "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))",
                OSMTools.Utilities.parsePolygon(poly1, new GeometryFactory()).toString()

        assertEquals "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 3, 2 2), (2 5, 8 5, 8 7, 2 5))",
                OSMTools.Utilities.parsePolygon(poly2, new GeometryFactory()).toString()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#parsePolygon(java.lang.Object, org.locationtech.jts.geom.GeometryFactory)}
     * method with bad data.
     */
    @Test
    void badParsePolygonTest() {
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        def poly1 = []
        poly1 << outer

        assertNull OSMTools.Utilities.parsePolygon(null, new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon([], new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon([[]], new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon([[null]], new GeometryFactory())
        assertNull OSMTools.Utilities.parsePolygon(poly1, new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     * This test performs a web request to the Nominatim service.
     */
    @Test
    @Disabled
    void getExecuteNominatimQueryTest() {
        def path = RANDOM_PATH()
        def file = new File(path)
        assertTrue OSMTools.Utilities.executeNominatimQuery("vannes", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    @Disabled
    void badGetExecuteNominatimQueryTest() {
        def file = new File(RANDOM_PATH())
        assertFalse OSMTools.Utilities.executeNominatimQuery(null, file)
        assertFalse OSMTools.Utilities.executeNominatimQuery("", file)
        assertFalse OSMTools.Utilities.executeNominatimQuery("query", file.getAbsolutePath())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method.
     */
    @Test
    void toBBoxTest() {
        def factory = new GeometryFactory()
        def point = factory.createPoint(new Coordinate(1.3, 7.7))
        Coordinate[] coordinates = [new Coordinate(2.0, 2.0),
                                    new Coordinate(4.0, 2.0),
                                    new Coordinate(4.0, 4.0),
                                    new Coordinate(2.0, 4.0),
                                    new Coordinate(2.0, 2.0)]
        def ring = factory.createLinearRing(coordinates)
        def polygon = factory.createPolygon(ring)

        assertEquals "(bbox:7.7,1.3,7.7,1.3)", OSMTools.Utilities.toBBox(point)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", OSMTools.Utilities.toBBox(ring)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", OSMTools.Utilities.toBBox(polygon)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToBBoxTest() {
        assertNull OSMTools.Utilities.toBBox(null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toPoly(org.locationtech.jts.geom.Geometry)} method.
     */
    @Test
    void toPolyTest() {
        def factory = new GeometryFactory()
        Coordinate[] coordinates = [new Coordinate(2.0, 2.0),
                                    new Coordinate(4.0, 2.0),
                                    new Coordinate(4.0, 4.0),
                                    new Coordinate(2.0, 4.0),
                                    new Coordinate(2.0, 2.0)]
        def ring = factory.createLinearRing(coordinates)
        def poly = factory.createPolygon(ring)
        assertGStringEquals "(poly:\"2.0 2.0 2.0 4.0 4.0 4.0 4.0 2.0\")", OSMTools.Utilities.toPoly(poly)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toPoly(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToPolyTest() {
        def factory = new GeometryFactory()
        assertNull OSMTools.Utilities.toPoly(null)
        assertNull OSMTools.Utilities.toPoly(factory.createPoint(new Coordinate(0.0, 0.0)))
        assertNull OSMTools.Utilities.toPoly(factory.createPolygon())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement [ ])}
     * method.
     */
    @Test
    void buildOSMQueryFromEnvelopeTest() {
        def enveloppe = new Envelope(0.0, 2.3, 7.6, 8.9)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode[\"building\"];\n" +
                "\tnode[\"water\"];\n" +
                "\tway[\"building\"];\n" +
                "\tway[\"water\"];\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode[\"building\"];\n" +
                "\tway[\"building\"];\n" +
                "\trelation[\"building\"];\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building"])
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode;\n" +
                "\tway;\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(enveloppe, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement [ ])}
     * method.
     */
    @Test
    void buildOSMQueryAllDataFromEnvelopeTest() {
        def enveloppe = new Envelope(0.0, 2.3, 7.6, 8.9)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "((\n" +
                "\tnode[\"building\"];\n" +
                "\tnode[\"water\"];\n" +
                "\tway[\"building\"];\n" +
                "\tway[\"water\"];\n" +
                ");\n" +
                ">;);\n" +
                "out;", OSMTools.Utilities.buildOSMQueryWithAllData(enveloppe, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "((\n" +
                ");\n" +
                ">;);\n" +
                "out;", OSMTools.Utilities.buildOSMQueryWithAllData(enveloppe, ["building", "water"])
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "((\n" +
                "\tnode;\n" +
                "\tway;\n" +
                ");\n" +
                ">;);\n" +
                "out;", OSMTools.Utilities.buildOSMQueryWithAllData(enveloppe, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement [ ])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromEnvelopeTest() {
        assertNull OSMTools.Utilities.buildOSMQuery((Envelope) null, ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement [ ])}
     * method.
     */
    @Test
    void buildOSMQueryFromPolygonTest() {
        def factory = new GeometryFactory();
        Coordinate[] coordinates = [
                new Coordinate(0.0, 2.3),
                new Coordinate(7.6, 2.3),
                new Coordinate(7.6, 8.9),
                new Coordinate(0.0, 8.9),
                new Coordinate(0.0, 2.3)
        ]
        def ring = factory.createLinearRing(coordinates)
        def polygon = factory.createPolygon(ring)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tnode[\"water\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway[\"water\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\trelation[\"building\"](poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building"])
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "out;", OSMTools.Utilities.buildOSMQuery(polygon, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement [ ])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromPolygonTest() {
        assertNull OSMTools.Utilities.buildOSMQuery((Polygon) null, ["building"], OSMElement.NODE)
        assertNull OSMTools.Utilities.buildOSMQuery(new GeometryFactory().createPolygon(), ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#readJSONParameters(java.lang.String)} method.
     */
    @Test
    void readJSONParametersTest() {
        def map = [
                "tags"   : [
                        "highway", "cycleway", "biclycle_road", "cyclestreet", "route", "junction"
                ],
                "columns": ["width", "highway", "surface", "sidewalk",
                            "lane", "layer", "maxspeed", "oneway",
                            "h_ref", "route", "cycleway",
                            "biclycle_road", "cyclestreet", "junction"
                ]
        ]
        assertEquals map, OSMTools.Utilities.readJSONParameters(new File(UtilitiesTest.getResource("road_tags.json").toURI()).absolutePath)
        assertEquals map, OSMTools.Utilities.readJSONParameters(UtilitiesTest.getResourceAsStream("road_tags.json"))
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#readJSONParameters(java.lang.Object)} method with bad data.
     */
    @Test
    void badReadJSONParametersTest() {
        assertNull OSMTools.Utilities.readJSONParameters(null)
        assertNull OSMTools.Utilities.readJSONParameters("")
        assertNull OSMTools.Utilities.readJSONParameters("toto")
        assertNull OSMTools.Utilities.readJSONParameters("target")
        assertNull OSMTools.Utilities.readJSONParameters(new File(UtilitiesTest.getResource("bad_json_params.json").toURI()).absolutePath)
        assertNull OSMTools.Utilities.readJSONParameters(UtilitiesTest.getResourceAsStream("bad_json_params.json"))
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildGeometry(java.lang.Object)} method.
     */
    @Test
    void buildGeometryTest() {
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OSMTools.Utilities.buildGeometry([-3.29109, 48.83535, -2.80357, 48.72223]).toString())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildGeometry(java.lang.Object)} method with bad data.
     */
    @Test
    void badBuildGeometryTest() {
        assertNull OSMTools.Utilities.buildGeometry([-3.29109, 48.83535, -2.80357])
        assertNull OSMTools.Utilities.buildGeometry([-Float.MAX_VALUE, 48.83535, -2.80357, 48.72223])
        assertNull OSMTools.Utilities.buildGeometry([-3.29109, Float.MAX_VALUE, -2.80357, 48.72223])
        assertNull OSMTools.Utilities.buildGeometry([-3.29109, 48.83535, -Float.MAX_VALUE, 48.72223])
        assertNull OSMTools.Utilities.buildGeometry([-3.29109, 48.83535, -2.80357, Float.MAX_VALUE])
        assertNull OSMTools.Utilities.buildGeometry(null)
        assertNull OSMTools.Utilities.buildGeometry()
        assertNull OSMTools.Utilities.buildGeometry(new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromNominatim(java.lang.Object)} method.
     */
    @Test
    void geometryFromNominatimTest() {
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OSMTools.Utilities.geometryFromNominatim([48.83535, -3.29109, 48.72223, -2.80357]).toString())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromNominatim(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromNominatimTest() {
        assertNull OSMTools.Utilities.geometryFromNominatim([-3.29109, 48.83535, -2.80357])
        assertNull OSMTools.Utilities.geometryFromNominatim(null)
        assertNull OSMTools.Utilities.geometryFromNominatim()
        assertNull OSMTools.Utilities.geometryFromNominatim(new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromValues(java.lang.Object)} method.
     */
    @Test
    void geometryFromOverpassTest() {
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                OSMTools.Utilities.geometryFromValues([48.83535, -3.29109, 48.72223, -2.80357]).toString())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromValues(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromOverpassTest() {
        assertNull OSMTools.Utilities.geometryFromValues([-3.29109, 48.83535, -2.80357])
        assertNull OSMTools.Utilities.geometryFromValues(null)
        assertNull OSMTools.Utilities.geometryFromValues()
        assertNull OSMTools.Utilities.geometryFromValues(new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#dropOSMTables(java.lang.String, org.orbisgis.data.jdbc.JdbcDataSource)}
     * method.
     */
    @Test
    void dropOSMTablesTest() {
        h2gis.execute """CREATE TABLE prefix_node;
        CREATE TABLE prefix_node_member;
        CREATE TABLE prefix_node_tag;
        CREATE TABLE prefix_relation;
        CREATE TABLE prefix_relation_member;
        CREATE TABLE prefix_relation_tag;
        CREATE TABLE prefix_way;
        CREATE TABLE prefix_way_member;
        CREATE TABLE prefix_way_node;
        CREATE TABLE prefix_way_tag""".toString()
        assertTrue OSMTools.Utilities.dropOSMTables("prefix", h2gis)

        h2gis.execute """CREATE TABLE _node;
        CREATE TABLE _node_member;
        CREATE TABLE _node_tag;
        CREATE TABLE _relation;
        CREATE TABLE _relation_member;
        CREATE TABLE _relation_tag;
        CREATE TABLE _way;
        CREATE TABLE _way_member;
        CREATE TABLE _way_node;
        CREATE TABLE _way_tag""".toString()
        assertTrue OSMTools.Utilities.dropOSMTables("", h2gis)

    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#dropOSMTables(java.lang.String, org.orbisgis.data.jdbc.JdbcDataSource)}
     * method with bad data.
     */
    @Test
    void badDropOSMTablesTest() {
        assertFalse OSMTools.Utilities.dropOSMTables("prefix", null)
        assertFalse OSMTools.Utilities.dropOSMTables(null, h2gis)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#getNominatimData(java.lang.Object)} method.
     */
    @Test
    @Disabled
    void getNominatimDataTest() {
        def pattern = Pattern.compile("^POLYGON \\(\\((?>-?\\d+(?>\\.\\d+)? -?\\d+(?>\\.\\d+)?(?>, )?)*\\)\\)\$")
        def data = OSMTools.Utilities.getNominatimData("Paimpol")
        assertTrue pattern.matcher(data["geom"].toString()).matches()
        Envelope env = data["geom"].getEnvelopeInternal()
        assertEquals([env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()].toString(), data["bbox"].toString())
        assertEquals(data["extratags"]["ref:INSEE"], "22162")
        data = OSMTools.Utilities.getNominatimData("Boston")
        assertTrue pattern.matcher(data["geom"].toString()).matches()
        assertEquals(data["extratags"]["population"], "689326")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#getNominatimData(java.lang.Object)} method with bad data.
     */
    @Test
    @Disabled
    void badGetNominatimDataTest() {
        assertNull OSMTools.Utilities.getNominatimData(null)
    }

    /**
     * Test the {@link Utilities#getServerStatus()} method.
     */
    @Test
    void getServerStatusTest() {
        def status = OSMTools.Utilities.getServerStatus()
        assertNotNull status
    }


    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    @Disabled
    void executeOverPassQueryTest() {
        def file = new File("target/" + UUID.randomUUID().toString().replaceAll("-", "_"))
        assertTrue file.createNewFile()
        assertTrue OSMTools.Utilities.executeOverPassQuery("(node(51.249,7.148,51.251,7.152);<;);out meta;", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method with bad data.
     */
    @Test
    @Disabled
    void badExecuteOverPassQueryTest() {
        def file = new File("target/" + UUID.randomUUID().toString().replaceAll("-", "_"))
        assertTrue file.createNewFile()
        assertFalse OSMTools.Utilities.executeOverPassQuery(null, file)
        assertTrue file.text.isEmpty()
        assertFalse OSMTools.Utilities.executeOverPassQuery("query", null)
        assertTrue file.text.isEmpty()
    }
}
