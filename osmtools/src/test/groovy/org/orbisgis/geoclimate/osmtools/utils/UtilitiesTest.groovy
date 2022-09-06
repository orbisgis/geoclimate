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

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.geoclimate.osmtools.AbstractOSMTest
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
class UtilitiesTest extends AbstractOSMTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UtilitiesTest)

    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeOverPassQuery
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def getNominatimData
    /** Used to store method pointer in order to replace it for the tests to avoid call to Overpass servers. */
    private static def executeNominatimQuery

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#arrayToCoordinate(java.lang.Object)} method.
     */
    @Test
    void arrayToCoordinateTest(){
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
    void badArrayToCoordinateTest(){
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
    void parsePolygonTest(){
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
                Utilities.parsePolygon(poly1, new GeometryFactory()).toString()

        assertEquals "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 3, 2 2), (2 5, 8 5, 8 7, 2 5))",
                Utilities.parsePolygon(poly2, new GeometryFactory()).toString()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#parsePolygon(java.lang.Object, org.locationtech.jts.geom.GeometryFactory)}
     * method with bad data.
     */
    @Test
    void badParsePolygonTest(){
        def outer = []
        outer << [0.0, 0.0, 0.0]
        outer << [10.0, 0.0]
        outer << [10.0, 10.0, 0.0]
        outer << [0.0, 10.0]
        def poly1 = []
        poly1 << outer

        assertNull Utilities.parsePolygon(null, new GeometryFactory())
        assertNull Utilities.parsePolygon([], new GeometryFactory())
        assertNull Utilities.parsePolygon([[]], new GeometryFactory())
        assertNull Utilities.parsePolygon([[null]], new GeometryFactory())
        assertNull Utilities.parsePolygon(poly1, new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     * This test performs a web request to the Nominatim service.
     */
    @Test
    @Disabled
    void getExecuteNominatimQueryTest(){
        def path = RANDOM_PATH()
        def file = new File(path)
        assertTrue Utilities.executeNominatimQuery("vannes", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#executeNominatimQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    @Disabled
    void badGetExecuteNominatimQueryTest(){
        def file = new File(RANDOM_PATH())
        assertFalse Utilities.executeNominatimQuery(null, file)
        assertFalse Utilities.executeNominatimQuery("", file)
        assertFalse Utilities.executeNominatimQuery("query", file.getAbsolutePath())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method.
     */
    @Test
    void toBBoxTest(){
        def factory = new GeometryFactory()
        def point = factory.createPoint(new Coordinate(1.3, 7.7))
        Coordinate[] coordinates = [new Coordinate(2.0, 2.0),
                                    new Coordinate(4.0, 2.0),
                                    new Coordinate(4.0, 4.0),
                                    new Coordinate(2.0, 4.0),
                                    new Coordinate(2.0, 2.0)]
        def ring = factory.createLinearRing(coordinates)
        def polygon = factory.createPolygon(ring)

        assertEquals "(bbox:7.7,1.3,7.7,1.3)", Utilities.toBBox(point)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", Utilities.toBBox(ring)
        assertEquals "(bbox:2.0,2.0,4.0,4.0)", Utilities.toBBox(polygon)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toBBox(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToBBoxTest(){
        assertNull Utilities.toBBox(null)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toPoly(org.locationtech.jts.geom.Geometry)} method.
     */
    @Test
    void toPolyTest(){
        def factory = new GeometryFactory()
        Coordinate[] coordinates = [new Coordinate(2.0, 2.0),
                                    new Coordinate(4.0, 2.0),
                                    new Coordinate(4.0, 4.0),
                                    new Coordinate(2.0, 4.0),
                                    new Coordinate(2.0, 2.0)]
        def ring = factory.createLinearRing(coordinates)
        def poly = factory.createPolygon(ring)
        assertGStringEquals "(poly:\"2.0 2.0 2.0 4.0 4.0 4.0 4.0 2.0\")", Utilities.toPoly(poly)
        }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#toPoly(org.locationtech.jts.geom.Geometry)} method with bad data.
     */
    @Test
    void badToPolyTest(){
        def factory = new GeometryFactory()
        assertNull Utilities.toPoly(null)
        assertNull Utilities.toPoly(factory.createPoint(new Coordinate(0.0, 0.0)))
        assertNull Utilities.toPoly(factory.createPolygon())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement[])}
     * method.
     */
    @Test
    void buildOSMQueryFromEnvelopeTest(){
        def enveloppe = new Envelope(0.0, 2.3, 7.6, 8.9)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode[\"building\"];\n" +
                "\tnode[\"water\"];\n" +
                "\tway[\"building\"];\n" +
                "\tway[\"water\"];\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(enveloppe, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(enveloppe, ["building", "water"])
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(enveloppe, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(enveloppe, ["building", "water"], null)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "(\n" +
                "\tnode;\n" +
                "\tway;\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(enveloppe, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement[])}
     * method.
     */
    @Test
    void buildOSMQueryAllDataFromEnvelopeTest(){
        def enveloppe = new Envelope(0.0, 2.3, 7.6, 8.9)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "((\n" +
                "\tnode[\"building\"];\n" +
                "\tnode[\"water\"];\n" +
                "\tway[\"building\"];\n" +
                "\tway[\"water\"];\n" +
                ");\n" +
                ">;);\n" +
                "out;", Utilities.buildOSMQueryWithAllData(enveloppe, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "((\n" +
                ");\n" +
                ">;);\n" +
                "out;", Utilities.buildOSMQueryWithAllData(enveloppe, ["building", "water"])
        assertEquals "[bbox:7.6,0.0,8.9,2.3];\n" +
                "((\n" +
                "\tnode;\n" +
                "\tway;\n" +
                ");\n" +
                ">;);\n" +
                "out;", Utilities.buildOSMQueryWithAllData(enveloppe, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Envelope, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement[])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromEnvelopeTest(){
        assertNull Utilities.buildOSMQuery((Envelope)null, ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement[])}
     * method.
     */
    @Test
    void buildOSMQueryFromPolygonTest(){
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
                "out;", Utilities.buildOSMQuery(polygon, ["building", "water"], OSMElement.NODE, OSMElement.WAY)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(polygon, ["building", "water"])
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                ");\n" +
                "(._;>;);\n" +
                "out;", Utilities.buildOSMQuery(polygon, ["building", "water"], null)
        assertEquals "[bbox:2.3,0.0,8.9,7.6];\n" +
                "(\n" +
                "\tnode(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                "\tway(poly:\"2.3 0.0 2.3 7.6 8.9 7.6 8.9 0.0\");\n" +
                ");\n" +
                "out;", Utilities.buildOSMQuery(polygon, [], OSMElement.NODE, OSMElement.WAY)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildOSMQuery(org.locationtech.jts.geom.Polygon, java.lang.Object, org.orbisgis.geoclimate.osmtools.utils.OSMElement[])}
     * method with bad data.
     */
    @Test
    void badBuildOSMQueryFromPolygonTest(){
        assertNull Utilities.buildOSMQuery((Polygon)null, ["building"], OSMElement.NODE)
        assertNull Utilities.buildOSMQuery(new GeometryFactory().createPolygon(), ["building"], OSMElement.NODE)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#readJSONParameters(java.lang.String)} method.
     */
    @Test
    void readJSONParametersTest(){
        def map = [
                "tags" : [
                        "highway", "cycleway", "biclycle_road", "cyclestreet", "route", "junction"
                ],
                "columns":["width","highway", "surface", "sidewalk",
                        "lane","layer","maxspeed","oneway",
                        "h_ref","route","cycleway",
                        "biclycle_road","cyclestreet","junction"
                ]
        ]
        assertEquals map, Utilities.readJSONParameters(new File(UtilitiesTest.getResource("road_tags.json").toURI()).absolutePath)
        assertEquals map, Utilities.readJSONParameters(UtilitiesTest.getResourceAsStream("road_tags.json"))
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#readJSONParameters(java.lang.Object)} method with bad data.
     */
    @Test
    void badReadJSONParametersTest(){
        assertNull Utilities.readJSONParameters(null)
        assertNull Utilities.readJSONParameters("")
        assertNull Utilities.readJSONParameters("toto")
        assertNull Utilities.readJSONParameters("target")
        assertNull Utilities.readJSONParameters(new File(UtilitiesTest.getResource("bad_json_params.json").toURI()).absolutePath)
        assertNull Utilities.readJSONParameters(UtilitiesTest.getResourceAsStream("bad_json_params.json"))
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildGeometry(java.lang.Object)} method.
     */
    @Test
    void buildGeometryTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                Utilities.buildGeometry([-3.29109, 48.83535, -2.80357, 48.72223]).toString())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#buildGeometry(java.lang.Object)} method with bad data.
     */
    @Test
    void badBuildGeometryTest(){
        assertNull Utilities.buildGeometry([-3.29109, 48.83535, -2.80357])
        assertNull Utilities.buildGeometry([-Float.MAX_VALUE, 48.83535, -2.80357, 48.72223])
        assertNull Utilities.buildGeometry([-3.29109, Float.MAX_VALUE, -2.80357, 48.72223])
        assertNull Utilities.buildGeometry([-3.29109, 48.83535, -Float.MAX_VALUE, 48.72223])
        assertNull Utilities.buildGeometry([-3.29109, 48.83535, -2.80357, Float.MAX_VALUE])
        assertNull Utilities.buildGeometry(null)
        assertNull Utilities.buildGeometry()
        assertNull Utilities.buildGeometry(new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromNominatim(java.lang.Object)} method.
     */
    @Test
    void geometryFromNominatimTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                Utilities.geometryFromNominatim([48.83535, -3.29109, 48.72223, -2.80357]).toString())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromNominatim(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromNominatimTest(){
        assertNull Utilities.geometryFromNominatim([-3.29109, 48.83535, -2.80357])
        assertNull Utilities.geometryFromNominatim(null)
        assertNull Utilities.geometryFromNominatim()
        assertNull Utilities.geometryFromNominatim(new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromValues(java.lang.Object)} method.
     */
    @Test
    void geometryFromOverpassTest(){
        assertEquals("POLYGON ((-3.29109 48.72223, -3.29109 48.83535, -2.80357 48.83535, -2.80357 48.72223, -3.29109 48.72223))",
                Utilities.geometryFromValues([48.83535, -3.29109, 48.72223, -2.80357]).toString())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#geometryFromValues(java.lang.Object)} method with bad data.
     */
    @Test
    void badGeometryFromOverpassTest(){
        assertNull Utilities.geometryFromValues([-3.29109, 48.83535, -2.80357])
        assertNull Utilities.geometryFromValues(null)
        assertNull Utilities.geometryFromValues()
        assertNull Utilities.geometryFromValues(new GeometryFactory())
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#dropOSMTables(java.lang.String, org.orbisgis.data.jdbc.JdbcDataSource)}
     * method.
     */
    @Test
    void dropOSMTablesTest(){
        def ds = RANDOM_DS()
        ds.execute "CREATE TABLE prefix_node"
        ds.execute "CREATE TABLE prefix_node_member"
        ds.execute "CREATE TABLE prefix_node_tag"
        ds.execute "CREATE TABLE prefix_relation"
        ds.execute "CREATE TABLE prefix_relation_member"
        ds.execute "CREATE TABLE prefix_relation_tag"
        ds.execute "CREATE TABLE prefix_way"
        ds.execute "CREATE TABLE prefix_way_member"
        ds.execute "CREATE TABLE prefix_way_node"
        ds.execute "CREATE TABLE prefix_way_tag"
        assertTrue Utilities.dropOSMTables("prefix", ds)

        ds.execute "CREATE TABLE _node"
        ds.execute "CREATE TABLE _node_member"
        ds.execute "CREATE TABLE _node_tag"
        ds.execute "CREATE TABLE _relation"
        ds.execute "CREATE TABLE _relation_member"
        ds.execute "CREATE TABLE _relation_tag"
        ds.execute "CREATE TABLE _way"
        ds.execute "CREATE TABLE _way_member"
        ds.execute "CREATE TABLE _way_node"
        ds.execute "CREATE TABLE _way_tag"
        assertTrue Utilities.dropOSMTables("", ds)

    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#dropOSMTables(java.lang.String, org.orbisgis.data.jdbc.JdbcDataSource)}
     * method with bad data.
     */
    @Test
    void badDropOSMTablesTest(){
        def ds = RANDOM_DS()
        assertFalse Utilities.dropOSMTables("prefix", null)
        assertFalse Utilities.dropOSMTables(null, ds)
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#getNominatimData(java.lang.Object)} method.
     */
    @Test
    @Disabled
    void getNominatimDataTest(){
        def pattern = Pattern.compile("^POLYGON \\(\\((?>-?\\d+(?>\\.\\d+)? -?\\d+(?>\\.\\d+)?(?>, )?)*\\)\\)\$")
        def data = Utilities.getNominatimData("Paimpol")
        assertTrue pattern.matcher(data["geom"].toString()).matches()
        Envelope env = data["geom"].getEnvelopeInternal()
        assertEquals([env.getMinY(), env.getMinX(), env.getMaxY(), env.getMaxX()].toString(), data["bbox"].toString())
        assertEquals(data["extratags"]["ref:INSEE"], "22162")
        data = Utilities.getNominatimData("Boston")
        assertTrue pattern.matcher(data["geom"].toString()).matches()
        assertEquals(data["extratags"]["population"], "689326")
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.utils.Utilities#getNominatimData(java.lang.Object)} method with bad data.
     */
    @Test
    @Disabled
    void badGetNominatimDataTest() {
        assertNull Utilities.getNominatimData(null)
    }

    /**
     * Test the {@link Utilities#getServerStatus()} method.
     */
    @Test
    void getServerStatusTest(){
        def status = Utilities.getServerStatus()
        assertNotNull status
    }


    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method.
     */
    @Test
    @Disabled
    void executeOverPassQueryTest(){
        def file = new File("target/" + UUID.randomUUID().toString().replaceAll("-", "_"))
        assertTrue file.createNewFile()
        assertTrue Utilities.executeOverPassQuery("(node(51.249,7.148,51.251,7.152);<;);out meta;", file)
        assertTrue file.exists()
        assertFalse file.text.isEmpty()
    }

    /**
     * Test the {@link org.orbisgis.geoclimate.osmtools.OSMTools#executeOverPassQuery(java.lang.Object, java.lang.Object)} method with bad data.
     */
    @Test
    @Disabled
    void badExecuteOverPassQueryTest(){
        def file = new File("target/" + UUID.randomUUID().toString().replaceAll("-", "_"))
        assertTrue file.createNewFile()
        assertFalse Utilities.executeOverPassQuery(null, file)
        assertTrue file.text.isEmpty()
        assertFalse Utilities.executeOverPassQuery("query", null)
        assertTrue file.text.isEmpty()
    }
}
