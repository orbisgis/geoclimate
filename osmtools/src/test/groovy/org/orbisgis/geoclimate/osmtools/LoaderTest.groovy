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

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.GeometryFactory
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.osmtools.utils.OSMElement
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.*

/**
 * Test class for the processes in {@link Loader}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */

@Disabled
class LoaderTest extends AbstractOSMToolsTest {

    @TempDir
    static File folder

    private static final Logger LOGGER = LoggerFactory.getLogger(LoaderTest)

    static H2GIS ds


    @BeforeAll
    static void loadDb() {
        ds = H2GIS.open(folder.getAbsolutePath() + File.separator + "LoaderTest;AUTO_SERVER=TRUE;")
    }

    @BeforeEach
    final void beforeEach(TestInfo testInfo) {
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo) {
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /** Used to store the OSM request to ensure the good query is generated. */
    static def query

    /**
     * Override the 'executeOverPassQuery' methods to avoid the call to the server
     */
    void badOverpassQueryOverride() {
        Utilities.metaClass.static.executeOverPassQuery = { query, outputOSMFile ->
            LoaderTest.query = query
            return false
        }
    }


    /**
     * Test the OSMTools.Loader.fromArea() process with bad data.
     */
    @Test
    void badFromAreaTest() {
        assertThrows(Exception.class, () -> OSMTools.Loader.fromArea(ds, null))
        assertThrows(Exception.class, () -> OSMTools.Loader.fromArea(ds, "A string"))
    }

    /**
     * Test the OSMTools.Loader.fromArea() process.
     */
    @Test
    void fromAreaNoDistTest() {
        //With polygon
        Map r = OSMTools.Loader.fromArea(ds, [48.733493, -3.076869, 48.733995, -3.075829])

        assertFalse r.isEmpty()
        assertTrue r.containsKey("zone")
        assertTrue Pattern.compile("ZONE_$uuidRegex").matcher(r.zone as String).matches()
        assertTrue r.containsKey("envelope")
        assertTrue Pattern.compile("ZONE_ENVELOPE_$uuidRegex").matcher(r.envelope as String).matches()
        assertTrue r.containsKey("prefix")
        assertTrue Pattern.compile("OSM_DATA_$uuidRegex").matcher(r.prefix as String).matches()
        assertTrue r.containsKey("epsg")
        assertEquals 4326, r.epsg

        def zone = ds.getSpatialTable(r.zone)
        assertEquals 1, zone.rowCount
        assertEquals 1, zone.getColumnCount()
        assertTrue zone.getColumnNames().contains("THE_GEOM")
        zone.next()
        assertEquals "POLYGON ((-3.076869 48.733493, -3.076869 48.733995, -3.075829 48.733995, -3.075829 48.733493, -3.076869 48.733493))", zone.getGeometry(1).toText()

        def zoneEnv = ds.getSpatialTable(r.envelope)
        assertEquals 1, zoneEnv.rowCount
        assertEquals 1, zoneEnv.getColumnCount()
        assertTrue zoneEnv.getColumnNames().contains("THE_GEOM")
        zoneEnv.next()
        assertEquals "POLYGON ((-3.076869 48.733493, -3.076869 48.733995, -3.075829 48.733995, -3.075829 48.733493, -3.076869 48.733493))", zoneEnv.getGeometry(1).toText()

        def env = OSMTools.Utilities.geometryFromValues([48.733493, -3.076869, 48.733995, -3.075829]).getEnvelopeInternal()
        //With Envelope
        r = OSMTools.Loader.fromArea(ds, env)

        assertFalse r.isEmpty()
        assertTrue r.containsKey("zone")
        assertTrue Pattern.compile("ZONE_$uuidRegex").matcher(r.zone as String).matches()
        assertTrue r.containsKey("envelope")
        assertTrue Pattern.compile("ZONE_ENVELOPE_$uuidRegex").matcher(r.envelope as String).matches()
        assertTrue r.containsKey("prefix")
        assertTrue Pattern.compile("OSM_DATA_$uuidRegex").matcher(r.prefix as String).matches()
        assertTrue r.containsKey("epsg")
        assertEquals 4326, r.epsg

        zone = ds.getSpatialTable(r.zone)
        assertEquals 1, zone.rowCount
        assertEquals 1, zone.getColumnCount()
        assertTrue zone.getColumnNames().contains("THE_GEOM")
        zone.next()
        assertEquals "POLYGON ((-3.076869 48.733493, -3.076869 48.733995, -3.075829 48.733995, -3.075829 48.733493, -3.076869 48.733493))", zone.getGeometry(1).toText()

        zoneEnv = ds.getSpatialTable(r.envelope)
        assertEquals 1, zoneEnv.rowCount
        assertEquals 1, zoneEnv.getColumnCount()
        assertTrue zoneEnv.getColumnNames().contains("THE_GEOM")
        zoneEnv.next()
        assertEquals "POLYGON ((-3.076869 48.733493, -3.076869 48.733995, -3.075829 48.733995, -3.075829 48.733493, -3.076869 48.733493))", zoneEnv.getGeometry(1).toText()

    }

    /**
     * Test the OSMTools.Loader.fromArea() process.
     */
    @Test
    void fromAreaWithDistTest() {
        def geomFacto = new GeometryFactory()
        def dist = 1000

        def polygon = OSMTools.Utilities.geometryFromValues([48.790598, -3.084508, 48.791800, -3.082228])

        def env = polygon.getEnvelopeInternal()

        //With polygon
        Map r = OSMTools.Loader.fromArea(ds, polygon, dist)

        assertFalse r.isEmpty()
        assertTrue r.containsKey("zone")
        assertTrue Pattern.compile("ZONE_$uuidRegex").matcher(r.zone as String).matches()
        assertTrue r.containsKey("envelope")
        assertTrue Pattern.compile("ZONE_ENVELOPE_$uuidRegex").matcher(r.envelope as String).matches()
        assertTrue r.containsKey("prefix")
        assertTrue Pattern.compile("OSM_DATA_$uuidRegex").matcher(r.prefix as String).matches()

        def zone = ds.getSpatialTable(r.zone)
        assertEquals 1, zone.rowCount
        assertEquals 1, zone.getColumnCount()
        assertTrue zone.getColumnNames().contains("THE_GEOM")
        zone.next()
        assertEquals wktReader.read("POLYGON ((-3.084508 48.790598, -3.084508 48.7918, -3.082228 48.7918, -3.082228 48.790598, -3.084508 48.790598))"), zone.getGeometry(1)

        def zoneEnv = ds.getSpatialTable(r.envelope)
        assertEquals 1, zoneEnv.rowCount
        assertEquals 1, zoneEnv.getColumnCount()
        assertTrue zoneEnv.getColumnNames().contains("THE_GEOM")
        zoneEnv.next()
        assertEquals wktReader.read("POLYGON ((-3.0981436889553313 48.78161484715881, -3.0981436889553313 48.8007831528412, -3.068592311044669 48.8007831528412, -3.068592311044669 48.78161484715881, -3.0981436889553313 48.78161484715881))"), zoneEnv.getGeometry(1)


        //With envelope
        r = OSMTools.Loader.fromArea(ds, env, dist)

        assertFalse r.isEmpty()
        assertTrue r.containsKey("zone")
        assertTrue Pattern.compile("ZONE_$uuidRegex").matcher(r.zone as String).matches()
        assertTrue r.containsKey("envelope")
        assertTrue Pattern.compile("ZONE_ENVELOPE_$uuidRegex").matcher(r.envelope as String).matches()
        assertTrue r.containsKey("prefix")
        assertTrue Pattern.compile("OSM_DATA_$uuidRegex").matcher(r.prefix as String).matches()

        zone = ds.getSpatialTable(r.zone)
        assertEquals 1, zone.rowCount
        assertEquals 1, zone.getColumnCount()
        assertTrue zone.getColumnNames().contains("THE_GEOM")
        zone.next()
        assertEquals wktReader.read("POLYGON ((-3.084508 48.790598, -3.084508 48.7918, -3.082228 48.7918, -3.082228 48.790598, -3.084508 48.790598))"), zone.getGeometry(1)

        zoneEnv = ds.getSpatialTable(r.envelope)
        assertEquals 1, zoneEnv.rowCount
        assertEquals 1, zoneEnv.getColumnCount()
        assertTrue zoneEnv.getColumnNames().contains("THE_GEOM")
        zoneEnv.next()
        assertEquals wktReader.read("POLYGON ((-3.0981436889553313 48.78161484715881, -3.0981436889553313 48.8007831528412, -3.068592311044669 48.8007831528412, -3.068592311044669 48.78161484715881, -3.0981436889553313 48.78161484715881))"), zoneEnv.getGeometry(1)
    }

    /**
     * Test the OSMTools.Loader.fromPlace() process.
     */
    @Test
    void fromPlaceNoDistTest() {
        if (OSMTools.Utilities.isNominatimReady()) {
            def placeName = "Lezoen, Plourivo"
            def formattedPlaceName = "Lezoen_Plourivo_"
            Map r = OSMTools.Loader.fromPlace(ds, placeName)
            assertFalse r.isEmpty()
            assertTrue r.containsKey("zone")
            assertTrue r.containsKey("envelope")
            assertTrue r.containsKey("prefix")
            assertTrue Pattern.compile("OSM_DATA_$formattedPlaceName$uuidRegex").matcher(r.prefix as String).matches()

            def zone = ds.getSpatialTable(r.zone)
            assertEquals 1, zone.rowCount
            assertEquals 2, zone.getColumnCount()
            def columns = zone.getColumnNames()
            assertTrue columns.contains("THE_GEOM")
            assertTrue columns.contains("ID_ZONE")
            zone.next()
            assertNotNull zone.getGeometry(1)

            def zoneEnv = ds.getSpatialTable(r.envelope)
            assertEquals 1, zoneEnv.rowCount
            assertEquals 2, zoneEnv.getColumnCount()
            columns = zone.getColumnNames()
            assertTrue columns.contains("THE_GEOM")
            assertTrue columns.contains("ID_ZONE")
            zoneEnv.next()
            assertEquals "POLYGON ((-3.0790622 48.7298266, -3.0790622 48.7367393, -3.0739517 48.7367393, -3.0739517 48.7298266, -3.0790622 48.7298266))", zoneEnv.getGeometry(1).toText()
            assertEquals "Lezoen, Plourivo", zoneEnv.getString(2)
        }
    }

    /**
     * Test the OSMTools.Loader.fromPlace() process.
     */
    @Test
    void fromPlaceWithDistTest() {
        def placeName = "  The place Name -toFind  "
        def dist = 5
        def formattedPlaceName = "The_place_Name_toFind_"
        assertThrows(Exception.class, () -> OSMTools.Loader.fromPlace(ds, placeName, dist))

        def r = OSMTools.Loader.fromPlace(ds, "Lezoen, Plourivo", dist)

        def zone = ds.getSpatialTable(r.zone)
        assertEquals 1, zone.rowCount
        assertEquals 2, zone.getColumnCount()
        def columns = zone.getColumnNames()
        assertTrue columns.contains("THE_GEOM")
        assertTrue columns.contains("ID_ZONE")
        zone.next()
        assertNotNull zone.getGeometry(1)
        def zoneEnv = ds.getSpatialTable(r.envelope)
        assertEquals 1, zoneEnv.rowCount
        assertEquals 2, zoneEnv.getColumnCount()
        columns = zone.getColumnNames()
        assertTrue columns.contains("THE_GEOM")
        assertTrue columns.contains("ID_ZONE")
        zoneEnv.next()
        assertEquals "POLYGON ((-3.079130303738262 48.729781684235796, -3.079130303738262 48.73678421576421, -3.073883596261738 48.73678421576421, -3.073883596261738 48.729781684235796, -3.079130303738262 48.729781684235796))", zoneEnv.getGeometry(1).toText()
        assertEquals "Lezoen, Plourivo", zoneEnv.getString(2)
    }

    /**
     * Test the OSMTools.Loader.fromPlace() process with bad data.
     */
    @Test
    void badFromPlaceTest() {
        def placeName = "  The place Name -toFind  "
        def dist = -5
        assertThrows(Exception.class, () -> OSMTools.Loader.fromPlace(ds, placeName, dist))
        assertThrows(Exception.class, () -> OSMTools.Loader.fromPlace(ds, placeName, -1))
        assertThrows(Exception.class, () -> OSMTools.Loader.fromPlace(ds, null))
        assertThrows(Exception.class, () -> OSMTools.Loader.fromPlace(null, placeName))
    }

    /**
     * Test the OSMTools.Loader.extract() process.
     */
    @Test
    void extractTest() {
        def env = OSMTools.Utilities.geometryFromValues([48.733493, -3.076869, 48.733995, -3.075829]).getEnvelopeInternal()
        def query = "[maxsize:1073741824]" + OSMTools.Utilities.buildOSMQuery(env, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
        def extract = OSMTools.Loader.extract(query)
        assertNotNull extract
        def file = new File(extract)
        assertTrue file.exists()
    }

    /**
     * Test the OSMTools.Loader.extract() process with bad data
     */
    @Test
    void badExtractTest() {
        assertThrows(Exception.class, () -> OSMTools.Loader.extract(null))
        badOverpassQueryOverride()
        assertThrows(Exception.class, () -> OSMTools.Loader.extract("toto"))
    }

    /**
     * Test the OSMTools.Loader.load() process with bad data.
     */
    @Test
    void badLoadTest() {
        def url = LoaderTest.getResource("sample.osm")
        assertNotNull url
        def osmFile = new File(url.toURI())
        assertTrue osmFile.exists()
        assertTrue osmFile.isFile()
        def prefix = uuid().toUpperCase()

        //Null dataSource
        assertThrows(Exception.class, () -> OSMTools.Loader.load(null, prefix, osmFile.absolutePath))

        //Null prefix
        assertThrows(Exception.class, () -> OSMTools.Loader.load(ds, null, osmFile.absolutePath))
        //Bad prefix
        assertThrows(Exception.class, () -> OSMTools.Loader.load(ds, "(╯°□°）╯︵ ┻━┻", osmFile.absolutePath))

        //Null path
        assertThrows(Exception.class, () -> OSMTools.Loader.load(ds, prefix, null))
        //Unexisting path
        assertThrows(Exception.class, () -> OSMTools.Loader.load(ds, prefix, "ᕕ(ᐛ)ᕗ"))
    }

    /**
     * Test the OSMTools.Loader.load() process.
     */
    @Test
    void loadTest() {
        def url = LoaderTest.getResource("sample.osm")
        assertNotNull url
        def osmFile = new File(url.toURI())
        assertTrue osmFile.exists()
        assertTrue osmFile.isFile()
        def prefix = "OSM_" + uuid().toUpperCase()

        assertTrue OSMTools.Loader.load(ds, prefix, osmFile.absolutePath)

        //Test on DataSource
        def tableArray = ["${prefix}_NODE", "${prefix}_NODE_MEMBER", "${prefix}_NODE_TAG",
                          "${prefix}_WAY", "${prefix}_WAY_MEMBER", "${prefix}_WAY_TAG", "${prefix}_WAY_NODE",
                          "${prefix}_RELATION", "${prefix}_RELATION_MEMBER", "${prefix}_RELATION_TAG"] as String[]
        tableArray.each { name ->
            assertNotNull ds.getTable(name), "The table named $name is not in the datasource"
        }

        //NODE
        //Test on NODE table
        def nodeTable = ds.getTable(tableArray[0])
        assertNotNull nodeTable
        assertEquals 5, nodeTable.rowCount
        def arrayNode = ["ID_NODE", "THE_GEOM", "ELE", "USER_NAME", "UID", "VISIBLE", "VERSION", "CHANGESET",
                         "LAST_UPDATE", "NAME"] as String[]
        assertArrayEquals(arrayNode, nodeTable.getColumnNames() as String[])
        nodeTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "256001", row.ID_NODE.toString()
                    assertEquals "POINT (32.8545692 57.0465758)", row.THE_GEOM.toString()
                    assertNull row.ELE
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    //assertEquals "2012-01-10T23:02:55Z", row.LAST_UPDATE.toString()
                    assertEquals "2012-01-10 00:00:00.0", row.LAST_UPDATE.toString()
                    assertEquals "", row.NAME.toString()
                    break
                case 2:
                    assertEquals "256002", row.ID_NODE.toString()
                    assertEquals "POINT (32.8645692 57.0565758)", row.THE_GEOM.toString()
                    assertNull row.ELE
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    //assertEquals "2012-01-10T23:02:55Z", row.LAST_UPDATE
                    assertEquals "2012-01-10 00:00:00.0", row.LAST_UPDATE.toString()
                    assertEquals "", row.NAME.toString()
                    break
                case 3:
                    assertEquals "256003", row.ID_NODE.toString()
                    assertEquals "POINT (32.8745692 57.0665758)", row.THE_GEOM.toString()
                    assertNull row.ELE
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    //assertEquals "2012-01-10T23:02:55Z", row.LAST_UPDATE.toString()
                    assertEquals "2012-01-10 00:00:00.0", row.LAST_UPDATE.toString()
                    assertEquals "", row.NAME.toString()
                    break
                case 4:
                    assertEquals "256004", row.ID_NODE.toString()
                    assertEquals "POINT (32.8845692 57.0765758)", row.THE_GEOM.toString()
                    assertNull row.ELE
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    //assertEquals "2012-01-10T23:02:55Z", row.LAST_UPDATE.toString()
                    assertEquals "2012-01-10 00:00:00.0", row.LAST_UPDATE.toString()
                    assertEquals "Just a house node", row.NAME.toString()
                    break
                case 5:
                    assertEquals "256005", row.ID_NODE.toString()
                    assertEquals "POINT (32.8945692 57.0865758)", row.THE_GEOM.toString()
                    assertNull row.ELE
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    //assertEquals "2012-01-10T23:02:55Z", row.LAST_UPDATE.toString()
                    assertEquals "2012-01-10 00:00:00.0", row.LAST_UPDATE.toString()
                    assertEquals "Just a tree", row.NAME.toString()
                    break
                default:
                    fail()
            }
        }

        //Test on NODE_MEMBER table
        def nodeMemberTable = ds.getTable(tableArray[1])
        assertNotNull nodeMemberTable
        assertEquals 2, nodeMemberTable.rowCount
        def arrayNodeMember = ["ID_RELATION", "ID_NODE", "ROLE", "NODE_ORDER"] as String[]
        assertArrayEquals(arrayNodeMember, nodeMemberTable.getColumnNames() as String[])
        nodeMemberTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "259001", row.ID_RELATION.toString()
                    assertEquals "256004", row.ID_NODE.toString()
                    assertEquals "center", row.ROLE.toString()
                    assertEquals "2", row.NODE_ORDER.toString()
                    break
                case 2:
                    assertEquals "259001", row.ID_RELATION.toString()
                    assertEquals "256005", row.ID_NODE.toString()
                    assertEquals "barycenter", row.ROLE.toString()
                    assertEquals "3", row.NODE_ORDER.toString()
                    break
                default:
                    fail()
            }
        }

        //Test on NODE_TAG table
        def nodeTagTable = ds.getTable(tableArray[2])
        assertNotNull nodeTagTable
        assertEquals 2, nodeTagTable.rowCount
        def arrayNodeTag = ["ID_NODE", "TAG_KEY", "TAG_VALUE"] as String[]
        assertArrayEquals(arrayNodeTag, nodeTagTable.getColumnNames() as String[])
        nodeTagTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "256004", row.ID_NODE.toString()
                    assertEquals "building", row.TAG_KEY.toString()
                    assertEquals "house", row.TAG_VALUE.toString()
                    break
                case 2:
                    assertEquals "256005", row.ID_NODE.toString()
                    assertEquals "natural", row.TAG_KEY.toString()
                    assertEquals "tree", row.TAG_VALUE.toString()
                    break
                default:
                    fail()
            }
        }

        //WAY
        //Test on WAY table
        def wayTable = ds.getTable(tableArray[3])
        assertNotNull wayTable
        assertEquals 1, wayTable.rowCount
        def arrayWay = ["ID_WAY", "USER_NAME", "UID", "VISIBLE", "VERSION", "CHANGESET",
                        "LAST_UPDATE", "NAME"] as String[]
        assertArrayEquals(arrayWay, wayTable.getColumnNames() as String[])
        wayTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "258001", row.ID_WAY.toString()
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    assertEquals "2012-01-10 23:02:55.0", row.LAST_UPDATE.toString()
                    assertEquals "", row.NAME.toString()
                    break
                default:
                    fail()
            }
        }

        //Test on WAY_MEMBER table
        def wayMemberTable = ds.getTable(tableArray[4])
        assertNotNull wayMemberTable
        assertEquals 1, wayMemberTable.rowCount
        def arrayWayMember = ["ID_RELATION", "ID_WAY", "ROLE", "WAY_ORDER"] as String[]
        assertArrayEquals(arrayWayMember, wayMemberTable.getColumnNames() as String[])
        wayMemberTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "259001", row.ID_RELATION.toString()
                    assertEquals "258001", row.ID_WAY.toString()
                    assertEquals "outer", row.ROLE.toString()
                    assertEquals "1", row.WAY_ORDER.toString()
                    break
                default:
                    fail()
            }
        }

        //Test on WAY_TAG table
        def wayTagTable = ds.getTable(tableArray[5])
        assertNotNull wayTagTable
        assertEquals 1, wayTagTable.rowCount
        def arrayWayTag = ["ID_WAY", "TAG_KEY", "TAG_VALUE"] as String[]
        assertArrayEquals(arrayWayTag, wayTagTable.getColumnNames() as String[])
        wayTagTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "258001", row.ID_WAY.toString()
                    assertEquals "highway", row.TAG_KEY.toString()
                    assertEquals "primary", row.TAG_VALUE.toString()
                    break
                default:
                    fail()
            }
        }

        //Test on WAY_NODE table
        def wayNodeTable = ds.getTable(tableArray[6])
        assertNotNull wayNodeTable
        assertEquals 3, wayNodeTable.rowCount
        def arrayWayNode = ["ID_WAY", "ID_NODE", "NODE_ORDER"] as String[]
        assertArrayEquals(arrayWayNode, wayNodeTable.getColumnNames() as String[])
        wayNodeTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "258001", row.ID_WAY.toString()
                    assertEquals "256001", row.ID_NODE.toString()
                    assertEquals "1", row.NODE_ORDER.toString()
                    break
                case 2:
                    assertEquals "258001", row.ID_WAY.toString()
                    assertEquals "256002", row.ID_NODE.toString()
                    assertEquals "2", row.NODE_ORDER.toString()
                    break
                case 3:
                    assertEquals "258001", row.ID_WAY.toString()
                    assertEquals "256003", row.ID_NODE.toString()
                    assertEquals "3", row.NODE_ORDER.toString()
                    break
                default:
                    fail()
            }
        }

        //RELATION
        //Test on RELATION table
        def relationTable = ds.getTable(tableArray[7])
        assertNotNull relationTable
        assertEquals 1, relationTable.rowCount
        def arrayRelation = ["ID_RELATION", "USER_NAME", "UID", "VISIBLE", "VERSION", "CHANGESET",
                             "LAST_UPDATE"] as String[]
        assertArrayEquals(arrayRelation, relationTable.getColumnNames() as String[])
        relationTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "259001", row.ID_RELATION.toString()
                    assertEquals "UserTest", row.USER_NAME.toString()
                    assertEquals "5001", row.UID.toString()
                    assertEquals "true", row.VISIBLE.toString()
                    assertEquals "1", row.VERSION.toString()
                    assertEquals "6001", row.CHANGESET.toString()
                    assertEquals "2012-01-10 23:02:55.0", row.LAST_UPDATE.toString()
                    break
                default:
                    fail()
            }
        }

        //Test on RELATION_MEMBER table
        def relationMemberTable = ds.getTable(tableArray[8])
        assertNotNull relationMemberTable
        assertEquals 0, relationMemberTable.rowCount
        def arrayRelationMember = ["ID_RELATION", "ID_SUB_RELATION", "ROLE", "RELATION_ORDER"] as String[]
        assertArrayEquals(arrayRelationMember, relationMemberTable.getColumnNames() as String[])

        //Test on RELATION_TAG table
        def relationTagTable = ds.getTable(tableArray[9])
        assertNotNull relationTagTable
        assertEquals 2, relationTagTable.rowCount
        def arrayRelationTag = ["ID_RELATION", "TAG_KEY", "TAG_VALUE"] as String[]
        assertArrayEquals(arrayRelationTag, relationTagTable.getColumnNames() as String[])
        relationTagTable.eachRow { row ->
            switch (row.row) {
                case 1:
                    assertEquals "259001", row.ID_RELATION.toString()
                    assertEquals "ref", row.TAG_KEY.toString()
                    assertEquals "123456", row.TAG_VALUE.toString()
                    break
                case 2:
                    assertEquals "259001", row.ID_RELATION.toString()
                    assertEquals "route", row.TAG_KEY.toString()
                    assertEquals "bus", row.TAG_VALUE.toString()
                    break
                default:
                    fail()
            }
        }
    }

    /**
     * For debug purpose
     */
    @Disabled
    @Test
    void forDebug() {
        println(OSMTools.Utilities.getNominatimData("Sassenage"))
    }
}
