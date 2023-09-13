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
package org.orbisgis.geoclimate.worldpoptools

import ch.qos.logback.classic.Logger
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.functions.io.asc.AscReaderDriver
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.utils.LoggerUtils

import static org.junit.jupiter.api.Assertions.*

class WorldPopExtractTest {

    @TempDir
    static File folder

    private static final Logger LOGGER = LoggerUtils.createLogger(WorldPopExtractTest)
    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "WorldPopExtractTest;AUTO_SERVER=TRUE;")
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
     * Test to extract a grid as an ascii file and load it in H2GIS
     */
    @Test
    void extractGrid() {
        def outputGridFile = new File(folder, "extractGrid.asc")
        if (outputGridFile.exists()) {
            outputGridFile.delete()
        }
        def coverageId = "wpGlobal:ppp_2018"
        def bbox = [47.63324, -2.78087, 47.65749, -2.75979]
        if (WorldPopTools.Extract.grid(coverageId, bbox, outputGridFile)) {
            AscReaderDriver ascReaderDriver = new AscReaderDriver()
            ascReaderDriver.setAs3DPoint(false)
            ascReaderDriver.setEncoding("UTF-8")
            ascReaderDriver.setDeleteTable(true)
            ascReaderDriver.read(h2GIS.getConnection(), outputGridFile, new EmptyProgressVisitor(), "grid", 4326)
            assertEquals(720, h2GIS.getSpatialTable("grid").rowCount)
        }
    }

    /**
     * Test to extract a grid from process
     */
    @Test
    void extractGridProcess() {
        if(WorldPopExtract.Extract.isCoverageAvailable("wpGlobal:ppp_2018")) {
            String outputFilePath = WorldPopTools.Extract.extractWorldPopLayer("wpGlobal:ppp_2018", [47.63324, -2.78087, 47.65749, -2.75979])
            assertNotNull(outputFilePath)
            assertTrue new File(outputFilePath).exists()
        }
    }

    /**
     * Test to extract a grid and load it in database
     */
    @Test
    void extractLoadGridProcess() {
        if(WorldPopExtract.Extract.isCoverageAvailable("wpGlobal:ppp_2018")) {
            String outputFilePath = WorldPopTools.Extract.extractWorldPopLayer("wpGlobal:ppp_2018", [47.63324, -2.78087, 47.65749, -2.75979])
            if (outputFilePath) {
                assertTrue new File(outputFilePath).exists()
                String outputTableWorldPopName = WorldPopTools.Extract.importAscGrid(h2GIS, outputFilePath)
                assertNotNull outputTableWorldPopName
                assertEquals(720, h2GIS.getSpatialTable(outputTableWorldPopName).rowCount)
                assertEquals(["ID_POP", "THE_GEOM", "POP"], h2GIS.getTable(outputTableWorldPopName).columns)
            }
        }
    }

    /**
     * Test to extract a grid from process
     */
    @Test
    void testCoverageAvailable() {
        assertFalse(WorldPopExtract.Extract.isCoverageAvailable("wpGlobal:ppp_2050"))
        assertTrue(WorldPopExtract.Extract.isCoverageAvailable("wpGlobal:ppp_2018"))
    }
}
