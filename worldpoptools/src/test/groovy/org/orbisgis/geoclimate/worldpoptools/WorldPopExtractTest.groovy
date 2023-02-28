package org.orbisgis.geoclimate.worldpoptools

import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.functions.io.asc.AscReaderDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class WorldPopExtractTest {

    @TempDir
    static File folder

    private static final Logger LOGGER = LoggerFactory.getLogger(WorldPopExtractTest)
    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll(){
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "WorldPopExtractTest;AUTO_SERVER=TRUE;")
    }

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test to extract a grid as an ascii file and load it in H2GIS
     */
    @Test
    void extractGrid(){
        def outputGridFile = new File(folder,"extractGrid.asc")
        if(outputGridFile.exists()){
            outputGridFile.delete()
        }
        def coverageId = "wpGlobal:ppp_2018"
        def bbox =[ 47.63324, -2.78087,47.65749, -2.75979]
        if(WorldPopTools.Extract.grid(coverageId, bbox, outputGridFile)) {
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
    void extractGridProcess(){
        String outputFilePath = WorldPopTools.Extract.extractWorldPopLayer( "wpGlobal:ppp_2018",[ 47.63324, -2.78087,47.65749, -2.75979])
        assertNotNull(outputFilePath)
        assertTrue new File(outputFilePath).exists()
    }

    /**
     * Test to extract a grid and load it in database
     */
    @Test
    void extractLoadGridProcess(){
        String outputFilePath = WorldPopTools.Extract.extractWorldPopLayer( "wpGlobal:ppp_2018", [ 47.63324, -2.78087,47.65749, -2.75979])
        if(outputFilePath) {
            assertTrue new File(outputFilePath).exists()
            String outputTableWorldPopName = WorldPopTools.Extract.importAscGrid(h2GIS,outputFilePath )
            assertNotNull outputTableWorldPopName
            assertEquals(720, h2GIS.getSpatialTable(outputTableWorldPopName).rowCount)
            assertEquals(["ID_POP", "THE_GEOM", "POP"], h2GIS.getTable(outputTableWorldPopName).columns)
        }
    }
}
