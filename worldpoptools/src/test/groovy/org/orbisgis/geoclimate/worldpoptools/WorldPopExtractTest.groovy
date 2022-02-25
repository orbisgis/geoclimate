package org.orbisgis.geoclimate.worldpoptools

import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.functions.io.asc.AscReaderDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS.open

class WorldPopExtractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorldPopExtractTest)
    private static H2GIS h2GIS

    @BeforeAll
    static void beforeAll(){
        h2GIS = open"./target/worldpop_extract;AUTO_SERVER=TRUE"
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
        def outputGridFile = new File("target/extractGrid.asc")
        if(outputGridFile.exists()){
            outputGridFile.delete()
        }
        def coverageId = "wpGlobal:ppp_2018"
        def bbox =[ 47.63324, -2.78087,47.65749, -2.75979]
        assertTrue WorldPopTools.Extract.grid(coverageId, bbox, outputGridFile)

        AscReaderDriver ascReaderDriver = new AscReaderDriver()
        ascReaderDriver.setAs3DPoint(false)
        ascReaderDriver.setEncoding("UTF-8")
        ascReaderDriver.setDeleteTable(true)
        ascReaderDriver.read(h2GIS.getConnection(),outputGridFile, new EmptyProgressVisitor(), "grid", 4326)
        assertEquals(720, h2GIS.getSpatialTable("grid").rowCount)
    }

    /**
     * Test to extract a grid from process
     */
    @Test
    void extractGridProcess(){
        IProcess process = WorldPopTools.Extract.extractWorldPopLayer()
        def coverageId = "wpGlobal:ppp_2018"
        def bbox =[ 47.63324, -2.78087,47.65749, -2.75979]
        def epsg = 4326
        assertTrue process.execute([coverageId:coverageId, bbox:bbox])
        assertTrue new File(process.results.outputFilePath).exists()
    }

    /**
     * Test to extract a grid and load it in database
     */
    @Test
    void extractLoadGridProcess(){
        IProcess extractWorldPopLayer = WorldPopTools.Extract.extractWorldPopLayer()
        def coverageId = "wpGlobal:ppp_2018"
        def bbox =[ 47.63324, -2.78087,47.65749, -2.75979]
        [46.257614, 4.866568, 46.271828, 4.8969374]
        def epsg = 4326
        assertTrue extractWorldPopLayer.execute([coverageId:coverageId, bbox:bbox])
        assertTrue new File(extractWorldPopLayer.results.outputFilePath).exists()
        IProcess importAscGrid =  WorldPopTools.Extract.importAscGrid()
        assertTrue importAscGrid.execute([worldPopFilePath:extractWorldPopLayer.results.outputFilePath,
                               epsg: epsg, datasource: h2GIS])
        assertEquals(720, h2GIS.getSpatialTable(importAscGrid.results.outputTableWorldPopName).rowCount)
        assertEquals(["ID_POP", "THE_GEOM", "POP"], h2GIS.getTable(importAscGrid.results.outputTableWorldPopName).columns)
    }
}
