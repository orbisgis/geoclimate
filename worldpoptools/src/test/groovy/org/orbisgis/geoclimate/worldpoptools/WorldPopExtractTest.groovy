package org.orbisgis.geoclimate.worldpoptools

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.imageio.ImageIO
import java.awt.Image

import static org.junit.jupiter.api.Assertions.assertTrue

class WorldPopExtractTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorldPopExtractTest)

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test to extract a grid as a tiff file
     */
    @Test
    void extractGrid(){
        def outputGridFile = new File("target/extractGrid.tiff")
        if(outputGridFile.exists()){
            outputGridFile.delete()
        }
        def coverageId = "wpGlobal:ppp_2018"
        def bbox =[ 47.63324, -2.78087,47.65749, -2.75979]
        assertTrue WorldPopTools.Extract.grid(coverageId, bbox, outputGridFile)
        Image image = ImageIO.read(outputGridFile)
        assertTrue image.getHeight(null)>0
    }
}
