package org.orbisgis.common

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class AbstractTablesInitializationTest {

    private final static File bdTopoDb = new File("./target/myh2gisbdtopodb.mv.db")

    @BeforeAll
    static void init(){
        //Check if the resource database exists
        boolean isFile = InputDataFormattingTest.getResource("myh2gisbdtopodb.mv.db") != null
        System.setProperty("test.bdtopo", Boolean.toString(isFile))
        //If the resource exists, copy it into the target folder to avoid working on the original database
        if(isFile) {
            bdTopoDb << InputDataFormattingTest.getResourceAsStream("myh2gisbdtopodb.mv.db")
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "test.bdtopo", matches = "true")
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open(bdTopoDb.absolutePath-".mv.db", "sa", "")
        def process = PrepareData.AbstractTablesInitialization.initParametersAbstract()
        assertTrue process.execute([datasource: h2GISDatabase])
        process.getResults().each {entry ->
            assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
