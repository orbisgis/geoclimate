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
        boolean isFile = InputDataFormattingTest.getResource("myh2gisbdtopodb.mv.db") != null
        System.setProperty("test.bdtopo", Boolean.toString(isFile))
        if(isFile) {
            InputStream is = null
            OutputStream os = null
            try {
                is = InputDataFormattingTest.getResourceAsStream("myh2gisbdtopodb.mv.db")
                os = new FileOutputStream(bdTopoDb)
                byte[] buffer = new byte[1024]
                int length
                while ((length = is.read(buffer)) > 0) {
                    os.write(buffer, 0, length)
                }
            } finally {
                is.close()
                os.close()
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "test.bdtopo", matches = "true")
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open(bdTopoDb.absolutePath-".mv.db", "sa", "")
        def process = PrepareData.AbstractTablesInitialization.initParametersAbstract()
        assertTrue process.execute([datasource: h2GISDatabase])
        process.getResults().each {entry ->
            println(entry)
            assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
