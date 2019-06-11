package org.orbisgis.common

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class AbstractTablesInitializationTest {

    private final static String bdTopoDb = System.getProperty("user.home") + "/myh2gisbdtopodb.mv.db"

    @BeforeAll
    static void init(){
        System.setProperty("test.bdtopo", Boolean.toString(new File(bdTopoDb).exists()))
    }

    //TODO create a dummy dataset (from BD Topo) to run the test

    @Test
    @EnabledIfSystemProperty(named = "test.bdtopo", matches = "true")
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open(bdTopoDb-".mv.db", "sa", "")
        def process = PrepareData.AbstractTablesInitialization.initParametersAbstract()
        assertTrue process.execute([datasource: h2GISDatabase])
        process.getResults().each {entry ->
            println(entry)
            assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
