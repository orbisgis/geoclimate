package org.orbisgis.common

import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNotNull

class AbstractTablesInitializationTest {

    //TODO create a dummy dataset (from BD Topo) to run the test

    @Test
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb")
        def process = PrepareData.AbstractTablesInitialization.initParametersAbstract()
        process.execute([h2gis: h2GISDatabase])
        process.getResults().each {
            entry -> assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
