package org.orbisgis.common

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class AbstractTablesInitializationTest {

    @BeforeAll
    static void init(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")

        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("IRIS_GE.shp"), "IRIS_GE", true)
        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("BATI_INDIFFERENCIE.shp"), "BATI_INDIFFERENCIE", true)
        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("BATI_INDUSTRIEL.shp"), "BATI_INDUSTRIEL", true)
        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("BATI_REMARQUABLE.shp"), "BATI_REMARQUABLE", true)
        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("ROUTE.shp"), "ROUTE",true)
        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("SURFACE_EAU.shp"), "SURFACE_EAU",true)
        h2GISDatabase.load(AbstractTablesInitialization.class.getResource("ZONE_VEGETATION.shp"), "ZONE_VEGETATION",true)
    }

    @Test
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
        def process = PrepareData.AbstractTablesInitialization.initParametersAbstract()
        assertTrue process.execute([datasource: h2GISDatabase])
        process.getResults().each {entry ->
            assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
