package org.orbisgis.common

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.PrepareData
import org.orbisgis.bdtopo.BDTopoGISLayers
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class AbstractTablesInitializationTest {

    @BeforeAll
    static void init(){
        if(AbstractTablesInitializationTest.class.getResource("bdtopofolder") != null &&
                new File(AbstractTablesInitializationTest.class.getResource("bdtopofolder").toURI()).exists()) {
            System.properties.setProperty("data.bd.topo", "true")

            H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/IRIS_GE.shp"), "IRIS_GE", true)
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/BATI_INDIFFERENCIE.shp"), "BATI_INDIFFERENCIE", true)
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/BATI_INDUSTRIEL.shp"), "BATI_INDUSTRIEL", true)
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/BATI_REMARQUABLE.shp"), "BATI_REMARQUABLE", true)
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/ROUTE.shp"), "ROUTE",true)
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/SURFACE_EAU.shp"), "SURFACE_EAU",true)
            h2GISDatabase.load(AbstractTablesInitializationTest.class.getResource("bdtopofolder/ZONE_VEGETATION.shp"), "ZONE_VEGETATION",true)
        }
        else{
            System.properties.setProperty("data.bd.topo", "false")
        }
    }

    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
        def process = PrepareData.AbstractTablesInitialization.initParametersAbstract()
        assertTrue process.execute([datasource: h2GISDatabase])
        process.getResults().each {entry ->
            assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
