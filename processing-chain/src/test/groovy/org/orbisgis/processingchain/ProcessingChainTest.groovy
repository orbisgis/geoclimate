package org.orbisgis.processingchain

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanager.ProcessMapper

import static org.junit.jupiter.api.Assertions.assertNull

class ProcessingChainTest {

    @BeforeAll
    static void init(){
        System.setProperty("test.processingchain",
                Boolean.toString(ProcessingChainTest.getResource("geoclimate_bdtopo_data_test") != null))
    }

    @EnabledIfSystemProperty(named = "test.processingchain", matches = "true")
    @Test
    void runBDTopoProcessingChain(){
        H2GIS h2GIS = H2GIS.open("./target/processingchaindb")

        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/IRIS_GE.geojson").toURI()).getAbsolutePath(),"IRIS_GE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/BATI_INDIFFERENCIE.geojson").toURI()).getAbsolutePath(),"BATI_INDIFFERENCIE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/BATI_REMARQUABLE.geojson").toURI()).getAbsolutePath(),"BATI_REMARQUABLE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/ROUTE.geojson").toURI()).getAbsolutePath(),"ROUTE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/TRONCON_VOIE_FERREE.geojson").toURI()).getAbsolutePath(),"TRONCON_VOIE_FERREE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/SURFACE_EAU.geojson").toURI()).getAbsolutePath(),"SURFACE_EAU",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/ZONE_VEGETATION.geojson").toURI()).getAbsolutePath(),"ZONE_VEGETATION",true)

        ProcessMapper pm =  ProcessingChain.prepareBDTopo.createMapper()
        pm.execute([datasource: h2GIS, distBuffer : 500, expand : 1000, idZone : "56260", tableIrisName: "IRIS_GE",
                    tableBuildIndifName: "BATI_INDIFFERENCIE", tableBuildIndusName: "BATI_INDUSTRIEL",
                    tableBuildRemarqName: "BATI_REMARQUABLE", tableRoadName: "ROUTE", tableRailName: "TRONCON_VOIE_FERREE",
                    tableHydroName: "SURFACE_EAU", tableVegetName: "ZONE_VEGETATION",  hLevMin: 3,  hLevMax: 15,
                    hThresholdLev2: 10])

        pm.getResults().each {
            entry -> assertNull h2GIS.getTable(entry.getValue())
        }
    }

}
