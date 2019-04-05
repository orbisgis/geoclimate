package org.orbisgis.processingchain

import org.junit.jupiter.api.Test

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanager.ProcessMapper

class ProcessingChainTest {

    @Test
    void runBDTopoProcessingChain(){
        H2GIS h2GIS = H2GIS.open("./target/processingchaindb")

/*      h2GIS.load(new File(this.class.getResource("IRIS_GE.shp").toURI()).getAbsolutePath(),"IRIS_GE",true)
        h2GIS.load(new File(this.class.getResource("BATI_INDIFFERENCIE.shp").toURI()).getAbsolutePath(),"BATI_INDIFFERENCIE",true)
        h2GIS.load(new File(this.class.getResource("BATI_REMARQUABLE.shp").toURI()).getAbsolutePath(),"BATI_REMARQUABLE",true)
        h2GIS.load(new File(this.class.getResource("ROUTES.shp").toURI()).getAbsolutePath(),"ROUTES",true)
        h2GIS.load(new File(this.class.getResource("RAIL.shp").toURI()).getAbsolutePath(),"RAIL",true)
        h2GIS.load(new File(this.class.getResource("HYDRO.shp").toURI()).getAbsolutePath(),"HYDRO",true)
        h2GIS.load(new File(this.class.getResource("VEGETATION.shp").toURI()).getAbsolutePath(),"VEGETATION",true)*/

        ProcessMapper pm =  ProcessingChain.prepareBDTopo.createMapper()
        pm.execute([datasource: h2GIS, distBuffer : 500, expand : 1000, idZone : "12313", tableIrisName: "IRIS_GE",
                    tableBuildIndifName: "BATI_INDIFFERENCIE", tableBuildIndusName: "BATI_INDUSTRIEL",
                    tableBuildRemarqName: "BATI_REMARQUABLE", tableRoadName: "ROUTES", tableRailName: "RAIL",
                    tableHydroName: "HYDRO", tableVegetName: "VEGETATION"])

    }

}
