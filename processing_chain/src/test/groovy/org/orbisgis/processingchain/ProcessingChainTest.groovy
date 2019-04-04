package org.orbisgis.processingchain

import org.junit.jupiter.api.Test

import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanager.ProcessMapper

class ProcessingChainTest {

    @Test
    void runBDTopoProcessingChain(){
        H2GIS h2GIS = H2GIS.open("./target/processingchaindb")
        ProcessMapper pm =  ProcessingChain.prepareBDTopo.createMapper()
        pm.execute([datasource: h2GIS])

    }

}
