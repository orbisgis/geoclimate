package org.orbisgis.orbisprocess.geoclimate

import examples.TestAutoregister
import org.junit.Test
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators as GI
import org.orbisgis.orbisprocess.geoclimate.osm.OSM as O
import org.orbisgis.orbisprocess.geoclimate.bdtopo_v2.BDTopo_V2 as Topo
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain as PC

import static org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager.load

/**
 * Root access point to the Geoindicators processes.
 */
class Geoclimate {

    public static def Geoindicators = load(GI)
    public static def Osm = load(O)
    public static def BDTopo = load(Topo)
    public static def ProcessingChain = load(PC)

    public static class OSM{
        public static def Workflow = Geoclimate.Osm.WorkflowOSM.Workflow
    }

    public static class BDTopo_V2{
        public static def Workflow = Geoclimate.BDTopo.WorkflowBDTopo_V2.Workflow
    }

    /**
     * Set the logger for all the processes.
     *
     * @param logger Logger to use in the processes.
     */
    static void setLogger(def logger){
        OSM.logger = logger
        BDTopo.logger = logger
        ProcessingChain.logger = logger
        Geoindicators.logger = logger
        OSM.getMetaClass().getMetaPropertyValues().forEach {it.value}
    }
}
