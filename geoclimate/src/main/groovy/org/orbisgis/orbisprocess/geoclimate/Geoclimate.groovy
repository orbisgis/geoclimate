package org.orbisgis.orbisprocess.geoclimate


import org.orbisgis.orbisprocess.geoclimate.bdtopo_v2.BDTopo_V2 as Topo
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators as GI
import org.orbisgis.orbisprocess.geoclimate.osm.OSM as O
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
        public static def workflow = Geoclimate.Osm.WorkflowOSM.workflow
    }

    public static class BDTOPO_V2{
        public static def workflow = Geoclimate.BDTopo.WorkflowBDTopo_V2.workflow
    }

    /**
     * Set the logger for all the processes.
     *
     * @param logger Logger to use in the processes.
     */
    static void setLogger(def logger){
        Osm.setLogger(logger)
        BDTopo.setLogger(logger)
        ProcessingChain.setLogger(logger)
        Geoindicators.setLogger(logger)
    }
}
