package org.orbisgis.orbisprocess.geoclimate;

import org.junit.jupiter.api.Test

/**
 * Test for the {@link Geoclimate} class.
 */
class GeoclimateTest {
    @Test
    void test() {
        assert Geoclimate.BDTOPO_V2.workflow
        assert Geoclimate.OSM.workflow

        assert Geoclimate.BDTopo_V2.AbstractTablesInitialization.initParametersAbstract
        assert Geoclimate.BDTopo_V2.BDTopoGISLayers.importPreprocess
        assert Geoclimate.BDTopo_V2.BDTopoGISLayers.initTypes
        assert Geoclimate.BDTopo_V2.InputDataFormatting.formatData
        assert Geoclimate.BDTopo_V2.PrepareBDTopo.prepareData
        assert Geoclimate.BDTopo_V2.WorkflowBDTopo_V2.workflow
    }
}