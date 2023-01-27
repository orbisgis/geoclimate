package org.orbisgis.geoclimate.bdtopo.v3

import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest

class WorkflowBDTopoV3Test extends WorkflowAbstractTest {


    @Override
    int getVersion() {
        return 0;
    }

    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}";
    }

    @Override
    String getInseeCode() {
        return "35236";
    }
}
