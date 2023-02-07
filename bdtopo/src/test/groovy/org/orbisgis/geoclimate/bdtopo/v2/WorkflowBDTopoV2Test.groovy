package org.orbisgis.geoclimate.bdtopo.v2


import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest

class WorkflowBDTopoV2Test extends WorkflowAbstractTest {


    @Override
    ArrayList getFileNames() {
        return ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "TRONCON_VOIE_FERREE",
                "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
                "SURFACE_ROUTE", "SURFACE_ACTIVITE"]
    }

    @Override
    int getVersion() {
        return 2
    }

    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}"
    }

    @Override
    String getInseeCode() {
        return "12174"
    }
}