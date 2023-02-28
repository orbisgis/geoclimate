package org.orbisgis.geoclimate.bdtopo.v3

import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest

class WorkflowBDTopoV3Test extends WorkflowAbstractTest {


    @Override
    int getVersion() {
        return 3
    }

    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}"
    }

    @Override
    String getInseeCode() {
        return "12174"
    }

    @Override
    ArrayList getFileNames() {
        return ["COMMUNE", "BATIMENT", "ZONE_D_ACTIVITE_OU_D_INTERET", "TERRAIN_DE_SPORT","CIMETIERE",
                "PISTE_D_AERODROME", "RESERVOIR", "CONSTRUCTION_SURFACIQUE", "EQUIPEMENT_DE_TRANSPORT",
                "TRONCON_DE_ROUTE", "TRONCON_DE_VOIE_FERREE", "SURFACE_HYDROGRAPHIQUE",
                "ZONE_DE_VEGETATION", "AERODROME"]
    }
}
