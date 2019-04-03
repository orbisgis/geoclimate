package org.orbisgis.bdtopo

import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

class BDTopoGISLayersTest {

    //TODO create a dummy dataset (from BD Topo) to run the test

    @Test
    void importPreprocessTest(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        process.execute([h2gis: h2GISDatabase, tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                         tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                         tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                         tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                         distBuffer: 500, expand: 1000, idZone: '56260'])
        process.getResults().each {
            entry -> assertFalse h2GISDatabase.tableNames.contains(entry.getValue())
        }
    }
}
