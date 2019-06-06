package org.orbisgis.bdtopo

import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNull

class BDTopoGISLayersTest {

    //TODO create a dummy dataset (from BD Topo) to run the test

    @Test
    void importPreprocessTest(){
        H2GIS h2GISDatabase = H2GIS.open("/tmp/myh2gisbdtopodb", "sa", "")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        process.execute([datasource: h2GISDatabase, tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                         tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                         tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                         tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                         distBuffer: 500, expand: 1000, idZone: '56260',
                         building_bd_topo_use_type: 'BUILDING_BD_TOPO_USE_TYPE' ,
                         building_abstract_use_type: 'BUILDING_ABSTRACT_USE_TYPE' ,
                         road_bd_topo_type: 'ROAD_BD_TOPO_TYPE', road_abstract_type: 'ROAD_ABSTRACT_TYPE',
                         rail_bd_topo_type: 'RAIL_BD_TOPO_TYPE', rail_abstract_type: 'RAIL_ABSTRACT_TYPE',
                         veget_bd_topo_type: 'VEGET_BD_TOPO_TYPE', veget_abstract_type: 'VEGET_ABSTRACT_TYPE'
        ])
        process.getResults().each {
            entry -> assertNotNull(h2GISDatabase.getTable(entry.getValue()))
        }
    }

    @Test
    void initTypes(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb")
        def process = PrepareData.BDTopoGISLayers.initTypes()
        process.execute([h2gis: h2GISDatabase, buildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE',
                         roadAbstractType: 'ROAD_ABSTRACT_TYPE',  railAbstractType: 'RAIL_ABSTRACT_TYPE',
                         vegetAbstractType: 'VEGET_ABSTRACT_TYPE'])
        process.getResults().each {
            entry -> assertNull h2GISDatabase.getTable(entry.getValue())
        }
    }


}
