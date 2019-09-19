package org.orbisgis.bdtopo

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class BDTopoGISLayersTest {

    @BeforeAll
    static void init(){
        if(BDTopoGISLayersTest.class.getResource("bdtopofolder") != null &&
                new File(BDTopoGISLayersTest.class.getResource("bdtopofolder").toURI()).exists()) {
            System.properties.setProperty("data.bd.topo", "true")
            H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/IRIS_GE.shp"), "IRIS_GE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/BATI_INDIFFERENCIE.shp"), "BATI_INDIFFERENCIE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/BATI_INDUSTRIEL.shp"), "BATI_INDUSTRIEL", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/BATI_REMARQUABLE.shp"), "BATI_REMARQUABLE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/ROUTE.shp"), "ROUTE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/SURFACE_EAU.shp"), "SURFACE_EAU", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("bdtopofolder/ZONE_VEGETATION.shp"), "ZONE_VEGETATION", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("TRONCON_VOIE_FERREE.csv"), "TRONCON_VOIE_FERREE0", true)
            h2GISDatabase.execute "DROP TABLE IF EXISTS TRONCON_VOIE_FERREE; CREATE TABLE TRONCON_VOIE_FERREE AS SELECT PK," +
                    "CAST(the_geom AS GEOMETRY) AS the_geom, ID, PREC_PLANI, NATURE, ELECTRIFIE, FRANCHISST, LARGEUR," +
                    "NB_VOIES, POS_SOL, ETAT, Z_INI, Z_FIN FROM TRONCON_VOIE_FERREE0;"
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("BUILDING_ABSTRACT_PARAMETERS.csv"), "BUILDING_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("BUILDING_ABSTRACT_USE_TYPE.csv"), "BUILDING_ABSTRACT_USE_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("BUILDING_BD_TOPO_USE_TYPE.csv"), "BUILDING_BD_TOPO_USE_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_ABSTRACT_TYPE.csv"), "RAIL_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_BD_TOPO_TYPE.csv"), "RAIL_BD_TOPO_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_PARAMETERS.csv"), "ROAD_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_SURFACE.csv"), "ROAD_ABSTRACT_SURFACE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_ABSTRACT_TYPE.csv"), "ROAD_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("RAIL_ABSTRACT_TYPE.csv"), "RAIL_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("ROAD_BD_TOPO_TYPE.csv"), "ROAD_BD_TOPO_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("VEGET_ABSTRACT_PARAMETERS.csv"), "VEGET_ABSTRACT_PARAMETERS", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("VEGET_ABSTRACT_TYPE.csv"), "VEGET_ABSTRACT_TYPE", true)
            h2GISDatabase.load(BDTopoGISLayersTest.class.getResource("VEGET_BD_TOPO_TYPE.csv"), "VEGET_BD_TOPO_TYPE", true)
        }
        else{
            System.properties.setProperty("data.bd.topo", "false")
        }
    }

    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void importPreprocessTest(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
        def process = PrepareData.BDTopoGISLayers.importPreprocess()
        assertTrue process.execute([datasource: h2GISDatabase, tableIrisName: 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                    tableBuildIndusName: 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                    tableRoadName: 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                    tableHydroName: 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                    distBuffer: 500, expand: 1000, idZone: '56195',
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
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void initTypes(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb", "sa", "")
        def process = PrepareData.BDTopoGISLayers.initTypes()
        assertTrue process.execute([datasource: h2GISDatabase, buildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE',
                                    roadAbstractType: 'ROAD_ABSTRACT_TYPE',  railAbstractType: 'RAIL_ABSTRACT_TYPE',
                                    vegetAbstractType: 'VEGET_ABSTRACT_TYPE'])
        process.getResults().each {
            entry -> assertNotNull h2GISDatabase.getTable(entry.getValue())
        }
    }


}
