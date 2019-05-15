package org.orbisgis.processingchain

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanager.ProcessMapper
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNull

class ProcessingChainTest {

    @BeforeAll
    static void init(){
        System.setProperty("test.processingchain",
                Boolean.toString(ProcessingChainTest.getResource("geoclimate_bdtopo_data_test") != null))
    }

    @EnabledIfSystemProperty(named = "test.processingchain", matches = "true")
    @Test
    void BDTopoProcessingChainTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchaindb")

        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/IRIS_GE.geojson").toURI()).getAbsolutePath(),"IRIS_GE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/BATI_INDIFFERENCIE.geojson").toURI()).getAbsolutePath(),"BATI_INDIFFERENCIE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/BATI_REMARQUABLE.geojson").toURI()).getAbsolutePath(),"BATI_REMARQUABLE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/ROUTE.geojson").toURI()).getAbsolutePath(),"ROUTE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/TRONCON_VOIE_FERREE.geojson").toURI()).getAbsolutePath(),"TRONCON_VOIE_FERREE",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/SURFACE_EAU.geojson").toURI()).getAbsolutePath(),"SURFACE_EAU",true)
        h2GIS.load(new File(this.class.getResource("geoclimate_bdtopo_data_test/ZONE_VEGETATION.geojson").toURI()).getAbsolutePath(),"ZONE_VEGETATION",true)

        ProcessMapper pm =  ProcessingChain.prepareBDTopo.createMapper()
        pm.execute([datasource: h2GIS, distBuffer : 500, expand : 1000, idZone : "56260", tableIrisName: "IRIS_GE",
                    tableBuildIndifName: "BATI_INDIFFERENCIE", tableBuildIndusName: "BATI_INDUSTRIEL",
                    tableBuildRemarqName: "BATI_REMARQUABLE", tableRoadName: "ROUTE", tableRailName: "TRONCON_VOIE_FERREE",
                    tableHydroName: "SURFACE_EAU", tableVegetName: "ZONE_VEGETATION",  hLevMin: 3,  hLevMax: 15,
                    hThresholdLev2: 10])

        pm.getResults().each {
            entry -> assertNull h2GIS.getTable(entry.getValue())
        }
    }

    @Test
    void CreateUnitsOfAnalysisTest(){
        H2GIS h2GIS = H2GIS.open("./target/processingchaindb")
        String sqlString = new File(this.class.getResource("data_for_tests.sql").toURI()).text
        h2GIS.execute(sqlString)

        // Only the first 6 first created buildings are selected since any new created building may alter the results
        h2GIS.execute("DROP TABLE IF EXISTS tempo_build, tempo_road, tempo_zone, tempo_veget, tempo_hydro; " +
                "CREATE TABLE tempo_build AS SELECT * FROM building_test WHERE id_build < 9; CREATE TABLE " +
                "tempo_road AS SELECT id_road, the_geom, zindex FROM road_test WHERE id_road < 5 UNION ALL " +
                "(SELECT 6, st_tomultiline(the_geom) as the_geom, 0 FROM rsu_test WHERE id_rsu < 5);" +
                "CREATE TABLE tempo_zone AS SELECT * FROM zone_test;" +
                "CREATE TABLE tempo_veget AS SELECT id_veget, the_geom FROM veget_test WHERE id_veget < 4;" +
                "CREATE TABLE tempo_hydro AS SELECT id_hydro, the_geom FROM hydro_test WHERE id_hydro < 2;")



        IProcess pm =  ProcessingChain.processingChains.createUnitsOfAnalysis()
        pm.execute([datasource: h2GIS, zoneTable : "tempo_zone", roadTable : "tempo_road", railTable : "tempo_road",
                    vegetationTable: "tempo_veget", hydrographicTable: "tempo_hydro", surface_vegetation: null,
                    surface_hydro: null, inputTableName: "tempo_build", distance: 0.0,
                    inputLowerScaleTableName: "tempo_build",  prefixName: "test"])

        // Test the number of blocks within RSU ID 2, whether id_build 4 and 8 belongs to the same block and are both
        // within id_rsu = 2
        def row_nb = h2GIS.firstRow(("SELECT COUNT(*) AS nb_blocks FROM ${pm.results.outputTableBlockName} " +
                "WHERE id_rsu = 2").toString())
        def row_bu4 = h2GIS.firstRow(("SELECT id_block AS id_block FROM ${pm.results.outputTableBuildingName} " +
                "WHERE id_build = 4 AND id_rsu = 2").toString())
        def row_bu8 = h2GIS.firstRow(("SELECT id_block AS id_block FROM ${pm.results.outputTableBuildingName} " +
                "WHERE id_build = 8 AND id_rsu = 2").toString())
        assertEquals 4 , row_nb.nb_blocks
        assertEquals row_bu4.id_block , row_bu8.id_blockgit
    }
}
