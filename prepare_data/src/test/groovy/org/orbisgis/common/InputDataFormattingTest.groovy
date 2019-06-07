package org.orbisgis.common

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertTrue

class InputDataFormattingTest {

    private final static String bdTopoDb = System.getProperty("user.home") + "/myh2gisbdtopodb.mv.db"

    @BeforeAll
    static void init(){
        System.setProperty("test.bdtopo", Boolean.toString(new File(bdTopoDb).exists()))
    }

    //TODO create a dummy dataset (from BD Topo) to run the test

    @Test
    @EnabledIfSystemProperty(named = "test.bdtopo", matches = "true")
    void inputDataFormatting(){
        H2GIS h2GISDatabase = H2GIS.open(bdTopoDb)
        def process = PrepareData.InputDataFormatting.inputDataFormatting()
        assertTrue process.execute([datasource: h2GISDatabase,
                         inputBuilding: 'INPUT_BUILDING', inputRoad: 'INPUT_ROAD', inputRail: 'INPUT_RAIL',
                         inputHydro: 'INPUT_HYDRO', inputVeget: 'INPUT_VEGET',
                         inputZone: 'ZONE', inputZoneNeighbors: 'ZONE_NEIGHBORS',

                         hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, idZone: '56260',

                         buildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE', buildingAbstractParameters: 'BUILDING_ABSTRACT_PARAMETERS',
                         roadAbstractType: 'ROAD_ABSTRACT_TYPE', roadAbstractParameters: 'ROAD_ABSTRACT_PARAMETERS',
                         railAbstractType: 'RAIL_ABSTRACT_TYPE',
                         vegetAbstractType: 'VEGET_ABSTRACT_TYPE', vegetAbstractParameters: 'VEGET_ABSTRACT_PARAMETERS'])
        process.getResults().each {
            entry -> assertNull h2GISDatabase.getTable(entry.getValue())
        }
    }
}
