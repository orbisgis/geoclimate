package org.orbisgis.common

import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS

import static org.junit.jupiter.api.Assertions.assertNull

class InputDataFormattingTest {

    //TODO create a dummy dataset (from BD Topo) to run the test

    @Test
    void inputDataFormatting(){
        H2GIS h2GISDatabase = H2GIS.open("./target/myh2gisbdtopodb")
        def process = PrepareData.InputDataFormatting.inputDataFormatting()
        process.execute([datasource: h2GISDatabase,
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
