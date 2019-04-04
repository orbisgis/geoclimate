package org.orbisgis.common

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript PrepareData prepareData

/**
 * This process allows to control the quality of input tables and then to format and enrich them
 *
 * @param datasource
 * @param inputBuilding
 * @param inputRoad
 * @param inputRail
 * @param inputHydro
 * @param inputVeget
 * @param inputZone
 * @param inputZoneNeighbors
 * @param hLevMin
 * @param hLevMax
 * @param hThresholdLev2
 *
 * @return outputBuilding
 * @return outputBuildingStatZone
 * @return outputBuildingStatZoneBuff
 * @return outputRoad
 * @return outputRoadStatZone
 * @return outputRoadStatZoneBuff
 * @return outputRail
 * @return outputRailStatZone
 * @return outputHydro
 * @return outputHydroStatZone
 * @return outputHydroStatZoneExt
 * @return outputVeget
 * @return outputVegetStatZone
 * @return outputVegetStatZoneExt
 * @return outputZone
 */

static IProcess inputDataFormatting(){
    return processFactory.create(
            'Allows to control, format and enrich the input data in order to feed the GeoClimate model',
            [datasource: JdbcDataSource, inputBuilding: String, inputRoad: String, inputRail: String,
             inputHydro: String, inputVeget: String, inputZone: String, inputZoneNeighbors: String,

             ajouter les tables de param

             hLevMin: int, hLevMax: int, hThresholdLev2: int, idZone: String
            ],
            [outputBuilding: String, outputBuildingStatZone: String, outputBuildingStatZoneBuff: String,
             outputRoad: String, outputRoadStatZone: String, outputRoadStatZoneBuff: String,
             outputRail: String, outputRailStatZone: String,
             outputHydro: String, outputHydroStatZone: String, outputHydroStatZoneExt: String,
             outputVeget: String, outputVegetStatZone: String, outputVegetStatZoneExt: String,
             outputZone: String
            ],
            {JdbcDataSource datasource, inputBuilding, inputRoad, inputRail,
                inputHydro, inputVeget, inputZone, inputZoneNeighbors,
                hLevMin, hLevMax, hThresholdLev2, idZone ->
                logger.info('Executing the inputDataFormatting.sql script')
                def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
                def buZone = 'BU_ZONE_' + uuid


                BUILD_WITHIN_ZONE
                ZONE
                ZONE_NEIGHBORS
                BUILD_OUTER_ZONE
                BUILD_OUTER_ZONE_MATRIX
                BUILD_ZONE_MATRIX
                BUILDING_FC
                FC_BUILD_H_ZERO
                FC_BUILD_H_NULL
                FC_BUILD_STATS_ZONE
                FC_BUILD_STATS_EXT_ZONE
                BUILDING
                INPUT_BUILDING
                BUILD_NUMB
                BUILD_VALID_ZONE
                BUILD_EMPTY
                BUILD_EQUALS_ZONE
                BUILD_OVERLAP_ZONE
                BUILD_H
                BUILD_H_RANGE
                BUILD_H_WALL_ROOF
                BUILD_LEV
                BUILD_LEV_RANGE
                BUILD_TYPE
                BUILD_TYPE_RANGE
                BUILDING_ABSTRACT_USE_TYPE
                BUILDING_ABSTRACT_PARAMETERS
                B_STATS_ZONE
                BUILD_VALID_EXT_ZONE
                BUILD_EQUALS_EXT_ZONE
                BUILD_OVERLAP_EXT_ZONE
                B_STATS_EXT_ZONE


                ROAD_FC_W_ZERO
                ROAD_FC_W_NULL
                ROAD_FC_W_RANGE
                R_FC_STATS_ZONE
                R_FC_STATS_ZONE_BUF
                ROAD
                INPUT_ROAD
                ROAD_ZONE
                ROAD_VALID
                ROAD_EMPTY
                ROAD_EQUALS
                ROAD_OVERLAP
                ROAD_W
                ROAD_W_RANGE
                ROAD_TYPE
                ROAD_TYPE_RANGE
                ROAD_ABSTRACT_TYPE
                ROAD_ABSTRACT_PARAMETERS
                -----R_STATS------

                RAIL
                INPUT_RAIL
                RAIL_NB
                RAIL_VALID
                RAIL_EMPTY
                RAIL_EQUALS
                RAIL_OVERLAP
                RAIL_TYPE
                RAIL_TYPE_RANGE
                RAIL_ABSTRACT_TYPE
                RAIL_STATS_ZONE

                HYDRO
                INPUT_HYDRO
                HYDRO_NUM_ZONE
                HYDRO_VALID
                HYDRO_EMPTY
                HYDRO_EQUALS
                HYDRO_OVERLAP
                HYDRO_STATS_ZONE
                HYDRO_STATS_EXT_ZONE

                VEGET
                VEGET_ABSTRACT_PARAMETERS
                INPUT_VEGET
                VEGET_NUM_ZONE
                VEGET_VALID
                VEGET_EMPTY
                VEGET_EQUALS
                VEGET_OVERLAP
                VEGET_TYPE
                VEGET_TYPE_RANGE
                VEGET_ABSTRACT_TYPE
                VEGET_STATS_ZONE

                VEGET_STATS_EXT_ZONE

                datasource.executeScript(this.class.getResource('inputDataFormatting.sql').toString(),
                [BU_ZONE: buZone,



                 H_LEV_MIN: hLevMin, H_LEV_MAX: hLevMax, H_THRESHOLD_LEV2: hThresholdLev2, ID_ZONE: idZone

                ])
                logger.info('The inputDataFormatting.sql script has been executed')

                [outputBuilding

                ]
            }
    )
}