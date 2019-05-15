package org.orbisgis.common

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript PrepareData prepareData

/**
 * This process allows to control the quality of input tables and then to format and enrich them
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param inputBuilding Table name in which the input buildings are stored
 * @param inputRoad Table name in which the input roads are stored
 * @param inputRail Table name in which the input rail ways are stored
 * @param inputHydro Table name in which the input hydrographic areas are stored
 * @param inputVeget Table name in which the input vegetation areas are stored
 * @param inputZone Table name in which the input zone stored
 * @param inputZoneNeighbors Table name in which the neighboring zones are stored
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param idZone The Zone id
 * @param buildingAbstractUseType The name of the table in which the abstract building's uses and types are stored
 * @param buildingAbstractParameters The name of the table in which the abstract building's parameters are stored
 * @param roadAbstractType The name of the table in which the abstract road's types are stored
 * @param roadAbstractParameters The name of the table in which the abstract road's parameters are stored
 * @param railAbstractType The name of the table in which the abstract rail's types are stored
 * @param vegetAbstractType The name of the table in which the abstract vegetation area's types are stored
 * @param vegetAbstractParameters The name of the table in which the abstract vegetation area's parameters are stored
 *
 * @return outputBuilding Table name in which the (ready to be used in the GeoIndicators part) buildings are stored
 * @return outputBuildingStatZone Table that store the building statistics at the zone level
 * @return outputBuildingStatZoneBuff Table that store the building statistics at the zone_buffer level
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRoadStatZone Table that store the road statistics at the zone level
 * @return outputRoadStatZoneBuff Table that store the road statistics at the zone_buffer level
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputRailStatZone Table that store the rail ways statistics at the zone level
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputHydroStatZone Table that store the hydrographic areas statistics at the zone level
 * @return outputHydroStatZoneExt Table that store the hydrographic areas statistics at the zone_extended level
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputVegetStatZone Table that store the vegetation areas statistics at the zone level
 * @return outputVegetStatZoneExt Table that store the vegetation areas statistics at the zone_extended level
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 */

static IProcess inputDataFormatting(){
    return processFactory.create(
            'Allows to control, format and enrich the input data in order to feed the GeoClimate model',
            [datasource: JdbcDataSource, inputBuilding: String, inputRoad: String, inputRail: String,
             inputHydro: String, inputVeget: String, inputZone: String, inputZoneNeighbors: String,

             hLevMin: int, hLevMax: int, hThresholdLev2: int, idZone: String,

             buildingAbstractUseType: String, buildingAbstractParameters: String, roadAbstractType: String,
             roadAbstractParameters: String, railAbstractType: String, vegetAbstractType: String,
             vegetAbstractParameters: String,
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
                hLevMin= 3, hLevMax= 15, hThresholdLev2= 10, idZone, buildingAbstractUseType, buildingAbstractParameters,
                roadAbstractType, roadAbstractParameters, railAbstractType, vegetAbstractType, vegetAbstractParameters ->
                logger.info('Executing the inputDataFormatting.sql script')
                def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
                def buZone = 'BU_ZONE_' + uuid
                def buildWithZone = 'BUILD_WITHIN_ZONE_' + uuid
                def buildOuterZone = 'BUILD_OUTER_ZONE_' + uuid
                def buildOuterZoneMatrix = 'BUILD_OUTER_ZONE_MATRIX_' + uuid
                def buildZoneMatrix = 'BUILD_ZONE_MATRIX_' + uuid
                def buildingFC = 'BUILDING_FC_' + uuid
                def fcBuildHZero = 'FC_BUILD_H_ZERO_' + uuid
                def fcBuildHNull = 'FC_BUILD_H_NULL_' + uuid
                def fcBuildHRange= 'FC_BUILD_H_RANGE'+uuid
                def fcBuildStatsZone = 'FC_BUILD_STATS_ZONE_' + uuid
                def fcBuildStatsExtZone = 'FC_BUILD_STATS_EXT_ZONE_' + uuid
                def buildNumb = 'BUILD_NUMB_' + uuid
                def buildValidZone = 'BUILD_VALID_ZONE_' + uuid
                def buildEmpty = 'BUILD_EMPTY_' + uuid
                def buildEqualsZone = 'BUILD_EQUALS_ZONE_' + uuid
                def buildOverlapZone = 'BUILD_OVERLAP_ZONE_' + uuid
                def buildH = 'BUILD_H_' + uuid
                def buildHRange = 'BUILD_H_RANGE_' + uuid
                def buildHWallRoof = 'BUILD_H_WALL_ROOF_' + uuid
                def buildLev = 'BUILD_LEV_' + uuid
                def buildLevRange = 'BUILD_LEV_RANGE_' + uuid
                def buildType = 'BUILD_TYPE_' + uuid
                def buildTypeRange = 'BUILD_TYPE_RANGE_' + uuid
                def buildValidExtZone = 'BUILD_VALID_EXT_ZONE_' + uuid
                def buildEqualsExtZone = 'BUILD_EQUALS_EXT_ZONE_' + uuid
                def buildOverlapExtZone = 'BUILD_OVERLAP_EXT_ZONE_' + uuid
                def roadFCWZero = 'ROAD_FC_W_ZERO_' + uuid
                def roadFCWNull = 'ROAD_FC_W_NULL_' + uuid
                def roadFCWRange = 'ROAD_FC_W_RANGE_' + uuid
                def rFCStatsZone = 'R_FC_STATS_ZONE_' + uuid
                def rFCStatsExtZone = 'R_FC_STATS_EXT_ZONE_' + uuid
                def roadZone = 'ROAD_ZONE_' + uuid
                def roadValid = 'ROAD_VALID_' + uuid
                def roadEmpty = 'ROAD_EMPTY_' + uuid
                def roadEquals = 'ROAD_EQUALS_' + uuid
                def roadOverlap = 'ROAD_OVERLAP_' + uuid
                def roadW = 'ROAD_W_' + uuid
                def roadWRange = 'ROAD_W_RANGE_' + uuid
                def roadType = 'ROAD_TYPE_' + uuid
                def roadTypeRange = 'ROAD_TYPE_RANGE_' + uuid
                def railNB = 'RAIL_NB_' + uuid
                def railValid = 'RAIL_VALID_' + uuid
                def railEmpty = 'RAIL_EMPTY_' + uuid
                def railEquals = 'RAIL_EQUALS_' + uuid
                def railOverlap = 'RAIL_OVERLAP_' + uuid
                def railType = 'RAIL_TYPE_' + uuid
                def railTypeRange = 'RAIL_TYPE_RANGE_' + uuid
                def hydroNumZone = 'HYDRO_NUM_ZONE_' + uuid
                def hydroValid = 'HYDRO_VALID_' + uuid
                def hydroEmpty = 'HYDRO_EMPTY_' + uuid
                def hydroEquals = 'HYDRO_EQUALS_' + uuid
                def hydroOlverlap = 'HYDRO_OVERLAP_' + uuid
                def vegetNumZone = 'VEGET_NUM_ZONE_' + uuid
                def vegetValid = 'VEGET_VALID_' + uuid
                def vegetEmpty = 'VEGET_EMPTY_' + uuid
                def vegetEquals = 'VEGET_EQUALS_' + uuid
                def vegetOverlap = 'VEGET_OVERLAP_' + uuid
                def vegetType = 'VEGET_TYPE_' + uuid
                def vegetTypeRange = 'VEGET_TYPE_RANGE_' + uuid

                //Final output table names
                def BUILDING = 'BUILDING'
                def BUILDING_STATS_ZONE = 'BUILDING_STATS_ZONE'
                def BUILDING_STATS_EXT_ZONE = 'BUILDING_STATS_EXT_ZONE'
                def ROAD='ROAD'
                def ROAD_STATS_ZONE='ROAD_STATS_ZONE'
                def ROAD_STATS_EXT_ZONE='ROAD_STATS_EXT_ZONE'
                def RAIL ='RAIL'
                def RAIL_STATS_ZONE='RAIL_STATS_ZONE'
                def HYDRO ='HYDRO'
                def HYDRO_STATS_ZONE = 'HYDRO_STATS_ZONE'
                def HYDRO_STATS_EXT_ZONE='HYDRO_STATS_EXT_ZONE'
                def VEGET ='VEGET'
                def VEGET_STATS_ZONE='VEGET_STATS_ZONE'
                def VEGET_STATS_EXT_ZONE='VEGET_STATS_EXT_ZONE'

                //Run the sql script
                datasource.executeScript(getClass().getResource('inputDataFormatting.sql').toString(),
                            [INPUT_BUILDING : inputBuilding, INPUT_ROAD: inputRoad, INPUT_RAIL: inputRail,
                             INPUT_HYDRO  : inputHydro, INPUT_VEGET: inputVeget, ZONE: inputZone, ZONE_NEIGHBORS: inputZoneNeighbors,
                             H_LEV_MIN : hLevMin, H_LEV_MAX: hLevMax, H_THRESHOLD_LEV2: hThresholdLev2, ID_ZONE: idZone,
                             BUILDING_ABSTRACT_USE_TYPE: buildingAbstractUseType, BUILDING_ABSTRACT_PARAMETERS: buildingAbstractParameters,
                             ROAD_ABSTRACT_TYPE : roadAbstractType, ROAD_ABSTRACT_PARAMETERS: roadAbstractParameters,
                             RAIL_ABSTRACT_TYPE        : railAbstractType,
                             VEGET_ABSTRACT_TYPE       : vegetAbstractType, VEGET_ABSTRACT_PARAMETERS: vegetAbstractParameters,
                             BU_ZONE                   : buZone, BUILD_WITHIN_ZONE: buildWithZone, BUILD_OUTER_ZONE: buildOuterZone,
                             BUILD_OUTER_ZONE_MATRIX   : buildOuterZoneMatrix, BUILD_ZONE_MATRIX: buildZoneMatrix,
                             BUILDING_FC               : buildingFC, FC_BUILD_H_ZERO: fcBuildHZero, FC_BUILD_H_NULL: fcBuildHNull,
                             FC_BUILD_STATS_ZONE       : fcBuildStatsZone, FC_BUILD_STATS_EXT_ZONE: fcBuildStatsExtZone,
                             BUILD_NUMB                : buildNumb, BUILD_VALID_ZONE: buildValidZone, BUILD_EMPTY: buildEmpty,
                             BUILD_EQUALS_ZONE         : buildEqualsZone, BUILD_OVERLAP_ZONE: buildOverlapZone,
                             BUILD_H                   : buildH, BUILD_H_RANGE: buildHRange, BUILD_H_WALL_ROOF: buildHWallRoof, BUILD_LEV: buildLev,
                             BUILD_LEV_RANGE           : buildLevRange, BUILD_TYPE: buildType, BUILD_TYPE_RANGE: buildTypeRange,
                             BUILD_VALID_EXT_ZONE      : buildValidExtZone, BUILD_EQUALS_EXT_ZONE: buildEqualsExtZone,
                             BUILD_OVERLAP_EXT_ZONE    : buildOverlapExtZone,
                             ROAD_FC_W_ZERO            : roadFCWZero, ROAD_FC_W_NULL: roadFCWNull, ROAD_FC_W_RANGE: roadFCWRange,
                             R_FC_STATS_ZONE           : rFCStatsZone, R_FC_STATS_EXT_ZONE: rFCStatsExtZone, ROAD_ZONE: roadZone,
                             ROAD_VALID                : roadValid, ROAD_EMPTY: roadEmpty, ROAD_EQUALS: roadEquals, ROAD_OVERLAP: roadOverlap,
                             ROAD_W                    : roadW, ROAD_W_RANGE: roadWRange, ROAD_TYPE: roadType, ROAD_TYPE_RANGE: roadTypeRange,
                             RAIL_NB                   : railNB, RAIL_VALID: railValid, RAIL_EMPTY: railEmpty, RAIL_EQUALS: railEquals,
                             RAIL_OVERLAP              : railOverlap, RAIL_TYPE: railType, RAIL_TYPE_RANGE: railTypeRange,
                             HYDRO_NUM_ZONE            : hydroNumZone, HYDRO_VALID: hydroValid, HYDRO_EMPTY: hydroEmpty,
                             HYDRO_EQUALS              : hydroEquals, HYDRO_OVERLAP: hydroOlverlap,
                             VEGET_NUM_ZONE            : vegetNumZone, VEGET_VALID: vegetValid, VEGET_EMPTY: vegetEmpty,
                             VEGET_EQUALS              : vegetEquals, VEGET_OVERLAP: vegetOverlap, VEGET_TYPE: vegetType,
                             VEGET_TYPE_RANGE          : vegetTypeRange,
                             BUILDING                  : BUILDING, BUILDING_STATS_ZONE: BUILDING_STATS_ZONE,
                             BUILDING_STATS_EXT_ZONE   : BUILDING_STATS_EXT_ZONE, ROAD: ROAD, ROAD_STATS_ZONE: ROAD_STATS_ZONE,
                             ROAD_STATS_EXT_ZONE       : ROAD_STATS_EXT_ZONE, RAIL: RAIL,
                             RAIL_STATS_ZONE           : RAIL_STATS_ZONE, HYDRO: HYDRO, HYDRO_STATS_ZONE: HYDRO_STATS_ZONE,
                             HYDRO_STATS_EXT_ZONE      : HYDRO_STATS_EXT_ZONE, VEGET: VEGET, VEGET_STATS_ZONE: VEGET_STATS_ZONE,
                             VEGET_STATS_EXT_ZONE      : VEGET_STATS_EXT_ZONE,FC_BUILD_H_RANGE:fcBuildHRange
                            ])

                logger.info('The inputDataFormatting.sql script has been executed')

                [outputBuilding: BUILDING,
                 outputBuildingStatZone: BUILDING_STATS_ZONE, outputBuildingStatZoneBuff: BUILDING_STATS_EXT_ZONE,
                 outputRoad: ROAD, outputRoadStatZone: ROAD_STATS_ZONE, outputRoadStatZoneBuff: ROAD_STATS_EXT_ZONE,
                 outputRail: RAIL, outputRailStatZone: RAIL_STATS_ZONE,
                 outputHydro: HYDRO, outputHydroStatZone: HYDRO_STATS_ZONE, outputHydroStatZoneExt: HYDRO_STATS_EXT_ZONE,
                 outputVeget: VEGET, outputVegetStatZone: VEGET_STATS_ZONE, outputVegetStatZoneExt: VEGET_STATS_EXT_ZONE,
                 outputZone: inputZone
                ]
            }
    )
}