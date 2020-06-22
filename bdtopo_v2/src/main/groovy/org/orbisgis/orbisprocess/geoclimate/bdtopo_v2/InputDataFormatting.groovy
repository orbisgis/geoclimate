package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import groovy.transform.Field
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory

@BaseScript GroovyProcessFactory pf

/**
 * This process allows to control the quality of input tables and then to format and enrich them
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param inputBuilding Table name in which the input buildings are stored
 * @param inputRoad Table name in which the input roads are stored
 * @param inputRail Table name in which the input rail ways are stored
 * @param inputHydro Table name in which the input hydrographic areas are stored
 * @param inputVeget Table name in which the input vegetation areas are stored
 * @param inputImpervious Table name in which the input impervious areas are stored
 * @param inputZone Table name in which the input zone stored
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param idZone The Zone id
 * @param expand The distance (expressed in meter) used to compute the extended area around the ZONE
 * @param buildingAbstractUseType The name of the table in which the abstract building's uses and types are stored
 * @param buildingAbstractParameters The name of the table in which the abstract building's parameters are stored
 * @param roadAbstractType The name of the table in which the abstract road's types are stored
 * @param roadAbstractParameters The name of the table in which the abstract road's parameters are stored
 * @param roadAbstractCrossing The name of the table in which the abstract road's crossing are stored
 * @param railAbstractType The name of the table in which the abstract rail's types are stored
 * @param railAbstractCrossing The name of the table in which the abstract rail's crossing are stored
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
 * @return outputImpervious Table name in which the (ready to be used in the GeoIndicators part) impervious areas are stored
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 */

@Field IProcess formatData = create {
    title 'Allows to control, format and enrich the input data in order to feed the GeoClimate model'
    id "formatData"
    inputs datasource: JdbcDataSource, inputBuilding: String, inputRoad: String, inputRail: String,
            inputHydro: String, inputVeget: String, inputImpervious: String, inputZone: String,
            hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, expand: 1000, idZone: String,
            buildingAbstractUseType: String, buildingAbstractParameters: String,
            roadAbstractType: String, roadAbstractParameters: String, roadAbstractCrossing: String,
            railAbstractType: String, railAbstractCrossing: String,
            vegetAbstractType: String, vegetAbstractParameters: String
    outputs outputBuilding : String , outputBuildingStatZone:String , outputBuildingStatZoneBuff: String ,
            outputRoad: String , outputRoadStatZone: String, outputRoadStatZoneBuff: String ,
            outputRail: String , outputRailStatZone: String ,
            outputHydro: String , outputHydroStatZone: String , outputHydroStatZoneExt: String ,
            outputVeget: String , outputVegetStatZone: String , outputVegetStatZoneExt: String ,
            outputImpervious: String , outputZone: String
    run { JdbcDataSource datasource, inputBuilding, inputRoad, inputRail,
          inputHydro, inputVeget, inputImpervious, inputZone,
          hLevMin, hLevMax, hThresholdLev2, expand, idZone,
          buildingAbstractUseType, buildingAbstractParameters,
          roadAbstractType, roadAbstractParameters, roadAbstractCrossing,
          railAbstractType, railAbstractCrossing,
          vegetAbstractType, vegetAbstractParameters ->
        info('Executing the inputDataFormatting.sql script')
        def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
        def zoneNeighbors = postfix 'ZONE_NEIGHBORS_'
        def buZone = postfix 'BU_ZONE_'
        def buildWithZone = postfix 'BUILD_WITHIN_ZONE_'
        def buildOuterZone = postfix 'BUILD_OUTER_ZONE_'
        def buildOuterZoneMatrix = postfix 'BUILD_OUTER_ZONE_MATRIX_'
        def buildZoneMatrix = postfix 'BUILD_ZONE_MATRIX_'
        def buildingFC = postfix 'BUILDING_FC_'
        def fcBuildHZero = postfix 'FC_BUILD_H_ZERO_'
        def fcBuildHNull = postfix 'FC_BUILD_H_NULL_'
        def fcBuildHRange = postfix 'FC_BUILD_H_RANGE'
        def fcBuildStatsZone = postfix 'FC_BUILD_STATS_ZONE_'
        def fcBuildStatsExtZone = postfix 'FC_BUILD_STATS_EXT_ZONE_'
        def buildNumb = postfix 'BUILD_NUMB_'
        def buildValidZone = postfix 'BUILD_VALID_ZONE_'
        def buildEmpty = postfix 'BUILD_EMPTY_'
        def buildEqualsZone = postfix 'BUILD_EQUALS_ZONE_'
        def buildOverlapZone = postfix 'BUILD_OVERLAP_ZONE_'
        def buildH = postfix 'BUILD_H_'
        def buildHRange = postfix 'BUILD_H_RANGE_'
        def buildHWallRoof = postfix 'BUILD_H_WALL_ROOF_'
        def buildLev = postfix 'BUILD_LEV_'
        def buildLevRange = postfix 'BUILD_LEV_RANGE_'
        def buildType = postfix 'BUILD_TYPE_'
        def buildTypeRange = postfix 'BUILD_TYPE_RANGE_'
        def buildValidExtZone = postfix 'BUILD_VALID_EXT_ZONE_'
        def buildEqualsExtZone = postfix 'BUILD_EQUALS_EXT_ZONE_'
        def buildOverlapExtZone = postfix 'BUILD_OVERLAP_EXT_ZONE_'
        def roadFCWZero = postfix 'ROAD_FC_W_ZERO_'
        def roadFCWNull = postfix 'ROAD_FC_W_NULL_'
        def roadFCWRange = postfix 'ROAD_FC_W_RANGE_'
        def rFCStatsZone = postfix 'R_FC_STATS_ZONE_'
        def rFCStatsExtZone = postfix 'R_FC_STATS_EXT_ZONE_'
        def roadZone = postfix 'ROAD_ZONE_'
        def roadNumb = postfix 'ROAD_NUMB'
        def roadValid = postfix 'ROAD_VALID_'
        def roadEmpty = postfix 'ROAD_EMPTY_'
        def roadEquals = postfix 'ROAD_EQUALS_'
        def roadOverlap = postfix 'ROAD_OVERLAP_'
        def roadW = postfix 'ROAD_W_'
        def roadWRange = postfix 'ROAD_W_RANGE_'
        def roadType = postfix 'ROAD_TYPE_'
        def roadTypeRange = postfix 'ROAD_TYPE_RANGE_'
        def railNB = postfix 'RAIL_NB_'
        def railValid = postfix 'RAIL_VALID_'
        def railEmpty = postfix 'RAIL_EMPTY_'
        def railEquals = postfix 'RAIL_EQUALS_'
        def railOverlap = postfix 'RAIL_OVERLAP_'
        def railType = postfix 'RAIL_TYPE_'
        def railTypeRange = postfix 'RAIL_TYPE_RANGE_'
        def hydroNumZone = postfix 'HYDRO_NUM_ZONE_'
        def hydroNum = postfix 'HYDRO_NB'
        def hydroValid = postfix 'HYDRO_VALID_'
        def hydroEmpty = postfix 'HYDRO_EMPTY_'
        def hydroEquals = postfix 'HYDRO_EQUALS_'
        def hydroOlverlap = postfix 'HYDRO_OVERLAP_'
        def vegetNumZone = postfix 'VEGET_NUM_ZONE_'
        def vegetNum = postfix 'VEGET_NB'
        def vegetValid = postfix 'VEGET_VALID_'
        def vegetEmpty = postfix 'VEGET_EMPTY_'
        def vegetEquals = postfix 'VEGET_EQUALS_'
        def vegetOverlap = postfix 'VEGET_OVERLAP_'
        def vegetType = postfix 'VEGET_TYPE_'
        def vegetTypeRange = postfix 'VEGET_TYPE_RANGE_'

        //Final output table names
        def building = 'BUILDING'
        def buildingStatsZone = 'BUILDING_STATS_ZONE'
        def buildingStatsExtZone = 'BUILDING_STATS_EXT_ZONE'
        def road = 'ROAD'
        def roadStatsZone = 'ROAD_STATS_ZONE'
        def roadStatsExtZone = 'ROAD_STATS_EXT_ZONE'
        def rail = 'RAIL'
        def railStatsZone = 'RAIL_STATS_ZONE'
        def hydro = 'HYDRO'
        def hydroStatsZone = 'HYDRO_STATS_ZONE'
        def hydroStatsExtZone = 'HYDRO_STATS_EXT_ZONE'
        def veget = 'VEGET'
        def vegetStatsZone = 'VEGET_STATS_ZONE'
        def vegetStatsExtZone = 'VEGET_STATS_EXT_ZONE'
        def impervious = 'IMPERVIOUS'

        //Run the sql script
        def success = datasource.executeScript(getClass().getResourceAsStream('inputDataFormatting.sql'),
                [INPUT_BUILDING: inputBuilding, INPUT_ROAD: inputRoad, INPUT_RAIL: inputRail,
                 INPUT_HYDRO: inputHydro, INPUT_VEGET: inputVeget, INPUT_IMPERVIOUS: inputImpervious,
                 ZONE: inputZone, ZONE_NEIGHBORS: zoneNeighbors,
                 H_LEV_MIN: hLevMin, H_LEV_MAX: hLevMax, H_THRESHOLD_LEV2: hThresholdLev2, EXPAND: expand, ID_ZONE: idZone,
                 BUILDING_ABSTRACT_USE_TYPE: buildingAbstractUseType, BUILDING_ABSTRACT_PARAMETERS: buildingAbstractParameters,
                 ROAD_ABSTRACT_TYPE: roadAbstractType, ROAD_ABSTRACT_PARAMETERS: roadAbstractParameters, ROAD_ABSTRACT_CROSSING: roadAbstractCrossing,
                 RAIL_ABSTRACT_TYPE: railAbstractType, RAIL_ABSTRACT_CROSSING: railAbstractCrossing,
                 VEGET_ABSTRACT_TYPE: vegetAbstractType, VEGET_ABSTRACT_PARAMETERS: vegetAbstractParameters,
                 BU_ZONE: buZone, BUILD_WITHIN_ZONE: buildWithZone, BUILD_OUTER_ZONE: buildOuterZone,
                 BUILD_OUTER_ZONE_MATRIX: buildOuterZoneMatrix, BUILD_ZONE_MATRIX: buildZoneMatrix,
                 BUILDING_FC: buildingFC, FC_BUILD_H_ZERO: fcBuildHZero, FC_BUILD_H_NULL: fcBuildHNull, FC_BUILD_H_RANGE: fcBuildHRange,
                 FC_BUILD_STATS_ZONE: fcBuildStatsZone, FC_BUILD_STATS_EXT_ZONE: fcBuildStatsExtZone,
                 BUILD_NUMB: buildNumb, BUILD_VALID_ZONE: buildValidZone, BUILD_EMPTY: buildEmpty,
                 BUILD_EQUALS_ZONE: buildEqualsZone, BUILD_OVERLAP_ZONE: buildOverlapZone,
                 BUILD_H: buildH, BUILD_H_RANGE: buildHRange, BUILD_H_WALL_ROOF: buildHWallRoof, BUILD_LEV: buildLev,
                 BUILD_LEV_RANGE: buildLevRange, BUILD_TYPE: buildType, BUILD_TYPE_RANGE: buildTypeRange,
                 BUILD_VALID_EXT_ZONE: buildValidExtZone, BUILD_EQUALS_EXT_ZONE: buildEqualsExtZone,
                 BUILD_OVERLAP_EXT_ZONE: buildOverlapExtZone,
                 ROAD_FC_W_ZERO: roadFCWZero, ROAD_FC_W_NULL: roadFCWNull, ROAD_FC_W_RANGE: roadFCWRange,
                 R_FC_STATS_ZONE: rFCStatsZone, R_FC_STATS_EXT_ZONE: rFCStatsExtZone, ROAD_ZONE: roadZone,
                 ROAD_VALID: roadValid, ROAD_NUMB : roadNumb,
                 ROAD_EMPTY: roadEmpty, ROAD_EQUALS: roadEquals, ROAD_OVERLAP: roadOverlap,
                 ROAD_W: roadW, ROAD_W_RANGE: roadWRange, ROAD_TYPE: roadType, ROAD_TYPE_RANGE: roadTypeRange,
                 RAIL_NB: railNB, RAIL_VALID: railValid, RAIL_EMPTY: railEmpty, RAIL_EQUALS: railEquals,
                 RAIL_OVERLAP: railOverlap, RAIL_TYPE: railType, RAIL_TYPE_RANGE: railTypeRange,
                 HYDRO_NUM_ZONE: hydroNumZone, HYDRO_NB: hydroNum, HYDRO_VALID: hydroValid, HYDRO_EMPTY: hydroEmpty,
                 HYDRO_EQUALS: hydroEquals, HYDRO_OVERLAP: hydroOlverlap,
                 VEGET_NUM_ZONE: vegetNumZone, VEGET_NB: vegetNum, VEGET_VALID: vegetValid, VEGET_EMPTY: vegetEmpty,
                 VEGET_EQUALS: vegetEquals, VEGET_OVERLAP: vegetOverlap, VEGET_TYPE: vegetType, VEGET_TYPE_RANGE: vegetTypeRange,
                 BUILDING: building, BUILDING_STATS_ZONE: buildingStatsZone, BUILDING_STATS_EXT_ZONE: buildingStatsExtZone,
                 ROAD: road, ROAD_STATS_ZONE: roadStatsZone, ROAD_STATS_EXT_ZONE: roadStatsExtZone,
                 RAIL: rail, RAIL_STATS_ZONE: railStatsZone,
                 HYDRO: hydro, HYDRO_STATS_ZONE: hydroStatsZone, HYDRO_STATS_EXT_ZONE: hydroStatsExtZone,
                 VEGET: veget, VEGET_STATS_ZONE: vegetStatsZone, VEGET_STATS_EXT_ZONE: vegetStatsExtZone,
                 IMPERVIOUS: impervious
                ])
        if(!success){
            error "Error occurred on the execution of the inputDataFormatting.sql script"
        }
        else{
            info 'The inputDataFormatting.sql script has been executed'
            [outputBuilding : building, outputBuildingStatZone: buildingStatsZone, outputBuildingStatZoneBuff: buildingStatsExtZone,
             outputRoad: road, outputRoadStatZone: roadStatsZone, outputRoadStatZoneBuff: roadStatsExtZone,
             outputRail: rail, outputRailStatZone: railStatsZone,
             outputHydro: hydro, outputHydroStatZone: hydroStatsZone, outputHydroStatZoneExt: hydroStatsExtZone,
             outputVeget: veget, outputVegetStatZone: vegetStatsZone, outputVegetStatZoneExt: vegetStatsExtZone,
             outputImpervious: impervious,
             outputZone: inputZone
            ]
        }
    }
}