package org.orbisgis.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

@BaseScript BDTopo_V2 BDTopo_V2

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
 * @param buildingAbstractParameters The name of the table in which the abstract building's parameters are stored
 * @param roadAbstractParameters The name of the table in which the abstract road's parameters are stored
 * @param vegetAbstractParameters The name of the table in which the abstract vegetation area's parameters are stored
 *
 * @return outputBuilding Table name in which the (ready to be used in the GeoIndicators part) buildings are stored
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputImpervious Table name in which the (ready to be used in the GeoIndicators part) impervious areas are stored
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 */
IProcess formatData() {
    return create {
        title 'Allows to format and enrich the input data in order to feed the GeoClimate model'
        id "formatData"
        inputs datasource: JdbcDataSource, inputBuilding: String, inputRoad: String, inputRail: String,
                inputHydro: String, inputVeget: String, inputImpervious: String, inputZone: String,
                hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, expand: 1000, idZone: String, buildingAbstractParameters: String,
                roadAbstractParameters: String,vegetAbstractParameters: String
        outputs outputBuilding: String, outputRoad: String,
                outputRail: String, outputHydro: String, outputVeget: String,
                outputImpervious: String, outputZone: String
        run { JdbcDataSource datasource, inputBuilding, inputRoad, inputRail,
              inputHydro, inputVeget, inputImpervious, inputZone,
              hLevMin, hLevMax, hThresholdLev2, expand, idZone,  buildingAbstractParameters,
            roadAbstractParameters,vegetAbstractParameters->
            info('Formating the BDTOPO data')
            def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
            def zoneNeighbors = postfix 'ZONE_NEIGHBORS_'
            def buZone = postfix 'BU_ZONE_'
            def buildWithZone = postfix 'BUILD_WITHIN_ZONE_'
            def buildOuterZone = postfix 'BUILD_OUTER_ZONE_'
            def buildOuterZoneMatrix = postfix 'BUILD_OUTER_ZONE_MATRIX_'
            def buildZoneMatrix = postfix 'BUILD_ZONE_MATRIX_'
            def buildingFC = postfix 'BUILDING_FC_'

            //Final output table names
            def building = 'BUILDING'
            def road = 'ROAD'
            def rail = 'RAIL'
            def hydro = 'HYDRO'
            def veget = 'VEGET'
            def impervious = 'IMPERVIOUS'

            //Run the sql script
            def success = datasource.executeScript(getClass().getResourceAsStream('inputDataFormatting.sql'),
                    [INPUT_BUILDING            : inputBuilding, INPUT_ROAD: inputRoad, INPUT_RAIL: inputRail,
                     INPUT_HYDRO               : inputHydro, INPUT_VEGET: inputVeget, INPUT_IMPERVIOUS: inputImpervious,
                     ZONE                      : inputZone, ZONE_NEIGHBORS: zoneNeighbors,
                     H_LEV_MIN                 : hLevMin, H_LEV_MAX: hLevMax, H_THRESHOLD_LEV2: hThresholdLev2, EXPAND: expand, ID_ZONE: idZone,
                     BUILDING_ABSTRACT_PARAMETERS: buildingAbstractParameters,ROAD_ABSTRACT_PARAMETERS: roadAbstractParameters,
                     VEGET_ABSTRACT_PARAMETERS: vegetAbstractParameters,
                     BU_ZONE                   : buZone, BUILD_WITHIN_ZONE: buildWithZone, BUILD_OUTER_ZONE: buildOuterZone,
                     BUILD_OUTER_ZONE_MATRIX   : buildOuterZoneMatrix, BUILD_ZONE_MATRIX: buildZoneMatrix,
                     BUILDING_FC               : buildingFC,
                     BUILDING                  : building,
                     ROAD                      : road,
                     RAIL                      : rail,
                     HYDRO                     : hydro,
                     VEGET                     : veget,
                     IMPERVIOUS                : impervious
                    ])
            if (!success) {
                error "Cannot format the BD Topo data"
            } else {
                info 'The BD Topo data have been formated'
                //Format population table if exists

                [outputBuilding  : building,
                 outputRoad      : road,
                 outputRail      : rail,
                 outputHydro     : hydro,
                 outputVeget     : veget,
                 outputImpervious: impervious,
                 outputZone      : inputZone
                ]
            }
        }
    }
}