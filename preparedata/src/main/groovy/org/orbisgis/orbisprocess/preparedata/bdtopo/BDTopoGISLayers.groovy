
package org.orbisgis.orbisprocess.preparedata.bdtopo

import groovy.transform.BaseScript
import org.orbisgis.orbisprocess.preparedata.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript PrepareData prepareData

/**
 * This script allows to import, filter and preprocess needed data from BD Topo for a specific ZONE
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param tableIrisName The table name in which the IRIS are stored
 * @param tableBuildIndifName The table name in which the undifferentiated ("Indifférencié" in french) buildings are stored
 * @param tableBuildIndusName The table name in which the industrial buildings are stored
 * @param tableBuildRemarqName The table name in which the remarkable ("Remarquable" in french) buildings are stored
 * @param tableRoadName The table name in which the roads are stored
 * @param tableRailName The table name in which the rail ways are stored
 * @param tableHydroName The table name in which the hydrographic areas are stored
 * @param tableVegetName The table name in which the vegetation areas are stored
 * @param distBuffer The distance (exprimed in meter) used to compute the buffer area around the ZONE
 * @param expand The distance (exprimed in meter) used to compute the extended area around the ZONE
 * @param idZone The ZONE id
 * @param building_bd_topo_use_type The name of the table in which the BD Topo building's use and type are stored
 * @param building_abstract_use_type The name of the table in which the abstract building's use and type are stored
 * @param road_bd_topo_type The name of the table in which the BD Topo road's types are stored
 * @param road_abstract_type The name of the table in which the abstract road's types are stored
 * @param rail_bd_topo_type The name of the table in which the BD Topo rails's types are stored
 * @param rail_abstract_type The name of the table in which the abstract rails's types are stored
 * @param veget_bd_topo_type The name of the table in which the BD Topo vegetation's types are stored
 * @param veget_abstract_type The name of the table in which the abstract vegetation's types are stored
 *
 * @return outputBuildingName Table name in which the (ready to feed the GeoClimate model) buildings are stored
 * @return outputRoadName Table name in which the (ready to feed the GeoClimate model) roads are stored
 * @return outputRailName Table name in which the (ready to feed the GeoClimate model) rail ways are stored
 * @return outputHydroName Table name in which the (ready to feed the GeoClimate model) hydrographic areas are stored
 * @return outputVegetName Table name in which the (ready to feed the GeoClimate model) vegetation areas are stored
 * @return outputZoneName Table name in which the (ready to feed the GeoClimate model) zone is stored
 * @return outputZoneNeighborsName Table name in which the (ready to feed the GeoClimate model) neighboring zones are stored
 */
IProcess importPreprocess(){
    return create({
        title 'Import and preprocess data from BD Topo in order to feed the abstract model'
        inputs datasource               : JdbcDataSource, tableIrisName: String, tableBuildIndifName: String,
         tableBuildIndusName      : String, tableBuildRemarqName: String, tableRoadName: String,
         tableRailName            : String, tableHydroName: String, tableVegetName: String,
         distBuffer               : int, expand: int, idZone: String,
         building_bd_topo_use_type: String, building_abstract_use_type: String, road_bd_topo_type: String,
         road_abstract_type       : String, rail_bd_topo_type: String, rail_abstract_type: String,
         veget_bd_topo_type       : String, veget_abstract_type: String
        outputs outputBuildingName: String, outputRoadName: String, outputRailName: String, outputHydroName: String,
                outputVegetName   : String, outputZoneName: String, outputZoneNeighborsName: String
        run { datasource, tableIrisName, tableBuildIndifName, tableBuildIndusName,
          tableBuildRemarqName, tableRoadName, tableRailName, tableHydroName,
          tableVegetName, distBuffer, expand, idZone,
          building_bd_topo_use_type, building_abstract_use_type, road_bd_topo_type, road_abstract_type,
          rail_bd_topo_type, rail_abstract_type, veget_bd_topo_type, veget_abstract_type ->

            logger.info('Executing the importPreprocess.sql script')
            def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
            def tmpIris = 'TMP_IRIS_' + uuid
            def zone = 'ZONE'
            def zoneBuffer = 'ZONE_BUFFER_' + uuid
            def zoneExtended = 'ZONE_EXTENDED_' + uuid
            def zoneNeighbors = 'ZONE_NEIGHBORS'
            def bu_zone_indif = 'BU_ZONE_INDIF_' + uuid
            def bu_zone_indus = 'BU_ZONE_INDUS_' + uuid
            def bu_zone_remarq = 'BU_ZONE_REMARQ_' + uuid
            def input_building = 'INPUT_BUILDING'
            def input_road = 'INPUT_ROAD'
            def input_rail = 'INPUT_RAIL'
            def input_hydro = 'INPUT_HYDRO'
            def input_veget = 'INPUT_VEGET'

            datasource.executeScript(getClass().getResourceAsStream('importPreprocess.sql'),
                    [ID_ZONE                  : idZone, DIST_BUFFER: distBuffer,
                     EXPAND                   : expand, IRIS_GE: tableIrisName,
                     BATI_INDIFFERENCIE       : tableBuildIndifName, BATI_INDUSTRIEL: tableBuildIndusName,
                     BATI_REMARQUABLE         : tableBuildRemarqName, ROUTE: tableRoadName,
                     TRONCON_VOIE_FERREE      : tableRailName, SURFACE_EAU: tableHydroName,
                     ZONE_VEGETATION          : tableVegetName, TMP_IRIS: tmpIris,
                     ZONE                     : zone, ZONE_BUFFER: zoneBuffer,
                     ZONE_EXTENDED            : zoneExtended, ZONE_NEIGHBORS: zoneNeighbors,
                     BU_ZONE_INDIF            : bu_zone_indif, BU_ZONE_INDUS: bu_zone_indus,
                     BU_ZONE_REMARQ           : bu_zone_remarq, INPUT_BUILDING: input_building,
                     INPUT_ROAD               : input_road, INPUT_RAIL: input_rail,
                     INPUT_HYDRO              : input_hydro, INPUT_VEGET: input_veget,
                     BUILDING_BD_TOPO_USE_TYPE: building_bd_topo_use_type, BUILDING_ABSTRACT_USE_TYPE: building_abstract_use_type,
                     ROAD_BD_TOPO_TYPE        : road_bd_topo_type, ROAD_ABSTRACT_TYPE: road_abstract_type,
                     RAIL_BD_TOPO_TYPE        : rail_bd_topo_type, RAIL_ABSTRACT_TYPE: rail_abstract_type,
                     VEGET_BD_TOPO_TYPE       : veget_bd_topo_type, VEGET_ABSTRACT_TYPE: veget_abstract_type
                    ])

            logger.info('The importPreprocess.sql script has been executed')

            [outputBuildingName: input_building, outputRoadName: input_road,
             outputRailName    : input_rail, outputHydroName: input_hydro, outputVegetName: input_veget,
             outputZoneName    : zone, outputZoneNeighborsName: zoneNeighbors
            ]
        }
    })
}

/**
 * This process initialize the tables in which the objects type (for buildings, roads, rails and vegetation areas) are stored
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param buildingAbstractUseType The name of the table in which the abstract building's use and type are stored
 * @param roadAbstractType The name of the table in which the abstract road's types are stored
 * @param railAbstractType The name of the table in which the abstract rail's types are stored
 * @param vegetAbstractType The name of the table in which the abstract vegetation's types are stored
 *
 * @return outputBuildingBDTopoUse Type The name of the table in which the BD Topo building's use and type are stored
 * @return outputroadBDTopoType The name of the table in which the BD Topo road's types are stored
 * @return outputrailBDTopoType The name of the table in which the BD Topo rail's types are stored
 * @return outputvegetBDTopoType The name of the table in which the BD Topo vegetation's types are stored
 */

IProcess initTypes(){
    return create({
        title 'Initialize the types tables for BD Topo and define the matching with the abstract types'
        inputs datasource: JdbcDataSource, buildingAbstractUseType: String, roadAbstractType: String,
                railAbstractType: String, vegetAbstractType: String
        outputs outputBuildingBDTopoUseType: String, outputroadBDTopoType: String,
                outputrailBDTopoType: String, outputvegetBDTopoType: String
        run { JdbcDataSource datasource, buildingAbstractUseType, roadAbstractType, railAbstractType, vegetAbstractType ->
            logger.info('Executing the typesMatching.sql script')
            def buildingBDTopoUseType = 'BUILDING_BD_TOPO_USE_TYPE'
            def roadBDTopoType = 'ROAD_BD_TOPO_TYPE'
            def railBDTopoType = 'RAIL_BD_TOPO_TYPE'
            def vegetBDTopoType = 'VEGET_BD_TOPO_TYPE'

            datasource.executeScript(getClass().getResourceAsStream('typesMatching.sql'),
                    [BUILDING_ABSTRACT_USE_TYPE: buildingAbstractUseType,
                     ROAD_ABSTRACT_TYPE        : roadAbstractType,
                     RAIL_ABSTRACT_TYPE        : railAbstractType,
                     VEGET_ABSTRACT_TYPE       : vegetAbstractType,
                     BUILDING_BD_TOPO_USE_TYPE : buildingBDTopoUseType,
                     ROAD_BD_TOPO_TYPE         : roadBDTopoType,
                     RAIL_BD_TOPO_TYPE         : railBDTopoType,
                     VEGET_BD_TOPO_TYPE        : vegetBDTopoType,
                    ])

            logger.info('The typesMatching.sql script has been executed')

            [outputBuildingBDTopoUseType: buildingBDTopoUseType, outputroadBDTopoType: roadBDTopoType,
             outputrailBDTopoType       : railBDTopoType, outputvegetBDTopoType: vegetBDTopoType
            ]
        }
    })
}
