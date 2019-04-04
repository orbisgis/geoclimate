
package org.orbisgis.bdtopo

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

import javax.lang.model.element.NestingKind

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
 *
 * @return outputBuildingName Table name in which the (ready to feed the GeoClimate model) buildings are stored
 * @return outputRoadName Table name in which the (ready to feed the GeoClimate model) roads are stored
 * @return outputRailName Table name in which the (ready to feed the GeoClimate model) rail ways are stored
 * @return outputHydroName Table name in which the (ready to feed the GeoClimate model) hydrographic areas are stored
 * @return outputVegetName Table name in which the (ready to feed the GeoClimate model) vegetation areas are stored
 * @return outputZoneName Table name in which the (ready to feed the GeoClimate model) zone is stored
 * @return outputZoneNeighborsName Table name in which the (ready to feed the GeoClimate model) neighboring zones are stored
 */

static IProcess importPreprocess(){
    return processFactory.create(
            'Import and preprocess data from BD Topo in order to feed the abstract model',
            [datasource: JdbcDataSource, tableIrisName: String, tableBuildIndifName: String, tableBuildIndusName: String,
             tableBuildRemarqName: String, tableRoadName: String, tableRailName: String,
             tableHydroName: String, tableVegetName: String, distBuffer: int, expand: int, idZone: String],
            [outputBuildingName: String, outputRoadName: String, outputRailName: String, outputHydroName: String,
             outputVegetName: String, outputZoneName: String, outputZoneNeighborsName: String],
            {JdbcDataSource datasource, tableIrisName, tableBuildIndifName, tableBuildIndusName,
                tableBuildRemarqName, tableRoadName, tableRailName,
                tableHydroName, tableVegetName, distBuffer, expand, idZone ->

                logger.info('Executing the importPreprocess.sql script')
                def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
                def tmpIris = 'TMP_IRIS_' + uuid
                def zone = 'ZONE_' + uuid
                def zoneBuffer = 'ZONE_BUFFER_' + uuid
                def zoneExtended = 'ZONE_EXTENDED_' + uuid
                def zoneNeighbors = 'ZONE_NEIGHBORS_' + uuid
                def bu_zone_indif = 'BU_ZONE_INDIF_' + uuid
                def bu_zone_indus = 'BU_ZONE_INDUS_' + uuid
                def bu_zone_remarq = 'BU_ZONE_REMARQ_' + uuid
                def input_building = 'INPUT_BUILDING_' + uuid
                def input_road = 'INPUT_ROAD_' + uuid
                def input_rail = 'INPUT_RAIL_' + uuid
                def input_hydro = 'INPUT_HYDRO_' + uuid
                def input_veget = 'INPUT_VEGET_' + uuid
                def building_bd_topo_use_type = 'BUILDING_BD_TOPO_USE_TYPE_' + uuid
                def building_abstract_use_type = 'BUILDING_ABSTRACT_USE_TYPE_' + uuid
                def road_bd_topo_type = 'ROAD_BD_TOPO_TYPE_' + uuid
                def road_abstract_type = 'ROAD_ABSTRACT_TYPE_' + uuid
                def rail_bd_topo_type = 'RAIL_BD_TOPO_TYPE_' + uuid
                def rail_abstract_type = 'RAIL_ABSTRACT_TYPE_' + uuid
                def veget_bd_topo_type = 'VEGET_BD_TOPO_TYPE_' + uuid
                def veget_abstract_type = 'VEGET_ABSTRACT_TYPE_' + uuid

                datasource.executeScript(this.class.getResource('importPreprocess.sql').toString(),
                        [ID_ZONE: idZone, DIST_BUFFER: distBuffer, EXPAND: expand, IRIS_GE: tableIrisName,
                         BATI_INDIFFERENCIE: tableBuildIndifName, BATI_INDUSTRIEL: tableBuildIndusName,
                         BATI_REMARQUABLE: tableBuildRemarqName, ROUTE: tableRoadName, TRONCON_VOIE_FERREE: tableRailName,
                         SURFACE_EAU: tableHydroName,ZONE_VEGETATION: tableVegetName,
                         TMP_IRIS: tmpIris, ZONE: zone, ZONE_BUFFER: zoneBuffer, ZONE_EXTENDED: zoneExtended,
                         ZONE_NEIGHBORS: zoneNeighbors, BU_ZONE_INDIF: bu_zone_indif, BU_ZONE_INDUS: bu_zone_indus,
                         BU_ZONE_REMARQ: bu_zone_remarq, INPUT_BUILDING: input_building, INPUT_ROAD: input_road,
                         INPUT_RAIL: input_rail, INPUT_HYDRO: input_hydro, INPUT_VEGET: input_veget,
                         BUILDING_BD_TOPO_USE_TYPE: building_bd_topo_use_type,
                         BUILDING_ABSTRACT_USE_TYPE: building_abstract_use_type, ROAD_BD_TOPO_TYPE: road_bd_topo_type,
                         ROAD_ABSTRACT_TYPE: road_abstract_type, RAIL_BD_TOPO_TYPE: rail_bd_topo_type,
                         RAIL_ABSTRACT_TYPE: rail_abstract_type, VEGET_BD_TOPO_TYPE: veget_bd_topo_type,
                         VEGET_ABSTRACT_TYPE: veget_abstract_type
                        ])

                logger.info('The importPreprocess.sql script has been executed')

                        [outputBuildingName: 'INPUT_BUILDING', outputRoadName: 'INPUT_ROAD',
                         outputRailName: 'INPUT_RAIL', outputHydroName: 'INPUT_HYDRO', outputVegetName: 'INPUT_VEGET',
                         outputZoneName: 'ZONE', outputZoneNeighborsName: 'ZONE_NEIGHBORS'
                        ]
            }
    )
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

static IProcess initTypes(){
    return processFactory.create(
            'Initialize the types tables for BD Topo and define the matching with the abstract types',
            [datasource: JdbcDataSource, buildingAbstractUseType: String, roadAbstractType: String,
             railAbstractType: String, vegetAbstractType: String
            ],
            [outputBuildingBDTopoUseType: String, outputroadBDTopoType: String,
             outputrailBDTopoType: String, outputvegetBDTopoType: String
            ],
            {JdbcDataSource datasource, buildingAbstractUseType, roadAbstractType, railAbstractType, vegetAbstractType ->
                logger.info('Executing the typesMatching.sql script')
                def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
                def buildingBDTopoUseType = 'BUILDING_BD_TOPO_USE_TYPE_' + uuid
                def roadBDTopoType = 'ROAD_BD_TOPO_TYPE_' + uuid
                def railBDTopoType = 'RAIL_BD_TOPO_TYPE_' + uuid
                def vegetBDTopoType = 'VEGET_BD_TOPO_TYPE_' + uuid

                datasource.executeScript(this.class.getResource('typesMatching.sql').toString(),
                        [BUILDING_ABSTRACT_USE_TYPE: buildingAbstractUseType,
                         ROAD_ABSTRACT_TYPE: roadAbstractType,
                         RAIL_ABSTRACT_TYPE: railAbstractType,
                         VEGET_ABSTRACT_TYPE: vegetAbstractType,
                         BUILDING_BD_TOPO_USE_TYPE: buildingBDTopoUseType,
                         ROAD_BD_TOPO_TYPE: roadBDTopoType,
                         RAIL_BD_TOPO_TYPE: railBDTopoType,
                         VEGET_BD_TOPO_TYPE: vegetBDTopoType,
                        ])

                logger.info('The typesMatching.sql script has been executed')

                [outputBuildingBDTopoUseType: 'BUILDING_BD_TOPO_USE_TYPE', outputroadBDTopoType: 'ROAD_BD_TOPO_TYPE',
                 outputrailBDTopoType: 'RAIL_BD_TOPO_TYPE', outputvegetBDTopoType:'VEGET_BD_TOPO_TYPE'
                ]
            }
    )
}
