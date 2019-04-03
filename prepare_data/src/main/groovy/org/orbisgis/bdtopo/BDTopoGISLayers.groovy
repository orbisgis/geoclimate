
package org.orbisgis.bdtopo

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import javax.lang.model.element.NestingKind

@BaseScript PrepareData prepareData

static IProcess importPreprocess(){
    return processFactory.create(
            'Import and preprocess data from BD Topo in order to feed the abstract model',
            [h2gis: H2GIS, tableIrisName: String, tableBuildIndifName: String, tableBuildIndusName: String,
             tableBuildRemarqName: String, tableRoadName: String, tableRailName: String,
             tableHydroName: String, tableVegetName: String, distBuffer: int, expand: int, idZone: String],
            [outputBuildingName: String, outputRoadName: String, outputRailName: String, outputHydroName: String,
             outputVegetName: String, outputZoneName: String, outputZoneNeighborsName: String],
            {H2GIS h2gis, tableIrisName, tableBuildIndifName, tableBuildIndusName,
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

                h2gis.executeScript(this.class.getResource('importPreprocess.sql').toString(),
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


static IProcess initTypes(){
    return processFactory.create(
            'Initialize the types tables for BD Topo and define the matching with the abstract types',
            [h2gis: H2GIS, buildingAbstractUseType: String, roadAbstractType: String,
             railAbstractType: String, vegetAbstractType: String
            ],
            [outputBuildingBDTopoUseType: String, outputroadBDTopoType: String,
             outputrailBDTopoType: String, outputvegetBDTopoType: String
            ],
            {H2GIS h2gis, buildingAbstractUseType, roadAbstractType, railAbstractType, vegetAbstractType ->
                logger.info('Executing the typesMatching.sql script')
                def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
                def buildingBDTopoUseType = 'BUILDING_BD_TOPO_USE_TYPE_' + uuid
                def roadBDTopoType = 'ROAD_BD_TOPO_TYPE_' + uuid
                def railBDTopoType = 'RAIL_BD_TOPO_TYPE_' + uuid
                def vegetBDTopoType = 'VEGET_BD_TOPO_TYPE_' + uuid

                h2gis.executeScript(this.class.getResource('typesMatching.sql').toString(),
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
