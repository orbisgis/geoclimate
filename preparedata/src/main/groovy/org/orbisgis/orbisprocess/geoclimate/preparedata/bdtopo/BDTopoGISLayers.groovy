
package org.orbisgis.orbisprocess.geoclimate.preparedata.bdtopo

import groovy.transform.BaseScript
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
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
 * @param tableImperviousSportName The table name in which the impervious sport areas are stored
 * @param tableImperviousBuildSurfName The table name in which the building impervious surfaces are stored
 * @param tableImperviousRoadSurfName The table name in which the impervious road areas are stored
 * @param tableImperviousActivSurfName The table name in which the impervious activities areas are stored
 * @param distBuffer The distance (expressed in meter) used to compute the buffer area around the ZONE
 * @param expand The distance (expressed in meter) used to compute the extended area around the ZONE
 * @param idZone The ZONE id
 * @param building_bd_topo_use_type The name of the table in which the BD Topo building's use and type are stored
 * @param building_abstract_use_type The name of the table in which the abstract building's use and type are stored
 * @param road_bd_topo_type The name of the table in which the BD Topo road's types are stored
 * @param road_abstract_type The name of the table in which the abstract road's types are stored
 * @param road_bd_topo_crossing The name of the table in which the BD Topo road's crossing are stored
 * @param road_abstract_crossing The name of the table in which the abstract road's crossing are stored
 * @param rail_bd_topo_type The name of the table in which the BD Topo rails's types are stored
 * @param rail_abstract_type The name of the table in which the abstract rails's types are stored
 * @param rail_bd_topo_crossing The name of the table in which the BD Topo rails's crossing are stored
 * @param rail_abstract_crossing The name of the table in which the abstract rails's crossing are stored
 * @param veget_bd_topo_type The name of the table in which the BD Topo vegetation's types are stored
 * @param veget_abstract_type The name of the table in which the abstract vegetation's types are stored
 *
 * @return outputBuildingName Table name in which the (ready to feed the GeoClimate model) buildings are stored
 * @return outputRoadName Table name in which the (ready to feed the GeoClimate model) roads are stored
 * @return outputRailName Table name in which the (ready to feed the GeoClimate model) rail ways are stored
 * @return outputHydroName Table name in which the (ready to feed the GeoClimate model) hydrographic areas are stored
 * @return outputVegetName Table name in which the (ready to feed the GeoClimate model) vegetation areas are stored
 * @return outputImperviousName Table name in which the (ready to feed the GeoClimate model) impervious areas are stored
 * @return outputZoneName Table name in which the (ready to feed the GeoClimate model) zone is stored
 */
IProcess importPreprocess(){
    return create({
        title 'Import and prepare BDTopo layers'
        inputs 	datasource: JdbcDataSource,
                tableIrisName: String,
                tableBuildIndifName: String,
                tableBuildIndusName: String,
                tableBuildRemarqName: String,
                tableRoadName: String,
                tableRailName: String,
                tableHydroName: String,
                tableVegetName: String,
                tableImperviousSportName: String,
                tableImperviousBuildSurfName: String,
                tableImperviousRoadSurfName: String,
                tableImperviousActivSurfName: String,
                distBuffer: 500,
                expand: 1000,
                idZone: String,
                building_bd_topo_use_type: String,
                building_abstract_use_type: String,
                road_bd_topo_type: String,
                road_abstract_type: String,
                road_bd_topo_crossing: String,
                road_abstract_crossing: String,
                rail_bd_topo_type: String,
                rail_abstract_type: String,
                rail_bd_topo_crossing: String,
                rail_abstract_crossing: String,
                veget_bd_topo_type: String,
                veget_abstract_type: String
        outputs outputBuildingName: String, outputRoadName: String, outputRailName: String, outputHydroName: String,
                outputVegetName   : String, outputImperviousName: String, outputZoneName: String
        run { datasource, tableIrisName, tableBuildIndifName, tableBuildIndusName,
              tableBuildRemarqName, tableRoadName, tableRailName, tableHydroName, tableVegetName,
              tableImperviousSportName, tableImperviousBuildSurfName, tableImperviousRoadSurfName,
              tableImperviousActivSurfName, distBuffer, expand, idZone,
              building_bd_topo_use_type, building_abstract_use_type,
              road_bd_topo_type, road_abstract_type, road_bd_topo_crossing, road_abstract_crossing,
              rail_bd_topo_type, rail_abstract_type, rail_bd_topo_crossing, rail_abstract_crossing,
              veget_bd_topo_type, veget_abstract_type ->

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
            def tmp_imperv_construction_surfacique = 'TMP_IMPERV_CONSTRUCTION_SURFACIQUE_' + uuid
            def tmp_imperv_surface_route = 'TMP_IMPERV_SURFACE_ROUTE_' + uuid
            def tmp_imperv_terrain_sport = 'TMP_IMPERV_TERRAIN_SPORT_' + uuid
            def tmp_imperv_surface_activite = 'TMP_IMPERV_SURFACE_ACTIVITE_' + uuid
            def input_impervious = 'INPUT_IMPERVIOUS'

            // -------------------------------------------------------------------------------
            // Control the SRIDs from input tables

            def list = [tableIrisName, tableBuildIndifName, tableBuildIndusName, tableBuildRemarqName,
                        tableRoadName, tableRailName, tableHydroName, tableVegetName,
                        tableImperviousSportName, tableImperviousBuildSurfName,
                        tableImperviousRoadSurfName, tableImperviousActivSurfName]

            // The SRID is stored and initialized to -1
            def srid = -1

            // For each tables in the list, we check the SRID and compare to the srid variable. If different, the process is stopped
            for(String name : list){
                def table = datasource.getTable(name)
                if(table != null){
                    if(srid == -1){
                        srid = table.srid
                    }
                    else{
                        if(srid != table.srid){
                            logger.error "The process has been stopped since the table $name has a different SRID from the others"
                            return null
                        }
                    }
                }
            }

            // -------------------------------------------------------------------------------
            // Check if the input files are present

            // If the IRIS_GE table does not exist or is empty, then the process is stopped
            if(!datasource.hasTable(tableIrisName) || datasource.getTable(tableIrisName).isEmpty()){
                logger.error 'The process has been stopped since the table IRIS_GE does not exist or is empty'
                return null}

            // If the following tables does not exists, we create corresponding empty tables
            if(!datasource.hasTable(tableBuildIndifName)){
                datasource.execute("CREATE TABLE $tableBuildIndifName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer);")
            }
            if(!datasource.hasTable(tableBuildIndusName)){
                datasource.execute("CREATE TABLE $tableBuildIndusName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);")
            }
            if(!datasource.hasTable(tableBuildRemarqName)){
                datasource.execute("CREATE TABLE $tableBuildRemarqName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);")
            }
            if(!datasource.hasTable(tableRoadName)){
                datasource.execute("CREATE TABLE $tableRoadName (THE_GEOM geometry(linestring, $srid), ID varchar, LARGEUR double precision, NATURE varchar, POS_SOL integer, FRANCHISST varchar);")
            }
            if(!datasource.hasTable(tableRailName)){
                datasource.execute("CREATE TABLE $tableRailName (THE_GEOM geometry(linestring, $srid), ID varchar, NATURE varchar, POS_SOL integer, FRANCHISST varchar);")
            }
            if(!datasource.hasTable(tableHydroName)){
                datasource.execute("CREATE TABLE $tableHydroName (THE_GEOM geometry(polygon, $srid), ID varchar);")
            }
            if(!datasource.hasTable(tableVegetName)){
                datasource.execute("CREATE TABLE $tableVegetName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);")
            }
            if(!datasource.hasTable(tableImperviousSportName)){
                datasource.execute("CREATE TABLE $tableImperviousSportName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);")
            }
            if(!datasource.hasTable(tableImperviousBuildSurfName)){
                datasource.execute("CREATE TABLE $tableImperviousBuildSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);")
            }
            if(!datasource.hasTable(tableImperviousRoadSurfName)){
                datasource.execute("CREATE TABLE $tableImperviousRoadSurfName (THE_GEOM geometry(polygon, $srid), ID varchar);")
            }
            if(!datasource.hasTable(tableImperviousActivSurfName)){
                datasource.execute("CREATE TABLE $tableImperviousActivSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, CATEGORIE varchar);")
            }
            // -------------------------------------------------------------------------------

            def success = datasource.executeScript(getClass().getResourceAsStream('importPreprocess.sql'),
                    [ID_ZONE: idZone, DIST_BUFFER: distBuffer, EXPAND: expand,
                     IRIS_GE: tableIrisName, BATI_INDIFFERENCIE: tableBuildIndifName,
                     BATI_INDUSTRIEL: tableBuildIndusName, BATI_REMARQUABLE: tableBuildRemarqName,
                     ROUTE: tableRoadName, TRONCON_VOIE_FERREE: tableRailName,
                     SURFACE_EAU: tableHydroName, ZONE_VEGETATION: tableVegetName,
                     TERRAIN_SPORT: tableImperviousSportName, CONSTRUCTION_SURFACIQUE: tableImperviousBuildSurfName,
                     SURFACE_ROUTE: tableImperviousRoadSurfName, SURFACE_ACTIVITE: tableImperviousActivSurfName,
                     TMP_IRIS: tmpIris,
                     ZONE: zone, ZONE_BUFFER: zoneBuffer, ZONE_EXTENDED: zoneExtended, ZONE_NEIGHBORS: zoneNeighbors,
                     BU_ZONE_INDIF: bu_zone_indif, BU_ZONE_INDUS: bu_zone_indus, BU_ZONE_REMARQ: bu_zone_remarq,
                     INPUT_BUILDING: input_building,
                     INPUT_ROAD: input_road,
                     INPUT_RAIL: input_rail,
                     INPUT_HYDRO: input_hydro,
                     INPUT_VEGET: input_veget,
                     TMP_IMPERV_CONSTRUCTION_SURFACIQUE : tmp_imperv_construction_surfacique, TMP_IMPERV_SURFACE_ROUTE : tmp_imperv_surface_route,
                     TMP_IMPERV_TERRAIN_SPORT : tmp_imperv_terrain_sport, TMP_IMPERV_SURFACE_ACTIVITE : tmp_imperv_surface_activite,
                     INPUT_IMPERVIOUS: input_impervious,
                     BUILDING_BD_TOPO_USE_TYPE: building_bd_topo_use_type, BUILDING_ABSTRACT_USE_TYPE: building_abstract_use_type,
                     ROAD_BD_TOPO_TYPE: road_bd_topo_type, ROAD_ABSTRACT_TYPE: road_abstract_type,
                     ROAD_BD_TOPO_CROSSING: road_bd_topo_crossing, ROAD_ABSTRACT_CROSSING: road_abstract_crossing,
                     RAIL_BD_TOPO_TYPE: rail_bd_topo_type, RAIL_ABSTRACT_TYPE: rail_abstract_type,
                     RAIL_BD_TOPO_CROSSING: rail_bd_topo_crossing, RAIL_ABSTRACT_CROSSING: rail_abstract_crossing,
                     VEGET_BD_TOPO_TYPE: veget_bd_topo_type, VEGET_ABSTRACT_TYPE: veget_abstract_type
                    ])
            if(!success){
                logger.error("Error occurred on the execution of the importPreprocess.sql script")
            }
            else{
                logger.info('The importPreprocess.sql script has been executed')
                [outputBuildingName: input_building, outputRoadName: input_road, outputRailName: input_rail,
                 outputHydroName: input_hydro, outputVegetName: input_veget, outputImperviousName : input_impervious,
                 outputZoneName: zone
                ]
            }
        }
    })
}

/**
 * This process initialize the tables in which the objects type (for buildings, roads, rails and vegetation areas) are stored
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param buildingAbstractUseType The name of the table in which the abstract building's use and type are stored
 * @param roadAbstractType The name of the table in which the abstract road's types are stored
 * @param roadAbstractCrossing The name of the table in which the abstract road's crossing are stored
 * @param railAbstractType The name of the table in which the abstract rail's types are stored
 * @param railAbstractCrossing The name of the table in which the abstract rail's crossing are stored
 * @param vegetAbstractType The name of the table in which the abstract vegetation's types are stored
 *
 * @return outputBuildingBDTopoUseType The name of the table in which the BD Topo building's use and type are stored
 * @return outputroadBDTopoType The name of the table in which the BD Topo road's types are stored
 * @return outputroadBDTopoCrossing The name of the table in which the BD Topo road's crossing are stored
 * @return outputrailBDTopoType The name of the table in which the BD Topo rail's types are stored
 * @return outputrailBDTopoCrossing The name of the table in which the BD Topo rail's crossing are stored
 * @return outputvegetBDTopoType The name of the table in which the BD Topo vegetation's types are stored
 */

IProcess initTypes(){
    return create({
        title 'Initialize the types tables for BD Topo and define the matching with the abstract types'
        inputs datasource: JdbcDataSource, buildingAbstractUseType: String, roadAbstractType: String,
                roadAbstractCrossing: String, railAbstractType: String, railAbstractCrossing: String,
                vegetAbstractType: String
        outputs outputBuildingBDTopoUseType: String, outputroadBDTopoType: String, outputroadBDTopoCrossing: String,
                outputrailBDTopoType: String, outputrailBDTopoCrossing: String, outputvegetBDTopoType: String
        run { JdbcDataSource datasource, buildingAbstractUseType, roadAbstractType, roadAbstractCrossing,
              railAbstractType, railAbstractCrossing, vegetAbstractType ->
            logger.info('Executing the typesMatching.sql script')
            def buildingBDTopoUseType = 'BUILDING_BD_TOPO_USE_TYPE'
            def roadBDTopoType = 'ROAD_BD_TOPO_TYPE'
            def roadBDTopoCrossing = 'ROAD_BD_TOPO_CROSSING'
            def railBDTopoType = 'RAIL_BD_TOPO_TYPE'
            def railBDTopoCrossing = 'RAIL_BD_TOPO_CROSSING'
            def vegetBDTopoType = 'VEGET_BD_TOPO_TYPE'

            def success = datasource.executeScript(getClass().getResourceAsStream('typesMatching.sql'),
                    [BUILDING_ABSTRACT_USE_TYPE: buildingAbstractUseType,
                     ROAD_ABSTRACT_TYPE        : roadAbstractType,
                     ROAD_ABSTRACT_CROSSING    : roadAbstractCrossing,
                     RAIL_ABSTRACT_TYPE        : railAbstractType,
                     RAIL_ABSTRACT_CROSSING    : railAbstractCrossing,
                     VEGET_ABSTRACT_TYPE       : vegetAbstractType,
                     BUILDING_BD_TOPO_USE_TYPE : buildingBDTopoUseType,
                     ROAD_BD_TOPO_TYPE         : roadBDTopoType,
                     ROAD_BD_TOPO_CROSSING     : roadBDTopoCrossing,
                     RAIL_BD_TOPO_TYPE         : railBDTopoType,
                     RAIL_BD_TOPO_CROSSING     : railBDTopoCrossing,
                     VEGET_BD_TOPO_TYPE        : vegetBDTopoType,
                    ])
            if(!success){
                logger.error("Error occurred on the execution of the typesMatching.sql script")
            }
            else{
                logger.info('The typesMatching.sql script has been executed')
                [outputBuildingBDTopoUseType: buildingBDTopoUseType, outputroadBDTopoType: roadBDTopoType,
                 outputroadBDTopoCrossing: roadBDTopoCrossing, outputrailBDTopoType: railBDTopoType,
                 outputrailBDTopoCrossing: railBDTopoCrossing, outputvegetBDTopoType: vegetBDTopoType
                ]
            }
        }
    })
}
