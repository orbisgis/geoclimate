
package org.orbisgis.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

@BaseScript BDTopo_V2 BDTopo_V2

/**
 * This script prepares the BDTopo data already imported in H2GIS database for a specific ZONE
 *
 * @param datasource A connexion to a database (H2GIS), in which the data to process will be stored
 * @param tableCommuneName The table name that represents the commune to process
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
 * @param distance The distance (expressed in meter) used to compute the extended area around the ZONE
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
IProcess prepareBDTopoData() {
    return create {
        title 'Prepare the BDTopo data already imported in the H2GIS database'
        id "importPreprocess"
        inputs datasource: JdbcDataSource,
                tableCommuneName: "",
                tableBuildIndifName: "",
                tableBuildIndusName: "",
                tableBuildRemarqName: "",
                tableRoadName: "",
                tableRailName: "",
                tableHydroName: "",
                tableVegetName: "",
                tableImperviousSportName: "",
                tableImperviousBuildSurfName: "",
                tableImperviousRoadSurfName: "",
                tableImperviousActivSurfName: "",
                tablePiste_AerodromeName:"",
                tableReservoirName :"",
                distBuffer: 500,
                distance: 1000,
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
        outputs outputBuildingName: String,
                outputRoadName: String,
                outputRailName: String,
                outputHydroName: String,
                outputVegetName: String,
                outputImperviousName: String,
                outputZoneName: String
        run { datasource, tableCommuneName, tableBuildIndifName, tableBuildIndusName,
              tableBuildRemarqName, tableRoadName, tableRailName, tableHydroName, tableVegetName,
              tableImperviousSportName, tableImperviousBuildSurfName, tableImperviousRoadSurfName,
              tableImperviousActivSurfName, tablePiste_AerodromeName,tableReservoirName, distBuffer, distance, idZone,
              building_bd_topo_use_type, building_abstract_use_type,
              road_bd_topo_type, road_abstract_type, road_bd_topo_crossing, road_abstract_crossing,
              rail_bd_topo_type, rail_abstract_type, rail_bd_topo_crossing, rail_abstract_crossing,
              veget_bd_topo_type, veget_abstract_type ->

            debug('Import the BDTopo data')
            def zone = postfix 'ZONE'
            def zoneBuffer = postfix 'ZONE_BUFFER_'
            def zoneExtended = postfix 'ZONE_EXTENDED_'
            def bu_zone_indif = postfix 'BU_ZONE_INDIF_'
            def bu_zone_indus = postfix 'BU_ZONE_INDUS_'
            def bu_zone_remarq = postfix 'BU_ZONE_REMARQ_'
            def input_building = 'INPUT_BUILDING'
            def input_road = 'INPUT_ROAD'
            def input_rail = 'INPUT_RAIL'
            def input_hydro = 'INPUT_HYDRO'
            def input_veget = 'INPUT_VEGET'
            def tmp_imperv_construction_surfacique = postfix 'TMP_IMPERV_CONSTRUCTION_SURFACIQUE_'
            def tmp_imperv_surface_route = postfix 'TMP_IMPERV_SURFACE_ROUTE_'
            def tmp_imperv_terrain_sport = postfix 'TMP_IMPERV_TERRAIN_SPORT_'
            def tmp_imperv_surface_activite = postfix 'TMP_IMPERV_SURFACE_ACTIVITE_'
            def input_impervious = 'INPUT_IMPERVIOUS'
            def tmp_imperv_piste_aerodrome = postfix 'TMP_IMPERV_PISTE_AERODROME'
            def bu_zone_reservoir = postfix 'BU_ZONE_RESERVOIR_'

            // If the Commune table is empty, then the process is stopped
            if (!tableCommuneName) {
                error 'The process has been stopped since the table Commnune is empty'
                return
            }

            // -------------------------------------------------------------------------------
            // Control the SRIDs from input tables
            def list = [tableCommuneName, tableBuildIndifName, tableBuildIndusName, tableBuildRemarqName,
                        tableRoadName, tableRailName, tableHydroName, tableVegetName,
                        tableImperviousSportName, tableImperviousBuildSurfName,
                        tableImperviousRoadSurfName, tableImperviousActivSurfName, tablePiste_AerodromeName,tableReservoirName]

            // The SRID is stored and initialized to -1
            def srid = -1

            def tablesExist = []
            // For each tables in the list, we check the SRID and compare to the srid variable. If different, the process is stopped
            for (String name : list) {
                if(name) {
                    def table = datasource.getTable(name)
                    if(table) {
                        def hasRow = datasource.firstRow("select 1 as id from ${name} limit 1".toString())
                        if (hasRow) {
                        tablesExist << name
                        def currentSrid = table.srid
                        if (srid == -1) {
                            srid = currentSrid
                        } else {
                            if (currentSrid == 0) {
                                error "The process has been stopped since the table $name has a no SRID"
                                return
                            } else if (currentSrid > 0 && srid != currentSrid) {
                                error "The process has been stopped since the table $name has a different SRID from the others"
                                return
                            }
                        }
                        }
                    }
                }
            }

            // -------------------------------------------------------------------------------
            // Check if the input files are present

            // If the COMMUNE table does not exist or is empty, then the process is stopped
            if (!tablesExist.contains(tableCommuneName)) {
                error 'The process has been stopped since the table zone does not exist or is empty'
                return
            }

            // If the following tables does not exists, we create corresponding empty tables
            if ( !tablesExist.contains(tableBuildIndifName)) {
                tableBuildIndifName ="BATI_INDIFFERENCIE"
                datasource.execute("DROP TABLE IF EXISTS $tableBuildIndifName; CREATE TABLE $tableBuildIndifName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer);".toString())
            }
            if (!tablesExist.contains(tableBuildIndusName)) {
                tableBuildIndusName="BATI_INDUSTRIEL"
                datasource.execute("DROP TABLE IF EXISTS $tableBuildIndusName; CREATE TABLE $tableBuildIndusName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableBuildRemarqName)) {
                tableBuildRemarqName="BATI_REMARQUABLE"
                datasource.execute("DROP TABLE IF EXISTS $tableBuildRemarqName;  CREATE TABLE $tableBuildRemarqName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableRoadName)) {
                tableRoadName ="ROUTE"
                datasource.execute("DROP TABLE IF EXISTS $tableRoadName;  CREATE TABLE $tableRoadName (THE_GEOM geometry(linestring, $srid), ID varchar, LARGEUR DOUBLE PRECISION, NATURE varchar, POS_SOL integer, FRANCHISST varchar, SENS varchar);".toString())
            }
            if (!tablesExist.contains(tableRailName)) {
                tableRailName = "TRONCON_VOIE_FERREE"
                datasource.execute("DROP TABLE IF EXISTS $tableRailName;  CREATE TABLE $tableRailName (THE_GEOM geometry(linestring, $srid), ID varchar, NATURE varchar, POS_SOL integer, FRANCHISST varchar);".toString())
            }
            if (!tablesExist.contains(tableHydroName)) {
                tableHydroName ="SURFACE_EAU"
                datasource.execute("DROP TABLE IF EXISTS $tableHydroName;  CREATE TABLE $tableHydroName (THE_GEOM geometry(polygon, $srid), ID varchar);".toString())
            }
            if (!tablesExist.contains(tableVegetName)) {
                tableVegetName ="ZONE_VEGETATION"
                datasource.execute("DROP TABLE IF EXISTS $tableVegetName; CREATE TABLE $tableVegetName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousSportName)) {
                tableImperviousSportName = "TERRAIN_SPORT"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousSportName; CREATE TABLE $tableImperviousSportName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousBuildSurfName)) {
                tableImperviousBuildSurfName ="CONSTRUCTION_SURFACIQUE"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousBuildSurfName; CREATE TABLE $tableImperviousBuildSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousRoadSurfName)) {
                tableImperviousRoadSurfName = "SURFACE_ROUTE"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousRoadSurfName; CREATE TABLE $tableImperviousRoadSurfName (THE_GEOM geometry(polygon, $srid), ID varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousActivSurfName)) {
                tableImperviousActivSurfName = "SURFACE_ACTIVITE"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousActivSurfName; CREATE TABLE $tableImperviousActivSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, CATEGORIE varchar);".toString())
            }
            if (!tablesExist.contains(tablePiste_AerodromeName)) {
                tablePiste_AerodromeName= "PISTE_AERODROME"
                datasource.execute("DROP TABLE IF EXISTS $tablePiste_AerodromeName; CREATE TABLE $tablePiste_AerodromeName (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);".toString())
            }

            if (!tablesExist.contains(tableReservoirName)) {
                tableReservoirName= "RESERVOIR"
                datasource.execute("DROP TABLE IF EXISTS $tableReservoirName; CREATE TABLE $tableReservoirName (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar, HAUTEUR integer);".toString())
            }

            // -------------------------------------------------------------------------------

            def success = datasource.executeScript(getClass().getResourceAsStream('prepareImportedBDTopo.sql'),
                    [ID_ZONE                           : idZone,
                     DIST_BUFFER                       : distBuffer,
                     EXPAND                            : distance,
                     COMMUNE                           : tableCommuneName,
                     BATI_INDIFFERENCIE                : tableBuildIndifName,
                     BATI_INDUSTRIEL                   : tableBuildIndusName, BATI_REMARQUABLE: tableBuildRemarqName,
                     ROUTE                             : tableRoadName, TRONCON_VOIE_FERREE: tableRailName,
                     SURFACE_EAU                       : tableHydroName, ZONE_VEGETATION: tableVegetName,
                     TERRAIN_SPORT                     : tableImperviousSportName, CONSTRUCTION_SURFACIQUE: tableImperviousBuildSurfName,
                     SURFACE_ROUTE                     : tableImperviousRoadSurfName, SURFACE_ACTIVITE: tableImperviousActivSurfName,
                     PISTE_AERODROME                   : tablePiste_AerodromeName,
                     RESERVOIR                         : tableReservoirName,
                     ZONE                              : zone,
                     ZONE_BUFFER                       : zoneBuffer,
                     ZONE_EXTENDED                     : zoneExtended,
                     BU_ZONE_INDIF                     : bu_zone_indif, BU_ZONE_INDUS: bu_zone_indus, BU_ZONE_REMARQ: bu_zone_remarq,
                     INPUT_BUILDING                    : input_building,
                     INPUT_ROAD                        : input_road,
                     INPUT_RAIL                        : input_rail,
                     INPUT_HYDRO                       : input_hydro,
                     INPUT_VEGET                       : input_veget,
                     TMP_IMPERV_CONSTRUCTION_SURFACIQUE: tmp_imperv_construction_surfacique, TMP_IMPERV_SURFACE_ROUTE: tmp_imperv_surface_route,
                     TMP_IMPERV_TERRAIN_SPORT          : tmp_imperv_terrain_sport, TMP_IMPERV_SURFACE_ACTIVITE: tmp_imperv_surface_activite,
                     INPUT_IMPERVIOUS                  : input_impervious,
                     TMP_IMPERV_PISTE_AERODROME        : tmp_imperv_piste_aerodrome,
                     BU_ZONE_RESERVOIR                 : bu_zone_reservoir,
                     BUILDING_BD_TOPO_USE_TYPE         : building_bd_topo_use_type, BUILDING_ABSTRACT_USE_TYPE: building_abstract_use_type,
                     ROAD_BD_TOPO_TYPE                 : road_bd_topo_type, ROAD_ABSTRACT_TYPE: road_abstract_type,
                     ROAD_BD_TOPO_CROSSING             : road_bd_topo_crossing, ROAD_ABSTRACT_CROSSING: road_abstract_crossing,
                     RAIL_BD_TOPO_TYPE                 : rail_bd_topo_type, RAIL_ABSTRACT_TYPE: rail_abstract_type,
                     RAIL_BD_TOPO_CROSSING             : rail_bd_topo_crossing, RAIL_ABSTRACT_CROSSING: rail_abstract_crossing,
                     VEGET_BD_TOPO_TYPE                : veget_bd_topo_type, VEGET_ABSTRACT_TYPE: veget_abstract_type,
                     SRID: srid
                    ])
            if (!success) {
                error("Error occurred when importing the BD Topo data")
            } else {
                info('The  BD Topo data have been imported')
                [outputBuildingName: input_building, outputRoadName: input_road, outputRailName: input_rail,
                 outputHydroName   : input_hydro, outputVegetName: input_veget, outputImperviousName: input_impervious,
                 outputZoneName    : zone
                ]
            }
        }
    }
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
IProcess initTypes() {
    return create {
        title 'Initialize the types tables for BD Topo and define the matching with the abstract types'
        id "initTypes"
        inputs datasource: JdbcDataSource, buildingAbstractUseType: String, roadAbstractType: String,
                roadAbstractCrossing: String, railAbstractType: String, railAbstractCrossing: String,
                vegetAbstractType: String
        outputs outputBuildingBDTopoUseType: String, outputroadBDTopoType: String, outputroadBDTopoCrossing: String,
                outputrailBDTopoType: String, outputrailBDTopoCrossing: String, outputvegetBDTopoType: String
        run { JdbcDataSource datasource, buildingAbstractUseType, roadAbstractType, roadAbstractCrossing,
              railAbstractType, railAbstractCrossing, vegetAbstractType ->
            debug 'Executing the typesMatching.sql script'
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
            if (!success) {
                error "Error occurred on the execution of the typesMatching.sql script"
            } else {
                debug 'The typesMatching.sql script has been executed'
                [outputBuildingBDTopoUseType: buildingBDTopoUseType, outputroadBDTopoType: roadBDTopoType,
                 outputroadBDTopoCrossing   : roadBDTopoCrossing, outputrailBDTopoType: railBDTopoType,
                 outputrailBDTopoCrossing   : railBDTopoCrossing, outputvegetBDTopoType: vegetBDTopoType
                ]
            }
        }
    }
}

/**
 * This process initialize the abstract tables in which the objects type and parameters (for buildings, roads, rails
 * hydrographic areas and vegetation areas) are stored
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 *
 * @return outputBuildingAbstractUseType The name of the table in which the abstract building's use and type are stored
 * @return outputBuildingAbstractParameters The name of the table in which the abstract building's parameters are stored
 * @return outputRoadAbstractType The name of the table in which the abstract road's types are stored
 * @return outputRoadAbstractSurface The name of the table in which the abstract road's surfaces are stored
 * @return outputRoadAbstractParameters The name of the table in which the abstract road's parameters are stored
 * @return outputRoadAbstractCrossing The name of the table in which the abstract road's crossing are stored
 * @return outputRailAbstractType The name of the table in which the abstract rail's types stored
 * @return outputRailAbstractCrossing The name of the table in which the abstract rail's crossing are stored
 * @return outputVegetAbstractType The name of the table in which the abstract vegetation's types are stored
 * @return outputVegetAbstractParameters The name of the table in which the abstract vegetation's parameters are stored
 */
IProcess initParametersAbstract() {
    return create {
        title 'Initialize the abstract and parameter tables'
        id "initParametersAbstract"
        inputs datasource: JdbcDataSource
        outputs outputBuildingAbstractUseType: String, outputBuildingAbstractParameters: String,
                outputRoadAbstractType: String, outputRoadAbstractSurface: String, outputRoadAbstractParameters: String,
                outputRoadAbstractCrossing: String, outputRailAbstractType: String, outputRailAbstractCrossing: String,
                outputVegetAbstractType: String, outputVegetAbstractParameters: String
        run { JdbcDataSource datasource ->
            debug 'Executing the parametersAndAbstractTables.sql script'
            def buildingAbstractUseType = 'BUILDING_ABSTRACT_USE_TYPE'
            def buildingAbstractParam = 'BUILDING_ABSTRACT_PARAMETERS'
            def roadAbstractType = 'ROAD_ABSTRACT_TYPE'
            def roadAbstractSurface = 'ROAD_ABSTRACT_SURFACE'
            def roadAbstractParam = 'ROAD_ABSTRACT_PARAMETERS'
            def roadAbstractCrossing = 'ROAD_ABSTRACT_CROSSING'
            def railAbstractType = 'RAIL_ABSTRACT_TYPE'
            def railAbstractCrossing = 'RAIL_ABSTRACT_CROSSING'
            def vegetAbstractType = 'VEGET_ABSTRACT_TYPE'
            def vegetAbstractParam = 'VEGET_ABSTRACT_PARAMETERS'

            datasource.executeScript(getClass().getResourceAsStream('parametersAndAbstractTables.sql'),
                    [BUILDING_ABSTRACT_USE_TYPE  : buildingAbstractUseType,
                     BUILDING_ABSTRACT_PARAMETERS: buildingAbstractParam,
                     ROAD_ABSTRACT_TYPE          : roadAbstractType,
                     ROAD_ABSTRACT_SURFACE       : roadAbstractSurface,
                     ROAD_ABSTRACT_PARAMETERS    : roadAbstractParam,
                     ROAD_ABSTRACT_CROSSING      : roadAbstractCrossing,
                     RAIL_ABSTRACT_TYPE          : railAbstractType,
                     RAIL_ABSTRACT_CROSSING      : railAbstractCrossing,
                     VEGET_ABSTRACT_TYPE         : vegetAbstractType,
                     VEGET_ABSTRACT_PARAMETERS   : vegetAbstractParam
                    ])

            debug 'The parametersAndAbstractTables.sql script has been executed'

            [outputBuildingAbstractUseType   : buildingAbstractUseType,
             outputBuildingAbstractParameters: buildingAbstractParam,
             outputRoadAbstractType          : roadAbstractType,
             outputRoadAbstractSurface       : roadAbstractSurface,
             outputRoadAbstractParameters    : roadAbstractParam,
             outputRoadAbstractCrossing      : roadAbstractCrossing,
             outputRailAbstractType          : railAbstractType,
             outputRailAbstractCrossing      : railAbstractCrossing,
             outputVegetAbstractType         : vegetAbstractType,
             outputVegetAbstractParameters   : vegetAbstractParam
            ]
        }
    }
}