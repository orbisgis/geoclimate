package org.orbisgis.processingchains

import groovy.transform.BaseScript
import org.orbisgis.SpatialUnits
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanager.ProcessManager
import org.orbisgis.processmanager.ProcessMapper
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.processmanagerapi.IProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class ProcessingChains extends Script {
    public static IProcessFactory processFactory = ProcessManager.getProcessManager().factory("processingchains")

    public static Logger logger = LoggerFactory.getLogger(ProcessingChains.class)

    /**
     * This process chains a set of subprocesses to extract and transform the OSM data into
     * the geoclimate model
     *
     * @return
     */
    static IProcess prepareOSM() {
        return processFactory.create("Extract and transform OSM data to Geoclimate model",
                [dbPath : String,
                 osmTablesPrefix: String,
                 idZone : String,
                 expand : int,
                 distBuffer : int,
                 hLevMin: int,
                 hLevMax: int,
                 hThresholdLev2: int,
                 buildingTableColumnsNames: Map,
                 buildingTagKeys: String[],
                 buildingTagValues: String[],
                 tablesPrefix: String,
                 buildingFilter: String,
                 roadTableColumnsNames: Map,
                 roadTagKeys: String[],
                 roadTagValues: String[],
                 roadFilter: String,
                 railTableColumnsNames: Map,
                 railTagKeys: String[],
                 railTagValues: String[],
                 railFilter: String[],
                 vegetTableColumnsNames: Map,
                 vegetTagKeys: String[],
                 vegetTagValues: String[],
                 vegetFilter: String,
                 hydroTableColumnsNames: Map ,
                 hydroTags: Map,
                 hydroFilter: String,
                 mappingForTypeAndUse : Map,
                 mappingForRoadType : Map,
                 mappingForSurface : Map,
                 mappingForRailType : Map,
                 mappingForVegetType : Map],
                [message: String],
                { dbPath, osmTablesPrefix, idZone , expand, distBuffer,  hLevMin, hLevMax,hThresholdLev2, buildingTableColumnsNames,
                    buildingTagKeys,buildingTagValues,
                    tablesPrefix,
                    buildingFilter,
                    roadTableColumnsNames,
                    roadTagKeys,
                    roadTagValues,
                    roadFilter,
                    railTableColumnsNames,
                    railTagKeys,
                    railTagValues,
                    railFilter,
                    vegetTableColumnsNames,
                    vegetTagKeys,
                    vegetTagValues,
                    vegetFilter,
                    hydroTableColumnsNames,
                    hydroTags,
                    hydroFilter,
                    mappingForTypeAndUse,
                    mappingForRoadType,
                    mappingForSurface,
                    mappingForRailType,
                    mappingForVegetType -> ;

                    IProcess loadInitialData = org.orbisgis.osm.OSMGISLayers.loadInitialData()

                    loadInitialData.execute([
                            dbPath : dbPath,
                            osmTablesPrefix: osmTablesPrefix,
                            idZone : idZone,
                            expand : expand,
                            distBuffer:distBuffer])

                    //The connection to the database
                    def datasource = oadInitialData.getResults().outDatasource

                    if(datasource==null){
                        logger.error("Cannot create the database to store the osm data")
                        return
                    }
                    //Init model
                    IProcess initParametersAbstract = org.orbisgis.common.AbstractTablesInitialization.initParametersAbstract()
                    initParametersAbstract.execute(datasource:datasource)

                    initParametersAbstract.getResults()

                    IProcess inputDataFormatting = org.orbisgis.common.InputDataFormatting.inputDataFormatting()

                    inputDataFormatting.execute([datasource: h2GISDatabase,
                                     inputBuilding: 'INPUT_BUILDING', inputRoad: 'INPUT_ROAD', inputRail: 'INPUT_RAIL',
                                     inputHydro: 'INPUT_HYDRO', inputVeget: 'INPUT_VEGET',
                                     inputZone: 'ZONE', inputZoneNeighbors: 'ZONE_NEIGHBORS',

                                     hLevMin: 3, hLevMax: 15, hThresholdLev2: 10, idZone: '56260',

                                     buildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE', buildingAbstractParameters: 'BUILDING_ABSTRACT_PARAMETERS',
                                     roadAbstractType: 'ROAD_ABSTRACT_TYPE', roadAbstractParameters: 'ROAD_ABSTRACT_PARAMETERS',
                                     railAbstractType: 'RAIL_ABSTRACT_TYPE',
                                     vegetAbstractType: 'VEGET_ABSTRACT_TYPE', vegetAbstractParameters: 'VEGET_ABSTRACT_PARAMETERS'])




                    IProcess prepareBuildings = org.orbisgis.osm.OSMGISLayers.prepareBuildings()
                    IProcess prepareRoads = org.orbisgis.osm.OSMGISLayers.prepareRoads()
                    IProcess prepareRails = org.orbisgis.osm.OSMGISLayers.prepareRails()
                    IProcess prepareVeget = org.orbisgis.osm.OSMGISLayers.prepareVeget()
                    IProcess prepareHydro = org.orbisgis.osm.OSMGISLayers.prepareHydro()
                    IProcess transformBuildings = org.orbisgis.osm.FormattingForAbstractModel.transformBuildings()
                    IProcess transformRoads = org.orbisgis.osm.FormattingForAbstractModel.transformRoads()
                    IProcess transformRails = org.orbisgis.osm.FormattingForAbstractModel.transformRails()
                    IProcess transformVeget = org.orbisgis.osm.FormattingForAbstractModel.transformVeget()
                    IProcess transformHydro = org.orbisgis.osm.FormattingForAbstractModel.transformHydro()



                })
    }
}

