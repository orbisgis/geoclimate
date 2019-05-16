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
                [directory : String,
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
                 mappingForVegetType : Map,
                 saveResults : boolean],
                [message: String],
                { directory, osmTablesPrefix, idZone , expand, distBuffer,  hLevMin, hLevMax,hThresholdLev2, buildingTableColumnsNames,
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
                    mappingForVegetType,
                    saveResults -> ;

                    if(directory==null){
                        logger.info("The directory to save the data cannot be null")
                        return
                    }
                    File dirFile = new File(directory)

                    if(!dirFile.exists()){
                        dirFile.mkdir()
                        logger.info("The folder ${directory} has been created")
                    }
                    else if (!dirFile.isDirectory()){
                        logger.info("Invalid directory path")
                        return
                    }


                    String dbPath = dirFile.absolutePath+ File.separator+ "osmdb"

                    IProcess loadInitialData = org.orbisgis.osm.OSMGISLayers.loadInitialData()

                    loadInitialData.execute([
                            dbPath : dbPath,
                            osmTablesPrefix: osmTablesPrefix,
                            idZone : idZone,
                            expand : expand,
                            distBuffer:distBuffer])

                    logger.info("The OSM data has been downloaded for the zone id : ${idZone}.")

                    //The connection to the database
                    JdbcDataSource datasource = loadInitialData.getResults().outDatasource

                    if(datasource==null){
                        logger.error("Cannot create the database to store the osm data")
                        return
                    }
                    //Init model
                    IProcess initParametersAbstract = org.orbisgis.common.AbstractTablesInitialization.initParametersAbstract()
                    initParametersAbstract.execute(datasource:datasource)

                    logger.info("The geoclimate data model has been initialized.")

                    logger.info("Transform OSM data to GIS tables.")

                    IProcess prepareBuildings = org.orbisgis.osm.OSMGISLayers.prepareBuildings()

                    prepareBuildings.execute([datasource:datasource, osmTablesPrefix:osmTablesPrefix,
                                              buildingTableColumnsNames : buildingTableColumnsNames,
                                              buildingTagKeys:buildingTagKeys,
                                              buildingTagValues:buildingTagValues,
                                              buildingTagValues:buildingTagValues,
                                              tablesPrefix:tablesPrefix,
                                              buildingFilter:buildingFilter,
                    ]);
                    IProcess prepareRoads = org.orbisgis.osm.OSMGISLayers.prepareRoads()
                    prepareRoads.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          roadTableColumnsNames: roadTableColumnsNames,
                                          roadTagKeys: roadTagKeys,
                                          roadTagValues: roadTagValues,
                                          tablesPrefix: tablesPrefix,
                                          roadFilter: roadFilter])

                    IProcess prepareRails = org.orbisgis.osm.OSMGISLayers.prepareRails()
                    prepareRails.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          railTableColumnsNames: railTableColumnsNames,
                                          railTagKeys: railTagKeys,
                                          railTagValues: railTagValues,
                                          tablesPrefix: tablesPrefix,
                                          railFilter: railFilter])

                    IProcess prepareVeget = org.orbisgis.osm.OSMGISLayers.prepareVeget()
                    prepareVeget.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          vegetTableColumnsNames: vegetTableColumnsNames,
                                          vegetTagKeys: vegetTagKeys,
                                          vegetTagValues: vegetTagValues,
                                          tablesPrefix: tablesPrefix,
                                          vegetFilter: vegetFilter])

                    IProcess prepareHydro = org.orbisgis.osm.OSMGISLayers.prepareHydro()
                    prepareHydro.execute([datasource: datasource,
                                          osmTablesPrefix: osmTablesPrefix,
                                          hydroTableColumnsNames: hydroTableColumnsNames,
                                          hydroTags: hydroTags,
                                          tablesPrefix: tablesPrefix,
                                          hydroFilter: hydroFilter])

                    IProcess transformBuildings = org.orbisgis.osm.FormattingForAbstractModel.transformBuildings()
                    transformBuildings.execute([datasource : datasource,
                            inputTableName      : prepareBuildings.getResults().buildingTableName,
                            mappingForTypeAndUse: mappingForTypeAndUse])
                    def inputBuilding =  transformBuildings.getResults().outputTableName

                    IProcess transformRoads = org.orbisgis.osm.FormattingForAbstractModel.transformRoads()
                    transformRoads.execute([datasource : datasource,
                            inputTableName      : prepareRoads.getResults().roadTableName,
                            mappingForRoadType: mappingForRoadType,
                            mappingForSurface: mappingForSurface])
                    def inputRoads =  transformRoads.getResults().outputTableName


                    IProcess transformRails = org.orbisgis.osm.FormattingForAbstractModel.transformRails()
                    transformRails.execute([datasource          : datasource,
                            inputTableName : prepareRails.getResults().railTableName,
                            mappingForRailType: mappingForRailType])
                    def inputRail =  transformRails.getResults().outputTableName

                    IProcess transformVeget = org.orbisgis.osm.FormattingForAbstractModel.transformVeget()
                    transformVeget.execute([datasource    : datasource,
                                            inputTableName: prepareVeget.getResults().vegetTableName,
                                            mappingForVegetType: mappingForVegetType])
                    def inputVeget =  transformVeget.getResults().outputTableName

                    IProcess transformHydro = org.orbisgis.osm.FormattingForAbstractModel.transformHydro()
                    transformHydro.execute([datasource    : datasource,
                                            inputTableName: prepareHydro.getResults().hydroTableName])
                    def inputHydro =  transformHydro.getResults().outputTableName

                    logger.info("All OSM data have been tranformed to GIS tables.")

                    logger.info("Formating GIS tables to Geoclimate model...")

                    def initResults = initParametersAbstract.getResults()

                    def inputZone = loadInitialData.getResults().outputZone
                    def inputZoneNeighbors = loadInitialData.getResults().outputZoneNeighbors

                    IProcess inputDataFormatting = org.orbisgis.common.InputDataFormatting.inputDataFormatting()

                    inputDataFormatting.execute([datasource: datasource,
                                     inputBuilding: inputBuilding, inputRoad: inputRoads, inputRail: inputRail,
                                     inputHydro: inputHydro, inputVeget: inputVeget,
                                     inputZone: inputZone, inputZoneNeighbors: inputZoneNeighbors,
                                     hLevMin: hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2, idZone: idZone,
                                     buildingAbstractUseType: initResults.outputBuildingAbstractUseType, buildingAbstractParameters: initResults.outputBuildingAbstractParameters,
                                     roadAbstractType: initResults.outputRoadAbstractType, roadAbstractParameters: initResults.outputRoadAbstractParameters,
                                     railAbstractType: initResults.outputRailAbstractType,
                                     vegetAbstractType: initResults.outputVegetAbstractType,
                                                 vegetAbstractParameters: initResults.outputVegetAbstractParameters])

                    logger.info("End of the OSM extract transform process.")

                    if(saveResults){

                        logger.info("Saving GIS layers in geojson format")
                        String finalBuildings = inputDataFormatting.getResults().outputBuilding
                        datasource.save(finalBuildings, dirFile.absolutePath+File.separator+"${finalBuildings}_${idZone}.geojson")

                        String finalRoads = inputDataFormatting.getResults().outputRoad
                        datasource.save(finalRoads, dirFile.absolutePath+File.separator+"${finalRoads}_${idZone}.geojson")

                        String finalRails= inputDataFormatting.getResults().outputRail
                        datasource.save(finalRails, dirFile.absolutePath+File.separator+"${finalRails}_${idZone}.geojson")

                        String finalHydro = inputDataFormatting.getResults().outputHydro
                        datasource.save(finalHydro, dirFile.absolutePath+File.separator+"${finalHydro}_${idZone}.geojson")

                        String finalVeget = inputDataFormatting.getResults().outputVeget
                        datasource.save(finalVeget, dirFile.absolutePath+File.separator+"${finalVeget}_${idZone}.geojson")

                        String finalZone = inputDataFormatting.getResults().outputZone
                        datasource.save(finalZone, dirFile.absolutePath+File.separator+"${finalZone}_${idZone}.geojson")

                        logger.info("Saving statistic tables in csv format")

                        String finalOutputBuildingStatZone = inputDataFormatting.getResults().outputBuildingStatZone
                        datasource.save(finalOutputBuildingStatZone, dirFile.absolutePath+File.separator+"${finalOutputBuildingStatZone}_${idZone}.csv")

                        String finalOutputBuildingStatZoneBuff = inputDataFormatting.getResults().outputBuildingStatZoneBuff
                        datasource.save(finalOutputBuildingStatZoneBuff, dirFile.absolutePath+File.separator+"${finalOutputBuildingStatZoneBuff}_${idZone}.csv")

                        String finalOutputRoadStatZone = inputDataFormatting.getResults().outputRoadStatZone
                        datasource.save(finalOutputRoadStatZone, dirFile.absolutePath+File.separator+"${finalOutputRoadStatZone}_${idZone}.csv")

                        String finalOutputRoadStatZoneBuff = inputDataFormatting.getResults().outputRoadStatZoneBuff
                        datasource.save(finalOutputRoadStatZoneBuff, dirFile.absolutePath+File.separator+"${finalOutputRoadStatZoneBuff}_${idZone}.csv")

                        String finalOutputRailStatZone = inputDataFormatting.getResults().outputRailStatZone
                        datasource.save(finalOutputRailStatZone, dirFile.absolutePath+File.separator+"${finalOutputRailStatZone}_${idZone}.csv")

                        String finalOutputHydroStatZone = inputDataFormatting.getResults().outputHydroStatZone
                        datasource.save(finalOutputHydroStatZone, dirFile.absolutePath+File.separator+"${finalOutputHydroStatZone}_${idZone}.csv")

                        String finalOutputHydroStatZoneExt= inputDataFormatting.getResults().outputHydroStatZoneExt
                        datasource.save(finalOutputHydroStatZoneExt, dirFile.absolutePath+File.separator+"${finalOutputHydroStatZoneExt}_${idZone}.csv")

                        String finalOutputVegetStatZone = inputDataFormatting.getResults().outputVegetStatZone
                        datasource.save(finalOutputVegetStatZone, dirFile.absolutePath+File.separator+"${finalOutputVegetStatZone}_${idZone}.csv")

                        String finalOutputVegetStatZoneExt = inputDataFormatting.getResults().outputVegetStatZoneExt
                        datasource.save(finalOutputVegetStatZoneExt, dirFile.absolutePath+File.separator+"${finalOutputVegetStatZoneExt}_${idZone}.csv")

                    }

                    [message: "Sucess"]

                })
    }
}

