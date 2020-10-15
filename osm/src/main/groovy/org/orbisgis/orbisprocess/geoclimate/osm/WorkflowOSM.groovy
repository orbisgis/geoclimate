package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2.tools.DeleteDbFiles
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisanalysis.osm.utils.Utilities
import org.orbisgis.orbisanalysis.osm.utils.OSMElement
import org.orbisgis.orbisdata.datamanager.api.dataset.ITable
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import org.orbisgis.orbisprocess.geoclimate.processingchain.ProcessingChain
import org.orbisgis.orbisanalysis.osm.OSMTools

import java.sql.Connection
import java.sql.SQLException

@BaseScript OSM_Utils osm_utils

/**
 * Extract OSM data and compute geoindicators. The parameters of the processing chain is defined
 * from a configuration file.
 * The configuration file is stored in a json format
 *
 * @param configurationFile The path of the configuration file
 *
 * The configuration file supports the following entries
 *
 * * {
 *  * [OPTIONAL ENTRY] "description" :"A description for the configuration file"
 *  *
 *  * [OPTIONAL ENTRY] "geoclimatedb" : { // Local H2GIS database used to run the processes
 *  *                                    // A default db is build when this entry is not specified
 *  *         "folder" : "/tmp/", //The folder to store the database
 *  *         "name" : "geoclimate_db;AUTO_SERVER=TRUE" // A name for the database
 *  *         "delete" :false
 *  *     },
 *  * [REQUIRED]   "input" : {
 *  *            "osm" : ["filter"] // OSM filter to extract the data. Can be a place name supported by nominatim
 *                                  // e.g "osm" : ["oran", "plourivo"]
 *                                  // or bbox expressed as "osm" : [[38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417]]
 *  *           "all":true //optional value if the user wants to download a limited set of data. Default is true to download all OSM elements
 *              }
 *  *             ,
 *  *  [OPTIONAL ENTRY]  "output" :{ //If not ouput is set the results are keep in the local database
 *  *             "srid" : //optional value to reproject the data
 *  *             "folder" : "/tmp/myResultFolder" //tmp folder to store the computed layers in a geojson format,
 *  *             "database": { //database parameters to store the computed layers.
 *  *                  "user": "-",
 *  *                  "password": "-",
 *  *                  "url": "jdbc:postgresql://", //JDBC url to connect with the database
 *  *                  "tables": { //table names to store the result layers. Create the table if it doesn't exist
 *  *                      "building_indicators":"building_indicators",
 *  *                      "block_indicators":"block_indicators",
 *  *                      "rsu_indicators":"rsu_indicators",
 *  *                      "rsu_lcz":"rsu_lcz",
 *  *                      "zones":"zones"} }
 *  *     },
 *  *     ,
 *  *   [OPTIONAL ENTRY]  "parameters":
 *  *     {"distance" : 1000,
 *  *         "indicatorUse": ["LCZ", "URBAN_TYPOLOGY", "TEB"],
 *  *         "svfSimplified": false,
 *  *         "prefixName": "",
 *  *         "mapOfWeights":
 *  *         {"sky_view_factor": 1,
 *  *             "aspect_ratio": 1,
 *  *             "building_surface_fraction": 1,
 *  *             "impervious_surface_fraction" : 1,
 *  *             "pervious_surface_fraction": 1,
 *  *             "height_of_roughness_elements": 1,
 *  *             "terrain_roughness_length": 1},
 *  *         "hLevMin": 3,
 *  *         "hLevMax": 15,
 *  *         "hThresho2": 10
 *  *     }
 *  *     }
 *  The parameters entry tag contains all geoclimate chain parameters.
 *  When a parameter is not specificied a default value is set.
 *
 * - distance The integer value to expand the envelope of zone when recovering the data
 * - distance The integer value to expand the envelope of zone when recovering the data
 * (some objects may be badly truncated if they are not within the envelope)
 * - indicatorUse List of geoindicator types to compute (default ["LCZ", "URBAN_TYPOLOGY", "TEB"]
 *                  --> "LCZ" : compute the indicators needed for the LCZ classification (Stewart et Oke, 2012)
 *                  --> "URBAN TYPOLOGY" : compute the indicators needed for the urban typology classification (Bocher et al., 2017)
 *                  --> "TEB" : compute the indicators needed for the Town Energy Balance model
 * - svfSimplified A boolean indicating whether or not the simplified version of the SVF should be used. This
 * version is faster since it is based on a simple relationship between ground SVF calculated at RSU scale and
 * facade density (Bernard et al. 2018).
 * - prefixName A prefix used to name the output table (default ""). Could be useful in case the user wants to
 * investigate the sensibility of the chain to some input parameters
 * - mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
 * - hLevMin Minimum building level height
 * - hLevMax Maximum building level height
 * - hThresholdLev2 Threshold on the building height, used to determine the number of levels
 *
 * @return a message if the geoclimate chain has been executed, otherwise throw an error.
 *
 * References:
 * --> Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 * --> Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. Sky View Factor Calculation in
 * Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools. Climate ,
 * MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American
 * Meteorological Society 93, no. 12 (2012): 1879-1900.
 *
 */
IProcess workflow() {
    return create {
        title "Create all Geoindicators from OSM data"
        id "workflow"
        inputs configurationFile: ""
        outputs outputMessage: String
        run { configurationFile ->
            def configFile
            if (configurationFile) {
                configFile = new File(configurationFile)
                if (!configFile.isFile()) {
                    error "Parameters file not found"
                    return null
                }
            } else {
                error "The file parameters cannot be null or empty"
                return null
            }
            Map parameters = readJSONParameters(configFile)

            if (parameters) {
                info "Reading file parameters from $configFile"
                info parameters.get("description")
                def input = parameters.get("input")
                def output = parameters.get("output")
                //Default H2GIS database properties
                def databaseFolder = System.getProperty("java.io.tmpdir")
                def databaseName = "osm"
                def databasePath = postfix databaseFolder + File.separator + databaseName
                def h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
                def delete_h2gis = true
                def geoclimatedb = parameters.get("geoclimatedb")
                if (geoclimatedb) {
                    def h2gis_folder = geoclimatedb.get("folder")
                    if(h2gis_folder){
                        databaseFolder=h2gis_folder
                    }
                    def h2gis_name= geoclimatedb.get("name")
                    if(h2gis_name){
                        def dbName = h2gis_name.split(";")
                        databaseName=dbName[0]
                    }
                    databasePath =  databaseFolder + File.separator + databaseName
                    def delete_h2gis_db = geoclimatedb.get("delete")
                    if (delete_h2gis_db == null) {
                        delete_h2gis = true
                    } else if (delete_h2gis_db instanceof String) {
                        delete_h2gis = true
                        if (delete_h2gis_db.equalsIgnoreCase("false")) {
                            delete_h2gis = false
                        }
                    } else if (delete_h2gis_db instanceof Boolean) {
                        delete_h2gis = delete_h2gis_db
                    }
                    if(databasePath){
                        h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
                    }
                }

                if (input) {
                    def osmFilters = input.get("osm")
                    def downloadAllOSMData = input.get("all")
                    if(!downloadAllOSMData){
                        downloadAllOSMData = true
                    }else if(!downloadAllOSMData in Boolean){
                        error "The all parameter must be a boolean value"
                        return null
                    }

                    if (!osmFilters) {
                        error "Please set at least one OSM filter. e.g osm : ['A place name']"
                        return null
                    }

                    if (output) {
                        def geoclimateTableNames = ["building_indicators",
                                                    "block_indicators",
                                                    "rsu_indicators",
                                                    "rsu_lcz",
                                                    "zones",
                                                    "building",
                                                    "road",
                                                    "rail",
                                                    "water",
                                                    "vegetation",
                                                    "impervious",
                                                    "urban_areas"]
                        //Get processing parameters
                        def processing_parameters = extractProcessingParameters(parameters.get("parameters"))
                        if(!processing_parameters){
                            return
                        }
                        def outputSRID = output.get("srid")
                        if(outputSRID && outputSRID.isInteger()){
                            outputSRID = outputSRID.toInteger()
                        }
                        def outputDataBase = output.get("database")
                        def outputFolder = output.get("folder")
                        def deleteOutputData = output.get("delete")
                        if(!deleteOutputData){
                            deleteOutputData = true
                        }else if(!deleteOutputData in Boolean){
                            error "The delete parameter must be a boolean value"
                            return null
                        }
                        if (outputDataBase && outputFolder) {
                            def outputFolderProperties = outputFolderProperties(outputFolder)
                            //Check if we can write in the output folder
                            def file_outputFolder = new File(outputFolderProperties.path)
                            if (!file_outputFolder.isDirectory()) {
                                error "The directory $file_outputFolder doesn't exist."
                                return null
                            }
                            if (!file_outputFolder.canWrite()) {
                                file_outputFolder = null
                            }
                            //Check not the conditions for the output database
                            def outputTableNames = outputDataBase.get("tables")
                            def allowedOutputTableNames = geoclimateTableNames.intersect(outputTableNames.keySet())
                            def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() != allowedOutputTableNames.size()
                            if (!allowedOutputTableNames && notSameTableNames) {
                                outputDataBase = null
                                outputTableNames = null
                            }
                            def finalOutputTables = outputTableNames.subMap(allowedTableNames)
                            def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                            if (!output_datasource) {
                                return null
                            }

                            def h2gis_datasource = H2GIS.open(h2gis_properties)
                            if (osmFilters && osmFilters in Collection) {
                                def osmprocessing = osm_processing()
                                if (!osmprocessing.execute(h2gis_datasource: h2gis_datasource,
                                        processing_parameters: processing_parameters,
                                        id_zones: osmFilters, outputFolder: file_outputFolder, ouputTableFiles: outputFolderProperties.tables,
                                        output_datasource: output_datasource, outputTableNames: finalOutputTables, outputSRID :outputSRID, downloadAllOSMData:downloadAllOSMData, deleteOutputData: deleteOutputData)) {
                                    return null
                                }
                                if (delete_h2gis) {
                                    def localCon = h2gis_datasource.getConnection()
                                    if(localCon){
                                        localCon.close()
                                        DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                        info "The local H2GIS database : ${databasePath} has been deleted"
                                    }
                                    else{
                                        error "Cannot delete the local H2GIS database : ${databasePath} "
                                    }
                                }
                            } else {
                                error "Cannot find any OSM filters"
                                return null
                            }

                        } else if (outputFolder) {
                            //Check if we can write in the output folder
                            def outputFolderProperties = outputFolderProperties(outputFolder)
                            def file_outputFolder = new File(outputFolderProperties.path)
                            if (!file_outputFolder.isDirectory()) {
                                error "The directory $file_outputFolder doesn't exist."
                                return null
                            }
                            if (file_outputFolder.canWrite()) {
                                def h2gis_datasource = H2GIS.open(h2gis_properties)
                                if (osmFilters && osmFilters in Collection) {
                                    def osmprocessing = osm_processing()
                                    if (!osmprocessing.execute(h2gis_datasource: h2gis_datasource,
                                            processing_parameters: processing_parameters,
                                            id_zones: osmFilters, outputFolder: file_outputFolder, ouputTableFiles: outputFolderProperties.tables,
                                            output_datasource: null, outputTableNames: null, outputSRID :outputSRID, downloadAllOSMData:downloadAllOSMData, deleteOutputData: deleteOutputData)) {
                                        return null
                                    }
                                    //Delete database
                                    if (delete_h2gis) {
                                        def localCon = h2gis_datasource.getConnection()
                                        if(localCon){
                                            localCon.close()
                                            DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                            info "The local H2GIS database : ${databasePath} has been deleted"
                                        }
                                        else{
                                            error "Cannot delete the local H2GIS database : ${databasePath} "
                                        }
                                        return [outputMessage: "The ${osmFilters.join(",")} have been processed"]
                                    }
                                } else {
                                    error "Cannot load the files from the folder $inputFolder"
                                    return null
                                }
                            } else {
                                error "You don't have permission to write in the folder $outputFolder \n Please check the folder."
                                return null
                            }

                        } else if (outputDataBase) {
                            def outputTableNames = outputDataBase.get("tables")
                            def allowedOutputTableNames = geoclimateTableNames.intersect(outputTableNames.keySet())
                            def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() != allowedOutputTableNames.size()
                            if (allowedOutputTableNames && !notSameTableNames) {
                                def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                if (!output_datasource) {
                                    return null
                                }
                                def h2gis_datasource = H2GIS.open(h2gis_properties)
                                if (osmFilters && osmFilters in Collection) {
                                    def osmprocessing = osm_processing()
                                    if (!osmprocessing.execute(h2gis_datasource: h2gis_datasource,
                                            processing_parameters: processing_parameters,
                                            id_zones: osmFilters, outputFolder: null, ouputTableFiles: null,
                                            output_datasource: output_datasource, outputTableNames: finalOutputTables,outputSRID :outputSRID,downloadAllOSMData:downloadAllOSMData, deleteOutputData: deleteOutputData)) {
                                        return null
                                    }
                                    if (delete_h2gis) {
                                        def localCon = h2gis_datasource.getConnection()
                                        if(localCon){
                                            localCon.close()
                                            DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                            info "The local H2GIS database : ${databasePath} has been deleted"
                                        }
                                        else{
                                            error "Cannot delete the local H2GIS database : ${databasePath} "
                                        }
                                    }
                                } else {
                                    error "Cannot load the files from the folder $inputFolder"
                                    return null
                                }

                            } else {
                                error "All output table names must be specified in the configuration file."
                                return null
                            }
                        } else {
                            error "Please set at least one output provider"
                            return null
                        }

                    } else {
                        error "Please set at least one output provider"
                        return null
                    }

                } else {
                    error "Cannot find any input parameter to extract data from Overpass API."

                }
            } else {
                error "Empty parameters"
            }

            return [outputMessage: "The process has been done"]

        }
    }
}

/**
 * Process to extract the OSM data, build the GIS layers and run the Geoclimate algorithms
 *
 * @param h2gis_datasource the local H2GIS database
 * @param processing_parameters the geoclimate chain parameters
 * @param id_zones a list of id zones to process
 * @param outputFolder folder to store the files, null otherwise
 * @param ouputTableFiles the name of the tables that will be saved
 * @param output_datasource a connexion to a database to save the results
 * @param outputTableNames the name of the tables in the output_datasource to save the results
 * @return
 */
IProcess osm_processing() {
    return create {
        title "Build OSM data and compute the geoindicators"
        id "osm_processing"
        inputs h2gis_datasource: JdbcDataSource, processing_parameters: Map, id_zones: Map,
                outputFolder: "", ouputTableFiles: "", output_datasource: "", outputTableNames: "", outputSRID : String, downloadAllOSMData : true,deleteOutputData:true
        outputs outputMessage: String
        run { h2gis_datasource, processing_parameters, id_zones, outputFolder, ouputTableFiles, output_datasource, outputTableNames, outputSRID, downloadAllOSMData,deleteOutputData ->

            // Temporary tables
            int nbAreas = id_zones.size();
            info "$nbAreas osm areas will be processed"
            def geoIndicatorsComputed = false
            id_zones.eachWithIndex { id_zone, index ->
                //Extract the zone table and read its SRID
                def zoneTableNames = extractOSMZone(h2gis_datasource, id_zone, processing_parameters)
                if (zoneTableNames) {
                    id_zone = id_zone in Map ? "bbox_" + id_zone.join('_') : id_zone
                    def zoneTableName = zoneTableNames.outputZoneTable
                    def zoneEnvelopeTableName = zoneTableNames.outputZoneEnvelopeTable
                    def srid = h2gis_datasource.getSpatialTable(zoneTableName).srid
                    def reproject =false
                    if(outputSRID){
                        if(outputSRID!=srid){
                            reproject = true
                        }
                    }else{
                        outputSRID =srid
                    }
                    //Prepare OSM extraction
                    def query = "[maxsize:1073741824]" + Utilities.buildOSMQuery(zoneTableNames.envelope, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

                    if(downloadAllOSMData){
                        //Create a custom OSM query to download all requiered data. It will take more time and resources
                        //because much more OSM elements will be returned
                        def  keysValues = ["building", "railway", "amenity",
                                           "leisure", "highway", "natural",
                                           "landuse", "landcover",
                                           "vegetation","waterway"]
                        query =  "[maxsize:1073741824]"+ Utilities.buildOSMQueryWithAllData(zoneTableNames.envelope, keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
                    }

                    def extract = OSMTools.Loader.extract()
                    if (extract.execute(overpassQuery: query)) {
                        IProcess createGISLayerProcess = OSM.createGISLayers
                        if (createGISLayerProcess.execute(datasource: h2gis_datasource, osmFilePath: extract.results.outputFilePath, epsg: srid)) {
                            def gisLayersResults = createGISLayerProcess.getResults()
                            if (zoneTableName != null) {
                                info "Formating OSM GIS layers"

                                //Format urban areas
                                IProcess format = OSM.formatUrbanAreas
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.urbanAreasTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid])
                                def urbanAreasTable = format.results.outputTableName

                                def estimateHeight = processing_parameters."estimateHeight"
                                format = OSM.formatBuildingLayer
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.buildingTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid,
                                        estimateHeight            : estimateHeight,
                                        urbanAreasTableName : urbanAreasTable ])

                                def buildingTableName = format.results.outputTableName
                                def buildingEstimateTableName = format.results.outputEstimateTableName

                                format = OSM.formatRoadLayer
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.roadTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid])
                                def roadTableName = format.results.outputTableName


                                format = OSM.formatRailsLayer
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.railTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid])
                                def railTableName = format.results.outputTableName

                                format = OSM.formatVegetationLayer
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.vegetationTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid])
                                def vegetationTableName = format.results.outputTableName

                                format = OSM.formatHydroLayer
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.hydroTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid])
                                def hydrographicTableName = format.results.outputTableName

                                format = OSM.formatImperviousLayer
                                format.execute([
                                        datasource                : h2gis_datasource,
                                        inputTableName            : gisLayersResults.imperviousTableName,
                                        inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                        epsg                      : srid])
                                def imperviousTableName = format.results.outputTableName


                                info "OSM GIS layers formated"

                                //If the config file contains the parameter estimateHeight then
                                //the indicatorUse must be updated

                                if (estimateHeight) {
                                    //First pass to compute all indicators that will be used to estimate the height
                                    IProcess geoIndicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
                                    if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zoneTableName,
                                            buildingTable: buildingTableName, roadTable: roadTableName,
                                            railTable: railTableName, vegetationTable: vegetationTableName,
                                            hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                                            indicatorUse:["URBAN_TYPOLOGY"],
                                            svfSimplified: true, prefixName: processing_parameters.prefixName,
                                            mapOfWeights: processing_parameters.mapOfWeights,
                                            lczRandomForest: false)) {
                                        error "Cannot build the geoindicators for the zone $id_zone to estimate the building height"
                                        geoIndicatorsComputed = false
                                    } else {
                                        geoIndicatorsComputed = true
                                        info "The whole indicators for the ${id_zone} has been processed to estimate the building height"
                                    }
                                    //Let's go to select the building's id that must be processed to fix the height
                                    if(geoIndicatorsComputed){
                                        info "Extracting the building having no height information for the ${id_zone} and estimate it"
                                        def results = geoIndicators.results;

                                        //Select indicators we need at building scales
                                        def buildingIndicatorsTableName = results.outputTableBuildingIndicators;
                                        h2gis_datasource.getTable(buildingEstimateTableName).id_build.createIndex()
                                        h2gis_datasource.getTable(buildingIndicatorsTableName).id_build.createIndex()
                                        h2gis_datasource.getTable(buildingIndicatorsTableName).id_rsu.createIndex()

                                        def estimated_building_with_indicators = "ESTIMATED_BUILDING_INDICATORS_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                                        h2gis_datasource.execute """DROP TABLE IF EXISTS $estimated_building_with_indicators;
                                           CREATE TABLE $estimated_building_with_indicators 
                                                    AS SELECT a.*
                                                    FROM $buildingIndicatorsTableName a 
                                                        RIGHT JOIN $buildingEstimateTableName b 
                                                        ON a.id_build=b.id_build
                                                    WHERE b.ESTIMATED = true AND a.ID_RSU IS NOT NULL;"""

                                        info "Collect building indicators to estimate the height for the ${id_zone}"

                                        def applygatherScales = Geoindicators.GenericIndicators.gatherScales()
                                        applygatherScales.execute([
                                                buildingTable    : estimated_building_with_indicators,
                                                blockTable       : results.outputTableBlockIndicators,
                                                rsuTable         : results.outputTableRsuIndicators,
                                                targetedScale    : "BUILDING",
                                                operationsToApply: ["AVG", "STD"],
                                                prefixName       : processing_parameters.prefixName,
                                                datasource       : h2gis_datasource])
                                        def gatheredScales = applygatherScales.results.outputTableName

                                        info "Start estimating the building height for the ${id_zone}"

                                        //Apply RF model
                                        def applyRF = Geoindicators.TypologyClassification.applyRandomForestModel()
                                        applyRF.execute([
                                                explicativeVariablesTableName: gatheredScales,
                                                pathAndFileName              : "BUILDING_HEIGHT_OSM_RF_1_0.model",
                                                idName                       : "id_build",
                                                prefixName                   : processing_parameters.prefixName,
                                                datasource                   : h2gis_datasource])

                                        //Update the abstract building table
                                        info "Replace the input building table by the estimated height for the ${id_zone}"
                                        def buildEstimatedHeight = applyRF.results.outputTableName

                                        h2gis_datasource.getTable(buildEstimatedHeight).id_build.createIndex()

                                        def newEstimatedHeigthWithIndicators = "NEW_BUILDING_INDICATORS_${UUID.randomUUID().toString().replaceAll("-", "_")}"

                                        h2gis_datasource.execute """DROP TABLE IF EXISTS $newEstimatedHeigthWithIndicators;
                                           CREATE TABLE $newEstimatedHeigthWithIndicators as 
                                            SELECT  a.THE_GEOM, a.ID_BUILD,a.ID_SOURCE,
                                        CASE WHEN b.HEIGHT_ROOF IS NULL THEN a.HEIGHT_WALL ELSE 0 END AS HEIGHT_WALL ,
                                                COALESCE(b.HEIGHT_ROOF, a.HEIGHT_ROOF) AS HEIGHT_ROOF,
                                                CASE WHEN b.HEIGHT_ROOF IS NULL THEN a.NB_LEV ELSE 0 END AS NB_LEV, a.TYPE,a.MAIN_USE, a.ZINDEX from $buildingTableName
                                        a LEFT JOIN $buildEstimatedHeight b on a.id_build=b.id_build"""

                                        //We must format only estimated buildings
                                        //Apply format on the new abstract table
                                        IProcess formatEstimatedBuilding = OSM.formatEstimatedBuilding
                                        formatEstimatedBuilding.execute([
                                                datasource                : h2gis_datasource,
                                                inputTableName            : newEstimatedHeigthWithIndicators,
                                                epsg                      : srid])
                                        def newbuildingTableName = formatEstimatedBuilding.results.outputTableName

                                        //Drop tables
                                        h2gis_datasource.execute """DROP TABLE IF EXISTS $estimated_building_with_indicators,
                                        $newEstimatedHeigthWithIndicators, $buildEstimatedHeight,
                                        $gatheredScales, $buildingTableName, ${results.outputTableBlockIndicators},
                                        ${results.outputTableRsuIndicators},${results.outputTableRsuIndicators}"""

                                        //Re-compute geoindicators chain with the config parameters
                                        geoIndicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
                                        if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zoneTableName,
                                                buildingTable: newbuildingTableName, roadTable: roadTableName,
                                                railTable: railTableName, vegetationTable: vegetationTableName,
                                                hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                                                indicatorUse: processing_parameters.indicatorUse,
                                                svfSimplified: processing_parameters.svfSimplified, prefixName: processing_parameters.prefixName,
                                                mapOfWeights: processing_parameters.mapOfWeights,
                                                lczRandomForest: false)) {
                                            error "Cannot build the geoindicators for the zone $id_zone"
                                            geoIndicatorsComputed = false
                                        } else {
                                            geoIndicatorsComputed = true
                                            info "${id_zone} has been processed"
                                        }
                                        results = geoIndicators.getResults()
                                        results.put("buildingTableName", buildingTableName)
                                        results.put("roadTableName", roadTableName)
                                        results.put("railTableName", railTableName)
                                        results.put("hydrographicTableName", hydrographicTableName)
                                        results.put("vegetationTableName", vegetationTableName)
                                        results.put("imperviousTableName", imperviousTableName)
                                        results.put("urbanAreasTableName", urbanAreasTable)
                                        if (outputFolder && geoIndicatorsComputed && ouputTableFiles) {
                                            saveOutputFiles(h2gis_datasource, id_zone, results, ouputTableFiles, outputFolder, "osm_", outputSRID, reproject, deleteOutputData)
                                        }
                                        if (output_datasource && geoIndicatorsComputed) {
                                            saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)
                                        }
                                        info "Re-compute the whole geoindicators for the ${id_zone}"
                                    }
                                }
                                else {
                                    //Compute the indicators
                                    IProcess geoIndicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
                                    if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zoneTableName,
                                            buildingTable: buildingTableName, roadTable: roadTableName,
                                            railTable: railTableName, vegetationTable: vegetationTableName,
                                            hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                                            indicatorUse: processing_parameters.indicatorUse,
                                            svfSimplified: processing_parameters.svfSimplified, prefixName: processing_parameters.prefixName,
                                            mapOfWeights: processing_parameters.mapOfWeights,
                                            lczRandomForest: processing_parameters.lczRandomForest)) {
                                        error "Cannot build the geoindicators for the zone $id_zone"
                                        geoIndicatorsComputed = false
                                    } else {
                                        geoIndicatorsComputed = true
                                        info "${id_zone} has been processed"
                                    }
                                    def results = geoIndicators.getResults()
                                    results.put("buildingTableName", buildingTableName)
                                    results.put("roadTableName", roadTableName)
                                    results.put("railTableName", railTableName)
                                    results.put("hydrographicTableName", hydrographicTableName)
                                    results.put("vegetationTableName", vegetationTableName)
                                    results.put("imperviousTableName", imperviousTableName)
                                    results.put("urbanAreasTableName", urbanAreasTable)
                                    if (outputFolder && geoIndicatorsComputed && ouputTableFiles) {
                                        saveOutputFiles(h2gis_datasource, id_zone, results, ouputTableFiles, outputFolder, "osm_", outputSRID, reproject, deleteOutputData)
                                    }
                                    if (output_datasource && geoIndicatorsComputed) {
                                        saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)
                                    }
                                }
                            }
                        } else {
                            error "Cannot load the OSM file ${extract.results.outputFilePath}"
                        }
                    } else {
                        error "Cannot execute the overpass query $query"
                    }
                } else {
                    error "Cannot calculate a bounding box to extract OSM data"
                }

                info "Number of areas processed ${index + 1} on $nbAreas"
            }
            return [outputMessage: "The OSM processing tasks have been done"]
        }
    }
}

/**
 * Extract the OSM zone and its envelope area from Nominatim API
 *
 * @param datasource a connexion to the local H2GIS database
 * @param zoneToExtract the osm filter : place or bbox
 * @param processing_parameters geoclimate parameters
 * @return
 */
def extractOSMZone(def datasource, def zoneToExtract, def processing_parameters) {
    def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    if (zoneToExtract) {
        def GEOMETRY_TYPE
        Geometry geom
        if (zoneToExtract in Collection) {
            GEOMETRY_TYPE = "POLYGON"
            geom = Utilities.geometryFromOverpass(zoneToExtract)
            if (!geom) {
                error("The bounding box cannot be null")
                return null
            }
        } else if (zoneToExtract instanceof String) {
            geom = Utilities.getAreaFromPlace(zoneToExtract);
            if (!geom) {
                error("Cannot find an area from the place name ${zoneToExtract}")
                return null
            } else {
                GEOMETRY_TYPE = "GEOMETRY"
                if (geom instanceof Polygon) {
                    GEOMETRY_TYPE = "POLYGON"
                } else if (geom instanceof MultiPolygon) {
                    GEOMETRY_TYPE = "MULTIPOLYGON"
                }
            }
        } else {
            error("The zone to extract must be a place name or a JTS envelope")
            return null;
        }

        /**
         * Extract the OSM file from the envelope of the geometry
         */
        def envelope = GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), processing_parameters.distance)

        //Find the best utm zone
        //Reproject the geometry and its envelope to the UTM zone
        def con = datasource.getConnection();
        def interiorPoint = envelope.centre()
        def epsg = GeographyUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
        def geomUTM = ST_Transform.ST_Transform(con, geom, epsg)
        def tmpGeomEnv = geom.getFactory().toGeometry(envelope)
        tmpGeomEnv.setSRID(4326)

        datasource.execute """drop table if exists ${outputZoneTable}; create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);"""
        datasource.execute(" INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT(?, ?), ?);", geomUTM.toString(),epsg, zoneToExtract.toString())

        datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);"""
        datasource.execute("INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT(?,?), ?);"
        ,ST_Transform.ST_Transform(con, tmpGeomEnv, epsg).toString(), epsg, zoneToExtract.toString())

        return [outputZoneTable: outputZoneTable,
                outputZoneEnvelopeTable: outputZoneEnvelopeTable,
                envelope:envelope]
    }else{
        error "The zone to extract cannot be null or empty"
        return null
    }
    return null
}

/**
 * Compute all geoindicators at the 3 scales :
 * building, block and RSU
 * Compute also the LCZ classification and the urban typology
 *
 * @return 4 tables outputTableBuildingIndicators, outputTableBlockIndicators, outputTableRsuIndicators,
 * outputTableRsuLcz . The first three tables contains the geoindicators and the last table the LCZ classification.
 * This table can be empty if the user decides not to calculate it.
 *
 */
IProcess GeoIndicators() {
    return create {
        title "Compute all geoindicators"
        id "GeoIndicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified: false, prefixName: "",
                mapOfWeights: ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_length": 1]
        outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String,
                outputTableRsuIndicators: String, outputTableRsuLcz: String, outputTableZone: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance, indicatorUse, svfSimplified, prefixName, mapOfWeights ->
            info "Start computing the geoindicators..."
            // Temporary tables are created
            def lczIndicTable = postfix "LCZ_INDIC_TABLE"
            def COLUMN_ID_RSU = "id_rsu"
            def GEOMETRIC_COLUMN = "the_geom"

            // Output Lcz table name is set to null in case LCZ indicators are not calculated
            def rsuLcz = null

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis
            if (!spatialUnits.execute([datasource       : datasource, zoneTable: zoneTable,
                                       buildingTable    : buildingTable, roadTable: roadTable,
                                       railTable        : railTable, vegetationTable: vegetationTable,
                                       hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                       surface_hydro    : surface_hydro, distance: distance,
                                       prefixName       : prefixName,
                                       indicatorUse     : indicatorUse])) {
                error "Cannot create the spatial units"
                return null
            }
            String relationBuildings = spatialUnits.getResults().outputTableBuildingName
            String relationBlocks = spatialUnits.getResults().outputTableBlockName
            String relationRSU = spatialUnits.getResults().outputTableRsuName

            //Compute building indicators
            def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators
            if (!computeBuildingsIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: relationBuildings,
                                                     inputRoadTableName    : roadTable,
                                                     indicatorUse          : indicatorUse,
                                                     prefixName            : prefixName])) {
                error "Cannot compute the building indicators"
                return null
            }

            def buildingIndicators = computeBuildingsIndicators.results.outputTableName

            //Compute block indicators
            def blockIndicators = null
            if (indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")) {
                def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators
                if (!computeBlockIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: buildingIndicators,
                                                     inputBlockTableName   : relationBlocks,
                                                     prefixName            : prefixName])) {
                    error "Cannot compute the block indicators"
                    return null
                }
                blockIndicators = computeBlockIndicators.results.outputTableName
            }

            //Compute RSU indicators
            def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators
            if (!computeRSUIndicators.execute([datasource       : datasource,
                                               buildingTable    : buildingIndicators,
                                               rsuTable         : relationRSU,
                                               vegetationTable  : vegetationTable,
                                               roadTable        : roadTable,
                                               hydrographicTable: hydrographicTable,
                                               indicatorUse     : indicatorUse,
                                               svfSimplified    : svfSimplified,
                                               prefixName       : prefixName])) {
                error "Cannot compute the RSU indicators"
                return null
            }
            info "All geoindicators have been computed"

            // If the LCZ indicators should be calculated, we only affect a LCZ class to each RSU
            if (indicatorUse.contains("LCZ")) {
                def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"              : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                     "BUILDING_FRACTION_LCZ"             : "BUILDING_SURFACE_FRACTION",
                                     "ASPECT_RATIO"                      : "ASPECT_RATIO",
                                     "GROUND_SKY_VIEW_FACTOR"            : "SKY_VIEW_FACTOR",
                                     "PERVIOUS_FRACTION_LCZ"             : "PERVIOUS_SURFACE_FRACTION",
                                     "IMPERVIOUS_FRACTION_LCZ"           : "IMPERVIOUS_SURFACE_FRACTION",
                                     "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH" : "TERRAIN_ROUGHNESS_LENGTH"]

                // Get into a new table the ID, geometry column and the 7 indicators defined by Stewart and Oke (2012)
                // for LCZ classification (rename the indicators with the real names)
                def queryReplaceNames = ""
                lczIndicNames.each { oldIndic, newIndic ->
                    queryReplaceNames += "ALTER TABLE $lczIndicTable ALTER COLUMN $oldIndic RENAME TO $newIndic;"
                }
                datasource """DROP TABLE IF EXISTS $lczIndicTable;
                                CREATE TABLE $lczIndicTable 
                                        AS SELECT $COLUMN_ID_RSU, $GEOMETRIC_COLUMN, ${lczIndicNames.keySet().join(",")} 
                                        FROM ${computeRSUIndicators.results.outputTableName};
                                $queryReplaceNames"""

                // The classification algorithm is called
                def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType
                if (!classifyLCZ([rsuLczIndicators : lczIndicTable,
                                  rsuAllIndicators : computeRSUIndicators.results.outputTableName,
                                  normalisationType: "AVG",
                                  mapOfWeights     : mapOfWeights,
                                  prefixName       : prefixName,
                                  datasource       : datasource,
                                  prefixName       : prefixName])) {
                    info "Cannot compute the LCZ classification."
                    return
                }
                rsuLcz = classifyLCZ.results.outputTableName
            }

            datasource "DROP TABLE IF EXISTS $lczIndicTable;"


            return [outputTableBuildingIndicators: computeBuildingsIndicators.getResults().outputTableName,
                    outputTableBlockIndicators   : blockIndicators,
                    outputTableRsuIndicators     : computeRSUIndicators.getResults().outputTableName,
                    outputTableRsuLcz            : rsuLcz,
                    outputTableZone              : zoneTable]
        }
    }
}

/**
 * Return the properties parameters to store results in a folder
 * @param outputFolder the output folder parameters from the json file
 */
def outputFolderProperties(def outputFolder){
    def tablesToSave = ["building_indicators",
                        "block_indicators",
                        "rsu_indicators",
                        "rsu_lcz",
                        "zones",
                        "building",
                        "road",
                        "rail" ,
                        "water",
                        "vegetation",
                        "impervious",
                        "urban_areas"]
    if(outputFolder in Map){
        def outputPath = outputFolder.get("path")
        def outputTables = outputFolder.get("tables")
        if(!outputPath){
            return null
        }
        if(outputTables){
            return ["path":outputPath, "tables" : tablesToSave.intersect(outputTables)]
        }
        return ["path":outputPath, "tables" : tablesToSave]
    }
    else{
        return ["path":outputFolder, "tables" : tablesToSave]
    }
}

/**
 * Create a datasource from the following parameters :
 * user, password, url
 *
 * @param database_properties from the json file
 * @return a connection or null if the parameters are invalid
 */
def createDatasource(def database_properties){
    def db_output_url = database_properties.get("url")
    if(db_output_url){
        if (db_output_url.startsWith("jdbc:")) {
            String url = db_output_url.substring("jdbc:".length());
            if (url.startsWith("h2") ) {
                return H2GIS.open(database_properties)
            } else if (url.startsWith("postgresql")) {
                return POSTGIS.open(database_properties)
            }
            else {
                error"Unsupported database"
                return null
            }
        }
        else {
            error"Invalid output database url"
            return null
        }

    }else{
        error "The output database url cannot be null or empty"
    }
}

/**
 * Workaround to change the postgresql URL when a linked table is created with H2GIS
 * @param input_database_properties
 * @return the input_database_properties with the h2 url
 */
def updateDriverURL(def input_database_properties){
    def db_output_url = input_database_properties.get("url")
    if(db_output_url){
        if (db_output_url.startsWith("jdbc:")) {
            String url = db_output_url.substring("jdbc:".length())
            if (url.startsWith("postgresql")) {
                input_database_properties.put("url", "jdbc:postgresql_h2"+db_output_url.substring("jdbc:postgresql".length()))
                return input_database_properties
            }
            else{
                return input_database_properties
            }
        }
    }
    else {
        error"Invalid output database url"
        return null
    }
}

/**
 * Load  shapefiles into the local H2GIS database
 *
 * @param inputFolder where the files are
 * @param h2gis_datasource the local database for the geoclimate processes
 * @param id_zones a list of id zones to process
 * @return a list of id_zones
 */
def loadDataFromFolder(def inputFolder, def h2gis_datasource, def id_zones){
    def  folder = new File(inputFolder)
    if(folder.isDirectory()) {
        def geoFiles = []
        folder.eachFileRecurse groovy.io.FileType.FILES,  { file ->
            if (file.name.toLowerCase().endsWith(".shp")) {
                geoFiles << file.getAbsolutePath()
            }
        }
        //Looking for IRIS_GE shape file
        def iris_ge_file = geoFiles.find{ it.toLowerCase().endsWith("iris_ge.shp")}
        if(iris_ge_file) {
            //Load IRIS_GE and check if there is some id_zones inside
            h2gis_datasource.load(iris_ge_file, true)
            id_zones = findIDZones(h2gis_datasource, id_zones)
            geoFiles.remove(iris_ge_file)
            if(id_zones){
                //Load the files
                def numberFiles = geoFiles.size()
                geoFiles.eachWithIndex { geoFile , index->
                    info "Loading file $geoFile $index on $numberFiles"
                    h2gis_datasource.load(geoFile, true)
                }
                return id_zones

            }else{
                error "The iris_ge file doesn't contains any zone identifiers"
                return null
            }
        }
        else{
            error "The input folder must contains a file named iris_ge"
            return null
        }
    }else{
        error "The input folder must be a directory"
        return null
    }

}

/**
 * Return a list of id_zones
 * @param h2gis_datasource the local database for the geoclimate processes
 * @param id_zones a list of id zones to process
 * @return
 */
def findIDZones(def h2gis_datasource, def id_zones){
    def inseeCodes = []
    if(h2gis_datasource.hasTable("IRIS_GE")) {
        if (id_zones) {
            if(id_zones in Collection){
                if (h2gis_datasource.firstRow("select count(*) as COUNT_ZONES FROM IRIS_GE where insee_com in ('${id_zones.join("','")}')").COUNT_ZONES > 0) {
                    inseeCodes = id_zones
                } else {
                    error "Cannot find any commune from the list of zones  : ${id_zones.join(",")}"
                }
            }
            else {
                if (h2gis_datasource.firstRow("select count(*) as COUNT_ZONES FROM IRIS_GE where ${id_zones}").COUNT_ZONES > 0) {
                    inseeCodes = id_zones
                } else {
                    error "Cannot find any commune from the query : ${id_zones}"
                }
            }

        } else {
            h2gis_datasource.eachRow("select distinct insee_com from IRIS_GE group by insee_com ;") { row ->
                inseeCodes << row.insee_com
            }
        }

        return inseeCodes
    }
    else{
        return inseeCodes
    }

}
/**
 * Read the file parameters and create a new map of parameters
 * The map of parameters is initialized with default values
 *
 * @param processing_parameters the file parameters
 * @return a filled map of parameters
 */
def extractProcessingParameters(def processing_parameters){
    def defaultParameters = [distance: 0,indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"],
                             svfSimplified:false, prefixName: "",
                             mapOfWeights : ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                                             "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                                             "height_of_roughness_elements": 1, "terrain_roughness_length": 1],
                             hLevMin : 3, hLevMax: 15, hThresholdLev2: 10,
                             lczRandomForest :false,
                             estimateHeight:false,
                             lczModelName: "LCZ_OSM_RF_1_0.model",
                             urbanTypoModelName: "URBAN_TYPOLOGY_OSM_RF_1_0.model"]
    if(processing_parameters){
        def distanceP =  processing_parameters.distance
        if(distanceP && distanceP in Number){
            defaultParameters.distance = distanceP
        }
        def indicatorUseP = processing_parameters.indicatorUse
        if(indicatorUseP && indicatorUseP in List){
            defaultParameters.indicatorUse = indicatorUseP
        }

        def svfSimplifiedP = processing_parameters.svfSimplified
        if(svfSimplifiedP && svfSimplifiedP in Boolean){
            defaultParameters.svfSimplified = svfSimplifiedP
        }
        def prefixNameP = processing_parameters.prefixName
        if(prefixNameP && prefixNameP in String){
            defaultParameters.prefixName = prefixNameP
        }

        def mapOfWeightsP = processing_parameters.mapOfWeights
        if(mapOfWeightsP && mapOfWeightsP in Map){
            def defaultmapOfWeights = defaultParameters.mapOfWeights
            if((defaultmapOfWeights+mapOfWeightsP).size()!=defaultmapOfWeights.size()){
                error "The number of mapOfWeights parameters must contain exactly the parameters ${defaultmapOfWeights.keySet().join(",")}"
                return
            }else{
                defaultParameters.mapOfWeights = mapOfWeightsP
            }
        }

        def hLevMinP =  processing_parameters.hLevMin
        if(hLevMinP && hLevMinP in Integer){
            defaultParameters.hLevMin = hLevMinP
        }
        def hLevMaxP =  processing_parameters.hLevMax
        if(hLevMaxP && hLevMaxP in Integer){
            defaultParameters.hLevMax = hLevMaxP
        }
        def hThresholdLev2P =  processing_parameters.hThresholdLev2
        if(hThresholdLev2P && hThresholdLev2P in Integer){
            defaultParameters.hThresholdLev2 = hThresholdLev2P
        }
        /*Disable lczRandomForest
        def lczRandomForest = processing_parameters.lczRandomForest
        if(lczRandomForest && lczRandomForest in Boolean){
            defaultParameters.lczRandomForest = lczRandomForest
        }*/

        def estimateHeight = processing_parameters.estimateHeight
        if(estimateHeight && estimateHeight in Boolean){
            defaultParameters.estimateHeight = estimateHeight
        }

        return defaultParameters
    }
    else{
        return defaultParameters
    }
}



/**
 * Save the geoclimate tables into geojson files
 * @param id_zone the id of the zone
 * @param results a list of tables computed by geoclimate
 * @param ouputFolder the ouput folder
 * @param outputSRID srid code to reproject the result
 * @param reproject  true if the file must reprojected
 * @return
 */
def saveOutputFiles(def h2gis_datasource, def id_zone, def results, def outputFiles, def ouputFolder, def subFolderName, def outputSRID, def reproject, def deleteOutputData){
    //Create a subfolder to store each results
    def folderName = id_zone in Map?id_zone.join("_"):id_zone
    def subFolder = new File(ouputFolder.getAbsolutePath()+File.separator+subFolderName+folderName)
    if(!subFolder.exists()){
        subFolder.mkdir()
    }
    outputFiles.each{
        //Save indicators
        if(it.equals("building_indicators")){
            saveTableAsGeojson(results.outputTableBuildingIndicators, "${subFolder.getAbsolutePath()+File.separator+"building_indicators"}.geojson",h2gis_datasource,outputSRID, reproject, deleteOutputData)
        }
        else if(it.equals("block_indicators")){
            saveTableAsGeojson(results.outputTableBlockIndicators, "${subFolder.getAbsolutePath()+File.separator+"block_indicators"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else  if(it.equals("rsu_indicators")){
            saveTableAsGeojson(results.outputTableRsuIndicators, "${subFolder.getAbsolutePath()+File.separator+"rsu_indicators"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else  if(it.equals("rsu_lcz")){
            saveTableAsGeojson(results.outputTableRsuLcz,  "${subFolder.getAbsolutePath()+File.separator+"rsu_lcz"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else  if(it.equals("zones")){
            saveTableAsGeojson(results.outputTableZone,  "${subFolder.getAbsolutePath()+File.separator+"zones"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }

        //Save input GIS tables
        else  if(it.equals("building")){
            saveTableAsGeojson(results.buildingTableName, "${subFolder.getAbsolutePath()+File.separator+"building"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it.equals("road")){
            saveTableAsGeojson(results.roadTableName,  "${subFolder.getAbsolutePath()+File.separator+"road"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it.equals("rail")){
            saveTableAsGeojson(results.railTableName,  "${subFolder.getAbsolutePath()+File.separator+"rail"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        if(it.equals("water")){
            saveTableAsGeojson(results.hydrographicTableName, "${subFolder.getAbsolutePath()+File.separator+"water"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it.equals("vegetation")){
            saveTableAsGeojson(results.vegetationTableName,  "${subFolder.getAbsolutePath()+File.separator+"vegetation"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it.equals("impervious")){
            saveTableAsGeojson(results.imperviousTableName, "${subFolder.getAbsolutePath()+File.separator+"impervious"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it.equals("urban_areas")){
            saveTableAsGeojson(results.urbanAreasTableName, "${subFolder.getAbsolutePath()+File.separator+"urban_areas"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
    }
}

/**
 * Method to save a table into a geojson file
 * @param outputTable name of the table to export
 * @param filePath path to save the table
 * @param h2gis_datasource connection to the database
 * @param outputSRID srid code to reproject the outputTable.
 * @param reproject true if the file must be reprojected
 * @param deleteOutputData true to delete the file if exists
 */
def saveTableAsGeojson(def outputTable , def filePath,def h2gis_datasource,def outputSRID, def reproject, def deleteOutputData){
    if(outputTable && h2gis_datasource.hasTable(outputTable)){
        if(!reproject){
        h2gis_datasource.save(outputTable, filePath,deleteOutputData)
        }else{
            h2gis_datasource.getSpatialTable(outputTable).reproject(outputSRID.toInteger()).save(filePath, deleteOutputData)
        }
        info "${outputTable} has been saved in ${filePath}."
    }
}

/**
 * Create the output tables in the output_datasource
 * @param output_datasource connexion to the output database
 * @param outputTableNames name of tables to store the geoclimate results
 * @param srid epsg code for the output tables
 * @return
 */
def createOutputTables(def output_datasource, def outputTableNames, def srid){
    //Output table names
    def output_zones = outputTableNames.zones
    def output_building_indicators = outputTableNames.building_indicators
    def output_block_indicators = outputTableNames.block_indicators
    def output_rsu_indicators = outputTableNames.rsu_indicators
    def output_rsu_lcz = outputTableNames.rsu_lcz
    def output_building = outputTableNames.building
    def output_road = outputTableNames.road
    def output_rail = outputTableNames.rail
    def output_water = outputTableNames.water
    def output_vegetation = outputTableNames.vegetation
    def output_impervious = outputTableNames.impervious
    def output_urban_areas = outputTableNames.urban_areas


    if (output_block_indicators && !output_datasource.hasTable(output_block_indicators)){
        output_datasource.execute """CREATE TABLE $output_block_indicators (
        ID_BLOCK INTEGER, THE_GEOM GEOMETRY(GEOMETRY,$srid),
        ID_RSU INTEGER, AREA DOUBLE PRECISION,
        FLOOR_AREA DOUBLE PRECISION,VOLUME DOUBLE PRECISION,
        HOLE_AREA_DENSITY DOUBLE PRECISION,
        BUILDING_DIRECTION_EQUALITY DOUBLE PRECISION,
        BUILDING_DIRECTION_UNIQUENESS DOUBLE PRECISION,
        MAIN_BUILDING_DIRECTION VARCHAR,
        CLOSINGNESS DOUBLE PRECISION, NET_COMPACTNESS DOUBLE PRECISION,
        AVG_HEIGHT_ROOF_AREA_WEIGHTED DOUBLE PRECISION,
        STD_HEIGHT_ROOF_AREA_WEIGHTED DOUBLE PRECISION,
        ID_ZONE VARCHAR
        );
        CREATE INDEX IF NOT EXISTS idx_${output_block_indicators}_id_zone ON $output_block_indicators (ID_ZONE);"""
    }
    else if (output_block_indicators){
        def outputTableSRID = output_datasource.getSpatialTable(output_block_indicators).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_block_indicators is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_block_indicators (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_block_indicators WHERE ID_ZONE= 'geoclimate';"""
    }

    if (output_building_indicators && !output_datasource.hasTable(output_building_indicators)){
        output_datasource.execute """
        CREATE TABLE $output_building_indicators (
                THE_GEOM GEOMETRY(GEOMETRY,$srid),
                ID_BUILD INTEGER,
                ID_SOURCE VARCHAR,
                HEIGHT_WALL INTEGER,
                HEIGHT_ROOF INTEGER,
                NB_LEV INTEGER,
                TYPE VARCHAR,
                MAIN_USE VARCHAR,
                ZINDEX INTEGER,
                ID_ZONE VARCHAR,
                ID_BLOCK INTEGER,
                ID_RSU INTEGER,
                PERIMETER DOUBLE PRECISION,
                AREA DOUBLE PRECISION,
                VOLUME DOUBLE PRECISION,
                FLOOR_AREA DOUBLE PRECISION,
                TOTAL_FACADE_LENGTH DOUBLE PRECISION,
                CONTIGUITY DOUBLE PRECISION,
                COMMON_WALL_FRACTION DOUBLE PRECISION,
                NUMBER_BUILDING_NEIGHBOR BIGINT,
                AREA_CONCAVITY DOUBLE PRECISION,
                FORM_FACTOR DOUBLE PRECISION,
                RAW_COMPACTNESS DOUBLE PRECISION,
                PERIMETER_CONVEXITY DOUBLE PRECISION,
                MINIMUM_BUILDING_SPACING DOUBLE PRECISION,
                ROAD_DISTANCE DOUBLE PRECISION,
                LIKELIHOOD_LARGE_BUILDING DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_${output_building_indicators}_id_zone  ON $output_building_indicators (ID_ZONE);
        """
    }
    else if (output_building_indicators){
        def outputTableSRID = output_datasource.getSpatialTable(output_building_indicators).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_building_indicators is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_building_indicators (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_building_indicators WHERE ID_ZONE= 'geoclimate';"""
    }

    if (output_rsu_indicators && !output_datasource.hasTable(output_rsu_indicators)){
        output_datasource.execute """
    CREATE TABLE $output_rsu_indicators (
    ID_RSU INTEGER,
	THE_GEOM GEOMETRY(GEOMETRY,$srid),	
	HIGH_VEGETATION_FRACTION DOUBLE PRECISION,
	HIGH_VEGETATION_WATER_FRACTION DOUBLE PRECISION,
	HIGH_VEGETATION_BUILDING_FRACTION DOUBLE PRECISION,
	HIGH_VEGETATION_LOW_VEGETATION_FRACTION DOUBLE PRECISION,
	HIGH_VEGETATION_ROAD_FRACTION DOUBLE PRECISION,
	HIGH_VEGETATION_IMPERVIOUS_FRACTION DOUBLE PRECISION,
	WATER_FRACTION DOUBLE PRECISION,
	BUILDING_FRACTION DOUBLE PRECISION,
	LOW_VEGETATION_FRACTION DOUBLE PRECISION,
	ROAD_FRACTION DOUBLE PRECISION,
	IMPERVIOUS_FRACTION DOUBLE PRECISION,
	VEGETATION_FRACTION_URB DOUBLE PRECISION,
	LOW_VEGETATION_FRACTION_URB DOUBLE PRECISION,
	HIGH_VEGETATION_IMPERVIOUS_FRACTION_URB DOUBLE PRECISION,
	HIGH_VEGETATION_PERVIOUS_FRACTION_URB DOUBLE PRECISION,
	ROAD_FRACTION_URB DOUBLE PRECISION,
	IMPERVIOUS_FRACTION_URB DOUBLE PRECISION,
	BUILDING_FRACTION_LCZ DOUBLE PRECISION,
	PERVIOUS_FRACTION_LCZ DOUBLE PRECISION,
	HIGH_VEGETATION_FRACTION_LCZ DOUBLE PRECISION,
	LOW_VEGETATION_FRACTION_LCZ DOUBLE PRECISION,
	IMPERVIOUS_FRACTION_LCZ DOUBLE PRECISION,
	WATER_FRACTION_LCZ DOUBLE PRECISION,
	AREA DOUBLE PRECISION,
	AVG_HEIGHT_ROOF_AREA_WEIGHTED DOUBLE PRECISION,
	STD_HEIGHT_ROOF_AREA_WEIGHTED DOUBLE PRECISION,
	ROAD_DIRECTION_DISTRIBUTION_H0_D0_30 DOUBLE PRECISION,
	ROAD_DIRECTION_DISTRIBUTION_H0_D30_60 DOUBLE PRECISION,
	ROAD_DIRECTION_DISTRIBUTION_H0_D60_90 DOUBLE PRECISION,
	ROAD_DIRECTION_DISTRIBUTION_H0_D90_120 DOUBLE PRECISION,
	ROAD_DIRECTION_DISTRIBUTION_H0_D120_150 DOUBLE PRECISION,
	ROAD_DIRECTION_DISTRIBUTION_H0_D150_180 DOUBLE PRECISION,
	GROUND_LINEAR_ROAD_DENSITY DOUBLE PRECISION,
	NON_VERT_ROOF_AREA_H0_10 DOUBLE PRECISION,
	NON_VERT_ROOF_AREA_H10_20 DOUBLE PRECISION,
	NON_VERT_ROOF_AREA_H20_30 DOUBLE PRECISION,
	NON_VERT_ROOF_AREA_H30_40 DOUBLE PRECISION,
	NON_VERT_ROOF_AREA_H40_50 DOUBLE PRECISION,
	NON_VERT_ROOF_AREA_H50 DOUBLE PRECISION,
	VERT_ROOF_AREA_H0_10 DOUBLE PRECISION,
	VERT_ROOF_AREA_H10_20 DOUBLE PRECISION,
	VERT_ROOF_AREA_H20_30 DOUBLE PRECISION,
	VERT_ROOF_AREA_H30_40 DOUBLE PRECISION,
	VERT_ROOF_AREA_H40_50 DOUBLE PRECISION,
	VERT_ROOF_AREA_H50 DOUBLE PRECISION,
	VERT_ROOF_DENSITY DOUBLE PRECISION,
	NON_VERT_ROOF_DENSITY DOUBLE PRECISION,
	FREE_EXTERNAL_FACADE_DENSITY DOUBLE PRECISION,
	GEOM_AVG_HEIGHT_ROOF DOUBLE PRECISION,
	BUILDING_VOLUME_DENSITY DOUBLE PRECISION,
	AVG_VOLUME DOUBLE PRECISION,
	AVG_NUMBER_BUILDING_NEIGHBOR DOUBLE PRECISION,
	BUILDING_FLOOR_AREA_DENSITY DOUBLE PRECISION,
	AVG_MINIMUM_BUILDING_SPACING DOUBLE PRECISION,
	BUILDING_NUMBER_DENSITY DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D0_30 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D0_30 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D0_30 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D0_30 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D0_30 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D0_30 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D30_60 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D30_60 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D30_60 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D30_60 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D30_60 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D30_60 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D60_90 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D60_90 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D60_90 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D60_90 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D60_90 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D60_90 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D90_120 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D90_120 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D90_120 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D90_120 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D90_120 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D90_120 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D120_150 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D120_150 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D120_150 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D120_150 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D120_150 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D120_150 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D150_180 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D150_180 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D150_180 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D150_180 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D150_180 DOUBLE PRECISION,
	PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D150_180 DOUBLE PRECISION,
	BUILDING_TOTAL_FRACTION DOUBLE PRECISION,
	ASPECT_RATIO DOUBLE PRECISION,
	GROUND_SKY_VIEW_FACTOR DOUBLE PRECISION,
	EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH DOUBLE PRECISION,
	EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH INTEGER,
	BUILDING_DIRECTION_EQUALITY DOUBLE PRECISION,
	BUILDING_DIRECTION_UNIQUENESS DOUBLE PRECISION,
	MAIN_BUILDING_DIRECTION VARCHAR,
    ID_ZONE VARCHAR
    );    
        CREATE INDEX IF NOT EXISTS idx_${output_rsu_indicators}_id_zone ON $output_rsu_indicators (ID_ZONE);
        """
    } else if (output_rsu_indicators){
        def outputTableSRID = output_datasource.getSpatialTable(output_rsu_indicators).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_rsu_indicators is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_rsu_indicators (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_rsu_indicators WHERE ID_ZONE= 'geoclimate';"""
    }

    if (output_rsu_lcz && !output_datasource.hasTable(output_rsu_lcz)){
        output_datasource.execute """
        CREATE TABLE $output_rsu_lcz(
                ID_ZONE VARCHAR,
                ID_RSU INTEGER,
                THE_GEOM GEOMETRY(GEOMETRY,$srid),
                LCZ1 INTEGER,
                LCZ2 INTEGER,
                MIN_DISTANCE DOUBLE PRECISION,
                PSS DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_${output_rsu_lcz}_id_zone ON $output_rsu_lcz (ID_ZONE);
        """
    }else if (output_rsu_lcz){
        def outputTableSRID = output_datasource.getSpatialTable(output_rsu_lcz).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_rsu_lcz is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_rsu_lcz (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_rsu_lcz WHERE ID_ZONE= 'geoclimate';"""
    }

    if (output_zones && !output_datasource.hasTable(output_zones)){
        output_datasource.execute """
        CREATE TABLE $output_zones(
                ID_ZONE VARCHAR,
                THE_GEOM GEOMETRY(GEOMETRY,$srid)
        );
        CREATE INDEX IF NOT EXISTS idx_${output_zones}_id_zone ON $output_zones (ID_ZONE);
        """
    }else if (output_zones){
        def outputTableSRID = output_datasource.getSpatialTable(output_zones).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_zones is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_zones (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_zones WHERE ID_ZONE= 'geoclimate';"""
    }

    if (output_building && !output_datasource.hasTable(output_building)){
        output_datasource.execute """CREATE TABLE $output_building  (THE_GEOM GEOMETRY(POLYGON, $srid), 
        id_build serial, ID_SOURCE VARCHAR, HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT,
        NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER);
        CREATE INDEX IF NOT EXISTS idx_${output_building}_id_source ON $output_building (ID_SOURCE);"""
    }
    else if (output_building){
        def outputTableSRID = output_datasource.getSpatialTable(output_building).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_building is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_building (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_building WHERE ID_SOURCE= 'geoclimate';"""
    }

    if (output_road && !output_datasource.hasTable(output_road)){
        output_datasource.execute """CREATE TABLE $output_road  (THE_GEOM GEOMETRY(GEOMETRY, $srid), 
        id_road serial, ID_SOURCE VARCHAR, WIDTH FLOAT, TYPE VARCHAR, CROSSING VARCHAR(30),
        SURFACE VARCHAR, SIDEWALK VARCHAR, ZINDEX INTEGER);
        CREATE INDEX IF NOT EXISTS idx_${output_road}_id_source ON $output_road (ID_SOURCE);"""
    }
    else if (output_road){
        def outputTableSRID = output_datasource.getSpatialTable(output_road).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_road is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_road (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_road WHERE ID_SOURCE= 'geoclimate';"""
    }

    if (output_rail && !output_datasource.hasTable(output_rail)){
        output_datasource.execute """CREATE TABLE $output_rail  (THE_GEOM GEOMETRY(GEOMETRY, $srid), 
        id_rail serial,ID_SOURCE VARCHAR, TYPE VARCHAR,CROSSING VARCHAR(30), ZINDEX INTEGER);
        CREATE INDEX IF NOT EXISTS idx_${output_rail}_id_source ON $output_rail (ID_SOURCE);"""
    }
    else if (output_rail){
        def outputTableSRID = output_datasource.getSpatialTable(output_rail).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_rail is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_rail (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_rail WHERE ID_SOURCE= 'geoclimate';"""
    }

    if (output_water && !output_datasource.hasTable(output_water)){
        output_datasource.execute """CREATE TABLE $output_water  (THE_GEOM GEOMETRY(POLYGON, $srid), 
        id_hydro serial, ID_SOURCE VARCHAR);
        CREATE INDEX IF NOT EXISTS idx_${output_water}_id_source ON $output_water (ID_SOURCE);"""
    }
    else if (output_water){
        def outputTableSRID = output_datasource.getSpatialTable(output_water).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_water is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_water (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_water WHERE ID_SOURCE= 'geoclimate';"""
    }

    if (output_vegetation && !output_datasource.hasTable(output_vegetation)){
        output_datasource.execute """CREATE TABLE $output_vegetation  (THE_GEOM GEOMETRY(POLYGON, $srid), 
        id_veget serial, ID_SOURCE VARCHAR, TYPE VARCHAR, HEIGHT_CLASS VARCHAR(4));
        CREATE INDEX IF NOT EXISTS idx_${output_vegetation}_id_source ON $output_vegetation (ID_SOURCE);"""
    }
    else if (output_vegetation){
        def outputTableSRID = output_datasource.getSpatialTable(output_vegetation).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_vegetation is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_vegetation (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_vegetation WHERE ID_SOURCE= 'geoclimate';"""
    }

    if (output_impervious && !output_datasource.hasTable(output_impervious)){
        output_datasource.execute """CREATE TABLE $output_impervious  (THE_GEOM GEOMETRY(POLYGON, $srid), 
        id_impervious serial, ID_SOURCE VARCHAR);
        CREATE INDEX IF NOT EXISTS idx_${output_impervious}_id_source ON $output_impervious (ID_SOURCE);"""
    }
    else if (output_impervious){
        def outputTableSRID = output_datasource.getSpatialTable(output_impervious).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_impervious is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_impervious (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_impervious WHERE ID_SOURCE= 'geoclimate';"""
    }

    if (output_urban_areas && !output_datasource.hasTable(output_urban_areas)){
        output_datasource.execute """CREATE TABLE $output_urban_areas  (THE_GEOM GEOMETRY(POLYGON, $srid), 
        id_urban serial, ID_SOURCE VARCHAR, TYPE VARCHAR, MAIN_USE VARCHAR);
        CREATE INDEX IF NOT EXISTS idx_${output_urban_areas}_id_source ON $output_urban_areas (ID_SOURCE);"""
    }
    else if (output_urban_areas){
        def outputTableSRID = output_datasource.getSpatialTable(output_urban_areas).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_urban_areas is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_urban_areas (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_urban_areas WHERE ID_SOURCE= 'geoclimate';"""
    }

    return true
}

/**
 * Save the output tables in a database
 * @param output_datasource a connexion a database
 * @param h2gis_datasource local H2GIS database
 * @param outputTableNames name of the output tables
 * @param h2gis_tables name of H2GIS to save
 * @param id_zone id of the zone
 * @param outputSRID srid code to reproject the data
 * @return
 */
def saveTablesInDatabase(JdbcDataSource output_datasource, JdbcDataSource h2gis_datasource, def outputTableNames, def h2gis_tables, def id_zone,def inputSRID, def outputSRID, def reproject){
    Connection con = output_datasource.getConnection()
    con.setAutoCommit(true);
    //Export building indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators,id_zone,h2gis_datasource, h2gis_tables.outputTableBuildingIndicators
            , "WHERE ID_RSU IS NOT NULL", inputSRID, outputSRID,reproject)

    //Export block indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.block_indicators,id_zone, h2gis_datasource, h2gis_tables.outputTableBlockIndicators
            , "WHERE ID_RSU IS NOT NULL", inputSRID, outputSRID,reproject)

    //Export rsu indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_indicators,id_zone, h2gis_datasource, h2gis_tables.outputTableRsuIndicators
            , "",inputSRID, outputSRID,reproject)

    //Export rsu lcz
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_lcz,id_zone,h2gis_datasource, h2gis_tables.outputTableRsuLcz
            , "",inputSRID,outputSRID,reproject)

    //Export zone
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.zones,id_zone, h2gis_datasource, h2gis_tables.outputTableZone
            , "",inputSRID,outputSRID,reproject)

    //Export building
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.building,id_zone, h2gis_datasource, h2gis_tables.buildingTableName
            , "",inputSRID,outputSRID,reproject)

    //Export road
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.road, id_zone,h2gis_datasource, h2gis_tables.roadTableName
            , "", inputSRID,outputSRID,reproject)
    //Export rail
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.rail,id_zone, h2gis_datasource, h2gis_tables.railTableName
            , "",inputSRID,outputSRID,reproject)
    //Export vegetation
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.vegetation,id_zone, h2gis_datasource, h2gis_tables.vegetationTableName
            , "",inputSRID,outputSRID,reproject)
    //Export water
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.water,id_zone, h2gis_datasource, h2gis_tables.hydrographicTableName
            , "",inputSRID,outputSRID,reproject)
    //Export impervious
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.impervious, id_zone,h2gis_datasource, h2gis_tables.imperviousTableName
            , "",inputSRID,outputSRID,reproject)

    //Export urban areas table
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.urban_areas, id_zone,h2gis_datasource, h2gis_tables.urbanAreasTableName
            , "",inputSRID,outputSRID,reproject)

    con.setAutoCommit(false)
}


/**
 * Generic method to save the abstract model tables prepared in H2GIS to another database
 * @param output_datasource connexion to a database
 * @param output_table name of the output table
 * @param srid srid to reproject
 * @param h2gis_datasource local H2GIS database
 * @param h2gis_table_to_save name of the H2GIS table to save
 * @param batchSize size of the batch
 * @param filter to limit the data from H2GIS *
 * @param outputSRID srid code used to reproject the output table
 * @return
 */
def abstractModelTableBatchExportTable(def output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter,def inputSRID,def outputSRID, def reproject){
    if(output_table) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            if (output_datasource.hasTable(output_table)) {
                output_datasource.execute("DELETE FROM $output_table WHERE id_zone=?", id_zone.toString());
                //If the table exists we populate it with the last result
                info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                int BATCH_MAX_SIZE = 1000;
                ITable inputRes = prepareTableOutput(h2gis_table_to_save, filter, inputSRID, h2gis_datasource, output_table, outputSRID, output_datasource)
                if (inputRes) {
                    def outputColumns = output_datasource.getTable(output_table).getColumnsTypes();
                    def outputconnection = output_datasource.getConnection()
                    try {
                        def inputColumns = inputRes.getColumnsTypes();
                        //We check if the number of columns is not the same
                        //If there is more columns in the input table we alter the output table
                        def outPutColumnsNames = outputColumns.keySet()
                        int columnsCount = outPutColumnsNames.size();
                        def diffCols = inputColumns.keySet().findAll { e -> !outPutColumnsNames*.toLowerCase().contains(e.toLowerCase()) }
                        def alterTable = ""
                        if (diffCols) {
                            inputColumns.each { entry ->
                                if (diffCols.contains(entry.key)) {
                                    alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key ${entry.value.equalsIgnoreCase("double") ? "DOUBLE PRECISION" : entry.value};"
                                    outputColumns.put(entry.key, entry.value)
                                }
                            }
                            output_datasource.execute(alterTable)
                        }
                        def finalOutputColumns = outputColumns.keySet();

                        def insertTable = "INSERT INTO $output_table (${finalOutputColumns.join(",")}) VALUES("

                        def flatList = outputColumns.inject([]) { result, iter ->
                            result += ":${iter.key.toLowerCase()}"
                        }.join(",")
                        insertTable += flatList
                        insertTable += ")";
                        //Collect all values
                        def ouputValues = finalOutputColumns.collectEntries { [it.toLowerCase(), null] }
                        ouputValues.put("id_zone", id_zone)
                        outputconnection.setAutoCommit(false);
                        output_datasource.withBatch(BATCH_MAX_SIZE, insertTable) { ps ->
                            inputRes.eachRow { row ->
                                //Fill the value
                                inputColumns.keySet().each { columnName ->
                                    def inputValue = row.getObject(columnName)
                                    if (inputValue) {
                                        ouputValues.put(columnName.toLowerCase(), inputValue)
                                    } else {
                                        ouputValues.put(columnName.toLowerCase(), null)
                                    }
                                }
                                ps.addBatch(ouputValues)
                            }
                        }
                    } catch (SQLException e) {
                        error("Cannot save the table $output_table.\n", e);
                        return false;
                    } finally {
                        outputconnection.setAutoCommit(true);
                        info "The table $h2gis_table_to_save has been exported into the table $output_table"
                    }
                }
            }else {
                def tmpTable =null
                info "Start to export the table $h2gis_table_to_save into the table $output_table"
                if (filter) {
                    if(!reproject){
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().save(output_datasource, output_table, true);
                    }
                    else{
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                    }
                    if(tmpTable){
                    //Workarround to update the SRID on resulset
                    output_datasource.execute"""ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);"""
                    }
                } else {
                    if(!reproject){
                        tmpTable =h2gis_datasource.getTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                   }else{
                        tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                    }
                }
                if(tmpTable) {
                    output_datasource.execute("UPDATE $output_table SET id_zone= ?", id_zone);
                    output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""")
                    info "The table $h2gis_table_to_save has been exported into the table $output_table"
                }else{
                    warn  "The table $h2gis_table_to_save hasn't been exported into the table $output_table"
                }
            }
        }
    }
}

/**
 * Generic method to save the indicator tables prepared in H2GIS to another database
 * @param output_datasource connexion to a database
 * @param output_table name of the output table
 * @param id_zone id of the zone
 * @param h2gis_datasource local H2GIS database
 * @param h2gis_table_to_save name of the H2GIS table to save
 * @param filter to limit the data from H2GIS *
 * @param inputSRID srid code of the inputable
 * @param outputSRID srid code used to reproject the output table
 * @return
 */
def indicatorTableBatchExportTable(def output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter, def inputSRID, def outputSRID, def reproject){
    if(h2gis_table_to_save) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            if (output_datasource.hasTable(output_table)) {
                output_datasource.execute("DELETE FROM $output_table WHERE id_zone=?", id_zone.toString());
                //If the table exists we populate it with the last result
                info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                int BATCH_MAX_SIZE = 1000;
                ITable inputRes = prepareTableOutput(h2gis_table_to_save, filter, inputSRID, h2gis_datasource, output_table, outputSRID, output_datasource)
                if (inputRes) {
                    def outputColumns  = output_datasource.getTable(output_table).getColumnsTypes();
                    def outputconnection = output_datasource.getConnection()
                    try {
                        def inputColumns = inputRes.getColumnsTypes();
                        //We check if the number of columns is not the same
                        //If there is more columns in the input table we alter the output table
                        def outPutColumnsNames = outputColumns.keySet()
                        int columnsCount = outPutColumnsNames.size();
                        def diffCols = inputColumns.keySet().findAll { e ->  !outPutColumnsNames*.toLowerCase().contains( e.toLowerCase() ) }
                        def alterTable = ""
                        if(diffCols){
                            inputColumns.each { entry ->
                                if (diffCols.contains(entry.key)){
                                    alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key ${entry.value.equalsIgnoreCase("double")?"DOUBLE PRECISION":entry.value};"
                                    outputColumns.put(entry.key, entry.value)
                                }
                            }
                            output_datasource.execute(alterTable)
                        }
                        def finalOutputColumns = outputColumns.keySet();

                        def insertTable = "INSERT INTO $output_table (${finalOutputColumns.join(",")}) VALUES("

                        def flatList =  outputColumns.inject([]) { result, iter ->
                            result+= ":${iter.key.toLowerCase()}"
                        }.join(",")
                        insertTable+= flatList
                        insertTable+=")";
                        //Collect all values
                        def ouputValues = finalOutputColumns.collectEntries {[it.toLowerCase(), null]}
                        ouputValues.put("id_zone", id_zone)
                        outputconnection.setAutoCommit(false);
                            output_datasource.withBatch(BATCH_MAX_SIZE, insertTable) { ps ->
                                inputRes.eachRow{ row ->
                                    //Fill the value
                                    inputColumns.keySet().each{columnName ->
                                        def inputValue = row.getObject(columnName)
                                        if(inputValue){
                                            ouputValues.put(columnName.toLowerCase(), inputValue)
                                        }else{
                                            ouputValues.put(columnName.toLowerCase(), null)
                                        }
                                    }
                                    ps.addBatch(ouputValues)
                                }
                            }

                    } catch (SQLException e) {
                        error("Cannot save the table $output_table.\n", e);
                        return false;
                    } finally {
                        outputconnection.setAutoCommit(true);
                        info "The table $h2gis_table_to_save has been exported into the table $output_table"
                    }
                }
                } else {
                    def tmpTable = null
                    info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                    if (filter) {
                        if (!reproject) {
                            tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().save(output_datasource, output_table, true);
                        } else {
                            tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                        }
                        if(tmpTable) {
                            //Workarround to update the SRID on resulset
                            output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);"""
                        }
                    } else {
                        if (!reproject) {
                            tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                        } else {
                            tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                        }
                    }
                    if(tmpTable){
                    if(!output_datasource.getTable(output_table).hasColumn("id_zone")) {
                        output_datasource.execute("ALTER TABLE $output_table ADD COLUMN id_zone VARCHAR");
                    }
                    output_datasource.execute("UPDATE $output_table SET id_zone= ?", id_zone);
                    output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""")
                    info "The table $h2gis_table_to_save has been exported into the table $output_table"
                    }
                    else{
                        warn "The table $h2gis_table_to_save hasn't been exported into the table $output_table"
                    }
                }
        }
    }
}

/**
 * Method to prepare a ITable aka resulset to export table in a database
 * @param h2gis_table_to_save
 * @param inputSRID
 * @param h2gis_datasource
 * @param output_table
 * @param outputSRID
 * @param output_datasource
 * @return
 */
def prepareTableOutput(def h2gis_table_to_save, def filter, def inputSRID,def h2gis_datasource, def output_table, def outputSRID,def output_datasource){
    def targetTableSrid = output_datasource.getSpatialTable(output_table).srid
    if (filter) {
        if(outputSRID==0){
            if(inputSRID==targetTableSrid){
                inputRes =  h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getTable()
            }else {
                if(targetTableSrid==0 && inputSRID==0){
                    return h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getTable()
                }else if(targetTableSrid!=0 && inputSRID!=0){
                    return h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(targetTableSrid)
                }
                else{
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        }
        else{
            if(inputSRID==targetTableSrid){
                return h2gis_datasource.getTable(h2gis_table_to_save)
            }else{
                if(targetTableSrid==0 && inputSRID==0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save)
                }else if(targetTableSrid!=0 && inputSRID!=0){
                    return h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(targetTableSrid)
                }
                else{
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        }
    }
    else {
        if(outputSRID==0){
            if(inputSRID==targetTableSrid){
                return  h2gis_datasource.getTable(h2gis_table_to_save)
            }else {
                if(targetTableSrid==0 && inputSRID==0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save)
                }else if(targetTableSrid!=0 && inputSRID!=0){
                    return h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(targetTableSrid)
                }
                else{
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        }
        else{
            if(inputSRID==targetTableSrid){
                return h2gis_datasource.getTable(h2gis_table_to_save)
            }else{
                if(targetTableSrid==0 && inputSRID==0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save)
                }else if(targetTableSrid!=0 && inputSRID!=0){
                    return h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(targetTableSrid)
                }
                else{
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        }
    }
}


/**
 * Parse a json file to a Map
 * @param jsonFile
 * @return
 */
static Map readJSONParameters(def jsonFile) {
    def jsonSlurper = new JsonSlurper()
    if (jsonFile) {
        return jsonSlurper.parse(jsonFile)
    }
}
