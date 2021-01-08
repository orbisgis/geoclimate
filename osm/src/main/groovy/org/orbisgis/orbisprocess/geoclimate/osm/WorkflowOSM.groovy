package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2.tools.DeleteDbFiles
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.FileUtilities
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
                    error "The configuration file doesn't exist"
                    return null
                }
                if(!FileUtilities.isExtensionWellFormated(configFile, "json")){
                    error "The configuration file must be a json file"
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
                    databasePath =  databaseFolder + File.separator + databaseName
                    def h2gis_name= geoclimatedb.get("name")
                    if(h2gis_name){
                        def dbName = h2gis_name.split(";")
                        databaseName=dbName[0]
                        databasePath =  databaseFolder + File.separator + h2gis_name
                    }
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
                                                    "urban_areas",
                                                    "rsu_urban_typo_area",
                                                    "rsu_urban_typo_floor_area",
                                                    "building_urban_typo"]
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
                            if(!h2gis_datasource){
                                error "Cannot load the local H2GIS database to run Geoclimate"
                                return
                            }
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
                                if(!h2gis_datasource){
                                    error "Cannot load the local H2GIS database to run Geoclimate"
                                    return
                                }
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
                            if(!outputTableNames){
                                error "You must set at least one table name to export in the database.\n" +
                                        "Available tables key names are : ${geoclimateTableNames.join(",")}"
                                return
                            }
                            def allowedOutputTableNames = geoclimateTableNames.intersect(outputTableNames.keySet())
                            def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() != allowedOutputTableNames.size()
                            if (allowedOutputTableNames && !notSameTableNames) {
                                def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                if (!output_datasource) {
                                    return null
                                }
                                def h2gis_datasource = H2GIS.open(h2gis_properties)
                                if(!h2gis_datasource){
                                    error "Cannot load the local H2GIS database to run Geoclimate"
                                    return
                                }
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
            id_zones.eachWithIndex { id_zone, index ->
                def start = System.currentTimeMillis();
                //Extract the zone table and read its SRID
                def zoneTableNames = extractOSMZone(h2gis_datasource, id_zone, processing_parameters)
                if (zoneTableNames) {
                    id_zone = id_zone in Map ? "bbox_" + id_zone.join('_') : id_zone
                    def zoneTableName = zoneTableNames.outputZoneTable
                    def zoneEnvelopeTableName = zoneTableNames.outputZoneEnvelopeTable
                    if(h2gis_datasource.getTable(zoneTableName).getRowCount()==0){
                        error "Cannot find any geometry to define the zone to extract the OSM data"
                        return
                    }
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
                                    urbanAreasTableName       : urbanAreasTable])

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

                            //Sea/Land mask
                            format = OSM.formatSeaLandMask
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.coastlineTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])

                            def seaLandMaskTableName = format.results.outputTableName

                            //Sea/Land mask
                            format = OSM.mergeWaterAndSeaLandTables
                            format.execute([
                                    datasource           : h2gis_datasource,
                                    inputSeaLandTableName: seaLandMaskTableName, inputWaterTableName: hydrographicTableName,
                                    epsg                 : srid])

                            hydrographicTableName = format.results.outputTableName

                            info "OSM GIS layers formated"

                            //Compute the indicators
                            IProcess geoIndicators = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
                            if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zoneTableName,
                                    buildingTable: buildingTableName, roadTable: roadTableName,
                                    railTable: railTableName, vegetationTable: vegetationTableName,
                                    hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                                    buildingEstimateTableName: buildingEstimateTableName,
                                    surface_vegetation: processing_parameters.surface_vegetation,
                                    surface_hydro: processing_parameters.surface_hydro,
                                    snappingTolerance: processing_parameters.snappingTolerance,
                                    indicatorUse: processing_parameters.indicatorUse,
                                    svfSimplified: processing_parameters.svfSimplified,
                                    prefixName: processing_parameters.prefixName,
                                    mapOfWeights: processing_parameters.mapOfWeights,
                                    urbanTypoModelName: "URBAN_TYPOLOGY_OSM_RF_2_1.model",
                                    buildingHeightModelName: estimateHeight ? "BUILDING_HEIGHT_OSM_RF_2_0.model" : "")) {
                                error "Cannot build the geoindicators for the zone $id_zone"
                            } else {
                                def results = geoIndicators.getResults()
                                results.put("buildingTableName", buildingTableName)
                                results.put("roadTableName", roadTableName)
                                results.put("railTableName", railTableName)
                                results.put("hydrographicTableName", hydrographicTableName)
                                results.put("vegetationTableName", vegetationTableName)
                                results.put("imperviousTableName", imperviousTableName)
                                results.put("urbanAreasTableName", urbanAreasTable)

                                //Compute the indicator at grid scale if specified
                                def   grid_indicators = processing_parameters.grid_indicators
                                if(grid_indicators){
                                    def x_size = grid_indicators.x_size
                                    def y_size = grid_indicators.y_size
                                    //Start to compute the grid
                                    def gridP = Geoindicators.SpatialUnits.createGrid()
                                    def box = h2gis_datasource.getSpatialTable(zoneEnvelopeTableName).getExtent()
                                    if(gridP.execute([geometry: box, deltaX: x_size, deltaY: y_size,  datasource: h2GIS])) {
                                        def list_indicators = grid_indicators.indicators
                                        /*
                                        * Make aggregation process with previous grid and current rsu lcz
                                        */
                                        def outputTableRsuLcz =results.outputTableRsuLcz
                                        if (list_indicators.contains("RSU_LCZ") && outputTableRsuLcz) {
                                        def targetTableName = gridProcess.results.outputTableName
                                        def indicatorName = "LCZ1"
                                        def upperScaleAreaStatistics = Geoclimate.GenericIndicators.upperScaleAreaStatistics()
                                        if (upperScaleAreaStatistics.execute(
                                                [upperTableName: targetTableName,
                                                 upperColumnId : "id",
                                                 lowerTableName: outputTableRsuLcz,
                                                 lowerTableName: outputTableRsuLcz,
                                                 lowerColumName: indicatorName,
                                                 prefixName    : "agg",
                                                 datasource    : h2gis_datasource])) {
                                            results.put("grid_indicators_lcz", upperScaleAreaStatistics.results.outputTableName)
                                        }
                                        else{
                                           info "Cannot compute the LCZ at grid scale"
                                        }
                                        }
                                    }
                                }
                            if (outputFolder  && ouputTableFiles) {
                                saveOutputFiles(h2gis_datasource, id_zone, results, ouputTableFiles, outputFolder, "osm_", outputSRID, reproject, deleteOutputData)
                            }
                            if (output_datasource) {
                                saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)
                            }
                        }

                        } else {
                            error "Cannot load the OSM file ${extract.results.outputFilePath}"
                            return
                        }
                    } else {
                        error "Cannot execute the overpass query $query"
                        return
                    }
                } else {
                    error "Cannot calculate a bounding box to extract OSM data"
                    return
                }

                info "Number of areas processed ${index + 1} on $nbAreas"
            }
            return [outputMessage: "The OSM workflow has been executed"]
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

        datasource.execute """drop table if exists ${outputZoneTable}; 
        create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomUTM.toString()}', ${epsg}), '${zoneToExtract.toString()}');"""

        datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; 
         create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(con, tmpGeomEnv, epsg).toString()}',${epsg}), '${zoneToExtract.toString()}');
        """

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
                        "urban_areas",
                        "rsu_urban_typo_area",
                        "rsu_urban_typo_floor_area",
                        "building_urban_typo"]
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
                             surface_vegetation: 10000,
                             surface_hydro: 2500,
                             snappingTolerance :0.01,
                             mapOfWeights :  ["sky_view_factor"                : 4,
                                              "aspect_ratio"                   : 3,
                                              "building_surface_fraction"      : 8,
                                              "impervious_surface_fraction"    : 0,
                                              "pervious_surface_fraction"      : 0,
                                              "height_of_roughness_elements"   : 6,
                                              "terrain_roughness_length"       : 0.5],
                             hLevMin : 3, hLevMax: 15, hThresholdLev2: 10,
                             estimateHeight:false,
                             lczModelName: "LCZ_OSM_RF_1_0.model",
                             urbanTypoModelName: "URBAN_TYPOLOGY_OSM_RF_2_0.model"]
    if(processing_parameters){
        def distanceP =  processing_parameters.distance
        if(distanceP && distanceP in Number){
            defaultParameters.distance = distanceP
        }
        def indicatorUseP = processing_parameters.indicatorUse
        if(indicatorUseP && indicatorUseP in List){
            defaultParameters.indicatorUse = indicatorUseP
        }

        def snappingToleranceP =  processing_parameters.snappingTolerance
        if(snappingToleranceP && snappingToleranceP in Number){
            defaultParameters.snappingTolerance = snappingToleranceP
        }

        def surface_vegetationP =  processing_parameters.surface_vegetation
        if(surface_vegetationP && surface_vegetationP in Number){
            defaultParameters.surface_vegetation = surface_vegetationP
        }
        def surface_hydroP =  processing_parameters.surface_hydro
        if(surface_hydroP && surface_hydroP in Number){
            defaultParameters.surface_hydro = surface_hydroP
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
        def estimateHeight = processing_parameters.estimateHeight
        if(estimateHeight && estimateHeight in Boolean){
            defaultParameters.estimateHeight = estimateHeight
        }

        //Check for grid indicators

        def  grid_indicators = processing_parameters.grid_indicators
        if(grid_indicators){
            def x_size = grid_indicators.x_size
            def y_size = grid_indicators.y_size
            def list_indicators = grid_indicators.indicators
            if(x_size && y_size){
                if(x_size<=0 || y_size<= 0){
                    info "Invalid grid size padding. Must be greater that 0"
                    return
                }
                if(!list_indicators){
                    info "The list of indicator names cannot be null or empty"
                    return
                }
                def allowed_grid_indicators=["BUILDING_AREA","WATER_AREA","VEGETATION_AREA","RSU_URBAN_TYPO", "RSU_LCZ"]
                def allowedOutputIndicators = allowed_grid_indicators.intersect(list_indicators*.toUpperCase())
                if(allowedOutputIndicators){
                def grid_indicators_tmp =  [
                        "x_size": x_size,
                        "y_size": y_size,
                        "indicators": allowedOutputIndicators
                ]
                defaultParameters.put("grid_indicators", grid_indicators_tmp)
                }
                else {
                    info "Please set a valid list of indicator names in ${allowed_grid_indicators}"
                    return
                }
            }
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
 * @param deleteOutputData delete the files if exist
 * @return
 */
def saveOutputFiles(def h2gis_datasource, def id_zone, def results, def outputFiles, def ouputFolder, def subFolderName, def outputSRID, def reproject, def deleteOutputData){
    //Create a subfolder to store each results
    def folderName = id_zone in Map?id_zone.join("_"):id_zone
    def subFolder = new File(ouputFolder.getAbsolutePath()+File.separator+subFolderName+folderName)
    if(!subFolder.exists()){
        subFolder.mkdir()
    }
    else{
        FileUtilities.deleteFiles(subFolder)
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
        }else if(it.equals("rsu_urban_typo_area")){
            saveTableAsGeojson(results.outputTableRsuUrbanTypoArea, "${subFolder.getAbsolutePath()+File.separator+"rsu_urban_typo_area"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it.equals("rsu_urban_typo_floor_area")){
            saveTableAsGeojson(results.outputTableRsuUrbanTypoFloorArea, "${subFolder.getAbsolutePath()+File.separator+"rsu_urban_typo_floor_area"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it.equals("building_urban_typo")){
            saveTableAsGeojson(results.outputTableBuildingUrbanTypo, "${subFolder.getAbsolutePath()+File.separator+"building_urban_typo"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
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
 * Save the output tables in a database
 * @param output_datasource a connexion a database
 * @param h2gis_datasource local H2GIS database
 * @param outputTableNames name of the output tables
 * @param h2gis_tables name of H2GIS to save
 * @param id_zone id of the zone
 * @param outputSRID srid code to reproject the data *
 * @param reproject the output table
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

    //Export rsu_urban_typo_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_urban_typo_area,id_zone,h2gis_datasource, h2gis_tables.outputTableRsuUrbanTypoArea
            , "",inputSRID,outputSRID,reproject)

    //Export rsu_urban_typo_floor_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_urban_typo_floor_area,id_zone,h2gis_datasource, h2gis_tables.outputTableRsuUrbanTypoFloorArea
            , "",inputSRID,outputSRID,reproject)

    //Export building_urban_typo
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_urban_typo,id_zone,h2gis_datasource, h2gis_tables.outputTableBuildingUrbanTypo
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
                        //Because the select query reproject doesn't contain any geometry metadata
                        output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""");
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
                        //Because the select query reproject doesn't contain any geometry metadata
                        output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""");
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
    if(output_table){
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
                            if(tmpTable) {
                                //Workarround to update the SRID on resultset
                                output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);"""
                            }

                        } else {
                            tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                            if(tmpTable) {
                                //Workarround to update the SRID on resultset
                                output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $outputSRID) USING ST_SetSRID(the_geom,$outputSRID);"""
                            }
                        }

                    } else {
                        if (!reproject) {
                            tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                        } else {
                            tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                            //Because the select query reproject doesn't contain any geometry metadata
                            output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""")
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