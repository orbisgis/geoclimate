package org.orbisgis.geoclimate.osm

import groovy.transform.BaseScript
import org.h2.tools.DeleteDbFiles
import org.h2gis.functions.io.utility.IOMethods
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.FileUtilities
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.geoclimate.osmtools.utils.OSMElement
import org.orbisgis.geoclimate.worldpoptools.WorldPopTools
import org.orbisgis.data.api.dataset.ITable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.data.H2GIS
import org.orbisgis.process.api.IProcess
import org.orbisgis.geoclimate.osmtools.OSMTools

import java.sql.SQLException

import org.orbisgis.geoclimate.Geoindicators

@BaseScript OSM OSM


/**
 * Extract OSM data and compute geoindicators.
 *
 * The parameters of the processing chain is defined
 * from a configuration file or a Map.
 * The configuration file is stored in a json format
 *
 * @param input The path of the configuration file or a Map
 *
 * The input file or the Map supports the following entries
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
 *  *            "locations" : ["filter"] // OSM filter to extract the data. Can be a place name supported by nominatim
 *                                  // e.g "osm" : ["oran", "plourivo"]
 *                                  // or bbox expressed as "osm" : [[38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417]]
 *  *           "all":true //optional value if the user wants to download a limited set of data. Default is true to download all OSM elements
 *  *           "area" : 2000 //optional value to control the area ov the OSM BBox, default is 1000 km²
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
 *  *         "prefixName": "",
 *  *        rsu_indicators:{
 *  *         "indicatorUse": ["LCZ", "UTRF", "TEB"],
 *  *         "svfSimplified": false,
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
 *          }
 *  *     }
 *  *     }
 *  The parameters entry tag contains all geoclimate chain parameters.
 *  When a parameter is not specificied a default value is set.
 *
 * - distance The integer value to expand the envelope of zone when recovering the data
 * - distance The integer value to expand the envelope of zone when recovering the data
 * (some objects may be badly truncated if they are not within the envelope)
 * - indicatorUse List of geoindicator types to compute (default ["LCZ", "UTRF", "TEB"]
 *                  --> "LCZ" : compute the indicators needed for the LCZ classification (Stewart et Oke, 2012)
 *                  --> "UTRF" : compute the indicators needed for the urban typology classification (Bocher et al., 2017)
 *                  --> "TEB" : compute the indicators needed for the Town Energy Balance model
 * - svfSimplified A boolean indicating whether or not the simplified version of the SVF should be used. This
 * version is faster since it is based on a simple relationship between ground SVF calculated at RSU scale and
 * facade density (Bernard et al. 2018).
 * - prefixName A prefix used to name the output table (default ""). Could be useful in case the user wants to
 * investigate the sensibility of the chain to some input parameters
 * - mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
 * - hLevMin Minimum building level height
 *
 * @return
 * a map with the name of zone and a list of the output tables computed and stored in the local database, otherwise throw an error.
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
        inputs input: Object
        outputs output: Map
        run { input ->
            //OSM workflow parameters
            Map parameters = null
            if (input) {
                if (input instanceof String) {
                    //Check if it's a path to a file
                    def configFile = new File(input)
                    if (!configFile.isFile()) {
                        error "The configuration file doesn't exist"
                        return
                    }
                    if (!FileUtilities.isExtensionWellFormated(configFile, "json")) {
                        error "The configuration file must be a json file"
                        return
                    }
                    parameters = Geoindicators.WorkflowUtilities.readJSON(configFile)
                } else if (input instanceof Map) {
                    parameters = input
                }
            } else {
                error "The input parameters cannot be null or empty.\n Please set a path to a configuration file or " +
                        "a map with all required parameters"
                return
            }
            if (!parameters) {
                error "Wrong input parameters"
                return
            }

            debug "Reading file parameters"
            debug parameters.get("description")
            def inputParameters = parameters.get("input")
            def outputParameter = parameters.get("output")
            //Default H2GIS database properties
            def databaseFolder = System.getProperty("java.io.tmpdir")
            def databaseName = "osm" + UUID.randomUUID().toString().replaceAll("-", "_")
            def databasePath = databaseFolder + File.separator + databaseName
            def h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
            def delete_h2gis = true
            def geoclimatedb = parameters.get("geoclimatedb")
            if (geoclimatedb) {
                def h2gis_folder = geoclimatedb.get("folder")
                if (h2gis_folder) {
                    File tmp_folder_db = new File(h2gis_folder)
                    if (!tmp_folder_db.exists()) {
                        if (!tmp_folder_db.mkdir()) {
                            h2gis_folder = null
                            error "You don't have permission to write in the folder $h2gis_folder \n" +
                                    "Please check the folder."
                            return
                        }
                    } else if (!tmp_folder_db.isDirectory()) {
                        error "Invalid output folder $h2gis_folder."
                        return
                    }
                    databaseFolder = h2gis_folder
                }
                databasePath = databaseFolder + File.separator + databaseName
                def h2gis_name = geoclimatedb.get("name")
                if (h2gis_name) {
                    def dbName = h2gis_name.split(";")
                    databaseName = dbName[0]
                    databasePath = databaseFolder + File.separator + h2gis_name
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
                if (databasePath) {
                    h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
                }
            }
            if (!inputParameters) {
                error "Cannot find any input parameters."
                return
            }

            def locations = inputParameters.locations as Set
            if (!locations) {
                error "Please set at least one OSM location (place name or bounding box)."
                return
            }
            def downloadAllOSMData = inputParameters.get("all")
            if (!downloadAllOSMData) {
                downloadAllOSMData = true
            } else if (!downloadAllOSMData in Boolean) {
                error "The all parameter must be a boolean value"
                return null
            }
            def osm_size_area = inputParameters.get("area")

            if (!osm_size_area) {
                //Default size in km²
                osm_size_area = 1000
            } else if (osm_size_area < 0) {
                error "The area of the bounding box to be extracted from OSM must be greater than 0 km²"
                return null
            }

            def overpass_timeout = inputParameters.get("timeout")
            if (!overpass_timeout) {
                overpass_timeout = 900
            } else if (overpass_timeout <= 180) {
                error "The timeout value must be greater than the default value : 180 s"
                return null
            }

            def overpass_maxsize = inputParameters.get("maxsize")

            if (!overpass_maxsize) {
                overpass_maxsize = 536870912
            } else if (overpass_maxsize <= 536870912) {
                error "The maxsize value must be greater than the default value :  536870912 (512 MB)"
                return null
            }

            //Change the endpoint to get the overpass data
            def overpass_enpoint = inputParameters.get("endpoint")

            if (!overpass_enpoint) {
                overpass_enpoint = "https://lz4.overpass-api.de/api"
            }
            System.setProperty("OVERPASS_ENPOINT", overpass_enpoint)

            def deleteOSMFile = inputParameters.get("delete")
            if (!deleteOSMFile) {
                deleteOSMFile = false
            } else if (!Boolean.valueOf(deleteOSMFile)) {
                error "The delete option must be false or true"
                return null
            }

            def outputWorkflowTableNames = ["building_indicators",
                                            "block_indicators",
                                            "rsu_indicators",
                                            "rsu_lcz",
                                            "zone",
                                            "building",
                                            "road",
                                            "rail",
                                            "water",
                                            "vegetation",
                                            "impervious",
                                            "urban_areas",
                                            "rsu_utrf_area",
                                            "rsu_utrf_floor_area",
                                            "building_utrf",
                                            "grid_indicators",
                                            "sea_land_mask",
                                            "building_height_missing",
                                            "road_traffic",
                                            "population",
                                            "ground_acoustic"]

            //Get processing parameters
            def processing_parameters = extractProcessingParameters(parameters.get("parameters"))
            if (!processing_parameters) {
                return
            }

            def outputDatasource
            def outputTables
            def file_outputFolder
            def outputFileTables
            def outputSRID
            def deleteOutputData

            if (outputParameter) {
                def outputDataBase = outputParameter.get("database")
                def outputFolder = outputParameter.get("folder")
                deleteOutputData = outputParameter.get("delete")
                if (!deleteOutputData) {
                    deleteOutputData = true
                } else if (!deleteOutputData in Boolean) {
                    error "The delete parameter must be a boolean value"
                    return null
                }
                outputSRID = outputParameter.get("srid")
                if (outputSRID && outputSRID <= 0) {
                    error "The output srid must be greater or equal than 0"
                    return null
                }
                if (outputFolder) {
                    //Check if we can write in the output folder
                    def outputFiles = Geoindicators.WorkflowUtilities.buildOutputFolderParameters(outputFolder, outputWorkflowTableNames)
                    outputFileTables = outputFiles.tables
                    //Check if we can write in the output folder
                    file_outputFolder = new File(outputFiles.path)
                    if (!file_outputFolder.exists()) {
                        if (file_outputFolder.mkdir()) {
                            file_outputFolder = null
                            error "You don't have permission to write in the folder $outputFolder \n" +
                                    "Please check the folder."
                            return
                        }
                    } else if (!file_outputFolder.isDirectory()) {
                        error "Invalid output folder $file_outputFolder."
                        return
                    }
                }
                if (outputDataBase) {
                    //Check the conditions to store the results a database
                    def outputDataBaseData = Geoindicators.WorkflowUtilities.buildOutputDBParameters(outputDataBase, outputDataBase.tables, outputWorkflowTableNames)
                    outputDatasource = outputDataBaseData.datasource
                    outputTables = outputDataBaseData.tables
                }
            }

            if (locations && locations in Collection) {
                def h2gis_datasource = H2GIS.open(h2gis_properties)
                if (!h2gis_datasource) {
                    error "Cannot load the local H2GIS database to run Geoclimate"
                    return
                }
                def logTableZones = postfix("log_zones")
                def osmprocessing = osm_processing()
                if (!osmprocessing.execute(h2gis_datasource: h2gis_datasource,
                        processing_parameters: processing_parameters,
                        id_zones: locations.findAll { it }, outputFolder: file_outputFolder, ouputTableFiles: outputFileTables,
                        output_datasource: outputDatasource, outputTableNames: outputTables, outputSRID: outputSRID, downloadAllOSMData: downloadAllOSMData,
                        deleteOutputData: deleteOutputData, deleteOSMFile: deleteOSMFile,
                        logTableZones: logTableZones, bbox_size: osm_size_area,
                        overpass_timeout: overpass_timeout, overpass_maxsize: overpass_maxsize)) {
                    h2gis_datasource.getSpatialTable(logTableZones).save("${databaseFolder + File.separator}logzones.geojson")
                    return null
                }
                if (delete_h2gis) {
                    def localCon = h2gis_datasource.getConnection()
                    if (localCon) {
                        localCon.close()
                        DeleteDbFiles.execute(databaseFolder, databaseName, true)
                        debug "The local H2GIS database : ${databasePath} has been deleted"
                    } else {
                        error "Cannot delete the local H2GIS database : ${databasePath} "
                    }
                }
                return [output: osmprocessing.getResults().outputTableNames]

            } else {
                error "Invalid  OSM area from $locations"
                return null
            }
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
 * @param bbox_size the size of OSM BBox in km²
 * @return the identifier of the zone and the list of the output tables computed and stored in the local database for this zone
 */
IProcess osm_processing() {
    return create {
        title "Build OSM data and compute the geoindicators"
        id "osm_processing"
        inputs h2gis_datasource: JdbcDataSource, processing_parameters: Map, id_zones: Map,
                outputFolder: "", ouputTableFiles: "", output_datasource: "", outputTableNames: "", outputSRID: Integer, downloadAllOSMData: true,
                deleteOutputData: true, deleteOSMFile: false, logTableZones: String, bbox_size: 1000,
                overpass_timeout: 180, overpass_maxsize: 536870912
        outputs outputTableNames: Map
        run { H2GIS h2gis_datasource, processing_parameters, id_zones, outputFolder, ouputTableFiles, output_datasource, outputTableNames,
              outputSRID, downloadAllOSMData, deleteOutputData, deleteOSMFile, logTableZones, bbox_size, overpass_timeout,
              overpass_maxsize ->
            //Store the zone identifier and the names of the tables
            def outputTableNamesResult = [:]
            //Create the table to log on the processed zone
            h2gis_datasource.execute """DROP TABLE IF EXISTS $logTableZones;
            CREATE TABLE $logTableZones (the_geom GEOMETRY(GEOMETRY, 4326), request VARCHAR, info VARCHAR);""".toString()
            int nbAreas = id_zones.size()
            info "$nbAreas osm areas will be processed"
            id_zones.each { id_zone ->
                //Extract the zone table and read its SRID
                def zones = extractOSMZone(h2gis_datasource, id_zone, processing_parameters.distance, bbox_size)
                if (zones) {
                    id_zone = id_zone in Collection ? id_zone.join('_') : id_zone
                    def zone = zones.outputZoneTable
                    def zoneEnvelopeTableName = zones.outputZoneEnvelopeTable
                    if (h2gis_datasource.getTable(zone).getRowCount() == 0) {
                        error "Cannot find any geometry to define the zone to extract the OSM data"
                        return
                    }
                    def srid = h2gis_datasource.getSpatialTable(zone).srid
                    def reproject = false
                    if (outputSRID) {
                        if (outputSRID != srid) {
                            reproject = true
                        }
                    } else {
                        outputSRID = srid
                    }
                    //Prepare OSM extraction
                    //TODO set key values ?
                    def query = "[timeout:$overpass_timeout][maxsize:$overpass_maxsize]" + Utilities.buildOSMQuery(zones.envelope, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

                    if (downloadAllOSMData) {
                        //Create a custom OSM query to download all requiered data. It will take more time and resources
                        //because much more OSM elements will be returned
                        def keysValues = ["building", "railway", "amenity",
                                          "leisure", "highway", "natural",
                                          "landuse", "landcover",
                                          "vegetation", "waterway", "area", "aeroway", "area:aeroway"]
                        query = "[timeout:$overpass_timeout][maxsize:$overpass_maxsize]" + Utilities.buildOSMQueryWithAllData(zones.envelope, keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
                    }

                    def extract = OSMTools.Loader.extract()
                    if (extract.execute(overpassQuery: query)) {
                        IProcess createGISLayerProcess = OSM.InputDataLoading.createGISLayers()
                        if (createGISLayerProcess.execute(datasource: h2gis_datasource, osmFilePath: extract.results.outputFilePath, epsg: srid)) {
                            if (deleteOSMFile) {
                                if (new File(extract.results.outputFilePath).delete()) {
                                    debug "The osm file ${extract.results.outputFilePath}has been deleted"
                                }
                            }
                            def gisLayersResults = createGISLayerProcess.getResults()
                            def rsu_indicators_params = processing_parameters.rsu_indicators
                            def grid_indicators_params = processing_parameters.grid_indicators
                            def road_traffic = processing_parameters.road_traffic
                            def worldpop_indicators = processing_parameters.worldpop_indicators

                            debug "Formating OSM GIS layers"
                            //Format urban areas
                            IProcess format = OSM.InputDataFormatting.formatUrbanAreas()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.urbanAreasTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])

                            def urbanAreasTable = format.results.outputTableName

                            format = OSM.InputDataFormatting.formatBuildingLayer()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.buildingTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid,
                                    h_lev_min                 : processing_parameters.hLevMin,
                                    urbanAreasTableName       : urbanAreasTable])

                            def buildingTableName = format.results.outputTableName
                            def buildingEstimateTableName = format.results.outputEstimateTableName


                            format = OSM.InputDataFormatting.formatRailsLayer()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.railTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])
                            def railTableName = format.results.outputTableName

                            format = OSM.InputDataFormatting.formatVegetationLayer()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.vegetationTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])
                            def vegetationTableName = format.results.outputTableName

                            format = OSM.InputDataFormatting.formatHydroLayer()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.hydroTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])
                            def hydrographicTableName = format.results.outputTableName

                            format = OSM.InputDataFormatting.formatImperviousLayer()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.imperviousTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])
                            def imperviousTableName = format.results.outputTableName

                            //Sea/Land mask
                            format = OSM.InputDataFormatting.formatSeaLandMask()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.coastlineTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])

                            def seaLandMaskTableName = format.results.outputTableName

                            //Merge the Sea/Land mask with water table
                            format = OSM.InputDataFormatting.mergeWaterAndSeaLandTables()
                            format.execute([
                                    datasource           : h2gis_datasource,
                                    inputSeaLandTableName: seaLandMaskTableName, inputWaterTableName: hydrographicTableName,
                                    epsg                 : srid])

                            hydrographicTableName = format.results.outputTableName

                            //Format road
                            format = OSM.InputDataFormatting.formatRoadLayer()
                            format.execute([
                                    datasource                : h2gis_datasource,
                                    inputTableName            : gisLayersResults.roadTableName,
                                    inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                    epsg                      : srid])

                            def roadTableName = format.results.outputTableName

                            debug "OSM GIS layers formated"
                            //Add the GIS layers to the list of results
                            def results = [:]
                            results.put("zone", zone)
                            results.put("road", roadTableName)
                            results.put("rail", railTableName)
                            results.put("water", hydrographicTableName)
                            results.put("vegetation", vegetationTableName)
                            results.put("impervious", imperviousTableName)
                            results.put("urban_areas", urbanAreasTable)
                            results.put("building", buildingTableName)
                            results.put("sea_land_mask", seaLandMaskTableName)
                            results.put("building_height_missing", buildingEstimateTableName)


                            //Compute traffic flow
                            if (road_traffic) {
                                IProcess format_traffic = Geoindicators.RoadIndicators.build_road_traffic()
                                format_traffic.execute([
                                        datasource    : h2gis_datasource,
                                        inputTableName: roadTableName,
                                        epsg          : srid])
                                results.put("road_traffic", format_traffic.results.outputTableName)
                            }

                            //Compute the RSU indicators
                            if (rsu_indicators_params.indicatorUse) {
                                def estimateHeight = rsu_indicators_params."estimateHeight"
                                IProcess geoIndicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators()
                                if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zone,
                                        buildingTable: buildingTableName, roadTable: roadTableName,
                                        railTable: railTableName, vegetationTable: vegetationTableName,
                                        hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                                        buildingEstimateTableName: buildingEstimateTableName,
                                        seaLandMaskTableName: seaLandMaskTableName,
                                        surface_vegetation: rsu_indicators_params.surface_vegetation,
                                        surface_hydro: rsu_indicators_params.surface_hydro,
                                        snappingTolerance: rsu_indicators_params.snappingTolerance,
                                        indicatorUse: rsu_indicators_params.indicatorUse,
                                        svfSimplified: rsu_indicators_params.svfSimplified,
                                        prefixName: processing_parameters.prefixName,
                                        mapOfWeights: rsu_indicators_params.mapOfWeights,
                                        utrfModelName: "UTRF_OSM_RF_2_2.model",
                                        buildingHeightModelName: estimateHeight ? "BUILDING_HEIGHT_OSM_RF_2_2.model" : "")) {

                                    error "Cannot build the geoindicators for the zone $id_zone"

                                    h2gis_datasource.execute "INSERT INTO $logTableZones VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error computing geoindicators')".toString()

                                    return
                                } else {
                                    results.putAll(geoIndicators.getResults())
                                }
                            }

                            //Extract and compute population indicators for the specified year
                            //This data can be used by the grid_indicators process
                            if (worldpop_indicators) {
                                IProcess extractWorldPopLayer = WorldPopTools.Extract.extractWorldPopLayer()
                                def coverageId = "wpGlobal:ppp_2020"
                                def bbox = [zones.envelope.getMinY() as Float, zones.envelope.getMinX() as Float,
                                            zones.envelope.getMaxY() as Float, zones.envelope.getMaxX() as Float]
                                if (extractWorldPopLayer.execute([coverageId: coverageId, bbox: bbox])) {
                                    IProcess importAscGrid = WorldPopTools.Extract.importAscGrid()
                                    if (importAscGrid.execute([worldPopFilePath: extractWorldPopLayer.results.outputFilePath,
                                                               epsg            : srid, tableName: coverageId.replaceAll(":", "_"), datasource: h2gis_datasource])) {
                                        results.put("population", importAscGrid.results.outputTableWorldPopName)

                                        IProcess process = Geoindicators.BuildingIndicators.buildingPopulation()
                                        if (!process.execute([inputBuilding  : results.building,
                                                              inputPopulation: importAscGrid.results.outputTableWorldPopName
                                                              , datasource   : h2gis_datasource])) {
                                            info "Cannot compute any population data at building level"
                                        }
                                        //Update the building table with the population data
                                        results.put("building", process.results.buildingTableName)

                                    } else {
                                        info "Cannot import the worldpop asc file $extractWorldPopLayer.results.outputFilePath"
                                        info "Create a default empty worldpop table"
                                        def outputTableWorldPopName = postfix "world_pop"
                                        h2gis_datasource.execute("""drop table if exists $outputTableWorldPopName;
                                        create table $outputTableWorldPopName (the_geom GEOMETRY(POLYGON, $srid), ID_POP INTEGER, POP FLOAT);""".toString())
                                        results.put("population", outputTableWorldPopName)
                                    }

                                } else {
                                    info "Cannot find the population grid $coverageId \n Create a default empty worldpop table"
                                    def outputTableWorldPopName = postfix "world_pop"
                                    h2gis_datasource.execute("""drop table if exists $outputTableWorldPopName;
                                    create table $outputTableWorldPopName (the_geom GEOMETRY(POLYGON, $srid), ID_POP INTEGER, POP FLOAT);""".toString())
                                    results.put("population", outputTableWorldPopName)
                                }
                            }
                            def noise_indicators = processing_parameters.noise_indicators

                            def geomEnv;
                            if (noise_indicators) {
                                if (noise_indicators.ground_acoustic) {
                                    geomEnv = h2gis_datasource.getSpatialTable(zone).getExtent()
                                    def gridP = Geoindicators.SpatialUnits.createGrid()
                                    if (gridP.execute([geometry: geomEnv, deltaX: 200, deltaY: 200, datasource: h2gis_datasource])) {
                                        def outputTable = gridP.results.outputTableName
                                        IProcess process = Geoindicators.NoiseIndicators.groundAcousticAbsorption()
                                        if (process.execute(["zone"    : outputTable, "id_zone": "id_grid",
                                                             building  : buildingTableName, road: roadTableName, vegetation: vegetationTableName,
                                                             water     : hydrographicTableName,
                                                             impervious: imperviousTableName,
                                                             datasource: h2gis_datasource])) {

                                            results.put("ground_acoustic", process.results.ground_acoustic)
                                        }
                                        h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTable".toString())
                                    }
                                }
                            }

                            //Default
                            def outputGrid = "geojson"
                            if (grid_indicators_params) {
                                if (!geomEnv) {
                                    geomEnv = h2gis_datasource.getSpatialTable(zone).getExtent()
                                }
                                outputGrid = grid_indicators_params.output
                                def x_size = grid_indicators_params.x_size
                                def y_size = grid_indicators_params.y_size
                                IProcess gridProcess = Geoindicators.WorkflowGeoIndicators.createGrid()
                                if (gridProcess.execute(datasource: h2gis_datasource, envelope: geomEnv,
                                        x_size: x_size, y_size: y_size,
                                        srid: srid, rowCol: grid_indicators_params.rowCol)) {
                                    def gridTableName = gridProcess.results.outputTableName
                                    IProcess rasterizedIndicators = Geoindicators.WorkflowGeoIndicators.rasterizeIndicators()
                                    if (rasterizedIndicators.execute(datasource: h2gis_datasource, grid: gridTableName,
                                            list_indicators: grid_indicators_params.indicators,
                                            building: buildingTableName, road: roadTableName, vegetation: vegetationTableName,
                                            water: hydrographicTableName, impervious: imperviousTableName,
                                            rsu_lcz: results.rsu_lcz,
                                            rsu_utrf_area: results.rsu_utrf_area,
                                            rsu_utrf_floor_area: results.rsu_utrf_floor_area,
                                            sea_land_mask: seaLandMaskTableName,
                                            prefixName: processing_parameters.prefixName
                                    )) {
                                        results.put("grid_indicators", rasterizedIndicators.results.outputTableName)
                                    }
                                } else {
                                    info "Cannot create a grid to aggregate the indicators"
                                    h2gis_datasource.execute "INSERT INTO $logTableZones VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error computing the grid indicators')".toString()

                                }
                            }

                            if (outputFolder && ouputTableFiles) {
                                saveOutputFiles(h2gis_datasource, id_zone, results, ouputTableFiles, outputFolder, "osm_", outputSRID, reproject, deleteOutputData, outputGrid)

                            }
                            if (output_datasource) {
                                saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)
                            }

                            outputTableNamesResult.put(id_zone in Collection ? id_zone.join("_") : id_zone, results.findAll { it.value != null })

                        } else {
                            h2gis_datasource.execute "INSERT INTO $logTableZones VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error loading the OSM file')".toString()
                            error "Cannot load the OSM file ${extract.results.outputFilePath}"
                            return
                        }
                    } else {
                        //Log in table
                        h2gis_datasource.execute "INSERT INTO $logTableZones VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error to extract the data with OverPass')".toString()
                        error "Cannot execute the overpass query $query"
                        return
                    }
                } else {
                    //Log in table
                    h2gis_datasource.execute "INSERT INTO $logTableZones VALUES(null,'$id_zone', 'Error to extract the zone with Nominatim')".toString()
                    return
                }
            }
            if (outputTableNamesResult) {
                return [outputTableNames: outputTableNamesResult]
            }
        }
    }
}

/**
 * Extract the OSM zone and its envelope area from Nominatim API
 *
 * @param datasource a connexion to the local H2GIS database
 * @param zoneToExtract the osm filter : place or bbox
 * @param distance to expand the OSM bbox
 * @return
 */
def static extractOSMZone(def datasource, def zoneToExtract, def distance, def bbox_size) {
    def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    if (zoneToExtract) {
        def GEOMETRY_TYPE = "GEOMETRY"
        Geometry geom = Utilities.getArea(zoneToExtract)
        if (!geom) {
            error("Cannot find an area from the location ${zoneToExtract}")
            return null
        }
        if (geom instanceof Polygon) {
            GEOMETRY_TYPE = "POLYGON"
        } else if (geom instanceof MultiPolygon) {
            GEOMETRY_TYPE = "MULTIPOLYGON"
        } else {
            error("Invalid geometry to extract the OSM data ${geom.getGeometryType()}")
            return null
        }

        /**
         * Create the OSM BBOX to be extracted
         */
        def envelope = GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

        //Find the best utm zone
        //Reproject the geometry and its envelope to the UTM zone
        def con = datasource.getConnection()
        def interiorPoint = envelope.centre()
        def epsg = GeographyUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
        def geomUTM = ST_Transform.ST_Transform(con, geom, epsg)

        //Check the size of the bbox
        if ((geomUTM.getEnvelopeInternal().getArea() / 1.0E+6) >= bbox_size) {
            error("The size of the OSM BBOX is greated than the limit : ${bbox_size} in km².\n" +
                    "Please increase the area parameter if you want to skip this limit.")
            return null
        }
        def tmpGeomEnv = geom.getFactory().toGeometry(envelope)
        tmpGeomEnv.setSRID(4326)

        datasource.execute """drop table if exists ${outputZoneTable}; 
        create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomUTM.toString()}', ${epsg}), '${zoneToExtract.toString()}');""".toString()

        datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; 
         create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(con, tmpGeomEnv, epsg).toString()}',${epsg}), '${zoneToExtract.toString()}');
        """.toString()

        return [outputZoneTable        : outputZoneTable,
                outputZoneEnvelopeTable: outputZoneEnvelopeTable,
                envelope               : envelope,
                geometry               : geom
        ]
    } else {
        error "The zone to extract cannot be null or empty"
        return null
    }
    return null
}


/**
 * Read the file parameters and create a new map of parameters
 * The map of parameters is initialized with default values
 *
 * @param processing_parameters the file parameters
 * @return a filled map of parameters
 */
def static extractProcessingParameters(def processing_parameters) {
    def defaultParameters = [distance: 0f, prefixName: "",
                             hLevMin : 3]
    def rsu_indicators_default = [indicatorUse      : [],
                                  svfSimplified     : true,
                                  surface_vegetation: 10000f,
                                  surface_hydro     : 2500f,
                                  snappingTolerance : 0.01f,
                                  mapOfWeights      : ["sky_view_factor"             : 4,
                                                       "aspect_ratio"                : 3,
                                                       "building_surface_fraction"   : 8,
                                                       "impervious_surface_fraction" : 0,
                                                       "pervious_surface_fraction"   : 0,
                                                       "height_of_roughness_elements": 6,
                                                       "terrain_roughness_length"    : 0.5],
                                  estimateHeight    : true,
                                  utrfModelName     : "UTRF_OSM_RF_2_2.model"]
    defaultParameters.put("rsu_indicators", rsu_indicators_default)

    if (processing_parameters) {
        def distanceP = processing_parameters.distance
        if (distanceP && distanceP in Number) {
            defaultParameters.distance = distanceP
        }
        def prefixNameP = processing_parameters.prefixName
        if (prefixNameP && prefixNameP in String) {
            defaultParameters.prefixName = prefixNameP
        }

        def hLevMinP = processing_parameters.hLevMin
        if (hLevMinP && hLevMinP in Integer) {
            defaultParameters.hLevMin = hLevMinP
        }

        //Check for rsu indicators
        def rsu_indicators = processing_parameters.rsu_indicators
        if (rsu_indicators) {
            def indicatorUseP = rsu_indicators.indicatorUse
            if (indicatorUseP && indicatorUseP in List) {
                def allowed_rsu_indicators = ["LCZ", "UTRF", "TEB"]
                def allowedOutputRSUIndicators = allowed_rsu_indicators.intersect(indicatorUseP*.toUpperCase())
                if (allowedOutputRSUIndicators) {
                    rsu_indicators_default.indicatorUse = indicatorUseP
                } else {
                    error "Please set a valid list of RSU indicator names in ${allowedOutputRSUIndicators}"
                    return
                }
            } else {
                rsu_indicators_default.indicatorUse = []
            }
            def snappingToleranceP = rsu_indicators.snappingTolerance
            if (snappingToleranceP && snappingToleranceP in Number) {
                rsu_indicators_default.snappingTolerance = snappingToleranceP
            }
            def surface_vegetationP = rsu_indicators.surface_vegetation
            if (surface_vegetationP && surface_vegetationP in Number) {
                rsu_indicators_default.surface_vegetation = surface_vegetationP
            }
            def surface_hydroP = rsu_indicators.surface_hydro
            if (surface_hydroP && surface_hydroP in Number) {
                rsu_indicators_default.surface_hydro = surface_hydroP
            }
            def svfSimplifiedP = rsu_indicators.svfSimplified
            if (svfSimplifiedP && svfSimplifiedP in Boolean) {
                rsu_indicators_default.svfSimplified = svfSimplifiedP
            }
            def estimateHeight = rsu_indicators.estimateHeight
            if (estimateHeight && estimateHeight in Boolean) {
                rsu_indicators_default.estimateHeight = estimateHeight
            }
            def mapOfWeightsP = rsu_indicators.mapOfWeights
            if (mapOfWeightsP && mapOfWeightsP in Map) {
                def defaultmapOfWeights = rsu_indicators_default.mapOfWeights
                if ((defaultmapOfWeights + mapOfWeightsP).size() != defaultmapOfWeights.size()) {
                    error "The number of mapOfWeights parameters must contain exactly the parameters ${defaultmapOfWeights.keySet().join(",")}"
                    return
                } else {
                    rsu_indicators_default.mapOfWeights = mapOfWeightsP
                }
            }
        } else {
            rsu_indicators = rsu_indicators_default
        }

        //Check for grid indicators
        def grid_indicators = processing_parameters.grid_indicators
        if (grid_indicators) {
            def x_size = grid_indicators.x_size
            def y_size = grid_indicators.y_size
            def list_indicators = grid_indicators.indicators
            if (x_size && y_size) {
                if (x_size <= 0 || y_size <= 0) {
                    error "Invalid grid size padding. Must be greater that 0"
                    return
                }
                if (!list_indicators) {
                    error "The list of indicator names cannot be null or empty"
                    return
                }
                def allowed_grid_indicators = ["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_POP", "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                                               "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "UTRF_AREA_FRACTION", "LCZ_FRACTION", "LCZ_PRIMARY", "FREE_EXTERNAL_FACADE_DENSITY",
                                               "BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY", "BUILDING_HEIGHT_DIST", "FRONTAL_AREA_INDEX", "SEA_LAND_FRACTION"]
                def allowedOutputIndicators = allowed_grid_indicators.intersect(list_indicators*.toUpperCase())
                if (allowedOutputIndicators) {
                    //Update the RSU indicators list according the grid indicators
                    list_indicators.each { val ->
                        if (val.trim().toUpperCase() in ["LCZ_FRACTION", "LCZ_PRIMARY"]) {
                            rsu_indicators.indicatorUse << "LCZ"
                        } else if (val.trim().toUpperCase() in ["UTRF_AREA_FRACTION"]) {
                            rsu_indicators.indicatorUse << "UTRF"
                        }
                    }
                    def grid_indicators_tmp = [
                            "x_size"    : x_size,
                            "y_size"    : y_size,
                            "output"    : "geojson",
                            "rowCol"    : false,
                            "indicators": allowedOutputIndicators
                    ]
                    def grid_output = grid_indicators.output
                    if (grid_output) {
                        if (grid_output.toLowerCase() in ["asc", "geojson"]) {
                            grid_indicators_tmp.output = grid_output.toLowerCase()
                        }
                    }
                    def grid_rowCol = grid_indicators.rowCol
                    if (grid_rowCol && grid_rowCol in Boolean) {
                        grid_indicators_tmp.rowCol = grid_rowCol
                    }

                    defaultParameters.put("grid_indicators", grid_indicators_tmp)
                } else {
                    error "Please set a valid list of indicator names in ${allowed_grid_indicators}"
                    return
                }
            }
        }

        //Check for road_traffic method
        def road_traffic = processing_parameters.road_traffic
        if (road_traffic && road_traffic in Boolean) {
            defaultParameters.put("road_traffic", road_traffic)
        }

        //Check if the pop indicators must be computed
        def pop_indics = processing_parameters.worldpop_indicators
        if (pop_indics && pop_indics in Boolean) {
            defaultParameters.put("worldpop_indicators", pop_indics)
        }

        //Check if the noise indicators must be computed
        def noise_indicators = processing_parameters.noise_indicators
        if (noise_indicators) {
            def ground_acoustic = noise_indicators.ground_acoustic
            if (ground_acoustic && ground_acoustic in Boolean) {
                defaultParameters.put("noise_indicators", ["ground_acoustic": ground_acoustic])
            }
        }
        return defaultParameters
    } else {
        return defaultParameters
    }
}


/**
 * Save the geoclimate tables into geojson files
 * @param id_zone the id of the zone
 * @param results a list of tables computed by geoclimate
 * @param ouputFolder the ouput folder
 * @param outputSRID srid code to reproject the result
 * @param reproject true if the file must reprojected
 * @param deleteOutputData delete the files if exist
 * @param outputGrid file format of the grid
 * @return
 */
def saveOutputFiles(def h2gis_datasource, def id_zone, def results, def outputFiles, def ouputFolder, def subFolderName, def outputSRID, def reproject, def deleteOutputData, def outputGrid) {
    //Create a subfolder to store each results
    def folderName = id_zone in Collection ? id_zone.join("_") : id_zone
    def subFolder = new File(ouputFolder.getAbsolutePath() + File.separator + subFolderName + folderName)
    if (!subFolder.exists()) {
        subFolder.mkdir()
    } else {
        FileUtilities.deleteFiles(subFolder)
    }
    outputFiles.each {
        if (it == "grid_indicators") {
            if (outputGrid == "geojson") {
                Geoindicators.WorkflowUtilities.saveToGeojson(results."$it", "${subFolder.getAbsolutePath() + File.separator + it}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
            } else if (outputGrid == "asc") {
                Geoindicators.WorkflowUtilities.saveToAscGrid(results."$it", subFolder.getAbsolutePath(), it, h2gis_datasource, outputSRID, reproject, deleteOutputData)
            }
        } else if (it == "building_height_missing") {
            Geoindicators.WorkflowUtilities.saveToCSV(results."$it", "${subFolder.getAbsolutePath() + File.separator + it}.csv", h2gis_datasource, deleteOutputData)
        } else {
            Geoindicators.WorkflowUtilities.saveToGeojson(results."$it", "${subFolder.getAbsolutePath() + File.separator + it}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        }
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
def saveTablesInDatabase(JdbcDataSource output_datasource, JdbcDataSource h2gis_datasource, def outputTableNames, def h2gis_tables, def id_zone, def inputSRID, def outputSRID, def reproject) {
    //Export building indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators, id_zone, h2gis_datasource, h2gis_tables.building_indicators
            , "WHERE ID_RSU IS NOT NULL", inputSRID, outputSRID, reproject)

    //Export block indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.block_indicators, id_zone, h2gis_datasource, h2gis_tables.block_indicators
            , "WHERE ID_RSU IS NOT NULL", inputSRID, outputSRID, reproject)

    //Export rsu indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_indicators, id_zone, h2gis_datasource, h2gis_tables.rsu_indicators
            , "", inputSRID, outputSRID, reproject)

    //Export rsu lcz
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_lcz, id_zone, h2gis_datasource, h2gis_tables.rsu_lcz
            , "", inputSRID, outputSRID, reproject)

    //Export rsu_utrf_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_utrf_area, id_zone, h2gis_datasource, h2gis_tables.rsu_utrf_area
            , "", inputSRID, outputSRID, reproject)

    //Export rsu_utrf_floor_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_utrf_floor_area, id_zone, h2gis_datasource, h2gis_tables.rsu_utrf_floor_area
            , "", inputSRID, outputSRID, reproject)

    //Export grid_indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.grid_indicators, id_zone, h2gis_datasource, h2gis_tables.grid_indicators
            , "", inputSRID, outputSRID, reproject)

    //Export building_utrf
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_utrf, id_zone, h2gis_datasource, h2gis_tables.building_utrf
            , "", inputSRID, outputSRID, reproject)

    //Export road_traffic
    indicatorTableBatchExportTable(output_datasource, outputTableNames.road_traffic, id_zone, h2gis_datasource, h2gis_tables.road_traffic
            , "", inputSRID, outputSRID, reproject)

    //Export zone
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.zone, id_zone, h2gis_datasource, h2gis_tables.zone
            , "", inputSRID, outputSRID, reproject)

    //Export building
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.building, id_zone, h2gis_datasource, h2gis_tables.building
            , "", inputSRID, outputSRID, reproject)

    //Export road
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.road, id_zone, h2gis_datasource, h2gis_tables.road
            , "", inputSRID, outputSRID, reproject)
    //Export rail
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.rail, id_zone, h2gis_datasource, h2gis_tables.rail
            , "", inputSRID, outputSRID, reproject)
    //Export vegetation
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.vegetation, id_zone, h2gis_datasource, h2gis_tables.vegetation
            , "", inputSRID, outputSRID, reproject)
    //Export water
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.water, id_zone, h2gis_datasource, h2gis_tables.water
            , "", inputSRID, outputSRID, reproject)
    //Export impervious
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.impervious, id_zone, h2gis_datasource, h2gis_tables.impervious
            , "", inputSRID, outputSRID, reproject)

    //Export urban areas table
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.urban_areas, id_zone, h2gis_datasource, h2gis_tables.urban_areas
            , "", inputSRID, outputSRID, reproject)

    //Export sea land mask table
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.sea_land_mask, id_zone, h2gis_datasource, h2gis_tables.sea_land_mask
            , "", inputSRID, outputSRID, reproject)

    //Export population table
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.population, id_zone, h2gis_datasource, h2gis_tables.population
            , "", inputSRID, outputSRID, reproject)

    //Export building_height_missing table
    def output_table = outputTableNames.building_height_missing
    def h2gis_table_to_save = h2gis_tables.building_height_missing

    if (output_table) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            if (output_datasource.hasTable(output_table)) {
                output_datasource.execute("DELETE FROM $output_table WHERE id_zone= '$id_zone'".toString())
            } else {
                output_datasource.execute """CREATE TABLE $output_table(ID_BUILD INTEGER, ID_SOURCE VARCHAR, ID_ZONE VARCHAR)""".toString()
            }
            IOMethods.exportToDataBase(h2gis_datasource.getConnection(), "(SELECT ID_BUILD, ID_SOURCE, '$id_zone' as ID_ZONE from $h2gis_table_to_save where estimated=true)".toString(),
                    output_datasource.getConnection(), output_table, 2, 100);
        }
    }
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
def abstractModelTableBatchExportTable(JdbcDataSource output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter, def inputSRID, def outputSRID, def reproject) {
    if (output_table) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            if (output_datasource.hasTable(output_table)) {
                output_datasource.execute("DELETE FROM $output_table WHERE id_zone= '$id_zone'".toString())
                //If the table exists we populate it with the last result
                info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                int BATCH_MAX_SIZE = 100
                ITable inputRes = prepareTableOutput(h2gis_table_to_save, filter, inputSRID, h2gis_datasource, output_table, outputSRID, output_datasource)
                if (inputRes) {
                    def outputColumns = output_datasource.getTable(output_table).getColumnsTypes()
                    def outputconnection = output_datasource.getConnection()
                    try {
                        def inputColumns = inputRes.getColumnsTypes();
                        //We check if the number of columns is not the same
                        //If there is more columns in the input table we alter the output table
                        def outPutColumnsNames = outputColumns.keySet()
                        outPutColumnsNames.remove("gid")
                        def diffCols = inputColumns.keySet().findAll { e -> !outPutColumnsNames*.toLowerCase().contains(e.toLowerCase()) }
                        def alterTable = ""
                        if (diffCols) {
                            inputColumns.each { entry ->
                                if (diffCols.contains(entry.key)) {
                                    //DECFLOAT is not supported by POSTSGRESQL
                                    def dataType = entry.value.equalsIgnoreCase("decfloat") ? "FLOAT" : entry.value
                                    alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key $dataType;"
                                    outputColumns.put(entry.key, dataType)
                                }
                            }
                            output_datasource.execute(alterTable.toString())
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
                        output_datasource.withBatch(BATCH_MAX_SIZE, insertTable.toString()) { ps ->
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
            } else {
                def tmpTable = null
                info "Start to export the table $h2gis_table_to_save into the table $output_table"
                if (filter) {
                    if (!reproject) {
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().save(output_datasource, output_table, true);
                    } else {
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                        //Because the select query reproject doesn't contain any geometry metadata
                        output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString());
                    }
                    if (tmpTable) {
                        //Workarround to update the SRID on resulset
                        output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);""".toString()
                    }
                } else {
                    if (!reproject) {
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                    } else {
                        tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                        //Because the select query reproject doesn't contain any geometry metadata
                        output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString());
                    }
                }
                if (tmpTable) {
                    output_datasource.execute """ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS gid serial;""".toString()
                    output_datasource.execute("UPDATE $output_table SET id_zone= '$id_zone'".toString());
                    output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""".toString())
                    info "The table $h2gis_table_to_save has been exported into the table $output_table"
                } else {
                    warn "The table $h2gis_table_to_save hasn't been exported into the table $output_table"
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
def indicatorTableBatchExportTable(def output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter, def inputSRID, def outputSRID, def reproject) {
    if (output_table) {
        if (h2gis_table_to_save) {
            if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
                if (output_datasource.hasTable(output_table)) {
                    output_datasource.execute("DELETE FROM $output_table WHERE id_zone='$id_zone'".toString())
                    //If the table exists we populate it with the last result
                    info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                    int BATCH_MAX_SIZE = 100;
                    ITable inputRes = prepareTableOutput(h2gis_table_to_save, filter, inputSRID, h2gis_datasource, output_table, outputSRID, output_datasource)
                    if (inputRes) {
                        def outputColumns = output_datasource.getTable(output_table).getColumnsTypes();
                        outputColumns.remove("gid")
                        def outputconnection = output_datasource.getConnection()
                        try {
                            def inputColumns = inputRes.getColumnsTypes();
                            //We check if the number of columns is not the same
                            //If there is more columns in the input table we alter the output table
                            def outPutColumnsNames = outputColumns.keySet()
                            def diffCols = inputColumns.keySet().findAll { e -> !outPutColumnsNames*.toLowerCase().contains(e.toLowerCase()) }
                            def alterTable = ""
                            if (diffCols) {
                                inputColumns.each { entry ->
                                    if (diffCols.contains(entry.key)) {
                                        //DECFLOAT is not supported by POSTSGRESQL
                                        def dataType = entry.value.equalsIgnoreCase("decfloat") ? "FLOAT" : entry.value
                                        alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key $dataType;"
                                        outputColumns.put(entry.key, dataType)
                                    }
                                }
                                output_datasource.execute(alterTable.toString())
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
                            output_datasource.withBatch(BATCH_MAX_SIZE, insertTable.toString()) { ps ->
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
                            error("Cannot save the table $output_table.\n $e");
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
                            if (tmpTable) {
                                //Workarround to update the SRID on resultset
                                output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);""".toString()
                            }

                        } else {
                            tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                            if (tmpTable) {
                                //Workarround to update the SRID on resultset
                                output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $outputSRID) USING ST_SetSRID(the_geom,$outputSRID);""".toString()
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
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString())
                        }
                    }
                    if (tmpTable) {
                        output_datasource.execute("ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS id_zone VARCHAR".toString())
                        output_datasource.execute("UPDATE $output_table SET id_zone= ?", id_zone);
                        output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""".toString())
                        //Add GID column
                        output_datasource.execute """ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS gid serial;""".toString()
                        info "The table $h2gis_table_to_save has been exported into the table $output_table"
                    } else {
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
def prepareTableOutput(def h2gis_table_to_save, def filter, def inputSRID, H2GIS h2gis_datasource, def output_table, def outputSRID, def output_datasource) {
    def targetTableSrid = output_datasource.getSpatialTable(output_table).srid
    if (filter) {
        if (outputSRID == 0) {
            if (inputSRID == targetTableSrid) {
                inputRes = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getTable()
            } else {
                if (targetTableSrid == 0 && inputSRID == 0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getTable()
                } else if (targetTableSrid != 0 && inputSRID != 0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(targetTableSrid)
                } else {
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        } else {
            if (inputSRID == targetTableSrid) {
                return h2gis_datasource.getTable(h2gis_table_to_save)
            } else {
                if (targetTableSrid == 0 && inputSRID == 0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save)
                } else if (targetTableSrid != 0 && inputSRID != 0) {
                    return h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(targetTableSrid)
                } else {
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        }
    } else {
        if (outputSRID == 0) {
            if (inputSRID == targetTableSrid) {
                return h2gis_datasource.getTable(h2gis_table_to_save)
            } else {
                if (targetTableSrid == 0 && inputSRID == 0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save)
                } else if (targetTableSrid != 0 && inputSRID != 0) {
                    return h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(targetTableSrid)
                } else {
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        } else {
            if (inputSRID == targetTableSrid) {
                return h2gis_datasource.getTable(h2gis_table_to_save)
            } else {
                if (targetTableSrid == 0 && inputSRID == 0) {
                    return h2gis_datasource.getTable(h2gis_table_to_save)
                } else if (targetTableSrid != 0 && inputSRID != 0) {
                    return h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(targetTableSrid)
                } else {
                    error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                    return
                }
            }
        }
    }
}

/**
 * This process chains a set of subprocesses to extract and transform the OSM data into
 * the geoclimate model
 *
 * @param datasource a connection to the database where the result files should be stored
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables build from the OSM file
 * @param zoneToExtract A zone to extract. Can be, a name of the place (neighborhood, city, etc. - cf https://wiki.openstreetmap.org/wiki/Key:level)
 * or a bounding box specified as a JTS envelope
 * @param distance The integer value to expand the envelope of zone
 * @return
 */
IProcess buildGeoclimateLayers() {
    return create {
        title "Extract and transform OSM data to the Geoclimate model"
        id "buildGeoclimateLayers"
        inputs datasource: JdbcDataSource,
                zoneToExtract: Object,
                distance: 500,
                hLevMin: 3
        outputs outputBuilding: String, outputRoad: String, outputRail: String,
                outputHydro: String, outputVeget: String, outputImpervious: String, outputZone: String, outputZoneEnvelope: String
        run { datasource, zoneToExtract, distance, hLevMin ->

            if (datasource == null) {
                error "Cannot access to the database to store the osm data"
                return
            }

            debug "Building OSM GIS layers"
            IProcess process = OSM.InputDataLoading.extractAndCreateGISLayers()
            if (process.execute([datasource: datasource, zoneToExtract: zoneToExtract,
                                 distance  : distance])) {

                debug "OSM GIS layers created"

                Map res = process.getResults()

                def buildingTableName = res.buildingTableName
                def roadTableName = res.roadTableName
                def railTableName = res.railTableName
                def vegetationTableName = res.vegetationTableName
                def hydroTableName = res.hydroTableName
                def imperviousTableName = res.imperviousTableName
                def zone = res.zone
                def zoneEnvelopeTableName = res.zoneEnvelopeTableName
                def epsg = datasource.getSpatialTable(zone).srid
                if (zoneEnvelopeTableName != null) {
                    debug "Formating OSM GIS layers"
                    IProcess format = OSM.InputDataFormatting.formatBuildingLayer()
                    format.execute([
                            datasource                : datasource,
                            inputTableName            : buildingTableName,
                            inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                            epsg                      : epsg])
                    buildingTableName = format.results.outputTableName

                    format = OSM.InputDataFormatting.formatRoadLayer()
                    format.execute([
                            datasource                : datasource,
                            inputTableName            : roadTableName,
                            inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                            epsg                      : epsg])
                    roadTableName = format.results.outputTableName


                    format = OSM.InputDataFormatting.formatRailsLayer()
                    format.execute([
                            datasource                : datasource,
                            inputTableName            : railTableName,
                            inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                            epsg                      : epsg])
                    railTableName = format.results.outputTableName

                    format = OSM.InputDataFormatting.formatVegetationLayer()
                    format.execute([
                            datasource                : datasource,
                            inputTableName            : vegetationTableName,
                            inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                            epsg                      : epsg])
                    vegetationTableName = format.results.outputTableName

                    format = OSM.InputDataFormatting.formatHydroLayer()
                    format.execute([
                            datasource                : datasource,
                            inputTableName            : hydroTableName,
                            inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                            epsg                      : epsg])
                    hydroTableName = format.results.outputTableName

                    format = OSM.InputDataFormatting.formatImperviousLayer()
                    format.execute([
                            datasource                : datasource,
                            inputTableName            : imperviousTableName,
                            inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                            epsg                      : epsg])
                    imperviousTableName = format.results.outputTableName

                    debug "OSM GIS layers formated"

                }

                return [outputBuilding: buildingTableName, outputRoad: roadTableName,
                        outputRail    : railTableName, outputHydro: hydroTableName,
                        outputVeget   : vegetationTableName, outputImpervious: imperviousTableName,
                        outputZone    : zone, outputZoneEnvelope: zoneEnvelopeTableName]

            }
        }
    }
}