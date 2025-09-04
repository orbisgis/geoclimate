/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.bdtopo

import org.h2.tools.DeleteDbFiles
import org.h2gis.functions.io.utility.IOMethods
import org.h2gis.utilities.FileUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.URIUtilities
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS
import org.orbisgis.data.api.dataset.ITable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.geoclimate.worldpoptools.WorldPopTools

import java.sql.Connection
import java.sql.SQLException

/**
 * Abstract class that contains all methods to run the GeoClimate workflow on formated data
 *
 */
abstract class AbstractBDTopoWorkflow extends BDTopoUtils {


    /**
     * Run the workflow
     *
     * @param input
     * @return
     */
    Map execute(def input) throws Exception {
        Map parameters = null
        if (input) {
            if (input instanceof String) {
                //Check if it's a path to a file
                def configFile = new File(input)
                if (!configFile.isFile()) {
                    throw new Exception("The configuration file doesn't exist")
                }
                if (!FileUtilities.isExtensionWellFormated(configFile, "json")) {
                    throw new Exception("The configuration file must be a json file")
                }
                parameters = Geoindicators.WorkflowUtilities.readJSON(configFile)
            } else if (input instanceof Map) {
                parameters = input
            }
        } else {
            throw new Exception("The input parameters cannot be null or empty.\n Please set a path to a configuration file or " +
                    "a map with all required parameters")
        }
        if (!parameters) {
            throw new Exception("Wrong input parameters")
        }
        //Store the zone identifier and the names of the tables
        def outputTableNamesResult = [:]
        debug "Reading file parameters"
        debug parameters.description
        def inputParameters = parameters.input
        def outputParameters = parameters.output
        //Default H2GIS database properties
        def databaseFolder = System.getProperty("java.io.tmpdir")
        def databaseName = "bdtopo_" + getVersion() + UUID.randomUUID().toString().replaceAll("-", "_")
        def databasePath = databaseFolder + File.separator + databaseName
        def h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
        def delete_h2gis = true
        def geoclimatedb = parameters.geoclimatedb
        //We collect informations to create the local H2GIS database
        if (geoclimatedb) {
            def h2gis_folder = geoclimatedb.get("folder")
            if (h2gis_folder) {
                File tmp_folder_db = new File(h2gis_folder)
                if (!tmp_folder_db.exists()) {
                    if (!tmp_folder_db.mkdir()) {
                        h2gis_folder = null
                        throw new Exception("You don't have permission to write in the folder $h2gis_folder \n" +
                                "Please check the folder.")
                    }
                } else if (!tmp_folder_db.isDirectory()) {
                    throw new Exception("Invalid output folder $h2gis_folder.")
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
            def delete_h2gis_db = geoclimatedb.delete
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
            throw new Exception("Cannot find any input parameters.")
        }
        def inputDataBase = inputParameters.database
        def inputFolder = inputParameters.folder
        def locations = inputParameters.locations as Set
        if (!locations) {
            throw new Exception("Cannot find any locations parameter.")
        }
        def inputSRID = inputParameters.srid
        if (inputSRID && inputSRID <= 0) {
            throw new Exception("The input srid must be greater than 0.")
        }
        if (inputFolder && inputDataBase) {
            throw new Exception("Please set only one input data provider")
        }

        def inputWorkflowTableNames = getInputTables()

        if (!inputWorkflowTableNames) {
            throw new Exception("The input table names cannot be null or empty.")
        }

        def outputWorkflowTableNames = getOutputTables()

        //Get processing parameters
        def processing_parameters = extractProcessingParameters(parameters.parameters)
        if (!processing_parameters) {
            throw new Exception("Invalid processing parameters")
        }

        //Get the out put parameters
        def outputDatasource
        def outputTables
        Map excluded_output_db_columns=[:]
        def file_outputFolder
        def outputFileTables
        def outputSRID
        def deleteOutputData
        String domain ="zone"
        if (outputParameters) {
            def outputDataBase = outputParameters.database
            def outputFolder = outputParameters.folder
            deleteOutputData = outputParameters.get("delete")
            if (!deleteOutputData) {
                deleteOutputData = true
            } else if (!deleteOutputData in Boolean) {
                throw new Exception("The delete parameter must be a boolean value")
            }
            outputSRID = outputParameters.get("srid")
            if (outputSRID && outputSRID <= 0) {
                throw new Exception("The output srid must be greater than 0")
            }
            def domainTmp = outputParameters.get("domain")
            if (domainTmp!=null && domainTmp instanceof String && domain.toLowerCase() in["zone", "zone_extended"]) {
                domain=domainTmp.toLowerCase()
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
                        throw new Exception("You don't have permission to write in the folder $outputFolder \n" +
                                "Please check the folder.")
                    }
                } else if (!file_outputFolder.isDirectory()) {
                    throw new Exception("Invalid output folder $file_outputFolder.")
                }
            }
            if (outputDataBase) {
                //Check the conditions to store the results a database
                def outputDataBaseData = Geoindicators.WorkflowUtilities.buildOutputDBParameters(outputDataBase, outputDataBase.tables, outputWorkflowTableNames)
                outputDatasource = outputDataBaseData.datasource
                outputTables = outputDataBaseData.tables
                outputDataBase.excluded_columns.collect{excluded_output_db_columns.put(it.key.toLowerCase(), it.value*.toUpperCase())}
            }
        }

        /**
         * Run the workflow when the input data comes from a folder
         */
        if (inputFolder) {
            def h2gis_datasource = H2GIS.open(h2gis_properties)
            def datafromFolder = linkDataFromFolder(inputFolder, inputWorkflowTableNames, h2gis_datasource, inputSRID)
            inputSRID = datafromFolder.inputSrid
            def sourceSrid = datafromFolder.sourceSrid
            LinkedHashMap tablesLinked = datafromFolder.tableNames
            if (tablesLinked) {
                locations.each { location ->
                    //We must extract the data from the shapefiles for each locations
                    try {
                        if (filterLinkedShapeFiles(location, processing_parameters.distance, tablesLinked, sourceSrid, inputSRID, h2gis_datasource)) {
                            def formatedZone = checkAndFormatLocations(location)
                            if (formatedZone) {
                                def bdtopo_results = bdtopo_processing(formatedZone, h2gis_datasource, processing_parameters,
                                        createMainFolder(file_outputFolder, formatedZone), outputFileTables, outputDatasource,
                                        outputTables, outputSRID, inputSRID, true, excluded_output_db_columns, domain)
                                if (bdtopo_results) {
                                    outputTableNamesResult.putAll(bdtopo_results)
                                }
                            }
                        }
                    } catch (Exception e) {
                        String new_location =location in Collection ? location.join("_") : location
                        saveLogZoneTable(h2gis_datasource, databaseFolder, new_location, e.getLocalizedMessage())
                        //eat the exception and process other zone
                        warn("The zone $location has not been processed. \nCause :  \n${e.getLocalizedMessage()}")
                    }
                }
                if(delete_h2gis){
                    h2gis_datasource.deleteClose()
                }
                return outputTableNamesResult
            } else {
                throw new Exception("Cannot find any data to process from the folder $inputFolder".toString())
            }

        }

        /**
         * Run the workflow when the input data comes from a database
         */
        if (inputDataBase) {
            def inputTables = inputDataBase.tables
            if (!inputTables) {
                inputTables = inputWorkflowTableNames.collect { name -> [name: name] }
            } else {
                def inputTables_tmp = [:]
                inputTables.each { table ->
                    if (inputWorkflowTableNames.contains(table.key.toLowerCase())) {
                        inputTables_tmp.put(table.key.toLowerCase(), table.value)
                    }
                }
                if (inputTables_tmp.size() == 0) {
                    throw new Exception("Please set a valid list of input tables as  : \n" +
                            "${inputWorkflowTableNames.collect { name -> [name: name] }}".toString())
                }
                inputTables = inputTables_tmp
            }
            def h2gis_datasource = H2GIS.open(h2gis_properties)
            def nbzones = 0
            for (location in locations) {
                nbzones++
                try {
                    inputSRID = loadDataFromPostGIS(inputDataBase.subMap(["user", "password", "url", "databaseName"]), location, processing_parameters.distance, inputTables, inputSRID, h2gis_datasource)

                    def formatedZone = checkAndFormatLocations(location)
                    if (formatedZone) {
                        def bdtopo_results = bdtopo_processing(formatedZone, h2gis_datasource, processing_parameters, createMainFolder(file_outputFolder, formatedZone), outputFileTables, outputDatasource, outputTables,
                                outputSRID, inputSRID, true, excluded_output_db_columns,domain)
                        if (bdtopo_results) {
                            outputTableNamesResult.putAll(bdtopo_results)
                        }
                    }
                } catch (Exception e) {
                    saveLogZoneTable(h2gis_datasource, databaseFolder, location in Collection ? location.join("_") : location, e.getLocalizedMessage())
                    //eat the exception and process other zone
                    warn("The zone $location has not been processed. Please check the log table to get more informations.")
                }
                info "${nbzones} location(s) on ${locations.size()}"
            }
            if(delete_h2gis){
                h2gis_datasource.deleteClose()
            }
            if (outputTableNamesResult) {
                return outputTableNamesResult
            }
        }
    }


    /**
     * Method to log message in a table
     * @param dataSource
     * @param logTableZones
     * @param location
     * @param message
     * @throws Exception
     */
    void saveLogZoneTable(JdbcDataSource dataSource, String databaseFolder, String location, String message) throws Exception {
        dataSource.save("""(SELECT st_union(st_accum(THE_GEOM)) as the_geom, 
            '${location.replace("'","''")}' as location, '${message.replace("'","''")}' as info, '${Geoindicators.version()}' as version, 
            '${Geoindicators.buildNumber()}' as build_number FROM commune)""", databaseFolder + File.separator + "log_zones_" + location + ".fgb", true)
    }

    /**
     *
     * Method to filter the table loaded from shapefiles
     * @param location
     * @param distance
     * @param inputTables
     * @param sourceSRID
     * @param inputSRID
     * @param h2gis_datasource
     * @return
     */
    abstract def filterLinkedShapeFiles(def location, float distance, LinkedHashMap inputTables,
                                        int sourceSRID, int inputSRID, H2GIS h2gis_datasource) throws Exception

    /**
     * Load the required tables stored in a database
     *
     * @param inputDatasource database where the tables are
     * @return true is succeed, false otherwise
     */
    abstract Integer loadDataFromPostGIS(def input_database_properties, def code, def distance,
                                         def inputTables, def inputSRID, H2GIS h2gis_datasource) throws Exception;


    /**
     * Load  shapefiles into the local H2GIS database
     *
     * @param locations the locations to process
     * @param inputFolder where the files are
     * @param h2gis_datasource the local database for the geoclimate processes
     * @param inputSRID a list of SRID set the geometries
     * @return a list of id_zones
     */
    def linkDataFromFolder(def inputFolder, def inputWorkflowTableNames,
                           H2GIS h2gis_datasource, def inputSRID) throws Exception {
        def folder = new File(inputFolder)
        if (!folder.exists()) {
            throw new Exception("The input folder doesn't exist")
        }
        if (folder.isDirectory()) {
            def geoFiles = []
            folder.eachFileRecurse groovy.io.FileType.FILES, { file ->
                if (file.name.toLowerCase().endsWith(".shp")) {
                    geoFiles << file.getAbsolutePath()
                }
            }
            //Looking for commune shape file
            def commune_file = geoFiles.find { it.toLowerCase().endsWith("commune.shp") ? it : null }
            if (commune_file) {
                //Load commune and check if there is some id_zones inside
                try {
                    h2gis_datasource.link(commune_file, "COMMUNE_TMP", true)
                } catch (Exception e) {
                    throw new Exception("Cannot read the commune.shp file", e)
                }
                geoFiles.remove(commune_file)
                int srid = h2gis_datasource.getSrid("COMMUNE_TMP")
                def sourceSrid = srid
                if (srid == 0 && inputSRID) {
                    srid = inputSRID
                } else if (srid == 0) {
                    warn "Cannot find a SRID value for the layer commune.shp.\n" +
                            "Please set a valid OGC prj or use the parameter srid to force it."
                    return null
                }
                //Link the files
                def numberFiles = geoFiles.size()
                def tableNames = [:]
                tableNames.put("commune", "COMMUNE_TMP")

                geoFiles.eachWithIndex { geoFile, index ->
                    debug "linking file $geoFile $index on $numberFiles"
                    //We must link only the allowed tables
                    def fileName = URIUtilities.fileFromString(geoFile).getName()
                    def name = fileName.substring(0, fileName.lastIndexOf(".")).toLowerCase()
                    if (inputWorkflowTableNames.contains(name)) {
                        try {
                            h2gis_datasource.link(geoFile, "${name}_tmp", true)
                            //h2gis_datasource.execute("CREATE SPATIAL INDEX ON ${name}_tmp(THE_GEOM)".toString())
                            tableNames.put(name, "${name}_tmp")
                        } catch (Exception e) {
                            throw new Exception("Cannot read the shp file " + geoFile, e)
                        }
                    }
                }
                h2gis_datasource.execute("DROP TABLE IF EXISTS COMMUNE_TMP_LINK;".toString())
                return ["sourceSrid": sourceSrid, "inputSrid": srid, "tableNames": tableNames]

            } else {
                throw new Exception("The input folder must contains a file named commune")
            }
        } else {
            throw new Exception("The input folder must be a directory")
        }
    }

/**
 * This method is used to create a main folder for each location.
 * This folder is used to store all results and can contain subfolders.
 * We delete it if it already exists
 * @param outputFolder
 * @param location
 * @return
 */
    def createMainFolder(def outputFolder, def location) {
        if (!outputFolder) {
            return
        }
        //Create the folder to save the results
        def folder = new File(outputFolder.getAbsolutePath() + File.separator + "bdtopo_" + getVersion() + "_" + location)
        if (!folder.exists()) {
            folder.mkdir()
        } else {
            FileUtilities.deleteFiles(folder)
        }
        return folder.getAbsolutePath()
    }


    /**
     * Read the file parameters and create a new map of parameters
     * The map of parameters is initialized with default values
     *
     * @param processing_parameters the file parameters
     * @return a filled map of parameters
     */
    def extractProcessingParameters(def processing_parameters) throws Exception {
        def defaultParameters = [distance: 500f, prefixName: "",
                                 hLevMin : 3]
        def rsu_indicators_default = [indicatorUse       : [],
                                      svfSimplified      : true,
                                      surface_vegetation : 10000f,
                                      surface_hydro      : 2500f,
                                      surface_urban_areas: 10000f,
                                      snappingTolerance  : 0.01f,
                                      mapOfWeights       : ["sky_view_factor"             : 4,
                                                            "aspect_ratio"                : 3,
                                                            "building_surface_fraction"   : 8,
                                                            "impervious_surface_fraction" : 0,
                                                            "pervious_surface_fraction"   : 0,
                                                            "height_of_roughness_elements": 6,
                                                            "terrain_roughness_length"    : 0.5],
                                      utrfModelName      : "UTRF_BDTOPO_V2_RF_2_2.model"]

        if (processing_parameters) {
            def distanceP = processing_parameters.distance
            if (distanceP!=null && distanceP in Number) {
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
            def target_grid_indicators =false
            if (rsu_indicators) {
                def indicatorUseP = rsu_indicators.indicatorUse
                if (indicatorUseP && indicatorUseP in List) {
                    def allowed_rsu_indicators = ["LCZ", "UTRF", "TEB", "TARGET"]
                    def allowedOutputRSUIndicators = allowed_rsu_indicators.intersect(indicatorUseP*.toUpperCase())
                    if (allowedOutputRSUIndicators) {
                        rsu_indicators_default.indicatorUse = indicatorUseP
                        //We must update the grid indicators for TARGET schema
                        if(indicatorUseP*.toUpperCase().contains("TARGET")){
                            target_grid_indicators = true
                        }
                    } else {
                        throw new Exception("Please set a valid list of RSU indicator names in ${allowedOutputRSUIndicators}".toString())
                    }
                } else {
                    throw new Exception("The list of RSU indicator names cannot be null or empty")
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
                def surface_UrbanAreasP = rsu_indicators.surface_urban_areas
                if (surface_UrbanAreasP && surface_UrbanAreasP in Number) {
                    rsu_indicators_default.surface_urban_areas = surface_UrbanAreasP
                }

                def svfSimplifiedP = rsu_indicators.svfSimplified
                if (svfSimplifiedP && svfSimplifiedP in Boolean) {
                    rsu_indicators_default.svfSimplified = svfSimplifiedP
                }

                def mapOfWeightsP = rsu_indicators.mapOfWeights
                if (mapOfWeightsP && mapOfWeightsP in Map) {
                    def defaultmapOfWeights = rsu_indicators_default.mapOfWeights
                    if ((defaultmapOfWeights + mapOfWeightsP).size() != defaultmapOfWeights.size()) {
                        throw new Exception("The number of mapOfWeights parameters must contain exactly the parameters ${defaultmapOfWeights.keySet().join(",")}".toString())
                    } else {
                        rsu_indicators_default.mapOfWeights = mapOfWeightsP
                    }
                }
                defaultParameters.put("rsu_indicators", rsu_indicators_default)

            }

            //Check for grid indicators
            def grid_indicators = processing_parameters.grid_indicators
            if(target_grid_indicators && !grid_indicators){
                def grid_indicators_tmp = [
                        "x_size"    : 100,
                        "y_size"    : 100,
                        "output"    : "fgb",
                        "rowCol"    : null, //Default to null
                        "indicators": ["BUILDING_FRACTION",
                                       "BUILDING_HEIGHT_WEIGHTED",
                                       "WATER_FRACTION",
                                       "ROAD_FRACTION",
                                       "IMPERVIOUS_FRACTION",
                                       "STREET_WIDTH" ,
                                       "IMPERVIOUS_FRACTION",
                                       "VEGETATION_FRACTION"]
                ]
                defaultParameters.put("grid_indicators", grid_indicators_tmp)
            }
            else if (grid_indicators) {
                def x_size = grid_indicators.x_size
                def y_size = grid_indicators.y_size
                def list_indicators = grid_indicators.indicators
                if (x_size && y_size) {
                    if (x_size <= 0 || y_size <= 0) {
                        error "Invalid grid size padding. Must be greater that 0"
                        return
                    }
                    if (!list_indicators) {
                        throw new Exception("The list of indicator names cannot be null or empty")
                    }
                    def allowed_grid_indicators = ["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_POP", "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                                                   "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "UTRF_AREA_FRACTION", "UTRF_FLOOR_AREA_FRACTION", "LCZ_FRACTION", "LCZ_PRIMARY", "FREE_EXTERNAL_FACADE_DENSITY",
                                                   "BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY",
                                                   "BUILDING_HEIGHT_DISTRIBUTION", "FRONTAL_AREA_INDEX", "SEA_LAND_FRACTION", "ASPECT_RATIO",
                                                   "SVF", "HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS_CLASS", "URBAN_SPRAWL_AREAS",
                                                   "URBAN_SPRAWL_DISTANCES", "URBAN_SPRAWL_COOL_DISTANCES", "STREET_WIDTH"]
                    def allowedOutputIndicators = list_indicators.findAll{
                        it.startsWith("COUNT_WARM_") || allowed_grid_indicators.contains(it)
                    }
                    if (allowedOutputIndicators) {
                        //Update the RSU indicators list according the grid indicators
                        list_indicators.each { val ->
                            if (val.trim().toUpperCase() in ["LCZ_FRACTION", "LCZ_PRIMARY"]) {
                                rsu_indicators.indicatorUse << "LCZ"
                            } else if (val.trim().toUpperCase() in ["UTRF_AREA_FRACTION", "UTRF_FLOOR_AREA_FRACTION"]) {
                                rsu_indicators.indicatorUse << "UTRF"
                            }
                        }
                        //Update the GRID indicators list if TARGET output is specified
                        if(target_grid_indicators){
                            allowedOutputIndicators.addAll(["BUILDING_FRACTION",
                                                            "BUILDING_HEIGHT",
                                                            "WATER_FRACTION",
                                                            "ROAD_FRACTION",
                                                            "IMPERVIOUS_FRACTION",
                                                            "STREET_WIDTH" ,
                                                            "IMPERVIOUS_FRACTION",
                                                            "VEGETATION_FRACTION"])
                        }
                        if(x_size != y_size){
                            throw new Exception("TARGET model supports only regular grid. Please set the same x and y resolutions")
                        }
                        def grid_indicators_tmp = [
                                "x_size"    : x_size,
                                "y_size"    : y_size,
                                "output"    : "fgb",
                                "rowCol"    : null, //Default to null
                                "indicators": allowedOutputIndicators
                        ]
                        def grid_output = grid_indicators.output
                        if (grid_output) {
                            if (grid_output.toLowerCase() in ["asc", "fgb"]) {
                                grid_indicators_tmp.output = grid_output.toLowerCase()
                            }
                        }
                        def domainP = grid_indicators.domain
                        if (domainP!=null && domainP in String && domainP.toLowerCase() in ["zone", "zone_extended"]) {
                            grid_indicators_tmp.domain = domainP.toLowerCase() //Grid is computed to the zone
                        }

                        def grid_rowCol = grid_indicators.rowCol
                        if (grid_rowCol && grid_rowCol in Boolean) {
                            grid_indicators_tmp.rowCol = grid_rowCol
                        }
                        def lcz_lod = grid_indicators.lcz_lod
                        if (lcz_lod && lcz_lod in Integer) {
                            if (lcz_lod < 0 && lcz_lod > 10) {
                                error "The number of level of details to aggregate the LCZ must be between 0 and 10"
                                return
                            }
                            grid_indicators_tmp.put("lcz_lod", lcz_lod)
                        }
                        defaultParameters.put("grid_indicators", grid_indicators_tmp)
                    } else {
                        throw new Exception("Please set a valid list of indicator names in ${allowed_grid_indicators}".toString())
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
        }
        return defaultParameters
    }


    abstract List getInputTables()

    List getOutputTables() {
        return ["building_indicators",
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
                "road_traffic",
                "population",
                "ground_acoustic",
                "urban_sprawl_areas",
                "urban_cool_areas",
                "grid_target",
                "zone_extended",
                "building_updated"]
    }


    /**
     * Sanity check for the location value
     * @param id_zones
     * @return
     */
    def checkAndFormatLocations(def locations) throws Exception {
        if (locations in Collection) {
            return locations.join("_")
        } else if (locations instanceof String) {
            return locations.trim()
        } else {
            throw new Exception("Invalid location input. \n" +
                    "The location input must be a string value or an array of 4 coordinates to define a bbox ")
        }
    }

    /**
     * Utility method to run commmune by commune the geoclimate chain and save the result in a folder or/and
     * in a database
     *
     * @param h2gis_datasource the local H2GIS database
     * @param processing_parameters the geoclimate chain parameters
     * @param outputFolder folder to store the files, null otherwise
     * @param outputFiles the name of the tables that will be saved
     * @param output_datasource a connexion to a database to save the results
     * @param outputTableNames the name of the tables in the output_datasource to save the results
     * @param outputSRID force the srid for the output data
     * @param inputSRID force the srid for the input data
     * @param deleteOutputData true to delete the output files if exist
     * @return
     */
    def bdtopo_processing(def location, H2GIS h2gis_datasource, def processing_parameters, def outputFolder, def outputFiles,
                          def output_datasource, def outputTableNames, def outputSRID,
                          def inputSRID, def deleteOutputData = true, Map excluded_columns, String domain)
            throws Exception {
        //Add clip info
        processing_parameters.put("domain", domain)
        //Add the GIS layers to the list of results
        def outputTableNamesResult = [:]
        def grid_indicators_params = processing_parameters.grid_indicators
        def outputGrid = "fgb"
        if (grid_indicators_params) {
            outputGrid = grid_indicators_params.output
        }

        def tmp_results = [:]
        def rows = h2gis_datasource.rows("SELECT ST_CollectionExtract(THE_GEOM, 3) as the_geom,code_insee  FROM COMMUNE")
        //We process each zones because the input zone can overlap several communes
        rows.each { row ->
            Geometry geom = row.the_geom
            def code_insee = row.code_insee
            info "Processing the commune with the code insee :  ${code_insee}"
            def numGeom = geom.getNumGeometries()
            if (numGeom == 1) {
                if (!geom.isEmpty()) {
                    def subCommuneTableName = postfix("COMMUNE")
                    //Let's create the subcommune table and run the BDTopo process from it
                    h2gis_datasource.execute("""
                DROP TABLE IF EXISTS $subCommuneTableName;
                CREATE TABLE $subCommuneTableName(the_geom GEOMETRY(POLYGON,$inputSRID), CODE_INSEE VARCHAR)  AS 
                SELECT ST_GEOMFROMTEXT('${geom.getGeometryN(0)}', $inputSRID) as the_geom , '${code_insee}' AS CODE_INSEE
                """)
                    def results = bdTopoProcessingSingleArea(h2gis_datasource, code_insee, subCommuneTableName, inputSRID, processing_parameters)
                    if (results) {
                        tmp_results.put(code_insee, results)
                    }
                }
            } else {
                info "The location $code_insee is represented by $numGeom polygons\n. " +
                        "GeoClimate will process each polygon individually."
                int subAreaCount = 1
                for (i in 0..<numGeom) {
                    info "Processing the polygon ${subAreaCount} on $numGeom "
                    def subGeom = geom.getGeometryN(i)
                    if (!subGeom.isEmpty()) {
                        def subCommuneTableName = postfix("COMMUNE")
                        //This code is used to encode the codeinsee plus an indice for each sub geometries
                        def code_insee_plus_indice = "${code_insee}_${subAreaCount}"
                        //Let's create the subcommune table and run the BDTopo process from it
                        h2gis_datasource.execute("""
                DROP TABLE IF EXISTS $subCommuneTableName;
                CREATE TABLE $subCommuneTableName(the_geom GEOMETRY(POLYGON,$inputSRID), CODE_INSEE VARCHAR)  AS 
                SELECT ST_GEOMFROMTEXT('${subGeom}', $inputSRID) as the_geom , '${code_insee_plus_indice}' AS CODE_INSEE
                """)
                        def results = bdTopoProcessingSingleArea(h2gis_datasource, code_insee, subCommuneTableName, inputSRID, processing_parameters)
                        if (results) {
                            tmp_results.put(code_insee_plus_indice, results)
                        }
                    }
                    subAreaCount++
                }
            }
        }

        if (tmp_results) {
            if (tmp_results.size() == 1) {
                def res = [:]
                tmp_results.each { code ->
                    code.value.each { it ->
                        if (it.value) {
                            res.put(it.key, it.value)
                        }
                    }
                }
                computeGridIndicators(h2gis_datasource, location, inputSRID, processing_parameters, res)
                outputTableNamesResult.put(location, res)
                saveResults(h2gis_datasource, location, res, inputSRID, outputFolder, outputFiles, output_datasource, outputTableNames, outputSRID, deleteOutputData, outputGrid, excluded_columns, domain)

            } else {
                def tablesToMerge = ["zone"               : [],
                                     "road"               : [], "rail": [], "water": [],
                                     "vegetation"         : [], "impervious": [], "building": [],
                                     "building_indicators": [], "block_indicators": [],
                                     "rsu_indicators"     : [], "rsu_lcz": [],
                                     "rsu_utrf_area"      : [], "rsu_utrf_floor_area": [],
                                     "building_utrf"      : [], "population": [], "road_traffic": [],
                                     "grid_indicators"    : [], "urban_areas": [], "ground_acoustic": []

                ]
                tmp_results.each { code ->
                    code.value.each { it ->
                        if (it.value) {
                            tablesToMerge[it.key] << it.value
                        }
                    }
                }
                //We must merge here all sub areas and then compute the grid indicators
                //so the user have a continuous spatial domain instead of multiples tables
                def results = mergeResultTables(tablesToMerge, h2gis_datasource)
                computeGridIndicators(h2gis_datasource, location, inputSRID, processing_parameters, results)
                outputTableNamesResult.put(location, results)
                saveResults(h2gis_datasource, location, results, inputSRID, outputFolder, outputFiles, output_datasource, outputTableNames, outputSRID, deleteOutputData, outputGrid,excluded_columns, domain)

            }
        }

        return outputTableNamesResult

    }

/**
 * Method to merge a list of tables
 * e.g : all building tables computed by zone are merged in one building table
 * @param tablesToMerge
 * @param h2gis_datasource
 * @return
 */
    def mergeResultTables(def tablesToMerge, H2GIS h2gis_datasource) {
        def results = [:]
        def con = h2gis_datasource.getConnection()
        tablesToMerge.each { it ->
            def tableNames = it.value
            def queryToJoin = []
            String tmp_table
            HashSet allColumns = new HashSet()
            def tableAndColumns = [:]
            tableNames.each { tableName ->
                if (tableName) {
                    def tableColumns = JDBCUtilities.getColumnNames(con, tableName)
                    allColumns.addAll(tableColumns)
                    tableAndColumns.put(tableName, tableColumns)
                }
            }
            tableNames.each { tableName ->
                def columns = tableAndColumns.get(tableName)
                def joinColumns = []
                allColumns.each { col ->
                    if (!columns.contains(col)) {
                        joinColumns << "null as ${col}"
                    } else {
                        joinColumns << col
                    }
                }
                queryToJoin << "SELECT ${joinColumns.join(",")} FROM $tableName"
                tmp_table = tableName
            }
            if (tmp_table) {
                def finalTableName = postfix(tmp_table.substring(0, tmp_table.lastIndexOf("_")))
                h2gis_datasource.execute("""
                        DROP TABLE IF EXISTS $finalTableName;
                        CREATE TABLE $finalTableName as ${queryToJoin.join(" union all ")};
                        DROP TABLE IF EXISTS  ${tableNames.join(",")} """.toString())
                results.put(it.key, finalTableName)
            }
        }
        return results
    }

/**
 * Compute the grid indicators outside the workflow to be sure that the grid domain is continuous
 * e.g if the input are is defined by a multipolygon (a location with islands)
 * @param h2gis_datasource
 * @param id_zone
 * @param processing_parameters
 * @param results
 * @return
 */
    def computeGridIndicators(H2GIS h2gis_datasource, def id_zone, def srid, def processing_parameters, def results) {
        def grid_indicators_params = processing_parameters.grid_indicators
        //Compute the grid indicators
        if (grid_indicators_params) {
            info("Start computing grid_indicators")
            int x_size
            int y_size
            def rowCol = grid_indicators_params.rowCol
            def grid_zone
            if(grid_indicators_params.domain=="zone_extended") { //Must the forced due to the zone parameter
                grid_zone = h2gis_datasource.getExtent(results.zone_extended)
            }else if(grid_indicators_params.domain==null){
                if(processing_parameters.domain=="zone"){
                    grid_zone = h2gis_datasource.getExtent(results.zone)
                }else if(processing_parameters.domain=="zone_extended"){
                    grid_zone = h2gis_datasource.getExtent(results.zone_extended)
                }
            }
            else  {
                grid_zone = h2gis_datasource.getExtent(results.zone)
            }
            if(rowCol==null){
                //Let's compute the number of row and col
                rowCol=true
                Envelope envGeom  = grid_zone.getEnvelopeInternal()
                x_size=(int) Math.max(Math.round(envGeom.getWidth()/grid_indicators_params.x_size),1)
                y_size=(int) Math.max(Math.round(envGeom.getHeight()/grid_indicators_params.y_size),1)
            }else{
                x_size = grid_indicators_params.x_size
                y_size = grid_indicators_params.y_size
            }
            String gridTableName = Geoindicators.WorkflowGeoIndicators.createGrid(h2gis_datasource, grid_zone,
                    x_size, y_size, srid, rowCol)
            if (gridTableName) {
                String rasterizedIndicators = Geoindicators.WorkflowGeoIndicators.rasterizeIndicators(h2gis_datasource, gridTableName,
                        grid_indicators_params.indicators,
                        results.building, results.road, results.vegetation,
                        results.water, results.impervious,
                        results.rsu_lcz, results.rsu_utrf_area, "", "",
                        processing_parameters.prefixName)
                if (rasterizedIndicators) {
                    h2gis_datasource.dropTable(gridTableName)
                    results.put("grid_indicators", rasterizedIndicators)
                    Map sprawl_indic = Geoindicators.WorkflowGeoIndicators.sprawlIndicators(h2gis_datasource, rasterizedIndicators, "id_grid", grid_indicators_params.indicators,
                            Math.max(x_size, y_size).floatValue())
                    if (sprawl_indic && sprawl_indic.urban_sprawl_areas) {
                        results.put("urban_sprawl_areas", sprawl_indic.urban_sprawl_areas)
                        if (sprawl_indic.urban_cool_areas) {
                            results.put("urban_cool_areas", sprawl_indic.urban_cool_areas)
                        }
                        results.put("grid_indicators", sprawl_indic.grid_indicators)
                    }
                    //We must transform the grid_indicators to produce the target land input
                    if(processing_parameters.rsu_indicators.indicatorUse.contains("TARGET")){
                        results.put("grid_target", Geoindicators.GridIndicators.formatGrid4Target(h2gis_datasource, rasterizedIndicators, x_size))
                    }
                    info("End computing grid_indicators")
                }
            } else {
                info "Cannot create a grid to aggregate the indicators"
            }
        }
    }


/**
 * Method to store the results in a folder or a database
 * @param h2gis_datasource
 * @param id_zone
 * @param srid
 * @param outputFolder
 * @param outputFiles
 * @param output_datasource
 * @param outputTableNames
 * @param outputSRID
 * @param deleteOutputData
 * @param data_domain domain to save the data
 * @return
 */
    def saveResults(H2GIS h2gis_datasource, def id_zone, def results, def srid, def outputFolder, def outputFiles,
                    def output_datasource, def outputTableNames, def outputSRID, def deleteOutputData, def outputGrid,
                    Map excluded_columns, String data_domain) throws Exception {
        //Check if the user decides to reproject the output data
        def reproject = false
        if (outputSRID) {
            if (outputSRID != srid) {
                reproject = true
            }
        } else {
            outputSRID = srid
        }
        Geometry outputZoneGeometry=null
        if(data_domain=="zone") {
            outputZoneGeometry = h2gis_datasource.firstRow("select st_union(st_accum(THE_GEOM)) as the_geom from $results.zone".toString()).the_geom
        } else if(data_domain=="zone_extended") {
            outputZoneGeometry = h2gis_datasource.getExtent(results.zone_extended)
        }

        if (outputFolder && outputFiles) {
            saveOutputFiles(h2gis_datasource, results, outputFiles, outputFolder, outputSRID, reproject, deleteOutputData, outputGrid, outputZoneGeometry)
        }
        if (output_datasource) {
            saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject,excluded_columns)
        }
    }

/**
 * This process is used to compute the indicators on a single geometry
 * @param h2gis_datasource
 * @param id_zone
 * @param subCommuneTableName
 * @param srid
 * @param processing_parameters
 * @return
 */
    def bdTopoProcessingSingleArea(def h2gis_datasource, def id_zone, def subCommuneTableName, def srid, def processing_parameters) {
        def results = [:]
        //Let's run the BDTopo process for the id_zone
        //Load and format the BDTopo data
        Map layers = getInputTables().collectEntries() { [it, it == "commune" ? subCommuneTableName : it] }
        Map dataFormated = formatLayers(h2gis_datasource, layers, processing_parameters.distance, processing_parameters.hLevMin)
        if (dataFormated) {
            def tablesToDrop = []
            def building = dataFormated.building
            def road = dataFormated.road
            def rail = dataFormated.rail
            def water = dataFormated.water
            def vegetation = dataFormated.vegetation
            def zone = dataFormated.zone
            def impervious = dataFormated.impervious
            def urban_areas = dataFormated.urban_areas
            def zone_extended = dataFormated.zone_extended
            def building_updated = dataFormated.building_updated

            info "BDTOPO GIS layers formated"

            def rsu_indicators_params = processing_parameters.rsu_indicators
            def worldpop_indicators = processing_parameters.worldpop_indicators

            results.put("zone", zone)
            results.put("road", road)
            results.put("rail", rail)
            results.put("water", water)
            results.put("vegetation", vegetation)
            results.put("impervious", impervious)
            results.put("urban_areas", urban_areas)
            results.put("building", building)
            results.put("zone_extended", zone_extended)
            results.put("building_updated", building_updated)

            //Compute the RSU indicators
            if (rsu_indicators_params && rsu_indicators_params.indicatorUse) {
                rsu_indicators_params.clip=processing_parameters.domain=="zone_extended"?false:true
                //Build the indicators
                rsu_indicators_params.put("utrfModelName", "UTRF_BDTOPO_V2_RF_2_2.model")
                Map geoIndicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(h2gis_datasource, zone,
                        zone_extended,
                        building, road,
                        rail, vegetation,
                        water, impervious, "", "", urban_areas, "",
                        rsu_indicators_params, processing_parameters.prefixName)
                if (!geoIndicators) {
                    error "Cannot build the geoindicators for the zone $id_zone"
                } else {
                    results.putAll(geoIndicators)
                }
            }
            //Extract and compute population indicators for the specified year
            //This data can be used by the grid_indicators process
            if (worldpop_indicators) {
                String coverageId = "wpGlobal:ppp_2018"
                //Test if the coverage is available
                if(WorldPopTools.Extract.isCoverageAvailable(coverageId)) {
                    def envelope = h2gis_datasource.firstRow("select st_transform(the_geom, 4326) as geom from $zone".toString()).geom.getEnvelopeInternal()
                    def bbox = [envelope.getMinY() as Float, envelope.getMinX() as Float,
                                envelope.getMaxY() as Float, envelope.getMaxX() as Float]

                    String worldPopFile = WorldPopTools.Extract.extractWorldPopLayer(coverageId, bbox)
                    if (worldPopFile) {
                        String worldPopTableName = WorldPopTools.Extract.importAscGrid(h2gis_datasource, worldPopFile, srid, coverageId.replaceAll(":", "_"))
                        if (worldPopTableName) {
                            results.put("population", worldPopTableName)
                            building = Geoindicators.BuildingIndicators.buildingPopulation(h2gis_datasource, results.building,
                                    worldPopTableName, ["pop"])
                            if (!building) {
                                info "Cannot compute any population data at building level"
                            }
                            tablesToDrop << results.building
                            //Update the building table with the population data
                            results.put("building", building)

                        } else {
                            info "Cannot import the worldpop asc file $worldPopFile"
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
                } else {
                    info "Cannot find the population grid $coverageId \n Create a default empty worldpop table"
                    def outputTableWorldPopName = postfix "world_pop"
                    h2gis_datasource.execute("""drop table if exists $outputTableWorldPopName;
                        create table $outputTableWorldPopName (the_geom GEOMETRY(POLYGON, $srid), ID_POP INTEGER, POP FLOAT);""".toString())
                    results.put("population", outputTableWorldPopName)
                }
            }

            //Compute traffic flow
            if (processing_parameters.road_traffic) {
                String road_traffic = Geoindicators.RoadIndicators.build_road_traffic(h2gis_datasource, road)
                results.put("road_traffic", road_traffic)
            }

            def noise_indicators = processing_parameters.noise_indicators

            def geomEnv
            if (noise_indicators) {
                if (noise_indicators.ground_acoustic) {
                    geomEnv = h2gis_datasource.getExtent(zone)
                    def gridP = Geoindicators.SpatialUnits.createGrid(h2gis_datasource, geomEnv, 200, 200)
                    if (gridP) {
                        String ground_acoustic = Geoindicators.NoiseIndicators.groundAcousticAbsorption(h2gis_datasource, gridP, "id_grid",
                                building, road, water, vegetation,
                                impervious)
                        if (ground_acoustic) {
                            results.put("ground_acoustic", ground_acoustic)
                        }
                        tablesToDrop << gridP
                    }
                }
            }
            //Clean the database
            if (tablesToDrop) {
                h2gis_datasource.execute("DROP TABLE IF EXISTS ${tablesToDrop.join(",")}".toString())
            }
            info "${id_zone} has been processed"

            return results
        }
    }

    /**
    * Save the geoclimate tables into files
    * @param id_zone the id of the zone
    * @param results a list of tables computed by geoclimate
    * @param outputFolder the output folder
    * @param outputSRID srid code to reproject the result
    * @param deleteOutputData true to delete the file if exists
     * @param outputZoneGeometry if the geometry is not null we use it to filter the output data
    * @return
    */
    def saveOutputFiles(def h2gis_datasource, def results, def outputFiles, def outputFolder, def outputSRID,
                        def reproject, def deleteOutputData, def outputGrid, Geometry outputZoneGeometry) throws Exception {
        def whereFilter =""
        if(outputZoneGeometry) {
            whereFilter+="""the_geom && ST_GEOMFROMTEXT('${outputZoneGeometry}',${outputZoneGeometry.getSRID()}) 
            and ST_INTERSECTS(the_geom, ST_GEOMFROMTEXT('${outputZoneGeometry}',${outputZoneGeometry.getSRID()}))"""
        }
        outputFiles.each {
            if (it in ["grid_indicators", "grid_target"]) {
                if (outputGrid == "fgb") {
                    Geoindicators.WorkflowUtilities.saveInFile(results."$it", "${outputFolder + File.separator + it}.fgb", h2gis_datasource, outputSRID, reproject,null, deleteOutputData)
                } else if (outputGrid == "asc") {
                    Geoindicators.WorkflowUtilities.saveToAscGrid(results."$it", outputFolder, it, h2gis_datasource, outputSRID, reproject, deleteOutputData)
                }
            } else if (it == "building_updated") {
                Geoindicators.WorkflowUtilities.saveToCSV(results."$it", "${outputFolder + File.separator + it}.csv", h2gis_datasource, deleteOutputData)
            }
            else if (it in["building","road",  "rail",   "water", "vegetation",  "impervious",
                           "urban_areas", "road_traffic","ground_acoustic",
                           "urban_sprawl_areas", "urban_cool_areas"]) {
                //Apply where filter
                Geoindicators.WorkflowUtilities.saveInFile(results."$it", "${outputFolder + File.separator + it}.fgb", h2gis_datasource, outputSRID, reproject, whereFilter?" WHERE "+whereFilter:null, deleteOutputData)
            } else if(it in[ "building_indicators", "block_indicators","rsu_utrf_area" ,
                             "rsu_utrf_floor_area" , "building_utrf" , "rsu_indicators" ,
                             "rsu_lcz"] ) {
                Geoindicators.WorkflowUtilities.saveInFile(results."$it", "${outputFolder + File.separator + it}.fgb", h2gis_datasource, outputSRID, reproject, whereFilter ? " WHERE " + whereFilter + " and ID_RSU IS NOT NULL" : " where ID_RSU IS NOT NULL", deleteOutputData)

            }  else {
                Geoindicators.WorkflowUtilities.saveInFile(results."$it", "${outputFolder + File.separator + it}.fgb", h2gis_datasource, outputSRID, reproject, null, deleteOutputData)
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
 * @param excluded_columns list of columns to exclude for each table saved in a database
 * @return
 */
    def saveTablesInDatabase(def output_datasource, H2GIS h2gis_datasource, def outputTableNames, def h2gis_tables,
                             def id_zone, def inputSRID, def outputSRID, def reproject,Map excluded_columns) throws Exception {
        Connection con = output_datasource.getConnection()
        con.setAutoCommit(true)

        //Export building indicators
        indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators, id_zone, h2gis_datasource, h2gis_tables.building_indicators
                , "WHERE ID_RSU IS NOT NULL", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.building_indicators))


        //Export block indicators
        indicatorTableBatchExportTable(output_datasource, outputTableNames.block_indicators, id_zone, h2gis_datasource, h2gis_tables.block_indicators
            , "WHERE ID_RSU IS NOT NULL", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.block_indicators))

    //Export rsu indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_indicators, id_zone, h2gis_datasource, h2gis_tables.rsu_indicators
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.rsu_indicators))

    //Export rsu lcz
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_lcz, id_zone, h2gis_datasource, h2gis_tables.rsu_lcz
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.rsu_lcz))

    //Export rsu_utrf_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_utrf_area, id_zone, h2gis_datasource, h2gis_tables.rsu_utrf_area
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.rsu_utrf_area))

    //Export rsu_utrf_floor_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_utrf_floor_area, id_zone, h2gis_datasource, h2gis_tables.rsu_utrf_floor_area
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.rsu_utrf_floor_area))

    //Export grid_indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.grid_indicators, id_zone, h2gis_datasource, h2gis_tables.grid_indicators
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.grid_indicators))

    //Export building_utrf
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_utrf, id_zone, h2gis_datasource, h2gis_tables.building_utrf
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.building_utrf))

    //Export road_traffic
    indicatorTableBatchExportTable(output_datasource, outputTableNames.road_traffic, id_zone, h2gis_datasource, h2gis_tables.road_traffic
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.road_traffic))

    //Export zone
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.zone, id_zone, h2gis_datasource, h2gis_tables.zone
            , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.zone))

    //Export building
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.building, id_zone, h2gis_datasource, h2gis_tables.building
            , "", inputSRID, outputSRID, reproject,excluded_columns.get( outputTableNames.building))

        //Export road
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.road, id_zone, h2gis_datasource, h2gis_tables.road
                , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.road))
        //Export rail
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.rail, id_zone, h2gis_datasource, h2gis_tables.rail
                , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.rail))
        //Export vegetation
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.vegetation, id_zone, h2gis_datasource, h2gis_tables.vegetation
                , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.vegetation))
        //Export water
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.water, id_zone, h2gis_datasource, h2gis_tables.water
                , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.water))
        //Export impervious
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.impervious, id_zone, h2gis_datasource, h2gis_tables.impervious
                , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.impervious))

        //Export population table
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.population, id_zone, h2gis_datasource, h2gis_tables.population
                , "", inputSRID, outputSRID, reproject,excluded_columns.get(outputTableNames.population))

        //Export building_updated table
        abstractModelTableBatchExportTable(output_datasource, outputTableNames.building_updated, id_zone, h2gis_datasource, h2gis_tables.building_updated
                , "", inputSRID, outputSRID, false,excluded_columns.get(outputTableNames.building_updated))

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
 * @param reproject the output table
 * @param excluded_columns list of columns to exclude for each table saved in a database
 * @return
 */
    def abstractModelTableBatchExportTable(JdbcDataSource output_datasource, def output_table, def id_zone, def h2gis_datasource,
                                           h2gis_table_to_save, def filter, def inputSRID, def outputSRID, def reproject,def excluded_columns) throws Exception {
        if (output_table) {
            if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
                if (output_datasource.hasTable(output_table)) {
                    output_datasource.execute("DELETE FROM $output_table WHERE id_zone= '${id_zone.replace("'","''")}'")
                    //If the table exists we populate it with the last result
                    info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                    int BATCH_MAX_SIZE = 100
                    //We must exclude the columns to save in the database
                    Map columnsToKeepWithType =h2gis_datasource.getColumnNamesTypes(h2gis_table_to_save)
                    def columnNamesToSave =  columnsToKeepWithType.keySet()
                    if(excluded_columns) {
                        columnsToKeepWithType = columnsToKeepWithType.findAll{it-> !excluded_columns.contains(it.key)}
                        columnNamesToSave =  columnsToKeepWithType.keySet()
                    }
                    ITable inputRes = prepareTableOutput(h2gis_table_to_save, filter, inputSRID, h2gis_datasource, output_table, outputSRID, output_datasource,columnNamesToSave)
                    if (inputRes) {
                        def outputColumns = output_datasource.getColumnNamesTypes(output_table)
                        def outputconnection = output_datasource.getConnection()
                        try {
                            //We check if the number of columns is not the same
                            //If there is more columns in the input table we alter the output table
                            def outPutColumnsNames =  outputColumns.keySet()*.toUpperCase()
                            outPutColumnsNames.remove("GID")
                            def diffCols = columnNamesToSave.findAll { e -> !outPutColumnsNames.contains(e) }
                            def alterTable = ""
                            if (diffCols) {
                                columnsToKeepWithType.each { entry ->
                                    if (diffCols.contains(entry.key)) {
                                        //DECFLOAT is not supported by POSTSGRESQL
                                        def dataType = entry.value.equalsIgnoreCase("decfloat") ? "FLOAT" : entry.value
                                        alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key $dataType;"
                                        outputColumns.put(entry.key, dataType)
                                    }
                                }
                                output_datasource.execute(alterTable.toString())
                            }

                            def insertTable = "INSERT INTO $output_table (${finalOutputColumns.join(",")}) VALUES("

                            def flatList = outPutColumnsNames.inject([]) { result, iter ->
                                result += ":${iter.toLowerCase()}"
                            }.join(",")
                            insertTable += flatList
                            insertTable += ")"
                            //Collect all values
                            def ouputValues = outPutColumnsNames.collectEntries { [it.toLowerCase(), null] }
                            ouputValues.put("id_zone", id_zone)
                            outputconnection.setAutoCommit(false)
                            output_datasource.withBatch(BATCH_MAX_SIZE, insertTable.toString()) { ps ->
                                inputRes.eachRow { row ->
                                    //Fill the value
                                    columnNamesToSave.each { columnName ->
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
                            error("Cannot save the table $output_table.\n", e)
                            return false
                        } finally {
                            outputconnection.setAutoCommit(true)
                            debug "The table $h2gis_table_to_save has been exported into the table $output_table"
                        }
                    }
                } else {
                    def tmpTable = null
                    info "Start to export the table $h2gis_table_to_save into the table $output_table"
                    List columnNamesToSave =h2gis_datasource.getColumnNames(h2gis_table_to_save)
                    if(excluded_columns) {
                        columnNamesToSave = columnNamesToSave.findAll{it-> !excluded_columns.contains(it)}
                    }
                    if (filter) {
                        if (!reproject) {
                            tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnNamesToSave, filter).save(output_datasource, output_table, true);
                        } else {
                            tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, outputSRID, columnNamesToSave, filter).save(output_datasource, output_table, true);
                            //Because the select query reproject doesn't contain any geometry metadata
                            output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString())

                        }
                        if (tmpTable) {
                            //Workarround to update the SRID on resulset
                            output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);""".toString()

                        }
                    } else {
                        if (!reproject) {
                            tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnNamesToSave, filter).save(output_datasource, output_table, true);
                        } else {
                            tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, outputSRID, columnNamesToSave, filter).save(output_datasource, output_table, true);
                            //Because the select query reproject doesn't contain any geometry metadata
                            output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString())

                        }
                    }
                    if (tmpTable) {
                        //Add a GID column
                        output_datasource.execute """ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS gid serial;""".toString()
                        output_datasource.execute("UPDATE $output_table SET id_zone= '${id_zone.replace("'","''")}'")
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
 * @param reproject true to reproject the table
 * @param excluded_columns list of columns to exclude for each table saved in a database
 * @return
 */
    def indicatorTableBatchExportTable(JdbcDataSource output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save,
                                       def filter, def inputSRID, def outputSRID, def reproject,def excluded_columns) throws Exception {
        if (output_table) {
            if (h2gis_table_to_save) {
                if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
                    if (output_datasource.hasTable(output_table)) {
                        output_datasource.execute("DELETE FROM $output_table WHERE id_zone='${id_zone.replace("'","''")}'")
                        //If the table exists we populate it with the last result
                        info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                        int BATCH_MAX_SIZE = 100
                        //We must exclude the columns to save in the database
                        Map columnsToKeepWithType =[:]
                        if(excluded_columns) {
                            columnsToKeepWithType = h2gis_datasource.getColumnNamesTypes(h2gis_table_to_save).findAll{it-> !excluded_columns.contains(it.key)}
                        }
                        def columnNamesToSave =  columnsToKeepWithType.keySet()
                        ITable inputRes = prepareTableOutput(h2gis_table_to_save, filter, inputSRID, h2gis_datasource, output_table, outputSRID, output_datasource,columnNamesToSave)
                        if (inputRes) {
                            def outputColumns = output_datasource.getColumnNamesTypes(output_table)
                            outputColumns.remove("gid")
                            def outputconnection = output_datasource.getConnection()
                            try {
                                //We check if the number of columns is not the same
                                //If there is more columns in the input table we alter the output table
                                def outPutColumnsNames = outputColumns.keySet()
                                def diffCols = columnNamesToSave.findAll { e -> !outPutColumnsNames.contains(e) }
                                def alterTable = ""
                                if (diffCols) {
                                    columnsToKeepWithType.each { entry ->
                                        if (diffCols.contains(entry.key)) {
                                            //DECFLOAT is not supported by POSTSGRESQL
                                            def dataType = entry.value.equalsIgnoreCase("decfloat") ? "FLOAT" : entry.value
                                            alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key $dataType;"
                                            outputColumns.put(entry.key, dataType)
                                        }
                                    }
                                    output_datasource.execute(alterTable.toString())
                                }
                                def finalOutputColumns = outputColumns.keySet()

                                def insertTable = "INSERT INTO $output_table (${finalOutputColumns.join(",")}) VALUES("

                                def flatList = outputColumns.inject([]) { result, iter ->
                                    result += ":${iter.key.toLowerCase()}"
                                }.join(",")
                                insertTable += flatList
                                insertTable += ")"
                                //Collect all values
                                def ouputValues = finalOutputColumns.collectEntries { [it.toLowerCase(), null] }
                                ouputValues.put("id_zone", id_zone)
                                outputconnection.setAutoCommit(false)
                                output_datasource.withBatch(BATCH_MAX_SIZE, insertTable.toString()) { ps ->
                                    inputRes.eachRow { row ->
                                        //Fill the value
                                        columnNamesToSave.keySet().each { columnName ->
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
                                error("Cannot save the table $output_table.\n $e")
                                return false
                            } finally {
                                outputconnection.setAutoCommit(true)
                                info "The table $h2gis_table_to_save has been exported into the table $output_table"
                            }
                        }
                    } else {
                        def tmpTable = null
                        info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                        List columnNamesToSave =h2gis_datasource.getColumnNames(h2gis_table_to_save)
                        if(excluded_columns) {
                            columnNamesToSave.removeAll(excluded_columns)
                        }
                        if (filter) {
                            if (!reproject) {
                                tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnNamesToSave, filter).save(output_datasource, output_table, true)
                                if (tmpTable) {
                                    //Workaround to update the SRID on resulset
                                    output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);""".toString()
                                }
                            } else {
                                tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, outputSRID, columnNamesToSave, filter).save(output_datasource, output_table, true)
                                if (tmpTable) {
                                    //Workaround to update the SRID on resulset
                                    output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $outputSRID) USING ST_SetSRID(the_geom,$outputSRID);""".toString()
                                }
                            }
                        } else {
                            if (!reproject) {
                                tmpTable = Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnNamesToSave, filter).save(output_datasource, output_table, true)
                            } else {
                                tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true)
                                if (tmpTable) {
                                    //Because the select query reproject doesn't contain any geometry metadata
                                    output_datasource.execute("""ALTER TABLE $output_table 
                                    ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID) 
                                    USING ST_SetSRID(the_geom,$outputSRID);""")
                                }
                            }
                        }
                        if (tmpTable) {
                            output_datasource.execute("ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS id_zone VARCHAR")
                            output_datasource.execute("UPDATE $output_table SET id_zone= '${id_zone.replace("'","''")}'")
                            output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""".toString())
                            //Add GID column
                            output_datasource.execute("""ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS gid serial;""")

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
 * Method to prepare a ITable aka resultset to export table in a database
 * @param h2gis_table_to_save
 * @param inputSRID
 * @param h2gis_datasource
 * @param output_table
 * @param outputSRID
 * @param output_datasource
 * @return
 */
    def prepareTableOutput(def h2gis_table_to_save, def filter, def inputSRID, H2GIS h2gis_datasource,
                           def output_table, def outputSRID, JdbcDataSource output_datasource, Collection columnsToKeep) throws Exception {
        boolean hasGeometryColumn = output_datasource.hasGeometryColumn(output_table)
        def targetTableSrid = output_datasource.getSpatialTable(output_table).srid
        if (filter) {
            if(!hasGeometryColumn){
                return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
            }
            else if (outputSRID == 0) {
                if (inputSRID == targetTableSrid) {
                    return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                } else {
                    if (targetTableSrid == 0 && inputSRID == 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                    } else if (targetTableSrid != 0 && inputSRID != 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, targetTableSrid, columnsToKeep,filter)
                    } else {
                        error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                        return
                    }
                }
            } else {
                if (inputSRID == targetTableSrid) {
                    return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                } else {
                    if (targetTableSrid == 0 && inputSRID == 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                    } else if (targetTableSrid != 0 && inputSRID != 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, targetTableSrid, columnsToKeep,filter)
                    } else {
                        error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                        return
                    }
                }
            }
        } else {
            if(!hasGeometryColumn){
                return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
            }
            else if (outputSRID == 0) {
                if (inputSRID == targetTableSrid) {
                    return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                } else {
                    if (targetTableSrid == 0 && inputSRID == 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                    } else if (targetTableSrid != 0 && inputSRID != 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, targetTableSrid, columnsToKeep,filter)
                    } else {
                        error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                        return
                    }
                }
            } else {
                if (inputSRID == targetTableSrid) {
                    return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                } else {
                    if (targetTableSrid == 0 && inputSRID == 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, null, columnsToKeep,filter)
                    } else if (targetTableSrid != 0 && inputSRID != 0) {
                        return Geoindicators.WorkflowUtilities.getTableToSave(h2gis_datasource, h2gis_table_to_save, targetTableSrid, columnsToKeep,filter)
                    } else {
                        error("Cannot export the $h2gis_table_to_save into the table $output_table \n due to inconsistent SRID")
                        return
                    }
                }
            }
        }
    }

    /**
     *
     * @return the version of the BDTopo database
     */
    abstract int getVersion();

    /**
     * Implement it to format the layers
     * @param datasource
     * @param layers
     * @param distance
     * @param hLevMin
     * @return
     */
    abstract Map formatLayers(JdbcDataSource datasource, Map layers, float distance = 1000, float hLevMin = 3) throws Exception
}
