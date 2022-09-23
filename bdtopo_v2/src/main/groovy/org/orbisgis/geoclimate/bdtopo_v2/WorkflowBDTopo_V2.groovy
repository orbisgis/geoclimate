package org.orbisgis.geoclimate.bdtopo_v2

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2.tools.DeleteDbFiles
import org.h2gis.postgis_jts.PostGISDBFactory
import org.h2gis.utilities.FileUtilities
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.h2gis.utilities.URIUtilities
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.api.dataset.ITable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.process.api.IProcess
import org.orbisgis.geoclimate.Geoindicators
import org.h2gis.functions.io.utility.IOMethods
import org.orbisgis.geoclimate.worldpoptools.WorldPopTools
import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException


@BaseScript BDTopo_V2 BDTopo_V2

/**
 * BDTopo workflow processing chain.
 *
 * The parameters of the processing chain is defined
 * from a configuration file or a Map.
 * The configuration file is stored in a json format
 *
 * @param input The path of the configuration file or a Map
 *
 * The input file or the Map supports the following entries *
 *
 * {
 * [OPTIONAL ENTRY] "description" :"A description for the configuration file"
 *
 * [OPTIONAL ENTRY] "geoclimatedb" : { // Local H2GIS database used to run the processes
 *                                    // A default db is build when this entry is not specified
 *       "folder" : "/tmp/", //The folder to store the database
 *       "name" : "geoclimate_db;AUTO_SERVER=TRUE" // A name for the database
 *       "delete" :false
 *     },
 * [ONE ENTRY REQUIRED]   "input" : {
 *         "folder": "path of the folder that contains the BD Topo layers as shapefile",
 *             "locations":["56260"] //list of insee code, with a comma separator
 *             "srid :  2154}, //SRID to force the projection
 *          "database": {
 *             "user": "-",
 *             "password": "-",
 *             "url": "jdbc:postgresql://", //JDBC url to connect with the database
 *             //List of BDTOPO tables required to compute the geoclimate indicators
 *             "tables": {
 *                 "commune":"commune",
 *                 "bati_indifferencie":"ign_bdtopo_2017.bati_indifferencie",
 *                 "bati_industriel":"ign_bdtopo_2017.bati_industriel",
 *                 "bati_remarquable":"ign_bdtopo_2017.bati_remarquable",
 *                 "route":"ign_bdtopo_2017.route",
 *                 "troncon_voie_ferree":"ign_bdtopo_2017.troncon_voie_ferree",
 *                 "surface_eau":"ign_bdtopo_2017.surface_eau",
 *                 "zone_vegetation":"ign_bdtopo_2017.zone_vegetation",
 *                 "terrain_sport":"ign_bdtopo_2017.terrain_sport",
 *                 "construction_surfacique":"ign_bdtopo_2017.construction_surfacique",
 *                 "surface_route":"ign_bdtopo_2017.surface_route",
 *                 "surface_activite":"ign_bdtopo_2017.surface_activite"
 *                 "population":"insee.population"} }
 *             }
 *             ,
 *  [OPTIONAL ENTRY]  "output" :{ //If not output is set the results are keep in the local database
 *             "folder" : "/tmp/myResultFolder" //tmp folder to store the computed layers in a geojson format,
 *             "database": { //database parameters to store the computed layers
 *                  "user": "-",
 *                  "password": "-",
 *                  "url": "jdbc:postgresql://", //JDBC url to connect with the database
 *                  "tables": { //table names to store the result layers. Create the table if it doesn't exist
 *                      "building_indicators":"building_indicators",
 *                      "block_indicators":"block_indicators",
 *                      "rsu_indicators":"rsu_indicators",
 *                      "rsu_lcz":"rsu_lcz",
 *                      "zones":"zones"} }
 *     },
 *     ,
 *   [OPTIONAL ENTRY]  "parameters":
 *     {"distance" : 1000,
 *     "prefixName": "",
 *          rsu_indicators:{
 *         "indicatorUse": ["LCZ", "UTRF", "TEB"],
 *         "svfSimplified": false, *
 *         "mapOfWeights":
 *         {"sky_view_factor"                : 4,
 *          "aspect_ratio"                   : 3,
 *          "building_surface_fraction"      : 8,
 *          "impervious_surface_fraction"    : 0,
 *          "pervious_surface_fraction"      : 0,
 *           "height_of_roughness_elements"   : 6,
 *           "terrain_roughness_length"       : 0.5},
 *         "hLevMin": 3,
 *         "hLevMax": 15,
 *         "hThresho2": 10
 *         }
 *     }
 *     }
 * The parameters entry tag contains all geoclimate chain parameters.
 * When a parameter is not specificied a default value is set.
 *
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
 * - hLevMax Maximum building level height
 * - hThresholdLev2 Threshold on the building height, used to determine the number of levels
 *
 * @return
 * a map with the name of zone and a list of the output tables computed and stored in the local database,
 * otherwise throw an error.
 *
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
        title "Create all Geoindicators from BDTopo data"
        id "workflow"
        inputs input: Object
        outputs output: Map
        run { input ->
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
                    parameters = readJSONParameters(configFile)
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
            //Store the zone identifier and the names of the tables
            def outputTableNamesResult = [:]
            debug "Reading file parameters"
            debug parameters.description
            def inputParameters = parameters.input
            def outputParameters = parameters.output
            //Default H2GIS database properties
            def databaseFolder = System.getProperty("java.io.tmpdir")
            def databaseName = "bdtopo_v2_2" + UUID.randomUUID().toString().replaceAll("-", "_")
            def databasePath = databaseFolder + File.separator + databaseName
            def h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
            def delete_h2gis = true
            def geoclimatedb = parameters.geoclimatedb
            //We collect informations to create the local H2GIS database
            if (geoclimatedb) {
                def h2gis_folder = geoclimatedb.get("folder")
                if (h2gis_folder) {
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
                error "Cannot find any input parameters."
                return
            }
            def inputDataBase = inputParameters.database
            def inputFolder = inputParameters.folder
            def locations = inputParameters.locations as Set
            if (!locations) {
                error "Cannot find any locations parameter."
                return
            }
            def inputSRID = inputParameters.srid
            if (inputSRID && inputSRID <= 0) {
                error "The input srid must be greater than 0."
                return
            }
            if (inputFolder && inputDataBase) {
                error "Please set only one input data provider"
                return null
            }

            def inputWorkflowTableNames = ["commune",
                                           "bati_indifferencie", "bati_industriel",
                                           "bati_remarquable", "route",
                                           "troncon_voie_ferree", "surface_eau",
                                           "terrain_sport", "construction_surfacique",
                                           "surface_route", "surface_activite",
                                           "piste_aerodrome", "reservoir", "zone_vegetation"]

            def outputWorkflowTableNames = ["building_indicators",
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
                                            "rsu_utrf_area",
                                            "rsu_utrf_floor_area",
                                            "building_utrf",
                                            "grid_indicators",
                                            "road_traffic",
                                            "population"]
            //Get processing parameters
            def processing_parameters = extractProcessingParameters(parameters.parameters)
            if (!processing_parameters) {
                warn "Please set \"parameters\" values to compute GeoClimate indicators."
                return
            }

            //Get the out put parameters
            def outputDatasource
            def outputTables
            def file_outputFolder
            def outputFileTables
            def outputSRID
            if (outputParameters) {
                def outputDataBase = outputParameters.database
                def outputFolder = outputParameters.folder
                def deleteOutputData = outputParameters.get("delete")
                if (!deleteOutputData) {
                    deleteOutputData = true
                } else if (!deleteOutputData in Boolean) {
                    error "The delete parameter must be a boolean value"
                    return null
                }
                outputSRID = outputParameters.get("srid")
                if (outputSRID && outputSRID <= 0) {
                    error "The output srid must be greater than 0"
                    return null
                }
                if (outputFolder) {
                    def outputFiles = outputFolderProperties(outputFolder, outputWorkflowTableNames)
                    outputFileTables = outputFiles.tables
                    //Check if we can write in the output folder
                    file_outputFolder = new File(outputFiles.path)
                    if (!file_outputFolder.isDirectory()) {
                        error "The directory $file_outputFolder doesn't exist."
                        return
                    }
                    if (!file_outputFolder.canWrite()) {
                        file_outputFolder = null
                        error "You don't have permission to write in the folder $outputFolder \n" +
                                "Please check the folder."
                        return
                    }

                } else if (outputDataBase) {
                    //Check the conditions to store the results a database
                    def outputDataBaseData = checkOutputTables(outputDataBase, outputDataBase.tables, outputWorkflowTableNames)
                    outputDatasource = outputDataBaseData.outputDatasource
                    outputTables = outputDataBaseData.outputTables
                }
            }

            /**
             * Run the workflow when the input data comes from a folder
             */
            if (inputFolder) {
                def h2gis_datasource = H2GIS.open(h2gis_properties)
                if (!h2gis_datasource) {
                    error "Cannot load the local H2GIS database to run Geoclimate"
                    return
                }
                def datafromFolder = linkDataFromFolder(inputFolder, inputWorkflowTableNames, h2gis_datasource, inputSRID)
                inputSRID = datafromFolder.inputSrid
                def sourceSrid = datafromFolder.sourceSrid
                def tablesLinked = datafromFolder.tableNames
                if (tablesLinked) {
                    locations.each { location ->
                        //We must extract the data from the shapefiles for each locations
                        if (filterLinkedData(location, processing_parameters.distance, tablesLinked, sourceSrid, inputSRID, h2gis_datasource)) {
                            def formatedZone = checkAndFormatLocations(location)
                            if (formatedZone) {
                                def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters,
                                        createMainFolder(file_outputFolder, formatedZone), outputFileTables, outputDatasource,
                                        outputTables, outputSRID, inputSRID)
                                if (bdtopo_results) {
                                    outputTableNamesResult.putAll(bdtopo_results)
                                }
                            }
                        }
                    }
                    deleteH2GISDb(delete_h2gis, h2gis_datasource.getConnection(), databaseFolder, databaseName)

                    return [output: outputTableNamesResult]
                } else {
                    error "Cannot find any data to process from the folder $inputFolder"
                    return
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
                    if (inputTables_tmp.size()==0) {
                        error "Please set a valid list of input tables as  : \n" +
                                "${inputWorkflowTableNames.collect { name -> [name: name] }}"
                        return
                    }
                    inputTables =inputTables_tmp
                }

                def h2gis_datasource = H2GIS.open(h2gis_properties)
                if (!h2gis_datasource) {
                    error "Cannot load the local H2GIS database to run Geoclimate"
                    return
                }
                def nbzones = 0;
                for (location in locations) {
                    nbzones++
                    inputSRID = loadDataFromPostGIS(inputDataBase.subMap(["user", "password", "url"]), location, processing_parameters.distance, inputTables,inputSRID, h2gis_datasource)
                    if (inputSRID) {
                        def formatedZone = checkAndFormatLocations(location)
                        if (formatedZone) {
                            def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters, createMainFolder(file_outputFolder, formatedZone), outputFileTables, outputDatasource, outputTables, outputSRID, inputSRID)
                            if (bdtopo_results) {
                                outputTableNamesResult.putAll(bdtopo_results)
                            } else {
                                error "Cannot execute the geoclimate processing chain on $location\n"
                            }
                        }
                    } else {
                        error "Cannot load the data for the location $location"
                    }
                    info "${nbzones} location(s) on ${locations.size()}"
                }
                deleteH2GISDb(delete_h2gis, h2gis_datasource.getConnection(), databaseFolder, databaseName)
                if (outputTableNamesResult) {
                    return [output: outputTableNamesResult]
                }
            }
            return [output: null]
        }
    }
}

/**
 *
 * Method to delete the local H2GIS database used by the workflow
 * @param delete
 * @param h2GIS
 * @return
 */
def deleteH2GISDb(def delete, Connection connection, def dbFolder, def dbName) {
    if (delete) {
        if (connection) {
            connection.close()
            DeleteDbFiles.execute(dbFolder, dbName, true)
            debug "The local H2GIS database : ${dbName} has been deleted"
        } else {
            error "Cannot delete the local H2GIS database : ${dbName} "
        }
    }
}

/**
 * Method to check the output tables set by the user and create the datasource connexion
 * Return null if the table names are invalid or if we cannot create the connection to the database
 */
def checkOutputTables(def outputDataBase, def outputTableNames, def outputWorkflowTableNames) {
    def outputTables = [:]
    if (!outputTableNames) {
        outputTables = outputWorkflowTableNames.collect { [it: it] }
    } else {
        outputTableNames.each { table ->
            if (outputWorkflowTableNames.contains(table.key.toLowerCase())) {
                outputTables.put(table.key.toLowerCase(), table.value)
            }
        }
        if (!outputTables) {
            error "Please set a valid list of output tables as  : \n" +
                    "${outputWorkflowTableNames.collect { name -> [name: name] }}"
        }
    }
    def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
    if (!output_datasource) {
        error "Cannot connect to the output database"
    }
    return [outputTables: outputTables, outputDatasource: output_datasource]

}
/**
 * Sanity check for the location value
 * @param id_zones
 * @return
 */
def checkAndFormatLocations(def locations) {
    if (locations in Collection) {
        return locations.join("_")
    } else if (locations instanceof String) {
        return locations.trim()
    } else {
        error "Invalid location input. \n" +
                "The location input must be a string value or an array of 4 coordinates to define a bbox "
        return null
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
    def folder = new File(outputFolder.getAbsolutePath() + File.separator + "bdtopo_v2_" + location)
    if (!folder.exists()) {
        folder.mkdir()
    } else {
        FileUtilities.deleteFiles(folder)
    }
    return folder.getAbsolutePath()
}

/**
 * Return the properties parameters to store results in a folder
 * @param outputFolder the output folder parameters from the json file
 */
def outputFolderProperties(def outputFolder, def outputWorkflowTableNames) {
    if (outputFolder in Map) {
        def outputPath = outputFolder.path
        def outputTables = outputFolder.tables
        if (!outputPath) {
            return
        }
        if (outputTables) {
            return ["path": outputPath, "tables": outputWorkflowTableNames.intersect(outputTables)]
        }
        return ["path": outputPath, "tables": outputWorkflowTableNames]
    } else {
        return ["path": outputFolder, "tables": outputWorkflowTableNames]
    }
}

/**
 * Create a datasource from the following parameters :
 * user, password, url
 *
 * @param database_properties from the json file
 * @return a connection or null if the parameters are invalid
 */
def createDatasource(def database_properties) {
    def db_output_url = database_properties.url
    if (db_output_url) {
        if (db_output_url.startsWith("jdbc:")) {
            String url = db_output_url.substring("jdbc:".length());
            if (url.startsWith("h2")) {
                return H2GIS.open(database_properties)
            } else if (url.startsWith("postgresql")) {
                return POSTGIS.open(database_properties)
            } else {
                error "Unsupported database"
                return
            }
        } else {
            error "Invalid output database url"
            return
        }

    } else {
        error "The output database url cannot be null or empty"
    }
}

/**
 *
 * Method to filter the data from the input shapefiles
 * @param location
 * @param distance
 * @param inputTables
 * @param sourceSRID
 * @param inputSRID
 * @param h2gis_datasource
 * @return
 */
def filterLinkedData(def location, def distance, def inputTables, def sourceSRID, def inputSRID, H2GIS h2gis_datasource) {
    def formatting_geom = "the_geom"
    if (sourceSRID == 0 && sourceSRID != inputSRID) {
        formatting_geom = "st_setsrid(the_geom, $inputSRID) as the_geom"
    } else if (sourceSRID >= 0 && sourceSRID == inputSRID) {
        formatting_geom = "st_transform(the_geom, $inputSRID) as the_geom"
    }

    String outputTableName = "COMMUNE"
    //Let's process the data by location
    //Check if code is a string or a bbox
    //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
    if (location in Collection) {
        debug "Loading in the H2GIS database $outputTableName"
        h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as  SELECT
                    ST_INTERSECTION(the_geom, ST_MakeEnvelope(${location[1]},${location[0]},${location[3]},${location[2]}, $sourceSRID)) as the_geom, CODE_INSEE  from ${inputTables.commune} where the_geom 
                    && ST_MakeEnvelope(${location[1]},${location[0]},${location[3]},${location[2]}, $sourceSRID) """.toString())
    } else if (location instanceof String) {
        debug "Loading in the H2GIS database $outputTableName"
        h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT $formatting_geom, CODE_INSEE FROM ${inputTables.commune} WHERE CODE_INSEE='$location' or lower(nom)='${location.toLowerCase()}'".toString())

    }
    def count = h2gis_datasource."$outputTableName".rowCount
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM

        if (inputTables.bati_indifferencie) {
            //Extract bati_indifferencie
            outputTableName = "BATI_INDIFFERENCIE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, HAUTEUR FROM ${inputTables.bati_indifferencie}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.bati_industriel) {
            //Extract bati_industriel
            outputTableName = "BATI_INDUSTRIEL"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, HAUTEUR FROM ${inputTables.bati_industriel}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.bati_remarquable) {
            //Extract bati_remarquable
            outputTableName = "BATI_REMARQUABLE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, HAUTEUR FROM ${inputTables.bati_remarquable}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.route) {
            //Extract route
            outputTableName = "ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST, SENS FROM ${inputTables.route}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        } else {
            error "The route table must be provided"
            return
        }

        if (inputTables.troncon_voie_ferree) {
            //Extract troncon_voie_ferree
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "TRONCON_VOIE_FERREE"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID,$formatting_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTables.troncon_voie_ferree}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.surface_eau) {
            //Extract surface_eau
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "SURFACE_EAU"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom FROM ${inputTables.surface_eau}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.zone_vegetation) {
            //Extract zone_vegetation
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "ZONE_VEGETATION"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName  AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.zone_vegetation}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.terrain_sport) {
            //Extract terrain_sport
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "TERRAIN_SPORT"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.terrain_sport}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport'".toString())

        }

        if (inputTables.construction_surfacique) {
            //Extract construction_surfacique
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier')".toString())
        }

        if (inputTables.surface_route) {
            //Extract surface_route
            outputTableName = "SURFACE_ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom,NATURE  FROM ${inputTables.surface_route}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.surface_activite) {
            //Extract surface_activite
            outputTableName = "SURFACE_ACTIVITE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, CATEGORIE  FROM ${inputTables.surface_activite}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND (CATEGORIE='Administratif' OR CATEGORIE='Enseignement' OR CATEGORIE='Santé')".toString())
        }
        //Extract PISTE_AERODROME
        if (inputTables.piste_aerodrome) {
            outputTableName = "PISTE_AERODROME"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName  AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.piste_aerodrome}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        //Extract RESERVOIR
        if (inputTables.reservoir) {
            outputTableName = "RESERVOIR"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE, HAUTEUR  FROM ${inputTables.reservoir}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }
        return true

    } else {
        error "Cannot find any commune with the insee code : $location"
        return
    }

}

/**
 * Load the required tables stored in a database
 *
 * @param inputDatasource database where the tables are
 * @return true is succeed, false otherwise
 */
def loadDataFromPostGIS(def input_database_properties, def code, def distance, def inputTables, def inputSRID, H2GIS h2gis_datasource) {
    def commune_location = inputTables.commune
    if (!commune_location) {
        error "The commune table must be specified to run Geoclimate"
        return
    }
    PostGISDBFactory dataSourceFactory = new PostGISDBFactory();
    Connection sourceConnection = null;
    try {
        Properties props = new Properties();
        input_database_properties.forEach(props::put);
        DataSource ds = dataSourceFactory.createDataSource(props);
        sourceConnection = ds.getConnection()
    } catch (SQLException e) {
        error("Cannot connect to the database to import the data ");
    }

    if (sourceConnection == null) {
        error("Cannot connect to the database to import the data ");
        return
    }

    //Check if the commune table exists
    if (!JDBCUtilities.tableExists(sourceConnection, commune_location)) {
        error("The commune table doesn't exist");
        return
    }

    //Find the SRID of the commune table
    def commune_srid = GeometryTableUtilities.getSRID(sourceConnection, commune_location)
    if (commune_srid <= 0) {
        error("The commune table doesn't have any SRID");
        return
    }
    if (commune_srid == 0 && inputSRID) {
        commune_srid = inputSRID
    } else if (commune_srid <= 0) {
        warn "Cannot find a SRID value for the layer commune.\n" +
                "Please set a valid OGC prj or use the parameter srid to force it."
        return null
    }

    String outputTableName = "COMMUNE"
    //Let's process the data by location
    //Check if code is a string or a bbox
    //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
    if (code in Collection) {
        //def tmp_insee = code.join("_")
        String inputTableName = """(SELECT
                    ST_INTERSECTION(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)) as the_geom, CODE_INSEE  from $commune_location where 
                    st_setsrid(the_geom, $commune_srid) 
                    && ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid) and
                    st_intersects(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)))""".toString()
        debug "Loading in the H2GIS database $outputTableName"
        IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 10);
    } else if (code instanceof String) {
        String inputTableName = "(SELECT st_setsrid(the_geom, $commune_srid) as the_geom, CODE_INSEE FROM $commune_location WHERE CODE_INSEE='$code' or lower(nom)='${code.toLowerCase()}')"
        debug "Loading in the H2GIS database $outputTableName"
        IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
    }
    def count = h2gis_datasource."$outputTableName".rowCount
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM
        def outputTableNameBatiInd = "BATI_INDIFFERENCIE"
        if (inputTables.bati_indifferencie) {
            //Extract bati_indifferencie
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, HAUTEUR FROM ${inputTables.bati_indifferencie}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiInd"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiInd, -1, 1000);
        }
        def outputTableNameBatiIndus = "BATI_INDUSTRIEL"
        if (inputTables.bati_industriel) {
            //Extract bati_industriel
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR FROM ${inputTables.bati_industriel}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiIndus"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiIndus, -1, 1000);
        }
        def outputTableNameBatiRem = "BATI_REMARQUABLE"
        if (inputTables.bati_remarquable) {
            //Extract bati_remarquable
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR FROM ${inputTables.bati_remarquable}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiRem"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiRem, -1, 1000);
        }
        def outputTableNameRoad = "ROUTE"
        if (inputTables.route) {
            //Extract route
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST, SENS FROM ${inputTables.route}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameRoad"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameRoad, -1, 1000);
        } else {
            error "The route table must be provided"
            return
        }

        if (inputTables.troncon_voie_ferree) {
            //Extract troncon_voie_ferree
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTables.troncon_voie_ferree}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "TRONCON_VOIE_FERREE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if (inputTables.surface_eau) {
            //Extract surface_eau
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom FROM ${inputTables.surface_eau}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_EAU"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if (inputTables.zone_vegetation) {
            //Extract zone_vegetation
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.zone_vegetation}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "ZONE_VEGETATION"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if (inputTables.terrain_sport) {
            //Extract terrain_sport
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.terrain_sport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport')"
            outputTableName = "TERRAIN_SPORT"
            debug "Loading in the H2GIS database $outputTableName"

            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if (inputTables.construction_surfacique) {
            //Extract construction_surfacique
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier'))"
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if (inputTables.surface_route) {
            //Extract surface_route
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom,NATURE  FROM ${inputTables.surface_route}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if (inputTables.surface_activite) {
            //Extract surface_activite
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, CATEGORIE  FROM ${inputTables.surface_activite}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (CATEGORIE='Administratif' OR CATEGORIE='Enseignement' OR CATEGORIE='Santé'))"
            outputTableName = "SURFACE_ACTIVITE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }
        //Extract PISTE_AERODROME
        if (inputTables.piste_aerodrome) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.piste_aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "PISTE_AERODROME"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        //Extract RESERVOIR
        if (inputTables.reservoir) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR  FROM ${inputTables.reservoir}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "RESERVOIR"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        sourceConnection.close();

        return commune_srid

    } else {
        error "Cannot find any commune with the insee code : $code"
        return
    }
}

/**
 * Load  shapefiles into the local H2GIS database
 *
 * @param inputFolder where the files are
 * @param h2gis_datasource the local database for the geoclimate processes
 * @param inputSRID a list of SRID set the geometries
 * @return a list of id_zones
 */
def linkDataFromFolder(def inputFolder, def inputWorkflowTableNames, H2GIS h2gis_datasource, def inputSRID) {
    def folder = new File(inputFolder)
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
            h2gis_datasource.link(commune_file, "COMMUNE_TMP", true)
            geoFiles.remove(commune_file)
            int srid = h2gis_datasource.getSpatialTable("COMMUNE_TMP").srid
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
            //h2gis_datasource.execute("CREATE SPATIAL INDEX ON COMMUNE_TMP(THE_GEOM)".toString())
            geoFiles.eachWithIndex { geoFile, index ->
                debug "linking file $geoFile $index on $numberFiles"
                //We must link only the allowed tables
                def fileName = URIUtilities.fileFromString(geoFile).getName()
                def name = fileName.substring(0, fileName.lastIndexOf(".")).toLowerCase();
                if (inputWorkflowTableNames.contains(name)) {
                    h2gis_datasource.link(geoFile, "${name}_tmp", true)
                    //h2gis_datasource.execute("CREATE SPATIAL INDEX ON ${name}_tmp(THE_GEOM)".toString())
                    tableNames.put(name, "${name}_tmp")
                }
            }
            return ["sourceSrid": sourceSrid, "inputSrid": srid, "tableNames": tableNames]

        } else {
            error "The input folder must contains a file named commune"
            return
        }
    } else {
        error "The input folder must be a directory"
        return
    }
}

/**
 * Read the file parameters and create a new map of parameters
 * The map of parameters is initialized with default values
 *
 * @param processing_parameters the file parameters
 * @return a filled map of parameters
 */
def extractProcessingParameters(def processing_parameters) {
    def defaultParameters = [distance       : 1000,
                             distance_buffer: 500, prefixName: "",
                             hLevMin        : 3, hLevMax: 15, hThresholdLev2: 10]
    def rsu_indicators_default = [indicatorUse      : [],
                                  svfSimplified     : true,
                                  surface_vegetation: 10000,
                                  surface_hydro     : 2500,
                                  snappingTolerance : 0.01,
                                  mapOfWeights      : ["sky_view_factor"             : 4,
                                                       "aspect_ratio"                : 3,
                                                       "building_surface_fraction"   : 8,
                                                       "impervious_surface_fraction" : 0,
                                                       "pervious_surface_fraction"   : 0,
                                                       "height_of_roughness_elements": 6,
                                                       "terrain_roughness_length"    : 0.5],
                                  utrfModelName     : "UTRF_BDTOPO_V2_RF_2_2.model"]
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
        def hLevMaxP = processing_parameters.hLevMax
        if (hLevMaxP && hLevMaxP in Integer) {
            defaultParameters.hLevMax = hLevMaxP
        }
        def hThresholdLev2P = processing_parameters.hThresholdLev2
        if (hThresholdLev2P && hThresholdLev2P in Integer) {
            defaultParameters.hThresholdLev2 = hThresholdLev2P
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
                error "The list of RSU indicator names cannot be null or empty"
                return
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

        return defaultParameters
    }
    return defaultParameters
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
def bdtopo_processing(H2GIS h2gis_datasource, def processing_parameters, def outputFolder, def outputFiles,
                      def output_datasource, def outputTableNames, def outputSRID, def inputSRID, def deleteOutputData = true) {
    //Add the GIS layers to the list of results
    def outputTableNamesResult = [:]
    //We process each zones because the input zone can overlap several communes
    h2gis_datasource.eachRow("SELECT ST_CollectionExtract(THE_GEOM, 3) as the_geom,code_insee  FROM COMMUNE") { row ->
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
                """.toString())
                def results = bdTopoProcessingSingleArea(h2gis_datasource, code_insee, subCommuneTableName, inputSRID, processing_parameters)
                if (results) {
                    saveResults(h2gis_datasource, code_insee, results, inputSRID, outputFolder, outputFiles, output_datasource, outputTableNames, outputSRID, deleteOutputData)
                    outputTableNamesResult.put(code_insee, results.findAll { it.value != null })
                }
            }
        } else {
            int subAreaCount = 1
            def tablesToMerge = ["zone"                : [],
                                 "road"                : [], "rail": [], "water": [],
                                 "vegetation"          : [], "impervious": [], "building": [],
                                 "building_indicators": [], "block_indicators": [],
                                 "rsu_indicators"     : [], "rsu_lcz": [],
                                 "rsu_utrf_area"       : [], "rsu_utrf_floor_area": [],
                                 "building_utrf"      : [], "population": [], "road_traffic": [],
                                 "grid_indicators": []
            ]
            for (i in 0..<numGeom) {
                info "Processing the sub area ${subAreaCount} on ${numGeom + 1}"
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
                """.toString())
                    def results = bdTopoProcessingSingleArea(h2gis_datasource, code_insee, subCommuneTableName, inputSRID, processing_parameters)
                    if (results) {
                        results.each { it ->
                            if (it.value) {
                                tablesToMerge[it.key] << it.value
                            }
                        }
                    }
                }
                subAreaCount++
            }
            //We must merge here all sub areas and then compute the grid indicators
            //so the user have a continuous spatial domain instead of multiples tables
            def results = [:]
            tablesToMerge.each { it ->
                def tableNames = it.value
                def queryToJoin = []
                String tmp_table
                tableNames.each { tableName ->
                    if (tableName) {
                        queryToJoin << "SELECT * FROM $tableName "
                        tmp_table = tableName
                    }
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
            outputTableNamesResult.put(code_insee, results)
            saveResults(h2gis_datasource, code_insee, results, inputSRID, outputFolder, outputFiles, output_datasource, outputTableNames, outputSRID, deleteOutputData)

        }
    }
    return outputTableNamesResult

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
 * @return
 */
def saveResults(def h2gis_datasource, def id_zone, def results, def srid, def outputFolder, def outputFiles, def output_datasource, def outputTableNames, def outputSRID, def deleteOutputData) {
    //Check if the user decides to reproject the output data
    def reproject = false
    if (outputSRID) {
        if (outputSRID != srid) {
            reproject = true
        }
    } else {
        outputSRID = srid
    }
    if (outputFolder && outputFiles) {
        //Create the folder to save the results
        def folder = new File(outputFolder + File.separator + id_zone)
        if (!folder.exists()) {
            folder.mkdir()
        } else {
            FileUtilities.deleteFiles(folder)
        }
        saveOutputFiles(h2gis_datasource, results, outputFiles, folder, outputSRID, reproject, deleteOutputData)
    }
    if (output_datasource) {
        saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)
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
    def start = System.currentTimeMillis()
    //Load and format the BDTopo data
    IProcess loadAndFormatData = loadAndFormatData()
    if (loadAndFormatData.execute([datasource                  : h2gis_datasource,
                                   tableCommuneName            : subCommuneTableName,
                                   tableBuildIndifName         : 'BATI_INDIFFERENCIE',
                                   tableBuildIndusName         : 'BATI_INDUSTRIEL',
                                   tableBuildRemarqName        : 'BATI_REMARQUABLE',
                                   tableRoadName               : 'ROUTE',
                                   tableRailName               : 'TRONCON_VOIE_FERREE',
                                   tableHydroName              : 'SURFACE_EAU',
                                   tableVegetName              : 'ZONE_VEGETATION',
                                   tableImperviousSportName    : 'TERRAIN_SPORT',
                                   tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                   tableImperviousRoadSurfName : 'SURFACE_ROUTE',
                                   tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                   tablePiste_AerodromeName    : 'PISTE_AERODROME',
                                   tableReservoirName          : 'RESERVOIR',
                                   distance                    : processing_parameters.distance,
                                   hLevMin                     : processing_parameters.hLevMin,
                                   hLevMax                     : processing_parameters.hLevMax,
                                   hThresholdLev2              : processing_parameters.hThresholdLev2
    ])) {

        def buildingTableName = loadAndFormatData.results.outputBuilding
        def roadTableName = loadAndFormatData.results.outputRoad
        def railTableName = loadAndFormatData.results.outputRail
        def hydrographicTableName = loadAndFormatData.results.outputHydro
        def vegetationTableName = loadAndFormatData.results.outputVeget
        def zone = loadAndFormatData.results.outputZone
        def imperviousTableName = loadAndFormatData.results.outputImpervious

        info "BDTOPO V2 GIS layers formated"

        def rsu_indicators_params = processing_parameters.rsu_indicators
        def grid_indicators_params = processing_parameters.grid_indicators
        def worldpop_indicators = processing_parameters.worldpop_indicators


        results.put("zone", zone)
        results.put("road", roadTableName)
        results.put("rail", railTableName)
        results.put("water", hydrographicTableName)
        results.put("vegetation", vegetationTableName)
        results.put("impervious", imperviousTableName)
        results.put("building", buildingTableName)

        //Compute the RSU indicators
        if (rsu_indicators_params.indicatorUse) {
            //Build the indicators
            IProcess geoIndicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators()
            if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zone,
                    buildingTable: buildingTableName, roadTable: roadTableName,
                    railTable: railTableName, vegetationTable: vegetationTableName,
                    hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                    surface_vegetation: rsu_indicators_params.surface_vegetation, surface_hydro: rsu_indicators_params.surface_hydro,
                    snappingTolerance: rsu_indicators_params.snappingTolerance,
                    indicatorUse: rsu_indicators_params.indicatorUse,
                    svfSimplified: rsu_indicators_params.svfSimplified, prefixName: processing_parameters.prefixName,
                    mapOfWeights: rsu_indicators_params.mapOfWeights,
                    utrfModelName: "UTRF_BDTOPO_V2_RF_2_2.model")) {
                error "Cannot build the geoindicators for the zone $id_zone"
            } else {
                results.putAll(geoIndicators.getResults())
            }
        }
        //Extract and compute population indicators for the specified year
        //This data can be used by the grid_indicators process
        if (worldpop_indicators) {
            IProcess extractWorldPopLayer = WorldPopTools.Extract.extractWorldPopLayer()
            def coverageId = "wpGlobal:ppp_2018"
            def envelope = h2gis_datasource.firstRow("select st_transform(the_geom, 4326) as geom from $zone".toString()).geom.getEnvelopeInternal()
            def bbox = [envelope.getMinY() as Float, envelope.getMinX() as Float,
                        envelope.getMaxY() as Float, envelope.getMaxX() as Float]
            if (extractWorldPopLayer.execute([coverageId: coverageId, bbox: bbox])) {
                IProcess importAscGrid = WorldPopTools.Extract.importAscGrid()
                if (importAscGrid.execute([worldPopFilePath: extractWorldPopLayer.results.outputFilePath,
                                           epsg            : srid, tableName: coverageId.replaceAll(":", "_"), datasource: h2gis_datasource, epsg: srid])) {
                    results.put("population", importAscGrid.results.outputTableWorldPopName)

                    IProcess process = Geoindicators.BuildingIndicators.buildingPopulation()
                    if (!process.execute([inputBuildingTableName  : results.building,
                                          inputpopulation: importAscGrid.results.outputTableWorldPopName,
                                          inputPopulationColumns  : ["pop"], datasource: h2gis_datasource])) {
                        info "Cannot compute any population data at building level"
                    }
                    //Update the building table with the population data
                    buildingTableName = process.results.buildingTableName
                    results.put("building", buildingTableName)

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

        //Compute the grid indicators
        if (grid_indicators_params) {
            def x_size = grid_indicators_params.x_size
            def y_size = grid_indicators_params.y_size
            IProcess gridProcess = Geoindicators.WorkflowGeoIndicators.createGrid()
            def geomEnv = h2gis_datasource.getSpatialTable(zone).getExtent()
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
                        prefixName: processing_parameters.prefixName
                )) {
                    results.put("grid_indicators", rasterizedIndicators.results.outputTableName)
                }
            } else {
                info "Cannot create a grid to aggregate the indicators"
            }
        }

        //Compute traffic flow
        if (processing_parameters.road_traffic) {
            IProcess format = Geoindicators.RoadIndicators.build_road_traffic()
            format.execute([
                    datasource    : h2gis_datasource,
                    inputTableName: roadTableName,
                    epsg          : srid])
            results.put("road_traffic", format.results.outputTableName)
        }

        info "${id_zone} has been processed"

        return results;
    }
}

/**
 * Save the geoclimate tables into geojson files
 * @param id_zone the id of the zone
 * @param results a list of tables computed by geoclimate
 * @param outputFolder the output folder
 * @param outputSRID srid code to reproject the result
 * @param deleteOutputData true to delete the file if exists
 * @return
 */
def saveOutputFiles(def h2gis_datasource, def results, def outputFiles, def outputFolder, def outputSRID, def reproject, def deleteOutputData) {
    outputFiles.each {
        //Save indicators
        if (it == "building_indicators") {
            saveTableAsGeojson(results.building_indicators, "${outputFolder.getAbsolutePath() + File.separator + "building_indicators"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "block_indicators") {
            saveTableAsGeojson(results.block_indicators, "${outputFolder.getAbsolutePath() + File.separator + "block_indicators"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "rsu_indicators") {
            saveTableAsGeojson(results.rsu_indicators, "${outputFolder.getAbsolutePath() + File.separator + "rsu_indicators"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "rsu_lcz") {
            saveTableAsGeojson(results.rsu_lcz, "${outputFolder.getAbsolutePath() + File.separator + "rsu_lcz"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "zones") {
            saveTableAsGeojson(results.zone, "${outputFolder.getAbsolutePath() + File.separator + "zones"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        }
        //Save input GIS tables
        else if (it == "building") {
            saveTableAsGeojson(results.buildingTableName, "${outputFolder.getAbsolutePath() + File.separator + "building"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "road") {
            saveTableAsGeojson(results.roadTableName, "${outputFolder.getAbsolutePath() + File.separator + "road"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "rail") {
            saveTableAsGeojson(results.railTableName, "${outputFolder.getAbsolutePath() + File.separator + "rail"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        }
        if (it == "water") {
            saveTableAsGeojson(results.hydrographicTableName, "${outputFolder.getAbsolutePath() + File.separator + "water"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "vegetation") {
            saveTableAsGeojson(results.vegetationTableName, "${outputFolder.getAbsolutePath() + File.separator + "vegetation"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "impervious") {
            saveTableAsGeojson(results.imperviousTableName, "${outputFolder.getAbsolutePath() + File.separator + "impervious"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "urban_areas") {
            saveTableAsGeojson(results.urbanAreasTableName, "${outputFolder.getAbsolutePath() + File.separator + "urban_areas"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "rsu_utrf_area") {
            saveTableAsGeojson(results.rsu_utrf_area, "${outputFolder.getAbsolutePath() + File.separator + "rsu_utrf_area"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "rsu_utrf_floor_area") {
            saveTableAsGeojson(results.rsu_utrf_floor_area, "${outputFolder.getAbsolutePath() + File.separator + "rsu_utrf_floor_area"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "building_utrf") {
            saveTableAsGeojson(results.building_utrf, "${outputFolder.getAbsolutePath() + File.separator + "building_utrf"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "grid_indicators") {
            saveTableAsGeojson(results.grid_indicators, "${outputFolder.getAbsolutePath() + File.separator + "grid_indicators"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "road_traffic") {
            saveTableAsGeojson(results.roadTrafficTableName, "${outputFolder.getAbsolutePath() + File.separator + "road_traffic"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        } else if (it == "population") {
            saveTableAsGeojson(results.population, "${outputFolder.getAbsolutePath() + File.separator + "population"}.geojson", h2gis_datasource, outputSRID, reproject, deleteOutputData)
        }
    }
}

/**
 * Method to save a table into a geojson file
 * @param outputTable name of the table to export
 * @param filePath path to save the table
 * @param h2gis_datasource connection to the database
 * @param outputSRID srid code to reproject the outputTable.
 * @param deleteOutputData true to delete the file if exists
 */
def saveTableAsGeojson(def outputTable, def filePath, def h2gis_datasource, def outputSRID, def reproject, def deleteOutputData) {
    if (outputTable && h2gis_datasource.hasTable(outputTable)) {
        if (!reproject) {
            h2gis_datasource.save(outputTable, filePath, deleteOutputData)
        } else {
            if (h2gis_datasource.getTable(outputTable).getRowCount() > 0) {
                h2gis_datasource.getSpatialTable(outputTable).reproject(outputSRID).save(filePath, deleteOutputData)
            }
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
 * @return
 */
def saveTablesInDatabase(def output_datasource, def h2gis_datasource, def outputTableNames, def h2gis_tables, def id_zone, def inputSRID, def outputSRID, def reproject) {
    Connection con = output_datasource.getConnection()
    con.setAutoCommit(true);

    //Export building indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators, id_zone, h2gis_datasource, h2gis_tables.building_indicators
            , "", inputSRID, outputSRID, reproject)


    //Export block indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.block_indicators, id_zone, h2gis_datasource, h2gis_tables.block_indicators
            , "where ID_RSU IS NOT NULL", inputSRID, outputSRID, reproject)

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

    //Export building_utrf
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_utrf, id_zone, h2gis_datasource, h2gis_tables.building_utrf
            , "", inputSRID, outputSRID, reproject)

    //Export grid_indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.grid_indicators, id_zone, h2gis_datasource, h2gis_tables.grid_indicators
            , "", inputSRID, outputSRID, reproject)

    //Export road_traffic
    indicatorTableBatchExportTable(output_datasource, outputTableNames.road_traffic, id_zone, h2gis_datasource, h2gis_tables.roadTrafficTableName
            , "", inputSRID, outputSRID, reproject)

    //Export zone
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.zones, id_zone, h2gis_datasource, h2gis_tables.zone
            , "", inputSRID, outputSRID, reproject)

    //Export building
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.building, id_zone, h2gis_datasource, h2gis_tables.buildingTableName
            , "", inputSRID, outputSRID, reproject)

    //Export road
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.road, id_zone, h2gis_datasource, h2gis_tables.roadTableName
            , "", inputSRID, outputSRID, reproject)
    //Export rail
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.rail, id_zone, h2gis_datasource, h2gis_tables.railTableName
            , "", inputSRID, outputSRID, reproject)
    //Export vegetation
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.vegetation, id_zone, h2gis_datasource, h2gis_tables.vegetationTableName
            , "", inputSRID, outputSRID, reproject)
    //Export water
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.water, id_zone, h2gis_datasource, h2gis_tables.hydrographicTableName
            , "", inputSRID, outputSRID, reproject)
    //Export impervious
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.impervious, id_zone, h2gis_datasource, h2gis_tables.imperviousTableName
            , "", inputSRID, outputSRID, reproject)

    //Export population table
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.population, id_zone, h2gis_datasource, h2gis_tables.population
            , "", inputSRID, outputSRID, reproject)


    con.setAutoCommit(false);

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
def abstractModelTableBatchExportTable(def output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter, def inputSRID, def outputSRID, def reproject) {
    if (output_table) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            if (output_datasource.hasTable(output_table)) {
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
                        debug "The table $h2gis_table_to_save has been exported into the table $output_table"
                    }
                }
            } else {
                def tmpTable = null
                debug "Start to export the table $h2gis_table_to_save into the table $output_table"
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
                    //Add a GID column
                    output_datasource.execute """ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS gid serial;""".toString()
                    output_datasource.execute("UPDATE $output_table SET id_zone= '$id_zone'".toString());
                    debug "The table $h2gis_table_to_save has been exported into the table $output_table"
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
                    //If the table exists we populate it with the last result
                    info "Start to export the table $h2gis_table_to_save into the table $output_table for the zone $id_zone"
                    int BATCH_MAX_SIZE = 1000;
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
                                //Workaround to update the SRID on resulset
                                output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $outputSRID) USING ST_SetSRID(the_geom,$outputSRID);""".toString()
                            }
                        } else {
                            tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                            if (tmpTable) {
                                //Workaround to update the SRID on resulset
                                output_datasource.execute """ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $outputSRID) USING ST_SetSRID(the_geom,$outputSRID);""".toString()
                            }
                        }
                    } else {
                        if (!reproject) {
                            tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                        } else {
                            tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                            if (tmpTable) {
                                //Because the select query reproject doesn't contain any geometry metadata
                                output_datasource.execute("""ALTER TABLE $output_table 
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID) 
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString())
                            }
                        }
                    }
                    if (tmpTable) {
                        output_datasource.execute("ALTER TABLE $output_table ADD COLUMN IF NOT EXISTS id_zone VARCHAR".toString())
                        output_datasource.execute("UPDATE $output_table SET id_zone= '$id_zone'".toString())
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
def prepareTableOutput(def h2gis_table_to_save, def filter, def inputSRID, def h2gis_datasource, def output_table, def outputSRID, def output_datasource) {
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


/** This process creates the Table from the Abstract model and feed them with the BDTopo data according to some
 * defined rules (i.e. certain types of buildings or vegetation are transformed into a generic type in the abstract
 * model). Then default values are set (or values are corrected) and the quality of the input data is assessed and
 * returned into a statistic table for each layer. For further information, cf. each of the four processes used in the
 * mapper.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param distBuffer The distance (exprimed in meter) used to compute the buffer area around the ZONE
 * @param distance The distance (exprimed in meter) used to compute the extended area around the ZONE
 * @param idZone The Zone id
 * @param tableZoneName The table name in which the zone area is stored
 * @param tableBuildIndifName The table name in which the undifferentiated ("Indifférencié" in french) buildings are stored
 * @param tableBuildIndusName The table name in which the industrial buildings are stored
 * @param tableBuildRemarqName The table name in which the remarkable ("Remarquable" in french) buildings are stored
 * @param tableRoadName The table name in which the roads are stored
 * @param tableRailName The table name in which the rail ways are stored
 * @param tableHydroName The table name in which the hydrographic areas are stored
 * @param tableVegetName The table name in which the vegetation areas are stored
 * @param tableImperviousSportName The table name in which the impervious sport areas are stored
 * @param tableImperviousBuildSurfName The table name in which the impervious surfacic buildings are stored
 * @param tableImperviousRoadSurfName The table name in which the impervious road areas are stored
 * @param tableImperviousActivSurfName The table name in which the impervious activities areas are stored
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param inputSRID to force the SRID of the input data
 *
 * @return outputBuilding Table name in which the (ready to be used in the GeoIndicators part) buildings are stored
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputImpervious Table name in which the (ready to be used in the GeoIndicators part) impervious areas are stored
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 *
 */
IProcess loadAndFormatData() {
    return create {
        title "Extract and transform BD Topo data to Geoclimate model"
        id "prepareData"
        inputs datasource: JdbcDataSource,
                distance: 1000,
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
                tablePiste_AerodromeName: "",
                tableReservoirName: "",
                hLevMin: 3,
                hLevMax: 15,
                hThresholdLev2: 10
        outputs outputBuilding: String, outputRoad: String, outputRail: String, outputHydro: String, outputVeget: String, outputImpervious: String,
                outputZone: String
        run { datasource, distance, tableCommuneName, tableBuildIndifName, tableBuildIndusName, tableBuildRemarqName, tableRoadName, tableRailName,
              tableHydroName, tableVegetName, tableImperviousSportName, tableImperviousBuildSurfName, tableImperviousRoadSurfName, tableImperviousActivSurfName,
              tablePiste_AerodromeName, tableReservoirName, hLevMin, hLevMax, hThresholdLev2 ->

            if (!hLevMin) {
                hLevMin = 3
            }
            if (!hLevMax) {
                hLevMax = 15
            }
            if (!hThresholdLev2) {
                hThresholdLev2 = 10
            }
            if (!datasource) {
                error "The database to store the BD Topo data doesn't exist"
                return
            }

            //Prepare the existing bdtopo data in the local database
            def importPreprocess = BDTopo_V2.InputDataLoading.prepareBDTopoData()
            if (!importPreprocess([datasource                  : datasource,
                                   tableCommuneName            : tableCommuneName,
                                   tableBuildIndifName         : tableBuildIndifName,
                                   tableBuildIndusName         : tableBuildIndusName,
                                   tableBuildRemarqName        : tableBuildRemarqName,
                                   tableRoadName               : tableRoadName,
                                   tableRailName               : tableRailName,
                                   tableHydroName              : tableHydroName,
                                   tableVegetName              : tableVegetName,
                                   tableImperviousSportName    : tableImperviousSportName,
                                   tableImperviousBuildSurfName: tableImperviousBuildSurfName,
                                   tableImperviousRoadSurfName : tableImperviousRoadSurfName,
                                   tableImperviousActivSurfName: tableImperviousActivSurfName,
                                   tablePiste_AerodromeName    : tablePiste_AerodromeName,
                                   tableReservoirName          : tableReservoirName,
                                   distance                    : distance])) {
                error "Cannot prepare the BDTopo data."
                return
            }

            def preprocessTables = importPreprocess.results
            def zoneTable = preprocessTables.outputZoneName
            def finalImpervious = preprocessTables.outputImperviousName

            //Format building
            IProcess processFormatting = BDTopo_V2.InputDataFormatting.formatBuildingLayer()
            processFormatting.execute([datasource                : datasource,
                                       inputTableName            : preprocessTables.outputBuildingName,
                                       inputImpervious           : finalImpervious, h_lev_min: hLevMin, h_lev_max: hLevMax,
                                       hThresholdLev2            : hThresholdLev2,
                                       inputZoneEnvelopeTableName: zoneTable])
            def finalBuildings = processFormatting.results.outputTableName

            //Format roads
            processFormatting = BDTopo_V2.InputDataFormatting.formatRoadLayer()
            processFormatting.execute([datasource                : datasource,
                                       inputTableName            : preprocessTables.outputRoadName,
                                       inputZoneEnvelopeTableName: zoneTable])
            def finalRoads = processFormatting.results.outputTableName

            //Format rails
            processFormatting = BDTopo_V2.InputDataFormatting.formatRailsLayer()
            processFormatting.execute([datasource                : datasource,
                                       inputTableName            : preprocessTables.outputRailName,
                                       inputZoneEnvelopeTableName: zoneTable])
            def finalRails = processFormatting.results.outputTableName


            //Format vegetation
            processFormatting = BDTopo_V2.InputDataFormatting.formatVegetationLayer()
            processFormatting.execute([datasource                : datasource,
                                       inputTableName            : preprocessTables.outputVegetName,
                                       inputZoneEnvelopeTableName: zoneTable])
            def finalVeget = processFormatting.results.outputTableName

            //Format water
            processFormatting = BDTopo_V2.InputDataFormatting.formatHydroLayer()
            processFormatting.execute([datasource                : datasource,
                                       inputTableName            : preprocessTables.outputHydroName,
                                       inputZoneEnvelopeTableName: zoneTable])
            def finalHydro = processFormatting.results.outputTableName

            debug "End of the BD Topo extract transform process."

            [outputBuilding: finalBuildings, outputRoad: finalRoads, outputRail: finalRails, outputHydro: finalHydro,
             outputVeget   : finalVeget, outputImpervious: finalImpervious, outputZone: zoneTable]

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
