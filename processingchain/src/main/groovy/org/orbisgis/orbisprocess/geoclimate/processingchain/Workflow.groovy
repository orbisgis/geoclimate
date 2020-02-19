package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.h2gis.utilities.jts_utils.GeographyUtils
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisanalysis.osm.OSMTools
import org.orbisgis.orbisanalysis.osm.utils.OSMElement
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData

import java.sql.SQLException

@BaseScript ProcessingChain processingChain


/**
 * Load the BDTopo layers from a configuration file and compute the geoclimate indicators.
 * The configuration file is stored in a json format
 *
 * @param configurationFile The path of the configuration file
 *
 * The configuration file supports the following entries
 *
 * {
 * [OPTIONAL ENTRY] "description" :"A description for the configuration file"
 *
 * [OPTIONAL ENTRY] "geoclimatedb" : { // Local H2GIS database used to run the processes
 *                                    // A default db is build when this entry is not specified
 *         "path" : "/tmp/geoclimate_db;AUTO_SERVER=TRUE",
 *         "delete" :false
 *     },
 * [ONE ENTRY REQUIRED]   "input" : {
 *         "folder": {"path" :"path of the folder that contains the BD Topo layers as shapefile",
 *             "id_zones":["56260"]}, //list of insee code, with a comma separator
 *          "database": {
 *             "user": "-",
 *             "password": "-",
 *             "url": "jdbc:postgresql://", //JDBC url to connect with the database
 *             "id_zones":["56220"], //list of insee code, with a comma separator
 *             //List of BDTOPO tables required to compute the geoclimate indicators
 *             //Pattern :  Catalog.Schema.Table
 *             "tables": {
 *                 "iris_ge":"ign_iris.iris_ge_2016",
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
 *                 "surface_activite":"ign_bdtopo_2017.surface_activite"} }
 *             }
 *             ,
 *  [OPTIONAL ENTRY]  "output" :{ //If not ouput is set the results are keep in the local database
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
 *         "indicatorUse": ["LCZ", "URBAN_TYPOLOGY", "TEB"],
 *         "svfSimplified": false,
 *         "prefixName": "",
 *         "mapOfWeights":
 *         {"sky_view_factor": 1,
 *             "aspect_ratio": 1,
 *             "building_surface_fraction": 1,
 *             "impervious_surface_fraction" : 1,
 *             "pervious_surface_fraction": 1,
 *             "height_of_roughness_elements": 1,
 *             "terrain_roughness_class": 1},
 *         "hLevMin": 3,
 *         "hLevMax": 15,
 *         "hThresho2": 10
 *     }
 *     }
 * The parameters entry tag contains all geoclimate chain parameters.
 * When a parameter is not specificied a default value is set.
 *
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
def BDTOPO_V2() {
    create({
        title "Create all geoindicators from BDTopo data"
        inputs configurationFile: String
        outputs outputMessage: String
        run {configurationFile ->
            def configFile
            if(configurationFile) {
                configFile= new File(configurationFile)
                if (!configFile.isFile()) {
                    error "Invalid file parameters"
                    return null
                }
            }else{
                error "The file parameters cannot be null or empty"
                return null
            }
            Map parameters = readJSONParameters(configFile)
            if(parameters){
                info "Reading file parameters from $configFile"
                info parameters.get("description")
                def input = parameters.get("input")
                def output = parameters.get("output")
                //Default H2GIS database properties
                def databaseName =System.getProperty("java.io.tmpdir")+File.separator +"bdtopo_v2"+uuid
                def h2gis_properties = ["databaseName":databaseName, "user": "sa", "password": ""]
                def delete_h2gis = true
                def geoclimatedb = parameters.get("geoclimatedb")
                if(geoclimatedb){
                    def h2gis_path = geoclimatedb.get("path")
                    def delete_h2gis_db = geoclimatedb.get("delete")
                    if(delete_h2gis_db && delete_h2gis_db in Boolean){
                        delete_h2gis = delete_h2gis_db
                    }
                    if(h2gis_path) {
                        h2gis_properties = ["databaseName":h2gis_path, "user": "sa", "password": ""]
                    }
                }
                if(input){
                        def inputDataBase = input.get("database")
                        def inputFolder = input.get("folder")
                        def id_zones = []
                        if(inputFolder && inputDataBase){
                            error "Please set only one input data provider"
                        }
                        else if(inputFolder){
                            def inputFolderPath = inputFolder
                            if(inputFolder in Map){
                                inputFolderPath  = inputFolder.path
                                if(!inputFolderPath){
                                    error "The input folder $inputFolderPath cannot be null or empty"
                                    return null
                                }
                                id_zones = inputFolder.id_zones
                            }
                            if(output) {
                                def geoclimatetTableNames = ["building_indicators",
                                                             "block_indicators",
                                                             "rsu_indicators",
                                                             "rsu_lcz",
                                                             "zones",
                                                             "building",
                                                             "road",
                                                             "rail" ,
                                                             "water",
                                                             "vegetation",
                                                             "impervious"]
                                //Get processing parameters
                                def processing_parameters = extractProcessingParameters(parameters.get("parameters"))
                                def outputDataBase = output.get("database")
                                def outputFolder = output.get("folder")
                                if (outputDataBase && outputFolder) {
                                    def outputFolderProperties = outputFolderProperties(outputFolder)
                                    //Check if we can write in the output folder
                                    def file_outputFolder  = new File(outputFolderProperties.path)
                                    if(!file_outputFolder.canWrite()){
                                        file_outputFolder = null
                                    }
                                    //Check not the conditions for the output database
                                    def outputTableNames = outputDataBase.get("tables")
                                    def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                                    def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size()!=allowedOutputTableNames.size()
                                    if(!allowedOutputTableNames && notSameTableNames){
                                        outputDataBase=null
                                        outputTableNames=null
                                    }
                                    def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                    def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                    if(!output_datasource){
                                        return null
                                    }
                                    def h2gis_datasource = H2GIS.open(h2gis_properties)
                                    id_zones = loadDataFromFolder(inputFolderPath, h2gis_datasource, id_zones)
                                    if(id_zones) {
                                        bdtopo_processing(h2gis_datasource, processing_parameters, id_zones, file_outputFolder, outputFolderProperties.tables, output_datasource, finalOutputTables)
                                        if(delete_h2gis){
                                            h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                            info "The local H2GIS database has been deleted"
                                        }
                                    }else{
                                        error "Cannot load the files from the folder $inputFolder"
                                        return null
                                    }

                                    } else if (outputFolder) {
                                    def outputFolderProperties = outputFolderProperties(outputFolder)
                                    //Check if we can write in the output folder
                                    def file_outputFolder  = new File(outputFolderProperties.path)
                                    if(file_outputFolder.canWrite()){
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        id_zones = loadDataFromFolder(inputFolderPath, h2gis_datasource, id_zones)
                                        if(id_zones) {
                                            bdtopo_processing(h2gis_datasource, processing_parameters, id_zones, file_outputFolder, outputFolderProperties.tables,null, null)
                                            //Delete database
                                            if(delete_h2gis){
                                                h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                                info "The local H2GIS database has been deleted"
                                                return  [outputMessage:"The $id_zones have been processed"]
                                            }
                                        }else{
                                            error "Cannot load the files from the folder $inputFolder"
                                            return null
                                        }
                                    }
                                    else {
                                        error "You don't have permission to write in the folder $outputFolder \n Please check the folder."
                                        return null
                                    }

                                } else if (outputDataBase) {
                                    def outputTableNames = outputDataBase.get("tables")
                                    def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                                    def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size()!=allowedOutputTableNames.size()
                                    if(allowedOutputTableNames && !notSameTableNames){
                                    def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                        def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                        if(!output_datasource){
                                            return null
                                        }
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        id_zones = loadDataFromFolder(inputFolderPath, h2gis_datasource, id_zones)
                                        if(id_zones) {
                                            bdtopo_processing(h2gis_datasource, processing_parameters, id_zones, null,null, output_datasource, finalOutputTables)
                                            if(delete_h2gis){
                                                h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                                info "The local H2GIS database has been deleted"
                                            }
                                        }else{
                                            error "Cannot load the files from the folder $inputFolder"
                                            return null
                                        }
                                    }else{
                                        error "All output table names must be specified in the configuration file."
                                        return null
                                    }
                                } else {
                                    error "Please set at least one output provider"
                                    return null
                                }

                            }
                            else{
                                error "Please set at least one output provider"
                                return null
                            }

                        }
                        else if(inputDataBase){
                            if(output) {
                                def geoclimatetTableNames = ["building_indicators",
                                                             "block_indicators",
                                                             "rsu_indicators",
                                                             "rsu_lcz",
                                                             "zones",
                                                             "building",
                                                             "road",
                                                             "rail" ,
                                                             "water",
                                                             "vegetation",
                                                             "impervious"]
                                //Get processing parameters
                                def processing_parameters = extractProcessingParameters(parameters.get("parameters"))
                                def outputDataBase = output.get("database")
                                def outputFolder = output.get("folder")
                                if (outputDataBase && outputFolder) {
                                    def outputFolderProperties = outputFolderProperties(outputFolder)
                                    //Check if we can write in the output folder
                                    def file_outputFolder  = new File(outputFolderProperties.path)
                                    if(!file_outputFolder.canWrite()){
                                        file_outputFolder = null
                                    }
                                    //Check not the conditions for the output database
                                    def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                                    def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size()!=allowedOutputTableNames.size()
                                    if(!allowedOutputTableNames && notSameTableNames){
                                        outputDataBase=null
                                        outputTableNames=null
                                    }
                                    def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                    def codes = inputDataBase.id_zones
                                    if (codes && codes in Collection) {
                                        def inputTableNames = inputDataBase.tables
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                        if(!output_datasource){
                                            return null
                                        }
                                        for (code in codes) {
                                            if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), code, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                bdtopo_processing(h2gis_datasource, processing_parameters, code, file_outputFolder,outputFolderProperties.tables, output_datasource, finalOutputTables)
                                            }
                                            else{
                                                return null
                                            }
                                        }
                                        if(delete_h2gis){
                                            h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                            info "The local H2GIS database has been deleted"
                                        }

                                    }
                                    else if (codes){
                                        def inputTableNames = inputDataBase.tables
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                        if(!output_datasource){
                                            return null
                                        }
                                        def iris_ge_location = inputTableNames.iris_ge
                                        if (iris_ge_location) {
                                            output_datasource.eachRow("select distinct insee_com from $iris_ge_locationge where $codes group by insee_com ;") { row ->
                                                id_zones << row.insee_com
                                            }
                                            for (id_zone in id_zones) {
                                                if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), id_zone, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                    if(!bdtopo_processing(h2gis_datasource, processing_parameters, id_zone, file_outputFolder, outputFolderProperties.tables,output_datasource, finalOutputTables)){
                                                        error "Cannot execute the geoclimate processing chain on $id_zone"
                                                        return null
                                                    }
                                                }
                                                else {
                                                    return null
                                                }
                                            }
                                            if(delete_h2gis){
                                                h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                                info "The local H2GIS database has been deleted"
                                            }
                                        }
                                        else {
                                            error "Cannot find any commune features from the query $codes"
                                        }
                                    }

                                } else if (outputFolder) {
                                    def outputFolderProperties = outputFolderProperties(outputFolder)
                                    //Check if we can write in the output folder
                                    def file_outputFolder  = new File(outputFolderProperties.path)
                                    if(file_outputFolder.canWrite()) {
                                        def codes = inputDataBase.id_zones
                                        if (codes && codes in Collection) {
                                            def inputTableNames = inputDataBase.tables
                                            def h2gis_datasource = H2GIS.open(h2gis_properties)
                                            for (code in codes) {
                                                if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), code, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                    bdtopo_processing(h2gis_datasource, processing_parameters, code,file_outputFolder, outputFolderProperties.tables,null, null)
                                                } else {
                                                    return null
                                                }
                                            }
                                        }

                                    }
                                    else {
                                        error "You don't have permission to write in the folder $outputFolder. \n Check if the folder exists."
                                        return null
                                    }
                                }
                                else if (outputDataBase) {
                                    def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                                    def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size()!=allowedOutputTableNames.size()
                                    if(allowedOutputTableNames && !notSameTableNames){
                                    def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                        def codes = inputDataBase.id_zones
                                        if (codes && codes in Collection) {
                                            def inputTableNames = inputDataBase.tables
                                            def h2gis_datasource = H2GIS.open(h2gis_properties)
                                            def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                            if(!output_datasource){
                                                return null
                                            }
                                            for (code in codes) {
                                                if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), code, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                    bdtopo_processing(h2gis_datasource, processing_parameters, code, null,null, output_datasource, finalOutputTables)
                                                }
                                                else{
                                                    return null
                                                }
                                            }
                                            if(delete_h2gis){
                                                h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                                info "The local H2GIS database has been deleted"
                                            }
                                        }else if(codes) {
                                            def inputTableNames = inputDataBase.tables
                                            def h2gis_datasource = H2GIS.open(h2gis_properties)
                                            def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                            if(!output_datasource){
                                                return null
                                            }
                                            def iris_ge_location = inputTableNames.iris_ge
                                            if (iris_ge_location) {
                                                output_datasource.eachRow("select distinct insee_com from $iris_ge_locationge where $codes group by insee_com ;") { row ->
                                                    id_zones << row.insee_com
                                                }
                                                for (id_zone in id_zones) {
                                                    if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), id_zone, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                        if(!bdtopo_processing(h2gis_datasource, processing_parameters, id_zone,null, null, output_datasource, finalOutputTables)){
                                                            error "Cannot execute the geoclimate processing chain on $id_zone"
                                                            return null
                                                        }
                                                    }
                                                    else {
                                                        return null
                                                    }
                                                }
                                                if(delete_h2gis){
                                                    h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                                    info "The local H2GIS database has been deleted"
                                                }
                                            }
                                            else {
                                                error "Cannot find any commune features from the query $codes"
                                            }
                                        }

                                    }else{
                                        error "All output table names must be specified in the configuration file."
                                        return null
                                    }

                                }
                                else{
                                    error "Please set at least one output provider"
                                    return null
                                }

                            }
                            else {
                                error "Please set at least one output provider"
                                return null
                            }
                        }
                        else{
                            error "Please set a valid input data provider"
                        }
                }
                else{
                    error "Cannot find any input parameters."
                }

            }
            else{
                error "Empty parameters"
            }
            return  [outputMessage:"The process has been done"]
        }
    })
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
                    "impervious"]
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
 * Read the geoclimatedb parameters and return the properties to build a local H2GIS database
 * @param geoclimatedb parameters from the json file
 * @return connection properties
 */
def readH2GISProperties(def geoclimatedb){
    //Default H2GIS database properties
    def databaseName =System.getProperty("java.io.tmpdir")+File.separator +"bdtopo_v2"+uuid
    Map h2gis_properties = ["databaseName":databaseName, "user": "sa", "password": ""]
    def db_delete = true
    if(geoclimatedb){
        def h2gis_path = geoclimatedb.get("path")
        def delete_h2gis_db = geoclimatedb.get("delete")
        if(delete_h2gis_db && delete_h2gis_db in Boolean){
            db_delete = delete_h2gis_db
        }
        if(h2gis_path) {
            h2gis_properties = ["databaseName":h2gis_path, "user": "sa", "password": ""]
        }
    }
    return ["db_properties" : h2gis_properties,"delete":db_delete]
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
 * Load the required tables stored in a database
 *
 * @param inputDatasource database where the tables are
 * @return true is succeed, false otherwise
 */
def loadDataFromDatasource(def input_database_properties, def code, def distance, def inputTableNames,  H2GIS h2gis_datasource) {
    def asValues = inputTableNames.every { it.key in ["iris_ge", "bati_indifferencie", "bati_industriel", "bati_remarquable", "route",
                                                      "troncon_voie_ferree", "surface_eau", "zone_vegetation", "terrain_sport", "construction_surfacique", "" +
                                                              "surface_route", "surface_activite"] && it.value }
    def notSameTableNames = inputTableNames.groupBy { it.value }.size()!=inputTableNames.size()

    if (asValues && !notSameTableNames) {
        def iris_ge_location = inputTableNames.iris_ge
        input_database_properties =updateDriverURL(input_database_properties)
        String inputTableName = "(SELECT THE_GEOM, INSEE_COM FROM $iris_ge_location WHERE insee_com=''$code'')"
        String outputTableName = "IRIS_GE"
        info "Loading in the H2GIS database $outputTableName"
        h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)
        def count = h2gis_datasource.getTable(outputTableName).rowCount
        if (count > 0) {
            //Compute the envelope of the extracted area to extract the thematic tables
            def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), 1000) AS THE_GEOM FROM $outputTableName").THE_GEOM
            int srid = geomToExtract.getSRID()

            //Extract bati_indifferencie
            inputTableName = "(SELECT ID, THE_GEOM, HAUTEUR FROM ${inputTableNames.bati_indifferencie}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "BATI_INDIFFERENCIE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract bati_industriel
            inputTableName = "(SELECT ID, THE_GEOM, NATURE, HAUTEUR FROM ${inputTableNames.bati_industriel}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "BATI_INDUSTRIEL"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract bati_remarquable
            inputTableName = "(SELECT ID, THE_GEOM, NATURE, HAUTEUR FROM ${inputTableNames.bati_remarquable}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "BATI_REMARQUABLE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract route
            inputTableName = "(SELECT ID, THE_GEOM, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTableNames.route}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "ROUTE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract troncon_voie_ferree
            inputTableName = "(SELECT ID, THE_GEOM, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTableNames.troncon_voie_ferree}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "TRONCON_VOIE_FERREE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract surface_eau
            inputTableName = "(SELECT ID, THE_GEOM FROM ${inputTableNames.surface_eau}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "SURFACE_EAU"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract zone_vegetation
            inputTableName = "(SELECT ID, THE_GEOM, NATURE  FROM ${inputTableNames.zone_vegetation}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "ZONE_VEGETATION"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract terrain_sport
            inputTableName = "(SELECT ID, THE_GEOM, NATURE  FROM ${inputTableNames.terrain_sport}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY) AND NATURE=''Piste de sport'')"
            outputTableName = "TERRAIN_SPORT"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract construction_surfacique
            inputTableName = "(SELECT ID, THE_GEOM, NATURE  FROM ${inputTableNames.construction_surfacique}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY) AND (NATURE=''Barrage'' OR NATURE=''Ecluse'' OR NATURE=''Escalier''))"
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract surface_route
            inputTableName = "(SELECT ID, THE_GEOM  FROM ${inputTableNames.surface_route}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY))"
            outputTableName = "SURFACE_ROUTE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            //Extract surface_activite
            inputTableName = "(SELECT ID, THE_GEOM, CATEGORIE  FROM ${inputTableNames.surface_activite}  WHERE the_geom && ''SRID=$srid;$geomToExtract''::GEOMETRY AND ST_INTERSECTS(the_geom, ''SRID=$srid;$geomToExtract''::GEOMETRY) AND (CATEGORIE=''Administratif'' OR CATEGORIE=''Enseignement'' OR CATEGORIE=''Santé''))"
            outputTableName = "SURFACE_ACTIVITE"
            info "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.load(input_database_properties, inputTableName, outputTableName, true)

            return true

        } else {
            error "Cannot find any commune with the insee code : $code"
            return null
        }
    } else {
        error "All table names must be specified in the configuration file."
        return null
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
                if (h2gis_datasource.firstRow("select count(*) as COUNT_ZONES FROM IRIS_GE where insee_com in ('${id_zones.join(",")}')").COUNT_ZONES > 0) {
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
    def defaultParameters = [distance: 1000,indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"],
    svfSimplified:false, prefixName: "",
    mapOfWeights : ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                    "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                    "height_of_roughness_elements": 1, "terrain_roughness_class": 1],
    hLevMin : 3, hLevMax: 15, hThresholdLev2: 10]
    if(processing_parameters){
        def distanceP =  processing_parameters.distance
        if(distanceP && distanceP in Number){
            defaultParameters.distance = distanceP
        }
        def indicatorUseP = processing_parameters.indicatorUse
        if(indicatorUseP && indicatorUseP in Map){
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
            defaultParameters.mapOfWeights = mapOfWeightsP
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

        return defaultParameters
    }
    else{
        return defaultParameters
    }
}


/**
 * Utility method to run commmune by commune the geoclimate chain and save the result in a folder or/and
 * in a database
 *
 * @param h2gis_datasource the local H2GIS database
 * @param processing_parameters the geoclimate chain parameters
 * @param id_zones a list of id zones to process
 * @param outputFolder folder to store the files, null otherwise
 * @param outputFiles the name of the tables that will be saved
 * @param output_datasource a connexion to a database to save the results
 * @param outputTableNames the name of the tables in the output_datasource to save the results
 * @return
 */
def bdtopo_processing(def  h2gis_datasource, def processing_parameters,def id_zones, def outputFolder, def outputFiles, def output_datasource, def outputTableNames ){
     def  srid =  h2gis_datasource.getSpatialTable("IRIS_GE").srid
     if(output_datasource){
         if(!createOutputTables(output_datasource,  outputTableNames, srid)){
             error "Cannot prepare the output tables to save the result"
             return null
         }
     }
    if(!(id_zones in Collection)){
        id_zones = [id_zones]
    }
    int nbAreas = id_zones.size();

    //Let's run the BDTopo process for each insee code
    def prepareBDTopoData = ProcessingChain.PrepareBDTopo.prepareBDTopo()
    def geoIndicatorsComputed = false
    info "$nbAreas communes will be processed"
    id_zones.eachWithIndex { id_zone, index->
        info "Starting to process insee id_zone $id_zone"
        if(prepareBDTopoData.execute([datasource                 : h2gis_datasource,
                                      tableIrisName              : 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                      tableBuildIndusName        : 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                      tableRoadName              : 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                      tableHydroName             : 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                      tableImperviousSportName   : 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                      tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                      distBuffer                 : 500, expand: processing_parameters.distance, idZone: id_zone,
                                      hLevMin                    : processing_parameters.hLevMin,
                                      hLevMax                    : processing_parameters.hLevMax, hThresholdLev2: processing_parameters.hThresholdLev2
        ])){

            String buildingTableName = prepareBDTopoData.getResults().outputBuilding

            String roadTableName = prepareBDTopoData.getResults().outputRoad

            String railTableName = prepareBDTopoData.getResults().outputRail

            String hydrographicTableName = prepareBDTopoData.getResults().outputHydro

            String vegetationTableName = prepareBDTopoData.getResults().outputVeget

            String zoneTableName = prepareBDTopoData.getResults().outputZone

            String imperviousTableName = prepareBDTopoData.getResults().outputImpervious

            IProcess geoIndicators = GeoIndicators()
            if (!geoIndicators.execute( datasource          : h2gis_datasource,           zoneTable       : zoneTableName,
                    buildingTable       : buildingTableName,    roadTable       : roadTableName,
                    railTable           : railTableName,        vegetationTable : vegetationTableName,
                    hydrographicTable   : hydrographicTableName,indicatorUse    : processing_parameters.indicatorUse,
                    svfSimplified       : processing_parameters.svfSimplified,        prefixName      : "${processing_parameters.prefixName}zone_$id_zone",
                    mapOfWeights        : processing_parameters.mapOfWeights)) {
                error "Cannot build the geoindicators for the area with the INSEE id_zone : $id_zone"
                geoIndicatorsComputed = false
            }else{
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
            if(outputFolder && geoIndicatorsComputed && outputFiles) {
                saveOutputFiles(h2gis_datasource, id_zone, results, outputFiles, outputFolder, "bdtopo_v2_")
            }
            if(output_datasource && geoIndicatorsComputed){
                saveTablesInDatabase(output_datasource,h2gis_datasource, outputTableNames, results, id_zone, srid, true)

            }
                info "${id_zone} has been processed"
        }
        info "Number of areas processed ${index+1} on $nbAreas"
    }

}

/**
 * Save the geoclimate tables into geojson files
 * @param id_zone the id of the zone
 * @param results a list of tables computed by geoclimate
 * @param ouputFolder the ouput folder
 * @return
 */
def saveOutputFiles(def h2gis_datasource, def id_zone, def results, def outputFiles, def ouputFolder, def subFolderName){
    //Create a subfolder to store each results
    def folderName = id_zone in Map?id_zone.join("_"):id_zone
    def subFolder = new File(ouputFolder.getAbsolutePath()+File.separator+subFolderName+folderName)
    if(!subFolder.exists()){
        subFolder.mkdir()
    }
    outputFiles.each{
        //Save indicators
        if(it.equals("building_indicators")){
            h2gis_datasource.save(results.outputTableBuildingIndicators, "${subFolder.getAbsolutePath()+File.separator+results.outputTableBuildingIndicators}.geojson")
            info "${results.outputTableBuildingIndicators} has been saved."
        }
        else if(it.equals("block_indicators")){
            h2gis_datasource.save(results.outputTableBlockIndicators, "${subFolder.getAbsolutePath()+File.separator+results.outputTableBlockIndicators}.geojson")
            info "${results.outputTableBlockIndicators} has been saved."
        }
        else  if(it.equals("rsu_indicators")){
            h2gis_datasource.save(results.outputTableRsuIndicators, "${subFolder.getAbsolutePath()+File.separator+results.outputTableRsuIndicators}.geojson")
            info "${results.outputTableRsuIndicators} has been saved."
        }
        else  if(it.equals("rsu_lcz")){
            h2gis_datasource.save(results.outputTableRsuLcz, "${subFolder.getAbsolutePath()+File.separator+results.outputTableRsuLcz}.geojson")
            info "${results.outputTableRsuLcz} has been saved."
        }
        else  if(it.equals("zones")){
            h2gis_datasource.save(results.outputTableZone, "${subFolder.getAbsolutePath()+File.separator+results.outputTableZone}.geojson")
            info "${results.outputTableZone} has been saved."
        }

        //Save input GIS tables
        else  if(it.equals("building")){
            h2gis_datasource.save(results.buildingTableName, "${subFolder.getAbsolutePath()+File.separator+results.buildingTableName}.geojson")
            info "${results.buildingTableName} has been saved."
        }
        else if(it.equals("road")){
            h2gis_datasource.save(results.roadTableName, "${subFolder.getAbsolutePath()+File.separator+results.roadTableName}.geojson")
            info "${results.roadTableName} has been saved."
        }
        else if(it.equals("rail")){
            h2gis_datasource.save(results.railTableName, "${subFolder.getAbsolutePath()+File.separator+results.railTableName}.geojson")
            info "${results.railTableName} has been saved."
        }
        if(it.equals("water")){
            h2gis_datasource.save(results.hydrographicTableName, "${subFolder.getAbsolutePath()+File.separator+results.hydrographicTableName}.geojson")
            info "${results.hydrographicTableName} has been saved."
        }
        else if(it.equals("vegetation")){
            h2gis_datasource.save(results.vegetationTableName, "${subFolder.getAbsolutePath()+File.separator+results.vegetationTableName}.geojson")
            info "${results.vegetationTableName} has been saved."
        }
        else if(it.equals("impervious")){
            h2gis_datasource.save(results.imperviousTableName, "${subFolder.getAbsolutePath()+File.separator+results.imperviousTableName}.geojson")
            info "${results.imperviousTableName} has been saved."
        }
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


    if (!output_datasource.hasTable(output_block_indicators)){
        output_datasource.execute """CREATE TABLE $output_block_indicators (
        ID_ZONE VARCHAR,
        ID_BLOCK INTEGER, THE_GEOM GEOMETRY(GEOMETRY,$srid),
        ID_RSU INTEGER, AREA DOUBLE PRECISION,
        FLOOR_AREA DOUBLE PRECISION,VOLUME DOUBLE PRECISION,
        HOLE_AREA_DENSITY DOUBLE PRECISION,MAIN_BUILDING_DIRECTION DECIMAL(4,1),
        BUILDING_DIRECTION_INEQUALITY DOUBLE PRECISION,BUILDING_DIRECTION_UNIQUENESS DOUBLE PRECISION,
        CLOSINGNESS DOUBLE PRECISION, NET_COMPACTNESS DOUBLE PRECISION,
        AVG_HEIGHT_ROOF_AREA_WEIGHTED DOUBLE PRECISION,
        STD_HEIGHT_ROOF_AREA_WEIGHTED DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_${output_block_indicators}_id_zone ON $output_block_indicators (ID_ZONE);"""
    }
    else{
        def outputTableSRID = output_datasource.getSpatialTable(output_block_indicators).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_block_indicators is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_block_indicators (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_block_indicators WHERE ID_ZONE= 'geoclimate';"""
    }

    if (!output_datasource.hasTable(output_building_indicators)){
        output_datasource.execute """
        CREATE TABLE $output_building_indicators (
                THE_GEOM GEOMETRY(GEOMETRY,$srid),
                ID_BUILD INTEGER,
                ID_SOURCE VARCHAR(24),
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
    else{
        def outputTableSRID = output_datasource.getSpatialTable(output_building_indicators).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_building_indicators is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_building_indicators (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_building_indicators WHERE ID_ZONE= 'geoclimate';"""
    }

    if (!output_datasource.hasTable(output_rsu_indicators)){
        output_datasource.execute """
    CREATE TABLE $output_rsu_indicators (
            ID_ZONE VARCHAR,
            ID_RSU INTEGER,
            THE_GEOM GEOMETRY(GEOMETRY,$srid),
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
            BUILDING_AREA_FRACTION DOUBLE PRECISION,
            BUILDING_VOLUME_DENSITY DOUBLE PRECISION,
            AVG_VOLUME DOUBLE PRECISION,
            AVG_NUMBER_BUILDING_NEIGHBOR DOUBLE PRECISION,
            BUILDING_FLOOR_AREA_DENSITY DOUBLE PRECISION,
            AVG_MINIMUM_BUILDING_SPACING DOUBLE PRECISION,
            BUILDING_NUMBER_DENSITY DOUBLE PRECISION,
            GROUND_ROAD_FRACTION DOUBLE PRECISION,
            OVERGROUND_ROAD_FRACTION DOUBLE PRECISION,
            WATER_FRACTION DOUBLE PRECISION,
            LOW_VEGETATION_FRACTION DOUBLE PRECISION,
            HIGH_VEGETATION_FRACTION DOUBLE PRECISION,
            ALL_VEGETATION_FRACTION DOUBLE PRECISION,
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
            ASPECT_RATIO DOUBLE PRECISION,
            GROUND_SKY_VIEW_FACTOR DOUBLE PRECISION,
            PERVIOUS_FRACTION DOUBLE PRECISION,
            IMPERVIOUS_FRACTION DOUBLE PRECISION,
            EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH DOUBLE PRECISION,
            EFFECTIVE_TERRAIN_ROUGHNESS_CLASS INTEGER,
            MAIN_BUILDING_DIRECTION INTEGER,
            BUILDING_DIRECTION_INEQUALITY DOUBLE PRECISION,
            BUILDING_DIRECTION_UNIQUENESS DOUBLE PRECISION
    );    
        CREATE INDEX IF NOT EXISTS idx_${output_rsu_indicators}_id_zone ON $output_rsu_indicators (ID_ZONE);
        """
    } else{
        def outputTableSRID = output_datasource.getSpatialTable(output_rsu_indicators).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_rsu_indicators is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_rsu_indicators (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_rsu_indicators WHERE ID_ZONE= 'geoclimate';"""
    }

    if (!output_datasource.hasTable(output_rsu_lcz)){
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
    }else{
        def outputTableSRID = output_datasource.getSpatialTable(output_rsu_lcz).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_rsu_lcz is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_rsu_lcz (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_rsu_lcz WHERE ID_ZONE= 'geoclimate';"""
    }

    if (!output_datasource.hasTable(output_zones)){
        output_datasource.execute """
        CREATE TABLE $output_zones(
                ID_ZONE VARCHAR,
                THE_GEOM GEOMETRY(GEOMETRY,$srid)
        );
        CREATE INDEX IF NOT EXISTS idx_${output_zones}_id_zone ON $output_zones (ID_ZONE);
        """
    }else{
        def outputTableSRID = output_datasource.getSpatialTable(output_zones).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_zones is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_zones (ID_ZONE) VALUES('geoclimate');
        DELETE from $output_zones WHERE ID_ZONE= 'geoclimate';"""
    }

    if (!output_datasource.hasTable(output_building)){
        output_datasource.execute """CREATE TABLE $output_building  (THE_GEOM GEOMETRY(POLYGON, $srid), 
        id_build serial, ID_SOURCE VARCHAR, HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT,
        NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER);
        CREATE INDEX IF NOT EXISTS idx_${output_building}_id_source ON $output_building (ID_SOURCE);"""
    }
    else{
        def outputTableSRID = output_datasource.getSpatialTable(output_building).srid
        if(outputTableSRID!=srid){
            error "The SRID of the output table ($outputTableSRID) $output_building is different than the srid of the result table ($srid)"
            return null
        }
        //Test if we can write in the database
        output_datasource.execute """INSERT INTO $output_building (ID_SOURCE) VALUES('geoclimate');
        DELETE from $output_building WHERE ID_SOURCE= 'geoclimate';"""
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
 * @param isBDTopo true if the data comes from BDTOPO
 * @return
 */
def saveTablesInDatabase(def output_datasource, def h2gis_datasource, def outputTableNames, def h2gis_tables, def id_zone, def srid, def isBDTopo){
    //Export building indicators
    if(isBDTopo){
        indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators,id_zone, srid,h2gis_datasource, h2gis_tables.outputTableBuildingIndicators
                , 1000, "where id_zone!='outside'")
    }
    else{
        indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators,id_zone, srid,h2gis_datasource, h2gis_tables.outputTableBuildingIndicators
                , 1000, "")
    }
    //Export block indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.block_indicators,id_zone,srid, h2gis_datasource, h2gis_tables.outputTableBlockIndicators
            , 1000, "where ID_RSU IS NOT NULL")

    //Export rsu indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_indicators,id_zone,srid, h2gis_datasource, h2gis_tables.outputTableRsuIndicators
            , 1000, "")

    //Export rsu lcz
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_lcz,id_zone, srid,h2gis_datasource, h2gis_tables.outputTableRsuLcz
            , 1000, "")

    //Export zone
    indicatorTableBatchExportTable(output_datasource, outputTableNames.zones,id_zone,srid, h2gis_datasource, h2gis_tables.outputTableZone
            , 1, "")

    //Export building
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.building,srid, h2gis_datasource, h2gis_tables.buildingTableName
            , 1000, "")

    //Export road
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.road,srid, h2gis_datasource, h2gis_tables.roadTableName
            , 1000, "")
    //Export rail
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.rail,srid, h2gis_datasource, h2gis_tables.railTableName
            , 1000, "")
    //Export vegetation
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.vegetation,srid, h2gis_datasource, h2gis_tables.vegetationTableName
            , 1000, "")
    //Export water
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.water,srid, h2gis_datasource, h2gis_tables.hydrographicTableName
            , 1000, "")
    //Export impervious
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.impervious,srid, h2gis_datasource, h2gis_tables.imperviousTableName
            , 1000, "")
}


/**
 * Generic method to save the abstract model tables prepared in H2GIS to another database
 * @param output_datasource connexion to a database
 * @param output_table name of the output table
 * @param srid srid to reproject
 * @param h2gis_datasource local H2GIS database
 * @param h2gis_table_to_save name of the H2GIS table to save
 * @param batchSize size of the batch
 * @param filter to limit the data from H2GIS
 * @return
 */
def abstractModelTableBatchExportTable(def output_datasource, def output_table, def srid, def h2gis_datasource, h2gis_table_to_save, def batchSize, def filter){
    if(output_table) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            def sridTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).srid
            info "Start to export the table $h2gis_table_to_save into the table $output_table"
            def columnTypes = h2gis_datasource.getTable(h2gis_table_to_save).getColumnsTypes()
            columnTypes.put("ID_SOURCE", "VARCHAR")
            def insertValues = columnTypes.collect { it ->
                if (it.value == "GEOMETRY") {
                    if (sridTable != srid) {
                        "${!it.key ? null : "ST_TRANSFORM('SRID=$sridTable;" + '$' + "${it.key}'::GEOMETRY,  $srid)"}"
                    } else {
                        "${!it.key ? null : "'SRID=$sridTable;" + '$' + "${it.key}'::GEOMETRY"}"
                    }
                } else if (it.value == "VARCHAR") {
                    "${!it.key ? null : "'" + '$' + "${it.key}'"}"
                } else {
                    "" + '$' + "${it.key}"
                }
            }

            def id_source = '$'+ "ID_SOURCE";
            def deleteTemplate = "DELETE from $output_table WHERE ID_SOURCE= '${id_source}';"
            def engine = new groovy.text.SimpleTemplateEngine()
            def deleteTemplateEG = engine.createTemplate(deleteTemplate)
            h2gis_datasource.withTransaction {
                output_datasource.withBatch(batchSize) { stmt ->
                    h2gis_datasource.eachRow("SELECT ID_SOURCE FROM ${h2gis_table_to_save} ${filter}") { row ->
                        def keyValues = row.toRowResult()
                        try {
                            stmt.addBatch(deleteTemplateEG.make(keyValues).toString())
                        }catch (SQLException e){
                            error e.getNextException()
                        }
                    }
                }
            }

            def insertTemplate = " INSERT INTO $output_table (${columnTypes.keySet().join(',')}) VALUES (${insertValues.join(',')})"
            def template = engine.createTemplate(insertTemplate)
            h2gis_datasource.withTransaction {
                output_datasource.withBatch(batchSize) { stmt ->
                    h2gis_datasource.eachRow("SELECT * FROM ${h2gis_table_to_save} ${filter}") { row ->
                        def keyValues = row.toRowResult()
                        try {
                            stmt.addBatch(template.make(keyValues).toString())
                        }catch (SQLException e){
                            error e.getNextException()
                        }
                    }
                }
            }
            info "The table $h2gis_table_to_save has been exported into the table $output_table"
        }
    }
}

/**
 * Generic method to save the indicator tables prepared in H2GIS to another database
 * @param output_datasource connexion to a database
 * @param output_table name of the output table
 * @param id_zone id of the zone
 * @param srid srid to reproject
 * @param h2gis_datasource local H2GIS database
 * @param h2gis_table_to_save name of the H2GIS table to save
 * @param batchSize size of the batch
 * @param filter to limit the data from H2GIS
 * @return
 */
def indicatorTableBatchExportTable(def output_datasource, def output_table, def id_zone, def srid, def h2gis_datasource, h2gis_table_to_save, def batchSize, def filter){
    if(h2gis_datasource.hasTable(h2gis_table_to_save)) {
        def sridTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).srid
        info "Start to export the table $h2gis_table_to_save into the table $output_table"
        def columnTypes = h2gis_datasource.getTable(h2gis_table_to_save).getColumnsTypes()
        columnTypes.put("ID_ZONE", "VARCHAR")
        def insertValues = columnTypes.collect { it ->
            if (it.value == "GEOMETRY") {
                if(sridTable!=srid){
                    "${!it.key ? null : "ST_TRANSFORM('SRID=$sridTable;" + '$' + "${it.key}'::GEOMETRY,  $srid)"}"
                }else{
                    "${!it.key ? null : "'SRID=$sridTable;" + '$' + "${it.key}'::GEOMETRY"}"
                }
            } else if (it.value == "VARCHAR") {
                "${!it.key ? null : "'" + '$' + "${it.key}'"}"
            } else {
                "" + '$' + "${it.key}"
            }
        }
        def insertTemplate = "INSERT INTO $output_table (${columnTypes.keySet().join(',')}) VALUES (${insertValues.join(',')})"
        def engine = new groovy.text.SimpleTemplateEngine()
        def template = engine.createTemplate(insertTemplate)
        // Delete former indicators if the zone has already been processed
        output_datasource.execute "DELETE from $output_table WHERE ID_ZONE= '$id_zone';"
        //Dump the indicators in the database
        h2gis_datasource.withTransaction {
            output_datasource.withBatch(batchSize) { stmt ->
                h2gis_datasource.eachRow("SELECT * FROM ${h2gis_table_to_save} ${filter}") { row ->
                    def keyValues = row.toRowResult()
                    keyValues.put("ID_ZONE", id_zone)
                    stmt.addBatch(template.make(keyValues).toString())
                }
            }
        }
        info "The table $h2gis_table_to_save has been exported into the table $output_table"
    }
}
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
 *  *         "path" : "/tmp/geoclimate_db;AUTO_SERVER=TRUE",
 *  *         "delete" :false
 *  *     },
 *  * [REQUIRED]   "input" : {
 *  *            "osm" : ["filter"] // OSM filter to extract the data. Can be a place name supported by nominatim
 *                                  // e.g "osm" : ["oran", "plourivo"]
 *                                  // or bbox expressed as "osm" : [[38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417]]
 *  *             }
 *  *             ,
 *  *  [OPTIONAL ENTRY]  "output" :{ //If not ouput is set the results are keep in the local database
 *  *             "folder" : "/tmp/myResultFolder" //tmp folder to store the computed layers in a geojson format,
 *  *             "database": { //database parameters to store the computed layers. Note that OSM data is stored in WGS84
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
 *  *             "terrain_roughness_class": 1},
 *  *         "hLevMin": 3,
 *  *         "hLevMax": 15,
 *  *         "hThresho2": 10
 *  *     }
 *  *     }
 *  The parameters entry tag contains all geoclimate chain parameters.
 *  When a parameter is not specificied a default value is set.
 *
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
def OSM() {
    create({
        title "Create all Geoindicators from OSM data"
        inputs configurationFile: ""
        outputs outputMessage: String
        run { configurationFile ->
            def configFile
            if(configurationFile) {
                configFile= new File(configurationFile)
                if (!configFile.isFile()) {
                    error "Invalid file parameters"
                    return null
                }
            }else{
                error "The file parameters cannot be null or empty"
                return null
            }
            Map parameters = readJSONParameters(configFile)
            if(parameters){
                info "Reading file parameters from $configFile"
                info parameters.get("description")
                def input = parameters.get("input")
                def output = parameters.get("output")
                //Default H2GIS database properties
                def databaseName =System.getProperty("java.io.tmpdir")+File.separator +"bdtopo_v2"+uuid
                def h2gis_properties = ["databaseName":databaseName, "user": "sa", "password": ""]
                def delete_h2gis = true
                def geoclimatedb = parameters.get("geoclimatedb")
                if(geoclimatedb){
                    def h2gis_path = geoclimatedb.get("path")
                    def delete_h2gis_db = geoclimatedb.get("delete")
                    if((delete_h2gis_db && delete_h2gis_db) in Boolean){
                        delete_h2gis = delete_h2gis_db
                    }
                    if(h2gis_path) {
                        h2gis_properties = ["databaseName":h2gis_path, "user": "sa", "password": ""]
                    }
                }
                if(input) {
                    def osmFilters = input.get("osm")
                    if (!osmFilters) {
                        error "Please set at least one OSM filter. e.g osm : ['A place name']"
                        return null
                    }

                    if(output) {
                        def geoclimatetTableNames = ["building_indicators",
                                                     "block_indicators",
                                                     "rsu_indicators",
                                                     "rsu_lcz",
                                                     "zones",
                                                     "building",
                                                     "road",
                                                     "rail" ,
                                                     "water",
                                                     "vegetation",
                                                     "impervious"]
                        //Get processing parameters
                        def processing_parameters = extractProcessingParameters(parameters.get("parameters"))
                        def outputDataBase = output.get("database")
                        def outputFolder = output.get("folder")
                        if (outputDataBase && outputFolder) {
                            def outputFolderProperties = outputFolderProperties(outputFolder)
                            //Check if we can write in the output folder
                            def file_outputFolder  = new File(outputFolderProperties.path)
                            if(!file_outputFolder.canWrite()){
                                file_outputFolder = null
                            }
                            //Check not the conditions for the output database
                            def outputTableNames = outputDataBase.get("tables")
                            def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                            def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size()!=allowedOutputTableNames.size()
                            if(!allowedOutputTableNames && notSameTableNames){
                                outputDataBase=null
                                outputTableNames=null
                            }
                            def finalOutputTables = outputTableNames.subMap(allowedTableNames)
                            def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                            if(!output_datasource){
                                return null
                            }
                            def h2gis_datasource = H2GIS.open(h2gis_properties)
                            if(osmFilters && osmFilters in Collection) {
                                def osmprocessing =  osm_processing()
                                if(!osmprocessing.execute(h2gis_datasource:h2gis_datasource,
                                        processing_parameters:processing_parameters,
                                        id_zones :osmFilters, outputFolder:file_outputFolder,ouputTableFiles :outputFolderProperties.tables,
                                        output_datasource:output_datasource, outputTableNames :finalOutputTables)){
                                    return null
                                }
                                if(delete_h2gis){
                                    h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                    info "The local H2GIS database has been deleted"
                                }
                            }else{
                                error "Cannot find any OSM filters"
                                return null
                            }

                        } else if (outputFolder) {
                            //Check if we can write in the output folder
                            def outputFolderProperties = outputFolderProperties(outputFolder)
                            def file_outputFolder  = new File(outputFolderProperties.path)
                            if(file_outputFolder.canWrite()){
                                def h2gis_datasource = H2GIS.open(h2gis_properties)
                                if(osmFilters && osmFilters in Collection) {
                                    def osmprocessing =  osm_processing()
                                    if(!osmprocessing.execute(h2gis_datasource:h2gis_datasource,
                                            processing_parameters:processing_parameters,
                                            id_zones :osmFilters, outputFolder:file_outputFolder,ouputTableFiles :outputFolderProperties.tables,
                                            output_datasource:null, outputTableNames :null)){
                                        return null
                                    }
                                    //Delete database
                                    if(delete_h2gis){
                                        h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                        info "The local H2GIS database has been deleted"
                                        return  [outputMessage:"The ${osmFilters.join(",")} have been processed"]
                                    }
                                }else{
                                    error "Cannot load the files from the folder $inputFolder"
                                    return null
                                }
                            }
                            else {
                                error "You don't have permission to write in the folder $outputFolder \n Please check the folder."
                                return null
                            }

                        } else if (outputDataBase) {
                            def outputTableNames = outputDataBase.get("tables")
                            def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                            def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size()!=allowedOutputTableNames.size()
                            if(allowedOutputTableNames && !notSameTableNames){
                            def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                if(!output_datasource){
                                    return null
                                }
                                def h2gis_datasource = H2GIS.open(h2gis_properties)
                                if(osmFilters && osmFilters in Collection) {
                                    if(!osmprocessing.execute(h2gis_datasource:h2gis_datasource,
                                            processing_parameters:processing_parameters,
                                            id_zones :osmFilters, outputFolder:null,ouputTableFiles :null, output_datasource:output_datasource, outputTableNames :finalOutputTables)){
                                        return null
                                    }
                                    if(delete_h2gis){
                                        h2gis_datasource.execute("DROP ALL OBJECTS DELETE FILES")
                                        info "The local H2GIS database has been deleted"
                                    }
                                }else{
                                    error "Cannot load the files from the folder $inputFolder"
                                    return null
                                }

                            }else{
                                error "All output table names must be specified in the configuration file."
                                return null
                            }
                        } else {
                            error "Please set at least one output provider"
                            return null
                        }

                    }
                    else{
                        error "Please set at least one output provider"
                        return null
                    }

                }
                else{
                        error "Cannot find any input parameter to extract data from Overpass API."

                }
            }
            else{
                error "Empty parameters"
            }

            return  [outputMessage:"The process has been done"]

        }
    })}

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
def osm_processing(){
    create({
        title "Build OSM data and compute the geoindicators"
        inputs h2gis_datasource: JdbcDataSource,  processing_parameters: Map, id_zones: Map,
         outputFolder:"", ouputTableFiles:"", output_datasource:"", outputTableNames:""
        outputs outputMessage: String
        run { h2gis_datasource,  processing_parameters, id_zones,  outputFolder,  ouputTableFiles,  output_datasource,  outputTableNames ->

    int nbAreas = id_zones.size();
    info "$nbAreas osm areas will be processed"
    def geoIndicatorsComputed =false
    id_zones.eachWithIndex { id_zone, index ->
        //Extract the zone table and read its SRID
        def zoneTableNames = extractOSMZone(h2gis_datasource, id_zone, processing_parameters)
        if (zoneTableNames) {
            id_zone = id_zone in Map ? "bbox_" + id_zone.join('_') : id_zone
            def zoneTableName = zoneTableNames.outputZoneTable
            def zoneEnvelopeTableName = zoneTableNames.outputZoneEnvelopeTable
            def srid = h2gis_datasource.getSpatialTable(zoneTableName).srid
            if (output_datasource) {
                if (!createOutputTables(output_datasource, outputTableNames, 4326)) {
                    error "Cannot prepare the output tables to save the result"
                    return null
                }
            }
            //Prepare OSM extraction
            def query = "[maxsize:1073741824]" + OSMTools.Utilities.buildOSMQuery(zoneTableNames.envelope, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
            def extract = OSMTools.Loader.extract()
            if (extract.execute(overpassQuery: query)) {
                IProcess createGISLayerProcess = PrepareData.OSMGISLayers.createGISLayers()
                if (createGISLayerProcess.execute(datasource: h2gis_datasource, osmFilePath: extract.results.outputFilePath, epsg: srid)) {
                    def gisLayersResults = createGISLayerProcess.getResults()
                    if (zoneTableName != null) {
                        info "Formating OSM GIS layers"
                        IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
                        format.execute([
                                datasource                : h2gis_datasource,
                                inputTableName            : gisLayersResults.buildingTableName,
                                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                epsg                      : srid])
                        def buildingTableName = format.results.outputTableName

                        format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
                        format.execute([
                                datasource                : h2gis_datasource,
                                inputTableName            : gisLayersResults.roadTableName,
                                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                epsg                      : srid])
                        def roadTableName = format.results.outputTableName


                        format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
                        format.execute([
                                datasource                : h2gis_datasource,
                                inputTableName            : gisLayersResults.railTableName,
                                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                epsg                      : srid])
                        def railTableName = format.results.outputTableName

                        format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
                        format.execute([
                                datasource                : h2gis_datasource,
                                inputTableName            : gisLayersResults.vegetationTableName,
                                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                epsg                      : srid])
                        def vegetationTableName = format.results.outputTableName

                        format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
                        format.execute([
                                datasource                : h2gis_datasource,
                                inputTableName            : gisLayersResults.hydroTableName,
                                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                epsg                      : srid])
                        def hydrographicTableName = format.results.outputTableName

                        //TODO : to be used in the geoindicators chains
                        format = PrepareData.FormattingForAbstractModel.formatImperviousLayer()
                        format.execute([
                                datasource                : h2gis_datasource,
                                inputTableName            : gisLayersResults.imperviousTableName,
                                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                                epsg                      : srid])
                        def imperviousTableName = format.results.outputTableName

                        info "OSM GIS layers formated"

                        //Build the indicators
                        IProcess geoIndicators = GeoIndicators()
                        if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zoneTableName,
                                buildingTable: buildingTableName, roadTable: roadTableName,
                                railTable: railTableName, vegetationTable: vegetationTableName,
                                hydrographicTable: hydrographicTableName, indicatorUse: processing_parameters.indicatorUse,
                                svfSimplified: processing_parameters.svfSimplified, prefixName: processing_parameters.prefixName,
                                mapOfWeights: processing_parameters.mapOfWeights)) {
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
                        if (outputFolder && geoIndicatorsComputed && ouputTableFiles) {
                            saveOutputFiles(h2gis_datasource, id_zone, results, ouputTableFiles, outputFolder, "osm_")
                        }
                        if (output_datasource && geoIndicatorsComputed) {
                            saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, 4326, false)
                        }
                    }
                } else {
                    logger.error "Cannot load the OSM file ${extract.results.outputFilePath}"
                }
            } else {
                logger.error "Cannot execute the overpass query $query"
            }
        } else {
            logger.error "Cannot calculate a bounding box to extract OSM data"
        }

        info "Number of areas processed ${index + 1} on $nbAreas"
    }
            return  [outputMessage:"The OSM processing tasks have been done"]
        }
    })}


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
            geom = OSMTools.Utilities.geometryFromOverpass(zoneToExtract)
            if (!geom) {
                logger.error("The bounding box cannot be null")
                return null
            }
        } else if (zoneToExtract instanceof String) {
            geom = OSMTools.Utilities.getAreaFromPlace(zoneToExtract);
            if (!geom) {
                logger.error("Cannot find an area from the place name ${zoneToExtract}")
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
            logger.error("The zone to extract must be a place name or a JTS envelope")
            return null;
        }

        /**
         * Extract the OSM file from the envelope of the geometry
         */
        def envelope = GeographyUtils.expandEnvelopeByMeters(geom.getEnvelopeInternal(), processing_parameters.distance)

        //Find the best utm zone
        //Reproject the geometry and its envelope to the UTM zone
        def con = datasource.getConnection();
        def interiorPoint = envelope.centre()
        def epsg = SFSUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
        def geomUTM = ST_Transform.ST_Transform(con, geom, epsg)
        def tmpGeomEnv = geom.getFactory().toGeometry(envelope)
        tmpGeomEnv.setSRID(4326)

        datasource.execute """drop table if exists ${outputZoneTable}; create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${
            geomUTM.toString()
        }', $epsg), '$zoneToExtract');"""

        datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${
            ST_Transform.ST_Transform(con, tmpGeomEnv, epsg).toString()
        }',$epsg), '$zoneToExtract');"""

        return [outputZoneTable: outputZoneTable,
                outputZoneEnvelopeTable: outputZoneEnvelopeTable,
                envelope:envelope]
    }else{
        logger.error "The zone to extract cannot be null or empty"
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
def GeoIndicators() {
    def final COLUMN_ID_RSU = "id_rsu"
    def final GEOMETRIC_COLUMN = "the_geom"
    return create({
        title "Compute all geoindicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified:false, prefixName: "",
                mapOfWeights: ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_class": 1]
        outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String,
                outputTableRsuIndicators: String, outputTableRsuLcz:String, outputTableZone:String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance,indicatorUse,svfSimplified, prefixName, mapOfWeights ->
            info "Start computing the geoindicators..."
            // Temporary tables are created
            def lczIndicTable = "LCZ_INDIC_TABLE$uuid"

            // Output Lcz table name is set to null in case LCZ indicators are not calculated
            def rsuLcz = null

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
            if (!spatialUnits.execute([datasource       : datasource,           zoneTable           : zoneTable,
                                       buildingTable    : buildingTable,        roadTable           : roadTable,
                                       railTable        : railTable,            vegetationTable     : vegetationTable,
                                       hydrographicTable: hydrographicTable,    surface_vegetation  : surface_vegetation,
                                       surface_hydro    : surface_hydro,        distance            : distance,
                                       prefixName       : prefixName,
                                       indicatorUse:indicatorUse])) {
                error "Cannot create the spatial units"
                return null
            }
            String relationBuildings = spatialUnits.getResults().outputTableBuildingName
            String relationBlocks = spatialUnits.getResults().outputTableBlockName
            String relationRSU = spatialUnits.getResults().outputTableRsuName

            //Compute building indicators
            def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
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
            if(indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")){
                def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
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
            def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
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
            if(indicatorUse.contains("LCZ")){
                def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"             : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                     "BUILDING_AREA_FRACTION"           : "BUILDING_SURFACE_FRACTION",
                                     "ASPECT_RATIO"                     : "ASPECT_RATIO",
                                     "GROUND_SKY_VIEW_FACTOR"           : "SKY_VIEW_FACTOR",
                                     "PERVIOUS_FRACTION"                : "PERVIOUS_SURFACE_FRACTION",
                                     "IMPERVIOUS_FRACTION"              : "IMPERVIOUS_SURFACE_FRACTION",
                                     "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS": "TERRAIN_ROUGHNESS_CLASS"]

                // Get into an other table the ID, geometry column and the 7 indicators useful for LCZ classification
                datasource.execute "DROP TABLE IF EXISTS $lczIndicTable;" +
                        "CREATE TABLE $lczIndicTable AS SELECT $COLUMN_ID_RSU, $GEOMETRIC_COLUMN, " +
                        "${lczIndicNames.keySet().join(",")} FROM ${computeRSUIndicators.results.outputTableName}"

                // Rename the indicators in order to be consistent with the LCZ ones
                def queryReplaceNames = ""

                lczIndicNames.each { oldIndic, newIndic ->
                    queryReplaceNames += "ALTER TABLE $lczIndicTable ALTER COLUMN $oldIndic RENAME TO $newIndic;"
                }
                datasource.execute "$queryReplaceNames"

                // The classification algorithm is called
                def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
                if(!classifyLCZ([rsuLczIndicators   : lczIndicTable,
                                 normalisationType  : "AVG",
                                 mapOfWeights       : mapOfWeights,
                                 prefixName         : prefixName,
                                 datasource         : datasource,
                                 prefixName         : prefixName])){
                    info "Cannot compute the LCZ classification."
                    return
                }
                rsuLcz = classifyLCZ.results.outputTableName
            }

            datasource.execute "DROP TABLE IF EXISTS $lczIndicTable;"


            return [outputTableBuildingIndicators: computeBuildingsIndicators.getResults().outputTableName,
                    outputTableBlockIndicators   : blockIndicators,
                    outputTableRsuIndicators     : computeRSUIndicators.getResults().outputTableName,
                    outputTableRsuLcz   : rsuLcz,
                    outputTableZone:zoneTable]
        }
    })
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
