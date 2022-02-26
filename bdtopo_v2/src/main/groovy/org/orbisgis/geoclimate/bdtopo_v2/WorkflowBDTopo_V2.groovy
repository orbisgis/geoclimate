package org.orbisgis.geoclimate.bdtopo_v2

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2.tools.DeleteDbFiles
import org.h2gis.postgis_jts_osgi.DataSourceFactoryImpl
import org.h2gis.utilities.FileUtilities
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.orbisgis.orbisdata.datamanager.api.dataset.ITable
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.datamanager.jdbc.postgis.POSTGIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.geoclimate.Geoindicators
import org.h2gis.functions.io.utility.IOMethods
import org.osgi.service.jdbc.DataSourceFactory
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
 *                 "commune":"ign_bdtopo_2017.commune",
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
              Map parameters =null
            if(input) {
                if (input instanceof String){
                    //Check if it's a path to a file
                    def configFile = new File(input)
                    if (!configFile.isFile()) {
                        error "The configuration file doesn't exist"
                        return
                    }
                    if(!FileUtilities.isExtensionWellFormated(configFile, "json")){
                        error "The configuration file must be a json file"
                        return
                    }
                    parameters = readJSONParameters(configFile)
                }else if(input instanceof Map){
                    parameters =input
                }
            }
            else {
                error "The input parameters cannot be null or empty.\n Please set a path to a configuration file or " +
                        "a map with all required parameters"
                return
            }
            //Store the zone identifier and the names of the tables
            def outputTableNamesResult = [:]
            if (parameters) {
                debug "Reading file parameters"
                debug parameters.description
                def inputParameters = parameters.input
                def output = parameters.output
                //Default H2GIS database properties
                def databaseFolder = System.getProperty("java.io.tmpdir")
                def databaseName = "bdtopo_v2_2"+UUID.randomUUID().toString().replaceAll("-", "_")
                def databasePath =  databaseFolder + File.separator + databaseName
                def h2gis_properties = ["databaseName": databasePath, "user": "sa", "password": ""]
                def delete_h2gis = true
                def geoclimatedb = parameters.geoclimatedb
                if (geoclimatedb) {
                    def h2gis_folder = geoclimatedb.get("folder")
                    if(h2gis_folder){
                        databaseFolder=h2gis_folder
                    }
                    databasePath =  databaseFolder + File.separator +  databaseName
                    def h2gis_name= geoclimatedb.get("name")
                    if(h2gis_name){
                        def dbName = h2gis_name.split(";")
                        databaseName=dbName[0]
                        databasePath =  databaseFolder + File.separator + h2gis_name
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
                if (inputParameters) {
                    def isbdTopo_v2 = inputParameters.bdtopo_v2
                    if(!isbdTopo_v2){
                        error "The input datasource must be defined with the name bdtopo_v2"
                        return
                    }
                    def inputDataBase = isbdTopo_v2.database
                    def inputFolder = isbdTopo_v2.folder
                    def id_zones = []
                    if (inputFolder && inputDataBase) {
                        error "Please set only one input data provider"
                    } else if (inputFolder) {
                        def inputFolderPath = inputFolder
                        if (inputFolder in Map) {
                            inputFolderPath = inputFolder.path
                            if (!inputFolderPath) {
                                error "The input folder $inputFolderPath cannot be null or empty"
                                return
                            }
                            id_zones = inputFolder.id_zones
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
                                                         "building_urban_typo",
                                                         "grid_indicators",
                                                         "road_traffic",
                                                         "population"]
                            //Get processing parameters
                            def processing_parameters = extractProcessingParameters(parameters.parameters)
                            if(!processing_parameters){
                                return
                            }
                            def outputDataBase = output.database
                            def outputFolder = output.folder
                            def deleteOutputData = output.get("delete")
                            if(!deleteOutputData){
                                deleteOutputData = true
                            }else if(!deleteOutputData in Boolean){
                                error "The delete parameter must be a boolean value"
                                return null
                            }
                            def outputSRID = output.get("srid")
                            if(outputSRID && outputSRID <= 0) {
                                    error "The ouput srid must be greater or equal than 0"
                                    return null
                            }
                            if (outputDataBase && outputFolder) {
                                def outputFolderProperties = outputFolderProperties(outputFolder)
                                //Check if we can write in the output folder
                                def file_outputFolder = new File(outputFolderProperties.path)
                                if (!file_outputFolder.isDirectory()) {
                                    error "The directory $file_outputFolder doesn't exist."
                                    return
                                }
                                if (!file_outputFolder.canWrite()) {
                                    file_outputFolder = null
                                }
                                //Check not the conditions for the output database
                                def outputTableNames = outputDataBase.get("tables")
                                def allowedOutputTableNames = geoclimateTableNames.intersect(outputTableNames.keySet())
                                def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() !=
                                        allowedOutputTableNames.size()
                                if (!allowedOutputTableNames && notSameTableNames) {
                                    outputDataBase = null
                                    outputTableNames = null
                                }
                                def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                if (!output_datasource) {
                                    return
                                }
                                def h2gis_datasource = H2GIS.open(h2gis_properties)
                                if(!h2gis_datasource){
                                    error "Cannot load the local H2GIS database to run Geoclimate"
                                    return
                                }
                                id_zones = loadDataFromFolder(inputFolderPath, h2gis_datasource, id_zones)
                                if (id_zones) {
                                    id_zones.each { id_zone ->
                                    def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters, id_zone,
                                            file_outputFolder, outputFolderProperties.tables, output_datasource,
                                            finalOutputTables, outputSRID)
                                        if(bdtopo_results) {
                                            outputTableNamesResult.put(id_zone, bdtopo_results.findAll { it.value != null })
                                        }
                                    }
                                    if (delete_h2gis) {
                                        def localCon = h2gis_datasource.getConnection()
                                        if(localCon){
                                            localCon.close()
                                            DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                            debug "The local H2GIS database : ${databasePath} has been deleted"
                                        }
                                        else{
                                            error "Cannot delete the local H2GIS database : ${databasePath} "
                                        }
                                    }
                                    return [output: outputTableNamesResult]
                                } else {
                                    error "Cannot load the files from the folder $inputFolder"
                                    return
                                }

                            } else if (outputFolder) {
                                def outputFolderProperties = outputFolderProperties(outputFolder)
                                //Check if we can write in the output folder
                                def file_outputFolder = new File(outputFolderProperties.path)
                                if (!file_outputFolder.isDirectory()) {
                                    error "The directory $file_outputFolder doesn't exist."
                                    return
                                }
                                if (file_outputFolder.canWrite()) {
                                    def h2gis_datasource = H2GIS.open(h2gis_properties)
                                    if(!h2gis_datasource){
                                        error "Cannot load the local H2GIS database to run Geoclimate"
                                        return
                                    }
                                    id_zones = loadDataFromFolder(inputFolderPath, h2gis_datasource, id_zones)
                                    if (id_zones) {
                                        id_zones.each { id_zone ->
                                        def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters, id_zone,
                                                file_outputFolder, outputFolderProperties.tables, null, null, outputSRID)
                                            if(bdtopo_results) {
                                                outputTableNamesResult.put(id_zone, bdtopo_results.findAll { it.value != null })
                                            }

                                        }
                                        //Delete database
                                        if (delete_h2gis) {
                                            def localCon = h2gis_datasource.getConnection()
                                            if(localCon){
                                                localCon.close()
                                                DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                                debug "The local H2GIS database : ${databasePath} has been deleted"
                                            }
                                            else{
                                                error "Cannot delete the local H2GIS database : ${databasePath} "
                                            }
                                        }
                                        return [output: outputTableNamesResult]
                                    } else {
                                        error "Cannot load the files from the folder $inputFolder"
                                        return
                                    }
                                } else {
                                    error "You don't have permission to write in the folder $outputFolder \n" +
                                            "Please check the folder."
                                    return
                                }

                            } else if (outputDataBase) {
                                def outputTableNames = outputDataBase.tables
                                def allowedOutputTableNames = geoclimateTableNames.intersect(outputTableNames.keySet())
                                def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() !=
                                        allowedOutputTableNames.size()
                                if (allowedOutputTableNames && !notSameTableNames) {
                                    def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                    def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                    if (!output_datasource) {
                                        return
                                    }
                                    def h2gis_datasource = H2GIS.open(h2gis_properties)
                                    if(!h2gis_datasource){
                                        error "Cannot load the local H2GIS database to run Geoclimate"
                                        return
                                    }
                                    id_zones = loadDataFromFolder(inputFolderPath, h2gis_datasource, id_zones)
                                    if (id_zones) {
                                        id_zones.each {id_zone->
                                            def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters, id_zone, null,
                                                    null, output_datasource, finalOutputTables, outputSRID)
                                            if(bdtopo_results) {
                                                outputTableNamesResult.put(id_zone, bdtopo_results.findAll { it.value != null })
                                            }
                                        }
                                        if (delete_h2gis) {
                                            def localCon = h2gis_datasource.getConnection()
                                            if(localCon){
                                                localCon.close()
                                                DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                                debug "The local H2GIS database : ${databasePath} has been deleted"
                                            }
                                            else{
                                                error "Cannot delete the local H2GIS database : ${databasePath} "
                                            }
                                        }
                                        return [output:outputTableNamesResult]
                                    } else {
                                        error "Cannot load the files from the folder $inputFolder"
                                        return
                                    }
                                } else {
                                    error "All output table names must be specified in the configuration file."
                                    return
                                }
                            } else {
                                error "Please set at least one output provider"
                                return
                            }
                        } else {
                            error "Please set at least one output provider"
                            return
                        }

                    } else if (inputDataBase) {
                        if (output) {
                            def geoclimatetTableNames = ["building_indicators",
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
                                                         "building_urban_typo",
                                                         "grid_indicators",
                                                         "road_traffic"]
                            //Get processing parameters
                            def processing_parameters = extractProcessingParameters(parameters.parameters)
                            if(!processing_parameters){
                                return
                            }
                            def outputSRID = output.get("srid")
                            if(!outputSRID && outputSRID>=0){
                                error "The ouput srid must be greater or equal than 0"
                                return null
                            }
                            def outputDataBase = output.database
                            def outputFolder = output.folder
                            if (outputDataBase && outputFolder) {
                                def outputFolderProperties = outputFolderProperties(outputFolder)
                                //Check if we can write in the output folder
                                def file_outputFolder = new File(outputFolderProperties.path)
                                if (!file_outputFolder.isDirectory()) {
                                    error "The directory $file_outputFolder doesn't exist."
                                    return
                                }
                                if (!file_outputFolder.canWrite()) {
                                    file_outputFolder = null
                                }
                                //Check not the conditions for the output database
                                def outputTableNames = outputDataBase.tables
                                def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                                def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() !=
                                        allowedOutputTableNames.size()
                                if (!allowedOutputTableNames && notSameTableNames) {
                                    outputDataBase = null
                                    outputTableNames = null
                                }
                                def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                def codes = inputDataBase.id_zones
                                if (codes && codes in Collection) {
                                    def inputTableNames = inputDataBase.tables
                                    def h2gis_datasource = H2GIS.open(h2gis_properties)
                                    if(!h2gis_datasource){
                                        error "Cannot load the local H2GIS database to run Geoclimate"
                                        return
                                    }
                                    def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                    if (!output_datasource) {
                                        return
                                    }
                                    info "${codes.size()} areas will be processed"
                                    def nbzones=0;
                                    for (code in codes) {
                                        nbzones++
                                        if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), code, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                            def bdtopo_results =  bdtopo_processing(h2gis_datasource, processing_parameters, code,
                                                    file_outputFolder, outputFolderProperties.tables, output_datasource,
                                                    finalOutputTables, outputSRID)
                                            if(bdtopo_results){
                                                outputTableNamesResult.put(code, bdtopo_results.findAll{ it.value!=null })
                                            }

                                        } else {
                                            return
                                        }
                                        info  "${nbzones} area(s) on ${codes.size()}"
                                    }
                                    if (delete_h2gis) {
                                        def localCon = h2gis_datasource.getConnection()
                                        if(localCon){
                                            localCon.close()
                                            DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                            debug "The local H2GIS database : ${databasePath} has been deleted"
                                        }
                                        else{
                                            error "Cannot delete the local H2GIS database : ${databasePath} "
                                        }
                                    }

                                    return [output: outputTableNamesResult]

                                } else if (codes) {
                                    def inputTableNames = inputDataBase.tables
                                    def h2gis_datasource = H2GIS.open(h2gis_properties)
                                    if(!h2gis_datasource){
                                        error "Cannot load the local H2GIS database to run Geoclimate"
                                        return
                                    }
                                    def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                    if (!output_datasource) {
                                        return
                                    }
                                    def commune_location = inputTableNames.commune
                                    if (commune_location) {
                                        output_datasource.eachRow("select distinct insee_com from $commune_location where $codes group by insee_com ;") { row ->
                                            id_zones << row.insee_com
                                        }
                                        info "${id_zones.size()} areas will be processed"
                                        def nbzones=0;
                                        for (id_zone in id_zones) {
                                            nbzones++
                                            if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]),
                                                    id_zone, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                def bdtopo_results  = bdtopo_processing(h2gis_datasource, processing_parameters, id_zone,
                                                        file_outputFolder, outputFolderProperties.tables,
                                                        output_datasource, finalOutputTables, outputSRID)
                                                if(bdtopo_results){
                                                    outputTableNamesResult.put(id_zone, bdtopo_results.findAll{ it.value!=null })
                                                }else{
                                                    error "Cannot execute the geoclimate processing chain on $id_zone"
                                                    return
                                                }
                                            } else {
                                                return
                                            }
                                            info  "${nbzones} area(s) on ${id_zones.size()}"
                                        }
                                        if (delete_h2gis) {
                                            def localCon = h2gis_datasource.getConnection()
                                            if(localCon){
                                                localCon.close()
                                                DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                                debug "The local H2GIS database : ${databasePath} has been deleted"
                                            }
                                            else{
                                                error "Cannot delete the local H2GIS database : ${databasePath} "
                                            }
                                        }
                                    } else {
                                        error "Cannot find any commune features from the query $codes"
                                    }
                                }

                            } else if (outputFolder) {
                                def outputFolderProperties = outputFolderProperties(outputFolder)
                                //Check if we can write in the output folder
                                def file_outputFolder = new File(outputFolderProperties.path)
                                if (!file_outputFolder.isDirectory()) {
                                    error "The directory $file_outputFolder doesn't exist."
                                    return null
                                }
                                if (file_outputFolder.canWrite()) {
                                    def codes = inputDataBase.id_zones
                                    if (codes && codes in Collection) {
                                        def inputTableNames = inputDataBase.tables
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        if(!h2gis_datasource){
                                            error "Cannot load the local H2GIS database to run Geoclimate"
                                            return
                                        }
                                        info "${codes.size()} areas will be processed"
                                        def nbzones=0;
                                        for (code in codes) {
                                            nbzones++
                                            if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]),
                                                    code, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                def bdtopo_results  = bdtopo_processing(h2gis_datasource, processing_parameters,
                                                        code, file_outputFolder, outputFolderProperties.tables, null, null, outputSRID)
                                                if(bdtopo_results){
                                                    outputTableNamesResult.put(id_zone, bdtopo_results.findAll{ it.value!=null })
                                                }else{
                                                    error "Cannot execute the geoclimate processing chain on $code"
                                                    return
                                                }
                                            } else {
                                                return
                                            }
                                            info  "${nbzones} area(s) on ${codes.size()}"
                                        }
                                    }

                                } else {
                                    error "You don't have permission to write in the folder $outputFolder. \n Check if the folder exists."
                                    return
                                }
                            } else if (outputDataBase) {
                                def outputTableNames = outputDataBase.tables
                                if(!outputTableNames){
                                    error "You must set at least one table name to export in the database.\n" +
                                            "Available tables key names are : ${geoclimatetTableNames.join(",")}"
                                    return
                                }
                                def allowedOutputTableNames = geoclimatetTableNames.intersect(outputTableNames.keySet())
                                def notSameTableNames = allowedOutputTableNames.groupBy { it.value }.size() !=
                                        allowedOutputTableNames.size()
                                if (allowedOutputTableNames && !notSameTableNames) {
                                    def finalOutputTables = outputTableNames.subMap(allowedOutputTableNames)
                                    def codes = inputDataBase.id_zones
                                    if (codes && codes in Collection) {
                                        def inputTableNames = inputDataBase.tables
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        if(!h2gis_datasource){
                                            error "Cannot load the local H2GIS database to run Geoclimate"
                                            return
                                        }
                                        def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                        if (!output_datasource) {
                                            return null
                                        }
                                        info "${codes.size()} areas will be processed"
                                        def nbzones=0;
                                        for (code in codes) {
                                            nbzones++
                                            if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), code, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters, code, null, null, output_datasource, finalOutputTables, outputSRID)
                                                if(bdtopo_results){
                                                    outputTableNamesResult.put(code, bdtopo_results.findAll{ it.value!=null })
                                                }else{
                                                    error "Cannot execute the geoclimate processing chain on $code"
                                                    return
                                                }
                                            } else {
                                                return null
                                            }
                                            info  "${nbzones} area(s) on ${codes.size()}"
                                        }
                                        if (delete_h2gis) {
                                            def localCon = h2gis_datasource.getConnection()
                                            if(localCon){
                                                localCon.close()
                                                DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                                debug "The local H2GIS database : ${databasePath} has been deleted"
                                            }
                                            else{
                                                error "Cannot delete the local H2GIS database : ${databasePath} "
                                            }
                                        }
                                    } else if (codes) {
                                        def inputTableNames = inputDataBase.tables
                                        def h2gis_datasource = H2GIS.open(h2gis_properties)
                                        if(!h2gis_datasource){
                                            error "Cannot load the local H2GIS database to run Geoclimate"
                                            return
                                        }
                                        def output_datasource = createDatasource(outputDataBase.subMap(["user", "password", "url"]))
                                        if (!output_datasource) {
                                            return null
                                        }
                                        def commune_location = inputTableNames.commune
                                        if (commune_location) {
                                            output_datasource.eachRow("select distinct CODE_INSEE from $commune_location where $codes group by CODE_INSEE ;") { row ->
                                                id_zones << row.insee_com
                                            }

                                            info "${id_zones.size()} communes will be processed"
                                            def nbzones=0
                                            for (id_zone in id_zones) {
                                                nbzones++
                                                if (loadDataFromDatasource(inputDataBase.subMap(["user", "password", "url"]), id_zone, processing_parameters.distance, inputTableNames, h2gis_datasource)) {
                                                    def bdtopo_results = bdtopo_processing(h2gis_datasource, processing_parameters, id_zone, null, null, output_datasource, finalOutputTables, outputSRID)
                                                    if(bdtopo_results){
                                                        outputTableNamesResult.put(id_zone, bdtopo_results.findAll{ it.value!=null })
                                                    }else{
                                                        error "Cannot execute the geoclimate processing chain on $id_zone"
                                                        return
                                                    }
                                                    info  "${nbzones} area(s) on ${id_zones.size()}"
                                                } else {
                                                    return
                                                }
                                            }
                                            if (delete_h2gis) {
                                                def localCon = h2gis_datasource.getConnection()
                                                if(localCon){
                                                    localCon.close()
                                                    DeleteDbFiles.execute(databaseFolder, databaseName, true)
                                                    debug "The local H2GIS database : ${databasePath} has been deleted"
                                                }
                                                else{
                                                    error "Cannot delete the local H2GIS database : ${databasePath} "
                                                }
                                            }
                                        } else {
                                            error "Cannot find any commune features from the query $codes"
                                        }
                                    }

                                } else {
                                    error "All output table names must be specified in the configuration file."
                                    return
                                }

                            } else {
                                error "Please set at least one output provider"
                                return
                            }
                        } else {
                            error "Please set at least one output provider"
                            return
                        }
                    } else {
                        error "Please set a valid input data provider"
                    }
                } else {
                    error "Cannot find any input parameters."
                }
            } else {
                error "Empty parameters"
            }
            return [outputMessage: "The BDTopo V2.2 workflow has been executed"]
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
                        "rsu_urban_typo_area",
                        "rsu_urban_typo_floor_area",
                        "building_urban_typo",
                        "grid_indicators",
                        "road_traffic",
                        "population"]

    if(outputFolder in Map){
        def outputPath = outputFolder.path
        def outputTables = outputFolder.tables
        if(!outputPath){
            return
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
    def db_output_url = database_properties.url
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
                return
            }
        }
        else {
            error"Invalid output database url"
            return
        }

    }
    else{
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
    def commune_location = inputTableNames.commune
    if (!commune_location) {
        error "The commune table must be specified to run Geoclimate"
        return
    }
    DataSourceFactory dataSourceFactory = new DataSourceFactoryImpl();
    Connection sourceConnection = null;
    try {
        Properties props = new Properties();
        input_database_properties.forEach(props::put);
        DataSource ds = dataSourceFactory.createDataSource(props);
        sourceConnection = ds.getConnection()
    } catch (SQLException e) {
        error("Cannot connect to the database to import the data ");
    }

    if(sourceConnection==null){
        error("Cannot connect to the database to import the data ");
        return
    }

    //Check if the commune table exists
    if(!JDBCUtilities.tableExists(sourceConnection, commune_location)){
        error("The commune table doesn't exist");
        return
    }

    //Find the SRID of the commune table
    def commune_srid = GeometryTableUtilities.getSRID(sourceConnection, commune_location)
    if(commune_srid==-1){
        error("The commune table doesn't have any SRID");
        return
    }
    String outputTableName = "COMMUNE"
    //Let's process the data by i_zone
    //Check if code is a string or a bbox
    //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
    if (code in Collection) {
        def tmp_insee = code.join("-")
        h2gis_datasource.execute("""DROP TABLE IF EXISTS ${outputTableName};
        CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(GEOMETRY),CODE_INSEE VARCHAR);
        INSERT INTO ${outputTableName} VALUES(ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, ${commune_srid}), ${tmp_insee})""".toString())
    }else if (code instanceof String) {
        //input_database_properties =updateDriverURL(input_database_properties)
        String inputTableName = "(SELECT THE_GEOM, CODE_INSEE FROM $commune_location WHERE CODE_INSEE='$code' or nom='$code')"
        debug "Loading in the H2GIS database $outputTableName"
        IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);
    }
    def count = h2gis_datasource."$outputTableName".rowCount
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM
        def outputTableNameBatiInd = "BATI_INDIFFERENCIE"
        if(inputTableNames.bati_indifferencie){
            //Extract bati_indifferencie
            def  inputTableName = "(SELECT ID, THE_GEOM, HAUTEUR FROM ${inputTableNames.bati_indifferencie}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiInd"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableNameBatiInd, -1, 1000);
        }
        def  outputTableNameBatiIndus = "BATI_INDUSTRIEL"
        if(inputTableNames.bati_industriel) {
            //Extract bati_industriel
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE, HAUTEUR FROM ${inputTableNames.bati_industriel}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiIndus"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableNameBatiIndus, -1, 1000);
        }
        def outputTableNameBatiRem = "BATI_REMARQUABLE"
        if(inputTableNames.bati_remarquable) {
            //Extract bati_remarquable
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE, HAUTEUR FROM ${inputTableNames.bati_remarquable}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiRem"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableNameBatiRem, -1, 1000);
        }
        def  outputTableNameRoad = "ROUTE"
        if(inputTableNames.route) {
            //Extract route
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE, LARGEUR, POS_SOL, FRANCHISST, SENS FROM ${inputTableNames.route}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameRoad"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableNameRoad, -1, 1000);
        }
        else{
            error "The route table must be provided"
            return
        }

        //Before starting geoclimate algorithms we must check if some tables exist
        if(!h2gis_datasource.hasTable(outputTableNameBatiInd)&& !h2gis_datasource.hasTable(outputTableNameBatiIndus) && !h2gis_datasource.hasTable(outputTableNameBatiRem)){
            error "At least one of the following tables must be provided : bati_indifferencie, bati_industriel, bati_remarquable"
            return
        }
        if(!h2gis_datasource.hasTable(outputTableNameRoad)){
            error "The route table must be provided"
            return
        }

        if(inputTableNames.troncon_voie_ferree) {
            //Extract troncon_voie_ferree
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTableNames.troncon_voie_ferree}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "TRONCON_VOIE_FERREE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if(inputTableNames.surface_eau) {
            //Extract surface_eau
            def inputTableName = "(SELECT ID, THE_GEOM FROM ${inputTableNames.surface_eau}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_EAU"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if(inputTableNames.zone_vegetation) {
            //Extract zone_vegetation
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE  FROM ${inputTableNames.zone_vegetation}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "ZONE_VEGETATION"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        if(inputTableNames.terrain_sport) {
            //Extract terrain_sport
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE  FROM ${inputTableNames.terrain_sport}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport')"
            outputTableName = "TERRAIN_SPORT"
            debug "Loading in the H2GIS database $outputTableName"

            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);}

        if(inputTableNames.construction_surfacique) {
            //Extract construction_surfacique
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE  FROM ${inputTableNames.construction_surfacique}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier'))"
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);}

        if(inputTableNames.surface_route) {
            //Extract surface_route
            def inputTableName = "(SELECT ID, THE_GEOM  FROM ${inputTableNames.surface_route}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);}

        if(inputTableNames.surface_activite) {
            //Extract surface_activite
            def inputTableName = "(SELECT ID, THE_GEOM, CATEGORIE  FROM ${inputTableNames.surface_activite}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (CATEGORIE='Administratif' OR CATEGORIE='Enseignement' OR CATEGORIE='Santé'))"
            outputTableName = "SURFACE_ACTIVITE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }
        //Extract PISTE_AERODROME
        if(inputTableNames.piste_aerodrome){
            def inputTableName = "(SELECT ID, THE_GEOM  FROM ${inputTableNames.piste_aerodrome}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "PISTE_AERODROME"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        //Extract RESERVOIR
        if(inputTableNames.reservoir){
            def inputTableName = "(SELECT ID, THE_GEOM, NATURE, HAUTEUR  FROM ${inputTableNames.reservoir}  WHERE the_geom && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "RESERVOIR"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName,h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }

        sourceConnection.close();

        return true

        } else {
            error "Cannot find any commune with the insee code : $code"
            return
        }
}

/**
 * Workaround to change the postgresql URL when a linked table is created with H2GIS
 * @param input_database_properties
 * @return the input_database_properties with the h2 url
 */
def updateDriverURL(def input_database_properties){
    def db_output_url = input_database_properties.url
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
        error "Invalid output database url"
        return
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
        //Looking for commune shape file
        def commune_file = geoFiles.find{ it.toLowerCase().endsWith("commune.shp")?it:null}
        if(commune_file) {
            //Load commune and check if there is some id_zones inside
            h2gis_datasource.link(commune_file, "COMMUNE_TMP", true)
            int srid = h2gis_datasource.getSpatialTable("COMMUNE_TMP").srid
            if(srid==-1){
                error "The commune file doesn't have any srid"
                return
            }
            id_zones = findIDZones(h2gis_datasource, id_zones, srid)
            geoFiles.remove(commune_file)
            if(id_zones){
                //Load the files
                def numberFiles = geoFiles.size()
                geoFiles.eachWithIndex { geoFile , index->
                    debug "Loading file $geoFile $index on $numberFiles"
                    h2gis_datasource.link(geoFile, true)
                }
                return id_zones

            }else{
                error "The commune file doesn't contains any zone identifiers"
                return
            }
        }
        else{
            error "The input folder must contains a file named commune"
            return
        }
    }else{
        error "The input folder must be a directory"
        return
    }
}

/**
 * Return a list of id_zones
 * @param h2gis_datasource the local database for the geoclimate processes
 * @param id_zones a list of id zones to process
 * @return
 */
def findIDZones(JdbcDataSource h2gis_datasource, def id_zones, def srid){
    def inseeCodes = []
    if(h2gis_datasource.hasTable("COMMUNE_TMP")) {
        h2gis_datasource.execute("""DROP TABLE IF EXISTS COMMUNE;
            CREATE TABLE COMMUNE (THE_GEOM GEOMETRY(GEOMETRY, ${srid}), CODE_INSEE VARCHAR)""".toString())
        if (id_zones) {
            def id_zones_tmp =[]
            id_zones.eachWithIndex { id_zone, index ->
                /*The zone is a osm bounding box represented by
                *  ymin,xmin , ymax,xmax,
                */
                if (id_zone in Collection) {
                    def tmp_insee = id_zone.join("-")
                    def params =[id_zone[1],id_zone[0],id_zone[3],id_zone[2],srid, tmp_insee]
                    h2gis_datasource.executeInsert("INSERT INTO COMMUNE (THE_GEOM,CODE_INSEE) VALUES (ST_MakeEnvelope(?, ?, ?, ?, ?), ?)", params)
                    inseeCodes<<tmp_insee
                }else if (id_zone instanceof String) {
                    id_zones_tmp<<id_zone
                }
            }
            if(id_zones_tmp){
                def zones =id_zones_tmp.join(",")
                h2gis_datasource.withBatch(100, "INSERT INTO COMMUNE VALUES(?,?)") { ps ->
                    h2gis_datasource.eachRow("""select THE_GEOM, CODE_INSEE FROM COMMUNE_TMP where code_insee in 
                    ('${zones}') OR nom in('${zones}')"""){ row ->
                        if(!inseeCodes.contains(row.CODE_INSEE)){
                            ps.addBatch([row.the_geom, row.CODE_INSEE])
                            inseeCodes<<row.CODE_INSEE
                        }
                    }
                }
            }
            if(!inseeCodes) {
                error "Cannot find any commune or area from : ${id_zones}"
            }
        } else {
            h2gis_datasource.withBatch(100, "INSERT INTO COMMUNE VALUES(?,?)") { ps ->
                h2gis_datasource.eachRow("""select THE_GEOM, CODE_INSEE FROM COMMUNE_TMP group by code_insee"""){ row ->
                    if(!inseeCodes.contains(row.CODE_INSEE)){
                        ps.addBatch([row.the_geom, row.CODE_INSEE])
                        inseeCodes<<row.CODE_INSEE
                    }
                }
            }
            if(!inseeCodes) {
                error "Cannot find any commune from : ${id_zones}"
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
    def defaultParameters = [distance: 1000,
                             distance_buffer:500,prefixName: "",
                             hLevMin : 3, hLevMax: 15, hThresholdLev2: 10]
    def rsu_indicators_default =[indicatorUse: [],
                                 svfSimplified:true,
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
                                 urbanTypoModelName: "URBAN_TYPOLOGY_BDTOPO_V2_RF_2_1.model"]
    defaultParameters.put("rsu_indicators", rsu_indicators_default)

    if(processing_parameters){
        def distanceP =  processing_parameters.distance
        if(distanceP && distanceP in Number){
            defaultParameters.distance = distanceP
        }
        def distance_bufferP =  processing_parameters.distance_buffer
        if(distance_bufferP && distance_bufferP in Number){
            defaultParameters.distance_buffer = distance_bufferP
        }
        def prefixNameP = processing_parameters.prefixName
        if(prefixNameP && prefixNameP in String){
            defaultParameters.prefixName = prefixNameP
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
        //Check for rsu indicators
        def  rsu_indicators = processing_parameters.rsu_indicators
        if(rsu_indicators){
            def indicatorUseP = rsu_indicators.indicatorUse
            if(indicatorUseP && indicatorUseP in List) {
                def allowed_rsu_indicators = ["LCZ", "UTRF", "TEB"]
                def allowedOutputRSUIndicators = allowed_rsu_indicators.intersect(indicatorUseP*.toUpperCase())
                if (allowedOutputRSUIndicators) {
                    rsu_indicators_default.indicatorUse = indicatorUseP
                }
                else {
                    error "Please set a valid list of RSU indicator names in ${allowedOutputRSUIndicators}"
                    return
                }
            }else{
                error "The list of RSU indicator names cannot be null or empty"
                return
            }
            def snappingToleranceP =  rsu_indicators.snappingTolerance
            if(snappingToleranceP && snappingToleranceP in Number){
                rsu_indicators_default.snappingTolerance = snappingToleranceP
            }
            def surface_vegetationP =  rsu_indicators.surface_vegetation
            if(surface_vegetationP && surface_vegetationP in Number){
                rsu_indicators_default.surface_vegetation = surface_vegetationP
            }
            def surface_hydroP =  rsu_indicators.surface_hydro
            if(surface_hydroP && surface_hydroP in Number){
                rsu_indicators_default.surface_hydro = surface_hydroP
            }
            def svfSimplifiedP = rsu_indicators.svfSimplified
            if(svfSimplifiedP && svfSimplifiedP in Boolean){
                rsu_indicators_default.svfSimplified = svfSimplifiedP
            }

            def mapOfWeightsP = rsu_indicators.mapOfWeights
            if(mapOfWeightsP && mapOfWeightsP in Map){
                def defaultmapOfWeights = rsu_indicators_default.mapOfWeights
                if((defaultmapOfWeights+mapOfWeightsP).size()!=defaultmapOfWeights.size()){
                    error "The number of mapOfWeights parameters must contain exactly the parameters ${defaultmapOfWeights.keySet().join(",")}"
                    return
                }else{
                    rsu_indicators_default.mapOfWeights = mapOfWeightsP
                }
            }
        }else{
            rsu_indicators=rsu_indicators_default
        }
        //Check for grid indicators
        def  grid_indicators = processing_parameters.grid_indicators
        if(grid_indicators){
            def x_size = grid_indicators.x_size
            def y_size = grid_indicators.y_size
            def list_indicators = grid_indicators.indicators
            if(x_size && y_size){
                if(x_size<=0 || y_size<= 0){
                    error "Invalid grid size padding. Must be greater that 0"
                    return
                }
                if(!list_indicators){
                    error "The list of indicator names cannot be null or empty"
                    return
                }
                def allowed_grid_indicators=["BUILDING_FRACTION","BUILDING_HEIGHT", "BUILDING_POP","BUILDING_TYPE_FRACTION","WATER_FRACTION","VEGETATION_FRACTION",
                                             "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "URBAN_TYPO_AREA_FRACTION", "LCZ_FRACTION", "LCZ_PRIMARY"]
                def allowedOutputIndicators = allowed_grid_indicators.intersect(list_indicators*.toUpperCase())
                if(allowedOutputIndicators){
                    //Update the RSU indicators list according the grid indicators
                    list_indicators.each { val ->
                        if(val.trim().toUpperCase() in ["LCZ_FRACTION","LCZ_PRIMARY"]){
                            rsu_indicators.indicatorUse<<"LCZ"
                        }else if (val.trim().toUpperCase() in ["URBAN_TYPO_AREA_FRACTION"]){
                            rsu_indicators.indicatorUse<<"UTRF"
                        }
                    }
                    def grid_indicators_tmp =  [
                            "x_size": x_size,
                            "y_size": y_size,
                            "output": "geojson",
                            "rowCol" : false,
                            "indicators": allowedOutputIndicators
                    ]
                    def grid_output = grid_indicators.output
                    if(grid_output) {
                        if(grid_output.toLowerCase() in ["asc","geojson"]){
                            grid_indicators_tmp.output =grid_output.toLowerCase()
                        }
                    }
                    def grid_rowCol = grid_indicators.rowCol
                    if(grid_rowCol && grid_rowCol in Boolean) {
                        grid_indicators_tmp.rowCol =grid_rowCol
                    }

                    defaultParameters.put("grid_indicators", grid_indicators_tmp)
                }
                else {
                    error "Please set a valid list of indicator names in ${allowed_grid_indicators}"
                    return
                }
            }
        }

        //Check for road_traffic method
        def  road_traffic = processing_parameters.road_traffic
        if(road_traffic && road_traffic in Boolean){
            defaultParameters.put("road_traffic", road_traffic)
        }

        //Check if the pop indicators must be computed
        def  pop_indics = processing_parameters.worldpop_indicators
        if(pop_indics && pop_indics in Boolean){
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
 * @param id_zones a list of id zones to process
 * @param outputFolder folder to store the files, null otherwise
 * @param outputFiles the name of the tables that will be saved
 * @param output_datasource a connexion to a database to save the results
 * @param outputTableNames the name of the tables in the output_datasource to save the results
 * @param deleteOutputData true to delete the ouput files if exist
 * @return
 */
def bdtopo_processing(def  h2gis_datasource, def processing_parameters,def id_zones, def outputFolder, def outputFiles, def output_datasource, def outputTableNames, def outputSRID, def deleteOutputData=true){
    //Add the GIS layers to the list of results
    def results = [:]
    def  srid =  h2gis_datasource.getSpatialTable("COMMUNE").srid
    def id_zone
    if(id_zones in Collection){
        id_zone =  id_zones.join("-")
    }else if (id_zones instanceof String){
        id_zone = id_zones
    }
    if(!id_zone){
        error "Invalid id_zones input"
    }

    def reproject =false
    if(outputSRID){
        if(outputSRID!=srid){
            reproject = true
        }
    }else{
        outputSRID =srid
    }
    //Let's run the BDTopo process for the id_zone
        def start  = System.currentTimeMillis()
        info "Starting to process insee id_zone $id_zone"
        //Load and format the BDTopo data
        IProcess loadAndFormatData = loadAndFormatData()
        if(loadAndFormatData.execute([datasource                 : h2gis_datasource,
                                      tableCommuneName            : 'COMMUNE',
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
                                      distBuffer                  : processing_parameters.distance_buffer,
                                      distance                    : processing_parameters.distance,
                                      idZone                      : id_zone,
                                      hLevMin                     : processing_parameters.hLevMin,
                                      hLevMax                     : processing_parameters.hLevMax,
                                      hThresholdLev2              : processing_parameters.hThresholdLev2
        ])){

            def buildingTableName = loadAndFormatData.results.outputBuilding
            def roadTableName = loadAndFormatData.results.outputRoad
            def railTableName = loadAndFormatData.results.outputRail
            def hydrographicTableName = loadAndFormatData.results.outputHydro
            def vegetationTableName = loadAndFormatData.results.outputVeget
            def zoneTableName = loadAndFormatData.results.outputZone
            def imperviousTableName = loadAndFormatData.results.outputImpervious

            info "BDTOPO V2 GIS layers formated"

            def rsu_indicators_params = processing_parameters.rsu_indicators
            def grid_indicators_params = processing_parameters.grid_indicators
            def worldpop_indicators = processing_parameters.worldpop_indicators


            results.put("zoneTableName", zoneTableName)
            results.put("roadTableName", roadTableName)
            results.put("railTableName", railTableName)
            results.put("hydrographicTableName", hydrographicTableName)
            results.put("vegetationTableName", vegetationTableName)
            results.put("imperviousTableName", imperviousTableName)
            results.put("buildingTableName", buildingTableName)

            //Compute the RSU indicators
            if(rsu_indicators_params.indicatorUse){
                //Build the indicators
                IProcess geoIndicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators()
                if (!geoIndicators.execute(datasource: h2gis_datasource, zoneTable: zoneTableName,
                        buildingTable: buildingTableName, roadTable: roadTableName,
                        railTable: railTableName, vegetationTable: vegetationTableName,
                        hydrographicTable: hydrographicTableName, imperviousTable :imperviousTableName,
                        surface_vegetation: rsu_indicators_params.surface_vegetation, surface_hydro: rsu_indicators_params.surface_hydro,
                        snappingTolerance : rsu_indicators_params.snappingTolerance,
                        indicatorUse: rsu_indicators_params.indicatorUse,
                        svfSimplified: rsu_indicators_params.svfSimplified, prefixName: processing_parameters.prefixName,
                        mapOfWeights: rsu_indicators_params.mapOfWeights,
                        urbanTypoModelName: "URBAN_TYPOLOGY_BDTOPO_V2_RF_2_1.model")) {
                    error "Cannot build the geoindicators for the zone $id_zone"
                } else {
                    results.putAll(geoIndicators.getResults())
                }
            }

            //Extract and compute population indicators for the specified year
            //This data can be used by the grid_indicators process
            if(worldpop_indicators){
                IProcess extractWorldPopLayer = WorldPopTools.Extract.extractWorldPopLayer()
                def coverageId = "wpGlobal:ppp_2018"
                def envelope = h2gis_datasource.firstRow("select st_transform(the_geom, 4326) as geom from $zoneTableName".toString()).geom.getEnvelopeInternal()
                def bbox = [envelope.getMinY() as Float,envelope.getMinX()as Float,
                            envelope.getMaxY()as Float,envelope.getMaxX()as Float]
                if(extractWorldPopLayer.execute([coverageId:coverageId,  bbox :bbox])){
                    IProcess importAscGrid = WorldPopTools.Extract.importAscGrid()
                    if(importAscGrid.execute([worldPopFilePath:extractWorldPopLayer.results.outputFilePath,
                                              epsg: srid, tableName : coverageId.replaceAll(":", "_"),  datasource: h2gis_datasource, epsg:srid])){
                        results.put("populationTableName", importAscGrid.results.outputTableWorldPopName)

                        IProcess process = Geoindicators.BuildingIndicators.buildingPopulation()
                        if(!process.execute([inputBuildingTableName: results.buildingTableName,
                                             inputPopulationTableName: importAscGrid.results.outputTableWorldPopName,  datasource: h2gis_datasource])) {
                            info "Cannot compute any population data at building level"
                        }
                        //Update the building table with the population data
                        buildingTableName =process.results.buildingTableName
                        results.put("buildingTableName", buildingTableName)

                    }else {
                        info "Cannot import the worldpop asc file $extractWorldPopLayer.results.outputFilePath"
                    }

                }else {
                    info "Cannot find the population grid $coverageId"
                }
            }

            //Compute the grid indicators
            if(grid_indicators_params) {
                def x_size = grid_indicators_params.x_size
                def y_size = grid_indicators_params.y_size
                IProcess rasterizedIndicators =  Geoindicators.WorkflowGeoIndicators.rasterizeIndicators()
                def geomEnv = h2gis_datasource.getSpatialTable(zoneTableName).getExtent()
                if(rasterizedIndicators.execute(datasource:h2gis_datasource,envelope: geomEnv,
                        x_size : x_size, y_size : y_size,
                        srid : srid,rowCol: grid_indicators_params.rowCol,
                        list_indicators :grid_indicators_params.indicators,
                        buildingTable: buildingTableName, roadTable: roadTableName, vegetationTable: vegetationTableName,
                        hydrographicTable: hydrographicTableName, imperviousTable: imperviousTableName,
                        rsu_lcz:results.outputTableRsuLcz,
                        rsu_urban_typo_area:results.outputTableRsuUrbanTypoArea,
                        prefixName: processing_parameters.prefixName
                )){
                    results.put("gridIndicatorsTableName", rasterizedIndicators.results.outputTableName)
                }
            }

            //Compute traffic flow
            if(processing_parameters.road_traffic){
                IProcess format =  Geoindicators.RoadIndicators.build_road_traffic()
                format.execute([
                        datasource : h2gis_datasource,
                        inputTableName: roadTableName,
                        epsg: srid])
                results.put("roadTrafficTableName", format.results.outputTableName)
            }


            if (outputFolder  && outputFiles) {
                saveOutputFiles(h2gis_datasource, id_zone, results, outputFiles, outputFolder, "bdtopo_v2_", outputSRID, reproject, deleteOutputData)
            }
            if (output_datasource ) {
                saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)

            }
            info "${id_zone} has been processed"

            return results
        }
}

/**
 * Save the geoclimate tables into geojson files
 * @param id_zone the id of the zone
 * @param results a list of tables computed by geoclimate
 * @param ouputFolder the ouput folder
 * @param outputSRID srid code to reproject the result
 * @param deleteOutputData true to delete the file if exists
 * @return
 */
def saveOutputFiles(def h2gis_datasource, def id_zone, def results, def outputFiles, def ouputFolder, def subFolderName,def outputSRID, def reproject, def deleteOutputData){
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
        if(it == "building_indicators"){
            saveTableAsGeojson(results.outputTableBuildingIndicators, "${subFolder.getAbsolutePath()+File.separator+"building_indicators"}.geojson",h2gis_datasource,outputSRID, reproject, deleteOutputData)
        }
        else if(it == "block_indicators"){
            saveTableAsGeojson(results.outputTableBlockIndicators, "${subFolder.getAbsolutePath()+File.separator+"block_indicators"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else  if(it == "rsu_indicators"){
            saveTableAsGeojson(results.outputTableRsuIndicators, "${subFolder.getAbsolutePath()+File.separator+"rsu_indicators"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else  if(it == "rsu_lcz"){
            saveTableAsGeojson(results.outputTableRsuLcz,  "${subFolder.getAbsolutePath()+File.separator+"rsu_lcz"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else  if(it == "zones"){
            saveTableAsGeojson(results.zoneTableName,  "${subFolder.getAbsolutePath()+File.separator+"zones"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        //Save input GIS tables
        else  if(it == "building"){
            saveTableAsGeojson(results.buildingTableName, "${subFolder.getAbsolutePath()+File.separator+"building"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it == "road"){
            saveTableAsGeojson(results.roadTableName,  "${subFolder.getAbsolutePath()+File.separator+"road"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it == "rail"){
            saveTableAsGeojson(results.railTableName,  "${subFolder.getAbsolutePath()+File.separator+"rail"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        if(it == "water"){
            saveTableAsGeojson(results.hydrographicTableName, "${subFolder.getAbsolutePath()+File.separator+"water"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it == "vegetation"){
            saveTableAsGeojson(results.vegetationTableName,  "${subFolder.getAbsolutePath()+File.separator+"vegetation"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it == "impervious"){
            saveTableAsGeojson(results.imperviousTableName, "${subFolder.getAbsolutePath()+File.separator+"impervious"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
        else if(it == "urban_areas"){
            saveTableAsGeojson(results.urbanAreasTableName, "${subFolder.getAbsolutePath()+File.separator+"urban_areas"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it == "rsu_urban_typo_area"){
            saveTableAsGeojson(results.outputTableRsuUrbanTypoArea, "${subFolder.getAbsolutePath()+File.separator+"rsu_urban_typo_area"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it == "rsu_urban_typo_floor_area"){
            saveTableAsGeojson(results.outputTableRsuUrbanTypoFloorArea, "${subFolder.getAbsolutePath()+File.separator+"rsu_urban_typo_floor_area"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it == "building_urban_typo"){
            saveTableAsGeojson(results.outputTableBuildingUrbanTypo, "${subFolder.getAbsolutePath()+File.separator+"building_urban_typo"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it == "grid_indicators"){
            saveTableAsGeojson(results.gridIndicatorsTableName, "${subFolder.getAbsolutePath()+File.separator+"grid_indicators"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else if(it == "road_traffic"){
            saveTableAsGeojson(results.roadTrafficTableName,  "${subFolder.getAbsolutePath()+File.separator+"road_traffic"}.geojson",h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }else  if(it == "population"){
            saveTableAsGeojson(results.populationTableName, "${subFolder.getAbsolutePath()+File.separator+"population"}.geojson", h2gis_datasource,outputSRID,reproject,deleteOutputData)
        }
    }
}

/**
 * Method to save a table into a geojson file
 * @param outputTable name of the table to export
 * @param filePath path to save the table
 * @param h2gis_datasource connection to the database
 * @param outputSRID srid code to reproject the outputTable.
 * @param  deleteOutputData true to delete the file if exists
 */
def saveTableAsGeojson(def outputTable , def filePath,def h2gis_datasource,def outputSRID, def reproject, def deleteOutputData){
    if(outputTable && h2gis_datasource.hasTable(outputTable)){
        if(!reproject){
            h2gis_datasource.save(outputTable, filePath, deleteOutputData)
        }else{
            if(h2gis_datasource.getTable(outputTable).getRowCount()>0){
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
def saveTablesInDatabase(def output_datasource, def h2gis_datasource, def outputTableNames, def h2gis_tables, def id_zone, def inputSRID, def outputSRID, def reproject ){
    Connection con = output_datasource.getConnection()
    con.setAutoCommit(true);

    //Export building indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_indicators,id_zone,h2gis_datasource, h2gis_tables.outputTableBuildingIndicators
                ,  "where id_zone!='outside'",  inputSRID, outputSRID,reproject)


    //Export block indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.block_indicators, id_zone, h2gis_datasource, h2gis_tables.outputTableBlockIndicators
            , "where ID_RSU IS NOT NULL",  inputSRID, outputSRID,reproject)

    //Export rsu indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_indicators, id_zone, h2gis_datasource, h2gis_tables.outputTableRsuIndicators
            ,  "",  inputSRID, outputSRID,reproject)

    //Export rsu lcz
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_lcz, id_zone, h2gis_datasource, h2gis_tables.outputTableRsuLcz
            ,  "",  inputSRID, outputSRID,reproject)

    //Export rsu_urban_typo_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_urban_typo_area,id_zone,h2gis_datasource, h2gis_tables.outputTableRsuUrbanTypoArea
            , "",inputSRID,outputSRID,reproject)

    //Export rsu_urban_typo_floor_area
    indicatorTableBatchExportTable(output_datasource, outputTableNames.rsu_urban_typo_floor_area,id_zone,h2gis_datasource, h2gis_tables.outputTableRsuUrbanTypoFloorArea
            , "",inputSRID,outputSRID,reproject)

    //Export building_urban_typo
    indicatorTableBatchExportTable(output_datasource, outputTableNames.building_urban_typo,id_zone,h2gis_datasource, h2gis_tables.outputTableBuildingUrbanTypo
            , "",inputSRID,outputSRID,reproject)

    //Export grid_indicators
    indicatorTableBatchExportTable(output_datasource, outputTableNames.grid_indicators,id_zone,h2gis_datasource, h2gis_tables.gridIndicatorsTableName
            , "",inputSRID,outputSRID,reproject)

    //Export road_traffic
    indicatorTableBatchExportTable(output_datasource, outputTableNames.road_traffic, id_zone,h2gis_datasource, h2gis_tables.roadTrafficTableName
            , "", inputSRID,outputSRID,reproject)

    //Export zone
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.zones, id_zone, h2gis_datasource, h2gis_tables.zoneTableName
            ,  "",  inputSRID, outputSRID,reproject)

    //Export building
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.building, id_zone, h2gis_datasource, h2gis_tables.buildingTableName
            , "",inputSRID, outputSRID,reproject)

    //Export road
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.road, id_zone,h2gis_datasource, h2gis_tables.roadTableName
            ,  "",inputSRID, outputSRID,reproject)
    //Export rail
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.rail, id_zone, h2gis_datasource, h2gis_tables.railTableName
            ,  "",inputSRID, outputSRID,reproject)
    //Export vegetation
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.vegetation, id_zone, h2gis_datasource, h2gis_tables.vegetationTableName
            ,  "",inputSRID, outputSRID,reproject)
    //Export water
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.water, id_zone, h2gis_datasource, h2gis_tables.hydrographicTableName
            ,  "",inputSRID, outputSRID,reproject)
    //Export impervious
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.impervious, id_zone, h2gis_datasource, h2gis_tables.imperviousTableName
            ,  "",inputSRID, outputSRID,reproject)

    //Export population table
    abstractModelTableBatchExportTable(output_datasource, outputTableNames.population, id_zone,h2gis_datasource, h2gis_tables.populationTableName
            , "", inputSRID,outputSRID,reproject)


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
def abstractModelTableBatchExportTable(def output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter,def inputSRID,def outputSRID, def reproject){
    if(output_table) {
        if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
            if (output_datasource.hasTable(output_table)) {
                output_datasource.execute("DELETE FROM $output_table WHERE id_zone= '$id_zone'".toString());
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
                        def diffCols = inputColumns.keySet().findAll { e -> !outPutColumnsNames*.toLowerCase().contains(e.toLowerCase()) }
                        def alterTable = ""
                        if (diffCols) {
                            inputColumns.each { entry ->
                                if (diffCols.contains(entry.key)) {
                                    //DECFLOAT is not supported by POSTSGRESQL
                                    def dataType = entry.value.equalsIgnoreCase("decfloat")?"FLOAT":entry.value
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
            }else {
                def tmpTable =null
                debug "Start to export the table $h2gis_table_to_save into the table $output_table"
                if (filter) {
                    if(!reproject){
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().save(output_datasource, output_table, true);
                    }
                    else{
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).filter(filter).getSpatialTable().reproject(outputSRID).save(output_datasource, output_table, true);
                        //Because the select query reproject doesn't contain any geometry metadata
                        output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString());

                    }
                    if(tmpTable){
                    //Workarround to update the SRID on resulset
                    output_datasource.execute"""ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $inputSRID) USING ST_SetSRID(the_geom,$inputSRID);""".toString()
                    }
                } else {
                    if(!reproject){
                        tmpTable = h2gis_datasource.getTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                    }else{
                        tmpTable = h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                        //Because the select query reproject doesn't contain any geometry metadata
                        output_datasource.execute("""ALTER TABLE $output_table
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID)
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString());

                    }
                }
                if(tmpTable) {
                    output_datasource.execute("UPDATE $output_table SET id_zone= $id_zone".toString());
                    output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""".toString())
                    debug "The table $h2gis_table_to_save has been exported into the table $output_table"
                }else{
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
def indicatorTableBatchExportTable(def output_datasource, def output_table, def id_zone, def h2gis_datasource, h2gis_table_to_save, def filter, def inputSRID, def outputSRID,def reproject){
    if(output_table){
        if(h2gis_table_to_save) {
            if (h2gis_datasource.hasTable(h2gis_table_to_save)) {
                if (output_datasource.hasTable(output_table)) {
                    output_datasource.execute("DELETE FROM $output_table WHERE id_zone='$id_zone'".toString());
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
                                        //DECFLOAT is not supported by POSTSGRESQL
                                        def dataType = entry.value.equalsIgnoreCase("decfloat")?"FLOAT":entry.value
                                        alterTable += "ALTER TABLE $output_table ADD COLUMN $entry.key $dataType;"
                                        outputColumns.put(entry.key, dataType)
                                    }
                                }
                                output_datasource.execute(alterTable.toString())
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
                            output_datasource.withBatch(BATCH_MAX_SIZE, insertTable.toString()) { ps ->
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
                            info "The table $h2gis_table_to_save has been exported into the table $output_table"
                        }
                    }
                } else {
                    def tmpTable =null
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
                                output_datasource.execute"""ALTER TABLE $output_table ALTER COLUMN the_geom TYPE geometry(GEOMETRY, $outputSRID) USING ST_SetSRID(the_geom,$outputSRID);""".toString()
                            }
                        }
                    } else {
                        if (!reproject) {
                            tmpTable =  h2gis_datasource.getSpatialTable(h2gis_table_to_save).save(output_datasource, output_table, true);
                        } else {
                            tmpTable =   h2gis_datasource.getSpatialTable(h2gis_table_to_save).reproject(outputSRID).save(output_datasource, output_table, true);
                            if(tmpTable){
                                //Because the select query reproject doesn't contain any geometry metadata
                                output_datasource.execute("""ALTER TABLE $output_table 
                            ALTER COLUMN the_geom TYPE geometry(geometry, $outputSRID) 
                            USING ST_SetSRID(the_geom,$outputSRID);""".toString())
                            }
                        }
                    }
                    if (tmpTable){
                        if(!output_datasource.getTable(output_table).hasColumn("id_zone")) {
                            output_datasource.execute("ALTER TABLE $output_table ADD COLUMN id_zone VARCHAR".toString());
                        }
                        output_datasource.execute("UPDATE $output_table SET id_zone= $id_zone".toString());
                        output_datasource.execute("""CREATE INDEX IF NOT EXISTS idx_${output_table.replaceAll(".", "_")}_id_zone  ON $output_table (ID_ZONE)""".toString())
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
                distBuffer: 500,
                distance: 1000,
                idZone: String,
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
        outputs outputBuilding: String, outputRoad: String, outputRail: String, outputHydro: String, outputVeget: String, outputImpervious: String, outputZone: String
        run { datasource, distBuffer, distance, idZone, tableCommuneName, tableBuildIndifName, tableBuildIndusName, tableBuildRemarqName, tableRoadName, tableRailName,
              tableHydroName, tableVegetName, tableImperviousSportName, tableImperviousBuildSurfName, tableImperviousRoadSurfName, tableImperviousActivSurfName,
              tablePiste_AerodromeName, tableReservoirName,hLevMin, hLevMax, hThresholdLev2 ->

            if(!hLevMin){
                hLevMin=3
            }
            if(!hLevMax){
                hLevMax = 15
            }
            if(!hThresholdLev2){
                hThresholdLev2 = 10
            }
            if (!datasource) {
                error "The database to store the BD Topo data doesn't exist"
                return
            }

            //Init model
            def initParametersAbstract = BDTopo_V2.InputDataLoading.initParametersAbstract()
            if (!initParametersAbstract(datasource: datasource)) {
                error "Cannot initialize the geoclimate data model."
                return
            }
            def abstractTables = initParametersAbstract.results

            //Init BD Topo parameters
            def initTypes = BDTopo_V2.InputDataLoading.initTypes()
            if (!initTypes([datasource             : datasource,
                            buildingAbstractUseType: abstractTables.outputBuildingAbstractUseType,
                            roadAbstractType       : abstractTables.outputRoadAbstractType, roadAbstractCrossing: abstractTables.outputRoadAbstractCrossing,
                            railAbstractType       : abstractTables.outputRailAbstractType, railAbstractCrossing: abstractTables.outputRailAbstractCrossing,
                            vegetAbstractType      : abstractTables.outputVegetAbstractType])) {
                error "Cannot initialize the BD Topo parameters."
                return
            }
            def initTables = initTypes.results

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
                                   distBuffer                  : distBuffer,
                                   distance                    : distance,
                                   idZone                      : idZone,
                                   building_bd_topo_use_type   : initTables.outputBuildingBDTopoUseType,
                                   building_abstract_use_type  : abstractTables.outputBuildingAbstractUseType,
                                   road_bd_topo_type           : initTables.outputroadBDTopoType,
                                   road_abstract_type          : abstractTables.outputRoadAbstractType,
                                   road_bd_topo_crossing       : initTables.outputroadBDTopoCrossing,
                                   road_abstract_crossing      : abstractTables.outputRoadAbstractCrossing,
                                   rail_bd_topo_type           : initTables.outputrailBDTopoType,
                                   rail_abstract_type          : abstractTables.outputRailAbstractType,
                                   rail_bd_topo_crossing       : initTables.outputrailBDTopoCrossing,
                                   rail_abstract_crossing      : abstractTables.outputRailAbstractCrossing,
                                   veget_bd_topo_type          : initTables.outputvegetBDTopoType,
                                   veget_abstract_type         : abstractTables.outputVegetAbstractType])) {
                error "Cannot import preprocess."
                return
            }
            def preprocessTables = importPreprocess.results

            // Input data formatting and statistics
            def inputDataFormatting = BDTopo_V2.InputDataFormatting.formatData()
            if (!inputDataFormatting([datasource                : datasource,
                                      inputBuilding             : preprocessTables.outputBuildingName,
                                      inputRoad                 : preprocessTables.outputRoadName,
                                      inputRail                 : preprocessTables.outputRailName,
                                      inputHydro                : preprocessTables.outputHydroName,
                                      inputVeget                : preprocessTables.outputVegetName,
                                      inputImpervious           : preprocessTables.outputImperviousName,
                                      inputZone                 : preprocessTables.outputZoneName,
                                      hLevMin                   : hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2, idZone: idZone,
                                      buildingAbstractParameters: abstractTables.outputBuildingAbstractParameters,
                                      roadAbstractParameters    : abstractTables.outputRoadAbstractParameters,
                                      vegetAbstractParameters   : abstractTables.outputVegetAbstractParameters])) {
                error "Cannot format data and compute statistics."
                return
            }

            debug "End of the BD Topo extract transform process."

            def finalBuildings = inputDataFormatting.results.outputBuilding
            def finalRoads = inputDataFormatting.results.outputRoad
            def finalRails = inputDataFormatting.results.outputRail
            def finalHydro = inputDataFormatting.results.outputHydro
            def finalVeget = inputDataFormatting.results.outputVeget
            def finalImpervious = inputDataFormatting.results.outputImpervious
            def finalZone = inputDataFormatting.results.outputZone

            [outputBuilding: finalBuildings, outputRoad: finalRoads, outputRail: finalRails, outputHydro: finalHydro,
             outputVeget   : finalVeget, outputImpervious: finalImpervious, outputZone: finalZone]

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
