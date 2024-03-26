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
import org.orbisgis.data.H2GIS
import org.orbisgis.data.api.dataset.ITable
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.geoclimate.osmtools.OSMTools
import org.orbisgis.geoclimate.osmtools.utils.OSMElement
import org.orbisgis.geoclimate.worldpoptools.WorldPopTools

import java.sql.SQLException

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
 *  *             "folder" : "/tmp/myResultFolder" //tmp folder to store the computed layers in a fgb format,
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
Map workflow(def input) {
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

    def osm_date= inputParameters.get("date")

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
        overpass_enpoint = "https://overpass-api.de/api"
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
        Map osmprocessing = osm_processing(h2gis_datasource, processing_parameters, locations.findAll { it }, file_outputFolder, outputFileTables,
                outputDatasource, outputTables, outputSRID, downloadAllOSMData, deleteOutputData, deleteOSMFile, logTableZones, osm_size_area,
                overpass_timeout, overpass_maxsize, osm_date)
        if (!osmprocessing) {
            h2gis_datasource.save(logTableZones,"${file_outputFolder.getAbsolutePath() + File.separator}logzones.fgb", true)
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
        return osmprocessing

    } else {
        error "Invalid  OSM area from $locations"
        return null
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
Map osm_processing(JdbcDataSource h2gis_datasource, def processing_parameters, def id_zones,
                   File outputFolder, def ouputTableFiles, def output_datasource, def outputTableNames,
                   def outputSRID, def downloadAllOSMData,
                   def deleteOutputData, def deleteOSMFile,
                   def logTableZones, def bbox_size,
                   def overpass_timeout, def overpass_maxsize,def overpass_date) {
    //Store the zone identifier and the names of the tables
    def outputTableNamesResult = [:]
    //Create the table to log on the processed zone
    h2gis_datasource.execute """DROP TABLE IF EXISTS $logTableZones;
            CREATE TABLE $logTableZones (the_geom GEOMETRY(GEOMETRY, 4326), 
            location VARCHAR, info VARCHAR, version  VARCHAR, build_number VARCHAR);""".toString()
    int nbAreas = id_zones.size()
    info "$nbAreas osm areas will be processed"
    id_zones.each { id_zone ->
        //Extract the zone table and read its SRID
        def zones = extractOSMZone(h2gis_datasource, id_zone, processing_parameters.distance, bbox_size)
        if (zones) {
            id_zone = id_zone in Collection ? id_zone.join('_') : id_zone
            def utm_zone_table = zones.utm_zone_table
            def utm_extended_bbox_table = zones.utm_extended_bbox_table
            if (h2gis_datasource.getRowCount(utm_zone_table) == 0) {
                error "Cannot find any geometry to define the zone to extract the OSM data"
                return
            }
            def srid = zones.utm_srid
            def reproject = false
            if (outputSRID) {
                if (outputSRID != srid) {
                    reproject = true
                }
            } else {
                outputSRID = srid
            }
            //Prepare OSM extraction from the osm_envelope_extented
            //TODO set key values ?
            def osm_date=""
            if(overpass_date){
               osm_date = "[date:\"$overpass_date\"]"
            }
            def query = "[timeout:$overpass_timeout][maxsize:$overpass_maxsize]$osm_date" + OSMTools.Utilities.buildOSMQuery(zones.osm_envelope_extented, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

            if (downloadAllOSMData) {
                //Create a custom OSM query to download all requiered data. It will take more time and resources
                //because much more OSM elements will be returned
                def keysValues = ["building", "railway", "amenity",
                                  "leisure", "highway", "natural",
                                  "landuse", "landcover",
                                  "vegetation", "waterway", "area", "aeroway", "area:aeroway", "tourism", "sport"]
                query = "[timeout:$overpass_timeout][maxsize:$overpass_maxsize]$osm_date" + OSMTools.Utilities.buildOSMQueryWithAllData(zones.osm_envelope_extented, keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
            }

            def extract = OSMTools.Loader.extract(query)
            if (extract) {
                //We must build the GIS layers on the extended bbox area
                Geometry utm_extented_geom = h2gis_datasource.getExtent(utm_extended_bbox_table)
                utm_extented_geom.setSRID(srid)
                Map gisLayersResults = OSM.InputDataLoading.createGISLayers(h2gis_datasource, extract, utm_extented_geom, srid)
                if (gisLayersResults) {
                    if (deleteOSMFile) {
                        if (new File(extract).delete()) {
                            debug "The osm file ${extract}has been deleted"
                        }
                    }
                    def rsu_indicators_params = processing_parameters.rsu_indicators
                    def grid_indicators_params = processing_parameters.grid_indicators
                    def road_traffic = processing_parameters.road_traffic
                    def worldpop_indicators = processing_parameters.worldpop_indicators

                    info "Formating OSM GIS layers"
                    //Format urban areas
                    String urbanAreasTable = OSM.InputDataFormatting.formatUrbanAreas(h2gis_datasource, gisLayersResults.urban_areas, utm_extended_bbox_table)

                    info "Urban areas formatted"
                    /*
                     * Do not filter the data when formatting becausethe job is already done when extracting osm data                     *
                     */
                    Map formatBuilding = OSM.InputDataFormatting.formatBuildingLayer(
                            h2gis_datasource, gisLayersResults.building,
                            null, urbanAreasTable,
                            processing_parameters.hLevMin)

                    info "Building formatted"
                    def buildingTableName = formatBuilding.building
                    def buildingEstimateTableName = formatBuilding.building_estimated

                    String railTableName = OSM.InputDataFormatting.formatRailsLayer(h2gis_datasource, gisLayersResults.rail, null)

                    info "Rail formatted"

                    String vegetationTableName = OSM.InputDataFormatting.formatVegetationLayer(h2gis_datasource, gisLayersResults.vegetation, utm_extended_bbox_table)

                    info "Vegetation formatted"

                    String hydrographicTableName = OSM.InputDataFormatting.formatWaterLayer(
                            h2gis_datasource, gisLayersResults.water,
                            utm_extended_bbox_table)

                    info "Water formatted"

                    String imperviousTableName = OSM.InputDataFormatting.formatImperviousLayer(
                            h2gis_datasource, gisLayersResults.impervious, utm_extended_bbox_table)

                    info "Impervious formatted"

                    //Sea/Land mask
                    String seaLandMaskTableName = OSM.InputDataFormatting.formatSeaLandMask(
                            h2gis_datasource, gisLayersResults.coastline, utm_extended_bbox_table, hydrographicTableName)

                    info "Sea/Land formatted"

                    if(h2gis_datasource.getRowCount(seaLandMaskTableName)>0){
                    //Select the water and sea features
                    h2gis_datasource.execute """Drop table if exists $hydrographicTableName;
                    CREATE TABLE $hydrographicTableName as select the_geom, id as id_water, cast(0 as integer) as zindex, type from $seaLandMaskTableName where type in ('water', 'sea') """.toString()
                    }

                    //Format road
                    String roadTableName = OSM.InputDataFormatting.formatRoadLayer(
                            h2gis_datasource, gisLayersResults.road,
                            utm_extended_bbox_table)

                    info "Road formatted"

                    info "All layers have been formatted"

                    //Drop the intermediate GIS layers
                    h2gis_datasource.dropTable(gisLayersResults.values().toArray(new String[0]))

                    debug "OSM GIS layers formated"
                    //Add the GIS layers to the list of results
                    def results = [:]
                    results.put("zone", utm_zone_table)
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
                        String format_traffic = Geoindicators.RoadIndicators.build_road_traffic(h2gis_datasource, roadTableName)
                        results.put("road_traffic", format_traffic)
                    }

                    //Compute the RSU indicators
                    if (rsu_indicators_params.indicatorUse) {
                        String estimateHeight = rsu_indicators_params."estimateHeight" ? "BUILDING_HEIGHT_OSM_RF_2_2.model" : ""
                        rsu_indicators_params.put("utrfModelName", "UTRF_OSM_RF_2_2.model")
                        rsu_indicators_params.put("buildingHeightModelName", estimateHeight)
                        Map geoIndicators = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(
                                h2gis_datasource, utm_zone_table,
                                buildingTableName, roadTableName,
                                railTableName, vegetationTableName,
                                hydrographicTableName, imperviousTableName,
                                buildingEstimateTableName,
                                seaLandMaskTableName,
                                urbanAreasTable,"",
                                rsu_indicators_params,
                                processing_parameters.prefixName)
                        if (!geoIndicators) {
                            error "Cannot build the geoindicators for the zone $id_zone"
                            h2gis_datasource.execute("""
                            INSERT INTO $logTableZones VALUES(st_geomfromtext('${zones.geometry}',4326) ,
                            '$id_zone', 'Error computing geoindicators', 
                            '${Geoindicators.version()}',
                            '${Geoindicators.buildNumber()}' )""".toString())
                            return
                        } else {
                            results.putAll(geoIndicators)
                        }
                    }

                    //Extract and compute population indicators for the specified year
                    //This data can be used by the grid_indicators process
                    if (worldpop_indicators) {
                        def bbox = [zones.envelope.getMinY() as Float, zones.envelope.getMinX() as Float,
                                    zones.envelope.getMaxY() as Float, zones.envelope.getMaxX() as Float]
                        String coverageId = "wpGlobal:ppp_2020"
                        String worldPopFile = WorldPopTools.Extract.extractWorldPopLayer(coverageId, bbox)
                        if (worldPopFile) {
                            String worldPopTableName = WorldPopTools.Extract.importAscGrid(h2gis_datasource, worldPopFile, srid, coverageId.replaceAll(":", "_"))
                            if (worldPopTableName) {
                                results.put("population", worldPopTableName)
                                String buildingWithPop = Geoindicators.BuildingIndicators.buildingPopulation(h2gis_datasource, results.building, worldPopTableName, ["pop"])
                                h2gis_datasource.dropTable(worldPopTableName)
                                if (!buildingWithPop) {
                                    info "Cannot compute any population data at building level"
                                }
                                else{
                                    h2gis_datasource.dropTable(results.building)
                                    //Update the building table with the population data
                                    results.put("building", buildingWithPop)
                                }

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
                    }
                    def noise_indicators = processing_parameters.noise_indicators

                    def geomEnv;
                    if (noise_indicators) {
                        if (noise_indicators.ground_acoustic) {
                            geomEnv = h2gis_datasource.getSpatialTable(utm_zone_table).getExtent()
                            def outputTable = Geoindicators.SpatialUnits.createGrid(h2gis_datasource, geomEnv, 200, 200)
                            if (outputTable) {
                                String ground_acoustic = Geoindicators.NoiseIndicators.groundAcousticAbsorption(h2gis_datasource, outputTable, "id_grid",
                                        results.building, roadTableName, hydrographicTableName,
                                        vegetationTableName, imperviousTableName)
                                if (ground_acoustic) {
                                    results.put("ground_acoustic", ground_acoustic)
                                }
                                h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTable".toString())
                            }
                        }
                    }
                    //Default
                    def outputGrid = "fgb"
                    if (grid_indicators_params) {
                        if (!geomEnv) {
                            geomEnv = h2gis_datasource.getSpatialTable(utm_zone_table).getExtent()
                        }
                        outputGrid = grid_indicators_params.output
                        def x_size = grid_indicators_params.x_size
                        def y_size = grid_indicators_params.y_size
                        String grid = Geoindicators.WorkflowGeoIndicators.createGrid(h2gis_datasource, geomEnv,
                                x_size, y_size, srid, grid_indicators_params.rowCol)
                        if (grid) {
                            String rasterizedIndicators = Geoindicators.WorkflowGeoIndicators.rasterizeIndicators(h2gis_datasource, grid,
                                    grid_indicators_params.indicators,
                                    results.building, roadTableName, vegetationTableName,
                                    hydrographicTableName, imperviousTableName,
                                    results.rsu_lcz,
                                    results.rsu_utrf_area,
                                    results.rsu_utrf_floor_area,
                                    seaLandMaskTableName,
                                    processing_parameters.prefixName)
                            if (rasterizedIndicators) {
                                h2gis_datasource.dropTable(grid)
                                results.put("grid_indicators", rasterizedIndicators)
                                if(grid_indicators_params.lzc_lod && grid_indicators_params.indicators.contains("LCZ_PRIMARY")){

                                }
                            }
                        } else {
                            info "Cannot create a grid to aggregate the indicators"
                            h2gis_datasource.execute("""INSERT INTO $logTableZones 
                            VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error computing the grid indicators'
                            '${Geoindicators.version()}',
                            '${Geoindicators.buildNumber()}')""".toString())
                        }
                    }

                    if (outputFolder && ouputTableFiles) {
                        saveOutputFiles(h2gis_datasource, id_zone, results, ouputTableFiles, outputFolder, "osm_", outputSRID, reproject, deleteOutputData, outputGrid)

                    }
                    if (output_datasource) {
                        saveTablesInDatabase(output_datasource, h2gis_datasource, outputTableNames, results, id_zone, srid, outputSRID, reproject)
                    }

                    outputTableNamesResult.put(id_zone in Collection ? id_zone.join("_") : id_zone, results.findAll { it.value != null })

                    h2gis_datasource.dropTable(Geoindicators.getCachedTableNames())

                } else {
                    h2gis_datasource.execute("""INSERT INTO $logTableZones 
                    VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error loading the OSM file', 
                            '${Geoindicators.version()}',
                            '${Geoindicators.buildNumber()}')""".toString())
                    error "Cannot load the OSM file ${extract}"
                    return
                }
            } else {
                //Log in table
                h2gis_datasource.execute("""INSERT INTO $logTableZones 
                VALUES(st_geomfromtext('${zones.geometry}',4326) ,'$id_zone', 'Error to extract the data with OverPass'
                ,'${Geoindicators.version()}', '${Geoindicators.buildNumber()}')""".toString())
                error "Cannot execute the overpass query $query"
                return
            }
        } else {
            //Log in table
            h2gis_datasource.execute("""INSERT INTO $logTableZones 
            VALUES(null,'$id_zone', 'Error to extract the zone with Nominatim', 
                            '${Geoindicators.version()}',
                            '${Geoindicators.buildNumber()}')""".toString())
            return
        }
    }
    if (outputTableNamesResult) {
        return outputTableNamesResult
    }
}

/**
 * Extract the OSM zone and its envelope area from Nominatim API
 *
 * @param datasource a connexion to the local H2GIS database
 * @param zoneToExtract the osm filter : place or bbox
 * @param distance to expand the OSM bbox
 * @return
 * utm_zone_table the geometries that represents the processed zone in UTM coordinate system
 * utm_extended_bbox_table the envelope of the zone to be processed extended by a distance in UTM coordinate system
 * osm_envelope_extented the bbox used to query overpass in lat/lon
 * osm_geometry the geometry that represents the processed zone in lat/lon
 * utm_srid the UTM srid code
 */
def extractOSMZone(def datasource, def zoneToExtract, def distance, def bbox_size) {
    def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}".toString()
    def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}".toString()
    if (zoneToExtract) {
        def GEOMETRY_TYPE = "GEOMETRY"
        Geometry geom = OSMTools.Utilities.getArea(zoneToExtract)
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
         * Expand it with a distance in meters
         */
        def lat_lon_bbox_extended = GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

        //Find the best utm zone
        //Reproject the geometry and its envelope to the UTM zone
        def con = datasource.getConnection()
        def interiorPoint = lat_lon_bbox_extended.centre()
        def epsg = GeographyUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
        def source_geom_utm = ST_Transform.ST_Transform(con, geom, epsg)

        //Check the size of the bbox
        if ((source_geom_utm.getEnvelopeInternal().getArea() / 1.0E+6) >= bbox_size) {
            error("The size of the OSM BBOX is greated than the limit : ${bbox_size} in km².\n" +
                    "Please increase the area parameter if you want to skip this limit.")
            return null
        }
        def lat_lon_bbox_geom_extended = geom.getFactory().toGeometry(lat_lon_bbox_extended)
        lat_lon_bbox_geom_extended.setSRID(4326)

        datasource.execute """drop table if exists ${outputZoneTable}; 
        create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${source_geom_utm.toString()}', ${epsg}), '${zoneToExtract.toString()}');""".toString()

        datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; 
         create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(con, lat_lon_bbox_geom_extended, epsg).toString()}',${epsg}), '${zoneToExtract.toString()}');
        """.toString()

        return ["utm_zone_table"         : outputZoneTable,
                "utm_extended_bbox_table": outputZoneEnvelopeTable,
                "osm_envelope_extented"     : lat_lon_bbox_extended,
                "osm_geometry"     : geom,
                "utm_srid"     : epsg
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
def extractProcessingParameters(def processing_parameters) {
    def defaultParameters = [distance: 0f, prefixName: "",
                             hLevMin : 3]
    def rsu_indicators_default = [indicatorUse      : [],
                                  svfSimplified     : true,
                                  surface_vegetation: 10000f,
                                  surface_hydro     : 2500f,
                                  surface_urban_areas  : 10000f,
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
            def surface_urbanAreasP = rsu_indicators.surface_urban_areas
            if (surface_urbanAreasP && surface_urbanAreasP in Number) {
                rsu_indicators_default.surface_urban_areas = surface_urbanAreasP
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
                Map defaultmapOfWeights = rsu_indicators_default.mapOfWeights
                if ((defaultmapOfWeights + mapOfWeightsP).size() != defaultmapOfWeights.size()) {
                    error("The number of mapOfWeights parameters must contain exactly the parameters ${defaultmapOfWeights.keySet().join(",")}")
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
                                               "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "UTRF_AREA_FRACTION","UTRF_FLOOR_AREA_FRACTION",
                                               "LCZ_FRACTION", "LCZ_PRIMARY", "FREE_EXTERNAL_FACADE_DENSITY",
                                               "BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY", "BUILDING_HEIGHT_DIST",
                                               "FRONTAL_AREA_INDEX", "SEA_LAND_FRACTION", "ASPECT_RATIO", "SVF",
                                               "HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS_CLASS"]
                def allowedOutputIndicators = allowed_grid_indicators.intersect(list_indicators*.toUpperCase())
                if (allowedOutputIndicators) {
                    //Update the RSU indicators list according the grid indicators
                    list_indicators.each { val ->
                        if (val.trim().toUpperCase() in ["LCZ_FRACTION", "LCZ_PRIMARY"]) {
                            rsu_indicators.indicatorUse << "LCZ"
                        } else if (val.trim().toUpperCase() in ["UTRF_AREA_FRACTION", "UTRF_FLOOR_AREA_FRACTION"]) {
                            rsu_indicators.indicatorUse << "UTRF"
                        }
                    }
                    def grid_indicators_tmp = [
                            "x_size"    : x_size,
                            "y_size"    : y_size,
                            "output"    : "fgb",
                            "rowCol"    : false,
                            "indicators": allowedOutputIndicators
                    ]
                    def grid_output = grid_indicators.output
                    if (grid_output) {
                        if (grid_output.toLowerCase() in ["asc", "fgb"]) {
                            grid_indicators_tmp.output = grid_output.toLowerCase()
                        }
                    }
                    def grid_rowCol = grid_indicators.rowCol
                    if (grid_rowCol && grid_rowCol in Boolean) {
                        grid_indicators_tmp.rowCol = grid_rowCol
                    }

                    def lcz_lod = grid_indicators.lcz_lod

                    if (lcz_lod && lcz_lod in Integer) {
                        grid_indicators_tmp.put("lcz_lod", lcz_lod)
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
 * Save the geoclimate tables into files
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
            if (outputGrid == "fgb") {
                Geoindicators.WorkflowUtilities.saveInFile(results."$it", "${subFolder.getAbsolutePath() + File.separator + it}.fgb", h2gis_datasource, outputSRID, reproject, deleteOutputData)
            } else if (outputGrid == "asc") {
                Geoindicators.WorkflowUtilities.saveToAscGrid(results."$it", subFolder.getAbsolutePath(), it, h2gis_datasource, outputSRID, reproject, deleteOutputData)
            }
        } else if (it == "building_height_missing") {
            Geoindicators.WorkflowUtilities.saveToCSV(results."$it", "${subFolder.getAbsolutePath() + File.separator + it}.csv", h2gis_datasource, deleteOutputData)
        } else {
            Geoindicators.WorkflowUtilities.saveInFile(results."$it", "${subFolder.getAbsolutePath() + File.separator + it}.fgb", h2gis_datasource, outputSRID, reproject, deleteOutputData)
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
            IOMethods.exportToDataBase(h2gis_datasource.getConnection(), "(SELECT ID_BUILD, ID_SOURCE, '$id_zone' as ID_ZONE from $h2gis_table_to_save)".toString(),
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
Map buildGeoclimateLayers(JdbcDataSource datasource, Object zoneToExtract,
                          float distance = 500, int hLevMin = 3) {
    if (datasource == null) {
        error "Cannot access to the database to store the osm data"
        return
    }

    debug "Building OSM GIS layers"
    Map res = OSM.InputDataLoading.extractAndCreateGISLayers(datasource, zoneToExtract, distance)
    if (res) {
        debug "OSM GIS layers created"
        def buildingTableName = res.building
        def roadTableName = res.road
        def railTableName = res.rail
        def vegetationTableName = res.vegetation
        def hydroTableName = res.water
        def imperviousTableName = res.impervious
        def zone = res.zone
        def zoneEnvelopeTableName = res.zone_envelope
        def epsg = datasource.getSpatialTable(zone).srid
        if (zoneEnvelopeTableName != null) {
            debug "Formating OSM GIS layers"
            Map buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(datasource, buildingTableName, zoneEnvelopeTableName)
            buildingTableName = buildingLayers.building

            roadTableName = OSM.InputDataFormatting.formatRoadLayer(datasource, roadTableName, zoneEnvelopeTableName)

            railTableName = OSM.InputDataFormatting.formatRailsLayer(datasource, railTableName, zoneEnvelopeTableName)

            vegetationTableName = OSM.InputDataFormatting.formatVegetationLayer(datasource, vegetationTableName, zoneEnvelopeTableName)

            hydroTableName = OSM.InputDataFormatting.formatWaterLayer(datasource, hydroTableName, zoneEnvelopeTableName)

            imperviousTableName = OSM.InputDataFormatting.formatImperviousLayer(datasource, imperviousTableName, zoneEnvelopeTableName)

            debug "OSM GIS layers formated"
        }

        return [building  : buildingTableName, road: roadTableName,
                rail      : railTableName, water: hydroTableName,
                vegetation: vegetationTableName, impervious: imperviousTableName,
                zone      : zone, zone_envelope: zoneEnvelopeTableName]

    }
}