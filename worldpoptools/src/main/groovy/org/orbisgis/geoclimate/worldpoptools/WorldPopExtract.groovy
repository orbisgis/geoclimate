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
package org.orbisgis.geoclimate.worldpoptools

import groovy.transform.BaseScript
import groovy.xml.XmlSlurper
import groovy.xml.slurpersupport.GPathResult
import org.cts.util.UTMUtils
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.functions.io.asc.AscReaderDriver
import org.h2gis.utilities.FileUtilities
import org.orbisgis.data.jdbc.JdbcDataSource


@BaseScript WorldPopTools pf


/**
 * This process is used to extract a grid population data from  https://www.worldpop.org
 * The grid is stored in a tmp dir.
 * If the grid already exists the file is reused.
 * @param coverageId coverage identifier on the server
 * @param bbox an array of coordinates in lat/long
 * @return the path of the extract grid, stored on a temporary directory
 */
String extractWorldPopLayer(String coverageId, List bbox) {
    info "Extract the world population grid"
    def gridRequest = createGridRequest(coverageId, bbox)
    //hash the query to cache it
    def queryHash = gridRequest.digest('SHA-256')
    def outputGridFile = new File(System.getProperty("java.io.tmpdir") + File.separator + "${queryHash}.asc")
    def popGridFilePath = outputGridFile.absolutePath
    if (outputGridFile.exists()) {
        if (outputGridFile.length() == 0) {
            outputGridFile.delete()
            if (outputGridFile.createNewFile()) {
                if (grid(gridRequest, outputGridFile)) {
                    info "The grid file has been downloaded at ${popGridFilePath}."
                } else {
                    outputGridFile.delete()
                    error "Cannot extract the grid data for the query $gridRequest"
                    return
                }
            }
        } else {
            debug "\nThe cached grid file ${popGridFilePath} will be re-used for the query :  \n$gridRequest."
        }
    } else {
        if (outputGridFile.createNewFile()) {
            if (grid(gridRequest, outputGridFile)) {
                info "The WorldPop file has been downloaded at ${popGridFilePath}."
            } else {
                outputGridFile.delete()
                error "Cannot extract the WorldPop file for the query $gridRequest"
                return
            }
        }
    }
    return popGridFilePath;
}

/**
 * Process to import and asc grid into the database
 * @param datasource a connection to the database
 * @param worldPopFilePath the path of the grid
 * @param epsg SRID code set to the grid
 * @param tableName the name of table that contains the grid data in the database
 * @return the name of the imported table
 */
String importAscGrid(JdbcDataSource datasource, String worldPopFilePath, int epsg = 4326, String tableName = "world_pop")
        throws Exception{
    info "Import the the world pop asc file"
    // The name of the outputTableName is constructed
    def outputTableWorldPopName = postfix tableName
    if (!worldPopFilePath) {
        info "Create a default empty worldpop table"
        datasource.execute("""drop table if exists $outputTableWorldPopName;
                    create table $outputTableWorldPopName (the_geom GEOMETRY(POLYGON, $epsg), ID_POP INTEGER, POP FLOAT);""".toString())
    }
    AscReaderDriver ascReaderDriver = new AscReaderDriver()
    ascReaderDriver.setAs3DPoint(false)
    ascReaderDriver.setEncoding("UTF-8")
    if (epsg != 4326) {
        def importTable = postfix "imported_grid"
        try {
            ascReaderDriver.read(datasource.getConnection(), new File(worldPopFilePath), new EmptyProgressVisitor(), importTable, 4326)
            datasource.execute("""drop table if exists $outputTableWorldPopName;
                    create table $outputTableWorldPopName as select ST_TRANSFORM(THE_GEOM, $epsg) as the_geom,
                PK AS ID_POP, Z as POP from $importTable;
                drop table if exists $importTable""".toString())
        } catch (Exception ex) {
            throw new Exception("Cannot find any worldpop data on the requested area")
        }finally {
            datasource.execute("""drop table if exists $outputTableWorldPopName;
                    create table $outputTableWorldPopName (the_geom GEOMETRY(POLYGON, $epsg), ID_POP INTEGER, POP FLOAT);""".toString())
        }
    } else {
        try {
            ascReaderDriver.read(datasource.getConnection(), new File(worldPopFilePath), new EmptyProgressVisitor(), outputTableWorldPopName, 4326)
            datasource.execute("""ALTER TABLE $outputTableWorldPopName RENAME COLUMN PK TO ID_POP;
                                ALTER TABLE $outputTableWorldPopName RENAME COLUMN Z TO POP;""".toString())
        } catch (Exception ex) {
            throw new Exception( "Cannot find any worldpop data on the requested area")
        }finally {
            datasource.execute("""drop table if exists $outputTableWorldPopName;
                    create table $outputTableWorldPopName (the_geom GEOMETRY(POLYGON, $epsg), ID_POP INTEGER, POP FLOAT);""".toString())
        }
    }
    return outputTableWorldPopName
}


/**
 * A method to create the WCS grid request
 * @param coverageId coverage identifier on the server
 * @param bbox an array of coordinates in lat/long
 * @return
 */
String createGridRequest(String coverageId, def bbox) {
    if (!coverageId) {
        error "The coverageId cannot be null or empty"
        return
    }
    def subset = buildSubset(bbox)

    if (!subset) {
        error "Cannot create the subset WCS filter"
        return
    }
    def crsOutPut = "outputCRS=http://www.opengis.net/def/crs/EPSG/0/4326"

    def WCS_SERVER = """https://ogc.worldpop.org/geoserver/ows?service=WCS&version=2.0.1&request=GetCoverage&coverageId=$coverageId&format=ArcGrid&$subset&$crsOutPut""".toString()

    return WCS_SERVER
}
/**
 * Method to execute a query on the WorldPop Web Coverage Service and
 * return grid coverage in ascii format
 *
 * @param coverageId the name of the layer on the WCS service
 * @param bbox to extract the data
 * @param epsg output epsg code, default is 4326
 * @param outputGridFile the output file
 *
 * @return True if the query has been successfully executed, false otherwise.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 */
boolean grid(String coverageId, def bbox, File outputGridFile) {
    def queryUrl = createGridRequest(coverageId, bbox)
    if (!queryUrl) {
        error "The request to the server cannot be null or empty"
        return false
    }
    if (!outputGridFile) {
        error "The output Grid File cannot be null or empty"
        return false
    }
    if (!FileUtilities.isExtensionWellFormated(outputGridFile, "asc")) {
        error "Only support asc (ArcGrid format) as output Grid File"
        return false
    }
    return grid(queryUrl, outputGridFile)
}

/**
 * Method to execute a query on the WorldPop Web Coverage Service and
 * return grid coverage in ascii format
 *
 * @param wcsRequest the worldpop WCS request
 * @param outputGridFile the output file
 *
 * @return True if the query has been successfully executed, false otherwise.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 */
boolean grid(String wcsRequest, File outputGridFile) {
    def queryUrl = new URL(wcsRequest)

    if (!wcsRequest) {
        error "The request cannot be null or empty"
        return false
    }
    if (!FileUtilities.isExtensionWellFormated(outputGridFile, "asc")) {
        error "Only support asc (ArcGrid format) as output Grid File"
        return false
    }

    final String proxyHost = System.getProperty("http.proxyHost");
    final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));
    def connection
    if (proxyHost != null) {
        def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        connection = queryUrl.openConnection(proxy) as HttpURLConnection
    } else {
        connection = queryUrl.openConnection() as HttpURLConnection
    }
    info queryUrl.toString()
    connection.connect()

    info "Executing query... $queryUrl"
    //Save the result in a file
    if (connection.responseCode == 200) {
        info "Downloading the GridCoverage data from WorldPop server in ${outputGridFile}"
        outputGridFile << connection.inputStream
        return true
    } else if (connection.responseCode == 404) {
        error "The service is not available to execute the WCS query.$queryUrl"
        return false
    } else {
        error "Cannot execute the WCS query.$queryUrl"
        return false
    }
}

/**
 * A method to test if the coverageId is available
 * @param coverageId
 * @return
 */
boolean isCoverageAvailable(String coverageId){
    String describeRequest = """https://ogc.worldpop.org/geoserver/ows?service=WCS&version=2.0.1&request=DescribeCoverage&coverageId=$coverageId""".toString()
    def queryUrl = new URL(describeRequest)
    final String proxyHost = System.getProperty("http.proxyHost");
    final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));
    def connection
    if (proxyHost != null) {
        def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        connection = queryUrl.openConnection(proxy) as HttpURLConnection
    } else {
        connection = queryUrl.openConnection() as HttpURLConnection
    }
    info queryUrl.toString()
    connection.connect()

    info "Executing query... $queryUrl"
    //Save the result in a file
    if (connection.responseCode == 200) {
            XmlSlurper xmlParser = new XmlSlurper()
        GPathResult nodes = xmlParser.parse(connection.inputStream)
        if(nodes.Exception){
            return true
        }else {
            error "The service is not available for the coverageId : $coverageId"
            return false
        }
    } else if (connection.responseCode == 404) {
        error "The service is not available for the coverageId : $coverageId"
        return false
    } else {
        error "Cannot request the WorldPop service"
        return false
    }
}

/**
 * This method is used to build the subset filter used by the WCS request
 * from bbox defined by a set of latitude and longitude coordinates
 *
 * See OGC® Web Coverage Service 2.0 Interface Standard - KVP Protocol Binding Extension
 *
 * The order of values in the bounding box is :
 * south ,west, north, east
 *
 *  south : float -> southern latitude of bounding box
 *  west : float  -> western longitude of bounding box
 *  north : float -> northern latitude of bounding box
 *  east : float  -> eastern longitude of bounding box
 *
 *  So : minimum latitude, minimum longitude, maximum latitude, maximum longitude
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 *
 * @param bbox 4 values
 * @return the subset filter
 */
static String buildSubset(def bbox) {
    if (!bbox) {
        error "The latitude and longitude values cannot be null or empty"
        return null
    }
    if (!(bbox instanceof Collection)) {
        error "The latitude and longitude values must be set as an array"
        return null
    }

    if (bbox.size() == 4) {
        if (UTMUtils.isValidLatitude(bbox[0]) && UTMUtils.isValidLatitude(bbox[2])
                && UTMUtils.isValidLongitude(bbox[1]) && UTMUtils.isValidLongitude(bbox[3])) {
            return "subset=Lat(${bbox[0]},${bbox[2]})&subset=Long(${bbox[1]},${bbox[3]})"
        }
    }
    error("The bbox must be defined with 4 values")
}
