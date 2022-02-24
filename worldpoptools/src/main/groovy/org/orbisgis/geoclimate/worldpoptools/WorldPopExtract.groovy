/*
 * Bundle WorldPopTools is part of the GeoClimate tool
 *
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies .
 * GeoClimate is developed by the GIS group of the DECIDE team of the
 * Lab-STICC CNRS laboratory, see <http://www.lab-sticc.fr/>.
 *
 * The GIS group of the DECIDE team is located at :
 *
 * Laboratoire Lab-STICC – CNRS UMR 6285
 * Equipe DECIDE
 * UNIVERSITÉ DE BRETAGNE-SUD
 * Institut Universitaire de Technologie de Vannes
 * 8, Rue Montaigne - BP 561 56017 Vannes Cedex
 *
 * WorldPopTools is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019-2021 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * WorldPopTools is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * WorldPopTools is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * WorldPopTools. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <https://github.com/orbisgis/geoclimate>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.geoclimate.worldpoptools

import groovy.transform.BaseScript
import org.cts.util.UTMUtils
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.functions.io.asc.AscReaderDriver
import org.h2gis.utilities.FileUtilities
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess


@BaseScript WorldPopTools pf


/**
 * This process is used to extract a grid population data from  https://www.worldpop.org
 * The grid is stored in a OS tmp dir.
 * If the grid already exists the file is reused.
 * @return
 */
IProcess extractWorldPopLayer() {
    return create {
        title "Extract the estimated world population data on 100 X 100 m grid"
        id "extractWorldPop"
        inputs  coverageId:String,  bbox :[], epsg :int
        outputs outputFilePath: String
        run { coverageId, bbox, epsg ->
            info "Extract the world population grid"
            def gridRequest =  createGridRequest(coverageId, bbox, epsg)
            //hash the query to cache it
            def queryHash = gridRequest.digest('SHA-256')
            def outputGridFile=new File(System.getProperty("java.io.tmpdir")+File.separator+"${queryHash}.asc")
            def popGridFilePath = outputGridFile.absolutePath
            if(outputGridFile.exists()){
                if(outputGridFile.length()==0){
                    outputGridFile.delete()
                    if(outputGridFile.createNewFile()) {
                        if ( grid(gridRequest, outputGridFile)) {
                            info "The grid file has been downloaded at ${popGridFilePath}."
                        } else {
                            outputGridFile.delete()
                            error "Cannot extract the grid data for the query $gridRequest"
                            return
                        }
                    }
                }
                else {
                    debug "\nThe cached grid file ${popGridFilePath} will be re-used for the query :  \n$gridRequest."
                }
            }
            else{
                if(outputGridFile.createNewFile()){
                    if (grid(gridRequest, outputGridFile)) {
                        info "The OSM file has been downloaded at ${popGridFilePath}."
                    } else {
                        outputGridFile.delete()
                        error "Cannot extract the OSM data for the query $overpassQuery"
                        return
                    }}
            }
            [outputFilePath: popGridFilePath]
        }
    }
}

/**
 * Process to import and asc grid into the database
 * @return
 */
IProcess importAscGrid() {
    return create {
        title "Import an asc grid into the database"
        id "importAscGrid"
        inputs  worldPopFilePath:String, epsg: 4326, datasource: JdbcDataSource
        outputs outputTableWorldPopName: String
        run { worldPopFilePath,epsg,datasource ->
            info "Import the the world pop asc file"
            // The name of the outputTableName is constructed
            def outputTableWorldPopName = postfix "world_pop"
            AscReaderDriver ascReaderDriver = new AscReaderDriver()
            ascReaderDriver.setAs3DPoint(false)
            ascReaderDriver.setEncoding("UTF-8")
            ascReaderDriver.read(datasource.getConnection(),new File(worldPopFilePath), new EmptyProgressVisitor(), outputTableWorldPopName, epsg)
            datasource.execute("""ALTER TABLE $outputTableWorldPopName RENAME COLUMN PK TO ID_POP;
                                ALTER TABLE $outputTableWorldPopName RENAME COLUMN Z TO POP;""".toString())
            [outputTableWorldPopName: outputTableWorldPopName]
        }
    }
}



/**
 * A method to create the WCS grid request
 * @param coverageId
 * @param bbox
 * @param epsg
 * @return
 */
String createGridRequest(String coverageId, def bbox, int epsg =4326) {
    if(!coverageId){
        error "The coverageId cannot be null or empty"
        return
    }
    def subset = buildSubset(bbox)

    if(!subset){
        error "Cannot create the subset WCS filter"
        return
    }
    def crsOutPut =  "outputCRS=http://www.opengis.net/def/crs/EPSG/0/$epsg"

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
 boolean grid(String coverageId, def bbox, int epsg =4326, File outputGridFile) {
     def queryUrl = createGridRequest(coverageId,bbox, epsg)
     if(!queryUrl){
         error "The request to the server cannot be null or empty"
         return false
     }
     if(!outputGridFile){
         error "The output Grid File cannot be null or empty"
         return false
     }
     if(!FileUtilities.isExtensionWellFormated(outputGridFile, "asc")){
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
boolean grid(String wcsRequest,  File outputGridFile) {
    def queryUrl = new URL(wcsRequest)

    if(!wcsRequest){
        error "The request cannot be null or empty"
        return false
    }
    if(!FileUtilities.isExtensionWellFormated(outputGridFile, "asc")){
        error "Only support asc (ArcGrid format) as output Grid File"
        return false
    }

    final String proxyHost = System.getProperty("http.proxyHost");
    final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));
    def connection
    if (proxyHost != null) {
        def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost,proxyPort ));
        connection = queryUrl.openConnection(proxy) as HttpURLConnection
    } else {
        connection = queryUrl.openConnection() as HttpURLConnection
    }
    info queryUrl
    connection.connect()

    info "Executing query... $queryUrl"
    //Save the result in a file
    if (connection.responseCode == 200) {
        info "Downloading the GridCoverage data from WorldPop server in ${outputGridFile}"
        outputGridFile << connection.inputStream
        return true
    }
    else {
        error "Cannot execute the WCS query.$queryUrl"
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
            return  "subset=Lat(${bbox[0]},${bbox[2]})&subset=Long(${bbox[1]},${bbox[3]})"
        }
    }
    error("The bbox must be defined with 4 values")
}
