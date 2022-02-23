/*
 * Bundle OSMTools is part of the GeoClimate tool
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
 * OSMTools is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019-2021 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSMTools is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSMTools is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSMTools. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <https://github.com/orbisgis/geoclimate>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.geoclimate.worldpoptools

import groovy.transform.BaseScript
import groovy.transform.Field
import org.cts.util.UTMUtils
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource

@BaseScript WorldPopTools pf


/**
 * Method to execute a query on the WorldPop Web Coverage Service and
 * return grid coverage a a geotiff file
 *
 * @param query the Overpass query
 * @param outputGridFile the output file
 *
 * @return True if the query has been successfully executed, false otherwise.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 */
 boolean grid(String coverageId, def bbox, int epsg =4326, File outputGridFile) {
    if(!coverageId){
        error "The coverageId cannot be null or empty"
        return false
    }
    def subset = buildSubset(bbox)

    if(!subset){
        error "Cannot create the subset WCS filter"
        return false
    }

    def crsOutPut =  "outputCRS=http://www.opengis.net/def/crs/EPSG/0/$epsg"

    def WCS_SERVER = """https://ogc.worldpop.org/geoserver/ows?service=WCS&version=2.0.1&request=GetCoverage&coverageId=$coverageId&format=image/tiff&$subset&$crsOutPut""".toString()

    def queryUrl = new URL(WCS_SERVER)// URLEncoder.encode(utf8, UTF_8.toString())

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
