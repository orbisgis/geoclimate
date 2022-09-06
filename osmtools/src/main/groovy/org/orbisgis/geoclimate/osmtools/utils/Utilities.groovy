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
package org.orbisgis.geoclimate.osmtools.utils

import groovy.json.JsonSlurper
import org.cts.util.UTMUtils
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.*
import org.orbisgis.data.jdbc.JdbcDataSource
import org.slf4j.LoggerFactory

import static java.nio.charset.StandardCharsets.UTF_8

class Utilities {

    static def LOGGER = LoggerFactory.getLogger(Utilities)

    /** {@link Closure} returning a {@link String} prefix/suffix build from a random {@link UUID} with '-' replaced by '_'. */
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }

    static def uuid() { getUuid() }
    /** {@link Closure} logging with INFO level the given {@link Object} {@link String} representation. */
    static def info(def obj) { LOGGER.info(obj.toString()) }
    /** {@link Closure} logging with WARN level the given {@link Object} {@link String} representation. */
    static def warn(def obj) { LOGGER.warn(obj.toString()) }
    /** {@link Closure} logging with ERROR level the given {@link Object} {@link String} representation. */
    static def error(def obj) { LOGGER.error(obj.toString()) }
    /** {@link Closure} logging with DEBUG level the given {@link Object} {@link String} representation. */
    static def debug(def obj) { LOGGER.debug(obj.toString()) }



    /**
     * Return the area of a city name as a geometry.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param placeName The nominatim place name.
     *
     * @return a New geometry.
     */
    static Map getAreaFromPlace(def placeName) {
        if (!placeName) {
            error "The place name should not be null or empty."
            return null
        }
        def outputOSMFile = File.createTempFile("nominatim_osm", ".geojson")
        if (!executeNominatimQuery(placeName, outputOSMFile)) {
            if (!outputOSMFile.delete()) {
                warn "Unable to delete the file '$outputOSMFile'."
            }
            warn "Unable to execute the Nominatim query."
            return null
        }

        def jsonRoot = new JsonSlurper().parse(outputOSMFile)
        if (jsonRoot == null) {
            error "Cannot find any data from the place $placeName."
            return null
        }

        if (jsonRoot.features.size() == 0) {
            error "Cannot find any features from the place $placeName."
            if (!outputOSMFile.delete()) {
                warn "Unable to delete the file '$outputOSMFile'."
            }
            return null
        }

        GeometryFactory geometryFactory = new GeometryFactory()

        def data = [:]
        jsonRoot.features.find() { feature ->
            if (feature.geometry != null) {
                if (feature.geometry.type.equalsIgnoreCase("polygon")) {
                    def area = parsePolygon(feature.geometry.coordinates, geometryFactory)
                    area.setSRID(4326)
                    data.put("geom", area)
                    //Add properties and extrat tags
                    data.putAll(feature.properties)
                } else if (feature.geometry.type.equalsIgnoreCase("multipolygon")) {
                    def mp = feature.geometry.coordinates.collect { it ->
                        parsePolygon(it, geometryFactory)
                    }.toArray(new Polygon[0])
                    def area = geometryFactory.createMultiPolygon(mp)
                    area.setSRID(4326)
                    data.put("geom", area)
                    //Add properties and extrat tags
                    data.putAll(feature.properties)
                } else {
                    return false
                }
                return true
            }
            return false
        }
        if (!outputOSMFile.delete()) {
            warn "Unable to delete the file '$outputOSMFile'."
        }
        return data
    }

    /**
     * Create a geometry from two coordinates and a distance
     * Expand the geometry in x and y directions
     * @param lat latitude
     * @param lon longitude
     * @param distance in meters
     * @return
     */
    static Geometry getAreaFromPoint(def lat, def lon, float distance) {
        org.locationtech.jts.geom.Geometry geom =  new GeometryFactory().toGeometry(GeographyUtilities.createEnvelope(new Coordinate(lat,lon), distance, distance))
        geom.setSRID(4326)
        return geom
    }

    /**
     * Create a bbox represented by a list of  values
     * @param lat
     * @param lon
     * @param distance
     * @return
     */
    static List createBBox(def lat, def lon, float distance){
        Envelope env = GeographyUtilities.createEnvelope(new Coordinate(lat,lon), distance, distance)
        if(env){
            return [(float)env.getMinX(),(float)env.getMinY(), (float)env.getMaxX(),(float)env.getMaxY()]
        }
        return null
    }

    /**
     * Generic method to return one OSM polygon (area) from
     *
     * - a place name. e.g Redon
     * - a bbox defined by an array of 4 values
     * - a point and a distance defined by an array of 3 values
     *
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     *
     * @param placeName The nominatim place name.
     *
     * @return a New geometry.
     */
    static Geometry getArea(def location) {
        Geometry geom
        if (location in Collection) {
            return Utilities.geometryFromValues(location)
        } else if (location instanceof String) {
            return Utilities.getAreaFromPlace(location)["geom"]
        } else {
            return null;
        }
    }

    /**
     * Parse geojson coordinates to create a polygon.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param coordinates Coordinates to parse.
     * @param geometryFactory Geometry factory used for the geometry creation.
     *
     * @return A polygon.
     */
    static Polygon parsePolygon(def coordinates, GeometryFactory geometryFactory) {
        if (!coordinates in Collection || !coordinates ||
                !coordinates[0] in Collection || !coordinates[0] ||
                !coordinates[0][0] in Collection || !coordinates[0][0]) {
            error "The given coordinate should be an array of an array of an array of coordinates (3D array)."
            return null
        }
        def ring
        try {
            ring = geometryFactory.createLinearRing(arrayToCoordinate(coordinates[0]))
        }
        catch (IllegalArgumentException e) {
            error e.getMessage()
            return null
        }
        if (coordinates.size() == 1) {
            return geometryFactory.createPolygon(ring)
        } else {
            def holes = coordinates[1..coordinates.size() - 1].collect { it ->
                geometryFactory.createLinearRing(arrayToCoordinate(it))
            }.toArray(new LinearRing[0])
            return geometryFactory.createPolygon(ring, holes)
        }
    }

    /**
     * Convert and array of numeric coordinates into of an array of {@link Coordinate}.
     *
     * @param coordinates Array of array of numeric value (array of numeric coordinates)
     *
     * @return Array of {@link Coordinate}.
     */
    static Coordinate[] arrayToCoordinate(def coordinates) {
        coordinates.collect { it ->
            if (it.size() == 2) {
                def (x, y) = it
                new Coordinate(x, y)
            } else if (it.size() == 3) {
                def (x, y, z) = it
                new Coordinate(x, y, z)
            }
        }.findAll { it != null }.toArray(new Coordinate[0])
    }

    /**
     * Method to execute an Nominatim query and save the result in a file.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param query The Nominatim query.
     * @param outputNominatimFile The output file.
     *
     * @return True if the file has been downloaded, false otherwise.
     *
     */
    static boolean executeNominatimQuery(def query, def outputOSMFile) {
        if (!query) {
            error "The Nominatim query should not be null."
            return false
        }
        if (!(outputOSMFile instanceof File)) {
            error "The OSM file should be an instance of File"
            return false
        }
        def endPoint = System.getProperty("NOMINATIM_ENPOINT");
        if(!endPoint){
            /** nominatim server endpoint as defined by WSDL2 definition */
            endPoint="https://nominatim.openstreetmap.org/";
        }

        def apiUrl = "${endPoint}search?q="
        def request = "&limit=5&format=geojson&polygon_geojson=1&extratags=1"

        URL url = new URL(apiUrl + Utilities.utf8ToUrl(query) + request)
        final String proxyHost = System.getProperty("http.proxyHost");
        final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));
        
        def connection
        if (proxyHost != null) {
            def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost,proxyPort ));
            connection = url.openConnection(proxy) as HttpURLConnection
        } else {
            connection = url.openConnection()
        }
        connection.requestMethod = "GET"
        connection.connect()

        info url
        info "Executing query... $query"
        //Save the result in a file
        if (connection.responseCode == 200) {
            info "Downloading the Nominatim data."
            outputOSMFile << connection.inputStream
            return true
        } else {
            error "Cannot execute the Nominatim query."
            return false
        }
    }

    /**
     * Extract the OSM bbox signature from a Geometry.
     * e.g. (bbox:"50.7 7.1 50.7 7.12 50.71 7.11")
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param geometry Input geometry.
     *
     * @return OSM bbox.
     */
    static String toBBox(Geometry geometry) {
        if (!geometry) {
            error "Cannot convert to an overpass bounding box."
            return null
        }
        def env = geometry.getEnvelopeInternal()
        return "(bbox:${env.getMinY()},${env.getMinX()},${env.getMaxY()},${env.getMaxX()})".toString()
    }

    /**
     * Extract the OSM poly signature from a Geometry
     * e.g. (poly:"50.7 7.1 50.7 7.12 50.71 7.11")
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param geometry Input geometry.
     *
     * @return The OSM polygon.
     */
    static String toPoly(Geometry geometry) {
        if (!geometry) {
            error "Cannot convert to an overpass poly filter."
            return null
        }
        if (!(geometry instanceof Polygon)) {
            error "The input geometry must be polygon."
            return null
        }
        def poly = (Polygon) geometry
        if (poly.isEmpty()) {
            error "The input geometry must be polygon."
            return null
        }
        Coordinate[] coordinates = poly.getExteriorRing().getCoordinates()
        def polyStr = "(poly:\""
        for (i in 0..coordinates.size() - 3) {
            def coord = coordinates[i]
            polyStr += "${coord.getY()} ${coord.getX()} "
        }
        def coord = coordinates[coordinates.size() - 2]
        polyStr += "${coord.getY()} ${coord.getX()}"
        return polyStr + "\")"
    }

    /**
     * Method to build a valid OSM query with a bbox.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param envelope The envelope to filter.
     * @param keys A list of OSM keys.
     * @param osmElement A list of OSM elements to build the query (node, way, relation).
     *
     * @return A string representation of the OSM query.
     */
    static String buildOSMQuery(Envelope envelope, def keys, OSMElement... osmElement) {
        if (!envelope) {
            error "Cannot create the overpass query from the bbox $envelope."
            return null
        }
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()},${envelope.getMaxX()}];\n(\n"
        osmElement.each { i ->
            if (keys == null || keys.isEmpty()) {
                query += "\t${i.toString().toLowerCase()};\n"
            } else {
                keys.each {
                    query += "\t${i.toString().toLowerCase()}[\"${it.toLowerCase()}\"];\n"
                }
            }
        }
        query += ");\n(._;>;);\nout;"
        return query
    }

    /**
     * Method to build a valid OSM query with a bbox to
     * download all the osm data concerning
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param envelope The envelope to filter.
     * @param keys A list of OSM keys.
     * @param osmElement A list of OSM elements to build the query (node, way, relation).
     *
     * @return A string representation of the OSM query.
     */
    static String buildOSMQueryWithAllData(Envelope envelope, def keys, OSMElement... osmElement) {
        if (!envelope) {
            error "Cannot create the overpass query from the bbox $envelope."
            return null
        }
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()},${envelope.getMaxX()}];\n((\n"
        osmElement.each { i ->
            if (keys == null || keys.isEmpty()) {
                query += "\t${i.toString().toLowerCase()};\n"
            } else {
                keys.each {
                    query += "\t${i.toString().toLowerCase()}[\"${it.toLowerCase()}\"];\n"
                }
            }
        }
        query += ");\n>;);\nout;"
        return query
    }

    /**
     * Method to build a valid and optimized OSM query
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param polygon The polygon to filter.
     * @param keys A list of OSM keys.
     * @param osmElement A list of OSM elements to build the query (node, way, relation).
     *
     * @return A string representation of the OSM query.
     */
    static String buildOSMQuery(Polygon polygon, def keys, OSMElement... osmElement) {
        if (polygon == null) {
            error "Cannot create the overpass query from a null polygon."
            return null
        }
        if (polygon.isEmpty()) {
            error "Cannot create the overpass query from an empty polygon."
            return null
        }
        Envelope envelope = polygon.getEnvelopeInternal()
        def query = "[bbox:${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()},${envelope.getMaxX()}];\n(\n"
        String filterArea = toPoly(polygon)
        def nokeys = false;
        osmElement.each { i ->
            if (keys == null || keys.isEmpty()) {
                query += "\t${i.toString().toLowerCase()}$filterArea;\n"
                nokeys = true
            } else {
                keys.each {
                    query += "\t${i.toString().toLowerCase()}[\"${it.toLowerCase()}\"]$filterArea;\n"
                    nokeys = false
                }
            }
        }
        if (nokeys) {
            query += ");\nout;"
        } else {
            query += ");\n(._;>;);\nout;"
        }

        return query
    }

    /**
     * Parse a json file to a Map.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param jsonFile JSON file to parse.
     *
     * @return A Map of parameters.
     */
    static Map readJSONParameters(def jsonFile) {
        if (!jsonFile) {
            error "The given file should not be null"
            return null
        }
        def file
        if (jsonFile instanceof InputStream) {
            file = jsonFile
        } else {
            file = new File(jsonFile)
            if (!file.exists()) {
                warn "No file named ${jsonFile} doesn't exists."
                return null
            }
            if (!file.isFile()) {
                warn "No file named ${jsonFile} found."
                return null
            }
        }
        def parsed = new JsonSlurper().parse(file)
        if (parsed in Map) {
            return parsed
        }
        error "The json file doesn't contains only parameter."
    }

    /**
     * This method is used to build a new geometry from the following input parameters :
     * min Longitude , min Latitude , max Longitude , max Latitude
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     *
     * @param bbox 4 values
     * @return a JTS polygon
     *
     */
    static Geometry buildGeometry(def bbox) {
        if (!bbox) {
            error "The BBox should not be null"
            return null
        }
        if (!bbox.class.isArray() && !(bbox instanceof Collection)) {
            error "The BBox should be an array"
            return null
        }
        if (bbox.size() != 4) {
            error "The BBox should be an array of 4 values"
            return null
        }
        def minLong = bbox[0]
        def minLat = bbox[1]
        def maxLong = bbox[2]
        def maxLat = bbox[3]
        //Check values
        if (UTMUtils.isValidLatitude(minLat) && UTMUtils.isValidLatitude(maxLat)
                && UTMUtils.isValidLongitude(minLong) && UTMUtils.isValidLongitude(maxLong)) {
            GeometryFactory geometryFactory = new GeometryFactory()
            Geometry geom = geometryFactory.toGeometry(new Envelope(minLong, maxLong, minLat, maxLat))
            geom.setSRID(4326)
            return geom.isValid() ? geom : null

        }
        error("Invalid latitude longitude values")
    }

    /**
     * This method is used to build a geometry following the Nominatim bbox signature
     * Nominatim API returns a boundingbox property of the form:
     * south Latitude, north Latitude, west Longitude, east Longitude
     *  south : float -> southern latitude of bounding box
     *  west : float  -> western longitude of bounding box
     *  north : float -> northern latitude of bounding box
     *  east : float  -> eastern longitude of bounding box
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     *
     * @param bbox 4 values
     * @return a JTS polygon
     */
    //TODO why not merging methods
    static Geometry geometryFromNominatim(def bbox) {
        if (!bbox) {
            error "The latitude and longitude values cannot be null or empty"
            return null
        }
        if (!(bbox instanceof Collection) && !bbox.class.isArray()) {
            error "The latitude and longitude values must be set as an array"
            return null
        }
        if (bbox.size() == 4) {
            return buildGeometry([bbox[1], bbox[0], bbox[3], bbox[2]]);
        }
        error("The bbox must be defined with 4 values")
    }

    /**
     * This method is used to build a geometry from 4 values (bbox)
     *
     * The order of the 4 values to defined in the bounding box is :
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
     * @param bbox 4 values to define a bbox
     * @return a JTS polygon
     */
    static Geometry geometryFromValues(def bbox) {
        if (!bbox) {
            return null
        }
        if (!(bbox instanceof Collection)) {
            return null
        }
        if (bbox.size() == 4) {
            return buildGeometry([bbox[1], bbox[0], bbox[3], bbox[2]]);
        }
    }


    /**
     ** Function to drop the temp tables coming from the OSM extraction
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Le Saux (UBS LAB-STICC)
     *
     * @param prefix Prefix of the OSM tables.
     * @param datasource Datasource where the OSM tables are.
     */
    static boolean dropOSMTables(String prefix, JdbcDataSource datasource) {
        if (prefix == null) {
            error "The prefix should not be null"
            return false
        }
        if (!datasource) {
            error "The data source should not be null"
            return false
        }
        datasource.execute("""DROP TABLE IF EXISTS ${prefix}_NODE, ${prefix}_NODE_MEMBER, ${prefix}_NODE_TAG,
                ${prefix}_RELATION,${prefix}_RELATION_MEMBER,${prefix}_RELATION_TAG, ${prefix}_WAY,
                ${prefix}_WAY_MEMBER,${prefix}_WAY_NODE,${prefix}_WAY_TAG""".toString())
        return true
    }

    /** Get method for HTTP request */
    private static def GET = "GET"
    /** Overpass server endpoint as defined by WSDL2 definition */
    static def OVERPASS_ENDPOINT ="https://overpass-api.de/api"
    /** Overpass server base URL */
    static def OVERPASS_BASE_URL = "${OVERPASS_ENDPOINT}/interpreter?data="
    /** Url of the status of the Overpass server */
    static def OVERPASS_STATUS_URL = "${OVERPASS_ENDPOINT}/status"

    /**
     * Return the status of the Overpass server.
     * @return A string representation of the overpass status.
     */
    static def getServerStatus()  {
        final String proxyHost = System.getProperty("http.proxyHost");
        final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));
        def connection
        def endPoint = System.getProperty("OVERPASS_ENPOINT");
        if(endPoint){
            OVERPASS_STATUS_URL= "${endPoint}/status"
        }
        if (proxyHost != null) {
            def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost,proxyPort ));
            connection = new URL(OVERPASS_STATUS_URL).openConnection(proxy) as HttpURLConnection
        } else {
            connection = new URL(OVERPASS_STATUS_URL).openConnection() as HttpURLConnection
        }
        connection.requestMethod = GET
        connection.connect()
        if (connection.responseCode == 200) {
            return connection.inputStream.text
        }
        else {
            error "Cannot get the status of the server.\n Server answer with code ${connection.responseCode} : " +
                    "${connection.inputStream.text}"
        }
    }

    /** {@link Closure} converting and UTF-8 {@link String} into an {@link URL}. */
    static def utf8ToUrl = { utf8 -> URLEncoder.encode(utf8, UTF_8.toString()) }


    /**
     * Method to execute an Overpass query and save the result in a file
     *
     * @param query the Overpass query
     * @param outputOSMFile the output file
     *
     * @return True if the query has been successfully executed, false otherwise.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Lesaux (UBS LAB-STICC)
     */
    static boolean executeOverPassQuery(URL queryUrl, def outputOSMFile) {
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
        connection.requestMethod = GET
        connection.connect()

        info "Executing query... $queryUrl"
        //Save the result in a file
        if (connection.responseCode == 200) {
            info "Downloading the OSM data from overpass api in ${outputOSMFile}"
            outputOSMFile << connection.inputStream
            return true
        }
        else {
            error "Cannot execute the query.\n${getServerStatus()}"
            return false
        }
    }

    /**
     * Method to execute an Overpass query and save the result in a file
     *
     * @param query the Overpass query
     * @param outputOSMFile the output file
     *
     * @return True if the query has been successfully executed, false otherwise.
     *
     * @author Erwan Bocher (CNRS LAB-STICC)
     * @author Elisabeth Lesaux (UBS LAB-STICC)
     */
    static boolean executeOverPassQuery(def query, def outputOSMFile) {
        if(!query){
            error "The query should not be null or empty."
            return false
        }
        if(!outputOSMFile){
            error "The output file should not be null or empty."
            return false
        }
        def endPoint = System.getProperty("OVERPASS_ENPOINT");
        if(endPoint){
            OVERPASS_BASE_URL= "${endPoint}/interpreter?data="
        }
        def queryUrl = new URL(OVERPASS_BASE_URL + utf8ToUrl(query))
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
        connection.requestMethod = GET
        connection.connect()

        info "Executing query... $query"
        //Save the result in a file
        if (connection.responseCode == 200) {
            info "Downloading the OSM data from overpass api in ${outputOSMFile}"
            outputOSMFile << connection.inputStream
            return true
        }
        else {
            error "Cannot execute the query.\n${getServerStatus()}"
            return false
        }
    }

}