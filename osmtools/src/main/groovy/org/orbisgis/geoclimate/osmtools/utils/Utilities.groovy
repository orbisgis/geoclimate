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
package org.orbisgis.geoclimate.osmtools.utils

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import groovy.transform.Field
import org.cts.util.UTMUtils
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.*
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.osmtools.OSMTools

import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.TimeUnit
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import static java.nio.charset.StandardCharsets.UTF_8

@BaseScript OSMTools pf


/**
 * Return all data from a place name with nominatim api.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 *
 * @param placeName The nominatim place name.
 *
 * @return a New geometry.
 */
Map getNominatimData(def placeName) throws Exception {
    if (!placeName) {
        throw new Exception("The place name should not be null or empty.")
    }
    def nominatimQ = "nomination_" + Utilities.utf8ToUrl(placeName)
    //hash the query to cache it
    def queryHash = nominatimQ.digest('SHA-256')
    def outputOSMFile = new File(System.getProperty("java.io.tmpdir") + File.separator + "${queryHash}.geojson")
    def nominatimFilePath = outputOSMFile.absolutePath

    if (outputOSMFile.exists()) {
        if (outputOSMFile.length() == 0) {
            outputOSMFile.delete()
            if (outputOSMFile.createNewFile()) {
                if (!executeNominatimQuery(placeName, outputOSMFile)) {
                    if (!outputOSMFile.delete()) {
                        warn "Unable to delete the file '$outputOSMFile'."
                    }
                    throw new Exception("Unable to execute the Nominatim query.")
                }
            }
        } else {
            debug "\nThe cached Nomination file ${nominatimFilePath} will be re-used for the query :  \n$nominatimQ."
        }
    } else {
        if (outputOSMFile.createNewFile()) {
            if (!executeNominatimQuery(placeName, outputOSMFile)) {
                if (!outputOSMFile.delete()) {
                    warn "Unable to delete the file '$outputOSMFile'."
                }
                throw new Exception("Unable to execute the Nominatim query.")
            }
        }
    }

    def jsonRoot = new JsonSlurper().parse(outputOSMFile)
    if (jsonRoot == null) {
        throw new Exception("Cannot find any data from the place $placeName.".toString())
    }

    if (jsonRoot.features.size() == 0) {
        if (!outputOSMFile.delete()) {
            warn "Unable to delete the file '$outputOSMFile'."
        }
        throw new Exception("Cannot find any features from the place $placeName.".toString())
    }

    GeometryFactory geometryFactory = new GeometryFactory()

    def geometry_admin = 0
    def data = [:]
    jsonRoot.features.find() { feature ->
        if (feature.geometry != null) {
            if (feature.geometry.type.equalsIgnoreCase("polygon")) {
                def area = parsePolygon(feature.geometry.coordinates, geometryFactory)
                area.setSRID(4326)
                data.put("geom", area)
                //Add properties and extrat tags
                data.putAll(feature.properties)
                def bbox = feature.bbox
                data.put("bbox", [bbox[1], bbox[0], bbox[3], bbox[2]])
                if (feature.properties.type == "administrative" && feature.properties.category == 'boundary') {
                    return true
                }
            } else if (feature.geometry.type.equalsIgnoreCase("multipolygon")) {
                def mp = feature.geometry.coordinates.collect { it ->
                    parsePolygon(it, geometryFactory)
                }.toArray(new Polygon[0])
                def area = geometryFactory.createMultiPolygon(mp)
                area.setSRID(4326)
                data.put("geom", area)
                //Add properties and extrat tags
                data.putAll(feature.properties)
                def bbox = feature.bbox
                data.put("bbox", [bbox[1], bbox[0], bbox[3], bbox[2]])
                if (feature.properties.type == "administrative" && feature.properties.category == 'boundary') {
                    return true
                }
            } else {
                return false
            }
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
    org.locationtech.jts.geom.Geometry geom = new GeometryFactory().toGeometry(GeographyUtilities.createEnvelope(new Coordinate(lat, lon), distance, distance))
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
static List createBBox(def lat, def lon, float distance) {
    Envelope env = GeographyUtilities.createEnvelope(new Coordinate(lat, lon), distance, distance)
    if (env) {
        return [(float) env.getMinX(), (float) env.getMinY(), (float) env.getMaxX(), (float) env.getMaxY()]
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
    if (location in Collection) {
        return OSMTools.Utilities.geometryFromValues(location)
    } else if (location instanceof String) {
        return OSMTools.Utilities.getNominatimData(location)["geom"]
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
Polygon parsePolygon(def coordinates, GeometryFactory geometryFactory) throws Exception {
    if (!coordinates in Collection || !coordinates ||
            !coordinates[0] in Collection || !coordinates[0] ||
            !coordinates[0][0] in Collection || !coordinates[0][0]) {
        throw new Exception("The given coordinate should be an array of an array of an array of coordinates (3D array).")
    }
    def ring
    try {
        ring = geometryFactory.createLinearRing(arrayToCoordinate(coordinates[0]))
    }
    catch (IllegalArgumentException e) {
        throw new Exception(e)
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
boolean executeNominatimQuery(def query, def outputOSMFile) throws Exception {
    if (!query) {
        throw new Exception("The Nominatim query should not be null.")
    }
    if (!(outputOSMFile instanceof File)) {
        throw new Exception("The OSM file should be an instance of File")
    }
    def endPoint = System.getProperty("NOMINATIM_ENDPOINT");
    if (!endPoint) {
        /** nominatim server endpoint as defined by WSDL2 definition */
        endPoint = "https://nominatim.openstreetmap.org/";
    }

    def apiUrl = "${endPoint}search?q="
    def request = "&limit=5&format=geojson&polygon_geojson=1&extratags=1"

    URL url = new URL(apiUrl + Utilities.utf8ToUrl(query) + request)
    final String proxyHost = System.getProperty("http.proxyHost");
    final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));

    HttpURLConnection connection
    if (proxyHost != null) {
        def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        connection = url.openConnection(proxy) as HttpURLConnection
    } else {
        connection = url.openConnection() as HttpURLConnection
    }
    connection.requestMethod = "GET"
    connection.setRequestProperty("User-Agent", "GEOCLIMATE/${version()}")

    Matcher timeoutMatcher = Pattern.compile("\\[timeout:(\\d+)\\]").matcher(query.toString());
    int timeout = OVERPASS_TIMEOUT
    if (timeoutMatcher.find()) {
        timeout = (int) TimeUnit.SECONDS.toMillis(Integer.parseInt(timeoutMatcher.group(1)));
    } else {
        timeout = (int) TimeUnit.MINUTES.toMillis(3);
    }

    connection.setConnectTimeout(timeout)
    connection.setReadTimeout(timeout)

    int responseCode = connection.getResponseCode();
    if (responseCode != RESPONSECODE_OK) {
        throw new Exception("Cannot execute the Nominatim query : ${url.toString()}")
    }
    debug "Executing query... $query"
    //Save the result in a file
    info "Downloading the Nominatim data."
    outputOSMFile << connection.inputStream
    connection.disconnect()
    return true

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
String toBBox(Geometry geometry) throws Exception {
    if (!geometry) {
        throw new Exception("Cannot convert to an overpass bounding box.")
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
String toPoly(Geometry geometry) throws Exception {
    if (!geometry) {
        throw new Exception("Cannot convert to an overpass poly filter.")
    }
    if (!(geometry instanceof Polygon)) {
        throw new Exception("The input geometry must be polygon.")
    }
    def poly = (Polygon) geometry
    if (poly.isEmpty()) {
        throw new Exception("The input geometry must be polygon.")
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
 * @param keys A list of OSM keys. Must be null
 *
 * @return A string representation of the OSM query.
 */
String buildOSMQuery(Envelope envelope, def keys = null) throws Exception {
    return buildOSMQuery(envelope, keys, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
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
String buildOSMQuery(Envelope envelope, def keys, OSMElement... osmElement) throws Exception {
    if (!envelope) {
        throw new Exception("Cannot create the overpass query from the bbox $envelope.".toString())
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
 * Method to build a valid OSM query with a list of coordinates that define a Bbox.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 *
 * @param latLonCoordinates an array of 4 coordinates in lat/lon
 * @param keys A list of OSM keys.
 *
 * @return A string representation of the OSM query.
 */
String buildOSMQuery(List latLonCoordinates, def keys = null) throws Exception {
    return buildOSMQuery(latLonCoordinates, keys, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
}

/**
 * Method to build a valid OSM query with a list of coordinates that define a Bbox.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 *
 * @param latLonCoordinates an array of 4 coordinates in lat/lon
 * @param keys A list of OSM keys.
 * @param osmElement A list of OSM elements to build the query (node, way, relation).
 *
 * @return A string representation of the OSM query.
 */
String buildOSMQuery(List latLonCoordinates, def keys, OSMElement... osmElement) throws Exception {
    if (!latLonCoordinates) {
        throw new Exception("Cannot create the overpass query from the bbox $latLonCoordinates.".toString())
    }
    Geometry geom = OSMTools.Utilities.geometryFromValues(latLonCoordinates)
    if (geom == null) {
        throw new Exception("Invalid BBOX")
    }
    return buildOSMQuery(geom.getEnvelopeInternal(), keys, osmElement)
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
String buildOSMQueryWithAllData(Envelope envelope, def keys, OSMElement... osmElement) throws Exception {
    if (!envelope) {
        throw new Exception("Cannot create the overpass query from the bbox $envelope.".toString())
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
 *
 * @return A string representation of the OSM query.
 */
String buildOSMQuery(Polygon polygon, def keys = null) throws Exception {
    return buildOSMQuery(polygon, keys, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
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
String buildOSMQuery(Polygon polygon, def keys, OSMElement... osmElement) throws Exception {
    if (polygon == null) {
        throw new Exception("Cannot create the overpass query from a null polygon.")
    }
    if (polygon.isEmpty()) {
        throw new Exception("Cannot create the overpass query from an empty polygon.")
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
Map readJSONParameters(def jsonFile) throws Exception {
    if (!jsonFile) {
        throw new Exception("The given file should not be null")
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
Geometry buildGeometry(def bbox) {
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
//TODO: why not merging methods
Geometry geometryFromNominatim(def bbox) {
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
 * @param bbox 4 values to define a bbox or 3 values to define a point (lat/long)
 * with a distance around it, expressed in meters
 * @return a JTS polygon
 */
Geometry geometryFromValues(def bbox) {
    if (!bbox) {
        return null
    }
    if (!(bbox instanceof Collection)) {
        return null
    }
    if (bbox.size() == 4) {
        return buildGeometry([bbox[1], bbox[0], bbox[3], bbox[2]]);
    } else if (bbox.size() == 3) {
        if (bbox[2] < 100) {
            error("The distance to create a bbox from a point must be greater than 100 meters")
            return null
        }
        return getAreaFromPoint(bbox[1], bbox[0], bbox[2])
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
boolean dropOSMTables(String prefix, JdbcDataSource datasource) {
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
private static @Field GET = "GET"
/** Overpass server endpoint as defined by WSDL2 definition */
static @Field OVERPASS_ENDPOINT = "https://overpass-api.de/api"
/** Overpass server base URL */
static @Field OVERPASS_BASE_URL = "${OVERPASS_ENDPOINT}/interpreter?data="
/** Url of the status of the Overpass server */
static @Field OVERPASS_STATUS_URL = "${OVERPASS_ENDPOINT}/status"

/** OVERPASS TIMEOUT Default to 180 seconds.*/
static @Field int OVERPASS_TIMEOUT = 180 * 1000

static @Field int RESPONSECODE_OK = 200

/** Default number of attempts on each Overpass endpoint (system property OVERPASS_MAX_ATTEMPTS) */
static @Field int OVERPASS_MAX_ATTEMPTS = 4
/** Delay before the first retry in ms, doubled at each new attempt */
static @Field long OVERPASS_RETRY_DELAY = 10 * 1000
/** Upper bound of the delay between two attempts in ms */
static @Field long OVERPASS_MAX_RETRY_DELAY = 120 * 1000

/**
 * Return the list of the Overpass endpoints to use, the main one first.
 * The main endpoint is read from the system property OVERPASS_ENDPOINT
 * (the legacy misspelled name OVERPASS_ENPOINT is still accepted).
 * Fallback endpoints can be set with the system property OVERPASS_FALLBACK_ENDPOINTS,
 * as a comma separated list (e.g. https://overpass.kumi.systems/api).
 *
 * @return a list of endpoint urls, without trailing slash
 */
List<String> getOverpassEndpoints() {
    def endpoints = []
    def mainEndPoint = System.getProperty("OVERPASS_ENDPOINT") ?: System.getProperty("OVERPASS_ENPOINT")
    endpoints << (mainEndPoint ?: OVERPASS_ENDPOINT).toString()
    def fallbacks = System.getProperty("OVERPASS_FALLBACK_ENDPOINTS")
    if (fallbacks) {
        fallbacks.split(",").each {
            def endPoint = it.trim()
            if (endPoint) {
                endpoints << endPoint
            }
        }
    }
    return endpoints.collect { it.endsWith("/") ? it.substring(0, it.length() - 1) : it }.unique()
}

/**
 * Open an http connection, using the http proxy set with the system properties if any.
 */
private HttpURLConnection openHttpConnection(URL url) {
    final String proxyHost = System.getProperty("http.proxyHost")
    final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"))
    if (proxyHost != null) {
        def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort))
        return url.openConnection(proxy) as HttpURLConnection
    }
    return url.openConnection() as HttpURLConnection
}

/**
 * Return the status of the Overpass server.
 * @return A string representation of the overpass status.
 */
def getServerStatus() {
    return getServerStatus(getOverpassEndpoints().first())
}

/**
 * Return the status of an Overpass server.
 * @param endPoint the Overpass endpoint (e.g. https://overpass-api.de/api)
 * @return A string representation of the overpass status.
 */
def getServerStatus(String endPoint) {
    HttpURLConnection connection = openHttpConnection(new URL("${endPoint}/status"))
    try {
        connection.setRequestProperty("User-Agent", "GEOCLIMATE/${version()}")
        connection.setConnectTimeout(OVERPASS_TIMEOUT)
        connection.setReadTimeout(OVERPASS_TIMEOUT)
        connection.requestMethod = GET
        if (connection.responseCode != RESPONSECODE_OK) {
            error "Cannot get the status of the server ${endPoint}.\n Server answer with code ${connection.responseCode}"
            return null
        }
        return connection.inputStream.getText(UTF_8.name())
    } finally {
        connection.disconnect()
    }
}

/**
 * Method to execute an Overpass query and save the result in a file.
 *
 * The query is sent to the main endpoint and then to the fallback endpoints if any
 * (see {@link #getOverpassEndpoints()}).
 * Transient errors (429, 502, 503, 504, network errors, truncated or incomplete answers)
 * are retried with an increasing delay. The data is first written in a temporary file
 * which is moved to the output file only when the download is complete and valid,
 * so a broken download can never be reused as a cached file.
 *
 * @param query the Overpass query
 * @param outputOSMFile the output file. If its name ends with .gz the file is gzipped.
 *
 * @return True if the query has been successfully executed, false otherwise.
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Lesaux (UBS LAB-STICC)
 */
boolean executeOverPassQuery(String query, File outputOSMFile) {
    if (!query) {
        error "The query should not be null or empty."
        return false
    }
    if (!outputOSMFile) {
        error "The output file should not be null or empty."
        return false
    }
    int maxAttempts = Math.max(1, Integer.getInteger("OVERPASS_MAX_ATTEMPTS", OVERPASS_MAX_ATTEMPTS))
    def endpoints = getOverpassEndpoints()
    String lastMessage = ""
    for (String endPoint : endpoints) {
        long delay = OVERPASS_RETRY_DELAY
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Map result = downloadOverpassData(endPoint, query, outputOSMFile)
            if (result.ok) {
                return true
            }
            lastMessage = result.message
            if (!result.retry) {
                //The query itself is wrong, another try or another server will not help
                error "Cannot execute the overpass query on ${endPoint}. Cause : ${lastMessage}"
                return false
            }
            if (attempt < maxAttempts) {
                long wait = Math.min(Math.max(delay, result.waitMs as long), OVERPASS_MAX_RETRY_DELAY)
                warn "Overpass query failed on ${endPoint} (attempt ${attempt}/${maxAttempts}) : ${lastMessage}\n" +
                        "Next attempt in ${(long) (wait / 1000)} s."
                Thread.sleep(wait)
                delay = Math.min(delay * 2, OVERPASS_MAX_RETRY_DELAY)
            } else {
                warn "Overpass query failed on ${endPoint} after ${maxAttempts} attempts : ${lastMessage}"
            }
        }
    }
    error "Cannot execute the overpass query on ${endpoints.join(", ")}. Cause : ${lastMessage}"
    return false
}

/**
 * Execute once an Overpass query on one endpoint.
 *
 * @return a map with the keys ok (boolean), retry (boolean, true if the error is transient),
 * waitMs (minimum delay in ms advised by the server before retrying) and message
 */
private Map downloadOverpassData(String endPoint, String query, File outputOSMFile) {
    HttpURLConnection connection = null
    File tmpFile = null
    try {
        connection = openHttpConnection(new URL("${endPoint}/interpreter"))
        //POST avoids the url length limit for big queries (e.g. complex polygons)
        connection.requestMethod = "POST"
        connection.doOutput = true
        connection.setRequestProperty("User-Agent", "GEOCLIMATE/${version()}")
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
        boolean gzipOutput = outputOSMFile.name.toLowerCase().endsWith(".gz")
        if (gzipOutput) {
            connection.setRequestProperty("Accept-Encoding", "gzip")
        }
        int timeout = getOverpassQueryTimeout(query)
        connection.setConnectTimeout(timeout)
        connection.setReadTimeout(timeout)
        debug "Executing query on ${endPoint}... $query"
        connection.outputStream.withStream { it.write(("data=" + URLEncoder.encode(query, UTF_8.name())).getBytes(UTF_8)) }
        int code = connection.responseCode
        if (code != RESPONSECODE_OK) {
            String message = "${code} ${connection.responseMessage ?: ""}. ${readOverpassError(connection)}".trim()
            switch (code) {
                case 429:
                    long wait = Math.max(getRetryAfter(connection), getOverpassWaitTime(endPoint))
                    return [ok: false, retry: true, waitMs: wait,
                            message: "Too Many Requests, too many queries sent from the same IP. ${message}"]
                case 502:
                case 503:
                case 504:
                    return [ok: false, retry: true, waitMs: getRetryAfter(connection),
                            message: "The server is overloaded. ${message}"]
                case 400:
                    return [ok: false, retry: false, waitMs: 0L, message: "Bad Request, the query has a syntax error. ${message}"]
                default:
                    return [ok: false, retry: code >= 500, waitMs: 0L, message: message]
            }
        }

        info "Downloading the OSM data from ${endPoint} in ${outputOSMFile}"
        File parent = outputOSMFile.absoluteFile.parentFile
        if (parent && !parent.exists()) {
            parent.mkdirs()
        }
        tmpFile = File.createTempFile("${outputOSMFile.name}_", ".part", parent)
        boolean serverGzip = "gzip".equalsIgnoreCase(connection.contentEncoding)
        InputStream input = connection.inputStream
        try {
            tmpFile.withOutputStream { OutputStream output ->
                if (gzipOutput && !serverGzip) {
                    def gzip = new GZIPOutputStream(output)
                    gzip << input
                    gzip.finish()
                } else if (!gzipOutput && serverGzip) {
                    output << new GZIPInputStream(input)
                } else {
                    output << input
                }
            }
        } finally {
            input.close()
        }

        String problem = checkOverpassFile(tmpFile, gzipOutput)
        if (problem) {
            return [ok: false, retry: true, waitMs: 0L, message: problem]
        }
        try {
            Files.move(tmpFile.toPath(), outputOSMFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(tmpFile.toPath(), outputOSMFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
        return [ok: true]
    } catch (IOException e) {
        //Timeout, connection reset, truncated gzip stream...
        return [ok: false, retry: true, waitMs: 0L, message: "${e.class.simpleName} : ${e.message}"]
    } catch (Exception e) {
        return [ok: false, retry: false, waitMs: 0L, message: "${e.class.simpleName} : ${e.message}"]
    } finally {
        if (connection != null) {
            connection.disconnect()
        }
        if (tmpFile != null && tmpFile.exists()) {
            tmpFile.delete()
        }
    }
}

/**
 * Check that a downloaded Overpass file is complete.
 * Overpass can answer 200 with a partial result and a runtime error remark
 * (query timeout, out of memory). Reading the whole file also checks the gzip integrity.
 *
 * @return null if the file is valid, otherwise a description of the problem
 */
private String checkOverpassFile(File file, boolean gzip) {
    if (!file.exists() || file.length() == 0) {
        return "The server returned an empty answer"
    }
    InputStream input = new FileInputStream(file)
    if (gzip) {
        input = new GZIPInputStream(input)
    }
    String problem = null
    new InputStreamReader(input, UTF_8).withReader { reader ->
        String line
        while ((line = reader.readLine()) != null) {
            if (line.contains("runtime error")) {
                problem = "The server returned an incomplete answer : ${line.replaceAll("<[^>]*>", "").trim()}"
                break
            }
        }
    }
    return problem
}

/**
 * Read the error message sent by the Overpass server, without the html tags.
 */
private String readOverpassError(HttpURLConnection connection) {
    try {
        def errorStream = connection.errorStream
        if (errorStream == null) {
            return ""
        }
        if ("gzip".equalsIgnoreCase(connection.contentEncoding)) {
            errorStream = new GZIPInputStream(errorStream)
        }
        String text = errorStream.getText(UTF_8.name())
        def matcher = text =~ /(?s)<strong[^>]*>Error<\/strong>:(.*?)<\/p>/
        if (matcher.find()) {
            text = matcher.group(1)
        }
        text = text.replaceAll("<[^>]*>", " ").replaceAll("\\s+", " ").trim()
        return text.length() > 500 ? text.substring(0, 500) + "..." : text
    } catch (Exception ignored) {
        return ""
    }
}

/**
 * Return the delay in ms given by the Retry-After http header, 0 if not set.
 */
private long getRetryAfter(HttpURLConnection connection) {
    def retryAfter = connection.getHeaderField("Retry-After")
    if (retryAfter && retryAfter.trim().isLong()) {
        return TimeUnit.SECONDS.toMillis(retryAfter.trim() as long)
    }
    return 0L
}

/**
 * Read the Overpass status page to know when a query slot will be available.
 *
 * @return the delay in ms before a slot is available, 0 if unknown or available now
 */
private long getOverpassWaitTime(String endPoint) {
    try {
        String status = getServerStatus(endPoint)
        if (!status || status =~ /[1-9]\d* slots? available now/) {
            return 0L
        }
        def waits = (status =~ /in (\d+) seconds/).collect { it[1] as long }
        return waits ? TimeUnit.SECONDS.toMillis(waits.min() + 1) : 0L
    } catch (Exception ignored) {
        return 0L
    }
}

/**
 * Return the http timeout in ms to use for a query : the [timeout:...] value of the query if any,
 * 3 minutes otherwise.
 */
private int getOverpassQueryTimeout(String query) {
    Matcher timeoutMatcher = Pattern.compile("\\[timeout:(\\d+)\\]").matcher(query)
    if (timeoutMatcher.find()) {
        //Let the server some time to send its answer after the query timeout
        return (int) TimeUnit.SECONDS.toMillis(Integer.parseInt(timeoutMatcher.group(1)) + 30)
    }
    return (int) TimeUnit.MINUTES.toMillis(3)
}


/**
 * Return the status of the Nominatim server.
 * @return true is the server is OK
 */
def isNominatimReady() {
    final String proxyHost = System.getProperty("http.proxyHost");
    final int proxyPort = Integer.parseInt(System.getProperty("http.proxyPort", "80"));
    def connection
    def endPoint = "https://nominatim.openstreetmap.org/status"

    if (proxyHost != null) {
        def proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        connection = new URL(endPoint).openConnection(proxy) as HttpURLConnection
    } else {
        connection = new URL(endPoint).openConnection() as HttpURLConnection
    }
    connection.setRequestProperty("User-Agent", "GEOCLIMATE/${version()}")

    connection.setConnectTimeout(OVERPASS_TIMEOUT)
    connection.setReadTimeout(OVERPASS_TIMEOUT)

    connection.requestMethod = GET
    def ok = connection.responseCode == RESPONSECODE_OK
    connection.disconnect()
    return ok
}