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
package org.orbisgis.geoclimate.osmtools

import groovy.transform.BaseScript
import groovy.transform.Field
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource

import java.util.regex.Pattern

import static org.orbisgis.geoclimate.osmtools.utils.OSMElement.*

@BaseScript OSMTools pf

/** Default SRID */
@Field DEFAULT_SRID = 4326

/**
 * This process extracts OSM data file and load it in a database using an area
 * The area must be a JTS envelope
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param filterArea Filtering area as envelope
 * @param distance to expand the envelope of the query box. Default is 0
 *
 * @return The name of the tables that contains the geometry representation of the extracted area (outputZoneTable) and
 * its envelope (outputZoneEnvelopeTable)
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
def fromArea() {
    create {
        title "Extract the OSM data using an area"
        id "fromArea"
        inputs datasource: JdbcDataSource, filterArea: Object, distance: 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String, epsg: int
        run { JdbcDataSource datasource, filterArea, distance ->
            if(!datasource) {
                error("No datasource provided.")
                return
            }
            if (!filterArea) {
                error("Filter area not defined")
                return
            }
            def outputZoneTable = postfix "ZONE"
            def outputZoneEnvelopeTable = postfix "ZONE_ENVELOPE"
            def osmTablesPrefix = postfix "OSM_DATA"
            def geom
            if (filterArea instanceof Envelope) {
                geom = new GeometryFactory().toGeometry(filterArea)
            } else if (filterArea instanceof Polygon) {
                geom = filterArea
            } else {
                error "The filter area must be an Envelope or a Polygon"
                return
            }

            def epsg = DEFAULT_SRID
            def env = GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

            //Create table to store the geometry and the envelope of the extracted area
            datasource """
                CREATE TABLE $outputZoneTable (the_geom GEOMETRY(POLYGON, $epsg));
                INSERT INTO $outputZoneTable VALUES (ST_GEOMFROMTEXT('${geom}', $epsg));
        """.toString()

            def geometryFactory = new GeometryFactory()
            def geomEnv = geometryFactory.toGeometry(env)

            datasource.execute """CREATE TABLE $outputZoneEnvelopeTable (the_geom GEOMETRY(POLYGON, $epsg));
                    INSERT INTO $outputZoneEnvelopeTable VALUES 
                    (ST_GEOMFROMTEXT('$geomEnv',$epsg));""".toString()

            def query = Utilities.buildOSMQuery(geomEnv, [], NODE, WAY, RELATION)
            def extract = extract()
            if (extract(overpassQuery: query)) {
                info "Downloading OSM data from the area $filterArea"
                def load = load()
                if (load(datasource: datasource,
                        osmTablesPrefix: osmTablesPrefix,
                        osmFilePath: extract.results.outputFilePath)) {
                    info "Loading OSM data from the area $filterArea"
                    return [zoneTableName        : outputZoneTable,
                            zoneEnvelopeTableName: outputZoneEnvelopeTable,
                            osmTablesPrefix      : osmTablesPrefix,
                            epsg                 : epsg]
                } else {
                    error "Cannot load the OSM data from the area $filterArea"
                }
            } else {
                error "Cannot download OSM data from the area $filterArea"
            }
        }
    }
}


/**
 * This process extracts OSM data file and load it in a database using a place name
 *
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName The name of the place to extract
 * @param distance to expand the envelope of the query box. Default is 0
 *
 * @return The name of the tables that contains the geometry representation of the extracted area (outputZoneTable) and
 * its envelope extended or not by a distance (outputZoneEnvelopeTable)
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
def fromPlace() {
    create {
        title "Extract the OSM data using a place name"
        id "fromPlace"
        inputs datasource: JdbcDataSource, placeName: String, distance: 0
        outputs zoneTableName: String, zoneEnvelopeTableName: String, osmTablesPrefix: String
        run { JdbcDataSource datasource, placeName, distance ->
            if(!placeName) {
                error("Cannot find an area from a void place name.")
                return
            }
            if(!datasource) {
                error("No datasource provided.")
                return
            }
            def formatedPlaceName = placeName.trim().replaceAll("([\\s|,|\\-|])+", "_")
            def outputZoneTable = postfix "ZONE_$formatedPlaceName"
            def outputZoneEnvelopeTable = postfix "ZONE_ENVELOPE_$formatedPlaceName"
            def osmTablesPrefix = postfix "OSM_DATA_$formatedPlaceName"
            def epsg = DEFAULT_SRID

            def geom = Utilities.getAreaFromPlace(placeName);
            if (!geom) {
                error("Cannot find an area from the place name $placeName")
                return
            }
            if (distance < 0) {
                error("Cannot use a negative distance")
                return
            }
            def env = GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

            //Create table to store the geometry and the envelope of the extracted area
            datasource """
                CREATE TABLE $outputZoneTable (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
                INSERT INTO $outputZoneTable VALUES (ST_GEOMFROMTEXT('${geom}', $epsg), '$placeName');
        """

            def geometryFactory = new GeometryFactory()
            def geomEnv = geometryFactory.toGeometry(env)
            datasource """
                CREATE TABLE $outputZoneEnvelopeTable (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
                INSERT INTO $outputZoneEnvelopeTable VALUES (ST_GEOMFROMTEXT('$geomEnv',$epsg), '$placeName');
        """

            def query = Utilities.buildOSMQuery(geomEnv, [], NODE, WAY, RELATION)
            def extract = extract()
            if (extract(overpassQuery: query)) {
                info "Downloading OSM data from the place $placeName"
                def load = load()
                if (load(datasource: datasource,
                        osmTablesPrefix: osmTablesPrefix,
                        osmFilePath: extract.results.outputFilePath)) {
                    info "Loading OSM data from the place $placeName"
                    return [zoneTableName        : outputZoneTable,
                            zoneEnvelopeTableName: outputZoneEnvelopeTable,
                            osmTablesPrefix      : osmTablesPrefix]
                } else {
                    error "Cannot load the OSM data from the place $placeName"
                }

            } else {
                error "Cannot download OSM data from the place $placeName"
            }
        }
    }
}

/**
 * This process extracts OSM data as an XML file using the Overpass API
 *
 * @param overpassQuery The overpass api to be executed
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
def extract() {
    create {
        title "Extract the OSM data using the overpass api and save the result in an XML file"
        id "extract"
        inputs overpassQuery: String
        outputs outputFilePath: String
        run { overpassQuery ->
            info "Extract the OSM data"
            if(!overpassQuery){
                error "The query should not be null or empty."
                return
            }
            def bboxUrl = Utilities.utf8ToUrl(overpassQuery);
            //hash the query to cache it
            def queryHash = bboxUrl.digest('SHA-256')
            def outputOSMFile=new File(System.getProperty("java.io.tmpdir")+File.separator+"${queryHash}.osm")
            def osmFilePath = outputOSMFile.absolutePath
            if(outputOSMFile.exists()){
                if(outputOSMFile.length()==0){
                    outputOSMFile.delete()
                    if(outputOSMFile.createNewFile()) {
                        if ( Utilities.executeOverPassQuery(overpassQuery, outputOSMFile)) {
                            info "The OSM file has been downloaded at ${osmFilePath}."
                        } else {
                            outputOSMFile.delete()
                            error "Cannot extract the OSM data for the query $overpassQuery"
                            return
                        }
                    }
                }
                else {
                    debug "\nThe cached OSM file ${osmFilePath} will be re-used for the query :  \n$overpassQuery."
                }
            }
            else{
                if(outputOSMFile.createNewFile()){
                if (Utilities.executeOverPassQuery(overpassQuery, outputOSMFile)) {
                    info "The OSM file has been downloaded at ${osmFilePath}."
                } else {
                    outputOSMFile.delete()
                    error "Cannot extract the OSM data for the query $overpassQuery"
                    return
                }}
            }
            [outputFilePath: osmFilePath]
        }
    }
}

/**
 * This process is used to load an OSM file in a database.
 *
 * @param datasource A connection to a database
 * @param osmTablesPrefix A prefix to identify the 10 OSM tables
 * @param omsFilePath The path where the OSM file is
 *
 * @return datasource The connection to the database
 *
 * @author Erwan Bocher (CNRS LAB-STICC)
 * @author Elisabeth Le Saux (UBS LAB-STICC)
 */
def load() {
    create {
        title "Load an OSM file to the current database"
        id "load"
        inputs datasource: JdbcDataSource, osmTablesPrefix: String, osmFilePath: String
        outputs datasource: JdbcDataSource
        run { JdbcDataSource datasource, osmTablesPrefix, osmFilePath ->
            if (!datasource) {
                error "Please set a valid database connection."
                return
            }

            if (!osmTablesPrefix ||
                    !Pattern.compile('^[a-zA-Z0-9_]*$').matcher(osmTablesPrefix).matches()) {
                error "Please set a valid table prefix."
                return
            }

            if (!osmFilePath) {
                error "Please set a valid osm file path."
                return
            }
            def osmFile = new File(osmFilePath)
            if (!osmFile.exists()) {
                error "The input OSM file does not exist."
                return
            }

            info "Load the OSM file in the database."
            datasource.load(osmFile, osmTablesPrefix, true)
            info "The input OSM file has been loaded in the database."

            [datasource: datasource]
        }
    }
}
