package org.orbisgis.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.GeographyUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.osmtools.OSMTools
import org.orbisgis.geoclimate.osmtools.utils.OSMElement
import org.orbisgis.geoclimate.osmtools.utils.Utilities

@BaseScript OSM OSM

/**
 * This process is used to create the GIS layers using the Overpass API
 * @param datasource A connexion to a DB to load the OSM file
 * @param zoneToExtract A zone to extract.
 * Can be, a name of the place (neighborhood, city, etc. - cf https://wiki.openstreetmap.org/wiki/Key:level)
 * or a bounding box specified as a JTS envelope
 * @param distance in meters to expand the envelope of the query box. Default is 0
 * @return The name of the resulting GIS tables : buildingTableName, roadTableName,
 * railTableName, vegetationTableName, hydroTableName, zone, zoneEnvelopeTableName and urbanAreasTableName.
 * Note that the GIS tables are projected in a local utm projection
 */
Map extractAndCreateGISLayers(JdbcDataSource datasource, Object zoneToExtract, float distance = 0, boolean downloadAllOSMData = true) {
    if (datasource == null) {
        error('The datasource cannot be null')
        return null
    }
    if (zoneToExtract) {
        def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
        def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
        def GEOMETRY_TYPE = "GEOMETRY"
        Geometry geom = Utilities.getArea(zoneToExtract)
        if (!geom) {
            error("Cannot find an area from the place name ${zoneToExtract}")
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
         * Extract the OSM file from the envelope of the geometry
         */
        def envelope = GeographyUtilities.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

        //Find the best utm zone
        //Reproject the geometry and its envelope to the UTM zone
        def con = datasource.getConnection();
        def interiorPoint = envelope.centre()
        def epsg = GeographyUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
        def geomUTM = ST_Transform.ST_Transform(con, geom, epsg)
        def tmpGeomEnv = geom.getFactory().toGeometry(envelope)
        tmpGeomEnv.setSRID(4326)

        datasource.execute """drop table if exists ${outputZoneTable}; 
        create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomUTM.toString()}', ${epsg}), '${zoneToExtract.toString()}');""".toString()

        datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; 
         create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(con, tmpGeomEnv, epsg).toString()}',${epsg}), '${zoneToExtract.toString()}');
        """.toString()

        //Prepare OSM extraction
        def query = "[maxsize:1073741824]" + OSMTools.Utilities.buildOSMQuery(envelope, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

        if (downloadAllOSMData) {
            //Create a custom OSM query to download all requiered data. It will take more time and resources
            //because much more OSM elements will be returned
            def keysValues = ["building", "railway", "amenity",
                              "leisure", "highway", "natural",
                              "landuse", "landcover",
                              "vegetation", "waterway", "area", "aeroway", "area:aeroway"]
            query = "[maxsize:1073741824]" + OSMTools.Utilities.buildOSMQueryWithAllData(envelope, keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
        }

        def extract = OSMTools.Loader.extract(query)
        if (extract) {
            Map results = createGISLayers(datasource: datasource, osmFilePath: extract, epsg: epsg)
            if (results) {
                return [building     : results.buildingTableName,
                        road         : results.roadTableName,
                        rail         : results.railTableName,
                        vegetation   : results.vegetationTableName,
                        water        : results.hydroTableName,
                        impervious   : results.imperviousTableName,
                        urban_areas  : results.urbanAreasTableName,
                        zone         : outputZoneTable,
                        zone_envelope: outputZoneEnvelopeTable,
                        coastline    : results.coastlineTableName]
            } else {
                error "Cannot load the OSM file ${extract}"
            }
        } else {
            error "Cannot execute the overpass query $query"
        }

    } else {
        error "The zone to extract cannot be null or empty"
        return null
    }
}

/**
 * This process is used to create the GIS layers from an OSM xml file
 * @param datasource A connexion to a DB to load the OSM file
 * @param osmFilePath a path to the OSM xml file
 * @param epsg code to reproject the GIS layers, default is -1
 *
 * @return The name of the resulting GIS tables : buildingTableName, roadTableName,
 * railTableName, vegetationTableName, hydroTableName, imperviousTableName
 */
Map createGISLayers(JdbcDataSource datasource, String osmFilePath, int epsg = -1) {
    if (epsg <= -1) {
        error "Invalid epsg code $epsg"
        return null
    }
    def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
    debug "Loading"
    if (OSMTools.Loader.load(datasource, prefix, osmFilePath)) {
        def outputBuildingTableName = null
        def outputRoadTableName = null
        def outputRailTableName = null
        def outputVegetationTableName = null
        def outputHydroTableName = null
        def outputImperviousTableName = null
        def outputUrbanAreasTableName = null
        def outputCoastlineTableName = null
        //Create building layer
        debug "Create the building layer"
        def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
        def parametersMap = readJSONParameters(paramsDefaultFile)
        def tags = parametersMap.get("tags")
        def columnsToKeep = parametersMap.get("columns")
        def building = OSMTools.Transform.toPolygons(datasource, prefix, epsg, tags, columnsToKeep)
        if (building) {
            outputBuildingTableName = postfix("OSM_BUILDING")
            datasource.execute("ALTER TABLE ${building} RENAME TO $outputBuildingTableName".toString())
            info "Building layer created"
        }

        //Create road layer
        debug "Create the road layer"
        paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        columnsToKeep = parametersMap.get("columns")
        String road = OSMTools.Transform.extractWaysAsLines(datasource, prefix, epsg, tags, columnsToKeep)
        if (road) {
            outputRoadTableName = postfix("OSM_ROAD")
            datasource.execute("ALTER TABLE ${road} RENAME TO $outputRoadTableName".toString())
            info "Road layer created"
        }

        //Create rail layer
        debug "Create the rail layer"
        paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        columnsToKeep = parametersMap.get("columns")
        String rail = OSMTools.Transform.extractWaysAsLines(datasource, prefix, epsg, tags, columnsToKeep)
        if (rail) {
            outputRailTableName = postfix("OSM_RAIL")
            datasource.execute("ALTER TABLE ${rail} RENAME TO $outputRailTableName".toString())
            info "Rail layer created"
        }
        //Create vegetation layer
        paramsDefaultFile = this.class.getResourceAsStream("vegetParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        columnsToKeep = parametersMap.get("columns")
        String vegetation = OSMTools.Transform.toPolygons(datasource, prefix, epsg, tags, columnsToKeep)
        debug "Create the vegetation layer"
        if (vegetation) {
            outputVegetationTableName = postfix("OSM_VEGETATION")
            datasource.execute("ALTER TABLE ${vegetation} RENAME TO $outputVegetationTableName".toString())
            info "Vegetation layer created"
        }

        //Create water layer
        paramsDefaultFile = this.class.getResourceAsStream("waterParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        columnsToKeep = parametersMap.get("columns")
        String water = OSMTools.Transform.toPolygons(datasource, prefix, epsg, tags, columnsToKeep)
        debug "Create the water layer"
        if (water) {
            outputHydroTableName = postfix("OSM_WATER")
            datasource.execute("ALTER TABLE ${water} RENAME TO $outputHydroTableName".toString())
            info "Water layer created"
        }

        //Create impervious layer
        paramsDefaultFile = this.class.getResourceAsStream("imperviousParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        columnsToKeep = parametersMap.get("columns")
        String impervious = OSMTools.Transform.toPolygons(datasource, prefix, epsg, tags, columnsToKeep)
        debug "Create the impervious layer"
        if (impervious) {
            outputImperviousTableName = postfix("OSM_IMPERVIOUS")
            datasource.execute("ALTER TABLE ${impervious} RENAME TO $outputImperviousTableName".toString())
            info "Impervious layer created"
        }

        //Create urban areas layer
        paramsDefaultFile = this.class.getResourceAsStream("urbanAreasParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        columnsToKeep = parametersMap.get("columns")
        String urban_areas = OSMTools.Transform.toPolygons(datasource, prefix, epsg, tags, columnsToKeep)
        debug "Create the urban areas layer"
        if (urban_areas) {
            outputUrbanAreasTableName = postfix("OSM_URBAN_AREAS")
            datasource.execute("ALTER TABLE ${urban_areas} RENAME TO $outputUrbanAreasTableName".toString())
            info "Urban areas layer created"
        }

        //Create coastline layer
        debug "Create the coastline layer"
        paramsDefaultFile = this.class.getResourceAsStream("coastlineParams.json")
        parametersMap = readJSONParameters(paramsDefaultFile)
        tags = parametersMap.get("tags")
        String coastlines = OSMTools.Transform.toLines(datasource, prefix, epsg, tags, [])
        if (coastlines) {
            outputCoastlineTableName = postfix("OSM_COASTLINE")
            datasource.execute("ALTER TABLE ${coastlines} RENAME TO $outputCoastlineTableName".toString())
            info "Coastline layer created"
        }

        //Drop the OSM tables
        OSMTools.Utilities.dropOSMTables(prefix, datasource)

        return [building  : outputBuildingTableName, road: outputRoadTableName, rail: outputRailTableName,
                vegetation: outputVegetationTableName, water: outputHydroTableName,
                impervious: outputImperviousTableName, urban_areas: outputUrbanAreasTableName,
                coastline : outputCoastlineTableName]
    }
}

/**
 * Parse a json file to a Map
 * @param jsonFile
 * @return
 */
static def readJSONParameters(def jsonFile) {
    if (jsonFile) {
        new JsonSlurper().parse(jsonFile)
    }
}
