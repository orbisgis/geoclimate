package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.orbisanalysis.osm.OSMTools
import org.orbisgis.orbisanalysis.osm.utils.Utilities
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.h2gis.utilities.GeographyUtilities
import org.h2gis.functions.spatial.crs.ST_Transform
import org.orbisgis.orbisanalysis.osm.utils.OSMElement

@BaseScript OSM_Utils osm_utils

/**
  * This process is used to create the GIS layers using the Overpass API
  * @param datasource A connexion to a DB to load the OSM file
  * @param zoneToExtract A zone to extract.
  * Can be, a name of the place (neighborhood, city, etc. - cf https://wiki.openstreetmap.org/wiki/Key:level)
  * or a bounding box specified as a JTS envelope
  * @param distance in meters to expand the envelope of the query box. Default is 0
  * @return The name of the resulting GIS tables : buildingTableName, roadTableName,
  * railTableName, vegetationTableName, hydroTableName, zoneTableName, zoneEnvelopeTableName and urbanAreasTableName.
  * Note that the GIS tables are projected in a local utm projection
 */
IProcess extractAndCreateGISLayers() {
    return create {
        title "Create GIS layer from the OSM data model"
        id "extractAndCreateGISLayers"
        inputs datasource: JdbcDataSource, zoneToExtract: Object, distance: 0, downloadAllOSMData : true
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String, hydroTableName: String, imperviousTableName : String,
                urbanAreasTableName: String, zoneTableName: String,
                zoneEnvelopeTableName: String, coastlineTableName : String
        run { datasource, zoneToExtract, distance,downloadAllOSMData ->
            if (datasource == null) {
                error('The datasource cannot be null')
                return null
            }
            if (zoneToExtract) {
                def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                def GEOMETRY_TYPE
                Geometry geom
                if (zoneToExtract in Collection) {
                    GEOMETRY_TYPE = "POLYGON"
                    geom = Utilities.geometryFromOverpass(zoneToExtract)
                    if (!geom) {
                        error("The bounding box cannot be null")
                        return null
                    }
                } else if (zoneToExtract instanceof String) {
                    geom = Utilities.getAreaFromPlace(zoneToExtract);
                    if (!geom) {
                        error("Cannot find an area from the place name ${zoneToExtract}")
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
                    error("The zone to extract must be a place name or a JTS envelope")
                    return null;
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
        INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomUTM.toString()}', ${epsg}), '${zoneToExtract.toString()}');"""

                datasource.execute """drop table if exists ${outputZoneEnvelopeTable}; 
         create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
        INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(con, tmpGeomEnv, epsg).toString()}',${epsg}), '${zoneToExtract.toString()}');
        """

                //Prepare OSM extraction
                def query = "[maxsize:1073741824]" + Utilities.buildOSMQuery(envelope, null, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

                if(downloadAllOSMData){
                    //Create a custom OSM query to download all requiered data. It will take more time and resources
                    //because much more OSM elements will be returned
                    def  keysValues = ["building", "railway", "amenity",
                                       "leisure", "highway", "natural",
                                       "landuse", "landcover",
                                       "vegetation","waterway"]
                    query =  "[maxsize:1073741824]"+ Utilities.buildOSMQueryWithAllData(envelope, keysValues, OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)
                }

                def extract = OSMTools.Loader.extract()
                if (extract.execute(overpassQuery: query)) {
                    IProcess createGISLayerProcess = createGISLayers()
                    if (createGISLayerProcess.execute(datasource: datasource, osmFilePath: extract.results.outputFilePath, epsg: epsg)) {
                        def results =createGISLayerProcess.getResults();
                        return [buildingTableName    : results.buildingTableName,
                         roadTableName        : results.roadTableName,
                         railTableName        : results.railTableName,
                         vegetationTableName  : results.vegetationTableName,
                         hydroTableName       : results.hydroTableName,
                         imperviousTableName  : results.imperviousTableName,
                         urbanAreasTableName : results.urbanAreasTableName,
                         zoneTableName        : outputZoneTable,
                         zoneEnvelopeTableName: outputZoneEnvelopeTable,
                         coastlineTableName : results.coastlineTableName]
                    } else {
                        error "Cannot load the OSM file ${extract.results.outputFilePath}"
                    }
                } else {
                    error "Cannot execute the overpass query $query"
                }

            } else {
                error "The zone to extract cannot be null or empty"
                return null
            }
        }
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
IProcess createGISLayers() {
    return create {
        title "Create GIS layer from an OSM XML file"
        id "createGISLayers"
        inputs datasource: JdbcDataSource, osmFilePath: String, epsg: -1
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String, hydroTableName: String, imperviousTableName: String,
                urbanAreasTableName :String, coastlineTableName : String
        run { datasource, osmFilePath, epsg ->
            if (epsg <= -1) {
                error "Invalid epsg code $epsg"
                return null
            }
            def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def load = OSMTools.Loader.load()
            info "Loading"
            if (load(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: osmFilePath)) {
                def outputBuildingTableName = null
                def outputRoadTableName = null
                def outputRailTableName = null
                def outputVegetationTableName = null
                def outputHydroTableName = null
                def outputImperviousTableName = null
                def outputUrbanAreasTableName = null
                def outputCoastlineTableName =null
                //Create building layer
                def transform = OSMTools.Transform.toPolygons()
                info "Create the building layer"
                def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
                def parametersMap = readJSONParameters(paramsDefaultFile)
                def tags = parametersMap.get("tags")
                def columnsToKeep = parametersMap.get("columns")
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                    outputBuildingTableName = postfix("OSM_BUILDING")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputBuildingTableName")
                    info "Building layer created"
                }

                //Create road layer
                transform = OSMTools.Transform.extractWaysAsLines()
                info "Create the road layer"
                paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                    outputRoadTableName = postfix("OSM_ROAD")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputRoadTableName")
                    info "Road layer created"
                }

                //Create rail layer
                transform = OSMTools.Transform.extractWaysAsLines()
                info "Create the rail layer"
                paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                    outputRailTableName = postfix("OSM_RAIL")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputRailTableName")
                    info "Rail layer created"
                }
                //Create vegetation layer
                paramsDefaultFile = this.class.getResourceAsStream("vegetParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                info "Create the vegetation layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                    outputVegetationTableName = postfix("OSM_VEGETATION")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputVegetationTableName")
                    info "Vegetation layer created"
                }

                //Create water layer
                paramsDefaultFile = this.class.getResourceAsStream("waterParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                transform = OSMTools.Transform.toPolygons()
                info "Create the water layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)) {
                    outputHydroTableName = postfix("OSM_WATER")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputHydroTableName")
                    info "Water layer created"
                }

                //Create impervious layer
                paramsDefaultFile = this.class.getResourceAsStream("imperviousParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                info "Create the impervious layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                    outputImperviousTableName = postfix("OSM_IMPERVIOUS")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputImperviousTableName")
                    info "Impervious layer created"
                }

                //Create urban areas layer
                paramsDefaultFile = this.class.getResourceAsStream("urbanAreasParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                info "Create the urban areas layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)) {
                    outputUrbanAreasTableName = postfix("OSM_URBAN_AREAS")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputUrbanAreasTableName")
                    info "Urban areas layer created"
                }

                //Create coastline layer
                transform = OSMTools.Transform.toLines()
                info "Create the coastline layer"
                paramsDefaultFile = this.class.getResourceAsStream("coastlineParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags = parametersMap.get("tags")
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)) {
                    outputCoastlineTableName = postfix("OSM_COASTLINE")
                    datasource.execute("ALTER TABLE ${transform.results.outputTableName} RENAME TO $outputCoastlineTableName")
                    info "Coastline layer created"
                }

                //Drop the OSM tables
                Utilities.dropOSMTables(prefix, datasource)

                [buildingTableName  : outputBuildingTableName, roadTableName: outputRoadTableName, railTableName: outputRailTableName,
                 vegetationTableName: outputVegetationTableName, hydroTableName: outputHydroTableName,
                 imperviousTableName: outputImperviousTableName,  urbanAreasTableName: outputUrbanAreasTableName,
                 coastlineTableName: outputCoastlineTableName]
            }
        }
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
