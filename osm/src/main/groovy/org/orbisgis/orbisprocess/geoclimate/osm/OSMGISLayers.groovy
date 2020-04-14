package org.orbisgis.orbisprocess.geoclimate.osm

import JsonSlurper
import BaseScript
import ST_Transform
import SFSUtilities
import GeographyUtils
import Envelope
import Geometry
import MultiPolygon
import Polygon
import OSMElement
import JdbcDataSource
import IProcess
import PrepareData
import OSMTools

@BaseScript PrepareData prepareData


/**
  * This process is used to create the GIS layers using the Overpass API
  * @param datasource A connexion to a DB to load the OSM file
  * @param zoneToExtract A zone to extract.
  * Can be, a name of the place (neighborhood, city, etc. - cf https://wiki.openstreetmap.org/wiki/Key:level)
  * or a bounding box specified as a JTS envelope
  * @param distance in meters to expand the envelope of the query box. Default is 0
  * @return The name of the resulting GIS tables : buildingTableName, roadTableName,
  * railTableName, vegetationTableName, hydroTableName, zoneTableName, zoneEnvelopeTableName.
  * Note that the GIS tables are projected in a local utm projection
 */
IProcess extractAndCreateGISLayers(){
    return create({
        title "Create GIS layer from the OSM data model"
        inputs datasource: JdbcDataSource, zoneToExtract: Object, distance:0
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String,hydroTableName: String, zoneTableName: String,
                zoneEnvelopeTableName: String
        run { datasource, zoneToExtract, distance ->
            def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            if (datasource == null) {
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error('The datasource cannot be null')
                return null
            }
            if(zoneToExtract){
                def GEOMETRY_TYPE
                Geometry geom
                if(zoneToExtract in Collection){
                     GEOMETRY_TYPE = "POLYGON"
                     geom = OSMTools.Utilities.geometryFromOverpass(zoneToExtract)
                    if (!geom) {
                        org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error("The bounding box cannot be null")
                        return null

                    }
                }
                else if(zoneToExtract instanceof  String){
                    geom = OSMTools.Utilities.getAreaFromPlace(zoneToExtract);
                    if (!geom) {
                        org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error("Cannot find an area from the place name ${zoneToExtract}")
                        return null
                    } else {
                         GEOMETRY_TYPE = "GEOMETRY"
                        if (geom instanceof Polygon) {
                            GEOMETRY_TYPE = "POLYGON"
                        } else if (geom instanceof MultiPolygon) {
                            GEOMETRY_TYPE = "MULTIPOLYGON"
                        }
                    }
                }
                else{
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error("The zone to extract must be a place name or a JTS envelope")
                    return null;
                }

                /**
                 * Extract the OSM file from the envelope of the geometry
                 */
                def envelope = GeographyUtils.expandEnvelopeByMeters(geom.getEnvelopeInternal(), distance)

                //Find the best utm zone
                //Reproject the geometry and its envelope to the UTM zone
                def con = datasource.getConnection();
                def interiorPoint = envelope.centre()
                def epsg = SFSUtilities.getSRID(con, interiorPoint.y as float, interiorPoint.x as float)
                def geomUTM = ST_Transform.ST_Transform(con, geom, epsg)
                def tmpGeomEnv = geom.getFactory().toGeometry(envelope)
                tmpGeomEnv.setSRID(4326)

                datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${geomUTM.toString()}', $epsg), '$zoneToExtract');"""

                datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${ST_Transform.ST_Transform(con,tmpGeomEnv,epsg).toString()}',$epsg), '$zoneToExtract');"""

                def query =  "[maxsize:1073741824]" + OSMTools.Utilities.buildOSMQuery(envelope,null,OSMElement.NODE, OSMElement.WAY, OSMElement.RELATION)

                def extract = OSMTools.Loader.extract()
                if (extract.execute(overpassQuery: query)) {
                    IProcess createGISLayerProcess = createGISLayers()
                    if (createGISLayerProcess.execute(datasource: datasource, osmFilePath: extract.results.outputFilePath, epsg: epsg)) {

                        [buildingTableName  : createGISLayerProcess.getResults().buildingTableName,
                         roadTableName      : createGISLayerProcess.getResults().roadTableName,
                         railTableName      : createGISLayerProcess.getResults().railTableName,
                         vegetationTableName: createGISLayerProcess.getResults().vegetationTableName,
                         hydroTableName     : createGISLayerProcess.getResults().hydroTableName,
                         imperviousTableName     : createGISLayerProcess.getResults().imperviousTableName,
                         zoneTableName      : outputZoneTable,
                         zoneEnvelopeTableName: outputZoneEnvelopeTable]
                    } else {
                        org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error "Cannot load the OSM file ${extract.results.outputFilePath}"
                    }
                } else {
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error "Cannot execute the overpass query $query"
                }

            }else{
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error "The zone to extract cannot be null or empty"
                return null
            }
        }
    }
    )
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
    return create({
        title "Create GIS layer from an OSM XML file"
        inputs datasource: JdbcDataSource, osmFilePath: String, epsg: -1
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String, hydroTableName: String, imperviousTableName: String
        run { datasource, osmFilePath, epsg ->
            if(epsg<=-1){
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.error "Invalid epsg code $epsg"
                return null
            }
            def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def load = OSMTools.Loader.load()
            org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Loading"
            if (load(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: osmFilePath)) {
                def outputBuildingTableName =null
                def outputRoadTableName =null
                def outputRailTableName =null
                def outputVegetationTableName =null
                def outputHydroTableName =null
                def outputImperviousTableName =null
                //Create building layer
                def transform = OSMTools.Transform.toPolygons()
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Create the building layer"
                def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
                def parametersMap = readJSONParameters(paramsDefaultFile)
                def tags = parametersMap.get("tags")
                def columnsToKeep = parametersMap.get("columns")
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep:columnsToKeep)){
                    outputBuildingTableName = transform.results.outputTableName
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Building layer created"
                }

                //Create road layer
                transform = OSMTools.Transform.extractWaysAsLines()
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Create the road layer"
                paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
                 outputRoadTableName = transform.results.outputTableName
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Road layer created"
                    }

                //Create rail layer
                transform = OSMTools.Transform.extractWaysAsLines()
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Create the rail layer"
                paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
                    outputRailTableName = transform.results.outputTableName
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Rail layer created"
                }
                //Create vegetation layer
                paramsDefaultFile = this.class.getResourceAsStream("vegetParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Create the vegetation layer"
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags,columnsToKeep: columnsToKeep)){
                    outputVegetationTableName = transform.results.outputTableName
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Vegetation layer created"
                }

                //Create water layer
                paramsDefaultFile = this.class.getResourceAsStream("waterParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                transform = OSMTools.Transform.toPolygons()
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Create the water layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)){
                    outputHydroTableName = transform.results.outputTableName
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Water layer created"
                }

                //Create impervious layer
                paramsDefaultFile = this.class.getResourceAsStream("imperviousParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "Create the impervious layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags,columnsToKeep: columnsToKeep)){
                    outputImperviousTableName = transform.results.outputTableName
                    org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData.logger.info "impervious layer created"
                }
                //Drop the OSM tables
                OSMTools.Utilities.dropOSMTables(prefix, datasource)

                [buildingTableName  : outputBuildingTableName, roadTableName: outputRoadTableName, railTableName: outputRailTableName,
                 vegetationTableName: outputVegetationTableName, hydroTableName: outputHydroTableName,
                 imperviousTableName: outputImperviousTableName]
            }
        }
    })
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
