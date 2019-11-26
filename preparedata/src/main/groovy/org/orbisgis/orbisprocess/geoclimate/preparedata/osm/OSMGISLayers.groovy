package org.orbisgis.orbisprocess.geoclimate.preparedata.osm

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.MultiPolygon
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.osm.OSMTools
import org.orbisgis.processmanagerapi.IProcess

@BaseScript PrepareData prepareData


/**
  * This process is used to create the GIS layers using the Overpass API
  * @param datasource A connexion to a DB to load the OSM file
  * @param placeName the name of the place to extract
  * @param epsg code to reproject the GIS layers, default is -1
  * @param distance to expand the envelope of the query box. Default is 0
  * @return The name of the resulting GIS tables : buildingTableName, roadTableName,
  * railTableName, vegetationTableName, hydroTableName, zoneTableName, zoneEnvelopeTableName
 *  and the epsg of the processed zone
 */
IProcess extractAndCreateGISLayers(){
    return create({
        title "Create GIS layer from the OSM data model"
        inputs datasource: JdbcDataSource, placeName: String, epsg:-1,distance:0
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String,hydroTableName: String, zoneTableName: String,
                zoneEnvelopeTableName: String, epsg: int
        run { datasource, placeName, epsg, distance ->
            def outputZoneTable = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def outputZoneEnvelopeTable = "ZONE_ENVELOPE_${UUID.randomUUID().toString().replaceAll("-", "_")}"

            if (datasource == null) {
                logger.error('The datasource cannot be null')
                return null
            }
            Geometry geom = OSMTools.Utilities.getAreaFromPlace(placeName);

            if (geom == null) {
                logger.error("Cannot find an area from the place name ${placeName}")
                return null
            } else {
                def GEOMETRY_TYPE = "GEOMETRY"
                if(geom instanceof Polygon){
                    GEOMETRY_TYPE ="POLYGON"
                }else if(geom instanceof MultiPolygon){
                    GEOMETRY_TYPE ="MULTIPOLYGON"
                }
                /**
                 * Extract the OSM file from the envelope of the geometry
                 */
                def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, epsg, distance, datasource)
                epsg = geomAndEnv.geom.getSRID()

                datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(${GEOMETRY_TYPE}, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${
                    geomAndEnv.geom.toString()
                }', $epsg), '$placeName');"""

                datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${
                    ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg).toString()
                }',$epsg), '$placeName');"""

                Envelope envelope  = geomAndEnv.filterArea.getEnvelopeInternal()
                def query =  "[maxsize:1073741824];((node(${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()});" +
                        "way(${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()});" +
                        "relation(${envelope.getMinY()},${envelope.getMinX()},${envelope.getMaxY()}, ${envelope.getMaxX()}););>;);out;"

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
                         zoneEnvelopeTableName: outputZoneEnvelopeTable,
                         epsg: epsg]
                    } else {
                        logger.error "Cannot load the OSM file ${extract.results.outputFilePath}"
                    }
                } else {
                    logger.error "Cannot execute the overpass query $query"
                }
            }
        }
    }
    )
}

/**
 * This process is used to create the GIS layers from an osm xml file
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName the name of the place to extract
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
                vegetationTableName: String, hydroTableName: String, imperviousTableName: String, epsg: int
        run { datasource, osmFilePath, epsg ->
            if(epsg<=-1){
                logger.error "Invalid epsg code $epsg"
                return null
            }
            def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def load = OSMTools.Loader.load()
            logger.info "Loading"
            if (load(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: osmFilePath)) {
                def outputBuildingTableName =null
                def outputRoadTableName =null
                def outputRailTableName =null
                def outputVegetationTableName =null
                def outputHydroTableName =null
                def outputImperviousTableName =null
                //Create building layer
                def transform = OSMTools.Transform.toPolygons()
                logger.info "Create the building layer"
                def paramsDefaultFile = this.class.getResourceAsStream("buildingParams.json")
                def parametersMap = readJSONParameters(paramsDefaultFile)
                def tags = parametersMap.get("tags")
                def columnsToKeep = parametersMap.get("columns")
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep:columnsToKeep)){
                    outputBuildingTableName = transform.results.outputTableName
                    logger.info "Building layer created"
                }

                //Create road layer
                transform = OSMTools.Transform.extractWaysAsLines()
                logger.info "Create the road layer"
                paramsDefaultFile = this.class.getResourceAsStream("roadParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
                 outputRoadTableName = transform.results.outputTableName
                logger.info "Road layer created"
                    }

                //Create rail layer
                transform = OSMTools.Transform.extractWaysAsLines()
                logger.info "Create the rail layer"
                paramsDefaultFile = this.class.getResourceAsStream("railParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
                    outputRailTableName = transform.results.outputTableName
                    logger.info "Rail layer created"
                }
                //Create vegetation layer
                paramsDefaultFile = this.class.getResourceAsStream("vegetParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                logger.info "Create the vegetation layer"
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags,columnsToKeep: columnsToKeep)){
                    outputVegetationTableName = transform.results.outputTableName
                    logger.info "Vegetation layer created"
                }

                //Create water layer
                paramsDefaultFile = this.class.getResourceAsStream("waterParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                transform = OSMTools.Transform.toPolygons()
                logger.info "Create the water layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)){
                    outputHydroTableName = transform.results.outputTableName
                    logger.info "Water layer created"
                }

                //Create impervious layer
                paramsDefaultFile = this.class.getResourceAsStream("imperviousParams.json")
                parametersMap = readJSONParameters(paramsDefaultFile)
                tags  = parametersMap.get("tags")
                columnsToKeep = parametersMap.get("columns")
                transform = OSMTools.Transform.toPolygons()
                logger.info "Create the impervious layer"
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags,columnsToKeep: columnsToKeep)){
                    outputImperviousTableName = transform.results.outputTableName
                    logger.info "impervious layer created"
                }
                //Drop the OSM tables
                OSMTools.Utilities.dropOSMTables(prefix, datasource)

                [buildingTableName  : outputBuildingTableName, roadTableName: outputRoadTableName, railTableName: outputRailTableName,
                 vegetationTableName: outputVegetationTableName, hydroTableName: outputHydroTableName,
                 imperviousTableName: outputImperviousTableName, epsg: epsg]
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

