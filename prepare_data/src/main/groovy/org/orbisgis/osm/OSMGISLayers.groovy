package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.PrepareData
import org.orbisgis.osm.utils.OSMElement
import org.orbisgis.processmanagerapi.IProcess

import static org.orbisgis.osm.utils.OSMElement.NODE
import static org.orbisgis.osm.utils.OSMElement.RELATION
import static org.orbisgis.osm.utils.OSMElement.WAY

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
                /**
                 * Extract the OSM file from the envelope of the geometry
                 */
                def geomAndEnv = OSMTools.Utilities.buildGeometryAndZone(geom, epsg, distance, datasource)
                epsg = geomAndEnv.geom.getSRID()

                datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${
                    geomAndEnv.geom.toString()
                }', $epsg), '$placeName');"""

                datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${
                    ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg).toString()
                }',$epsg), '$placeName');"""

                def query = OSMTools.Utilities.buildOSMQuery(geomAndEnv.filterArea, [], NODE, WAY, RELATION)
                def extract = OSMTools.Loader.extract()
                if (extract.execute(overpassQuery: query)) {
                    IProcess createGISLayerProcess = createGISLayers()
                    if (createGISLayerProcess.execute(datasource: datasource, osmFilePath: extract.results.outputFilePath, epsg: epsg)) {

                        [buildingTableName  : createGISLayerProcess.getResults().buildingTableName,
                         roadTableName      : createGISLayerProcess.getResults().roadTableName,
                         railTableName      : createGISLayerProcess.getResults().railTableName,
                         vegetationTableName: createGISLayerProcess.getResults().vegetationTableName,
                         hydroTableName     : createGISLayerProcess.getResults().hydroTableName,
                         zoneTableName      : outputZoneTable, zoneEnvelopeTableName: outputZoneEnvelopeTable,
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
 * railTableName, vegetationTableName, hydroTableName
 */
IProcess createGISLayers() {
    return create({
        title "Create GIS layer from an OSM XML file"
        inputs datasource: JdbcDataSource, osmFilePath: String, epsg: -1
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String, hydroTableName: String, epsg: int
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
                def outputhydroTableName =null
                //Create building layer
                def transform = OSMTools.Transform.toPolygons()
                logger.info "Create the building layer"
                def tags = ['building']
                def columnsToKeep = ['layer','height', 'building:height', 'roof:height', 'building:roof:height',
                                     'building:levels', 'roof:levels', 'building:roof:levels','shop',
                                     'amenity', 'aeroway', 'historic', 'leisure', 'monument',
                                     'place_of_worship', 'military', 'railway', 'public_transport',
                                     'barrier', 'government', 'historic:building', 'grandstand',
                                     'house', 'industrial', 'man_made', 'residential',
                                     'apartments', 'ruins', 'agricultural', 'barn', 'healthcare',
                                     'education', 'restaurant', 'sustenance', 'office']

                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep:columnsToKeep)){
                    outputBuildingTableName = transform.results.outputTableName
                    logger.info "Building layer created"
                 }

                //Create road layer
                transform = OSMTools.Transform.extractWaysAsLines()
                logger.info "Create the road layer"
                tags = ['highway', 'cycleway', 'biclycle_road', 'cyclestreet', 'route', 'junction']
                columnsToKeep = ['width','highway', 'surface', 'sidewalk',
                                 'lane','layer','maxspeed','oneway',
                                 'h_ref','route','cycleway',
                                 'biclycle_road','cyclestreet','junction']
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
                 outputRoadTableName = transform.results.outputTableName
                logger.info "Road layer created"
                    }

                //Create rail layer
                transform = OSMTools.Transform.extractWaysAsLines()
                logger.info "Create the rail layer"
                tags = ['railway']
                columnsToKeep =['highspeed','railway','service',
                                'tunnel','layer','bridge']
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)){
                    outputRailTableName = transform.results.outputTableName
                    logger.info "Rail layer created"
                }
                //Create vegetation layer
                tags = ['natural':['tree', 'wetland', 'grassland', 'tree_row', 'scrub', 'heath', 'sand', 'land', 'mud'],
                       'landuse':['farmland', 'forest', 'grass', 'meadow', 'orchard', 'vineyard', 'village_green'],
                       'landcover':[],
                        'vegetation':['grass'],'barrier':['hedge'],'fence_type':['hedge', 'wood'],
                                         'hedge':[],'wetland':[],'vineyard':[],
                                         'trees':[],'crop':[],'produce':[]]

                transform = OSMTools.Transform.toPolygons()
                logger.info "Create the vegetation layer"
                if(transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)){
                    outputVegetationTableName = transform.results.outputTableName
                    logger.info "Vegetation layer created"
                }

                //Create water layer
                tags = ['natural':['water','waterway','bay'],'water':[],'waterway':[]]
                transform = OSMTools.Transform.toPolygons()
                logger.info "Create the water layer"
                tags = ['natural', 'water', 'waterway']
                if (transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)){
                    outputhydroTableName = transform.results.outputTableName
                    logger.info "Water layer created"
                }
                //Drop the OSM tables
                OSMTools.Utilities.dropOSMTables(prefix, datasource)

                [buildingTableName  : outputBuildingTableName, roadTableName: outputRoadTableName, railTableName: outputRailTableName,
                 vegetationTableName: outputVegetationTableName, hydroTableName: outputhydroTableName, epsg: epsg]
            }
        }
    })
}




