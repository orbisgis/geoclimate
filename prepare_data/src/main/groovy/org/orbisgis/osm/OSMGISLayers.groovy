package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Polygon
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.PrepareData
import org.orbisgis.processmanagerapi.IProcess

import static org.orbisgis.osm.OSMElement.NODE
import static org.orbisgis.osm.OSMElement.RELATION
import static org.orbisgis.osm.OSMElement.WAY

@BaseScript PrepareData prepareData


/**
  * This process is used to create the GIS layers using the Overpass API
  * @param datasource A connexion to a DB to load the OSM file
  * @param placeName the name of the place to extract
  * @return
  */
IProcess extractAndCreateGISLayers(){
    return create({
        title "Create GIS layer from the OSM data model"
        inputs datasource: JdbcDataSource, placeName: String, epsg:-1,extendedValue:0
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String,hydroTableName: String
        run {datasource, placeName, epsg,  extendedValue ->

            if(datasource==null){
                logger.error('The datasource cannot be null')
                return null
            }
            Geometry geom = OSMHelper.Utilities.getAreaFromPlace(placeName);

            if(geom==null){
                logger.error("Cannot find an area from the place name ${placeName}")
                return null
            }
            /**
             * Extract the OSM file from the envelope of the geometry
             */
            if(epsg==-1){
                interiorPoint = geom.getCentroid()
                epsg = SFSUtilities.getSRID(datasource.getConnection(), interiorPoint.y as float, interiorPoint.x as float)
            }
            Envelope filterArea
            if(extendedValue==0){
                filterArea = geom.getEnvelopeInternal()
            }
            else {
                geom = ST_Transform.ST_Transform(datasource.getConnection(), geom, epsg).buffer(extendedValue)
                filterArea = ST_Transform.ST_Transform(datasource.getConnection(), geom, 4326).getEnvelopeInternal()
            }
            GeometryFactory gf = new GeometryFactory()
            Polygon polygon  = gf.toGeometry(filterArea)
            //def outputEnvelopeTableName = "ZONE_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            //datasource.execute "create table ${outputEnvelopeTableName}"
            def  query = OSMHelper.Utilities.buildOSMQuery(polygon, [], NODE, WAY, RELATION)
                def extract = OSMHelper.Loader.extract()
                if (extract.execute(overpassQuery: query)) {
                    def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
                    IProcess createGISLayerProcess = createGISLayers()
                    if (createGISLayerProcess.execute(datasource: datasource, osmFilePath: extract.results.outputFilePath, epsg:epsg)) {

                        [buildingTableName:  createGISLayerProcess.getResults().outputBuildingTableName,
                         roadTableName: createGISLayerProcess.getResults().outputRoadTableName,
                         railTableName: createGISLayerProcess.getResults().outputRailTableName,
                         vegetationTableName: createGISLayerProcess.getResults().outputVegetationTableName,
                         hydroTableName: createGISLayerProcess.getResults().outputhydroTableName]
                    }
                    else{
                        logger.error "Cannot load the OSM file ${extract.results.outputFilePath}"
                    }
                }
                else{
                    logger.error "Cannot execute the overpass query $query"
                }
        }
    }
    )
}

/**
 * This process is used to create the GIS layers from an osm xml file
 * @param datasource A connexion to a DB to load the OSM file
 * @param placeName the name of the place to extract
 * @return buildingTableName The name of the resulting buildings table
 */
IProcess createGISLayers() {
    return create({
        title "Create GIS layer from an OSM XML file"
        inputs datasource: JdbcDataSource, osmFilePath: String, epsg: -1
        outputs buildingTableName: String, roadTableName: String, railTableName: String,
                vegetationTableName: String, hydroTableName: String
        run { datasource, osmFilePath, epsg ->
            if(epsg<=-1){
                logger.error "Invalid epsg code $epsg"
                return null
            }
            def prefix = "OSM_DATA_${UUID.randomUUID().toString().replaceAll("-", "_")}"
            def load = OSMHelper.Loader.load()
            logger.info "Loading"
            if (load(datasource: datasource, osmTablesPrefix: prefix, osmFilePath: osmFilePath)) {
                //Create building layer
                def transform = OSMHelper.Transform.toPolygons()
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
                assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep:columnsToKeep)
                def outputBuildingTableName = transform.results.outputTableName
                logger.info "Building layer created"

                //Create road layer
                transform = OSMHelper.Transform.extractWaysAsLines()
                logger.info "Create the road layer"
                tags = ['highway', 'cycleway', 'biclycle_road', 'cyclestreet', 'route', 'junction']
                columnsToKeep = ['width','highway', 'surface', 'sidewalk',
                                 'lane','layer','maxspeed','oneway',
                                 'h_ref','route','cycleway',
                                 'biclycle_road','cyclestreet','junction']
                assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)
                def outputRoadTableName = transform.results.outputTableName
                logger.info "Road layer created"

                //Create rail layer
                transform = OSMHelper.Transform.extractWaysAsLines()
                logger.info "Create the rail layer"
                tags = ['railway']
                columnsToKeep =['highspeed','railway','service',
                                'tunnel','layer','bridge']
                assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags, columnsToKeep: columnsToKeep)
                def outputRailTableName = transform.results.outputTableName
                logger.info "Rail layer created"
                //Create vegetation layer
                tags = ['natural':['tree', 'wetland', 'grassland', 'tree_row', 'scrub', 'heath', 'sand', 'land', 'mud'],
                       'landuse':['farmland', 'forest', 'grass', 'meadow', 'orchard', 'vineyard', 'village_green'],
                       'landcover':[],
                        'vegetation':['grass'],'barrier':['hedge'],'fence_type':['hedge', 'wood'],
                                         'hedge':[],'wetland':[],'vineyard':[],
                                         'trees':[],'crop':[],'produce':[]]

                transform = OSMHelper.Transform.toPolygons()
                logger.info "Create the vegetation layer"
                assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)
                def outputVegetationTableName = transform.results.outputTableName
                logger.info "Vegetation layer created"

                //Create water layer
                tags = ['natural':['water','waterway','bay'],'water':[],'waterway':[]]
                transform = OSMHelper.Transform.toPolygons()
                logger.info "Create the water layer"
                tags = ['natural', 'water', 'waterway']
                assert transform(datasource: datasource, osmTablesPrefix: prefix, epsgCode: epsg, tags: tags)
                def outputhydroTableName = transform.results.outputTableName
                logger.info "Water layer created"

                [buildingTableName  : outputBuildingTableName, roadTableName: outputRoadTableName, railTableName: outputRailTableName,
                 vegetationTableName: outputVegetationTableName, hydroTableName: outputhydroTableName]
            }
        }
    })
}




