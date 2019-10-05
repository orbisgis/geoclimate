package org.orbisgis.osm

import groovy.transform.BaseScript
import org.h2gis.functions.spatial.crs.ST_Transform
import org.h2gis.utilities.SFSUtilities
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
            Geometry geom = OSMHelper.Utilities.getAreaFromPlace(placeName);

            if (geom == null) {
                logger.error("Cannot find an area from the place name ${placeName}")
                [buildingTableName    : null,
                 roadTableName        : null,
                 railTableName        : null,
                 vegetationTableName  : null,
                 hydroTableName       : null,
                 zoneTableName        : null,
                 zoneEnvelopeTableName: null,
                 epsg: epsg]
            } else {
                /**
                 * Extract the OSM file from the envelope of the geometry
                 */
                def geomAndEnv = buildGeometryAndZone(geom, epsg, distance, datasource)
                epsg = geomAndEnv.geom.getSRID()

                datasource.execute """create table ${outputZoneTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneTable} VALUES (ST_GEOMFROMTEXT('${
                    geomAndEnv.geom.toString()
                }', $epsg), '$placeName');"""

                datasource.execute """create table ${outputZoneEnvelopeTable} (the_geom GEOMETRY(POLYGON, $epsg), ID_ZONE VARCHAR);
            INSERT INTO ${outputZoneEnvelopeTable} VALUES (ST_GEOMFROMTEXT('${
                    ST_Transform.ST_Transform(datasource.getConnection(), geomAndEnv.filterArea, epsg).toString()
                }',$epsg), '$placeName');"""

                def query = OSMHelper.Utilities.buildOSMQuery(geomAndEnv.filterArea, [], NODE, WAY, RELATION)
                def extract = OSMHelper.Loader.extract()
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
 * This method is used to build a new geometry and its envelope according an EPSG code and a distance
 * The geometry and the envelope are set up in an UTM coordinate system when the epsg code is unknown.
 *
 * @param geom the input geometry
 * @param epsg the input epsg code
 * @param distance a value to expand the envelope of the geometry
 * @param datasource a connexion to the database
 *
 * @return a map with the input geometry and the envelope of the input geometry. Both are projected in a new reference
 * system depending on the epsg code.
 * Note that the envelope of the geometry can be expanded according the input distance value.
 */
def buildGeometryAndZone(Geometry geom, int epsg, int distance, def datasource) {
    GeometryFactory gf = new GeometryFactory()
    def con = datasource.getConnection();
    Polygon filterArea = null
    if(epsg==-1 || epsg==0){
        def interiorPoint = geom.getCentroid()
        epsg = SFSUtilities.getSRID(datasource.getConnection(), interiorPoint.y as float, interiorPoint.x as float)
        geom = ST_Transform.ST_Transform(con, geom, epsg);
        if(distance==0){
            Geometry tmpEnvGeom = gf.toGeometry(geom.getEnvelopeInternal())
            tmpEnvGeom.setSRID(epsg)
            filterArea = ST_Transform.ST_Transform(con, tmpEnvGeom, 4326)
        }
        else {
            def tmpEnvGeom = gf.toGeometry(geom.getEnvelopeInternal().expandBy(distance))
            tmpEnvGeom.setSRID(epsg)
            filterArea = ST_Transform.ST_Transform(con, tmpEnvGeom, 4326)
        }
    }
    else {
        geom = ST_Transform.ST_Transform(con, geom, epsg);
        if(distance==0){
            filterArea = gf.toGeometry(geom.getEnvelopeInternal())
            filterArea.setSRID(epsg)
        }
        else {
            filterArea = gf.toGeometry(geom.getEnvelopeInternal().expandBy(distance))
            filterArea.setSRID(epsg)
        }
    }
    return [geom :  geom, filterArea : filterArea]
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
                 vegetationTableName: outputVegetationTableName, hydroTableName: outputhydroTableName, epsg: epsg]
            }
        }
    })
}




