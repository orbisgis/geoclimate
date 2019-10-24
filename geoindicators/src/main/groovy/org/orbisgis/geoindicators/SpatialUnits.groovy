package org.orbisgis.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

import static org.h2gis.network.functions.ST_ConnectedComponents.getConnectedComponents


@BaseScript Geoindicators geoindicators

/**
 * This process is used to create the reference spatial units (RSU)
 *
 * @param inputTableName The input spatial table to be processed
 * @param inputZoneTableName The zone table to keep the RSU inside
 * Default value is empty so all RSU are kept.
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @return A database table name and the name of the column ID
 */
IProcess createRSU(){
    def final COLUMN_ID_NAME = "id_rsu"
    def final BASE_NAME = "rsu"
    return create({
        title "Create reference spatial units (RSU)"
        inputs inputTableName: String, inputZoneTableName: "", prefixName: "", datasource: JdbcDataSource
        outputs outputTableName: String, outputIdRsu: String
        run { inputTableName, inputZoneTableName, prefixName , datasource ->
            info "Creating the reference spatial units"
            // The name of the outputTableName is constructed
            def outputTableName = getOutputTableName(prefixName, BASE_NAME)

            def epsg = datasource.getSpatialTable(inputTableName).srid

            if(inputZoneTableName!=null && !inputZoneTableName.isEmpty()){

                datasource.execute """DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName as  select  EXPLOD_ID as $COLUMN_ID_NAME, st_setsrid(a.the_geom, $epsg) as the_geom
                     from st_explode ('(select st_polygonize(st_union(
                    st_precisionreducer(st_node(st_accum(st_force2d(the_geom))), 3))) as the_geom from $inputTableName)') as a,
                    $inputZoneTableName as b where a.the_geom && b.the_geom and st_intersects(ST_POINTONSURFACE(a.THE_GEOM), b.the_geom)"""
            }
            else{
            datasource.execute """DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName as  select  EXPLOD_ID as $COLUMN_ID_NAME, st_setsrid(st_force2D(the_geom), $epsg) as the_geom 
                     from st_explode ('(select st_polygonize(st_union(
                    st_precisionreducer(st_node(st_accum(st_force2d(the_geom))), 3))) as the_geom from $inputTableName)')"""
            }

            info "Reference spatial units table created"

            [outputTableName: outputTableName, outputIdRsu: COLUMN_ID_NAME]
        }
    })
}

/**
 * This process is used to prepare the input abstract model
 * in order to compute the reference spatial units (RSU)
 *
 * @param zoneTable The area of zone to be processed
 * @param roadTable The road table to be processed
 * @param railTable The rail table to be processed
 * @param vegetationTable The vegetation table to be processed
 * @param hydrographicTable The hydrographic table to be processed
 * @param surface_vegetation A double value to select the vegetation geometry areas.
 * Expressed in geometry unit of the vegetationTable
 * @param surface_hydro  A double value to select the hydrographic geometry areas.
 * Expressed in geometry unit of the vegetationTable
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 * @param outputTableName The name of the output table
 * @return A database table name.
 */
IProcess prepareRSUData() {
    def final BASE_NAME = "prepared_rsu_data"
    return create({
        title "Prepare the abstract model to build the RSU"
        inputs  zoneTable               : "",   roadTable           : "",                       railTable           : "",
                vegetationTable         : "",   hydrographicTable   : "",                       surface_vegetation  : 100000,
                surface_hydro           : 2500, prefixName          : "unified_abstract_model", datasource          : JdbcDataSource
        outputs outputTableName: String
        run { zoneTable, roadTable, railTable, vegetationTable, hydrographicTable, surface_vegetation,
              surface_hydrographic, prefixName, datasource ->
            info "Preparing the abstract model to build the RSU"

            // The name of the outputTableName is constructed
            def outputTableName = getOutputTableName(prefixName, BASE_NAME)

            // Create temporary table names (for tables that will be removed at the end of the IProcess)
            def vegetation_indice = vegetationTable + "_" + uuid
            def vegetation_unified = "vegetation_unified" + uuid
            def vegetation_tmp = "vegetation_tmp" + uuid
            def hydrographic_indice = hydrographicTable + uuid
            def hydrographic_unified = "hydrographic_unified" + uuid
            def hydrographic_tmp = "hydrographic_tmp" + uuid

            def queryCreateOutputTable =[:]

            def numberZone = datasource.firstRow("select count(*) as nb from $zoneTable").nb

            if (numberZone == 1) {
                def epsg = datasource.getSpatialTable(zoneTable).srid
                if(vegetationTable) {
                    info "Preparing vegetation..."

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_indice"
                    datasource.execute "CREATE TABLE $vegetation_indice(THE_GEOM geometry, ID serial," +
                            " CONTACT integer) AS (SELECT st_makevalid(THE_GEOM) as the_geom, null , 0 FROM ST_EXPLODE('" +
                            "(SELECT * FROM $vegetationTable)') " +
                            " where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false)"
                    datasource.execute "CREATE INDEX IF NOT EXISTS veg_indice_idx ON  $vegetation_indice(THE_GEOM) " +
                            "using rtree"
                    datasource.execute "UPDATE $vegetation_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)" +
                            " FROM $vegetation_indice a, $vegetation_indice b WHERE a.THE_GEOM && b.THE_GEOM AND " +
                            "ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)"

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_unified"
                    datasource.execute "CREATE TABLE $vegetation_unified AS " +
                            "(SELECT ST_SETSRID(the_geom, $epsg) as the_geom FROM ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM))" +
                            " AS THE_GEOM FROM $vegetation_indice WHERE CONTACT=1)') " +
                            "where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false AND " +
                            "st_area(the_geom)> $surface_vegetation) " +
                            "UNION ALL (SELECT THE_GEOM FROM $vegetation_indice WHERE contact=0 AND " +
                            "st_area(the_geom)> $surface_vegetation)"

                    datasource.execute "CREATE  INDEX IF NOT EXISTS veg_unified_idx ON  $vegetation_unified(THE_GEOM)" +
                            " using rtree"

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_tmp"
                    datasource.execute "CREATE TABLE $vegetation_tmp AS SELECT a.the_geom AS THE_GEOM FROM " +
                            "$vegetation_unified AS a, $zoneTable AS b WHERE a.the_geom && b.the_geom " +
                            "AND ST_INTERSECTS(a.the_geom, b.the_geom)"

                    queryCreateOutputTable+=[vegetation_tmp:"(SELECT st_force2d(THE_GEOM) as THE_GEOM FROM $vegetation_tmp)"]
                }

                if(hydrographicTable) {
                    //Extract water
                    info "Preparing hydrographic..."

                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_indice"
                    datasource.execute "CREATE TABLE $hydrographic_indice(THE_GEOM geometry, ID serial," +
                            " CONTACT integer) AS (SELECT st_makevalid(THE_GEOM) as the_geom, null , 0 FROM " +
                            "ST_EXPLODE('(SELECT * FROM $zoneTable)')" +
                            " where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false)"

                    datasource.execute "CREATE  INDEX IF NOT EXISTS hydro_indice_idx ON $hydrographic_indice(THE_GEOM)"


                    datasource.execute "UPDATE $hydrographic_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)" +
                            " FROM $hydrographic_indice a, $hydrographic_indice b WHERE a.THE_GEOM && b.THE_GEOM" +
                            " AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)"
                    datasource.execute "CREATE INDEX ON $hydrographic_indice(contact)"


                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_unified"
                    datasource.execute "CREATE TABLE $hydrographic_unified AS (SELECT ST_SETSRID(the_geom, $epsg) as the_geom FROM " +
                            "ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM" +
                            " $hydrographic_indice  WHERE CONTACT=1)') where st_dimension(the_geom)>0" +
                            " AND st_isempty(the_geom)=false AND st_area(the_geom)> $surface_hydrographic) " +
                            " UNION ALL (SELECT  the_geom FROM $hydrographic_indice WHERE contact=0 AND " +
                            " st_area(the_geom)> $surface_hydrographic)"


                    datasource.execute "CREATE INDEX IF NOT EXISTS hydro_unified_idx ON $hydrographic_unified(THE_GEOM)"


                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_tmp"
                    datasource.execute "CREATE TABLE $hydrographic_tmp AS SELECT a.the_geom" +
                            " AS THE_GEOM FROM $hydrographic_unified AS a, $zoneTable AS b " +
                            "WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)"

                    queryCreateOutputTable+=[hydrographic_tmp:"(SELECT st_force2d(THE_GEOM) as THE_GEOM FROM $hydrographic_tmp)"]
                }

                if(roadTable) {
                    info "Preparing road..."
                    queryCreateOutputTable+=[road_tmp:"(SELECT st_force2d(THE_GEOM) as THE_GEOM FROM $roadTable where zindex=0 or crossing = 'bridge')"]
                }

                if(railTable) {
                    info "Preparing rail..."
                    queryCreateOutputTable+=[rail_tmp:"(SELECT st_force2d(THE_GEOM) as THE_GEOM FROM $railTable where zindex=0 or crossing = 'bridge')"]
                }

                // The input table that contains the geometries to be transformed as RSU
                info "Grouping all tables..."
                if(queryCreateOutputTable){
                    datasource.execute """DROP TABLE if exists $outputTableName;
                CREATE TABLE $outputTableName(the_geom GEOMETRY) AS (SELECT st_setsrid(ST_ToMultiLine(THE_GEOM),$epsg) 
                 FROM $zoneTable) UNION ${queryCreateOutputTable.values().join(' union ')};
                   DROP TABLE IF EXISTS ${queryCreateOutputTable.keySet().join(' , ')}"""
                }
                else {
                    datasource.execute """DROP TABLE if exists $outputTableName;
                CREATE TABLE $outputTableName(the_geom GEOMETRY) AS (SELECT st_setsrid(ST_ToMultiLine(THE_GEOM),$epsg) 
                FROM $zoneTable);"""

                }

                datasource.execute "DROP TABLE IF EXISTS $vegetation_indice, $vegetation_unified, $vegetation_tmp, " +
                        "$hydrographic_indice, $hydrographic_unified, $hydrographic_tmp;"
                info "RSU created..."

            } else {
                error "Cannot compute the RSU. The input zone table must have one row."
                outputTableName=null
            }
            [outputTableName: outputTableName]
        }

    })
}


/**
 * This process is used to merge the geometries that touch each other
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputTableName The input table tos create the block (group of geometries)
 * @param distance A distance to group the geometries
 * @param prefixName A prefix used to name the output table
 * @param outputTableName The name of the output table
 * @return A database table name and the name of the column ID
 */
IProcess createBlocks(){
    def final BASE_NAME = "blocks"
    return create({
        title "Merge the geometries that touch each other"
        inputs inputTableName: String, distance : 0.0d, prefixName: "block", datasource: JdbcDataSource
        outputs outputTableName : String, outputIdBlock: String
        run { inputTableName,distance, prefixName, JdbcDataSource datasource ->
            info "Creating the blocks..."
            def columnIdName = "id_block"

            // The name of the outputTableName is constructed
            def outputTableName = getOutputTableName(prefixName, BASE_NAME)

            //Find all neighbors for each building
            info "Building index to perform the process..."
            datasource.getSpatialTable(inputTableName).the_geom.createSpatialIndex()
            datasource.getSpatialTable(inputTableName).id_build.createIndex()

            info "Building spatial clusters..."

            // Create temporary table names (for tables that will be removed at the end of the IProcess)
            String graphTable = "spatial_clusters"+ uuid
            String subGraphTableNodes =  graphTable+ "_NODE_CC"
            String subGraphTableEdges =  graphTable+ "_EDGE_CC"
            String subGraphBlocks =  "subgraphblocks"+ uuid

            datasource.execute """drop table if exists $graphTable; create table $graphTable 
             (EDGE_ID SERIAL, START_NODE INT, END_NODE INT) as select null, a.id_build as START_NODE, b.id_build as END_NODE 
            from  $inputTableName as a, $inputTableName as b 
            where a.id_build<>b.id_build AND a.the_geom && b.the_geom and  
            st_dwithin(b.the_geom,a.the_geom, $distance) ;"""

            datasource.execute"DROP TABLE IF EXISTS $subGraphTableEdges, $subGraphTableNodes;"

            getConnectedComponents(datasource.getConnection(),graphTable,"undirected")

            //Unify buildings that share a boundary
            info "Merging spatial clusters..."

            datasource.execute """
            CREATE INDEX ON $subGraphTableNodes(NODE_ID);
            DROP TABLE IF EXISTS $subGraphBlocks;
            CREATE TABLE $subGraphBlocks
            AS SELECT ST_UNION(ST_ACCUM(ST_MAKEVALID(A.THE_GEOM))) AS THE_GEOM
            FROM $inputTableName A, $subGraphTableNodes B
            WHERE A.id_build=B.NODE_ID GROUP BY B.CONNECTED_COMPONENT;"""

            //Create the blocks
            info "Creating the block table..."
            datasource.execute """DROP TABLE IF EXISTS $outputTableName; 
            CREATE TABLE $outputTableName ($columnIdName SERIAL, THE_GEOM GEOMETRY) 
            AS (SELECT null, THE_GEOM FROM $subGraphBlocks) UNION ALL (SELECT null, a.the_geom FROM $inputTableName a 
            LEFT JOIN $subGraphTableNodes b ON a.id_build = b.NODE_ID WHERE b.NODE_ID IS NULL);"""

            // Temporary tables are deleted
            datasource.execute "DROP TABLE IF EXISTS $graphTable, ${graphTable+"_EDGE_CC"}, " +
                    "$subGraphBlocks, ${subGraphBlocks+"_NODE_CC"};"

            info "The blocks have been created"
            [outputTableName: outputTableName, outputIdBlock: columnIdName]
        }
    })
}

/**
 * This process is used to link each object of a lower scale to an upper scale ID (i.e.: building to RSU). If a
 * lower scale object intersects several upper scale objects, the user may choose the number of upper scale object
 * kept by the lower scale object as relation (the upper scale objects are retained based on the
 * area shared between the objects (i.e. building and RSU - default 1). If nbRelations is null, all relations are
 * conserved.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputLowerScaleTableName The input table where are stored the lowerScale objects (i.e. buildings)
 * @param inputUpperScaleTableName The input table where are stored the upperScale objects (i.e. RSU)
 * @param idColumnUp The column name where is stored the ID of the upperScale objects (i.e. RSU)
 * @param prefixName A prefix used to name the output table
 * @param nbRelations Number of relations that the lower scale object can have with the upper scale object (i.e. if
 * nbRelations = 1 for buildings and RSU, the buildings can have only one RSU as relation)
 *
 * @return A database table name and the name of its ID field
 */
IProcess createScalesRelations(){
    def final GEOMETRIC_COLUMN_LOW = "the_geom"
    def final GEOMETRIC_COLUMN_UP = "the_geom"

    return create({
        title "Creating the Tables of relations between two scales"
        inputs inputLowerScaleTableName: String, inputUpperScaleTableName: String, idColumnUp: String,
                prefixName: String, nbRelations: 1, datasource: JdbcDataSource
        outputs outputTableName: String, outputIdColumnUp: String
        run { inputLowerScaleTableName, inputUpperScaleTableName, idColumnUp, prefixName, nbRelations, datasource ->

            info "Creating the Tables of relations between two scales"

            // The name of the outputTableName is constructed (the prefix name is not added since it is already contained
            // in the inputLowerScaleTableName object
            def outputTableName = inputLowerScaleTableName + "_corr"
            datasource.getSpatialTable(inputLowerScaleTableName).the_geom.createSpatialIndex()
            datasource.getSpatialTable(inputUpperScaleTableName).the_geom.createSpatialIndex()

            datasource.execute """DROP TABLE IF EXISTS $outputTableName;
                     CREATE TABLE $outputTableName AS SELECT a.*, (SELECT b.$idColumnUp 
                     FROM $inputUpperScaleTableName b WHERE a.$GEOMETRIC_COLUMN_LOW && b.$GEOMETRIC_COLUMN_UP AND 
                     ST_INTERSECTS(st_force2d(a.$GEOMETRIC_COLUMN_LOW), st_force2d(b.$GEOMETRIC_COLUMN_UP)) ORDER BY 
                     ST_AREA(ST_INTERSECTION(st_force2d(st_makevalid(a.$GEOMETRIC_COLUMN_LOW)), st_force2d(st_makevalid(b.$GEOMETRIC_COLUMN_UP)))) 
                     DESC LIMIT 1) AS $idColumnUp FROM $inputLowerScaleTableName a """

            info "The relations between scales have been created"

            [outputTableName: outputTableName, outputIdColumnUp: idColumnUp]
        }
    })
}