package org.orbisgis.orbisprocess.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory

import static org.h2gis.network.functions.ST_ConnectedComponents.getConnectedComponents

@BaseScript GroovyProcessFactory pf

/**
 * This process is used to create the reference spatial units (RSU)
 *
 * @param inputTableName The input spatial table to be processed
 * @param inputZoneTableName The zone table to keep the RSU inside
 * Default value is empty so all RSU are kept.
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 * @param area RSU less or equals than area are removed
 * @return A database table name and the name of the column ID
 */
create {
    title "Create reference spatial units (RSU)"
    id "createRSU"
    inputs inputTableName: String, inputZoneTableName: "", prefixName: "", datasource: JdbcDataSource, area:0.1d
    outputs outputTableName: String, outputIdRsu: String
    run { inputTableName, inputZoneTableName, prefixName , datasource, area ->
        
        def COLUMN_ID_NAME = "id_rsu"
        def BASE_NAME = "rsu"
        
        info "Creating the reference spatial units"
        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        def epsg = datasource."$inputTableName".srid

        if(area<=0){
            error "The area value to filter the RSU must be greater to 0"
            return null
        }

        if(inputZoneTableName){
            datasource."$inputTableName".the_geom.createSpatialIndex()
            datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName AS 
                        SELECT EXPLOD_ID AS $COLUMN_ID_NAME, ST_SETSRID(a.the_geom, $epsg) AS the_geom
                        FROM ST_EXPLODE('(
                                SELECT ST_POLYGONIZE(ST_UNION(ST_PRECISIONREDUCER(ST_NODE(ST_ACCUM(ST_FORCE2D(the_geom))), 3))) AS the_geom 
                                FROM $inputTableName)') AS a,
                            $inputZoneTableName AS b
                        WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(ST_POINTONSURFACE(a.THE_GEOM), b.the_geom) and st_area(a.the_geom) > $area
            """
        }
        else{
            datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName AS 
                        SELECT EXPLOD_ID AS $COLUMN_ID_NAME, ST_SETSRID(ST_FORCE2D(the_geom), $epsg) AS the_geom 
                        FROM ST_EXPLODE('(
                                SELECT ST_POLYGONIZE(ST_UNION(ST_PRECISIONREDUCER(ST_NODE(ST_ACCUM(ST_FORCE2D(the_geom))), 3))) AS the_geom 
                                FROM $inputTableName)') where st_area(the_geom) > $area"""
        }

        info "Reference spatial units table created"

        [outputTableName: outputTableName, outputIdRsu: COLUMN_ID_NAME]
    }
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
create {
    title "Prepare the abstract model to build the RSU"
    id "prepareRSUData"
    inputs  zoneTable               : "",   roadTable           : "",                       railTable           : "",
            vegetationTable         : "",   hydrographicTable   : "",                       surface_vegetation  : 100000,
            surface_hydro           : 2500, prefixName          : "unified_abstract_model", datasource          : JdbcDataSource
    outputs outputTableName: String
    run { zoneTable, roadTable, railTable, vegetationTable, hydrographicTable, surface_vegetation,
          surface_hydrographic, prefixName, datasource ->

        def BASE_NAME = "prepared_rsu_data"
        
        info "Preparing the abstract model to build the RSU"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        // Create temporary table names (for tables that will be removed at the end of the IProcess)
        def vegetation_indice
        def vegetation_unified
        def vegetation_tmp
        def hydrographic_indice
        def hydrographic_unified
        def hydrographic_tmp

        def queryCreateOutputTable =[:]
        def dropTableList = []

        def numberZone = datasource.firstRow("SELECT COUNT(*) AS nb FROM $zoneTable").nb

        if (numberZone == 1) {
            def epsg = datasource."$zoneTable".srid
            if(vegetationTable && datasource.hasTable(vegetationTable)) {
                if (datasource."$vegetationTable") {
                    info "Preparing vegetation..."
                    vegetation_indice = postfix vegetationTable
                    vegetation_unified = postfix "vegetation_unified"
                    vegetation_tmp = postfix "vegetation_tmp"

                    datasource "DROP TABLE IF EXISTS $vegetation_indice"
                    datasource "CREATE TABLE $vegetation_indice(THE_GEOM geometry, ID serial," +
                            " CONTACT integer) AS (SELECT ST_MAKEVALID(THE_GEOM) AS the_geom, NULL , 0 FROM ST_EXPLODE('" +
                            "(SELECT * FROM $vegetationTable)') " +
                            " WHERE ST_DIMENSION(the_geom)>0 AND ST_ISEMPTY(the_geom)=FALSE)"
                    datasource "CREATE INDEX IF NOT EXISTS veg_indice_idx ON $vegetation_indice USING RTREE(THE_GEOM)"
                    datasource "UPDATE $vegetation_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)" +
                            " FROM $vegetation_indice a, $vegetation_indice b WHERE a.THE_GEOM && b.THE_GEOM AND " +
                            "ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)"

                    datasource "DROP TABLE IF EXISTS $vegetation_unified"
                    datasource "CREATE TABLE $vegetation_unified AS " +
                            "(SELECT ST_SETSRID(the_geom, $epsg) AS the_geom FROM ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM))" +
                            " AS THE_GEOM FROM $vegetation_indice WHERE CONTACT=1)') " +
                            "WHERE ST_DIMENSION(the_geom)>0 AND ST_ISEMPTY(the_geom)=FALSE AND " +
                            "ST_AREA(the_geom)> $surface_vegetation) " +
                            "UNION ALL (SELECT THE_GEOM FROM $vegetation_indice WHERE contact=0 AND " +
                            "ST_AREA(the_geom)> $surface_vegetation)"

                    datasource "CREATE INDEX IF NOT EXISTS veg_unified_idx ON $vegetation_unified USING RTREE(THE_GEOM)"

                    datasource "DROP TABLE IF EXISTS $vegetation_tmp"
                    datasource "CREATE TABLE $vegetation_tmp AS SELECT a.the_geom AS THE_GEOM FROM " +
                            "$vegetation_unified AS a, $zoneTable AS b WHERE a.the_geom && b.the_geom " +
                            "AND ST_INTERSECTS(a.the_geom, b.the_geom)"

                    queryCreateOutputTable += [vegetation_tmp: "(SELECT ST_FORCE2D(THE_GEOM) AS THE_GEOM FROM $vegetation_tmp)"]
                    dropTableList.addAll([vegetation_indice,
                                          vegetation_unified ,
                                          vegetation_tmp ])
                }
            }

            if(hydrographicTable && datasource.hasTable(hydrographicTable)) {
                if (datasource."$hydrographicTable") {
                    //Extract water
                    info "Preparing hydrographic..."
                    hydrographic_indice = postfix hydrographicTable
                    hydrographic_unified = postfix "hydrographic_unified"
                    hydrographic_tmp = postfix "hydrographic_tmp"

                    datasource "DROP TABLE IF EXISTS $hydrographic_indice"
                    datasource "CREATE TABLE $hydrographic_indice(THE_GEOM geometry, ID serial," +
                            " CONTACT integer) AS (SELECT st_makevalid(THE_GEOM) AS the_geom, NULL , 0 FROM " +
                            "ST_EXPLODE('(SELECT * FROM $hydrographicTable)')" +
                            " WHERE ST_DIMENSION(the_geom)>0 AND ST_ISEMPTY(the_geom)=false)"

                    datasource "CREATE INDEX IF NOT EXISTS hydro_indice_idx ON $hydrographic_indice USING RTREE(THE_GEOM)"


                    datasource "UPDATE $hydrographic_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)" +
                            " FROM $hydrographic_indice a, $hydrographic_indice b WHERE a.THE_GEOM && b.THE_GEOM" +
                            " AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)"
                    datasource "CREATE INDEX ON $hydrographic_indice USING BTREE(contact)"


                    datasource "DROP TABLE IF EXISTS $hydrographic_unified"
                    datasource "CREATE TABLE $hydrographic_unified AS (SELECT ST_SETSRID(the_geom, $epsg) as the_geom FROM " +
                            "ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM" +
                            " $hydrographic_indice  WHERE CONTACT=1)') where st_dimension(the_geom)>0" +
                            " AND st_isempty(the_geom)=false AND st_area(the_geom)> $surface_hydrographic) " +
                            " UNION ALL (SELECT  the_geom FROM $hydrographic_indice WHERE contact=0 AND " +
                            " st_area(the_geom)> $surface_hydrographic)"


                    datasource "CREATE INDEX IF NOT EXISTS hydro_unified_idx ON $hydrographic_unified USING RTREE(THE_GEOM)"


                    datasource "DROP TABLE IF EXISTS $hydrographic_tmp"
                    datasource "CREATE TABLE $hydrographic_tmp AS SELECT a.the_geom" +
                            " AS THE_GEOM FROM $hydrographic_unified AS a, $zoneTable AS b " +
                            "WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)"

                    queryCreateOutputTable += [hydrographic_tmp: "(SELECT st_force2d(THE_GEOM) as THE_GEOM FROM $hydrographic_tmp)"]
                    dropTableList.addAll([hydrographic_indice,
                                          hydrographic_unified ,
                                          hydrographic_tmp ])
                }
            }

            if(roadTable && datasource.hasTable(roadTable)) {
                if(datasource."$roadTable") {
                    info "Preparing road..."
                    queryCreateOutputTable += [road_tmp: "(SELECT ST_FORCE2D(THE_GEOM) AS THE_GEOM FROM $roadTable where zindex=0 or crossing = 'bridge')"]
                }
            }

            if(railTable && datasource.hasTable(railTable) && !datasource."$railTable".isEmpty()) {
                if(datasource."$railTable") {
                    info "Preparing rail..."
                    queryCreateOutputTable += [rail_tmp: "(SELECT ST_FORCE2D(THE_GEOM) AS THE_GEOM FROM $railTable where zindex=0 or crossing = 'bridge')"]
                }
            }

            // The input table that contains the geometries to be transformed as RSU
            info "Grouping all tables..."
            if(queryCreateOutputTable){
                datasource """
                        DROP TABLE if exists $outputTableName;
                        CREATE TABLE $outputTableName(the_geom GEOMETRY) AS 
                            (
                                SELECT st_setsrid(ST_ToMultiLine(THE_GEOM),$epsg) 
                                FROM $zoneTable) 
                            UNION ${queryCreateOutputTable.values().join(' union ')};
                        DROP TABLE IF EXISTS ${queryCreateOutputTable.keySet().join(' , ')}
                """
            }
            else {
                datasource """DROP TABLE if exists $outputTableName;
            CREATE TABLE $outputTableName(the_geom GEOMETRY) AS (SELECT st_setsrid(ST_ToMultiLine(THE_GEOM),$epsg) 
            FROM $zoneTable);"""

            }
            if(dropTableList){
            datasource "DROP TABLE IF EXISTS ${dropTableList.join(',')};"
            }
            info "RSU created..."

        } else {
            error "Cannot compute the RSU. The input zone table must have one row."
            outputTableName = null
        }
        [outputTableName: outputTableName]
    }
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
create {
    title "Merge the geometries that touch each other"
    id "createBlocks"
    inputs inputTableName: String, distance : 0.0d, prefixName: "block", datasource: JdbcDataSource
    outputs outputTableName : String, outputIdBlock: String
    run { inputTableName,distance, prefixName, JdbcDataSource datasource ->

        def BASE_NAME = "blocks"
        
        info "Creating the blocks..."
        
        def columnIdName = "id_block"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        //Find all neighbors for each building
        info "Building index to perform the process..."
        datasource."$inputTableName".the_geom.createSpatialIndex()
        datasource."$inputTableName".id_build.createIndex()

        info "Building spatial clusters..."

        // Create temporary table names (for tables that will be removed at the end of the IProcess)
         def graphTable = postfix "spatial_clusters"
         def subGraphTableNodes =  postfix graphTable, "NODE_CC"
         def subGraphTableEdges =  postfix graphTable, "EDGE_CC"
         def subGraphBlocks =  postfix "subgraphblocks"

        datasource """
                DROP TABLE IF EXISTS $graphTable; 
                CREATE TABLE $graphTable (EDGE_ID SERIAL, START_NODE INT, END_NODE INT) AS 
                    SELECT null, a.id_build as START_NODE, b.id_build AS END_NODE 
                    FROM $inputTableName AS a, $inputTableName AS b 
                    WHERE a.id_build<>b.id_build AND a.the_geom && b.the_geom 
                    AND ST_DWITHIN(b.the_geom,a.the_geom, $distance);
        """

        datasource"DROP TABLE IF EXISTS $subGraphTableEdges, $subGraphTableNodes;"

        getConnectedComponents(datasource.getConnection(), graphTable,"undirected")

        //Unify buildings that share a boundary
        info "Merging spatial clusters..."

        if(distance>0){
            datasource """
                    CREATE INDEX ON $subGraphTableNodes USING BTREE(NODE_ID);
                    DROP TABLE IF EXISTS $subGraphBlocks;
                    CREATE TABLE $subGraphBlocks AS
                        SELECT ST_UNION(ST_ACCUM(ST_buffer(A.THE_GEOM, $distance))) AS THE_GEOM
                        FROM $inputTableName A, $subGraphTableNodes B
                        WHERE A.id_build=B.NODE_ID GROUP BY B.CONNECTED_COMPONENT;
            """
        }
        else{
            datasource """
        CREATE INDEX ON $subGraphTableNodes USING BTREE(NODE_ID);
        DROP TABLE IF EXISTS $subGraphBlocks;
        CREATE TABLE $subGraphBlocks
        AS SELECT ST_UNION(ST_ACCUM(ST_MAKEVALID(A.THE_GEOM))) AS THE_GEOM
        FROM $inputTableName A, $subGraphTableNodes B
        WHERE A.id_build=B.NODE_ID GROUP BY B.CONNECTED_COMPONENT;"""
        }


        //Create the blocks
        info "Creating the block table..."
        datasource """DROP TABLE IF EXISTS $outputTableName; 
        CREATE TABLE $outputTableName ($columnIdName SERIAL, THE_GEOM GEOMETRY) 
        AS (SELECT null, THE_GEOM FROM $subGraphBlocks) UNION ALL (SELECT null, a.the_geom FROM $inputTableName a 
        LEFT JOIN $subGraphTableNodes b ON a.id_build = b.NODE_ID WHERE b.NODE_ID IS NULL);"""

        // Temporary tables are deleted
        datasource "DROP TABLE IF EXISTS $graphTable, ${graphTable+"_EDGE_CC"}, " +
                "$subGraphBlocks, ${subGraphBlocks+"_NODE_CC"};"

        info "The blocks have been created"
        [outputTableName: outputTableName, outputIdBlock: columnIdName]
    }
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

create {
    title "Creating the Tables of relations between two scales"
    id "createScalesRelations"
    inputs inputLowerScaleTableName: String, inputUpperScaleTableName: String, idColumnUp: String,
            prefixName: String, nbRelations: 1, datasource: JdbcDataSource
    outputs outputTableName: String, outputIdColumnUp: String
    run { inputLowerScaleTableName, inputUpperScaleTableName, idColumnUp, prefixName, nbRelations, datasource ->

        def GEOMETRIC_COLUMN_LOW = "the_geom"
        def GEOMETRIC_COLUMN_UP = "the_geom"

        info "Creating the Tables of relations between the twos scales :  $inputLowerScaleTableName and $inputUpperScaleTableName"

        // The name of the outputTableName is constructed (the prefix name is not added since it is already contained
        // in the inputLowerScaleTableName object
        def outputTableName = postfix inputLowerScaleTableName, "corr"
        datasource."$inputLowerScaleTableName".the_geom.createSpatialIndex()
        datasource."$inputUpperScaleTableName".the_geom.createSpatialIndex()

        datasource """DROP TABLE IF EXISTS $outputTableName;
                 CREATE TABLE $outputTableName AS SELECT a.*, (SELECT b.$idColumnUp 
                 FROM $inputUpperScaleTableName b WHERE a.$GEOMETRIC_COLUMN_LOW && b.$GEOMETRIC_COLUMN_UP AND 
                 ST_INTERSECTS(st_force2d(a.$GEOMETRIC_COLUMN_LOW), st_force2d(b.$GEOMETRIC_COLUMN_UP)) ORDER BY 
                 ST_AREA(ST_INTERSECTION(st_force2d(st_makevalid(a.$GEOMETRIC_COLUMN_LOW)), st_force2d(st_makevalid(b.$GEOMETRIC_COLUMN_UP)))) 
                 DESC LIMIT 1) AS $idColumnUp FROM $inputLowerScaleTableName a """

        info "The relations between scales have been created between :  $inputLowerScaleTableName and $inputUpperScaleTableName"

        [outputTableName: outputTableName, outputIdColumnUp: idColumnUp]
    }
}
