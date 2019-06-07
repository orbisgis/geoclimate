package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate

/**
 * This process is used to create the reference spatial units (RSU)
 *
 * @param inputTableName The input spatial table to be processed
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @return A database table name and the name of the column ID
 */
static IProcess createRSU(){
    def final COLUMN_ID_NAME = "id_rsu"
    def final BASE_NAME = "created_rsu"

    return processFactory.create("Create reference spatial units (RSU)",
            [inputTableName: String, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String, outputIdRsu: String],
            { inputTableName, prefixName='rsu', datasource ->
                logger.info("Creating the reference spatial units")

                // The name of the outputTableName is constructed
                String outputTableName = prefixName + "_" + BASE_NAME

                datasource.execute "DROP TABLE IF EXISTS $outputTableName"
                datasource.execute "CREATE TABLE $outputTableName as  select  EXPLOD_ID as $COLUMN_ID_NAME, the_geom" +
                        " from st_explode ('(select st_polygonize(st_union(" +
                "st_precisionreducer(st_node(st_accum(the_geom)), 3))) as the_geom from $inputTableName)')"

                logger.info("Reference spatial units table created")

                [outputTableName: outputTableName, outputIdRsu: COLUMN_ID_NAME]
            }
    )
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
static IProcess prepareRSUData(){
    def final BASE_NAME = "prepared_rsu_data"

    return processFactory.create("Prepare the abstract model to build the RSU",
            [zoneTable: String, roadTable: String,  railTable: String, vegetationTable : String,
                    hydrographicTable :String, surface_vegetation : 100000, surface_hydro : 2500,
                    prefixName: "unified_abstract_model", datasource: JdbcDataSource],
            [outputTableName : String],
            { zoneTable ,roadTable, railTable, vegetationTable , hydrographicTable, surface_vegetation,
                    surface_hydrographic ,prefixName, datasource ->

                logger.info("Creating the reference spatial units")

                // The name of the outputTableName is constructed
                def outputTableName = prefixName + "_" + BASE_NAME

                def numberZone = datasource.firstRow("select count(*) as nb from $zoneTable").nb

                if(numberZone==1){
                    logger.info("Preparing vegetation...")

                    def  vegetation_indice = vegetationTable+"_" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_indice"
                    datasource.execute "CREATE TABLE $vegetation_indice(THE_GEOM geometry, ID serial,"+
                            " CONTACT integer) AS (SELECT THE_GEOM, null , 0 FROM ST_EXPLODE('" +
                            "(SELECT * FROM $vegetationTable)') " +
                            " where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false)"
                    datasource.execute "CREATE INDEX IF NOT EXISTS veg_indice_idx ON  $vegetation_indice(THE_GEOM) " +
                            "using rtree"
                    datasource.execute "UPDATE $vegetation_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)" +
                            " FROM $vegetation_indice a, $vegetation_indice b WHERE a.THE_GEOM && b.THE_GEOM AND " +
                            "ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)"

                    def vegetation_unified ="vegetation_unified" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_unified"
                    datasource.execute "CREATE TABLE $vegetation_unified AS " +
                            "(SELECT the_geom FROM ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM))"+
                            " AS THE_GEOM FROM $vegetation_indice WHERE CONTACT=1)') " +
                            "where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false AND " +
                            "st_area(the_geom)> $surface_vegetation) " +
                            "UNION ALL (SELECT THE_GEOM FROM $vegetation_indice WHERE contact=0 AND " +
                            "st_area(the_geom)> $surface_vegetation)"

                    datasource.execute "CREATE  INDEX IF NOT EXISTS veg_unified_idx ON  $vegetation_unified(THE_GEOM)" +
                            " using rtree"

                    def vegetation_tmp ="vegetation_tmp" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_tmp"
                    datasource.execute "CREATE TABLE $vegetation_tmp AS SELECT a.the_geom AS THE_GEOM FROM " +
                            "$vegetation_unified AS a, $zoneTable AS b WHERE a.the_geom && b.the_geom " +
                            "AND ST_INTERSECTS(a.the_geom, b.the_geom)"

                    //Extract water
                    logger.info("Preparing hydrographic...")
                    String hydrographic_indice = hydrographicTable + uuid()
                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_indice"
                    datasource.execute "CREATE TABLE $hydrographic_indice(THE_GEOM geometry, ID serial," +
                            " CONTACT integer) AS (SELECT THE_GEOM, null , 0 FROM " +
                            "ST_EXPLODE('(SELECT * FROM $zoneTable)')" +
                            " where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false)"

                    datasource.execute "CREATE  INDEX IF NOT EXISTS hydro_indice_idx ON $hydrographic_indice(THE_GEOM)"


                    datasource.execute "UPDATE $hydrographic_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID)" +
                            " FROM $hydrographic_indice a, $hydrographic_indice b WHERE a.THE_GEOM && b.THE_GEOM"+
                            " AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)"
                    datasource.execute "CREATE INDEX ON $hydrographic_indice(contact)"

                    def hydrographic_unified ="hydrographic_unified" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_unified"
                    datasource.execute "CREATE TABLE $hydrographic_unified AS (SELECT THE_GEOM FROM " +
                            "ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM" +
                            " $hydrographic_indice  WHERE CONTACT=1)') where st_dimension(the_geom)>0" +
                            " AND st_isempty(the_geom)=false AND st_area(the_geom)> $surface_hydrographic) " +
                            " UNION ALL (SELECT  the_geom FROM $hydrographic_indice WHERE contact=0 AND " +
                            " st_area(the_geom)> $surface_hydrographic)"


                    datasource.execute "CREATE INDEX IF NOT EXISTS hydro_unified_idx ON $hydrographic_unified(THE_GEOM)"

                    def hydrographic_tmp ="hydrographic_tmp" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_tmp"
                    datasource.execute "CREATE TABLE $hydrographic_tmp AS SELECT a.the_geom" +
                            " AS THE_GEOM FROM $hydrographic_unified AS a, $zoneTable AS b " +
                            "WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)"


                    logger.info("Preparing road...")

                    def road_tmp ="road_tmp" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $road_tmp"
                    datasource.execute "CREATE TABLE $road_tmp AS SELECT the_geom AS THE_GEOM FROM $roadTable " +
                            "where zindex=0"


                    logger.info("Preparing rail...")

                    def rail_tmp ="rail_tmp" + uuid()

                    datasource.execute "DROP TABLE IF EXISTS $rail_tmp"
                    datasource.execute "CREATE TABLE $rail_tmp AS SELECT the_geom AS THE_GEOM FROM $railTable " +
                            "where zindex=0"

                    // The input table that contains the geometries to be transformed as RSU
                    logger.info("Grouping all tables...")
                    datasource.execute "DROP TABLE if exists $outputTableName"
                    datasource.execute "CREATE TABLE $outputTableName AS (SELECT THE_GEOM FROM $road_tmp)" +
                            " UNION (SELECT THE_GEOM FROM $rail_tmp) " +
                            "UNION (SELECT THE_GEOM FROM $hydrographic_tmp)" +
                            " UNION  (SELECT THE_GEOM FROM $vegetation_tmp)"

                }
                else {
                    logger.error("Cannot compute the RSU. The input zone table must have one row.")
                }

                [outputTableName: outputTableName]
            }

    )
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
static IProcess createBlocks(){
    return processFactory.create("Merge the geometries that touch each other",
            [inputTableName: String, distance : double, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String, outputIdBlock: String],
            { inputTableName,distance =0.0, prefixName="block", datasource ->
                logger.info("Merging the geometries...")

                def columnIdName = "id_block"

                // The name of the outputTableName is constructed
                String baseName = "created_blocks"
                String outputTableName = prefixName + "_" + baseName

                datasource.execute "DROP TABLE IF EXISTS $outputTableName"
                datasource.execute "CREATE TABLE $outputTableName as  select  EXPLOD_ID as $columnIdName, the_geom "+
                        "from st_explode ('(select ST_UNION(ST_ACCUM(ST_BUFFER(THE_GEOM,$distance))) as the_geom"+
                        " from $inputTableName)')"
                logger.info("The geometries have been merged")
                [outputTableName: outputTableName, outputIdBlock: columnIdName]
            }
    )
}

/**
 * This process is used to link each object of a lower scale to an upper scale ID (i.e.: building to RSU). If a
 * lower scale object intersects two upper scale objects, it is attributed to the one where is located the
 * major part of its surface.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputLowerScaleTableName The input table where are stored the lowerScale objects (i.e. buildings)
 * @param inputUpperScaleTableName The input table where are stored the upperScale objects (i.e. RSU)
 * @param idColumnUp The column name where is stored the ID of the upperScale objects (i.e. RSU)
 * @param prefixName A prefix used to name the output table
 *
 * @return A database table name and the name of its ID field
 */
static IProcess createScalesRelations(){
    def final GEOMETRIC_COLUMN_LOW = "the_geom"
    def final GEOMETRIC_COLUMN_UP = "the_geom"

    return processFactory.create("Creating the Tables of relations between two scales",
            [inputLowerScaleTableName: String, inputUpperScaleTableName : String, idColumnUp: String,
                    prefixName: String, datasource: JdbcDataSource],
            [outputTableName: String, outputIdColumnUp: String],
            { inputLowerScaleTableName, inputUpperScaleTableName, idColumnUp, prefixName, datasource ->

                logger.info("Creating the Tables of relations between two scales")

                // The name of the outputTableName is constructed
                def outputTableName =  prefixName+"_"+inputLowerScaleTableName+"_corr"

                datasource.execute "DROP TABLE IF EXISTS $outputTableName; CREATE INDEX IF NOT EXISTS ids_l "+
                        "ON $inputLowerScaleTableName($GEOMETRIC_COLUMN_LOW) USING RTREE; CREATE INDEX IF NOT EXISTS "+
                        "ids_u ON $inputUpperScaleTableName($GEOMETRIC_COLUMN_UP) USING RTREE"
                datasource.execute"CREATE TABLE $outputTableName AS SELECT a.*, (SELECT b.$idColumnUp "+
                        "FROM $inputUpperScaleTableName b WHERE a.$GEOMETRIC_COLUMN_LOW && b.$GEOMETRIC_COLUMN_UP AND "+
                        "ST_INTERSECTS(a.$GEOMETRIC_COLUMN_LOW, b.$GEOMETRIC_COLUMN_UP) ORDER BY "+
                        "ST_AREA(ST_INTERSECTION(a.$GEOMETRIC_COLUMN_LOW, b.$GEOMETRIC_COLUMN_UP)) " +
                        "DESC LIMIT 1) AS $idColumnUp FROM $inputLowerScaleTableName a"
                logger.info("The relations between scales have been created")

                [outputTableName: outputTableName, outputIdColumnUp: idColumnUp]
            }
    )
}