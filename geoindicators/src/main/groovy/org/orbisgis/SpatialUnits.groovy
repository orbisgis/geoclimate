package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate

/**
 * This process is used to create the reference spatial units (RSU)
 *
 * @param inputTableName the input spatial table to be processed
 * @param prefixName a prefix used to name the output table
 * @param datasource a connection to a database
 * @param outputTableName the name of the output table
 * @return A database table name.
 */
static IProcess createRSU(){
    return processFactory.create("Create reference spatial units (RSU)",
            [inputTableName: String, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputTableName, prefixName='rsu', datasource ->
                logger.info("Creating the reference spatial units")
                String outputTableName =  prefixName+"_"+UUID.randomUUID().toString().replaceAll("-","_")
                datasource.execute "DROP TABLE IF EXISTS $outputTableName".toString()
                datasource.execute "CREATE TABLE $outputTableName as  select  EXPLOD_ID as id_rsu, the_geom from st_explode ('(select st_polygonize(st_union(st_precisionreducer(st_node(st_accum(the_geom)), 3))) as the_geom from $inputTableName)')".toString()
                logger.info("Reference spatial units table created")
                [outputTableName: outputTableName]
        }
    )
}

/**
 * This process is used to prepare the input abstract model
 * in order to compute the reference spatial units (RSU)
 *
 * @param zoneTable the area of zone to be processed
 * @param roadTable the road table to be processed
 * @param railTable the rail table to be processed
 * @param vegetationTable the vegetation table to be processed
 * @param hydrographicTable the hydrographic table to be processed
 * @param surface_vegetation a double value to select the vegetation geometry areas.
 * Expressed in geometry unit of the vegetationTable
 * @param surface_hydro  double value to select the hydrographic geometry areas.
 * Expressed in geometry unit of the vegetationTable
 * @param prefixName a prefix used to name the output table
 * @param datasource a connection to a database
 * @param outputTableName the name of the output table
 * @return A database table name.
 *
 * @return A database table name.
 */
static IProcess prepareRSUData(){
    return processFactory.create("Prepare the abstract model to build the RSU",
            [zoneTable: String, roadTable: String,  railTable: String, vegetationTable : String, hydrographicTable :String,
             surface_vegetation : double, surface_hydro : double, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { zoneTable ,roadTable, railTable, vegetationTable , hydrographicTable, surface_vegetation =100000, surface_hydrographic=2500 ,prefixName="unified_abstract_model", datasource ->
                logger.info("Creating the reference spatial units")
                String uuid_tmp =  UUID.randomUUID().toString().replaceAll("-", "_")+"_"
                String outputTableName =  prefixName+"_"+uuid_tmp
                def numberZone = datasource.firstRow("select count(*) as nb from $zoneTable".toString()).nb

                if(numberZone==1){
                    logger.info("Preparing vegetation...")
                    String  vegetation_indice = vegetationTable+"_"+uuid_tmp
                    datasource.execute "DROP TABLE IF EXISTS $vegetation_indice".toString()
                    datasource.execute "CREATE TABLE $vegetation_indice(THE_GEOM geometry, ID serial, CONTACT integer) AS (SELECT THE_GEOM, null , 0 FROM ST_EXPLODE('(SELECT * FROM $vegetationTable)') " .toString()+
                            "where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false)".toString()
                    datasource.execute "CREATE INDEX IF NOT EXISTS veg_indice_idx ON  $vegetation_indice(THE_GEOM) using rtree".toString()
                    datasource.execute " UPDATE $vegetation_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID) FROM $vegetation_indice a, $vegetation_indice b WHERE a.THE_GEOM && b.THE_GEOM AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)".toString()

                    String vegetation_unified ="vegetation_unified"+uuid_tmp

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_unified".toString()
                    datasource.execute "CREATE TABLE $vegetation_unified AS ".toString() +
                            "(SELECT the_geom FROM ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM $vegetation_indice WHERE CONTACT=1)') ".toString() +
                            "where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false AND st_area(the_geom)> $surface_vegetation) ".toString() +
                            "UNION ALL (SELECT THE_GEOM FROM $vegetation_indice WHERE contact=0 AND st_area(the_geom)> $surface_vegetation)".toString()

                    datasource.execute "CREATE  INDEX IF NOT EXISTS veg_unified_idx ON  $vegetation_unified(THE_GEOM) using rtree".toString()

                    String vegetation_tmp ="vegetation_tmp"+uuid_tmp

                    datasource.execute "DROP TABLE IF EXISTS $vegetation_tmp".toString()
                    datasource.execute "CREATE TABLE $vegetation_tmp AS SELECT a.the_geom AS THE_GEOM FROM $vegetation_unified AS a, $zoneTable AS b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)".toString()

                    //Extract water
                    logger.info("Preparing hydrographic...")
                    String hydrographic_indice = hydrographicTable+uuid_tmp
                    datasource.execute " DROP TABLE IF EXISTS $hydrographic_indice".toString()
                    datasource.execute " CREATE TABLE $hydrographic_indice(THE_GEOM geometry, ID serial, CONTACT integer) AS (SELECT THE_GEOM, null , 0 FROM ST_EXPLODE('(SELECT * FROM $zoneTable)') where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false)".toString()

                    datasource.execute "CREATE  INDEX IF NOT EXISTS hydro_indice_idx ON $hydrographic_indice(THE_GEOM)".toString()


                    datasource.execute "UPDATE $hydrographic_indice SET CONTACT=1 WHERE ID IN(SELECT DISTINCT(a.ID) FROM $hydrographic_indice a, $hydrographic_indice b WHERE a.THE_GEOM && b.THE_GEOM AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) AND a.ID<>b.ID)".toString()
                    datasource.execute "CREATE INDEX ON $hydrographic_indice(contact)".toString()

                    String hydrographic_unified ="hydrographic_unified"+uuid_tmp
                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_unified".toString()
                    datasource.execute "CREATE TABLE $hydrographic_unified AS (SELECT THE_GEOM FROM ST_EXPLODE('(SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM FROM $hydrographic_indice  WHERE CONTACT=1)') where st_dimension(the_geom)>0 AND st_isempty(the_geom)=false AND st_area(the_geom)> $surface_hydrographic) UNION ALL (SELECT  the_geom FROM $hydrographic_indice WHERE contact=0 AND st_area(the_geom)> $surface_hydrographic)".toString()


                    datasource.execute "CREATE INDEX IF NOT EXISTS hydro_unified_idx ON $hydrographic_unified(THE_GEOM)".toString()

                    String hydrographic_tmp ="hydrographic_tmp"+uuid_tmp

                    datasource.execute "DROP TABLE IF EXISTS $hydrographic_tmp".toString()
                    datasource.execute "CREATE TABLE $hydrographic_tmp AS SELECT a.the_geom AS THE_GEOM FROM $hydrographic_unified AS a, $zoneTable AS b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)".toString()


                    logger.info("Preparing road...")

                    String road_tmp ="road_tmp"+uuid_tmp

                    datasource.execute "DROP TABLE IF EXISTS $road_tmp".toString()
                    datasource.execute "CREATE TABLE $road_tmp AS SELECT the_geom AS THE_GEOM FROM $roadTable where zindex=0".toString()


                    logger.info("Preparing rail...")

                    String rail_tmp ="rail_tmp"+uuid_tmp

                    datasource.execute "DROP TABLE IF EXISTS $rail_tmp".toString()
                    datasource.execute "CREATE TABLE $rail_tmp AS SELECT the_geom AS THE_GEOM FROM $railTable where zindex=0".toString()

                    // The input table that contains the geometries to be transformed as RSU
                    logger.info("Grouping all tables...")
                    datasource.execute "DROP TABLE if exists $outputTableName".toString()
                    datasource.execute "CREATE TABLE $outputTableName AS (SELECT THE_GEOM FROM $road_tmp) UNION (SELECT THE_GEOM FROM $rail_tmp) ".toString() +
                            "UNION (SELECT THE_GEOM FROM $hydrographic_tmp) UNION  (SELECT THE_GEOM FROM $vegetation_tmp)".toString()


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
 * @param inputTableName the input table to create the block (group of geometries)
 * @param distance a distance to group the geometries
 * @param prefixName a prefix used to name the output table
 * @param outputTableName the name of the output table
 * @return A database table name.
 */
static IProcess createBlocks(){
    return processFactory.create("Merge the geometries that touch each other",
            [inputTableName: String, distance : double, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputTableName,distance =0.01, prefixName="block", datasource ->
                logger.info("Merging the geometries...")
                String outputTableName =  prefixName+"_"+UUID.randomUUID().toString().replaceAll("-","_")
                datasource.execute "DROP TABLE IF EXISTS $outputTableName".toString()
                datasource.execute "CREATE TABLE $outputTableName as  select  EXPLOD_ID as id_block, the_geom from st_explode ('(select ST_UNION(ST_ACCUM(ST_BUFFER(THE_GEOM,$distance))) as the_geom from $inputTableName)')".toString()
                logger.info("The geometries have been merged")
                [outputTableName: outputTableName]
            }
    )
}
