package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource


@BaseScript Geoclimate geoclimate

/**
 * This process is used to create the reference spatial units (RSU)
 *
 * @return A database table name.
 */
static createRSU(){
    processFactory.create("Create reference spatial units (RSU)",
            [inputTableName: String, outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputTableName, outputTableName, datasource ->
                logger.info("Creating the reference spatial units")
                datasource.execute "DROP TABLE IF EXISTS $outputTableName".toString()
                datasource.execute "CREATE TABLE $outputTableName as  select  explode_id as rsu_id, the_geom from st_explode ('(select st_polygonize(st_union(st_precisionreducer(st_node(st_accum(the_geom)), 3))) as the_geom from $inputTableName)')".toString()
                logger.info("Reference spatial units table created")
                [outputTableName: outputTableName]
        }
    )
}

/**
 * This process is used to prepare the input abstract model
 * in order to compute the reference spatial units (RSU)
 *
 * @return A database table name.
 */
static prepareRSUData(){
    processFactory.create("Prepare the abstract model to build the RSU",
            [zoneTable: String, roadTable: String,  railTable: String, vegetationTable : String, hydrographicTable :String,
             surface_vegetation : double, surface_hydro : double, outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { zoneTable ,roadTable, railTable, vegetationTable , hydrographicTable, surface_vegetation =100000, surface_hydrographic=2500 ,outputTableName, datasource ->
                logger.info("Creating the reference spatial units")
                def numberZone = datasource.firstRow("select count(*) as nb from $zoneTable".toString()).nb

                if(numberZone==1){
                    String uuid_tmp =  UUID.randomUUID().toString().replaceAll("-", "_")+"_"
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
