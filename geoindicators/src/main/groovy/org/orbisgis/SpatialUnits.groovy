package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource


@BaseScript Geoclimate geoclimate

/**
 * This process is used to create the reference spatial units (RSU)
 *
 * Reference:
 *  Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 *  urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
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
                datasource.execute "CREATE TABLE $outputTableName as  select  * from st_explode ('(select st_polygonize(st_union(st_precisionreducer(st_node(st_accum(the_geom)), 3))) as the_geom from $inputTableName)')".toString()
                logger.info("Reference spatial units table created")
                [outputTableName: outputTableName]
        }
    )
}
