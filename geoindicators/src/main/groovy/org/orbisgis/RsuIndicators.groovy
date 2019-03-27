package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * Process used to compute the sum of all building free facades (roofs are excluded) included in a
 * Reference Spatial Unit (RSU - such as urban blocks) divided by the RSU area. The calculation is performed
 * according to a building Table where are stored the "building_contiguity", the building wall height and
 * the "building_total_facade_length" values as well as a correlation Table between buildings and blocks.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess rsuFreeExternalFacadeDensity() {
return processFactory.create(
        "RSU free external facade density",
        [inputBuildingTableName: String,inputFields:String[],inputCorrelationTableName: String,
         buContiguityFieldName: String, buTotalFacadeLength: String, outputTableName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { inputTableName, inputFields, inputCorrelationTableName, buContiguityFieldName, buTotalFacadeLength,
          outputTableName, datasource ->
            def geometricFieldRsu = "the_geom"
            def idFieldBu = "id_build"
            def idFieldRsu = "id_rsu"
            def height_wall = "height_wall"

            String query = "CREATE INDEX IF NOT EXISTS id_bua ON $inputTableName($idFieldBu); "+
                            "CREATE INDEX IF NOT EXISTS id_bub ON $inputCorrelationTableName($idFieldBu); "+
                            "CREATE INDEX IF NOT EXISTS id_blb ON $inputCorrelationTableName($idFieldRsu); "+
                            "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                            "SELECT SUM((1-a.$buContiguityFieldName)*a.$buTotalFacadeLength*a.$height_wall)/"+
                            "st_area(b.$geometricFieldRsu) AS rsu_free_external_facade_density, b.$idFieldRsu "

            if(!inputFields.isEmpty()){
                query += ", a.${inputFields.join(",a.")} "
            }

            query += "FROM $inputTableName a, $inputCorrelationTableName b "+
                        "WHERE a.$idFieldBu = b.$idFieldBu GROUP BY b.$idFieldRsu, b.$geometricFieldRsu;"

            logger.info("Executing $query")
            datasource.execute query
            [outputTableName: outputTableName]
        }
)}