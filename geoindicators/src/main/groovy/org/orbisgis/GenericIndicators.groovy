package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate

/**
 * This process is used to compute basic statistical operations on a specific variable from a lower scale (for
 * example the sum of each building volume constituting a block to calculate the block volume)
 *
 * @param inputLowerScaleTableName the table name where are stored low scale objects and the id of the upper scale
 * zone they are belonging to (e.g. buildings)
 * @param inputUpperScaleTableName the table of the upper scale where the informations has to be aggregated to
 * @param inputIdUp the ID of the upper scale
 * @param inputVarAndOperations a map containing as key the informations that has to be transformed from the lower to
 * the upper scale and as values a LIST of operation to apply to this information (e.g. ["building_area":["SUM", "NB_DENS"]]).
 * Operations should be in the following list:
 *          --> "SUM": sum the geospatial variables at the upper scale
 *          --> "AVG": average the geospatial variables at the upper scale
 *          --> "GEOM_AVG": average the geospatial variables at the upper scale using a geometric average
 *          --> "DENS": sum the geospatial variables at the upper scale and divide by the area of the upper scale
 *          --> "NB_DENS" : count the number of lower scale objects within the upper scale object and divide by the upper
 *          scale area. NOTE THAT THE THREE FIRST LETTERS OF THE MAP KEY WILL BE USED TO CREATE THE NAME OF THE INDICATOR
 *          (e.g. "BUI" in the case of the example given above)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess unweightedOperationFromLowerScale() {
return processFactory.create(
        "Unweighted statistical operations from lower scale",
        [inputLowerScaleTableName: String,inputUpperScaleTableName: String, inputIdUp: String,
         inputVarAndOperations: Map, prefixName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { inputLowerScaleTableName, inputUpperScaleTableName, inputIdUp,
          inputVarAndOperations, prefixName, datasource ->

            def geometricFieldUp = "the_geom"

            // The name of the outputTableName is constructed
            String baseName = "unweighted_operation_from_lower_scale"
            String outputTableName = prefixName + "_" + baseName

            // List of possible operations
            def ops = ["SUM","AVG", "GEOM_AVG", "DENS", "NB_DENS"]

            String query = "CREATE INDEX IF NOT EXISTS id_l ON $inputLowerScaleTableName($inputIdUp); "+
                            "CREATE INDEX IF NOT EXISTS id_ucorr ON $inputUpperScaleTableName($inputIdUp); "+
                            "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "


            inputVarAndOperations.each {var, operations ->
                operations.each{op ->
                    op = op.toUpperCase()
                    if(ops.contains(op)){
                        if(op=="GEOM_AVG"){
                            query += "EXP(1.0/COUNT(*)*SUM(LOG(a.$var))) AS ${op+"_"+var},"
                        }
                        else if(op=="DENS"){
                            query += "SUM(a.$var::float)/ST_AREA(b.$geometricFieldUp) AS ${op+"_"+var},"
                        }
                        else if(op=="NB_DENS"){
                            query += "COUNT(a.*)/ST_AREA(b.$geometricFieldUp) AS ${op+"_"+var},"
                        }
                        else{
                            query += "$op(a.$var::float) AS ${op+"_"+var},"
                        }
                    }
                }

            }
            query += "b.$inputIdUp FROM $inputLowerScaleTableName a, $inputUpperScaleTableName b " +
                    "WHERE a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp"
            logger.info("Executing $query")
            datasource.execute query
            [outputTableName: outputTableName]
            }
    )}

/**
 * This process is used to compute weighted average and standard deviation on a specific variable from a lower scale (for
 * example the mean building roof height within a reference spatial unit where the roof height values are weighted
 * by the values of building area)
 *
 *  @param inputLowerScaleTableName the table name where are stored low scale objects and the id of the upper scale
 *  zone they are belonging to (e.g. buildings)
 *  @param inputUpperScaleTableName the table of the upper scale where the informations has to be aggregated to
 *  @param inputIdUp the ID of the upper scale
 *  @param inputVarWeightsOperations a map containing as key the informations that has to be transformed from the lower to
 *  the upper scale, and as value a map containing as key the variable that has to be used as weight and as values
 *  a LIST of operation to apply to this couple (information/weight). Note that the allowed operations should be in
 *  the following list:
 *           --> "STD": weighted standard deviation
 *           --> "AVG": weighted mean
 *  @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess weightedAggregatedStatistics() {
    return processFactory.create(
            "Weighted statistical operations from lower scale",
            [inputLowerScaleTableName: String,inputUpperScaleTableName: String, inputIdUp: String,
             inputVarWeightsOperations: Map, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputLowerScaleTableName, inputUpperScaleTableName, inputIdUp,
              inputVarWeightsOperations, prefixName, datasource ->
                def ops = ["AVG", "STD"]

                // The name of the outputTableName is constructed
                String baseName = "weighted_aggregated_statistics"
                String outputTableName = prefixName + "_" + baseName

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def weighted_mean = "weighted_mean"+uid_out

                // The weighted mean is calculated in all cases since it is useful for the STD calculation
                def weightedMeanQuery = "CREATE INDEX IF NOT EXISTS id_l ON $inputLowerScaleTableName($inputIdUp); "+
                        "CREATE INDEX IF NOT EXISTS id_ucorr ON $inputUpperScaleTableName($inputIdUp); "+
                        "DROP TABLE IF EXISTS $weighted_mean; CREATE TABLE $weighted_mean($inputIdUp INTEGER,"
                def nameAndType = ""
                def weightedMean = ""
                inputVarWeightsOperations.each{var, weights ->
                    weights.each{weight, operations ->
                        nameAndType += "weighted_avg_${var}_$weight DOUBLE DEFAULT 0,"
                        weightedMean += "SUM(a.$var*a.$weight) / SUM(a.$weight) AS weighted_avg_${var}_$weight,"
                    }
                }
                weightedMeanQuery += nameAndType[0..-2] + ") AS (SELECT b.$inputIdUp, ${weightedMean[0..-2]}" +
                        " FROM $inputLowerScaleTableName a, $inputUpperScaleTableName b " +
                        "WHERE a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp)"
                datasource.execute(weightedMeanQuery.toString())

                // The weighted std is calculated if needed and only the needed fields are returned
                def weightedStdQuery = "CREATE INDEX IF NOT EXISTS id_lcorr ON $weighted_mean($inputIdUp); " +
                        "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.$inputIdUp,"
                inputVarWeightsOperations.each{var, weights ->
                    weights.each{weight, operations ->
                        // The operation names are transformed into upper case
                        operations.replaceAll({s -> s.toUpperCase()})
                        if(operations.contains("AVG")) {
                            weightedStdQuery += "b.weighted_avg_${var}_$weight,"
                        }
                        if(operations.contains("STD")) {
                            weightedStdQuery += "POWER(SUM(a.$weight*POWER(a.$var-b.weighted_avg_${var}_$weight,2))/" +
                                    "SUM(a.$weight),0.5) AS weighted_std_${var}_$weight,"
                        }
                    }
                }
                weightedStdQuery = weightedStdQuery[0..-2] +" FROM $inputLowerScaleTableName a, $weighted_mean b " +
                        "WHERE a.$inputIdUp = b.$inputIdUp GROUP BY b.$inputIdUp"

                logger.info("Executing $weightedStdQuery")
                datasource.execute weightedStdQuery

                // The temporary tables are deleted
                datasource.execute("DROP TABLE IF EXISTS $weighted_mean".toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * This process extract geometry properties.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param inputFields Fields to conserved from the input table in the output table
 * @param inputTableName The name of the input ITable where are stored the geometries to apply the geometry operations
 * @param operations Operations that have to be applied. These operations should be in the following list (see http://www.h2gis.org/docs/dev/functions/):
 * ["st_geomtype","st_srid", "st_length","st_perimeter","st_area", "st_dimension", "st_coorddim", "st_num_geoms",
 * "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Erwan Bocher
 */
static IProcess geometryProperties() {
    return processFactory.create(
            "Geometry properties",
            [inputTableName: String,inputFields:String[],operations: String[]
             , prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { inputTableName,inputFields, operations, prefixName, datasource ->

                def geometricField = "the_geom";
                def ops = ["st_geomtype","st_srid", "st_length","st_perimeter","st_area", "st_dimension",
                           "st_coorddim", "st_num_geoms", "st_num_pts", "st_issimple", "st_isvalid", "st_isempty"]

                // The name of the outputTableName is constructed
                String baseName = "geometry_properties"
                String outputTableName = prefixName + "_" + baseName

                String query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT "

                // The operation names are transformed into lower case
                operations.replaceAll({s -> s.toLowerCase()})

                operations.each {operation ->
                    if(ops.contains(operation)){
                        query += "$operation($geometricField) as ${operation.substring(3)},"
                    }
                }
                query+= "${inputFields.join(",")} from $inputTableName"
                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}