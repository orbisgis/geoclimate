package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate

/**
 * This process is used to assign to a Reference Spatial Unit (RSU) a Local Climate Zone type (Stewart et Oke, 2012).
 * This assignment is performed according to the 7 indicators used for LCZ classification. Each LCZ type has a
 * given range for each of the 7 indicators. Then the method to find the LCZ type that is the most appropriate for
 * a given RSU is based on the minimum distance to each LCZ (in the 7 dimensions space). In order to calculate this
 * distance, each dimension is normalized according to the mean and the standard deviation (or median and absolute median
 * deviation) of the interval values.
 *
 * @param rsuLczIndicators The table name where are stored ONLY the LCZ indicator values and the RSU id
 * @param normalisationType The indicators used for normalisation of the indicators
 *          --> "AVG": the mean and the standard deviation are used
 *          --> "MEDIAN": the median and the mediane absolute deviation are used
 * @param prefixName String use as prefix to name the output table
 * @param datasource A connection to a database
 *
 * References:
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of
 * the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess setLczType() {
return processFactory.create(
        "Set the LCZ type of each RSU",
        [rsuLczIndicators: String, normalisationType: String, prefixName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { rsuLczIndicators, normalisationType = "MEAN", prefixName, datasource ->

            JdbcDataSource datasource
            // List of possible operations
            def ops = ["AVG", "MEDIAN"]

            if(ops.contains(normalisationType)){
                // Absolute path of the CSV file where are stored the LCZ definitions
                String lczPath = new File(this.class.getResource("LCZ_classes.csv").toURI()).getAbsolutePath()

                def geometricFieldUp = "the_geom"
                def idFieldRsu = "id_rsu"
                def centerName = "center"
                def variabilityName = "variability"
                def centerValue = [:]
                def variabilityValue = [:]
                String queryRangeNorm = ""
                String queryValuesNorm = ""

                // The name of the outputTableName is constructed
                String baseName = "LCZ_type"
                String outputTableName = prefixName + "_" + baseName

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names are defined
                def LCZ_classes = "LCZ_classes" + uid_out
                def normalizedValues = "normalizedValues" + uid_out
                def normalizedRange = "normalizedRange" + uid_out

                // I. Each dimension (each of the 7 indicators) is normalized according to average and standard deviation
                // (or median and median of the variability)
                // The LCZ definition is imported and the data type modified (null values could affect the type...)
                datasource.load(lczPath, LCZ_classes)
                datasource.getTable(LCZ_classes).columnNames.collect { indicCol ->
                    datasource.execute("ALTER TABLE $LCZ_classes ALTER COLUMN $indicCol float")
                }

                datasource.getTable(rsuLczIndicators).columnNames.collect { indicCol ->
                    if (!indicCol.equalsIgnoreCase(idFieldRsu)) {
                        // The values used for normalization ("mean" and "standard deviation") are calculated for each column
                        centerValue[indicCol]=datasource.firstRow(("SELECT ${normalisationType}(ALL) AS $centerName" +
                                "FROM " +
                                "(SELECT ${indicCol}_low AS ALL FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                "SELECT ${indicCol}_low AS ALL FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)")).toString())[centerName]
                        if(normalisationType == "AVG"){
                            variabilityValue[indicCol]=datasource.firstRow(("SELECT STDDEV_POP(ALL) AS $variabilityName" +
                                    "FROM " +
                                    "(SELECT ${indicCol}_low AS ALL FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                    "SELECT ${indicCol}_low AS ALL FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)").toString())[variabilityName]
                        }
                        else{
                            variabilityValue[indicCol]=datasource.firstRow(("SELECT MEDIAN(ABS(ALL-$centerValue)) AS $variabilityName" +
                                    "FROM " +
                                    "(SELECT ${indicCol}_low AS ALL FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                    "SELECT ${indicCol}_low AS ALL FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)").toString())[variabilityName]
                        }

                        // Piece of query useful for normalizing the LCZ indicator intervals
                        queryRangeNorm += " (${indicCol}_low-$centerValue[$indicCol])/$variabilityValue[$indicCol] AS " +
                                "${indicCol}_low, (${indicCol}_upp-$centerValue[$indicCol])/$variabilityValue[$indicCol] AS " +
                                "${indicCol}_upp, "

                        // Piece of query useful for normalizing the LCZ input values
                        queryValuesNorm += " ($indicCol-$centerValue[$indicCol])/$variabilityValue[$indicCol] AS " +
                                "$indicCol, "

                        columns.add(a + "." + item)
                    }
                }
                // The indicator interval of the LCZ types are normalized
                datasource.execute(("DROP TABLE IF EXISTS $normalizedRange; CREATE TABLE $normalizedRange " +
                        "AS SELECT ${queryRangeNorm[0..-3]} FROM $LCZ_classes").toString())

                // The input indicator values are normalized
                datasource.execute(("DROP TABLE IF EXISTS $normalizedRange; CREATE TABLE $normalizedValues " +
                        "AS SELECT ${queryValuesNorm[0..-3]} FROM $rsuLczIndicators").toString())

                // II. The distance of each RSU to each of the LCZ types is calculated in the normalized interval.
                // The LCZ type being the closer to the RSU definition is set for the given RSU. Then an indicator
                // of uncertainty is also associated to this LCZ type, based on the Perkin Skill Score method.
            }
            else{
                logger.error("The 'normalisationType' argument is not valid.")
            }


            logger.info("Executing $query")
            datasource.execute query
            [outputTableName: outputTableName]
            }
    )}
