package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate

/**
 * This process is used to assign to a Reference Spatial Unit (RSU) a Local Climate Zone type (Stewart et Oke, 2012).
 * This assignment is performed according to the 7 indicators used for LCZ classification. Each LCZ type has a
 * given range for each of the 7 indicators. Then the method to find the LCZ type that is the most appropriate for
 * a given RSU is based on the minimum distance to each LCZ (in the 7 dimensions space). In order to calculate this
 * distance, each dimension is normalized according to the mean and the standard deviation (or median and absolute median
 * deviation) of the interval values. Some of the indicators may be more important (or reliable) than the other
 * for the LCZ identification. In order to manage this point, a map containing weights may be passed and will be
 * used to multiply the distance due to a given indicator.
 *
 * @param rsuLczIndicators The table name where are stored ONLY the LCZ indicator values and the RSU id
 * @param normalisationType The indicators used for normalisation of the indicators
 *          --> "AVG": the mean and the standard deviation are used
 *          --> "MEDIAN": the median and the mediane absolute deviation are used
 * @param mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
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
static IProcess identifyLczType() {
return processFactory.create(
        "Set the LCZ type of each RSU",
        [rsuLczIndicators: String, normalisationType: String, mapOfWeights: String[], prefixName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { rsuLczIndicators, normalisationType = "AVG", mapOfWeights = ["sky_view_factor": 1,
                "aspect_ratio": 1, "building_surface_fraction": 1, "impervious_surface_fraction": 1, "pervious_surface_fraction": 1,
                "height_of_roughness_elements": 1, "terrain_roughness_class": 1], prefixName, datasource ->

            // List of possible operations
            def ops = ["AVG", "MEDIAN"]

            if(ops.contains(normalisationType)){

                def idFieldRsu = "id_rsu"
                def centerName = "center"
                def variabilityName = "variability"
                def centerValue = [:]
                def variabilityValue = [:]
                String queryRangeNorm = ""
                String queryValuesNorm = ""
                String queryForPivot = ""
                String queryPerkinsSkill = ""

                // The name of the outputTableName is constructed
                String baseName = "LCZ_type"
                String outputTableName = prefixName + "_" + baseName

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names are defined
                def LCZ_classes = "LCZ_classes" + uid_out
                def normalizedValues = "normalizedValues" + uid_out
                def normalizedRange = "normalizedRange" + uid_out
                def buffLczTable = "buffLczTable" + uid_out
                def allLczTable = "allLczTable" + uid_out
                def pivotedTable = "pivotedTable" + uid_out
                def mainLczTable = "mainLczTable" + uid_out

                // I. Each dimension (each of the 7 indicators) is normalized according to average and standard deviation
                // (or median and median of the variability)

                // The LCZ definitions are created in a Table of the DataBase
                datasource.execute(("DROP TABLE IF EXISTS $LCZ_classes; " +
                        "CREATE TABLE $LCZ_classes(name VARCHAR," +
                        "sky_view_factor_low FLOAT, sky_view_factor_upp FLOAT," +
                        " aspect_ratio_low FLOAT,  aspect_ratio_upp FLOAT, " +
                        "building_surface_fraction_low FLOAT, building_surface_fraction_upp FLOAT, " +
                        "impervious_surface_fraction_low FLOAT, impervious_surface_fraction_upp FLOAT, " +
                        "pervious_surface_fraction_low FLOAT, pervious_surface_fraction_upp FLOAT," +
                        " height_of_roughness_elements_low FLOAT, height_of_roughness_elements_upp FLOAT," +
                        "terrain_roughness_class_low FLOAT, terrain_roughness_class_upp FLOAT);" +
                        "INSERT INTO $LCZ_classes VALUES ('LCZ1',0.2,0.4,2.0,null,0.4,0.6,0.4,0.6,0.0,0.1,25.0,null,7.5,8.5)," +
                                                        "('LCZ2',0.3,0.6,0.8,2.0,0.4,0.7,0.3,0.5,0.0,0.2,10.0,25.0,5.5,7.5)," +
                                                        "('LCZ3',0.2,0.6,0.8,1.5,0.4,0.7,0.2,0.5,0.0,0.3,3.0,10.0,5.5,6.5),"+
                                                        "('LCZ4',0.5,0.7,0.8,1.3,0.2,0.4,0.3,0.4,0.3,0.4,25.0,null,6.5,8.5),"+
                                                        "('LCZ5',0.5,0.8,0.3,0.8,0.2,0.4,0.3,0.5,0.2,0.4,10.0,25.0,4.5,6.5),"+
                                                        "('LCZ6',0.6,0.9,0.3,0.8,0.2,0.4,0.2,0.5,0.3,0.6,3.0,10.0,4.5,6.5),"+
                                                        "('LCZ7',0.2,0.5,1.0,2.0,0.6,0.9,0.0,0.2,0.0,0.3,2.0,4.0,3.5,5.5),"+
                                                        "('LCZ8',0.7,1.0,0.1,0.3,0.3,0.5,0.4,0.5,0.0,0.2,3.0,10.0,4.5,5.5),"+
                                                        "('LCZ9',0.8,1.0,0.1,0.3,0.1,0.2,0.0,0.2,0.6,0.8,3.0,10.0,4.5,6.5),"+
                                                        "('LCZ10',0.6,0.9,0.2,0.5,0.2,0.3,0.2,0.4,0.4,0.5,5.0,15.0,4.5,6.5),"+
                                                        "('LCZA',0.0,0.4,1.0,null,0.0,0.1,0.0,0.1,0.9,1.0,3.0,30.0,7.5,8.5),"+
                                                        "('LCZB',0.5,0.8,0.3,0.8,0.0,0.1,0.0,0.1,0.9,1.0,3.0,15.0,4.5,6.5),"+
                                                        "('LCZC',0.7,0.9,0.3,1.0,0.0,0.1,0.0,0.1,0.9,1.0,0.0,2.0,3.5,5.5),"+
                                                        "('LCZD',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,1.0,2.5,4.5),"+
                                                        "('LCZE',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0.5,2.5),"+
                                                        "('LCZF',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.3,0.5,2.5),"+
                                                        "('LCZG',0.9,1.0,0.0,0.1,0.0,0.1,0.0,0.1,0.9,1.0,0.0,0.0,0.5,1.5);").toString())

                // For each LCZ indicator...
                datasource.getTable(rsuLczIndicators).columnNames.collect { indicCol ->
                    if (!indicCol.equalsIgnoreCase(idFieldRsu)) {
                        // The values used for normalization ("mean" and "standard deviation") are calculated
                        // (for each column) and stored into maps
                        centerValue[indicCol]=datasource.firstRow(("SELECT ${normalisationType}(all_val) AS $centerName " +
                                "FROM " +
                                "(SELECT ${indicCol}_low AS all_val FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                "SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)").toString())[centerName]
                        if(normalisationType == "AVG"){
                            variabilityValue[indicCol]=datasource.firstRow(("SELECT STDDEV_POP(all_val) AS $variabilityName " +
                                    "FROM " +
                                    "(SELECT ${indicCol}_low AS all_val FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                    "SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)").toString())[variabilityName]
                        }
                        else{
                            variabilityValue[indicCol]=datasource.firstRow(("SELECT MEDIAN(ABS(all_val-${centerValue[indicCol]})) AS $variabilityName " +
                                    "FROM " +
                                    "(SELECT ${indicCol}_low AS all_val FROM $LCZ_classes WHERE ${indicCol}_low IS NOT NULL UNION ALL " +
                                    "SELECT ${indicCol}_upp AS all_val FROM $LCZ_classes WHERE ${indicCol}_upp IS NOT NULL)").toString())[variabilityName]
                        }
                        // Piece of query useful for normalizing the LCZ indicator intervals
                        queryRangeNorm += " (${indicCol}_low-${centerValue[indicCol]})/${variabilityValue[indicCol]} AS " +
                                "${indicCol}_low, (${indicCol}_upp-${centerValue[indicCol]})/${variabilityValue[indicCol]} AS " +
                                "${indicCol}_upp, "

                        // Piece of query useful for normalizing the LCZ input values
                        queryValuesNorm += " ($indicCol-${centerValue[indicCol]})/${variabilityValue[indicCol]} AS " +
                                "$indicCol, "
                    }
                }
                // The indicator interval of the LCZ types are normalized according to "center" and "variability" values
                datasource.execute(("DROP TABLE IF EXISTS $normalizedRange; CREATE TABLE $normalizedRange " +
                        "AS SELECT name, ${queryRangeNorm[0..-3]} FROM $LCZ_classes").toString())

                // The input indicator values are normalized according to "center" and "variability" values
                datasource.execute(("DROP TABLE IF EXISTS $normalizedValues; CREATE TABLE $normalizedValues " +
                        "AS SELECT $idFieldRsu, ${queryValuesNorm[0..-3]} FROM $rsuLczIndicators").toString())


                // II. The distance of each RSU to each of the LCZ types is calculated in the normalized interval.
                // The two LCZ types being the closest to the RSU indicators are associated to this RSU. An indicator
                // of uncertainty based on the Perkin Skill Score method is also associated to this "assignment".

                // Create the table where will be stored the distance to each LCZ for each RSU
                datasource.execute(("DROP TABLE IF EXISTS $allLczTable; CREATE TABLE $allLczTable(" +
                        "pk serial, $idFieldRsu integer, lcz varchar, distance float);").toString())

                // For each LCZ type...
                datasource.eachRow("SELECT * FROM $normalizedRange".toString()) { LCZ ->
                    String queryLczDistance = ""
                    // For each indicator...
                    datasource.getTable(rsuLczIndicators).columnNames.collect { indic ->
                        if (!indic.equalsIgnoreCase(idFieldRsu)) {
                            // Define columns names where are stored lower and upper range values of the current LCZ
                            // and current indicator
                            String valLow = indic + "_low"
                            String valUpp = indic + "_upp"
                            // Piece of query useful for calculating the RSU distance to the current LCZ
                            // for the current indicator
                            queryLczDistance +=
                                    "POWER(${mapOfWeights[indic.toString().toLowerCase()]}*CASEWHEN(${LCZ[valLow]} IS NULL," +
                                            "CASEWHEN($indic<${LCZ[valUpp]}, 0, ${LCZ[valUpp]}-$indic)," +
                                            "CASEWHEN(${LCZ[valUpp]} IS NULL," +
                                            "CASEWHEN($indic>${LCZ[valLow]},0,${LCZ[valLow]}-$indic)," +
                                            "CASEWHEN($indic<${LCZ[valLow]},${LCZ[valLow]}-$indic," +
                                            "CASEWHEN($indic<${LCZ[valUpp]},0,${LCZ[valUpp]}-$indic)))),2)+"
                        }
                    }

                    // Fill the table where are stored the distance of each RSU to each LCZ type
                    datasource.execute(("DROP TABLE IF EXISTS $buffLczTable; ALTER TABLE $allLczTable RENAME TO $buffLczTable;" +
                            "DROP TABLE IF EXISTS $allLczTable; " +
                            "CREATE TABLE $allLczTable(pk serial, $idFieldRsu integer, lcz varchar, distance float) " +
                            "AS (SELECT pk, $idFieldRsu, lcz, distance FROM $buffLczTable UNION ALL " +
                            "SELECT null, $idFieldRsu, '${LCZ.name}', SQRT(${queryLczDistance[0..-2]}) FROM $normalizedValues)").toString())
                }

                //

                // The name of the two closest LCZ types are conserved
                datasource.execute(("DROP TABLE IF EXISTS $mainLczTable;" +
                        "CREATE INDEX IF NOT EXISTS all_id ON $allLczTable($idFieldRsu); " +
                        "CREATE TABLE $mainLczTable AS SELECT a.$idFieldRsu, " +
                                                                "(SELECT b.lcz FROM $allLczTable b " +
                                                                                      "WHERE a.$idFieldRsu = b.$idFieldRsu " +
                                                                                      "ORDER BY b.distance ASC LIMIT 1) AS LCZ1," +
                                                                "(SELECT b.lcz FROM $allLczTable b " +
                                                                                      "WHERE a.$idFieldRsu = b.$idFieldRsu " +
                                                                                      "ORDER BY b.distance ASC OFFSET 1 LIMIT 1) AS LCZ2 " +
                                                        "FROM $allLczTable a GROUP BY $idFieldRsu").toString())

                // Recover the LCZ TYPES list and the number of types in a map
                def lczTypeTempo = datasource.rows("SELECT name FROM $LCZ_classes".toString())
                def lczType = []
                lczTypeTempo.each{l ->
                    lczType.add(l["NAME"])
                }
                // For each LCZ type...
                datasource.eachRow("SELECT name FROM $LCZ_classes".toString()) { LCZ ->
                        // Piece of query that will be useful for pivoting the LCZ distance table
                        queryForPivot += "MAX(CASEWHEN(lcz = '${LCZ.name}', distance, null)) AS ${LCZ.name},"

                        // Piece of query that will be useful for the calculation of the Perkins Skill Score
                        queryPerkinsSkill += "LEAST(1./${lczType.size()}, b.${LCZ.name}/(b.${lczType.join("+b.")}))+"
                    }

                // The table is pivoted in order to have the distance for each LCZ type as column and for each RSU as row.
                // Then for each RSU, the distance to the closest LCZ type is stored and the Perkins Skill Score is calculated
                datasource.execute(("DROP TABLE IF EXISTS $pivotedTable;" +
                        "CREATE TABLE $pivotedTable AS SELECT $idFieldRsu,"  +
                        "${queryForPivot[0..-2]} FROM $allLczTable GROUP BY $idFieldRsu;" +
                        "DROP TABLE IF EXISTS $outputTableName;" +
                        "CREATE INDEX IF NOT EXISTS main_id ON $mainLczTable($idFieldRsu);" +
                        "CREATE INDEX IF NOT EXISTS piv_id ON $pivotedTable($idFieldRsu);" +
                        "CREATE TABLE $outputTableName AS SELECT a.*, LEAST(b.${lczType.join(",b.")}) AS min_distance, " +
                        "${queryPerkinsSkill[0..-2]} AS PSS FROM $mainLczTable a LEFT JOIN " +
                        "$pivotedTable b ON a.$idFieldRsu = b.$idFieldRsu;").toString())

                // Temporary tables are deleted
                datasource.execute(("DROP TABLE IF EXISTS $LCZ_classes, $normalizedValues, $normalizedRange," +
                        "$buffLczTable, $allLczTable, $pivotedTable, $mainLczTable;").toString())

                [outputTableName: outputTableName]
            }
            else{
                logger.error("The 'normalisationType' argument is not valid.")
            }
        }
    )}
