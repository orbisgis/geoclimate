package org.orbisgis.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoindicators geoindicators

/**
 * An utility process to join all tables in one table
 * @param  inputTableNamesWithId list of table names with a identifier column name
 * @param prefixName for the output table
 * @param datasource connection to the database
 *
 * @return
 */
static IProcess joinTables() {
    processFactory.create("Utility process to join tables in one",
            [inputTableNamesWithId: Map, outputTableName: String, datasource: JdbcDataSource],
            [outputTableName: String],
            { inputTableNamesWithId, outputTableName, JdbcDataSource datasource ->

                logger.info("Executing Utility process to join tables in one")

                def columnKey
                def alias = "a"
                def leftQuery = ""
                def indexes = ""

                def columns = []

                inputTableNamesWithId.each { key, value ->
                    if (alias == "a") {
                        columnKey = "$alias.${value}"
                        columns = datasource.getTable(key).columnNames.collect {
                            alias + "." + it
                        }
                        leftQuery += " FROM ${key} as $alias "
                    } else {
                        datasource.getTable(key).columnNames.forEach() { item ->
                            if (!item.equalsIgnoreCase(value)) {
                                columns.add(alias + "." + item)
                            }
                        }
                        leftQuery += " LEFT JOIN ${key} as ${alias} ON ${alias}.${value} = ${columnKey} "
                    }
                    indexes += "CREATE INDEX IF NOT EXISTS ${key}_ids ON ${key}($value);"
                    alias++
                }

                def columnsAsString = (columns as Set).join(",")

                datasource.execute "DROP TABLE IF EXISTS ${outputTableName}"
                datasource.execute indexes
                datasource.execute "CREATE TABLE ${outputTableName} AS SELECT ${columnsAsString} ${leftQuery}"

                [outputTableName: outputTableName]
            }
    )
}

/**
 * An utility process to save several tables in a folder
 * @param  inputTableNames to be stored in the directory.
 * Note : A spatial table is saved in a geojson file and the other in csv
 * @param directory folder to save the tables
 * @param datasource connection to the database
 *
 * @return
 */
static IProcess saveTablesAsFiles(){
    processFactory.create("Utility process to save tables in geojson or csv files",
            [inputTableNames: String[], directory: String, datasource: JdbcDataSource],
            [directory: String],
            { inputTableNames, directory, datasource ->
                if(directory==null){
                    logger.info("The directory to save the data cannot be null")
                    return
                }
                File dirFile = new File(directory)

                if(!dirFile.exists()){
                    dirFile.mkdir()
                    logger.info("The folder ${directory} has been created")
                }
                else if (!dirFile.isDirectory()){
                    logger.info("Invalid directory path")
                    return
                }
                inputTableNames.each{tableName ->
                    def fileToSave = dirFile.absolutePath + File.separator + tableName +
                            (datasource.getTable(tableName).isSpatial() ? ".geojson" : ".csv")
                    datasource.save(tableName, fileToSave)
                    logger.info("The table ${tableName} has been saved in file ${fileToSave}")
                }
                [directory:directory]
            }
    )
}