package org.orbisgis.orbisprocess.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.*
import org.orbisgis.orbisdata.processmanager.api.IProcess

@BaseScript Geoindicators geoindicators

/**
 * An utility process to join all tables in one table
 *
 * @param  inputTableNamesWithId list of table names with a identifier column name
 * @param prefixName for the output table
 * @param datasource connection to the database
 *
 * @return
 */
IProcess joinTables() {
    create {
        title "Utility process to join tables in one"
        id "joinTables"
        inputs inputTableNamesWithId: Map, outputTableName: String, datasource: JdbcDataSource,
                prefixWithTabName: false
        outputs outputTableName: String
        run { inputTableNamesWithId, outputTableName, datasource, prefixWithTabName ->

            debug "Executing Utility process to join tables in one"

            def columnKey
            def alias = "a"
            def leftQuery = ""
            def indexes = ""

            def columns = []

            inputTableNamesWithId.each { key, value ->
                //Reload cache to be sure that the table is up to date
                datasource."$key".reload()
                if (alias == "a") {
                    columnKey = "$alias.$value"
                    // Whether or not the table name is add as prefix of the indicator in the new table
                    if (prefixWithTabName) {
                        columns = datasource."$key".columns.collect {
                            alias + ".$it AS ${key}_$it"
                        }
                    } else {
                        columns = datasource."$key".columns.collect {
                            alias + ".$it"
                        }
                    }
                    leftQuery += " FROM $key as $alias "
                } else {
                    datasource."$key".columns.forEach() { item ->
                        if (!item.equalsIgnoreCase(value)) {
                            if (prefixWithTabName) {
                                columns.add(alias + ".$item AS ${key}_$item")
                            } else {
                                columns.add(alias + ".$item")
                            }
                        }
                    }
                    leftQuery += " LEFT JOIN $key as $alias ON $alias.$value = $columnKey "
                }
                indexes += "CREATE INDEX IF NOT EXISTS ${key}_ids ON $key USING BTREE($value);"
                alias++
            }

            def columnsAsString = columns.join(",")

            datasource "DROP TABLE IF EXISTS $outputTableName"
            datasource indexes
            datasource "CREATE TABLE $outputTableName AS SELECT $columnsAsString $leftQuery"

            [outputTableName: outputTableName]
        }
    }
}

/**
 * An utility process to save several tables in a folder
 *
 * @param  inputTableNames to be stored in the directory.
 * Note : A spatial table is saved in a geojson file and the other in csv
 * @param directory folder to save the tables
 * @param datasource connection to the database
 *
 * @return
 */
IProcess saveTablesAsFiles() {
    return create {
        title "Utility process to save tables in geojson or csv files"
        id "saveTablesAsFiles"
        inputs inputTableNames: String[], delete: false,directory: String, datasource: JdbcDataSource
        outputs directory: String
        run { inputTableNames, delete, directory, datasource ->
            if (directory == null) {
                error "The directory to save the data cannot be null"
                return
            }
            def dirFile = new File(directory)

            if (!dirFile.exists()) {
                dirFile.mkdir()
                debug "The folder $directory has been created"
            } else if (!dirFile.isDirectory()) {
                error "Invalid directory path"
                return
            }
            inputTableNames.each { tableName ->
                if (tableName) {
                    def fileToSave = dirFile.absolutePath + File.separator + tableName +
                            (datasource."$tableName".spatial ? ".geojson" : ".csv")
                    def table = datasource.getTable(tableName)
                    if (table) {
                        table.save(fileToSave, delete)
                        debug "The table $tableName has been saved in file $fileToSave"
                    }
                }
            }
            [directory: directory]
        }
    }
}