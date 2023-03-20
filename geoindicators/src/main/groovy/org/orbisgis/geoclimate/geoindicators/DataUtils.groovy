package org.orbisgis.geoclimate.geoindicators

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.data.jdbc.*

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
String joinTables(JdbcDataSource datasource , Map inputTableNamesWithId, String outputTableName , boolean prefixWithTabName= false){
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

            datasource "DROP TABLE IF EXISTS $outputTableName".toString()
            datasource indexes.toString()
            datasource "CREATE TABLE $outputTableName AS SELECT $columnsAsString $leftQuery".toString()

            return outputTableName
        }

/**
 * An utility process to save several tables in a folder
 *
 * @param  inputTableNames to be stored in the directory.
 * Note : A spatial table is saved in a geojson file and the other in csv
 * @param directory folder to save the tables
 * @param datasource connection to the database
 *
 * @return the directory where the tables are saved
 */
String saveTablesAsFiles(JdbcDataSource datasource, List inputTableNames, boolean  delete= false, String directory){
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
            return directory
        }


/**
 * Get a set of parameters stored in a json file
 *
 * @param file
 * @param altResourceStream
 * @return
 */
static Map parametersMapping(def file, def altResourceStream) {
    def paramStream
    def jsonSlurper = new JsonSlurper()
    if (file) {
        if (new File(file).isFile()) {
            paramStream = new FileInputStream(file)
        } else {
            warn("No file named ${file} found. Taking default instead")
            paramStream = altResourceStream
        }
    } else {
        paramStream = altResourceStream
    }
    return jsonSlurper.parse(paramStream)
}
