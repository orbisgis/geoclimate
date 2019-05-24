package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * An utility process to join all tables in one table
 * @param  inputTableNamesWithId list of table names with a identifier column name
 * @param prefixName for the output table
 * @param datasource connection to the database
 *
 * @return
 */
static IProcess joinTables(){
    processFactory.create("Utility process to join tables in one", [inputTableNamesWithId: Map
                                , outputTableName: String, datasource: JdbcDataSource], [outputTableName: String],
            { inputTableNamesWithId, outputTableName, JdbcDataSource datasource ->

                logger.info("Executing Utility process to join tables in one")

                String columnKey
                String a = "a"
                def leftQuery = ""
                def indexes = ""

                def columns = []

                inputTableNamesWithId.eachWithIndex{ key, value , i->
                    if(i==0){
                        columnKey = "a.${value}"
                        indexes += "CREATE INDEX IF NOT EXISTS ${key}_ids ON ${key}($value);"
                        columns = datasource.getTable(key).columnNames.collect{
                            a+"."+it
                        }
                        leftQuery+=" FROM ${key} as a "
                    }
                    else {
                        a++
                        datasource.getTable(key).columnNames.collect { item ->
                            if (!item.equalsIgnoreCase(value)) {
                                columns.add(a + "." + item)
                            }
                        }
                        leftQuery += " LEFT JOIN ${key} as ${a} ON ${a}.${value} = ${columnKey} "
                        indexes += "CREATE INDEX IF NOT EXISTS ${key}_ids ON ${key}($value);"
                    }
                }

                def setOfColumns = columns as Set
                def columnsAsString = setOfColumns.join(",")

                def query = "CREATE TABLE ${outputTableName} AS SELECT ${columnsAsString} ${leftQuery}"

                datasource.execute("DROP TABLE IF EXISTS ${outputTableName}".toString())
                datasource.execute(indexes.toString())
                datasource.execute(query.toString())
                [outputTableName:outputTableName]

            })
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
    processFactory.create("Utility process to save tables in geojson or csv files", [inputTableNames: String[]
                               , directory: String, datasource: JdbcDataSource], [directory: String],
            { inputTableNames, directory, JdbcDataSource datasource ->
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
                    if(datasource.getTable(tableName).isSpatial()){
                        def fileToSave =dirFile.absolutePath+File.separator+tableName+".geojson"
                        datasource.save(tableName, fileToSave)
                        logger.info("The table ${tableName} has been saved in file ${fileToSave}")
                    }
                    else{
                        def fileToSave =dirFile.absolutePath+File.separator+tableName+".csv"
                        datasource.save(tableName, fileToSave)
                        logger.info("The table ${tableName} has been saved in file ${fileToSave}")
                    }
                }
                [directory:directory]
})
}