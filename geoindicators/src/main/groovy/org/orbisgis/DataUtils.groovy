package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * An utility process to join all tables in one table using an id column
 * The id column must be same in all input tables.
 * @param  inputTableNames list of table names
 * @param columnId identified for the tables
 * @prefixName for the output table
 * @datasource connection to the database
 *
 * @return
 */
static IProcess joinTables(){
    processFactory.create("", [inputTableNames: String[],columnId: String
                                , prefixName: String, datasource: JdbcDataSource], [outputTableName: String],
            { inputTableNames, columnId, prefixName, JdbcDataSource datasource ->

                if(columnId==null){
                    logger.error("The column id cannot be null")
                    return
                }
                if(columnId.isEmpty()){
                    logger.error("The column id cannot be empty")
                    return
                }

                // The name of the outputTableName is constructed
                String baseName = "joined"
                String outputTableName = prefixName + "_" + baseName
                String columnsAsString = "*"
                String columnKey = "a.${columnId}"
                String a = "a"
                def leftQuery = ""
                def indexes = "CREATE INDEX IF NOT EXISTS ${inputTableNames[0]}_ids ON ${inputTableNames[0]}($columnId);"

                def columns = datasource.getTable(inputTableNames[0]).columnNames.collect{
                    a+"."+it
                }
                inputTableNames[1..-1].each{
                    a++
                    datasource.getTable(it).columnNames.collect {item->
                        if(!item.equalsIgnoreCase(columnId)){
                           columns.add(a+"."+item)}
                    }
                    leftQuery+= " LEFT JOIN ${it} as ${a} ON ${a}.${columnId} = ${columnKey} "
                    indexes+= "CREATE INDEX IF NOT EXISTS ${it}_ids ON ${it}($columnId);"
                }

                def setOfColumns = columns as Set
                columnsAsString = setOfColumns.join(",")

                def query = "CREATE TABLE ${outputTableName} AS SELECT ${columnsAsString} FROM ${inputTableNames[0]} as a ${leftQuery}"

                datasource.execute("DROP TABLE IF EXISTS ${outputTableName}".toString())
                datasource.execute(indexes.toString())
                datasource.execute(query.toString())
                [outputTableName:outputTableName]

            })
}