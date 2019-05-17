package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * An utility process to join all tables in one table
 * @param  inputTableNamesWithId list of table names with a identifier column name
 * @prefixName for the output table
 * @datasource connection to the database
 *
 * @return
 */
static IProcess joinTables(){
    processFactory.create("", [inputTableNamesWithId: Map
                                , prefixName: String, datasource: JdbcDataSource], [outputTableName: String],
            { inputTableNamesWithId, prefixName, JdbcDataSource datasource ->
                // The name of the outputTableName is constructed
                String baseName = "joined"
                String outputTableName = prefixName + "_" + baseName
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