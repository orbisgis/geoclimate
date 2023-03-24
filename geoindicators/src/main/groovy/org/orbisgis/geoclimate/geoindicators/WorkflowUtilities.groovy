/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.h2gis.functions.io.utility.PRJUtil
import org.orbisgis.data.H2GIS
import org.orbisgis.data.POSTGIS
import org.orbisgis.geoclimate.Geoindicators
import org.slf4j.LoggerFactory

@BaseScript Geoindicators geoindicators

/**
 * Method to
 * -  create a connection to a specified output DataBase
 * -  check if specified output table names to save are allowed
 *
 * @param outputDBProperties properties to define the db connection
 * @param outputTables list of tables to save
 * @parm allowedOutputTables list of supported tables
 *
 * @return a map with the allowed tables (tables) and a connection to the database (datasource)
 *
 */
def buildOutputDBParameters(def outputDBProperties, def outputTables, def allowedOutputTables) {
    def outputTables_tmp = [:]
    if (!outputTables) {
        outputTables_tmp = allowedOutputTables.collect { [it: it] }
    } else {
        outputTables.each { table ->
            if (allowedOutputTables.contains(table.key.toLowerCase())) {
                outputTables_tmp.put(table.key.toLowerCase(), table.value)
            }
        }
        if (!outputTables_tmp) {
            error "Please set a valid list of output tables as  : \n" +
                    "${allowedOutputTables.collect { name -> [name: name] }}"
        }
    }
    def output_datasource = createDatasource(outputDBProperties.subMap(["user", "password", "url", "databaseName"]))
    if (!output_datasource) {
        error "Cannot connect to the output database"
    }
    return ["tables": outputTables_tmp, "datasource": output_datasource]

}

/**
 * Create a datasource from the following parameters :
 * user, password, url
 *
 * @param database_properties from the json file
 * @return a connection or null if the connection cannot be created
 */
def createDatasource(def database_properties) {
    def db_output_url = database_properties.url
    if (db_output_url) {
        if (db_output_url.startsWith("jdbc:")) {
            String url = db_output_url.substring("jdbc:".length());
            if (url.startsWith("h2")) {
                return H2GIS.open(database_properties)
            } else if (url.startsWith("postgresql")) {
                return POSTGIS.open(database_properties)
            } else {
                error "Unsupported database"
                return
            }
        } else {
            error "Invalid output database url"
            return
        }

    } else {
        def h2gis = H2GIS.open(database_properties)
        if (h2gis) {
            return h2gis
        } else {
            error "The output database url cannot be null or empty"
            return
        }
    }
}

/**
 * Method to check the output tables that can be saved
 *
 * @param outputFolder properties to store in a folder (path and table names)
 * @parm allowedOutputTables list of supported tables
 *
 * @return a map with the allowed tables (tables) and a connection to the database (datasource)
 *
 */
def buildOutputFolderParameters(def outputFolder, def allowedOutputTables) {
    if (outputFolder in Map) {
        def outputPath = outputFolder.path
        def outputTables = outputFolder.tables
        if (!outputPath) {
            return
        }
        if (outputTables) {
            return ["path": outputPath, "tables": allowedOutputTables.intersect(outputTables)]
        }
        return ["path": outputPath, "tables": allowedOutputTables]
    } else {
        return ["path": outputFolder, "tables": allowedOutputTables]
    }
}

/**
 * Parse a json file to a Map
 * @param jsonFile
 * @return
 */
Map readJSON(def jsonFile) {
    def jsonSlurper = new JsonSlurper()
    if (jsonFile) {
        return jsonSlurper.parse(jsonFile)
    }
}

/**
 * Method to export a table corresponding to a grid with indicators in as many ESRI ASCII grid files as indicators
 * @param outputTable
 * @param subFolder
 * @param filePrefix
 * @param subFolder
 * @param outputSRID
 * @param reproject
 * @param deleteOutputData
 * @return
 */
def saveToAscGrid(def outputTable, def subFolder, def filePrefix, def h2gis_datasource, def outputSRID, def reproject, def deleteOutputData) {
    //Check if the table exists
    if (outputTable && h2gis_datasource.hasTable(outputTable)) {
        def env
        if (!reproject) {
            env = h2gis_datasource.getSpatialTable(outputTable).getExtent().getEnvelopeInternal()
        } else {
            def geom = h2gis_datasource.firstRow("SELECT st_transform(ST_EXTENT(the_geom), $outputSRID) as geom from $outputTable".toString()).geom
            if (geom) {
                env = geom.getEnvelopeInternal()
            }
        }
        if (env) {
            def xmin = env.getMinX()
            def ymin = env.getMinY()
            def nbColsRowS = h2gis_datasource.firstRow("select max(id_col) as cmax, max(id_row) as rmax from $outputTable".toString())
            def nbcols = nbColsRowS.cmax
            def nbrows = nbColsRowS.rmax

            double dy = env.getMaxY() - ymin
            def x_size = dy / nbrows

            def IndicsTable = h2gis_datasource."$outputTable"
            List columnNames = IndicsTable.columns
            columnNames.remove("THE_GEOM")
            columnNames.remove("ID_GRID")
            columnNames.remove("ID_COL")
            columnNames.remove("ID_ROW")

            //Add indexes
            h2gis_datasource.getTable(outputTable)."ID_COL".createIndex()
            h2gis_datasource.getTable(outputTable)."ID_ROW".createIndex()

            //Save each grid
            columnNames.each { it ->
                def outputFile = new File(subFolder + File.separator + filePrefix + "_" + it.toLowerCase() + ".asc")
                if (deleteOutputData) {
                    outputFile.delete()
                }
                outputFile.withOutputStream { stream ->
                    stream << "ncols $nbcols\nnrows $nbrows\nxllcorner $xmin\nyllcorner $ymin\ncellsize $x_size\nnodata_value -9999\n"
                    def query = "select id_row, id_col, case when $it is not null then cast($it as decimal(18, 3)) else -9999 end as $it from $outputTable order by id_row desc, id_col"
                    def rowData = ""
                    h2gis_datasource.eachRow(query.toString()) { row ->
                        rowData += row.getString(it) + " "
                        if (row.getInt("id_col") == nbcols) {
                            rowData += "\n"
                            stream << rowData
                            rowData = ""
                        }
                    }
                }
                //Save the PRJ file
                if (outputSRID >= 0) {
                    File outPrjFile = new File(subFolder + File.separator + filePrefix + "_" + it.toLowerCase() + ".prj")
                    PRJUtil.writePRJ(h2gis_datasource.getConnection(), outputSRID, outPrjFile)
                }
                info "$outputTable has been saved in $outputFile"
            }
        }
    }
}

/**
 * Method to save a table into a geojson file
 * @param outputTable name of the table to export
 * @param filePath path to save the table
 * @param h2gis_datasource connection to the database
 * @param outputSRID srid code to reproject the outputTable.
 * @param reproject true if the file must be reprojected
 * @param deleteOutputData true to delete the file if exists
 */
def saveToGeojson(def outputTable, def filePath, H2GIS h2gis_datasource, def outputSRID, def reproject, def deleteOutputData) {
    if (outputTable && h2gis_datasource.hasTable(outputTable)) {
        if (!reproject) {
            h2gis_datasource.save(outputTable, filePath, deleteOutputData)
        } else {
            if (!h2gis_datasource.getTable(outputTable).isEmpty()) {
                h2gis_datasource.getSpatialTable(outputTable).reproject(outputSRID).save(filePath, deleteOutputData)
            }
        }
        info "${outputTable} has been saved in ${filePath}."
    }
}


/**
 * Method to save a table into a CSV file
 * @param outputTable name of the table to export
 * @param filePath path to save the table
 * @param h2gis_datasource connection to the database
 * @param deleteOutputData true to delete the file if exists
 */
def saveToCSV(def outputTable, def filePath, def h2gis_datasource, def deleteOutputData) {
    if (outputTable && h2gis_datasource.hasTable(outputTable)) {
        h2gis_datasource.save("(SELECT ID_BUILD, ID_SOURCE FROM $outputTable WHERE estimated=true)", filePath, deleteOutputData)
        info "${outputTable} has been saved in ${filePath}."
    }
}

/**
 * Utility class to change log level for all loggers
 *
 */
static def setLoggerLevel(String loggerLevel) {
    if (loggerLevel) {
        Level level
        if (loggerLevel.equalsIgnoreCase("INFO")) {
            level = Level.INFO
        } else if (loggerLevel.equalsIgnoreCase("DEBUG")) {
            level = Level.DEBUG
        } else if (loggerLevel.equalsIgnoreCase("TRACE")) {
            level = Level.TRACE
        } else if (loggerLevel.equalsIgnoreCase("OFF")) {
            level = Level.OFF
        } else {
            throw new RuntimeException("Invalid log level. Allowed values are : INFO, DEBUG, TRACE, OFF")
        }
        var context = (LoggerContext) LoggerFactory.getILoggerFactory()
        context.getLoggerList().each { it -> it.setLevel(level) }
    }
}