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

import groovy.json.JsonSlurper
import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.PrecisionModel
import org.locationtech.jts.precision.GeometryPrecisionReducer
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.SQLException

@BaseScript Geoindicators geoindicators

/**
 * An utility process to join all tables in one table
 *
 * @param inputTableNamesWithId list of table names with a identifier column name
 * @param prefixName for the output table
 * @param datasource connection to the database
 *
 * @return
 */
String joinTables(JdbcDataSource datasource, Map inputTableNamesWithId, String outputTableName, boolean prefixWithTabName = false) throws Exception {
    try {
        debug "Executing Utility process to join tables in one"
        def columnKey
        def alias = "a"
        def leftQuery = ""
        def indexes = ""

        def columns = []

        inputTableNamesWithId.each { key, value ->
            if (alias == "a") {
                columnKey = "$alias.$value"
                // Whether or not the table name is add as prefix of the indicator in the new table
                if (prefixWithTabName) {
                    columns = datasource.getColumnNames(key).collect {
                        alias + ".$it AS ${key}_$it"
                    }
                } else {
                    columns = datasource.getColumnNames(key).collect {
                        alias + ".$it"
                    }
                }
                leftQuery += " FROM $key as $alias "
            } else {
                datasource.getColumnNames(key).forEach() { item ->
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
            indexes += "CREATE INDEX IF NOT EXISTS ${key}_ids ON $key ($value);"
            alias++
        }
        def columnsAsString = columns.join(",")
        datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
        ${indexes.toString()}
        CREATE TABLE $outputTableName AS SELECT $columnsAsString $leftQuery""")
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot join the tables", e)
    }
}

/**
 * An utility process to save several tables in a folder
 *
 *
 * @param datasource connection to the database
 * @param inputTableNames to be stored in the directory.
 * Note : A spatial table is saved in a flatgeobuffer file and the other in csv
 * @param delete true to delete the file is exist
 * @param directory folder to save the tables
 *
 * @return the directory where the tables are saved
 */
String saveTablesAsFiles(JdbcDataSource datasource, List inputTableNames, boolean delete = true, String directory) throws Exception {
    try {
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
                        (datasource.hasGeometryColumn(tableName) ? ".fgb" : ".csv")
                def table = datasource.getTable(tableName)
                if (table) {
                    table.save(fileToSave, delete)
                    debug "The table $tableName has been saved in file $fileToSave"
                }
            }
        }
        return directory
    } catch (java.sql.SQLException e) {
        throw new SQLException("Cannot save the tables", e)
    }
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

/**
 * Create the select projection and alias all columns
 * @param datasource
 * @param tableName
 * @param alias
 * @return
 */
static String aliasColumns(JdbcDataSource datasource, String tableName, String alias) {
    Collection columnNames = datasource.getColumnNames(tableName)
    return columnNames.inject([]) { result, iter ->
        result += "$alias.$iter"
    }.join(",")
}

/**
 * Create the select projection and alias all columns
 * @param datasource connection to the database
 * @param tableName table name
 * @param alias for the columns
 * @param exceptColumns columns to remove
 * @return
 */
static String aliasColumns(JdbcDataSource datasource, def tableName, def alias, def exceptColumns) {
    Collection columnNames = datasource.getColumnNames(tableName)
    columnNames.removeAll(exceptColumns)
    return columnNames.inject([]) { result, iter ->
        result += "$alias.$iter"
    }.join(",")
}

/**
 * An utility process to unioning two tables in one table
 *
 * @param tableA left table
 * @param tablelB right table
 *
 * @return a new table
 */
String unionTables(JdbcDataSource datasource, String tableA, String tablelB, String outputTableName) throws Exception {
    def columnsA = datasource.getColumnNames(tableA)
    def columnsB = datasource.getColumnNames(tablelB)
    TreeMap colsA = new TreeMap()
    columnsA.each {col -> colsA.put(col,col)}
    columnsB.each { e ->  if(!columnsA.contains(e)) colsA.put(e,null)}

    TreeMap colsB = new TreeMap()
    columnsB.each {col -> colsB.put(col,col)}
    columnsA.each { e ->  if(!columnsB.contains(e)) colsB.put(e,null)}

    datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
    CREATE TABLE $outputTableName as select ${colsA.collect {it.value +" as "+ it.key}.join(",")} from $tableA 
    union all select ${colsB.collect { it.value +" as "+ it.key}.join(",")} from $tablelB""")
    return outputTableName
}

/**
 * An utility process to tranform overalaping geometries in holes
 *
 * @param tableToProcess the table that contains overlaping geometry
 * @param columnId row identifier
 * @param outputTableName output table name
 *
 * @return a new table where the overlaps are converted to holes
 */
String withinToHoles(JdbcDataSource datasource, String tableToProcess, String columnId, String outputTableName) throws Exception {
    String holes = postfix("holes")
    datasource.createSpatialIndex(tableToProcess)
    datasource.createIndex(tableToProcess, columnId)
    datasource.execute("""
    DROP TABLE IF EXISTS $holes;
    CREATE TABLE  $holes as 
    SELECT a.$columnId, ST_BUFFER(ST_MAKEPOLYGON(ST_EXTERIORRING(a.the_geom), ST_ToMultiLine(ST_ACCUM(b.the_geom))), 0)  as the_geom
    FROM $tableToProcess as a, $tableToProcess as b where a.the_geom && b.the_geom and a.$columnId!=b.$columnId and
    st_contains(a.the_geom, b.the_geom) group by a.$columnId;
    """)
    def columns = datasource.getColumnNames(tableToProcess)
    columns.remove("THE_GEOM")
    columns.remove(columnId.toUpperCase())
    datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
    CREATE TABLE $outputTableName as
    SELECT a.$columnId, a.the_geom ${columns?",b."+columns.join(",b."):""} FROM $holes as a left join $tableToProcess as  b 
    on (a.$columnId=b.$columnId)
    union all 
    select $columnId, THE_GEOM ${columns?","+columns.join(","):""} from $tableToProcess where
    $columnId not in (select $columnId from $holes);
    DROP TABLE IF EXISTS $holes;""")
    return outputTableName
}

/**
 * An utility process to remove overalaping geometries using the min area
 *
 * @param tableToProcess the table to process
 * @param columnId row identifier
 * @param outputTableName output table name
 *
 * @return a new table where the overlaps are removed
 */
String removeOverlaps(JdbcDataSource datasource, String tableToProcess, String columnId, String outputTableName) throws Exception {
    String overlaps = postfix("overlaps")
    datasource.createSpatialIndex(tableToProcess)
    datasource.createIndex(tableToProcess, columnId)
    datasource.execute("""
    DROP TABLE IF EXISTS $overlaps;
    CREATE TABLE  $overlaps as 
    SELECT a.$columnId, ST_DIFFERENCE(a.the_geom, st_buffer(ST_ACCUM(b.the_geom), 0))  as the_geom, a.* EXCEPT($columnId, THE_GEOM)
    FROM $tableToProcess as a, $tableToProcess as b where a.the_geom && b.the_geom and a.$columnId!=b.$columnId and
    st_overlaps(a.the_geom, b.the_geom) and st_area(a.the_geom) > st_area(b.the_geom) group by a.$columnId;""")

    def columns = datasource.getColumnNames(tableToProcess)
    columns.remove("THE_GEOM")
    columns.remove(columnId.toUpperCase())

    datasource.execute("""
    DROP TABLE IF EXISTS $outputTableName;
    CREATE TABLE $outputTableName
            as select * from $overlaps union all
    select $columnId, the_geom ${columns?","+columns.join(","):""} from  $tableToProcess  where $columnId not in
    (select $columnId from $overlaps);
    drop table if exists $overlaps;
    """)
}


/**
 * Cast the input value as Float otherwise return NULL
 * @param value input value
 * @return casted value
 */
Float asFloat(def value){
    try {
        return value as Float
    }catch (Exception ex){
        return null
    }
}

/**
 * Cast the input value as Boolean otherwise return NULL
 * @param value input value
 * @return casted value
 */
Boolean asBoolean(def value){
    if(value==null){
        return null
    }
    try {
        return Boolean.valueOf(value)
    }catch (Exception ex){
        return null
    }
}


/**
 * Cast the input value as Integer otherwise return NULL
 * @param value input value
 * @return casted value
 */
Integer asInteger(def value){
    try {
        return value as Integer
    }catch (Exception ex){
        return null
    }
}

/**
 * Reduce the precision of a geometry to centimeter
 *
 * Note that a polygon can be split into a multipolygon and that a geometry smaller than the precision
 * can collapse into an empty geometry.
 *
 * @param geometry the geometry to reduce
 * @return the reduced geometry, with the same SRID
 */
Geometry reduceToCentimeter(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
        return geometry
    }
    Geometry reduced = GeometryPrecisionReducer.reduce(geometry, new PrecisionModel(100))
    reduced.setSRID(geometry.getSRID())
    return reduced
}


/**
 * Reduce the precision of the geometries of a table to the centimeter
 *
 * The input geometries must be in a metric coordinate reference system.
 * The output table has the same columns as the input table.
 *
 * @param datasource connection to the database
 * @param inputTable the table to process, with a geometry column named the_geom
 * @param columnId the name of the unique identifier column
 * @param outputTableName the name of the output table
 * @param minArea the polygons with an area lower than or equal to this value are removed
 * @return the name of the output table
 */
String reduceTableToCentimeter(JdbcDataSource datasource, String inputTable, String columnId, String outputTableName,
                               double minArea = 0) throws Exception {
    try {
        def columns = datasource.getColumnNames(inputTable)
        def geomColumn = columns.find { it.equalsIgnoreCase("THE_GEOM") }
        def idColumn = columns.find { it.equalsIgnoreCase(columnId) }
        if (!geomColumn || !idColumn) {
            throw new IllegalArgumentException("The table $inputTable must contain the columns the_geom and $columnId")
        }
        def srid = datasource.getSrid(inputTable)
        datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName AS SELECT * FROM $inputTable WHERE 1 = 0;
            ALTER TABLE $outputTableName ALTER COLUMN "$geomColumn" SET DATA TYPE GEOMETRY${srid > 0 ? "(GEOMETRY, $srid)" : ""};
            """.toString())

        def nextId = (datasource.firstRow("SELECT MAX(\"$idColumn\") AS MAXID FROM $inputTable".toString()).MAXID ?: 0) as long
        def insert = """INSERT INTO $outputTableName (${columns.collect { "\"$it\"" }.join(", ")}) 
                        VALUES (${columns.collect { "?" }.join(", ")})""".toString()
        datasource.withBatch(1000, insert) { ps ->
            datasource.eachRow("SELECT * FROM $inputTable ORDER BY \"$idColumn\"".toString()) { row ->
                Geometry geom = reduceToCentimeter(row[geomColumn] as Geometry)
                if (geom == null || geom.isEmpty()) {
                    return
                }
                boolean firstGeom = true
                for (int i = 0; i < geom.getNumGeometries(); i++) {
                    Geometry subGeom = geom.getGeometryN(i)
                    if (subGeom.isEmpty() || (minArea > 0 && subGeom.getDimension() == 2 && subGeom.getArea() <= minArea)) {
                        continue
                    }
                    subGeom.setSRID(geom.getSRID())
                    def id = firstGeom ? row[idColumn] : ++nextId
                    ps.addBatch(columns.collect { column ->
                        column == geomColumn ? subGeom : (column == idColumn ? id : row[column])
                    })
                    firstGeom = false
                }
            }
        }
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot reduce the precision of the table $inputTable", e)
    }
}