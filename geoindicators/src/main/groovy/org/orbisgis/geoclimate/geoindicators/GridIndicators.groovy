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

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.prep.PreparedGeometry
import org.locationtech.jts.geom.prep.PreparedGeometryFactory
import org.locationtech.jts.operation.distance.IndexedFacetDistance
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.SQLException

@BaseScript Geoindicators geoindicators


/**
 * Disaggregate a set of population values to a grid
 * Update the input grid table to add new population columns
 * @param inputGridTableName the building table
 * @param inputPopulation the spatial unit that contains the population to distribute
 * @param inputPopulationColumns the list of the columns to disaggregate
 * @return the input Grid table with the new population columns
 *
 * @author Erwan Bocher, CNRS
 */
String gridPopulation(JdbcDataSource datasource, String gridTable, String populationTable, List populationColumns = []) throws Exception {
    //temporary tables
    def gridTable_pop_sum = postfix "grid_pop_sum"
    def gridTable_area_sum = postfix "grid_area_sum"
    def gridTable_pop = postfix gridTable
    try {
        def BASE_NAME = "grid_with_population"
        def ID_RSU = "id_grid"
        def ID_POP = "id_pop"

        debug "Computing grid population"

        // The name of the outputTableName is constructed
        def outputTableName = postfix BASE_NAME

        //Indexing table
        datasource.createSpatialIndex(gridTable, "the_geom")
        datasource.createSpatialIndex(populationTable, "the_geom")
        def popColumns = []
        def sum_popColumns = []
        if (populationColumns) {
            datasource.getColumnNames(populationTable).each { col ->
                if (!["the_geom", "id_pop"].contains(col.toLowerCase()
                ) && populationColumns.contains(col.toLowerCase())) {
                    popColumns << "b.$col"
                    sum_popColumns << "sum((a.area_rsu * $col)/b.sum_area_rsu) as $col"
                }
            }
        } else {
            throw new IllegalArgumentException("Please set a list one column that contain population data to be disaggregated")
        }

        //Filtering the grid to get only the geometries that intersect the population table

        datasource.execute("""
                drop table if exists $gridTable_pop;
                CREATE TABLE $gridTable_pop AS SELECT 
                ST_AREA(ST_INTERSECTION(a.the_geom, st_force2D(b.the_geom)))  as area_rsu, a.$ID_RSU, 
                b.id_pop, ${popColumns.join(",")} from
                $gridTable as a, $populationTable as b where a.the_geom && b.the_geom and
                st_intersects(a.the_geom, b.the_geom);
                create index on $gridTable_pop ($ID_RSU);
                create index on $gridTable_pop ($ID_POP);
            """)

        //Aggregate population values
        datasource.execute("""drop table if exists $gridTable_pop_sum, $gridTable_area_sum;
            create table $gridTable_area_sum as select id_pop, sum(area_rsu) as sum_area_rsu
            from $gridTable_pop group by $ID_POP;
            create index on $gridTable_area_sum($ID_POP);
            create table $gridTable_pop_sum 
            as select a.$ID_RSU, ${sum_popColumns.join(",")} 
            from $gridTable_pop as a, $gridTable_area_sum as b where a.$ID_POP=b.$ID_POP group by $ID_RSU;
            CREATE INDEX ON $gridTable_pop_sum ($ID_RSU);
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName AS SELECT a.*, ${popColumns.join(",")} from $gridTable a  
            LEFT JOIN $gridTable_pop_sum  b on a.$ID_RSU=b.$ID_RSU;""")

        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the population on the grid", e)
    } finally {
        datasource.execute(" drop table if exists $gridTable_pop,$gridTable_pop_sum, $gridTable_area_sum ;")
    }
}


/**
 * Create a multi-scale grid and aggregate the LCZ_PRIMARY indicators for each level of the grid.
 * For each level, the adjacent cells are preserved as well as the number of urban and natural cells.
 * To distinguish between LCZ cells with the same count per level, a weight is used to select only one LCZ type
 * corresponding to their potential impact on heat
 *
 * @param datasource connection to the database
 * @param grid_indicators a grid that contains for each cell the LCZ_PRIMARY
 * @param id_grid grid cell column identifier
 * @param nb_levels number of aggregate levels. Default is 1
 *
 * @return a the initial grid with all aggregated values by levels and the indexes (row, col) for each levels
 * @author Erwan Bocher (CNRS)
 */
String multiscaleLCZGrid(JdbcDataSource datasource, String grid_indicators, String id_grid, int nb_levels = 1) throws Exception {
    def tablesToDrop = []
    try {
        if (!grid_indicators) {
            throw new IllegalArgumentException("No grid_indicators table to aggregate the LCZ values")
        }
        if (nb_levels <= 0 || nb_levels >= 10) {
            throw new IllegalArgumentException("The number of levels to aggregate the LCZ values must be between 1 and 10")
        }
        def gridColumns = datasource.getColumnNames(grid_indicators)

        if (gridColumns.intersect(["LCZ_PRIMARY", "ID_ROW", "ID_COLUMN", id_grid]).size() == 0) {
            throw new IllegalArgumentException("The grid indicators table must contain the columns LCZ_PRIMARY, ID_ROW, $id_grid")
        }

        datasource.execute("""create index on $grid_indicators(id_row,id_col)""")
        ///First build the index levels for each cell of the input grid
        def grid_scaling_indices = postfix("grid_scaling_indices")
        def grid_levels_query = []
        int grid_offset = 3
        int offsetCol = 1
        for (int i in 1..nb_levels) {
            int level = Math.pow(grid_offset, i)
            grid_levels_query << " (CAST (ABS(ID_ROW-1)/${level}  AS INT)+1) AS ID_ROW_LOD_$i," +
                    "(CAST (ABS(ID_COL-1)/${level} AS INT)+$offsetCol-1) AS ID_COL_LOD_$i"
            offsetCol++
        }

        //Compute the indices for each levels and find the 8 adjacent cells
        datasource.execute("""DROP TABLE IF EXISTS $grid_scaling_indices;
    CREATE TABLE $grid_scaling_indices as SELECT * ${!grid_levels_query.isEmpty()?","+grid_levels_query.join(","):""},
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW+1 AND ID_COL=a.ID_COL) AS LCZ_PRIMARY_N,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW+1 AND ID_COL=a.ID_COL+1) AS LCZ_PRIMARY_NE,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW AND ID_COL=a.ID_COL+1) AS LCZ_PRIMARY_E,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW-1 AND ID_COL=a.ID_COL+1) AS LCZ_PRIMARY_SE,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW-1 AND ID_COL=a.ID_COL) AS LCZ_PRIMARY_S,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW-1 AND ID_COL=a.ID_COL-1) AS LCZ_PRIMARY_SW,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW AND ID_COL=a.ID_COL-1) AS LCZ_PRIMARY_W,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW+1 AND ID_COL=a.ID_COL-1) AS LCZ_PRIMARY_NW
    FROM 
    $grid_indicators as a;  """.toString())
        //Add LCZ_WARM count at this first level
        def lcz_warm_first_level = postfix("lcz_warm_first_level")
        datasource.execute("""DROP TABLE IF EXISTS $lcz_warm_first_level;
    CREATE TABLE $lcz_warm_first_level as SELECT *,
    (CASE WHEN LCZ_PRIMARY_N in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_NE in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_E in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_SE in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_S in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_SW in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_W in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END +
    CASE WHEN  LCZ_PRIMARY_NW in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END+
    CASE WHEN  LCZ_PRIMARY in (1,2,3,4,5,6,7,8,9,10,105) THEN 1 ELSE 0 END)  AS LCZ_WARM 
    FROM $grid_scaling_indices """.toString())


        def tableLevelToJoin = lcz_warm_first_level
        tablesToDrop << grid_scaling_indices
        tablesToDrop << lcz_warm_first_level

        //Process all level of details
        for (int i in 1..nb_levels) {
            //Index the level row and col
            datasource.execute("""
        CREATE INDEX IF NOT EXISTS ${grid_scaling_indices}_idx ON $grid_scaling_indices(id_row_lod_${i},id_col_lod_${i})""".toString())
            //First compute the number of cells by level of detail
            //Use the original grid to aggregate the data
            //A weight is used to select the LCZ value when the mode returns more than one possibility
            def lcz_count_lod = postfix("lcz_count_lod")
            tablesToDrop << lcz_count_lod
            datasource.execute(""" 
        DROP TABLE IF EXISTS $lcz_count_lod;
        CREATE TABLE $lcz_count_lod as
        SELECT  COUNT(*) FILTER (WHERE LCZ_PRIMARY IS NOT NULL) AS COUNT, LCZ_PRIMARY,
        ID_ROW_LOD_${i}, ID_COL_LOD_${i},
        CASE WHEN LCZ_PRIMARY=105 THEN 11
        WHEN LCZ_PRIMARY=107 THEN 12
        WHEN LCZ_PRIMARY=106 THEN 13
        WHEN LCZ_PRIMARY= 101 THEN 14
        WHEN LCZ_PRIMARY =102 THEN 15
        WHEN LCZ_PRIMARY IN (103,104) THEN 16
        ELSE LCZ_PRIMARY END AS weight_lcz
        FROM $grid_scaling_indices 
        WHERE LCZ_PRIMARY IS NOT NULL 
        GROUP BY ID_ROW_LOD_${i}, ID_COL_LOD_${i}, LCZ_PRIMARY;""".toString())

            //Select the LCZ values according the maximum number of cells and the weight
            //Note that we compute the number of cells for urban and cool LCZ
            def lcz_count_lod_mode = postfix("lcz_count_lod_mode")
            tablesToDrop << lcz_count_lod_mode
            datasource.execute(""" 
        CREATE INDEX ON $lcz_count_lod(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
        DROP TABLE IF EXISTS  $lcz_count_lod_mode;    
        CREATE TABLE $lcz_count_lod_mode as
        select distinct on (ID_ROW_LOD_${i}, ID_COL_LOD_${i}) *,
        (select sum(count) from  $lcz_count_lod where   
        LCZ_PRIMARY in (1,2,3,4,5,6,7,8,9,10,105) 
        and ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} and ID_COL_LOD_${i}= a.ID_COL_LOD_${i}) AS  LCZ_WARM_LOD_${i},
        (select sum(count) from  $lcz_count_lod where  
        LCZ_PRIMARY in (101,102,103,104,106,107) 
        and ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} and ID_COL_LOD_${i}= a.ID_COL_LOD_${i}) AS  LCZ_COOL_LOD_${i}
        from $lcz_count_lod as a
        order by count desc, ID_ROW_LOD_${i}, ID_COL_LOD_${i}, weight_lcz;""".toString())

            //Find the 8 adjacent cells for the current level
            def grid_lod_level_final = postfix("grid_lod_level_final")
            tablesToDrop << grid_lod_level_final
            datasource.execute("""
        CREATE INDEX on $lcz_count_lod_mode(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
        DROP TABLE IF EXISTS $grid_lod_level_final;
        CREATE TABLE $grid_lod_level_final as select * EXCEPT(LCZ_PRIMARY, COUNT, weight_lcz), LCZ_PRIMARY AS LCZ_PRIMARY_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}) AS LCZ_PRIMARY_N_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_PRIMARY_NE_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_PRIMARY_E_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_PRIMARY_SE_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}) AS LCZ_PRIMARY_S_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_PRIMARY_SW_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_PRIMARY_W_LOD_${i},
        (SELECT LCZ_PRIMARY FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_PRIMARY_NW_LOD_${i},
     
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}) AS LCZ_WARM_N_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_WARM_NE_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_WARM_E_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_WARM_SE_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}) AS LCZ_WARM_S_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_WARM_SW_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_WARM_W_LOD_${i},
        (SELECT LCZ_WARM_LOD_${i} FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_WARM_NW_LOD_${i}
     
         FROM  $lcz_count_lod_mode as a;  """.toString())

            tableLevelToJoin << grid_lod_level_final

            //Join the final grid level with the original grid
            def grid_level_join = postfix("grid_level_join")
            datasource.execute("""
        CREATE INDEX IF NOT EXISTS ${tableLevelToJoin}_idx ON $tableLevelToJoin (ID_ROW_LOD_${i}, ID_COL_LOD_${i});
        create index on $grid_lod_level_final(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
        DROP TABLE IF EXISTS $grid_level_join;
        CREATE TABLE $grid_level_join as 
        select a.* EXCEPT(ID_ROW_LOD_${i}, ID_COL_LOD_${i}),  
        b.* from $tableLevelToJoin as a,  $grid_lod_level_final as b 
        where a.ID_ROW_LOD_${i} = b.ID_ROW_LOD_${i} and a.ID_COL_LOD_${i}= b.ID_COL_LOD_${i}
        group by a.ID_ROW_LOD_${i}, a.ID_COL_LOD_${i} , a.id_grid;
        """.toString())
            tableLevelToJoin = grid_level_join
        }
        return tableLevelToJoin
    } catch (SQLException e) {
        throw new SQLException("Cannot aggregate the LCZ_PRIMARY indicators to new grid levels", e)
    } finally {
        datasource.dropTable(tablesToDrop)
    }
}


/**
 * Compute the distance from each grid cell to the edge of a polygon
 *
 * @param datasource a connection to the database
 * @param input_polygons a set of polygons
 * @param grid a regular grid
 * @param id_grid name of the unique identifier column for the cells of the grid
 * @author Erwan Bocher (CNRS)
 * TODO : convert this method as a function table in H2GIS
 */
String gridDistances(JdbcDataSource datasource, String input_polygons, String grid, String id_grid, boolean keep_geometry = true) throws Exception {
    if (!input_polygons) {
        throw new IllegalArgumentException("The input polygons cannot be null or empty")
    }
    if (!grid) {
        throw new IllegalArgumentException("The grid cannot be null or empty")
    }
    if (!id_grid) {
        throw new IllegalArgumentException("Please set the column name identifier for the grid cells")
    }
    try {
        int epsg = datasource.getSrid(grid)
        def outputTableName = postfix("grid_distances")

        if (keep_geometry) {
            datasource.execute(""" DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName (THE_GEOM GEOMETRY,$id_grid INT, DISTANCE FLOAT);
        """.toString())

            datasource.createSpatialIndex(grid)
            datasource.withBatch(100) { stmt ->
                datasource.eachRow("SELECT the_geom from $input_polygons".toString()) { row ->
                    Geometry geom = row.the_geom
                    if (geom) {
                        PreparedGeometry prepGEom = PreparedGeometryFactory.prepare(geom)
                        IndexedFacetDistance indexedFacetDistance = new IndexedFacetDistance(geom)
                        datasource.eachRow("""SELECT the_geom, ${id_grid} as id from $grid 
                where ST_GEOMFROMTEXT('${geom}',$epsg)  && the_geom """.toString()) { cell ->
                            Geometry cell_geom = cell.the_geom
                            if (prepGEom.intersects(cell_geom.getCentroid())) {
                                double distance = indexedFacetDistance.distance(cell_geom.getCentroid())
                                stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${cell_geom}',$epsg), ${cell.id},${distance})".toString()
                            }
                        }
                    }
                }
            }
        } else {
            datasource.execute(""" DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName ($id_grid INT, DISTANCE FLOAT);
        """.toString())
            datasource.createSpatialIndex(grid)
            datasource.withBatch(100) { stmt ->
                datasource.eachRow("SELECT the_geom from $input_polygons".toString()) { row ->
                    Geometry geom = row.the_geom
                    if (geom) {
                        PreparedGeometry prepGEom = PreparedGeometryFactory.prepare(geom)
                        IndexedFacetDistance indexedFacetDistance = new IndexedFacetDistance(geom)
                        datasource.eachRow("""SELECT the_geom, ${id_grid} as id from $grid 
                where ST_GEOMFROMTEXT('${geom}',$epsg)  && the_geom""".toString()) { cell ->
                            Geometry cell_geom = cell.the_geom.getCentroid()
                            if (prepGEom.intersects(cell_geom)) {
                                double distance = indexedFacetDistance.distance(cell_geom)
                                stmt.addBatch "insert into $outputTableName values(${cell.id},${distance})".toString()
                            }
                        }
                    }
                }
            }
        }
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the grid distances", e)
    }
}

/**
 * Format the grid_indicators table to the TARGET land schema
 *
 * List of TARGET columns :
 * FID – cell identifier, unique numerical number for each grid point (can start at 0 or 1)
 * roof – fractional roof planar area
 * road – fractional road planar area
 * watr – factional water planar area
 * conc – fractional concrete planar area
 * veg – fractional tree planar area
 * dry – fractional dry grass area
 * irr – fractional irrigated grass/low vegetation area
 * H – average building height (m)
 * W – average street width, distance between buildings (m)
 *
 * @param datasource input database
 * @param gridTable input grid_indicators
 * @param resolution grid resolution in meters
 * @param land_superposition_grid Map defining land superpositions (eg. high vegetation and low vegetation)
 * @return raw target grid
 *
 * @author Erwan Bocher, CNRS
 */
String formatGrid4Target(JdbcDataSource datasource, String gridTable, float resolution, Map superpositions) throws Exception{
    // Put to upper case the land type defined in the superposition map
    def superpositions_upper = [:]
    superpositions.each {key, values ->
        superpositions_upper[key.toUpperCase()] = values.collect { it.toUpperCase() }
    }

    //Format target landcover
    def grid_target = postfix("grid_target")
    try {
        datasource.execute("""
                            DROP TABLE IF EXISTS ${grid_target};
                            CREATE TABLE ${grid_target} as SELECT
                            THE_GEOM,
                            ID_COL, ID_ROW,
                            CAST(row_number() over(ORDER BY ID_ROW DESC) as integer) as "FID",
                            BUILDING_FRACTION ${if(superpositions_upper.values()[0].contains("BUILDING")){"+ ${superpositions.keySet()[0]}_BUILDING_FRACTION"}else{""}} AS "roof",
                            ROAD_FRACTION AS "road",
                            WATER_PERMANENT_FRACTION  AS "watr",
                            IMPERVIOUS_FRACTION + RAIL_FRACTION + UNDEFINED_FRACTION  AS "conc",
                            HIGH_VEGETATION_FRACTION  ${if(superpositions_upper.values()[0].contains("ROAD")){"+ ${superpositions.keySet()[0]}_ROAD_FRACTION"}else{""}}
                                ${if(superpositions_upper.values()[0].contains("WATER_PERMANENT")){"+ ${superpositions.keySet()[0]}_WATER_PERMANENT_FRACTION"}else{""}} 
                                ${if(superpositions_upper.values()[0].contains("WATER_INTERMITTENT")){"+ ${superpositions.keySet()[0]}_WATER_INTERMITTENT_FRACTION"}else{""}} 
                                ${if(superpositions_upper.values()[0].contains("IMPERVIOUS")){"+ ${superpositions.keySet()[0]}_IMPERVIOUS_FRACTION"}else{""}}        
                                ${if(superpositions_upper.values()[0].contains("RAIL")){"+ ${superpositions.keySet()[0]}_RAIL_FRACTION"}else{""}}        
                                ${if(superpositions_upper.values()[0].contains("LOW_VEGETATION")){"+ ${superpositions.keySet()[0]}_LOW_VEGETATION_FRACTION"}else{""}} AS "Veg",                
                            LOW_VEGETATION_FRACTION  AS "dry",
                            0  AS "irr",
                            AVG_HEIGHT_ROOF_AREA_WEIGHTED AS "H",                            
                            STREET_WIDTH AS "W"
                            FROM ${gridTable} 
                            """)

        return grid_target
    }catch (SQLException e){
        //We create an empty table
        datasource.execute("""CREATE TABLE $grid_target (FID INT, ID_COL INT, ID_ROW INT, THE_GEOM GEOMETRY,
        "roof" VARCHAR, "road" VARCHAR, "watr" VARCHAR, "conc" VARCHAR,
        "Veg" VARCHAR, "dry" VARCHAR, "irr" VARCHAR , "H" VARCHAR, "W" VARCHAR)""")
    }
}

/**
 *  Compute the number of warm LCZ on a sliding window
 *  The sliding window is defined by a step size from the center of the cells.
 *  The step size is an integer value defines the width and height (in terms of cells) of the window
 *  we are going to extract from an input grid
 *  @param datasource connection to the database
 *  @param grid_indicators input grid
 *  @param  window_size array of window steps. User can specify multiple window steps.
 *  @author Erwan Bocher (CNRS)
 */
String gridCountCellsWarm(JdbcDataSource datasource, String grid_indicators, List window_size) throws Exception {

    if (!grid_indicators) {
        throw new IllegalArgumentException("No grid_indicators table to compute the statistics")
    }

    if(!window_size){
        throw new IllegalArgumentException("Please provide at least one window size")
    }


    if(window_size.min()<1){
        throw new IllegalArgumentException("The window sizes must be greater or equal than 1 cell")
    }

    if(window_size.max()>=10){
        throw new IllegalArgumentException("The window sizes must be less or equal than 100 cells")
    }
    try {
        datasource.createIndex(grid_indicators, "id_grid")
        datasource.createIndex(grid_indicators, "id_row")
        datasource.createIndex(grid_indicators, "id_col")

        def queries =[:]
        window_size.toSet().sort().each {size->
          def table_size= postfix("warm_$size")
         queries.put(table_size, """DROP TABLE IF EXISTS ${table_size}; CREATE TABLE ${table_size} as select a.id_grid, COUNT(b.id_grid) as count_cells_${size},
        sum(CASE WHEN  b.LCZ_PRIMARY in (1,2,3,4,5,6,7,8,9,10,105)  THEN 1 ELSE 0 END) as count_warm_${size}
        from ${grid_indicators}  a, ${grid_indicators} b
        where
        a.id_grid != b.id_grid and (b.id_row between a.id_row-${size} and a.id_row+${size})
        and (b.id_col between a.id_col-${size} and a.id_col+${size})
        group by a.id_grid;""")
        }
        datasource.execute(queries.values().join("\n"))
        def grid_final = postfix("grid_indicators")
        def tableToJoin =[:]
        queries.keySet().each {it->
            tableToJoin.put(it,"id_grid")
        }
        Geoindicators.DataUtils.joinTables(datasource, tableToJoin, grid_final)
        datasource.dropTable(tableToJoin.keySet().toList())
        return grid_final
    }catch (SQLException e) {
        throw new SQLException("Cannot count the number of warm LCZ on a sliding window", e)
    }
}
