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
import org.locationtech.jts.operation.distance.IndexedFacetDistance
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators


@BaseScript Geoindicators geoindicators

/**
 * This method allows to compute a sprawl areas layer.
 * A sprawl geometry represents a continous areas of urban LCZs (1 to 10 plus 105)
 * @author Erwan Bocher (CNRS)
 * TODO: SPATIAL UNITS
 */
String compute_sprawl_areas(JdbcDataSource datasource, String grid_indicators, float fraction = 0.65, float distance = 100) {
    if (!grid_indicators) {
        error("No grid_indicators table to compute the sprawl areas layer")
        return
    }
    if (fraction <= 0) {
        error("Please set a fraction greater than 0")
        return
    }
    if (distance < 0) {
        error("Please set a fraction greater or equal than 0")
        return
    }
    if (datasource.getRowCount(grid_indicators) == 0) {
        error("No geometries to compute the sprawl areas layer")
        return
    }
    def gridCols = datasource.getColumnNames(grid_indicators)

    def lcz_columns_urban = ["LCZ_PRIMARY_1", "LCZ_PRIMARY_2", "LCZ_PRIMARY_3", "LCZ_PRIMARY_4", "LCZ_PRIMARY_5", "LCZ_PRIMARY_6", "LCZ_PRIMARY_7",
                             "LCZ_PRIMARY_8", "LCZ_PRIMARY_9", "LCZ_PRIMARY_10", "LCZ_PRIMARY_105"]
    def lcz_fractions = gridCols.intersect(lcz_columns_urban)

    if (lcz_fractions.size() > 0) {
        def outputTableName = postfix("sprawl_areas")
        def tmp_sprawl = postfix("sprawl_tmp")
        datasource.execute("""
        DROP TABLE IF EXISTS  $tmp_sprawl, $outputTableName;
        CREATE TABLE $tmp_sprawl as select st_buffer(st_buffer(the_geom,-0.1, 'quad_segs=2 endcap=flat
                     join=mitre mitre_limit=2'),0.1, 'quad_segs=2 endcap=flat
                     join=mitre mitre_limit=2') as the_geom  from 
        st_explode('(
        SELECT ST_UNION(st_removeholes(ST_UNION(ST_ACCUM(the_geom)))) 
        AS THE_GEOM FROM $grid_indicators where ${lcz_fractions.join("+")} >= $fraction
        )');
        CREATE TABLE $outputTableName as SELECT CAST((row_number() over()) as Integer) as id, st_removeholes(the_geom) as the_geom
        FROM
        ST_EXPLODE('(
        SELECT 
        st_buffer(st_union(st_accum(st_buffer(the_geom,$distance, ''quad_segs=2 endcap=flat
                     join=mitre mitre_limit=2''))),
                     -$distance, ''quad_segs=2 endcap=flat join=mitre mitre_limit=2'') as the_geom  
         FROM ST_EXPLODE(''$tmp_sprawl''))') where st_isempty(st_buffer(the_geom, -$distance))=false;
        DROP TABLE IF EXISTS $tmp_sprawl;
        """.toString())

        return outputTableName
    }
    error("No LCZ_PRIMARY columns to compute the sprawl areas layer")
    return
}


/**
 *
 * This method is compute the difference between an input layer of polygons and the bounding box
 * of the input layer.
 * This layer is used to perform the distances out of the sprawl areas
 * @author Erwan Bocher (CNRS)
 * TODO : SPATIAL UNITS
 */
String inverse_geometries(JdbcDataSource datasource, String input_polygons) {
    def outputTableName = postfix("inverse_geometries")
    def tmp_extent = postfix("tmp_extent")
    datasource.execute("""DROP TABLE IF EXISTS $tmp_extent, $outputTableName;
    CREATE TABLE $tmp_extent as SELECT ST_EXTENT(THE_GEOM) as the_geom FROM $input_polygons;
    CREATE TABLE $outputTableName as
    SELECT CAST((row_number() over()) as Integer) as id, the_geom
    FROM
    ST_EXPLODE('(
        select st_difference(a.the_geom, st_accum(b.the_geom)) as the_geom from $tmp_extent as a, $input_polygons
        as b)');        
    DROP TABLE IF EXISTS $tmp_extent;
    """.toString())
    return outputTableName
}


/**
 * This methods allows to extract the cool area geometries.
 * A cool area is continous geometry defined by vegetation and water fractions.
 *
 * @author Erwan Bocher (CNRS)
 * TODO: SPATIAL UNITS
 */
String sprawl_cool_areas(JdbcDataSource datasource, String sprawl, String grid_indicators, float fraction = 0.4,
                         float distance = 100) {
    if (!grid_indicators) {
        error("No grid_indicators table to compute the sprawl cool areas layer")
        return
    }
    def gridCols = datasource.getColumnNames(grid_indicators)

    def lcz_columns_urban = ["LCZ_PRIMARY_101", "LCZ_PRIMARY_102",
                             "LCZ_PRIMARY_103", "LCZ_PRIMARY_104", "LCZ_PRIMARY_107"]
    def lcz_fractions = gridCols.intersect(lcz_columns_urban)

    if (lcz_fractions.size() > 0) {
        datasource.createSpatialIndex(sprawl)
        datasource.createSpatialIndex(grid_indicators)
        def outputTableName = postfix("cool_areas")
        datasource.execute("""
        DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName as SELECT CAST((row_number() over()) as Integer) as id, st_removeholes(the_geom) as the_geom FROM ST_EXPLODE('(
        SELECT ST_UNION(ST_ACCUM(a.THE_GEOM)) AS THE_GEOM FROM $grid_indicators as a, $sprawl as b  where
        a.the_geom && b.the_geom and st_intersects(b.the_geom, ST_POINTONSURFACE(a.the_geom)) and
         a.${lcz_fractions.join("+ a.")} >= $fraction
        )') where st_isempty(st_buffer(st_convexhull(the_geom), -$distance))=false;
        """.toString())
        return outputTableName
    }
    error("No LCZ_PRIMARY columns to compute the cool areas")
    return
}


/**
 * @author Erwan Bocher (CNRS)
 *
 * TODO : spatial units
 */
String inverse_cool_areas(JdbcDataSource datasource, String inverse_sprawl, String cool_areas) {

    def outputTableName = postfix("inverse_cool_areas")
    def tmp_inverse = postfix("tmp_inverse")
    datasource.execute("""
    DROP TABLE IF EXISTS $tmp_inverse;
    CREATE TABLE $tmp_inverse as 
     SELECT st_union(st_accum(the_geom)) as the_geom
     from (select the_geom from $inverse_sprawl union all select the_geom from $cool_areas) """.toString())

    def tmp_extent = postfix("tmp_extent")
    datasource.execute("""DROP TABLE IF EXISTS $tmp_extent, $outputTableName;
    CREATE TABLE $tmp_extent as SELECT ST_EXTENT(THE_GEOM) as the_geom FROM $tmp_inverse;
    CREATE TABLE $outputTableName as
    SELECT CAST((row_number() over()) as Integer) as id, st_buffer(st_buffer(the_geom,-0.1, 'quad_segs=2 endcap=flat
                     join=mitre mitre_limit=2'),0.1, 'quad_segs=2 endcap=flat
                     join=mitre mitre_limit=2') as the_geom 
    FROM
    ST_EXPLODE('(
        select st_difference(a.the_geom, st_accum(b.the_geom)) as the_geom from $tmp_extent as a, $tmp_inverse
        as b)');        
    DROP TABLE IF EXISTS $tmp_extent;
    """.toString())

    return outputTableName
}

/**
 * @author Erwan Bocher (CNRS)
 *
 * TODO : GRID INDICATORS
 */
String grid_distances(JdbcDataSource datasource, String sprawl, String grid_indicators) {
    int epsg = datasource.getSrid(grid_indicators)
    def outputTableName = postfix("grid_sprawl_distance")

    datasource.execute(""" DROP TABLE IF EXISTS $outputTableName;
        CREATE TABLE $outputTableName ( THE_GEOM GEOMETRY,ID INT, DISTANCE FLOAT);
        """.toString())

    datasource.createSpatialIndex(sprawl)
    datasource.createSpatialIndex(grid_indicators)

    datasource.withBatch(100) { stmt ->
        datasource.eachRow("SELECT the_geom from $sprawl".toString()) { row ->
            Geometry geom = row.the_geom
            if (geom) {
                IndexedFacetDistance indexedFacetDistance = new IndexedFacetDistance(geom)
                datasource.eachRow("""SELECT the_geom, id_grid as id from $grid_indicators 
                where ST_GEOMFROMTEXT('${geom}',$epsg)  && the_geom and 
            st_intersects(ST_GEOMFROMTEXT('${geom}',$epsg) , ST_POINTONSURFACE(the_geom))""".toString()) { pointRow ->
                    Geometry cell = pointRow.the_geom
                    double distance = indexedFacetDistance.distance(cell.getCentroid())
                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${cell}',$epsg), ${pointRow.id},${distance})".toString()
                }
            }
        }
    }
    return outputTableName
}

/**
 * @author Erwan Bocher (CNRS)
 *
 * TODO : grid indicators
 */
String scaling_grid(JdbcDataSource datasource, String grid_indicators, int nb_levels = 2) {

    if (!grid_indicators) {
        error("No grid_indicators table to aggregate the LCZ fraction")
        return
    }
    if (nb_levels <= 0 || nb_levels >= 10) {
        error("The number of levels to aggregate the LCZ fractions must be between 1 and 10")
        return
    }
    datasource.execute("""create index on $grid_indicators(id_row,id_col)""".toString())
    ///First build the index levels for each cell of the input grid
    def grid_scaling_indices = postfix("grid_scaling_indices")
    def grid_levels_query = []
    int grid_offset = 3
    int offsetCol = 0
    for (int i in 1..nb_levels) {
        int level = Math.pow(grid_offset, i)
        grid_levels_query << " (CAST (ABS(ID_ROW-1)/${level}  AS INT)+1) AS ID_ROW_LOD_$i," +
                "(CAST (ABS(ID_COL-1)/${level} AS INT)+$offsetCol) AS ID_COL_LOD_$i"
        offsetCol++
    }

    //Compute the indice for each levels and find the 8 adjacent cells
    datasource.execute("""DROP TABLE IF EXISTS $grid_scaling_indices;
    CREATE TABLE $grid_scaling_indices as SELECT the_geom,
    ID_GRID, ID_ROW as ID_ROW_LOD_0, ID_COL as ID_COL_LOD_0, ${grid_levels_query.join(",")},
    LCZ_PRIMARY AS LCZ_PRIMARY_LOD_0,   
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW+1 AND ID_COL=a.ID_COL) AS LCZ_PRIMARY_N_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW+1 AND ID_COL+1=a.ID_COL) AS LCZ_PRIMARY_NE_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW AND ID_COL=a.ID_COL+1) AS LCZ_PRIMARY_E_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW-1 AND ID_COL=a.ID_COL+1) AS LCZ_PRIMARY_SE_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW-1 AND ID_COL=a.ID_COL) AS LCZ_PRIMARY_S_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW-1 AND ID_COL=a.ID_COL-1) AS LCZ_PRIMARY_SW_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW AND ID_COL=a.ID_COL-1) AS LCZ_PRIMARY_W_LOD_0,
    (SELECT LCZ_PRIMARY FROM $grid_indicators WHERE ID_ROW = a.ID_ROW+1 AND ID_COL=a.ID_COL-1) AS LCZ_PRIMARY_NW_LOD_0 FROM 
    $grid_indicators as a;  """.toString())

    datasource.save(grid_scaling_indices, "/tmp/grid_lod_0.geojson", true)

    def tablesToDrop =[]
    def tablesLevelToJoin =[]
    tablesToDrop<<grid_scaling_indices

    //Process all level of details
    for (int i in 1..nb_levels) {
        //Index the level row and col
        datasource.execute("""create index on $grid_scaling_indices(id_row_lod_${i},id_col_lod_${i})""".toString())
        //First compute the number of cells by level of detail
        //Use the original grid to aggregate the data
        //A weight is used to select the LCZ value when the mode returns more than one possibility
        def lcz_count_lod = postfix("lcz_count_lod")
        tablesToDrop<<lcz_count_lod
        datasource.execute(""" 
    drop table if exists $lcz_count_lod;
    CREATE TABLE $lcz_count_lod as
    select  COUNT(*) FILTER (WHERE LCZ_PRIMARY_LOD_0 IS NOT NULL) AS COUNT, LCZ_PRIMARY_LOD_0,
    ID_ROW_LOD_${i}, ID_COL_LOD_${i},
    CASE WHEN LCZ_PRIMARY_LOD_0=105 THEN 11
    WHEN LCZ_PRIMARY_LOD_0=107 THEN 12
    WHEN LCZ_PRIMARY_LOD_0=106 THEN 13
    WHEN LCZ_PRIMARY_LOD_0= 101 THEN 14
    WHEN LCZ_PRIMARY_LOD_0 =102 THEN 15
    WHEN LCZ_PRIMARY_LOD_0 IN (103,104) THEN 16
    ELSE LCZ_PRIMARY_LOD_0 END AS weight_lcz,
    from $grid_scaling_indices 
    WHERE LCZ_PRIMARY_LOD_0 IS NOT NULL 
    group by ID_ROW_LOD_${i}, ID_COL_LOD_${i}, LCZ_PRIMARY_LOD_0;""".toString())

        //Select the LCZ values according the maximum number of cells and the weight
        //Note that we compute the number of cells for urban and cool LCZ
        def lcz_count_lod_mode = postfix("lcz_count_lod_mode")
        tablesToDrop<<lcz_count_lod_mode
        datasource.execute(""" 
    create index on $lcz_count_lod(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
    DROP TABLE IF EXISTS  $lcz_count_lod_mode;    
    CREATE TABLE $lcz_count_lod_mode as
    select distinct on (ID_ROW_LOD_${i}, ID_COL_LOD_${i}) *,
    (select sum(count) from  $lcz_count_lod where   
    LCZ_PRIMARY_LOD_0 in (1,2,3,4,5,6,7,8,9,10,105) 
    and ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} and ID_COL_LOD_${i}= a.ID_COL_LOD_${i}) AS  LCZ_PRIMARY_URBAN_LOD_${i},
    (select sum(count) from  $lcz_count_lod where  
    LCZ_PRIMARY_LOD_0 in (101,102,103,104,106,107) 
    and ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} and ID_COL_LOD_${i}= a.ID_COL_LOD_${i}) AS  LCZ_PRIMARY_COOL_LOD_${i},
    from $lcz_count_lod as a
    order by ID_ROW_LOD_${i}, ID_COL_LOD_${i}, weight_lcz desc, count asc;""".toString())

        //Find the 8 adjacent cells for the current level
        def grid_lod_level_final = postfix("grid_lod_level_final")
        tablesToDrop<<grid_lod_level_final
        datasource.execute("""
    CREATE INDEX on $lcz_count_lod_mode(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
    DROP TABLE IF EXISTS $grid_lod_level_final;
    CREATE TABLE $grid_lod_level_final as select *, 
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}) AS LCZ_PRIMARY_N_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}+1=a.ID_COL_LOD_${i}) AS LCZ_PRIMARY_NE_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_PRIMARY_E_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}+1) AS LCZ_PRIMARY_SE_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}) AS LCZ_PRIMARY_S_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}-1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_PRIMARY_SW_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i} AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_PRIMARY_W_${i},
    (SELECT LCZ_PRIMARY_LOD_0 FROM $lcz_count_lod_mode WHERE ID_ROW_LOD_${i} = a.ID_ROW_LOD_${i}+1 AND ID_COL_LOD_${i}=a.ID_COL_LOD_${i}-1) AS LCZ_PRIMARY_NW_${i} FROM 
    $lcz_count_lod_mode as a;  """.toString())

        tablesLevelToJoin<<grid_lod_level_final

        def grid_lod = postfix("grid_lod")
        tablesToDrop<<grid_lod
    datasource.execute("""
    create index on $grid_lod_level_final(ID_ROW_LOD_${i}, ID_COL_LOD_${i});
    DROP TABLE IF EXIsTS $grid_lod;
    CREATE TABLE $grid_lod AS 
    SELECT ST_UNION(ST_ACCUM(THE_GEOM)) AS THE_GEOM, B.* 
    from $grid_scaling_indices as a, $grid_lod_level_final as b 
    where a.ID_ROW_LOD_${i} = b.ID_ROW_LOD_${i} and a.ID_COL_LOD_${i}= b.ID_COL_LOD_${i}
    group by a.ID_ROW_LOD_${i}, a.ID_COL_LOD_${i};
    """.toString())

        datasource.save(grid_lod, "/tmp/grid_lod_${i}.geojson", true)

    }

    datasource.dropTable(tablesToDrop)

    return null

}


/**
 * @author Erwan Bocher (CNRS)
 *
 * TODO: spatial units
 */
String buffering_cool_areas(JdbcDataSource datasource, String rsu_lcz, String grid_indicators) {

    float distance = 10
    def outputTable = postfix("buffering_cool_areas")
    def merging_lcz = postfix("merging_lcz")
    datasource.execute("""
DROP TABLE IF EXISTS $merging_lcz;
CREATE TABLE $merging_lcz as 
select  CAST((row_number() over()) as Integer) as id,  the_geom from ST_EXPLODE('(
select st_union(st_accum(st_buffer(THE_GEOM, 0.1))) as the_geom FROM $rsu_lcz where  LCZ_PRIMARY IN (101, 102, 103,104, 107))')
""".toString())

    distance = 100

    datasource.execute("""
        DROP TABLE IF EXISTS $outputTable;
        CREATE TABLE $outputTable as  
select  st_buffer(the_geom,  -$distance,'quad_segs=2 endcap=flat
                     join=mitre mitre_limit=2') as the_geom from ST_EXPLODE('(
 select st_union(st_accum(st_buffer(the_geom, $distance, ''quad_segs=2 endcap=flat join=mitre mitre_limit=2'')))  as the_geom from  $merging_lcz)') as foo""".toString())

    def sprawl_layer = postfix("sprawl_layer")
    datasource.execute("""
       DROP TABLE IF EXISTS $sprawl_layer;
       CREATE TABLE $sprawl_layer as 
       
       """.toString())

    return outputTable
}