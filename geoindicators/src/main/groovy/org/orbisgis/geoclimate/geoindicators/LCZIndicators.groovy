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
 */
String compute_sprawl_areas(JdbcDataSource datasource, String grid_indicators, float fraction = 0.65, float distance=100) {
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
 */
String sprawl_cool_areas(JdbcDataSource datasource, String sprawl, String grid_indicators, float fraction = 0.4,
                         float distance =100 ) {
    if (!grid_indicators) {
        error("No grid_indicators table to compute the sprawl cool areas layer")
        return
    }
    def gridCols = datasource.getColumnNames(grid_indicators)

    def lcz_columns_urban = ["LCZ_PRIMARY_101", "LCZ_PRIMARY_102",
                             "LCZ_PRIMARY_103", "LCZ_PRIMARY_104","LCZ_PRIMARY_107"]
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
 */
String scaling_grid(JdbcDataSource datasource, String grid_indicators) {

    if (!grid_indicators) {
        error("No grid_indicators table to aggregate the LCZ fraction")
        return
    }
    def gridCols = datasource.getColumnNames(grid_indicators)

    def lcz_columns_cool_areas = ["LCZ_PRIMARY_101", "LCZ_PRIMARY_102",
                             "LCZ_PRIMARY_103", "LCZ_PRIMARY_104","LCZ_PRIMARY_107"]
    def lcz_fractions = gridCols.intersect(lcz_columns_cool_areas)

    def grid_scaling_indices = postfix("grid_scaling_indices")
    def grid_fraction_lod_1 = postfix("grid_fraction_lod_1")
    def grid_fraction_lod_2 = postfix("grid_fraction_lod_2")
    datasource.execute("""DROP TABLE IF EXISTS $grid_scaling_indices,$grid_fraction_lod_1,$grid_fraction_lod_2;
    CREATE TABLE $grid_scaling_indices as SELECT the_geom,
    ID_GRID, ID_COL as ID_COL_LOD_0, ID_ROW as ID_ROW_LOD_0, (CAST (ID_COL/3 AS INT)+1) AS ID_COL_LOD_1,
    (CAST (ID_ROW/3 AS INT)+1) AS ID_ROW_LOD_1,
    (CAST (ID_COL/9 AS INT)+1) AS ID_COL_LOD_2,
    (CAST (ID_ROW/9 AS INT)+1) AS ID_ROW_LOD_2,
    ${lcz_fractions.join(",")}, ${lcz_fractions.join("+")} AS SUM_LCZ_COOL FROM $grid_indicators;    
    CREATE TABLE $grid_fraction_lod_1 as SELECT ID_COL_LOD_1, ID_ROW_LOD_1, SUM(SUM_LCZ_COOL) AS LOD_1_SUM_LCZ_COOL, 
    ST_UNION(ST_ACCUM(the_geom)) as the_geom, count(*) as count
    FROM $grid_scaling_indices GROUP BY  ID_COL_LOD_1, ID_ROW_LOD_1;
    CREATE TABLE $grid_fraction_lod_2 as SELECT ID_COL_LOD_2, ID_ROW_LOD_2, SUM(SUM_LCZ_COOL) AS LOD_2_SUM_LCZ_COOL,
    ST_UNION(ST_ACCUM(the_geom)) as the_geom,count(*) as count
    FROM $grid_scaling_indices GROUP BY  ID_COL_LOD_2, ID_ROW_LOD_2;
    """.toString())
    datasource.createIndex(grid_scaling_indices, "ID_COL_LOD_1")
    datasource.createIndex(grid_scaling_indices, "ID_ROW_LOD_1")

    datasource.save(grid_fraction_lod_1, "/tmp/grid_lod1.geojson", true)
    datasource.save(grid_fraction_lod_2, "/tmp/grid_lod2.geojson", true)

    def grid_fractions = postfix("grid_fractions")

    //"neighbors"

    /*int level =1
    //Collect neighbors

     datasource.execute( """
DROP TABLE IF EXISTS neighbors;
create table neighbors as
        SELECT the_geom, LOD_${level}_SUM_LCZ_COOL as  LOD_${level}_LCZ_COOL_2 from $grid_fraction_lod_1
            where ID_COL_LOD_$level=ID_COL_LOD_$level AND ID_ROW_LOD_$level=ID_ROW_LOD_$level-1""".toString())


    datasource.save("neighbors", "/tmp/neighbors.geojson", true)*/



    return null

}


/**
 * @author Erwan Bocher (CNRS)
 */
String buffering_cool_areas(JdbcDataSource datasource, String rsu_lcz, String grid_indicators) {

    float  distance = 10
    def outputTable  = postfix("buffering_cool_areas")
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