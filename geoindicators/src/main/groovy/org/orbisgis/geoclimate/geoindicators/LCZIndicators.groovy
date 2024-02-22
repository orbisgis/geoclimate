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
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators


@BaseScript Geoindicators geoindicators



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