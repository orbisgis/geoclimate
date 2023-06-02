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
 * Disaggregate a set of population values to a grid
 * Update the input grid table to add new population columns
 * @param inputGridTableName the building table
 * @param inputPopulation the spatial unit that contains the population to distribute
 * @param inputPopulationColumns the list of the columns to disaggregate
 * @return the input Grid table with the new population columns
 *
 * @author Erwan Bocher, CNRS
 */
String gridPopulation(JdbcDataSource datasource, String gridTable, String populationTable, List populationColumns = []) {
    def BASE_NAME = "grid_with_population"
    def ID_RSU = "id_grid"
    def ID_POP = "id_pop"

    debug "Computing grid population"

    // The name of the outputTableName is constructed
    def outputTableName = postfix BASE_NAME

    //Indexing table
    datasource.createSpatialIndex(gridTable,"the_geom")
    datasource.createSpatialIndex(populationTable,"the_geom")
    def popColumns = []
    def sum_popColumns = []
    if (populationColumns) {
        datasource."$populationTable".getColumns().each { col ->
            if (!["the_geom", "id_pop"].contains(col.toLowerCase()
            ) && populationColumns.contains(col.toLowerCase())) {
                popColumns << "b.$col"
                sum_popColumns << "sum((a.area_rsu * $col)/b.sum_area_rsu) as $col"
            }
        }
    } else {
        warn "Please set a list one column that contain population data to be disaggregated"
        return
    }

    //Filtering the grid to get only the geometries that intersect the population table
    def gridTable_pop = postfix gridTable
    datasource.execute("""
                drop table if exists $gridTable_pop;
                CREATE TABLE $gridTable_pop AS SELECT (ST_AREA(ST_INTERSECTION(a.the_geom, st_force2D(b.the_geom))))  as area_rsu, a.$ID_RSU, 
                b.id_pop, ${popColumns.join(",")} from
                $gridTable as a, $populationTable as b where a.the_geom && b.the_geom and
                st_intersects(a.the_geom, b.the_geom);
                create index on $gridTable_pop ($ID_RSU);
                create index on $gridTable_pop ($ID_POP);
            """.toString())

    def gridTable_pop_sum = postfix "grid_pop_sum"
    def gridTable_area_sum = postfix "grid_area_sum"
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
            LEFT JOIN $gridTable_pop_sum  b on a.$ID_RSU=b.$ID_RSU;
            drop table if exists $gridTable_pop,$gridTable_pop_sum, $gridTable_area_sum ;""".toString())

    return outputTableName
}