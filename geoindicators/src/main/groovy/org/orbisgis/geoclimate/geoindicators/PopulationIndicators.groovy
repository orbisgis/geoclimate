package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess


@BaseScript Geoindicators geoindicators


/**
 * Compute an approximate value of the the number of inhabitants by building
 * Update the input building table to add new field pop that contains the estimated
 * value of population
 * @param inputBuildingTableName the building table
 * @param inputPopulationTableName the spatial unit that contains the population to distribute
 * @return the input building table with a population column
 *
 * @author Erwan Bocher, CNRS
 */
IProcess buildingPopulation() {
    return create {
        title "Compute the number of inhabitants for each building"
        id "buildingPopulation"
        inputs inputBuildingTableName: String, inputPopulationTableName: String,  datasource: JdbcDataSource
        outputs buildingTableName: String
        run { inputBuildingTableName, inputPopulationTableName, datasource ->
            def BASE_NAME = "building_with_population"
            def ID_BUILDING = "id_build"
            def ID_POP = "id_pop"

            debug "Computing building population"

            // The name of the outputTableName is constructed
            def outputTableName = postfix BASE_NAME

            //Indexing table
            datasource."$inputBuildingTableName".the_geom.createSpatialIndex()
            datasource."$inputPopulationTableName".the_geom.createSpatialIndex()

            //Filtering the building to get only residential and intersect it with the pop grid
            def inputBuildingTableName_pop = postfix inputBuildingTableName
            datasource.execute("""
                drop table if exists $inputBuildingTableName_pop;
                CREATE TABLE $inputBuildingTableName_pop AS SELECT (ST_AREA(ST_INTERSECTION(a.the_geom, st_force2D(b.the_geom)))*a.NB_LEV)  as area_building, a.$ID_BUILDING, 
                 b.$ID_POP, b.pop, st_area(b.the_geom) as cell_area from
                $inputBuildingTableName as a, $inputPopulationTableName as b where a.the_geom && b.the_geom and
                st_intersects(a.the_geom, b.the_geom) and a.main_use='residential';
                create index on $inputBuildingTableName_pop ($ID_BUILDING);
                create index on $inputBuildingTableName_pop ($ID_POP);
            """.toString())

            def inputBuildingTableName_pop_sum = postfix "building_pop_sum"
            def inputBuildingTableName_area_sum = postfix "building_area_sum"
            //Aggregate pop
            datasource.execute("""drop table if exists $inputBuildingTableName_pop_sum, $inputBuildingTableName_area_sum;
            create table $inputBuildingTableName_area_sum as select id_pop, sum(area_building) as sum_area_building
            from $inputBuildingTableName_pop group by $ID_POP;
            create index on $inputBuildingTableName_area_sum($ID_POP);
            create table $inputBuildingTableName_pop_sum 
            as select a.$ID_BUILDING, sum((a.area_building * pop)/b.sum_area_building) as pop
            from $inputBuildingTableName_pop as a, $inputBuildingTableName_area_sum as b where a.$ID_POP=b.$ID_POP group by $ID_BUILDING;
            CREATE INDEX ON $inputBuildingTableName_pop_sum ($ID_BUILDING);
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName AS SELECT a.*, b.pop from $inputBuildingTableName a  
            LEFT JOIN $inputBuildingTableName_pop_sum  b on a.$ID_BUILDING=b.$ID_BUILDING;
            drop table if exists $inputBuildingTableName_pop,$inputBuildingTableName_pop_sum, $inputBuildingTableName_area_sum ;""".toString())

            [buildingTableName: outputTableName]
        }
    }
}

