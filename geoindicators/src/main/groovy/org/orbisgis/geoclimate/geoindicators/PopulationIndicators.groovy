package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess


@BaseScript Geoindicators geoindicators



/**
 * Compute an approximate value of the the number of inhabitants by building
 * @return
 */
IProcess buildingPopulation() {
    return create {
        title "Compute the number of inhabitants for each building"
        id "buildingPopulation"
        inputs inputBuildingTableName: String, inputPopulationTableName: String, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { inputBuildingTableName, inputPopulationTableName, prefixName, datasource ->

            def BASE_NAME = "building_population"

            def ID_BUILDING = "id_build"
            def ID_POP = "id_pop"

            debug "Computing building population"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASE_NAME

            //Indexing table
            datasource."$inputBuildingTableName".the_geom.createSpatialIndex()
            datasource."$inputPopulationTableName".the_geom.createSpatialIndex()

            //Filtering the building to get only residential and intersect it with the pop grid
            def inputBuildingTableName_pop = postfix inputBuildingTableName
            datasource.execute("""
                drop table if exists $inputBuildingTableName_pop;
                CREATE TABLE $inputBuildingTableName_pop AS SELECT ST_AREA(ST_INTERSECTION(a.the_geom, b.the_geom)) as area_building, a.id_build, 
                a.NB_LEV, b.id_pop, b.pop, st_area(b.the_geom) as cell_area from
                $inputBuildingTableName as a, inputPopulationTableName as b where a.the_geom && b.the_geom and
                st_intersect(a.the_geom, b.the_geom) and a.main_use='residential';
                create index on $inputBuildingTableName_pop (id_build);
            """)

            //Aggregate pop
            datasource.execute("""drop table if exists $outputTableName;
            create table $outputTableName 
            as select a.id_build, sum((area_building*NB_LEV * pop)/cell_area) as pop
            from $inputBuildingTableName_pop group by id_build;
            drop table if exists $inputBuildingTableName_pop;""")

            [outputTableName: outputTableName]
        }
    }
}

