package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.process.api.IProcess

@BaseScript Geoindicators geoindicators


/**
 * This process is used to format a population table given by the user
 *
 * @param populationTable name of the population table
 * @param populationColumns the list of the columns to keep
 * @param zoneTable name of a zone table to limit the population table
 * @param datasource a connexion to the database
 */
IProcess formatPopulationTable() {
    return create {
        title "Format the population table"
        inputs populationTable: String, populationColumns:[],
                zoneTable:"",
                datasource: JdbcDataSource
        outputs populationTable: String
        run { populationTable, populationColumns, zoneTable, datasource ->

            def tablePopulation_tmp = postfix(populationTable)

            if(zoneTable){
            datasource.execute("""
                CREATE SPATIAL INDEX ON $populationTable(the_geom);
                CREATE SPATIAL INDEX ON $zoneTable(the_geom);
                DROP TABLE IF EXISTS $tablePopulation_tmp ;
                CREATE TABLE $tablePopulation_tmp AS SELECT ROWNUM() as id_pop, ST_MAKEVALID(a.the_geom) as the_geom , 
                ${populationColumns.join(",")} from $populationTable as a ,$zoneTable as b where a.the_geom && b.the_geom 
                and st_intersects(a.the_geom, b.the_geom) ;
                """.toString())
            }
            else{
                datasource.execute("""
                DROP TABLE IF EXISTS $tablePopulation_tmp ;
                CREATE TABLE $tablePopulation_tmp AS SELECT rownum() as id_pop, ST_MAKEVALID(the_geom) as the_geom , 
                ${populationColumns.join(",")} from $populationTable ;
                ALTER TABLE $tablePopulation_tmp rename to $tablePopulationName;
                """.toString())
            }
            [populationTable: tablePopulation_tmp]
        }
    }
}

/**
 * This process is used to aggregate population data at multiscales
 *
 * @param inputPopulationTableName name of the population table
 * @param inputPopulationColumns the list of the columns to keep
 * @param inputBuildingTableName name of the building table to distribute the population columns
 * @param inputRsuTableName name of the RSU table to distribute the population columns
 * @param inputGridTableName name of the GRID table to distribute the population columns
 * @param datasource a connexion to the database that contains the input data
 */
IProcess multiScalePopulation() {
    return create {
        title "Process to distribute a set of population values at multiscales"
        inputs populationTable : String , populationColumns: [],
                buildingTable: String, rsuTable:"", gridPopTable: "",
                datasource: JdbcDataSource
        outputs buildingPopTable: String, buildingRsuPopTable: String, rsuPopTable: String, gridPopTable: String
        run { populationTable, populationColumns, buildingTable, rsuTable, gridTable, datasource ->
            if(populationTable && populationColumns){
                def prefixName = "pop"
                def buildingPopTable=""
                def buildingRsuPopTable=""
                def rsuPopTable=""
                def gridPopTable=""
                if(buildingTable){
                    IProcess process = Geoindicators.BuildingIndicators.buildingPopulation()
                    if(process.execute(inputBuildingTableName: buildingTable,
                            inputPopulationTableName:populationTable , inputPopulationColumns: populationColumns,
                            datasource: datasource)) {
                        buildingPopTable = process.results.buildingTableName
                        if( rsuTable){
                            //Let's aggregate all population indicators at RSU scale from buildings
                            def sum_popColumns = []
                            def popColumns =[]
                            populationColumns.each { col ->
                                sum_popColumns << "sum($col) as $col"
                                popColumns<<"b.${col}"
                            }
                            def buildingPopRSU_tmp = postfix("building_rsu_pop_tmp")
                            def buildingPopRSU = postfix("building_rsu_pop")
                            datasource.execute("""DROP TABLE IF EXISTS $buildingPopRSU_tmp, $buildingPopRSU;
                            CREATE TABLE $buildingPopRSU_tmp AS SELECT ${sum_popColumns.join(",")}, ID_RSU FROM ${buildingPopTable} GROUP BY ID_RSU;
                            CREATE INDEX ON $buildingPopRSU_tmp (ID_RSU);
                            CREATE TABLE $buildingPopRSU AS SELECT a.*, ${popColumns.join(",")} from $rsuTable a  
                            LEFT JOIN $buildingPopRSU_tmp  b on a.ID_RSU=b.ID_RSU;
                            DROP TABLE IF EXISTS $buildingPopRSU_tmp;
                            """.toString())
                            buildingRsuPopTable = buildingPopRSU
                        }
                        if(gridTable){
                            //Here the population from the building to a grid
                            // Create the relations between grid cells and buildings
                            IProcess createScalesRelationsGridBl = Geoindicators.SpatialUnits.spatialJoin()
                            if (!createScalesRelationsGridBl([datasource              : datasource,
                                                              sourceTable             : buildingPopTable,
                                                              targetTable             : gridTable,
                                                              idColumnTarget          : "id_grid",
                                                              prefixName              : prefixName,
                                                              nbRelations             : null])) {
                                info "Cannot compute the scales relations between buildings and grid cells."
                                return
                            }
                            def unweightedBuildingIndicators =[:]
                            populationColumns.each { col ->
                                unweightedBuildingIndicators.put(col, ["sum"])
                            }
                            def computeBuildingStats = Geoindicators.GenericIndicators.unweightedOperationFromLowerScale()
                            if (computeBuildingStats([inputLowerScaleTableName: createScalesRelationsGridBl.results.outputTableName,
                                                       inputUpperScaleTableName: gridTable,
                                                       inputIdUp               : "id_grid",
                                                       inputIdLow              : "id_grid",
                                                       inputVarAndOperations   : unweightedBuildingIndicators,
                                                       prefixName              : prefixName,
                                                       datasource              : datasource])) {
                                gridPopTable = computeBuildingStats.results.outputTableName
                            }
                        }
                    }
                }
                return   [buildingPopTable: buildingPopTable, buildingRsuPopTable: buildingRsuPopTable, rsuPopTable: rsuPopTable, gridPopTable:gridPopTable]

            }
            warn "Please set a valid population table name and a list of population columns"
            return
        }
    }
}



