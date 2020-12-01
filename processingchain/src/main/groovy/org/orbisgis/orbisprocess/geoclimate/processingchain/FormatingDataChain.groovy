package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory

@BaseScript GroovyProcessFactory pf


/**
 * This process is used to re-format the building table when the estimated height RF is used.
 * It must be used to update the nb level according the estimated height roof value
 *
 * @param datasource A connexion to a DB containing the raw buildings table
 * @param inputTableName The name of the raw buildings table in the DB
 * @param hLevMin Minimum building level height
 * @param epsg srid code of the output table
 * @return outputTableName The name of the final buildings table
 */
IProcess formatEstimatedBuilding() {
    return create {
        title "Format the estimated OSM buildings table into a table that matches the constraints of the GeoClimate input model"
        id "formatEstimatedBuilding"
        inputs datasource: JdbcDataSource, inputTableName: String, epsg: int, h_lev_min: 3
        outputs outputTableName: String
        run { JdbcDataSource datasource, inputTableName,epsg, h_lev_min ->
            def outputTableName = postfix "INPUT_BUILDING_REFORMATED_"
            info 'Re-formating building layer'
            def outputEstimateTableName = ""
            datasource """ 
                DROP TABLE if exists ${outputTableName};
                CREATE TABLE ${outputTableName} (THE_GEOM GEOMETRY(POLYGON, $epsg), id_build INTEGER, ID_SOURCE VARCHAR, 
                    HEIGHT_WALL FLOAT, HEIGHT_ROOF FLOAT, NB_LEV INTEGER, TYPE VARCHAR, MAIN_USE VARCHAR, ZINDEX INTEGER, ID_BLOCK INTEGER, ID_RSU INTEGER);
            """
            if (inputTableName) {
                def queryMapper = "SELECT "
                def inputSpatialTable = datasource."$inputTableName"
                if (inputSpatialTable.rowCount > 0) {
                    def columnNames = inputSpatialTable.columns
                    queryMapper += " ${columnNames.join(",")} FROM $inputTableName"

                    datasource.withBatch(1000) { stmt ->
                        datasource.eachRow(queryMapper) { row ->
                            def heightRoof = row.height_roof
                            def heightWall = heightRoof
                            def type = row.type
                            def nbLevels = Math.floor(heightRoof/h_lev_min)
                            stmt.addBatch """
                                                INSERT INTO ${outputTableName} values(
                                                    ST_GEOMFROMTEXT('${row.the_geom}',$epsg), 
                                                    ${row.id_build}, 
                                                    '${row.id_source}',
                                                    ${heightWall},
                                                    ${heightRoof},
                                                    ${nbLevels},
                                                    '${type}',
                                                    '${row.main_use}',
                                                    ${row.zindex},
                                                    ${row.id_block},${row.id_rsu})
                                            """.toString()

                        }
                    }
                }
            }
            info 'Re-formating building finishes'
            [outputTableName: outputTableName]
        }
    }
}
