package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess


@BaseScript ProcessingChain processingChain


/** The processing chain creates the units used to describe the territory at three scales: Reference Spatial
 * Unit (RSU), block and building. The creation of the RSU needs several layers such as the hydrology,
 * the vegetation, the roads and the rail network and the boundary of the study zone. The blocks are created
 * from the buildings that are in contact.
 * Then the relationships between each scale is initialized in each unit table: the RSU ID is stored in
 * the block and in the building tables whereas the block ID is stored only in the building table.
 *
 * @param zoneTable The area of zone to be processed *
 * @param buildingTable The building table to be processed
 * @param roadTable The road table to be processed
 * @param railTable The rail table to be processed
 * @param vegetationTable The vegetation table to be processed
 * @param hydrographicTable The hydrographic table to be processed
 * @param surface_vegetation The minimum area of vegetation that will be considered to delineate the RSU (default 100,000 m²)
 * @param surface_hydro  The minimum area of water that will be considered to delineate the RSU (default 2,500 m²)
 * @param distance A distance to group two geometries (e.g. two buildings in a block - default 0.01 m)
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @return outputTableBuildingName Table name where are stored the buildings and the RSU and block ID
 * @return outputTableBlockName Table name where are stored the blocks and the RSU ID
 * @return outputTableRsuName Table name where are stored the RSU
 */
public static IProcess createUnitsOfAnalysis(){
    return processFactory.create("Merge the geometries that touch each other",
            [datasource: JdbcDataSource, zoneTable : String, buildingTable:String, roadTable : String, railTable : String,
             vegetationTable: String, hydrographicTable: String, surface_vegetation: double,
             surface_hydro: double, distance: double, prefixName: String],
            [outputTableBuildingName : String, outputTableBlockName: String, outputTableRsuName: String],
            { datasource, zoneTable,buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance,  prefixName ->
                logger.info("Create the units of analysis...")

                // Create the RSU
                IProcess prepareRSUData = org.orbisgis.Geoclimate.SpatialUnits.prepareRSUData()
                IProcess createRSU = org.orbisgis.Geoclimate.SpatialUnits.createRSU()
                prepareRSUData.execute([datasource: datasource, zoneTable : zoneTable, roadTable : roadTable,
                                        railTable : railTable, vegetationTable: vegetationTable,
                                        hydrographicTable: hydrographicTable, surface_vegetation: surface_vegetation,
                                        surface_hydro: surface_hydro, prefixName: prefixName])
                createRSU.execute([datasource: datasource, inputTableName : prepareRSUData.results.outputTableName,
                                   prefixName: prefixName])

                // Create the blocks
                IProcess createBlocks = org.orbisgis.Geoclimate.SpatialUnits.createBlocks()
                createBlocks.execute([datasource: datasource, inputTableName : buildingTable,
                                      prefixName: prefixName, distance: distance])


                // Create the relations between RSU and blocks (store in the block table)
                IProcess createScalesRelationsRsuBl = org.orbisgis.Geoclimate.SpatialUnits.createScalesRelations()
                createScalesRelationsRsuBl.execute([datasource: datasource,
                                                    inputLowerScaleTableName: createBlocks.results.outputTableName,
                                                    inputUpperScaleTableName: createRSU.results.outputTableName,
                                                    idColumnUp: createRSU.results.outputIdRsu,
                                                    prefixName: prefixName])


                // Create the relations between buildings and blocks (store in the buildings table)
                IProcess createScalesRelationsBlBu = org.orbisgis.Geoclimate.SpatialUnits.createScalesRelations()
                createScalesRelationsBlBu.execute([datasource: datasource,
                                                   inputLowerScaleTableName: inputLowerScaleTableName,
                                                   inputUpperScaleTableName: createBlocks.results.outputTableName,
                                                   idColumnUp: createBlocks.results.outputIdBlock,
                                                   prefixName: prefixName])


                // Create the relations between buildings and RSU (store in the buildings table)
                // WARNING : the building table will contain the id_block and id_rsu for each of its
                // id_build but the relations between id_block and i_rsu should not been consider in this Table
                // the relationships may indeed be different from the one in the block Table
                IProcess createScalesRelationsRsuBlBu = org.orbisgis.Geoclimate.SpatialUnits.createScalesRelations()
                createScalesRelationsRsuBlBu.execute([datasource: datasource,
                                                      inputLowerScaleTableName:
                                                              createScalesRelationsBlBu.results.outputTableName,
                                                      inputUpperScaleTableName: createRSU.results.outputTableName,
                                                      idColumnUp: createRSU.results.outputIdRsu,
                                                      prefixName: prefixName])


                [outputTableBuildingName : createScalesRelationsRsuBlBu.results.outputTableName,
                 outputTableBlockName: createScalesRelationsRsuBl.results.outputTableName,
                 outputTableRsuName: createRSU.results.outputTableName]
            }
    )
}
