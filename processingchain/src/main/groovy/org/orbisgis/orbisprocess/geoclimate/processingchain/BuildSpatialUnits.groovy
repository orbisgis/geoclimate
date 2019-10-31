package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.transform.BaseScript
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import org.orbisgis.datamanager.JdbcDataSource


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
 * @param indicatorUse The use defined for the indicator. Depending on this use, only a part of the indicators could
 * be calculated (default is all indicators : ["LCZ", "URBAN_TYPOLOGY", "TEB"])
 *
 * @return outputTableBuildingName Table name where are stored the buildings and the RSU and block ID
 * @return outputTableBlockName Table name where are stored the blocks and the RSU ID
 * @return outputTableRsuName Table name where are stored the RSU
 */
def createUnitsOfAnalysis(){
    return create({
        title "Create all new spatial units and their relations : building, block and RSU"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: double, prefixName: "", indicatorUse: ["LCZ",
                                                                     "URBAN_TYPOLOGY",
                                                                     "TEB"]
        outputs outputTableBuildingName: String, outputTableBlockName: String, outputTableRsuName: String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance, prefixName, indicatorUse ->
            info "Create the units of analysis..."

            // Create the RSU
            def prepareRSUData = Geoindicators.SpatialUnits.prepareRSUData()
            def createRSU = Geoindicators.SpatialUnits.createRSU()
            if (!prepareRSUData([datasource        : datasource,
                                 zoneTable         : zoneTable,
                                 roadTable         : roadTable,
                                 railTable         : railTable,
                                 vegetationTable   : vegetationTable,
                                 hydrographicTable : hydrographicTable,
                                 surface_vegetation: surface_vegetation,
                                 surface_hydro     : surface_hydro,
                                 prefixName        : prefixName])) {
                info "Cannot prepare the data for RSU calculation."
                return
            }
            def rsuDataPrepared = prepareRSUData.results.outputTableName

            if (!createRSU([datasource    : datasource,
                            inputTableName: rsuDataPrepared,
                            prefixName    : prefixName,
                            inputZoneTableName: zoneTable])) {
                info "Cannot compute the RSU."
                return
            }

            // By default, the building table is used to calculate the relations between buildings and RSU
            def inputLowerScaleBuRsu = buildingTable
            // And the block / RSU table does not exists
            def tableRsuBlocks = null
            // If the urban typology is needed
            if (indicatorUse.contains("URBAN_TYPOLOGY")) {
                // Create the blocks
                def createBlocks = Geoindicators.SpatialUnits.createBlocks()
                if (!createBlocks([datasource    : datasource,
                                   inputTableName: buildingTable,
                                   prefixName    : prefixName,
                                   distance      : distance])) {
                    info "Cannot create the blocks."
                    return
                }

                // Create the relations between RSU and blocks (store in the block table)
                def createScalesRelationsRsuBl = Geoindicators.SpatialUnits.createScalesRelations()
                if (!createScalesRelationsRsuBl([datasource              : datasource,
                                                 inputLowerScaleTableName: createBlocks.results.outputTableName,
                                                 inputUpperScaleTableName: createRSU.results.outputTableName,
                                                 idColumnUp              : createRSU.results.outputIdRsu,
                                                 prefixName              : prefixName])) {
                    info "Cannot compute the scales relations between blocks and RSU."
                    return
                }

                // Create the relations between buildings and blocks (store in the buildings table)
                def createScalesRelationsBlBu = Geoindicators.SpatialUnits.createScalesRelations()
                if (!createScalesRelationsBlBu([datasource              : datasource,
                                                inputLowerScaleTableName: buildingTable,
                                                inputUpperScaleTableName: createBlocks.results.outputTableName,
                                                idColumnUp              : createBlocks.results.outputIdBlock,
                                                prefixName              : prefixName])) {
                    info "Cannot compute the scales relations between blocks and buildings."
                    return
                }
                inputLowerScaleBuRsu = createScalesRelationsBlBu.results.outputTableName
                tableRsuBlocks = createScalesRelationsRsuBl.results.outputTableName
            }


            // Create the relations between buildings and RSU (store in the buildings table)
            // WARNING : if the blocks are used, the building table will contain the id_block and id_rsu for each of its
            // id_build but the relations between id_block and i_rsu should not been consider in this Table
            // the relationships may indeed be different from the one in the block Table
            def createScalesRelationsRsuBlBu = Geoindicators.SpatialUnits.createScalesRelations()
            if (!createScalesRelationsRsuBlBu([datasource              : datasource,
                                               inputLowerScaleTableName: inputLowerScaleBuRsu,
                                               inputUpperScaleTableName: createRSU.results.outputTableName,
                                               idColumnUp              : createRSU.results.outputIdRsu,
                                               prefixName              : prefixName])) {
                info "Cannot compute the scales relations between buildings and RSU."
                return
            }


            [outputTableBuildingName: createScalesRelationsRsuBlBu.results.outputTableName,
             outputTableBlockName   : tableRsuBlocks,
             outputTableRsuName     : createRSU.results.outputTableName]
        }
    })
}
