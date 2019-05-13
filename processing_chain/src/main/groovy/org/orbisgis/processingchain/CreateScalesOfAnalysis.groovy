package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.SpatialUnits
import org.orbisgis.processmanager.ProcessMapper

@BaseScript ProcessingChain processingChain

/** The processing chain creates the units used to describe the territory at three scales: Reference Spatial
 * Unit (RSU), block and building. The creation of the RSU needs several layers such as the hydrology,
 * the vegetation, the roads and the rail network and the boundary of the study zone. The blocks are created
 * from the buildings that are in contact.
 * Then the relationships between each scale is initialized in each unit table: the RSU ID is stored in
 * the block and in the building tables whereas the block ID is stored only in the building table.
 *
 * @param zoneTable The area of zone to be processed
 * @param roadTable The road table to be processed
 * @param railTable The rail table to be processed
 * @param vegetationTable The vegetation table to be processed
 * @param hydrographicTable The hydrographic table to be processed
 * @param surface_vegetation The minimum area of vegetation that will be considered to delineate the RSU (default 100,000 m²)
 * @param surface_hydro  The minimum area of water that will be considered to delineate the RSU (default 2,500 m²)
 * @param inputTableName The input table where are stored the geometries used to create the block (e.g. buildings...)
 * @param distance A distance to group two geometries (e.g. two buildings in a block - default 0.01 m)
 * @param inputLowerScaleTableName The input table where are stored the lowerScale objects (buildings)
 * @param prefixName A prefix used to name the output table
 * @param datasource A connection to a database
 *
 * @return outputTableName Table name where are stored the buildings and the RSU and block ID
 * @return outputTableName Table name where are stored the blocks and the RSU ID
 * @return outputTableName Table name where are stored the RSU
 */
public static ProcessMapper createMapper(){
    def prepareRSUData = SpatialUnits.prepareRSUData()
    def createRSU = SpatialUnits.createRSU()
    def createBlocks = SpatialUnits.createBlocks()
    def createScalesRelationsBlBu = SpatialUnits.createScalesRelations()
    def createScalesRelationsRsuBl = SpatialUnits.createScalesRelations()
    def createScalesRelationsRsuBlBu = SpatialUnits.createScalesRelations()

    ProcessMapper mapper = new ProcessMapper()
    // FROM prepareRSUData...
    // ...to createRSU
    mapper.link(outputTableName : prepareRSUData, inputTableName : createRSU)

    // FROM createRSU...
    // ...to createScalesRelations (relationship between RSU and buildings)
    mapper.link(outputTableName : createRSU, inputUpperScaleTableName : createScalesRelationsRsuBl)
    mapper.link(outputIdRsu : createRSU, idColumnUp : createScalesRelationsRsuBl)

    // ...to createScalesRelations (relationship between RSU and blocks and buildings)
    mapper.link(outputTableName : createRSU, inputUpperScaleTableName : createScalesRelationsRsuBlBu)
    mapper.link(outputIdRsu : createRSU, idColumnUp : createScalesRelationsRsuBlBu)

    // FROM createBlocks...
    // ...to createScalesRelations (relationships between blocks and RSU)
    mapper.link(outputTableName : createBlocks, inputLowerScaleTableName : createScalesRelationsBlBu)

    // ...to createScalesRelations (relationships between blocks and buildings)
    mapper.link(outputTableName : createBlocks, inputUpperScaleTableName : createScalesRelationsBlBu)
    mapper.link(outputIdBlock : createBlocks, idColumnUp : createScalesRelationsBlBu)

    // FROM createScalesRelations (that comes from the createBlocks)...
    // ...to createScalesRelations (relationships between blocks, RSU and buildings...)
    mapper.link(outputTableName : createScalesRelationsBlBu, inputLowerScaleTableName : createScalesRelationsRsuBlBu)

    return mapper
}