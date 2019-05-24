package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate


/**
 * This process calculates the ratio between the area of hole within a block and the area of the block.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param blockTable the name of the input ITable where are stored the block geometries
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the block id and their corresponding indicator value are stored
 * @author Jérémy Bernard
 */
static IProcess holeAreaDensity() {
    return processFactory.create(
            "Hole area ratio",
            [blockTable: String, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { blockTable, prefixName, datasource ->

                logger.info("Executing Hole area ratio")

                def geometricField = "the_geom"
                def idColumnBl = "id_block"

                // The name of the outputTableName is constructed
                String baseName = "block_hole_area_density"
                String outputTableName = prefixName + "_" + baseName

                String query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                        "SELECT $idColumnBl, ST_AREA(ST_HOLES($geometricField))/ST_AREA($geometricField) " +
                        "AS $baseName FROM $blockTable"

                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}

/**
 * The sum of the building free external area composing the block are divided by the sum of the building volume.
 * The sum of the building volume is calculated at the power 2/3 in order to have a ratio of homogeneous quantities (same unit)
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildTable the name of the input ITable where are stored the "building_volume", the "building_contiguity",
 * the geometry field and the block id
 * @param correlationTableName the name of the input ITable where are stored the relationships between blocks and buildings
 * @param buildingVolumeField the name of the input field where are stored the "building_volume" values within the "buildTable"
 * @param buildingContiguityField the name of the input field where are stored the "building_contiguity"
 * values within the "buildTable"
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the block id and their corresponding indicator value are stored
 * @author Jérémy Bernard
 */
static IProcess netCompacity() {
    return processFactory.create(
            "Block net compacity",
            [buildTable: String, buildingVolumeField: String, buildingContiguityField: String,
             prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { buildTable, buildingVolumeField, buildingContiguityField, prefixName, datasource ->

                logger.info("Executing Block net compacity")

                def geometryFieldBu = "the_geom"
                def idColumnBl = "id_block"
                def height_wall = "height_wall"

                // The name of the outputTableName is constructed
                String baseName = "block_net_compacity"
                String outputTableName = prefixName + "_" + baseName

                String query = "CREATE INDEX IF NOT EXISTS id_b " +
                        "ON $buildTable($idColumnBl); DROP TABLE IF EXISTS $outputTableName;" +
                        " CREATE TABLE $outputTableName AS SELECT $idColumnBl, " +
                        "SUM($buildingContiguityField*(ST_PERIMETER($geometryFieldBu)+" +
                        "ST_PERIMETER(ST_HOLES($geometryFieldBu)))*$height_wall)/POWER(SUM($buildingVolumeField)," +
                        " 2./3) AS $baseName FROM $buildTable GROUP BY $idColumnBl"

                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}

/**
 * This indicator is usefull for the urban fabric classification proposed in Thornay et al. (2017) and also described
 * in Bocher et al. (2018). It answers to the Step 11 of the manual decision tree which consists in checking whether
 * the block is closed (continuous buildings the aligned along road). In order to identify the RSU with closed blocks,
 * the difference  between the st_holes(bloc scale) and sum(st_holes(building scale)) indicators will be calculated.
 *
 * WARNING: this method will not be able to identify blocks that are nearly closed (e.g. 99 % of the RSU perimeter).
 *
 * References:
 *   Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 *   Tornay, Nathalie, Robert Schoetter, Marion Bonhomme, Serge Faraut, and Valéry Masson. "GENIUS: A methodology to
 * define a detailed description of buildings for urban climate and building energy consumption simulations."
 * Urban Climate 20 (2017): 75-93.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param blockTable the name of the input ITable where are stored the geometry field and the block ID
 * @param correlationTableName the name of the input ITable where are stored the relationships between blocks and buildings
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the block id and their corresponding indicator value are stored
 * @author Jérémy Bernard
 */
static IProcess closingness() {
    return processFactory.create(
            "Closingness of a block",
            [correlationTableName: String, blockTable: String, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { correlationTableName, blockTable, prefixName, datasource ->

                logger.info("Executing Closingness of a block")

                def geometryFieldBu = "the_geom"
                def geometryFieldBl = "the_geom"
                def idColumnBl = "id_block"

                // The name of the outputTableName is constructed
                String baseName = "block_closingness"
                String outputTableName = prefixName + "_" + baseName

                String query = "CREATE INDEX IF NOT EXISTS id_bubl ON $blockTable($idColumnBl); " +
                        "CREATE INDEX IF NOT EXISTS id_b ON $correlationTableName($idColumnBl); " +
                        "DROP TABLE IF EXISTS $outputTableName;" +
                        " CREATE TABLE $outputTableName AS SELECT b.$idColumnBl, " +
                        "ST_AREA(ST_HOLES(b.$geometryFieldBl))-SUM(ST_AREA(ST_HOLES(a.$geometryFieldBu))) AS $baseName " +
                        "FROM $correlationTableName a, $blockTable b WHERE a.$idColumnBl = b.$idColumnBl " +
                        "GROUP BY b.$idColumnBl"

                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}