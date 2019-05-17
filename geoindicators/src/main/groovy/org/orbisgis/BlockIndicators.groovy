package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess


@BaseScript Geoclimate geoclimate

/**
 * This process is used to compute the Perkins SKill Score (included within a [0 - 1] interval) of the distribution
 * of building direction within a block. This indicator gives an idea of the building direction variability within
 * each block. The length of the building SMBR sides is considered to calculate a distribution of building orientation.
 * Then the distribution is used to calculate the Perkins SKill Score. The distribution has an "angle_range_size"
 * interval that has to be defined by the user (default 15).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildingTableName the name of the input ITable where are stored the geometry field, the building and the block ID
 * @param angleRangeSize the range size (in °) of each interval angle used to calculate the distribution of building direction
 * (should be a divisor of 180 - default 15°)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess perkinsSkillScoreBuildingDirection() {
    return processFactory.create(
            "Block Perkins skill score building direction",
            [buildingTableName: String, angleRangeSize: int, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { buildingTableName, angleRangeSize = 15, prefixName, datasource->

                def geometricField = "the_geom"
                def idFieldBu = "id_build"
                def idFieldBl = "id_block"

                // The name of the outputTableName is constructed
                String baseName = "block_perkins_skill_score_building_direction"
                String outputTableName = prefixName + "_" + baseName

                // Test whether the angleRangeSize is a divisor of 180°
                if (180%angleRangeSize==0 & 180/angleRangeSize>1){
                    // To avoid overwriting the output files of this step, a unique identifier is created
                    def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                    // Temporary table names
                    def build_min_rec = "build_min_rec"+uid_out
                    def build_dir360 = "build_dir360"+uid_out
                    def build_dir180 = "build_dir180"+uid_out
                    def build_dir_dist = "build_dir_dist"+uid_out
                    def build_dir_tot = "build_dir_tot"+uid_out

                    // The minimum diameter of the minimum rectangle is created for each building
                    datasource.execute(("DROP TABLE IF EXISTS $build_min_rec; CREATE TABLE $build_min_rec AS "+
                                        "SELECT $idFieldBu, $idFieldBl, ST_MINIMUMDIAMETER(ST_MINIMUMRECTANGLE($geometricField)) "+
                                        "AS the_geom FROM $buildingTableName").toString())

                    // The length and direction of the smallest and the longest sides of the Minimum rectangle are calculated
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_bua ON $buildingTableName($idFieldBu);" +
                                        "CREATE INDEX IF NOT EXISTS id_bua ON $build_min_rec($idFieldBu);" +
                                        "DROP TABLE IF EXISTS $build_dir360; CREATE TABLE $build_dir360 AS "+
                                        "SELECT a.$idFieldBl, ST_LENGTH(a.the_geom) AS LEN_L, "+
                                        "ST_AREA(b.the_geom)/ST_LENGTH(a.the_geom) AS LEN_H, "+
                                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(a.the_geom), ST_ENDPOINT(a.the_geom)))) AS ANG_L, "+
                                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(a.the_geom, pi()/2)), "+
                                        "ST_ENDPOINT(ST_ROTATE(a.the_geom, pi()/2))))) AS ANG_H FROM $build_min_rec a  "+
                                        "LEFT JOIN $buildingTableName b ON a.$idFieldBu=b.$idFieldBu").toString())

                    // The angles are transformed in the [0, 180]° interval
                    datasource.execute(("DROP TABLE IF EXISTS $build_dir180; CREATE TABLE $build_dir180 AS "+
                                        "SELECT $idFieldBl, LEN_L, LEN_H, CASEWHEN(ANG_L>=180, ANG_L-180, ANG_L) AS ANG_L, "+
                                        "CASEWHEN(ANG_H>180, ANG_H-180, ANG_H) AS ANG_H FROM $build_dir360").toString())

                    // The query aiming to create the building direction distribution is created
                    String sqlQueryDist = "DROP TABLE IF EXISTS $build_dir_dist; CREATE TABLE $build_dir_dist AS SELECT "
                    for (int i=angleRangeSize; i<180; i+=angleRangeSize){
                        sqlQueryDist += "SUM(CASEWHEN(ANG_L>=${i-angleRangeSize} AND ANG_L<$i, LEN_L, " +
                                        "CASEWHEN(ANG_H>=${i-angleRangeSize} AND ANG_H<$i, LEN_H, 0))) AS ANG$i, "
                    }
                    sqlQueryDist += "$idFieldBl FROM $build_dir180 GROUP BY $idFieldBl;"
                    datasource.execute(sqlQueryDist)

                    // The total of building linear of direction is calculated for each block
                    String sqlQueryTot = "DROP TABLE IF EXISTS $build_dir_tot; CREATE TABLE $build_dir_tot AS SELECT *, "
                    for (int i=angleRangeSize; i<180; i+=angleRangeSize){
                        sqlQueryTot += "ANG$i + "
                    }
                    sqlQueryTot = sqlQueryTot[0..-3] + "AS ANG_TOT FROM $build_dir_dist;"
                    datasource.execute(sqlQueryTot)

                    // The Perkings Skill score is finally calculated using a last query
                    String sqlQueryPerkins = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT $idFieldBl, "
                    for (int i=angleRangeSize; i<180; i+=angleRangeSize){
                        sqlQueryPerkins += "LEAST(1./(${180/angleRangeSize}), ANG$i::float/ANG_TOT) + "
                    }
                    sqlQueryPerkins = sqlQueryPerkins[0..-3] + "AS block_perkins_skill_score_building_direction" +
                                        " FROM $build_dir_tot;"
                    datasource.execute(sqlQueryPerkins)

                    // The temporary tables are deleted
                    datasource.execute(("DROP TABLE IF EXISTS $build_min_rec, $build_dir360, $build_dir180, "+
                                        "$build_dir_dist, $build_dir_tot").toString())

                    [outputTableName: outputTableName]
                }
            }
    )}

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
                def geometricField = "the_geom"
                def idColumnBl = "id_block"

                // The name of the outputTableName is constructed
                String baseName = "block_hole_area_density"
                String outputTableName = prefixName + "_" + baseName

                String query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                        "SELECT $idColumnBl, ST_AREA(ST_HOLES($geometricField))/ST_AREA($geometricField) " +
                        "AS $baseName FROM $blockTable"

                logger.info("Executing $query")
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
                def geometryFieldBu = "the_geom"
                def idColumnBl = "id_block"
                def height_wall = "height_wall"

                // The name of the outputTableName is constructed
                String baseName = "block_net_compacity"
                String outputTableName = prefixName + "_" + baseName

                String query = "CREATE INDEX IF NOT EXISTS id_b " +
                        "ON $correlationTableName($idColumnBl); DROP TABLE IF EXISTS $outputTableName;" +
                        " CREATE TABLE $outputTableName AS SELECT $idColumnBl, " +
                        "SUM($buildingContiguityField*(ST_PERIMETER($geometryFieldBu)+" +
                        "ST_PERIMETER(ST_HOLES($geometryFieldBu)))*$height_wall)/POWER(SUM($buildingVolumeField)," +
                        " 2./3) AS $baseName FROM $buildTable GROUP BY $idColumnBl"

                logger.info("Executing $query")
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
 * @param buildTable the name of the input ITable where are stored the geometry field and the block ID
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

                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}