package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2gisSpatialTable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * Process used to compute the sum of all building free facades (roofs are excluded) included in a
 * Reference Spatial Unit (RSU - such as urban blocks) divided by the RSU area. The calculation is performed
 * according to a building Table where are stored the "building_contiguity", the building wall height and
 * the "building_total_facade_length" values as well as a correlation Table between buildings and blocks.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildingTable The name of the input ITable where are stored the buildings, the building contiguity values,
 * the building total facade length values and the building and rsu id
 * @param correlationTable The name of the input ITable where are stored the rsu geometries and the id_rsu
 * @param buContiguityColumn The name of the column where are stored the building contiguity values (within the building Table)
 * @param buTotalFacadeLengthColumn The name of the column where are stored the building total facade length values
 * (within the building Table)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess freeExternalFacadeDensity() {
return processFactory.create(
        "RSU free external facade density",
        [buildingTable: String,rsuTable: String,
         buContiguityColumn: String, buTotalFacadeLengthColumn: String, prefixName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { buildingTable, rsuTable, buContiguityColumn, buTotalFacadeLengthColumn,
          prefixName, datasource ->

            logger.info("Executing RSU free external facade density")

            def geometricFieldRsu = "the_geom"
            def idFieldRsu = "id_rsu"
            def height_wall = "height_wall"

            // The name of the outputTableName is constructed
            String baseName = "rsu_free_external_facade_density"
            String outputTableName = prefixName + "_" + baseName

            String query = "CREATE INDEX IF NOT EXISTS id_bua ON $buildingTable($idFieldRsu); "+
                            "CREATE INDEX IF NOT EXISTS id_blb ON $rsuTable($idFieldRsu); "+
                            "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                            "SELECT COALESCE(SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$height_wall)/"+
                            "st_area(b.$geometricFieldRsu),0) AS rsu_free_external_facade_density, b.$idFieldRsu "

            query += " FROM $buildingTable a RIGHT JOIN $rsuTable b "+
                        "ON a.$idFieldRsu = b.$idFieldRsu GROUP BY b.$idFieldRsu, b.$geometricFieldRsu;"

            datasource.execute query
            [outputTableName: outputTableName]
        }
)}

/**
 * Process used to compute the RSU ground sky view factor such as defined by Stewart et Oke (2012): ratio of the
 * amount of sky hemisphere visible from ground level to that of an unobstructed hemisphere. The calculation is
 * based on the ST_SVF function of H2GIS using the following parameters: ray length = 100, number of directions = 60
 * and for a grid resolution of 10 m (standard deviation of the estimate calculated to 0.03 according to
 * Bernard et al. (2018)). The density of points used for the calculation actually depends on building
 * density (higher the building density, lower the density of points). To avoid this phenomenon, we set
 * a constant density of point "point_density" for a given amount of free surfaces (default 0.008,
 * based on the median of Bernard et al. (2018) dataset). The calculation needs the "rsu_building_density"
 * and the "rsu_area".
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable The name of the input ITable where are stored the RSU
 * @param correlationBuildingTable The name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
 * @param rsuBuildingDensityColumn The name of the column where are stored the building density values (within the rsu Table)
 * @param pointDensity The density of points (nb / free m²) used to calculate the spatial average SVF
 * @param rayLength The maximum distance to consider an obstacle as potential sky cover
 * @param numberOfDirection the number of directions considered to calculate the SVF
 * @param prefixName String use as prefix to name the output table
 *
 * References:
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of
 * the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 * --> Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. Sky View Factor Calculation in
 * Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools. Climate ,
 * MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess groundSkyViewFactor() {
    return processFactory.create(
            "RSU ground sky view factor",
            [rsuTable: String,correlationBuildingTable: String, rsuBuildingDensityColumn: String, pointDensity: double, rayLength: double,
             numberOfDirection: int, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, correlationBuildingTable, rsuBuildingDensityColumn,
              pointDensity = 0.008, rayLength = 100, numberOfDirection = 60, prefixName, datasource ->

                logger.info("Executing RSU ground sky view factor")

                def geometricColumnRsu = "the_geom"
                def geometricColumnBu = "the_geom"
                def idColumnRsu = "id_rsu"
                def height_wall = "height_wall"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def ptsRSUtot = "ptsRSUtot"+uid_out
                def ptsRSUGrid = "ptsRSUGrid"+uid_out
                def ptsRSUtempo = "ptsRSUtempo"+uid_out
                def ptsRSUbu = "ptsRSUbu"+uid_out
                def ptsRSUfreeall = "ptsRSUfreeall"+uid_out
                def randomSample = "randomSample"+uid_out
                def svfPts = "svfPts"+uid_out

                // The name of the outputTableName is constructed
                String baseName = "rsu_ground_sky_view_factor"
                String outputTableName = prefixName + "_" + baseName

                // Create the needed index on input tables and the table that will contain the SVF calculation points
                H2gisSpatialTable rsuSpatialTable = datasource.getSpatialTable(rsuTable)
                H2gisSpatialTable buildingSpatialTable = datasource.getSpatialTable(correlationBuildingTable)
                rsuSpatialTable[geometricColumnRsu].createSpatialIndex()
                rsuSpatialTable[idColumnRsu].createIndex()
                buildingSpatialTable[geometricColumnRsu].createSpatialIndex()
                buildingSpatialTable[idColumnRsu].createIndex()

                def to_start = System.currentTimeMillis()

                datasource.execute(("DROP TABLE IF EXISTS $ptsRSUtot; CREATE TABLE $ptsRSUtot (pk serial, "+
                        "the_geom geometry, id_rsu int)").toString())

                // The points used for the SVF calculation should be selected within each RSU
                datasource.eachRow("SELECT * FROM $rsuTable".toString()) { row ->
                    // Size of the grid mesh used to sample each RSU (based on the building density + 10%) - if the
                    // building density exceeds 90%, the LCZ 7 building density is then set to 90%
                    def freeAreaDens = Math.max(1-(row[rsuBuildingDensityColumn]+0.1), 0.1)
                    def gms = (freeAreaDens/pointDensity)**0.5
                    // A grid of point is created for each RSU
                    datasource.execute(("DROP TABLE IF EXISTS $ptsRSUGrid; CREATE TABLE $ptsRSUGrid(pk SERIAL, the_geom GEOMETRY) AS (SELECT null, "+
                            "the_geom FROM ST_MAKEGRIDPOINTS('${row[geometricColumnRsu]}'::GEOMETRY, $gms, $gms))").toString())
                    // Grid points included inside the RSU are conserved
                    datasource.execute(("CREATE INDEX IF NOT EXISTS ids_temp ON $ptsRSUGrid(the_geom) USING RTREE; "+
                            "DROP TABLE IF EXISTS $ptsRSUtempo; CREATE TABLE $ptsRSUtempo AS SELECT a.pk, a.the_geom, "+
                            "${row[idColumnRsu]} AS id_rsu FROM $ptsRSUGrid a WHERE a.the_geom && '${row[geometricColumnRsu]}' AND "+
                            "ST_INTERSECTS(a.the_geom, '${row[geometricColumnRsu]}')").toString())
                    // If there is no point within the RSU (which could be the case for a long and thin RSU), the SVF
                    // is calculated for the centroid of the RSU
                    if(datasource.firstRow("SELECT COUNT(*) AS NB FROM $ptsRSUtempo".toString())["NB"]==0){
                        datasource.execute(("DROP TABLE IF EXISTS $ptsRSUtempo; CREATE TABLE $ptsRSUtempo AS SELECT " +
                                "1 AS pk, ST_CENTROID('${row[geometricColumnRsu]}') AS the_geom, ${row[idColumnRsu]} AS id_rsu").toString())
                    }
                    // The grid points intersecting buildings are identified
                    datasource.execute(("CREATE INDEX IF NOT EXISTS ids_temp ON $ptsRSUtempo(the_geom) USING RTREE; "+
                            "CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUtempo(id_rsu);"+
                            "DROP TABLE IF EXISTS $ptsRSUbu; CREATE TABLE $ptsRSUbu AS SELECT a.pk FROM $ptsRSUtempo a "+
                            "LEFT JOIN $correlationBuildingTable b ON a.id_rsu = b.$idColumnRsu WHERE "+
                            "a.the_geom && b.$geometricColumnBu AND ST_INTERSECTS(a.the_geom, b.$geometricColumnBu)").toString())
                    // The grid points intersecting buildings are then deleted
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUtempo(pk); "+
                            "CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUbu(pk); DROP TABLE IF EXISTS $ptsRSUfreeall; "+
                            "CREATE TABLE $ptsRSUfreeall(pk SERIAL, the_geom GEOMETRY, id_rsu INT) AS (SELECT null, a.the_geom, a.id_rsu FROM "+
                            "$ptsRSUtempo a LEFT JOIN $ptsRSUbu b ON a.pk=b.pk WHERE b.pk IS NULL)").toString())
                    // A random sample of points (of size corresponding to the point density defined by $pointDensity)
                    // is drawn in order to have the same density of point in each RSU. It is directly
                    // inserted into the Table gathering the SVF points used for all RSU
                    datasource.execute(("DROP TABLE IF EXISTS $randomSample; INSERT INTO $ptsRSUtot(pk, the_geom, id_rsu) "+
                            "SELECT null, the_geom, id_rsu FROM $ptsRSUfreeall ORDER BY RANDOM() LIMIT "+
                            "(TRUNC(${pointDensity}*ST_AREA('${row[geometricColumnRsu]}'::GEOMETRY)*${(1.0-row[rsuBuildingDensityColumn])})+1);").toString())
                }
                // The SVF calculation is performed at point scale
                datasource.execute(("CREATE INDEX IF NOT EXISTS ids_pts ON $ptsRSUtot(the_geom) USING RTREE; "+
                        "DROP TABLE IF EXISTS $svfPts; CREATE TABLE $svfPts AS SELECT a.pk, a.id_rsu, "+
                        "ST_SVF(ST_GEOMETRYN(a.the_geom,1), ST_ACCUM(ST_UPDATEZ(b.$geometricColumnBu, b.$height_wall)), "+
                        "$rayLength, $numberOfDirection, 5) AS SVF FROM $ptsRSUtot AS a, $correlationBuildingTable "+
                        "AS b WHERE ST_EXPAND(a.the_geom, $rayLength) && b.$geometricColumnBu AND "+
                        "ST_DWITHIN(b.$geometricColumnBu, a.the_geom, $rayLength) GROUP BY a.the_geom").toString())

                // The result of the SVF calculation is averaged at RSU scale and the rsu that do not have
                // buildings in the area of calculation are considered "free sky" (SVF = 1)
                datasource.execute(("CREATE INDEX IF NOT EXISTS id_svf ON $svfPts(id_rsu); " +
                        "CREATE INDEX IF NOT EXISTS id_pts ON $ptsRSUtot(id_rsu);"+
                        "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                        "SELECT COALESCE(AVG(a.SVF),1.0) AS rsu_ground_sky_view_factor, b.id_rsu AS $idColumnRsu " +
                        "FROM $svfPts a RIGHT JOIN $ptsRSUtot b ON a.id_rsu = b.id_rsu "+
                        "GROUP BY b.id_rsu").toString())
                def tObis = System.currentTimeMillis()-to_start

                logger.info("SVF calculation time: ${tObis/1000} s")


                // The temporary tables are deleted
                datasource.execute(("DROP TABLE IF EXISTS $ptsRSUtot, $ptsRSUGrid, $ptsRSUtempo, $ptsRSUbu, "+
                        "$ptsRSUfreeall, $randomSample, $svfPts").toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * Process used to compute the aspect ratio such as defined by Stewart et Oke (2012): mean height-to-width ratio
 * of street canyons (LCZs 1-7), building spacing (LCZs 8-10), and tree spacing (LCZs A - G). A simple approach based
 * on the street canyons assumption is used for the calculation. The sum of facade area within a given RSU area
 * is divided by the area of free surfaces of the given RSU (not covered by buildings). The
 * "rsu_free_external_facade_density" and "rsu_building_density" are used for the calculation.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable The name of the input ITable where are stored the RSU
 * @param rsuFreeExternalFacadeDensityColumn The name of the column where are stored the free external density
 * values (within the rsu Table)
 * @param rsuBuildingDensityColumn The name of the column where are stored the building density values (within the rsu Table)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess aspectRatio() {
    return processFactory.create(
            "RSU aspect ratio",
            [rsuTable: String, rsuFreeExternalFacadeDensityColumn: String,
             rsuBuildingDensityColumn: String, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, rsuFreeExternalFacadeDensityColumn, rsuBuildingDensityColumn,
              prefixName, datasource ->

                logger.info("Executing RSU aspect ratio")

                def columnIdRsu = "id_rsu"

                // The name of the outputTableName is constructed
                String baseName = "rsu_aspect_ratio"
                String outputTableName = prefixName + "_" + baseName

                String query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                        "SELECT $rsuFreeExternalFacadeDensityColumn/(1-$rsuBuildingDensityColumn) AS "+
                        "rsu_aspect_ratio "

                query += ", $columnIdRsu FROM $rsuTable"

                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the distribution of projected facade area within a RSU per vertical layer and direction
 * of analysis (ie. wind or sun direction). Note that the method used is an approximation if the RSU split
 * a building into two parts (the facade included within the RSU is counted half).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable The name of the input ITable where are stored the RSU
 * @param buildingTable The name of the input ITable where are stored the buildings
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20, 30, 40, 50])
 * @param numberOfDirection the number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 12)
 * @param prefixName String use as prefix to name the output table
 *
 * References:
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of
 * the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 * --> Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. Sky View Factor Calculation in
 * Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools. Climate ,
 * MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess projectedFacadeAreaDistribution() {
    return processFactory.create(
            "RSU projected facade area distribution",
            [buildingTable: String, rsuTable: String, listLayersBottom: double[], numberOfDirection: int,
             prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { buildingTable, rsuTable, listLayersBottom = [0, 10, 20, 30, 40, 50], numberOfDirection = 12,
              prefixName, datasource ->

                logger.info("Executing RSU projected facade area distribution")

                // The name of the outputTableName is constructed
                String baseName = "rsu_projected_facade_area_distribution"
                String outputTableName = prefixName + "_" + baseName

                if(360%numberOfDirection==0 & numberOfDirection%2==0) {
                    def geometricColumnRsu = "the_geom"
                    def geometricColumnBu = "the_geom"
                    def idColumnRsu = "id_rsu"
                    def idColumnBu = "id_build"
                    def height_wall = "height_wall"

                    // To avoid overwriting the output files of this step, a unique identifier is created
                    def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                    // Temporary table names
                    def buildingIntersection = "building_intersection" + uid_out
                    def buildingIntersectionExpl = "building_intersection_expl" + uid_out
                    def buildingFree = "buildingFree" + uid_out
                    def buildingLayer = "buildingLayer" + uid_out
                    def buildingFreeExpl = "buildingFreeExpl" + uid_out
                    def rsuInter = "rsuInter" + uid_out
                    def finalIndicator = "finalIndicator" + uid_out
                    def rsuInterRot = "rsuInterRot" + uid_out

                    // The projection should be performed at the median of the angle interval
                    def dirMedDeg = 180 / numberOfDirection
                    def dirMedRad = Math.toRadians(dirMedDeg)

                    dirMedDeg = Math.round(dirMedDeg)

                    // The list that will store the fields name is initialized
                    def names = []

                    // Indexes and spatial indexes are created on input tables
                    datasource.execute(("CREATE SPATIAL INDEX IF NOT EXISTS ids_ina ON $buildingTable($geometricColumnBu); " +
                            "CREATE SPATIAL INDEX IF NOT EXISTS ids_inb ON $rsuTable($geometricColumnRsu); " +
                            "CREATE INDEX IF NOT EXISTS id_ina ON $buildingTable($idColumnBu); " +
                            "CREATE INDEX IF NOT EXISTS id_inb ON $rsuTable($idColumnRsu);").toString())

                    // Common party walls between buildings are calculated
                    datasource.execute(("CREATE TABLE $buildingIntersection(pk SERIAL, the_geom GEOMETRY, " +
                            "ID_build_a INTEGER, ID_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS " +
                            "(SELECT NULL, ST_INTERSECTION(a.$geometricColumnBu, b.$geometricColumnBu), " +
                            "a.$idColumnBu, b.$idColumnBu, GREATEST(a.$height_wall,b.$height_wall), " +
                            "LEAST(a.$height_wall,b.$height_wall) FROM $buildingTable AS a, $buildingTable AS b " +
                            "WHERE a.$geometricColumnBu && b.$geometricColumnBu AND " +
                            "ST_INTERSECTS(a.$geometricColumnBu, b.$geometricColumnBu) AND a.$idColumnBu <> b.$idColumnBu)").toString())

                    // Common party walls are converted to multilines and then exploded
                    datasource.execute(("CREATE TABLE $buildingIntersectionExpl(pk SERIAL, the_geom GEOMETRY, " +
                            "ID_build_a INTEGER, ID_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS " +
                            "(SELECT NULL, ST_TOMULTILINE(the_geom), ID_build_a, ID_build_b, z_max, z_min " +
                            "FROM ST_EXPLODE('(SELECT the_geom AS the_geom, ID_build_a, ID_build_b, z_max, z_min " +
                            "FROM $buildingIntersection)'))").toString())

                    // Each free facade is stored TWICE (an intersection could be seen from the point of view of two buildings).
                    // Facades of isolated buildings are unioned to free facades of non-isolated buildings which are
                    // unioned to free intersection facades. To each facade is affected its corresponding free height
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_buint ON ${buildingIntersectionExpl}(ID_build_a); " +
                            "CREATE TABLE $buildingFree(pk SERIAL, the_geom GEOMETRY, z_max DOUBLE, z_min DOUBLE) " +
                            "AS (SELECT NULL, ST_TOMULTISEGMENTS(a.the_geom), a.$height_wall, 0 " +
                            "FROM $buildingTable a WHERE a.$idColumnBu NOT IN (SELECT ID_build_a " +
                            "FROM $buildingIntersectionExpl)) UNION ALL (SELECT NULL, " +
                            "ST_TOMULTISEGMENTS(ST_DIFFERENCE(ST_TOMULTILINE(a.$geometricColumnBu), " +
                            "ST_UNION(ST_ACCUM(b.the_geom)))), a.$height_wall, 0 FROM $buildingTable a, " +
                            "$buildingIntersectionExpl b WHERE a.$idColumnBu=b.ID_build_a " +
                            "GROUP BY b.ID_build_a) UNION ALL (SELECT NULL, ST_TOMULTISEGMENTS(the_geom) " +
                            "AS the_geom, z_max, z_min FROM $buildingIntersectionExpl WHERE ID_build_a<ID_build_b)").toString())

                    // The height of wall is calculated for each level
                    String layerQuery = "CREATE TABLE $buildingLayer AS SELECT pk, the_geom, "
                    for (i in 1..(listLayersBottom.size() - 1)) {
                        names[i - 1] = "rsu_projected_facade_area_distribution${listLayersBottom[i - 1]}" +
                                "_${listLayersBottom[i]}"
                        layerQuery += "CASEWHEN(z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(z_min >= ${listLayersBottom[i]}, " +
                                "0, ${listLayersBottom[i] - listLayersBottom[i - 1]}-GREATEST(${listLayersBottom[i]}-z_max,0)" +
                                "-GREATEST(z_min-${listLayersBottom[i - 1]},0))) AS ${names[i - 1]} ,"
                    }


                    names[listLayersBottom.size() - 1] = "rsu_projected_facade_area_distribution${listLayersBottom[listLayersBottom.size() - 1]}_"
                    layerQuery += "CASEWHEN(z_max >= ${listLayersBottom[listLayersBottom.size() - 1]}, " +
                            "z_max-GREATEST(z_min,${listLayersBottom[listLayersBottom.size() - 1]}), 0) " +
                            "AS ${names[listLayersBottom.size() - 1]} FROM $buildingFree"
                    datasource.execute(layerQuery.toString())

                    // Names and types of all columns are then useful when calling sql queries
                    String namesAndType = ""
                    String onlyNames = ""
                    String onlyNamesB = ""
                    for (n in names) {
                        namesAndType += " " + n + " double,"
                        onlyNames += " " + n + ","
                        onlyNamesB += " b." + n + ","
                    }

                    // Free facades are exploded to multisegments
                    datasource.execute(("CREATE TABLE $buildingFreeExpl(pk SERIAL, the_geom GEOMETRY, $namesAndType) AS " +
                            "(SELECT NULL, the_geom, ${onlyNames[0..-2]} FROM ST_EXPLODE('$buildingLayer'))").toString())

                    // Intersections between free facades and rsu geometries are calculated
                    datasource.execute(("CREATE SPATIAL INDEX IF NOT EXISTS ids_bufre ON $buildingFreeExpl(the_geom); " +
                            "CREATE TABLE $rsuInter(id_rsu INTEGER, the_geom GEOMETRY, $namesAndType) AS " +
                            "(SELECT a.$idColumnRsu, ST_INTERSECTION(a.$geometricColumnRsu, b.the_geom), " +
                            "${onlyNamesB[0..-2]} FROM $rsuTable a, $buildingFreeExpl b WHERE a.$geometricColumnRsu && b.the_geom " +
                            "AND ST_INTERSECTS(a.$geometricColumnRsu, b.the_geom))").toString())

                    // Basic informations are stored in the result Table where will be added all fields
                    // corresponding to the distribution
                    datasource.execute(("CREATE TABLE ${finalIndicator}_0 AS SELECT $idColumnRsu " +
                            "FROM $rsuTable").toString())


                    // The analysis is then performed for each direction ('numberOfDirection' / 2 because calculation
                    // is performed for a direction independently of the "toward")
                    for (int d = 0; d < numberOfDirection / 2; d++) {
                        def dirDeg = d * 360 / numberOfDirection
                        def dirRad = Math.toRadians(dirDeg)

                        // Define the field name for each of the directions and vertical layers
                        String namesAndTypeDir = ""
                        String onlyNamesDir = ""
                        String onlyNamesDirB = ""
                        for (n in names) {
                            namesAndTypeDir += " " + n + "D" + (dirDeg + dirMedDeg).toString() + " double,"
                            onlyNamesDir += " " + n + "D" + (dirDeg + dirMedDeg).toString() + ","
                            onlyNamesDirB += " b." + n + "D" + (dirDeg + dirMedDeg).toString() + ","
                        }

                        // To calculate the indicator for a new wind direction, the free facades are rotated
                        datasource.execute(("DROP TABLE IF EXISTS $rsuInterRot; CREATE TABLE $rsuInterRot(id_rsu INTEGER, the_geom geometry, " +
                                "${namesAndType[0..-2]}) AS (SELECT id_rsu, ST_ROTATE(the_geom,${dirRad + dirMedRad}), " +
                                "${onlyNames[0..-2]} FROM $rsuInter)").toString())

                        // The projected facade area indicator is calculated according to the free facades table
                        // for each vertical layer for this specific direction "d"
                        String calcQuery = ""
                        for (n in names) {
                            calcQuery += "sum((st_xmax(b.the_geom) - st_xmin(b.the_geom))*b.$n)/2 AS " +
                                    "${n}D${dirDeg + dirMedDeg}, "
                        }
                        datasource.execute(("CREATE INDEX IF NOT EXISTS id_rint ON $rsuInterRot(id_rsu); " +
                                "CREATE INDEX IF NOT EXISTS id_fin ON ${finalIndicator}_$d(id_rsu); " +
                                "CREATE TABLE ${finalIndicator}_${d + 1} AS SELECT a.*, ${calcQuery[0..-3]} " +
                                "FROM ${finalIndicator}_$d a LEFT JOIN $rsuInterRot b " +
                                "ON a.id_rsu = b.ID_RSU GROUP BY a.id_rsu").toString())
                    }

                    datasource.execute(("DROP TABLE IF EXISTS $outputTableName; ALTER TABLE " +
                            "${finalIndicator}_${numberOfDirection / 2} RENAME TO $outputTableName").toString())

                    // Remove all temporary tables created
                    String removeQuery = " ${finalIndicator}_0"
                    for (int d = 0; d < numberOfDirection / 2; d++) {
                        removeQuery += ", ${finalIndicator}_$d"
                    }
                    datasource.execute(("DROP TABLE IF EXISTS ${buildingIntersection}, ${buildingIntersectionExpl}, " +
                            "${buildingFree}, ${buildingFreeExpl}, ${rsuInter}, $removeQuery;").toString())
                }
            [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the distribution of roof (vertical, horizontal and tilted) area within a RSU per vertical layer.
 * Note that the method used is based on the assumption that all buildings have gable roofs. Since we do not know
 * what is the direction of the gable, we also consider that buildings are square in order to limit the potential
 * calculation error (for example considering that the gable is always oriented parallel to the larger side of the
 * building.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param buildingTable the name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
 * @param rsuTable the name of the input ITable where are stored the RSU
 * @param prefixName String use as prefix to name the output table
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20, 30, 40, 50])
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess roofAreaDistribution() {
    return processFactory.create(
            "RSU roof area distribution",
            [rsuTable: String, buildingTable: String, listLayersBottom: double[],
             prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, buildingTable, listLayersBottom = [0, 10, 20, 30, 40, 50],
              prefixName, datasource ->

                logger.info("Executing RSU roof area distribution")

                def geometricColumnRsu = "the_geom"
                def geometricColumnBu = "the_geom"
                def idColumnRsu = "id_rsu"
                def idColumnBu = "id_build"
                def height_wall = "height_wall"
                def height_roof = "height_roof"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def buildRoofSurfIni = "build_roof_surf_ini" + uid_out
                def buildVertRoofInter = "build_vert_roof_inter" + uid_out
                def buildVertRoofAll = "buildVertRoofAll" + uid_out
                def buildRoofSurfTot = "build_roof_surf_tot" + uid_out

                // The name of the outputTableName is constructed
                String baseName = "rsu_roof_area_distribution"
                String outputTableName = prefixName + "_" + baseName

                // Vertical and non-vertical (tilted and horizontal) roof areas are calculated
                datasource.execute(("CREATE TABLE $buildRoofSurfIni AS SELECT $geometricColumnBu, $idColumnRsu," +
                        "$idColumnBu, $height_roof AS z_max, $height_wall AS z_min, ST_AREA($geometricColumnBu) AS"+
                        " building_area, ST_PERIMETER($geometricColumnBu)+ST_PERIMETER(ST_HOLES($geometricColumnBu))"+
                        " AS building_total_facade_length, $height_roof-$height_wall AS delta_h, POWER(POWER(ST_AREA($geometricColumnBu),2)+4*"+
                        "ST_AREA($geometricColumnBu)*POWER($height_roof-$height_wall,2),0.5) AS non_vertical_roof_area,"+
                        "POWER(ST_AREA($geometricColumnBu), 0.5)*($height_roof-$height_wall) AS vertical_roof_area"+
                        " FROM $buildingTable;").toString())


                // Indexes and spatial indexes are created on rsu and building Tables
                datasource.execute(("CREATE INDEX IF NOT EXISTS ids_ina ON $buildRoofSurfIni($geometricColumnBu) USING RTREE; "+
                        "CREATE INDEX IF NOT EXISTS id_ina ON $buildRoofSurfIni($idColumnBu); "+
                        "CREATE INDEX IF NOT EXISTS id_ina ON $buildRoofSurfIni($idColumnRsu); ").toString())

                // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
                // and the corresponding area is estimated (only if the building roof does not overpass the building wall
                // of the neighbor)
                datasource.execute(("CREATE TABLE $buildVertRoofInter(id_build INTEGER, vert_roof_to_remove DOUBLE) AS "+
                        "(SELECT b.$idColumnBu, sum(CASEWHEN(b.building_area>a.building_area, POWER(a.building_area,0.5)*" +
                        "b.delta_h/2, POWER(b.building_area,0.5)*b.delta_h/2)) FROM $buildRoofSurfIni a, " +
                        "$buildRoofSurfIni b WHERE a.$geometricColumnBu && b.$geometricColumnBu" +
                        " AND ST_INTERSECTS(a.$geometricColumnBu, b.$geometricColumnBu) AND a.$idColumnBu <> b.$idColumnBu " +
                        "AND a.z_min >= b.z_max GROUP BY b.$idColumnBu);").toString())

                // Indexes and spatial indexes are created on rsu and building Tables
                datasource.execute(("CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofInter(id_build);").toString())

                // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
                // and the corresponding area is estimated (only if the building roof does not overpass the building wall
                // of the neighbor)
                datasource.execute(("CREATE TABLE $buildVertRoofAll(id_build INTEGER, the_geom GEOMETRY, "+
                        "id_rsu INTEGER, z_max DOUBLE, z_min DOUBLE, delta_h DOUBLE, building_area DOUBLE, " +
                        "building_total_facade_length DOUBLE, non_vertical_roof_area DOUBLE, vertical_roof_area DOUBLE," +
                        " vert_roof_to_remove DOUBLE) AS "+
                        "(SELECT a.$idColumnBu, a.$geometricColumnBu, a.$idColumnRsu, a.z_max, a.z_min," +
                        "a.delta_h, a.building_area, a.building_total_facade_length, a.non_vertical_roof_area, " +
                        "a.vertical_roof_area, ISNULL(b.vert_roof_to_remove,0) FROM $buildRoofSurfIni a LEFT JOIN " +
                        "$buildVertRoofInter b ON a.$idColumnBu=b.$idColumnBu);").toString())

                // Indexes and spatial indexes are created on rsu and building Tables
                datasource.execute(("CREATE INDEX IF NOT EXISTS ids_bu ON $buildVertRoofAll(the_geom) USING RTREE; "+
                        "CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofAll(id_build); "+
                        "CREATE INDEX IF NOT EXISTS id_rsu ON $buildVertRoofAll(id_rsu);" +
                        "CREATE INDEX IF NOT EXISTS ids_rsu ON $rsuTable($geometricColumnRsu) USING RTREE;" +
                        "CREATE INDEX IF NOT EXISTS id_rsu ON $rsuTable(id_rsu);").toString())

                //PEUT-ETRE MIEUX VAUT-IL FAIRE L'INTERSECTION À PART POUR ÉVITER DE LA FAIRE 2 FOIS ICI ?

                // Update the roof areas (vertical and non vertical) taking into account the vertical roofs shared with
                // the neighbor facade and the roof surfaces that are not in the RSU. Note that half of the facade
                // are considered as vertical roofs, the other to "normal roof".
                datasource.execute(("CREATE TABLE $buildRoofSurfTot(id_build INTEGER,"+
                        "id_rsu INTEGER, z_max DOUBLE, z_min DOUBLE, delta_h DOUBLE, non_vertical_roof_area DOUBLE, " +
                        "vertical_roof_area DOUBLE) AS (SELECT a.id_build, a.id_rsu, a.z_max, a.z_min, a.delta_h, " +
                        "a.non_vertical_roof_area*ST_AREA(ST_INTERSECTION(a.the_geom, b.$geometricColumnRsu))/a.building_area " +
                        "AS non_vertical_roof_area, (a.vertical_roof_area-a.vert_roof_to_remove)*" +
                        "(1-0.5*(1-ST_LENGTH(ST_ACCUM(ST_INTERSECTION(ST_TOMULTILINE(a.the_geom), b.$geometricColumnRsu)))/" +
                        "a.building_total_facade_length)) FROM $buildVertRoofAll a, $rsuTable b " +
                        "WHERE a.id_rsu=b.$idColumnRsu GROUP BY a.id_build, a.id_rsu, a.z_max, a.z_min, a.delta_h);").toString())

                // The roof area is calculated for each level except the last one (> 50 m in the default case)
                String finalQuery = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT id_rsu, "
                String nonVertQuery = ""
                String vertQuery = ""
                for (i in 1..(listLayersBottom.size()-1)){
                    nonVertQuery += " SUM(CASEWHEN(z_max <= ${listLayersBottom[i-1]}, 0, CASEWHEN(" +
                            "z_max <= ${listLayersBottom[i]}, CASEWHEN(delta_h=0, non_vertical_roof_area, " +
                            "non_vertical_roof_area*(z_max-GREATEST(${listLayersBottom[i-1]},z_min))/delta_h), " +
                            "CASEWHEN(z_min < ${listLayersBottom[i]}, non_vertical_roof_area*(${listLayersBottom[i]}-" +
                            "GREATEST(${listLayersBottom[i-1]},z_min))/delta_h, 0)))) AS rsu_non_vert_roof_area" +
                            "${listLayersBottom[i-1]}_${listLayersBottom[i]},"
                    vertQuery += " SUM(CASEWHEN(z_max <= ${listLayersBottom[i-1]}, 0, CASEWHEN(" +
                            "z_max <= ${listLayersBottom[i]}, CASEWHEN(delta_h=0, 0, " +
                            "vertical_roof_area*POWER((z_max-GREATEST(${listLayersBottom[i-1]}," +
                            "z_min))/delta_h, 2)), CASEWHEN(z_min < ${listLayersBottom[i]}, " +
                            "CASEWHEN(z_min>${listLayersBottom[i-1]}, vertical_roof_area*(1-" +
                            "POWER((z_max-${listLayersBottom[i]})/delta_h,2)),vertical_roof_area*(" +
                            "POWER((z_max-${listLayersBottom[i-1]})/delta_h,2)-POWER((z_max-${listLayersBottom[i]})/" +
                            "delta_h,2))), 0)))) AS rsu_vert_roof_area${listLayersBottom[i-1]}_${listLayersBottom[i]},"
                }
                // The roof area is calculated for the last level (> 50 m in the default case)
                def valueLastLevel = listLayersBottom[listLayersBottom.size()-1]
                nonVertQuery += " SUM(CASEWHEN(z_max <= $valueLastLevel, 0, CASEWHEN(delta_h=0, non_vertical_roof_area, " +
                        "non_vertical_roof_area*(z_max-GREATEST($valueLastLevel,z_min))/delta_h))) AS rsu_non_vert_roof_area" +
                        "${valueLastLevel}_,"
                vertQuery += " SUM(CASEWHEN(z_max <= $valueLastLevel, 0, CASEWHEN(delta_h=0, vertical_roof_area, " +
                        "vertical_roof_area*(z_max-GREATEST($valueLastLevel,z_min))/delta_h))) AS rsu_vert_roof_area" +
                        "${valueLastLevel}_,"

                String endQuery = " FROM $buildRoofSurfTot GROUP BY id_rsu;"

                datasource.execute((finalQuery+nonVertQuery+vertQuery[0..-2]+endQuery).toString())

                datasource.execute(("DROP TABLE IF EXISTS ${buildRoofSurfIni}, ${buildVertRoofInter}, "+
                        "${buildRoofSurfTot};").toString())

                [outputTableName: outputTableName]
            }
    )}


/**
 * Script to compute the effective terrain roughness (z0). The method for z0 calculation is based on the
 * Hanna and Britter (2010) procedure (see equation (17) and examples of calculation p. 156 in the
 * corresponding reference). It needs the "rsu_projected_facade_area_distribution" and the
 * "rsu_geometric_mean_height" as input data.
 *
 * Warning: the calculation of z0 is only performed for angles included in the range [0, 180[°.
 * To simplify the calculation, z0 is considered as equal for a given orientation independantly of the direction.
 * This assumption is right when the RSU do not split buildings but could slightly overestimate the results
 * otherwise (the real z0 is actually overestimated in one direction but OK in the opposite direction).
 *
 * References:
 * Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 * Hanna, Steven R., and Rex E. Britter. Wind flow and vapor cloud dispersion at industrial and urban sites. Vol. 7. John Wiley & Sons, 2010.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the RSU geometries, the rsu_geometric_mean_height and
 * the rsu_projected_facade_area_distribution fields
 * @param projectedFacadeAreaName the name base used for naming the projected facade area field within the
 * inputA rsuTable (default rsu_projected_facade_area_distribution - note that the field is also constructed
 * with the direction and vertical height informations)
 * @param geometricMeanBuildingHeightName the field name corresponding to the RSU geometric mean height of the
 * roughness elements (buildings, trees, etc.) (in the inputA ITable)
 * @param prefixName String use as prefix to name the output table
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers used for calculation
 * of the rsu_projected_facade_area_density (default [0, 10, 20, 30, 40, 50])
 * @param numberOfDirection the number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 12)
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess effectiveTerrainRoughnessHeight() {
    return processFactory.create(
            "RSU effective terrain roughness height",
            [rsuTable: String, projectedFacadeAreaName: String, geometricMeanBuildingHeightName: String,
             prefixName: String, listLayersBottom: double[], numberOfDirection: int,
             datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, projectedFacadeAreaName = "rsu_projected_facade_area_distribution", geometricMeanBuildingHeightName,
              prefixName, listLayersBottom = [0, 10, 20, 30, 40, 50], numberOfDirection = 12, datasource ->

                logger.info("Executing RSU effective terrain roughness height")

                def geometricColumn = "the_geom"
                def idColumnRsu = "id_rsu"

                // Processes used for the indicator calculation
                // Some local variables are initialized
                def names = []
                // The projection should be performed at the median of the angle interval
                def dirMedDeg = Math.round(180/numberOfDirection)

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def lambdaTable = "lambdaTable" + uid_out

                // The name of the outputTableName is constructed
                String baseName = "rsu_effective_terrain_roughness"
                String outputTableName = prefixName + "_" + baseName

                // The lambda_f indicator is first calculated
                def lambdaQuery = "CREATE TABLE $lambdaTable AS SELECT $idColumnRsu, $geometricMeanBuildingHeightName, ("
                for (int i in 1..listLayersBottom.size()){
                    names[i-1]="$projectedFacadeAreaName${listLayersBottom[i-1]}_${listLayersBottom[i]}"
                    if (i == listLayersBottom.size()){
                        names[listLayersBottom.size()-1]="$projectedFacadeAreaName${listLayersBottom[listLayersBottom.size()-1]}_"
                    }
                    for (int d=0; d<numberOfDirection/2; d++){
                        def dirDeg = d*360/numberOfDirection
                        lambdaQuery += "${names[i-1]}D${dirDeg+dirMedDeg}+"
                    }
                }
                lambdaQuery = lambdaQuery[0..-2] + ")/(${numberOfDirection/2}*ST_AREA($geometricColumn)) " +
                        "AS lambda_f FROM $rsuTable"
                datasource.execute(lambdaQuery.toString())

                // The rugosity z0 is calculated according to the indicator lambda_f (the value of indicator z0 is limited to 3 m)
                datasource.execute(("DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName " +
                        "AS SELECT $idColumnRsu, CASEWHEN(lambda_f < 0.15, CASEWHEN(lambda_f*$geometricMeanBuildingHeightName>3," +
                        "3,lambda_f*$geometricMeanBuildingHeightName), CASEWHEN(0.15*$geometricMeanBuildingHeightName>3,3," +
                        "0.15*$geometricMeanBuildingHeightName)) AS $baseName FROM $lambdaTable").toString())

                datasource.execute("DROP TABLE IF EXISTS $lambdaTable".toString())

                [outputTableName: outputTableName]
            }
    )}

/** Performs operations on the linear of road within the RSU scale objects. Note that when a road is located at
 * the boundary of two RSU, it is arbitrarily attributed to the RSU having the lowest ID in order to not
 * duplicate the corresponding road.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input tables and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the RSU geometries
 * @param roadTable the name of the input ITable where are stored the road geometries
 * @param operations the name of the geospatial variables to calculated using the linear of road:
 *      -> "rsu_road_direction_distribution": The direction of each segment of road is calculated. The percentage of linear of road
 *  * in each range of direction is then calculated (a range is defined - default 30°) for directions
 *  * included in [0, 180[°).
 *      -> "rsu_linear_road_density": linear of road within an area divided by the rsu_area.
 * @param levelConsiderated the indicators can be calculated independantly for each level (0 indicates that the object
 * is on the ground. 1 to 4 indicates that the object is in the air. -4 to -1 value indicates that the object is
 * underground) or not (null). Default [0, 1]
 * @param prefixName String use as prefix to name the output table
 * @param angleRangeSize the range size (in °) of each interval angle used to calculate the distribution of road per
 * direction (should be a divisor of 180 - default 30°)
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess linearRoadOperations() {
    return processFactory.create(
            "Operations on the linear of road",
            [rsuTable: String, roadTable: String, operations: String[], prefixName: String, angleRangeSize: 30,
             levelConsiderated: [0], datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, roadTable, operations, prefixName, angleRangeSize, levelConsiderated,
              datasource ->

                logger.info("Executing Operations on the linear of road")

                def ops = ["rsu_road_direction_distribution", "rsu_linear_road_density"]
                // Test whether the angleRangeSize is a divisor of 180°
                if (180 % angleRangeSize == 0 & 180 / angleRangeSize > 1) {
                    // Test whether the operations filled by the user are OK
                    if(operations == null){
                        logger.error("The parameter operations should not be null")
                    }
                    else if(operations.isEmpty()){
                        logger.error("The parameter operations is empty")
                    }
                    else {
                        // The operation names are transformed into lower case
                        operations.replaceAll({s -> s.toLowerCase()})

                        def opOk = true
                        operations.each { op ->
                            opOk &= ops.contains(op)
                        }
                        if (opOk == true) {
                            // Define the field names of the Abstract tables used
                            def geometricColumnRsu = "the_geom"
                            def idColumnRsu = "id_rsu"
                            def geometricColumnRoad = "the_geom"
                            def zindex = "zindex"

                            // To avoid overwriting the output files of this step, a unique identifier is created
                            def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                            // Temporary table names
                            def roadInter = "roadInter" + uid_out
                            def roadExpl = "roadExpl" + uid_out
                            def roadDistrib = "roadDistrib" + uid_out
                            def roadDens = "roadDens" + uid_out
                            def roadDistTot = "roadDistTot" + uid_out
                            def roadDensTot = "roadDensTot" + uid_out

                            // The name of the outputTableName is constructed
                            String baseName = "rsu_road_linear_properties"
                            String outputTableName = prefixName + "_" + baseName

                            //      1. Whatever are the operations to proceed, this step is done the same way
                            // Only some of the roads are selected according to the level they are located
                            // Initialize some parameters
                            def ifZindex = ""
                            def baseFiltering = "a.$geometricColumnRsu && b.$geometricColumnRoad AND ST_INTERSECTS(a.$geometricColumnRsu," +
                                    "b.$geometricColumnRoad) "
                            def filtering = baseFiltering
                            def nameDens = []
                            def caseQueryDistrib = ""
                            def caseQueryDens = ""

                            if (levelConsiderated != null) {
                                ifZindex = ", b.$zindex AS zindex"
                                filtering = ""
                                levelConsiderated.each { lev ->
                                    filtering += "$baseFiltering AND b.$zindex=$lev OR "
                                }
                                filtering = filtering[0..-4]
                            }
                            def selectionQuery = "DROP TABLE IF EXISTS $outputTableName; DROP TABLE IF EXISTS $roadInter; " +
                                    "CREATE INDEX IF NOT EXISTS ids_r ON $roadTable($geometricColumnRoad) USING RTREE; " +
                                    "CREATE INDEX IF NOT EXISTS ids_u ON $rsuTable($geometricColumnRsu) USING RTREE; " +
                                    "CREATE TABLE $roadInter AS SELECT a.$idColumnRsu AS id_rsu, " +
                                    "ST_AREA(a.$geometricColumnRsu) AS rsu_area, ST_INTERSECTION(a.$geometricColumnRsu, " +
                                    "b.$geometricColumnRoad) AS the_geom $ifZindex FROM $rsuTable a, $roadTable b " +
                                    "WHERE $filtering;"
                            datasource.execute(selectionQuery.toString())

                            // If all roads are considered at the same level...
                            if (levelConsiderated == null) {
                                nameDens.add("rsu_linear_road_density")
                                caseQueryDens = "SUM(ST_LENGTH(the_geom))/rsu_area AS rsu_linear_road_density "
                                for (int d = angleRangeSize; d <= 180; d += angleRangeSize) {
                                    caseQueryDistrib += "SUM(CASEWHEN(azimuth>=${d - angleRangeSize} AND azimuth<$d, length, 0)) AS " +
                                            "rsu_road_direction_distribution_d${d - angleRangeSize}_$d,"
                                }
                            }
                            // If only certain levels are considered independantly
                            else {
                                ifZindex = ", zindex "
                                levelConsiderated.each { lev ->
                                    caseQueryDens += "SUM(CASEWHEN(zindex = $lev, ST_LENGTH(the_geom), 0))/rsu_area " +
                                            "AS rsu_linear_road_density_h${lev.toString().replaceAll('-', 'minus')},"
                                    nameDens.add("rsu_linear_road_density_h${lev.toString().replaceAll('-', 'minus')}")
                                    for (int d = angleRangeSize; d <= 180; d += angleRangeSize) {
                                        caseQueryDistrib += "SUM(CASEWHEN(azimuth>=${d - angleRangeSize} AND azimuth<$d AND " +
                                                "zindex = $lev, length, 0)) AS " +
                                                "rsu_road_direction_distribution_h${lev.toString().replaceAll("-", "minus")}" +
                                                "_d${d - angleRangeSize}_$d,"
                                    }
                                }
                            }

                            //      2. Depending on the operations to proceed, the queries executed during this step will differ
                            // If the road direction distribution is calculated, explode the roads into segments in order to calculate
                            // their length for each azimuth range
                            if (operations.contains("rsu_road_direction_distribution")) {
                                def queryExpl = "DROP TABLE IF EXISTS $roadExpl;" +
                                        "CREATE TABLE $roadExpl AS SELECT id_rsu, the_geom, " +
                                        "CASEWHEN(ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom))>=pi()," +
                                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom))))-180," +
                                        "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom))))) AS azimuth," +
                                        "ST_LENGTH(the_geom) AS length $ifZindex FROM ST_EXPLODE('(SELECT ST_TOMULTISEGMENTS(the_geom)" +
                                        " AS the_geom, id_rsu $ifZindex FROM $roadInter)');"
                                // Calculate the road direction for each direction and optionally level
                                def queryDistrib = queryExpl + "CREATE TABLE $roadDistrib AS SELECT id_rsu, " + caseQueryDistrib[0..-2] +
                                        " FROM $roadExpl GROUP BY id_rsu;" +
                                        "CREATE INDEX IF NOT EXISTS id_u ON $rsuTable($idColumnRsu);" +
                                        "CREATE INDEX IF NOT EXISTS id_d ON $roadDistrib(id_rsu);" +
                                        "DROP TABLE IF EXISTS $roadDistTot; CREATE TABLE $roadDistTot AS SELECT b.* " +
                                        "FROM $rsuTable a LEFT JOIN $roadDistrib b ON a.$idColumnRsu=b.id_rsu;"
                                datasource.execute(queryDistrib.toString())
                                if (!operations.contains("rsu_linear_road_density")) {
                                    datasource.execute("ALTER TABLE $roadDistTot RENAME TO $outputTableName".toString())
                                }
                            }
                            // If the rsu linear density should be calculated
                            if (operations.contains("rsu_linear_road_density")) {
                                String queryDensity = "CREATE TABLE $roadDens AS SELECT id_rsu, " + caseQueryDens[0..-2] +
                                        " FROM $roadInter GROUP BY id_rsu;" +
                                        "CREATE INDEX IF NOT EXISTS id_u ON $rsuTable($idColumnRsu);" +
                                        "CREATE INDEX IF NOT EXISTS id_d ON $roadDens(id_rsu);" +
                                        "DROP TABLE IF EXISTS $roadDensTot; CREATE TABLE $roadDensTot AS SELECT b.* " +
                                        "FROM $rsuTable a LEFT JOIN $roadDens b ON a.$idColumnRsu=b.id_rsu"
                                datasource.execute(queryDensity.toString())
                                if (!operations.contains("rsu_road_direction_distribution")) {
                                    datasource.execute("ALTER TABLE $roadDensTot RENAME TO $outputTableName".toString())
                                }
                            }
                            if (operations.contains("rsu_road_direction_distribution") && operations.contains("rsu_linear_road_density")) {
                                datasource.execute(("CREATE TABLE $outputTableName AS SELECT a.*," +
                                        "b.${nameDens.join(",b.")} FROM $roadDistTot a LEFT JOIN $roadDensTot b " +
                                        "ON a.id_rsu=b.id_rsu").toString())
                            }

                            // NOTE THAT NONE OF THE POTENTIAL NULL VALUES ARE FILLED FOR THE MOMENT...

                            datasource.execute(("DROP TABLE IF EXISTS $roadInter, $roadExpl, $roadDistrib," +
                                    "$roadDens, $roadDistTot, $roadDensTot").toString())
                            [outputTableName: outputTableName]
                        } else {
                            logger.error("One of several operations are not valid.")
                        }
                    }
                } else {
                    logger.error("Cannot compute the indicator. The range size (angleRangeSize) should " +
                            "be a divisor of 180°")
                }
            }
    )}


/**
 * Script to compute the effective terrain class from the effective terrain roughness height (z0).
 * The classes are defined according to the Davenport lookup Table (cf Table 5 in Stewart and Oke, 2012)
 *
 * Warning: the Davenport definition defines a class for a given z0 value. Then there is no definition of the z0 range
 * corresponding to a certain class. Then we have arbitrarily defined the z0 value corresponding to a certain
 * Davenport class as the average of each interval, and the boundary between two classes is defined as the arithmetic
 * average between the z0 values of each class. A definition of the interval based on the profile of class = f(z0)
 * could lead to different results (especially for classes 3, 4 and 5).
 *
 * References:
 * Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the effectiveTerrainRoughnessHeight values
 * @param effectiveTerrainRoughnessHeight the field name corresponding to the RSU effective terrain roughness class due
 * to roughness elements (buildings, trees, etc.) (in the rsuTable)
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess effectiveTerrainRoughnessClass() {
    return processFactory.create(
            "RSU effective terrain roughness class",
            [datasource: JdbcDataSource, rsuTable: String, effectiveTerrainRoughnessHeight: String, prefixName: String],
            [outputTableName : String],
            { datasource, rsuTable, effectiveTerrainRoughnessHeight, prefixName ->

                logger.info("Executing RSU effective terrain roughness class")

                def idColumnRsu = "id_rsu"

                // The name of the outputTableName is constructed
                String baseName = "effective_terrain_roughness_class"
                String outputTableName = prefixName + "_" + baseName

                // Based on the lookup Table of Davenport
                datasource.execute(("DROP TABLE IF EXISTS $outputTableName;" +
                        "CREATE TABLE $outputTableName AS SELECT $idColumnRsu, " +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.0 OR $effectiveTerrainRoughnessHeight IS NULL, null," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.00035, 1," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.01525, 2," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.065, 3," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.175, 4," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.375, 5," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<0.75, 6," +
                        "CASEWHEN($effectiveTerrainRoughnessHeight<1.5, 7, 8)))))))) AS $baseName FROM $rsuTable").toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the vegetation fraction (low, high or total).
 *
 * References:
 * Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the RSU geometries
 * @param vegetTable the name of the input ITable where are stored the vegetation geometries and the height_class
 * @param fractionType the list of type of vegetation fraction to calculate
 *          --> "low": the low vegetation density is calculated
 *          --> "high": the high vegetation density is calculated
 *          --> "all": the total (low + high) vegetation density is calculated
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess vegetationFraction() {
    return processFactory.create(
            "vegetation fraction",
            [rsuTable: String, vegetTable: String, fractionType: String[], prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, vegetTable, fractionType, prefixName, datasource ->

                logger.info("Executing vegetation fraction")

                def geometricColumnRsu = "the_geom"
                def geometricColumnVeget = "the_geom"
                def idColumnRsu = "id_rsu"
                def vegetClass = "height_class"

                // The name of the outputTableName is constructed
                String baseName = "vegetation_fraction"
                String outputTableName = prefixName + "_" + baseName

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def interTable = "interTable" + uid_out
                def buffTable = "buffTable" + uid_out

                // Intersections between vegetation and RSU are calculated
                def interQuery = "DROP TABLE IF EXISTS $interTable; " +
                        "CREATE INDEX IF NOT EXISTS ids_r ON $rsuTable($geometricColumnRsu) USING RTREE; " +
                        "CREATE INDEX IF NOT EXISTS ids_v ON $vegetTable($geometricColumnVeget) USING RTREE;" +
                        "CREATE TABLE $interTable AS SELECT b.$idColumnRsu, a.$vegetClass, " +
                        "ST_AREA(b.$geometricColumnRsu) AS RSU_AREA, " +
                        "ST_AREA(ST_INTERSECTION(a.$geometricColumnVeget, b.$geometricColumnRsu)) AS VEGET_AREA " +
                        "FROM $vegetTable a, $rsuTable b WHERE a.$geometricColumnVeget && b.$geometricColumnRsu AND " +
                        "ST_INTERSECTS(a.$geometricColumnVeget, b.$geometricColumnRsu);"

                // Vegetation Fraction is calculated at RSU scale for different vegetation types
                def buffQuery = interQuery + "DROP TABLE IF EXISTS $buffTable;" +
                        "CREATE INDEX IF NOT EXISTS idi_i ON $interTable($idColumnRsu);" +
                        "CREATE INDEX IF NOT EXISTS idt_i ON $interTable($vegetClass);" +
                        "CREATE TABLE $buffTable AS SELECT $idColumnRsu,"
                def names = []

                // The fraction type names are transformed into lower case
                fractionType.replaceAll({s -> s.toLowerCase()})

                fractionType.each{op ->
                    names.add("${op}_vegetation_fraction")
                    if(op == "low" || op == "high"){
                        buffQuery += "SUM(CASEWHEN($vegetClass = '$op', VEGET_AREA, 0))/RSU_AREA AS ${names[-1]},"
                    }
                    else if(op == "all"){
                        buffQuery += "SUM(VEGET_AREA)/RSU_AREA AS ${names[-1]},"
                    }
                }
                buffQuery = buffQuery[0..-2] + " FROM $interTable GROUP BY $idColumnRsu;"

                def finalQuery = buffQuery + "DROP TABLE IF EXISTS $outputTableName; " +
                        "CREATE INDEX IF NOT EXISTS ids_r ON $buffTable($idColumnRsu); " +
                        "CREATE TABLE $outputTableName($idColumnRsu INTEGER, ${names.join(" DOUBLE DEFAULT 0,")} " +
                        " DOUBLE DEFAULT 0) AS (SELECT a.$idColumnRsu, COALESCE(b.${names.join(",0), COALESCE(b.")},0) " +
                        "FROM $rsuTable a LEFT JOIN $buffTable b ON a.$idColumnRsu = b.$idColumnRsu)"

                datasource.execute finalQuery

                datasource.execute("DROP TABLE IF EXISTS $interTable, $buffTable".toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the road fraction.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the RSU geometries
 * @param roadTable the name of the input ITable where are stored the road geometries and their level (zindex)
 * @param levelToConsiders a map containing the prefix name of the indicator to calculate and as values the
 * zindex of the road to consider for this indicator (e.g. ["underground": [-4,-3,-2,-1]])
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess roadFraction() {
    return processFactory.create(
            "road fraction",
            [rsuTable: String, roadTable: String, levelToConsiders: String[], prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, roadTable, levelToConsiders, prefixName, datasource ->

                logger.info("Executing road fraction")

                def geometricColumnRsu = "the_geom"
                def geometricColumnRoad = "the_geom"
                def idColumnRsu = "id_rsu"
                def zindexRoad = "zindex"
                def widthRoad = "width"

                // The name of the outputTableName is constructed
                String baseName = "road_fraction"
                String outputTableName = prefixName + "_" + baseName

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def surfTable = "surfTable" + uid_out
                def interTable = "interTable" + uid_out
                def buffTable = "buffTable" + uid_out

                def surfQuery = "DROP TABLE IF EXISTS $surfTable; CREATE TABLE $surfTable AS SELECT " +
                        "ST_BUFFER($geometricColumnRoad,$widthRoad/2,'endcap=flat') AS the_geom," +
                        "$zindexRoad FROM $roadTable;"

                // Intersections between road surfaces and RSU are calculated
                def interQuery = "DROP TABLE IF EXISTS $interTable; " +
                        "CREATE INDEX IF NOT EXISTS ids_r ON $rsuTable($geometricColumnRsu) USING RTREE; " +
                        "CREATE INDEX IF NOT EXISTS ids_v ON $surfTable($geometricColumnRoad) USING RTREE;" +
                        "CREATE TABLE $interTable AS SELECT b.$idColumnRsu, a.$zindexRoad, " +
                        "ST_AREA(b.$geometricColumnRsu) AS RSU_AREA, " +
                        "ST_AREA(ST_INTERSECTION(a.$geometricColumnRoad, b.$geometricColumnRsu)) AS ROAD_AREA " +
                        "FROM $surfTable a, $rsuTable b WHERE a.$geometricColumnRoad && b.$geometricColumnRsu AND " +
                        "ST_INTERSECTS(a.$geometricColumnRoad, b.$geometricColumnRsu);"


                // Road fraction is calculated at RSU scale for different road types (combinations of levels)
                def buffQuery = "DROP TABLE IF EXISTS $buffTable;" +
                        "CREATE INDEX IF NOT EXISTS idi_i ON $interTable($idColumnRsu);" +
                        "CREATE INDEX IF NOT EXISTS idt_i ON $interTable($zindexRoad);" +
                        "CREATE TABLE $buffTable AS SELECT $idColumnRsu,"
                def names = []
                levelToConsiders.each{name, levels ->
                    def conditions = ""
                    names.add("${name}_road_fraction")
                    levels.each{lev ->
                        conditions += "$zindexRoad=$lev OR "
                    }
                    buffQuery += "SUM(CASEWHEN(${conditions[0..-4]}, ROAD_AREA, 0))/RSU_AREA AS ${names[-1]},"
                }
                buffQuery = buffQuery[0..-2] + " FROM $interTable GROUP BY $idColumnRsu; "

                def finalQuery = "DROP TABLE IF EXISTS $outputTableName; " +
                        "CREATE INDEX IF NOT EXISTS ids_r ON $buffTable($idColumnRsu); " +
                        "CREATE TABLE $outputTableName($idColumnRsu INTEGER, ${names.join(" DOUBLE DEFAULT 0,")} " +
                        " DOUBLE DEFAULT 0) AS (SELECT a.$idColumnRsu, COALESCE(b.${names.join(",0), COALESCE(b.")},0) " +
                        "FROM $rsuTable a LEFT JOIN $buffTable b ON a.$idColumnRsu = b.$idColumnRsu)"

                datasource.execute((surfQuery+interQuery+buffQuery+finalQuery).toString())

                datasource.execute("DROP TABLE IF EXISTS $surfTable, $interTable, $buffTable".toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the water fraction.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the RSU geometries
 * @param waterTable the name of the input ITable where are stored the water surface geometries
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess waterFraction() {
    return processFactory.create(
            "water fraction",
            [rsuTable: String, waterTable: String, prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, waterTable, prefixName, datasource ->

                logger.info("Executing water fraction")

                def geometricColumnRsu = "the_geom"
                def geometricColumnWater = "the_geom"
                def idColumnRsu = "id_rsu"

                // The name of the outputTableName is constructed
                String baseName = "water_fraction"
                String outputTableName = prefixName + "_" + baseName

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = UUID.randomUUID().toString().replaceAll("-", "_")

                // Temporary table names
                def buffTable = "buffTable" + uid_out

                // Intersections between water and RSU are calculated
                def buffQuery = "DROP TABLE IF EXISTS $buffTable; " +
                        "CREATE INDEX IF NOT EXISTS ids_r ON $rsuTable($geometricColumnRsu) USING RTREE; " +
                        "CREATE INDEX IF NOT EXISTS ids_v ON $waterTable($geometricColumnWater) USING RTREE;" +
                        "CREATE INDEX IF NOT EXISTS ids_v ON $rsuTable($idColumnRsu);" +
                        "CREATE TABLE $buffTable AS SELECT b.$idColumnRsu, " +
                        "SUM(ST_AREA(ST_INTERSECTION(a.$geometricColumnWater, b.$geometricColumnRsu)))" +
                        "/ST_AREA(b.$geometricColumnRsu) AS water_fraction FROM $waterTable a, $rsuTable b " +
                        "WHERE a.$geometricColumnWater && b.$geometricColumnRsu AND " +
                        "ST_INTERSECTS(a.$geometricColumnWater, b.$geometricColumnRsu) GROUP BY b.$idColumnRsu;"

                def finalQuery = "DROP TABLE IF EXISTS $outputTableName; " +
                        "CREATE INDEX IF NOT EXISTS ids_r ON $buffTable($idColumnRsu); " +
                        "CREATE TABLE $outputTableName($idColumnRsu INTEGER, water_fraction DOUBLE DEFAULT 0) AS " +
                        "(SELECT a.$idColumnRsu, COALESCE(b.water_fraction,0) FROM $rsuTable a LEFT JOIN $buffTable b ON " +
                        "a.$idColumnRsu = b.$idColumnRsu)"
                datasource.execute((buffQuery+finalQuery).toString())

                datasource.execute("DROP TABLE IF EXISTS $buffTable".toString())

                [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the pervious and impervious fraction within each RSU of an area from other land fractions.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the land fractions used for the pervious and impervious
 * fractions"
 * @param operationsAndComposition a map containing as key the name of the operations to perform (pervious fraction
 * or impervious fraction) and as value the list of land fractions which constitutes them (and which are stored
 * in the rsuTable).
 *          -> "pervious_fraction": default composed of "low_vegetation_fraction" and "water_fraction"
 *          -> "impervious_fraction": default composed of "road_fraction" only
 * @param prefixName String used as prefix to name the output table
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
static IProcess perviousnessFraction() {
    return processFactory.create(
            "Perviousness fraction",
            [rsuTable: String, operationsAndComposition: String[], prefixName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, operationsAndComposition = ["pervious_fraction" : ["low_vegetation_fraction",
                                                                           "water_fraction"],
                                                    "impervious_fraction" : ["road_fraction"]],
              prefixName, datasource ->

                logger.info("Executing Perviousness fraction")

                def idColumnRsu = "id_rsu"
                def ops = ["pervious_fraction", "impervious_fraction"]

                // The name of the outputTableName is constructed
                String baseName = "perviousness_fraction"
                String outputTableName = prefixName + "_" + baseName

                // The pervious or impervious fractions are calculated
                def query = "DROP TABLE IF EXISTS $outputTableName; " +
                        "CREATE TABLE $outputTableName AS SELECT $idColumnRsu, "

                operationsAndComposition.each{indic, land_fractions ->
                    if(ops.contains(indic.toLowerCase())) {
                        land_fractions.each { lf ->
                            query += "$lf +"
                        }
                        query = query[0..-2] + "AS $indic,"
                    }
                    else{
                        logger.error("$indic is not a valid name (valid names are in $ops).".toString())
                    }
                }
                query = query[0..-2] + " FROM $rsuTable;"

                datasource.execute query
                [outputTableName: outputTableName]
            }
    )
}