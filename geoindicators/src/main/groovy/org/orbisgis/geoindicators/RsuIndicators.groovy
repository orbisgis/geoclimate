package org.orbisgis.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2gisSpatialTable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoindicators geoindicators

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
 * @param buContiguityColumn The name of the column where are stored the building contiguity values (within the
 * building Table)
 * @param buTotalFacadeLengthColumn The name of the column where are stored the building total facade length values
 * (within the building Table)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess freeExternalFacadeDensity() {
    def final GEOMETRIC_FIELD_RSU = "the_geom"
    def final ID_FIELD_RSU = "id_rsu"
    def final HEIGHT_WALL = "height_wall"
    def final BASE_NAME = "rsu_free_external_facade_density"

    return create({
        title "RSU free external facade density"
        inputs buildingTable: String, rsuTable: String, buContiguityColumn: String, buTotalFacadeLengthColumn: String,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTable, rsuTable, buContiguityColumn, buTotalFacadeLengthColumn, prefixName, datasource ->

            info "Executing RSU free external facade density"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            datasource.getSpatialTable(buildingTable).id_rsu.createIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()

            def query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                    "SELECT COALESCE(SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$HEIGHT_WALL)/" +
                    "st_area(b.$GEOMETRIC_FIELD_RSU),0) AS rsu_free_external_facade_density, b.$ID_FIELD_RSU "

            query += " FROM $buildingTable a RIGHT JOIN $rsuTable b " +
                    "ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU GROUP BY b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD_RSU;"

            datasource.execute query

            [outputTableName: outputTableName]
        }
    })
}

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
 * @param rsuBuildingDensityColumn The name of the column where are stored the building density values (within the rsu
 * Table)
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
IProcess groundSkyViewFactor() {
    def final GEOMETRIC_COLUMN_RSU = "the_geom"
    def final GEOMETRIC_COLUMN_BU = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final HEIGHT_WALL = "height_wall"
    def final BASE_NAME = "rsu_ground_sky_view_factor"
    
    return create({
        title "RSU ground sky view factor"
        inputs rsuTable: String, correlationBuildingTable: String, rsuBuildingDensityColumn: String, pointDensity: 0.008D,
                rayLength: 100D, numberOfDirection: 60, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, correlationBuildingTable, rsuBuildingDensityColumn, pointDensity, rayLength, numberOfDirection,
              prefixName, datasource ->

            info "Executing RSU ground sky view factor"

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def ptsRSUtot = "ptsRSUtot$uuid"
            def ptsRSUGrid = "ptsRSUGrid$uuid"
            def ptsRSUtempo = "ptsRSUtempo$uuid"
            def ptsRSUbu = "ptsRSUbu$uuid"
            def ptsRSUfreeall = "ptsRSUfreeall$uuid"
            def randomSample = "randomSample$uuid"
            def svfPts = "svfPts$uuid"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // Create the needed index on input tables and the table that will contain the SVF calculation points
            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()

            datasource.getSpatialTable(correlationBuildingTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(correlationBuildingTable).id_rsu.createIndex()

            def to_start = System.currentTimeMillis()

            datasource.execute "DROP TABLE IF EXISTS $ptsRSUtot; CREATE TABLE $ptsRSUtot (pk serial, " +
                    "the_geom geometry, id_rsu int)"

            // The points used for the SVF calculation should be selected within each RSU
            datasource.eachRow("SELECT * FROM $rsuTable") { row ->
                // Size of the grid mesh used to sample each RSU (based on the building density + 10%) - if the
                // building density exceeds 90%, the LCZ 7 building density is then set to 90%
                def freeAreaDens = Math.max(1 - (row[rsuBuildingDensityColumn] + 0.1), 0.1)
                def gms = (freeAreaDens / pointDensity)**0.5
                // A grid of point is created for each RSU
                datasource.execute "DROP TABLE IF EXISTS $ptsRSUGrid; CREATE TABLE $ptsRSUGrid(pk SERIAL, " +
                        "the_geom GEOMETRY) AS (SELECT null, the_geom " +
                        "FROM ST_MAKEGRIDPOINTS('${row[GEOMETRIC_COLUMN_RSU]}'::GEOMETRY, $gms, $gms))"

                // Grid points included inside the RSU are conserved
                datasource.execute "CREATE INDEX IF NOT EXISTS ids_temp ON $ptsRSUGrid(the_geom) USING RTREE; " +
                        "DROP TABLE IF EXISTS $ptsRSUtempo; CREATE TABLE $ptsRSUtempo AS SELECT a.pk, a.the_geom, " +
                        "${row[ID_COLUMN_RSU]} AS id_rsu FROM $ptsRSUGrid a WHERE a.the_geom && " +
                        "'${row[GEOMETRIC_COLUMN_RSU]}' AND " +
                        "ST_INTERSECTS(a.the_geom, '${row[GEOMETRIC_COLUMN_RSU]}')"

                // If there is no point within the RSU (which could be the case for a long and thin RSU), the SVF
                // is calculated for the centroid of the RSU
                if (datasource.firstRow("SELECT COUNT(*) AS NB FROM $ptsRSUtempo")["NB"] == 0) {
                    datasource.execute "DROP TABLE IF EXISTS $ptsRSUtempo; CREATE TABLE $ptsRSUtempo AS SELECT " +
                            "1 AS pk, ST_CENTROID('${row[GEOMETRIC_COLUMN_RSU]}') AS the_geom, " +
                            "${row[ID_COLUMN_RSU]} AS id_rsu"
                }

                // The grid points intersecting buildings are identified
                datasource.execute "CREATE INDEX IF NOT EXISTS ids_temp ON $ptsRSUtempo(the_geom) USING RTREE; " +
                        "CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUtempo(id_rsu);" +
                        "DROP TABLE IF EXISTS $ptsRSUbu; CREATE TABLE $ptsRSUbu AS SELECT a.pk FROM $ptsRSUtempo a " +
                        "LEFT JOIN $correlationBuildingTable b ON a.id_rsu = b.$ID_COLUMN_RSU WHERE " +
                        "a.the_geom && b.$GEOMETRIC_COLUMN_BU AND ST_INTERSECTS(a.the_geom, b.$GEOMETRIC_COLUMN_BU)"

                // The grid points intersecting buildings are then deleted
                datasource.execute "CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUtempo(pk); " +
                        "CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUbu(pk); DROP TABLE IF EXISTS $ptsRSUfreeall; " +
                        "CREATE TABLE $ptsRSUfreeall(pk SERIAL, the_geom GEOMETRY, id_rsu INT) AS (SELECT null, " +
                        "a.the_geom, a.id_rsu FROM $ptsRSUtempo a LEFT JOIN $ptsRSUbu b ON a.pk=b.pk WHERE b.pk " +
                        "IS NULL)"

                // A random sample of points (of size corresponding to the point density defined by $pointDensity)
                // is drawn in order to have the same density of point in each RSU. It is directly
                // inserted into the Table gathering the SVF points used for all RSU
                datasource.execute "DROP TABLE IF EXISTS $randomSample; INSERT INTO $ptsRSUtot(pk, the_geom, id_rsu) " +
                        "SELECT null, the_geom, id_rsu FROM $ptsRSUfreeall ORDER BY RANDOM() LIMIT " +
                        "(TRUNC(${pointDensity}*ST_AREA('${row[GEOMETRIC_COLUMN_RSU]}'::GEOMETRY)*" +
                        "${(1.0 - row[rsuBuildingDensityColumn])})+1);"
            }

            // The SVF calculation is performed at point scale
            datasource.execute "CREATE INDEX IF NOT EXISTS ids_pts ON $ptsRSUtot(the_geom) USING RTREE; " +
                    "DROP TABLE IF EXISTS $svfPts; CREATE TABLE $svfPts AS SELECT a.pk, a.id_rsu, " +
                    "ST_SVF(ST_GEOMETRYN(a.the_geom,1), ST_ACCUM(ST_UPDATEZ(b.$GEOMETRIC_COLUMN_BU, b.$HEIGHT_WALL)), " +
                    "$rayLength, $numberOfDirection, 5) AS SVF FROM $ptsRSUtot AS a, $correlationBuildingTable " +
                    "AS b WHERE ST_EXPAND(a.the_geom, $rayLength) && b.$GEOMETRIC_COLUMN_BU AND " +
                    "ST_DWITHIN(b.$GEOMETRIC_COLUMN_BU, a.the_geom, $rayLength) GROUP BY a.the_geom"


            // The result of the SVF calculation is averaged at RSU scale and the rsu that do not have
            // buildings in the area of calculation are considered "free sky" (SVF = 1)
            datasource.execute "CREATE INDEX IF NOT EXISTS id_svf ON $svfPts(id_rsu); " +
                    "CREATE INDEX IF NOT EXISTS id_pts ON $ptsRSUtot(id_rsu);" +
                    "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                    "SELECT COALESCE(AVG(a.SVF),1.0) AS rsu_ground_sky_view_factor, b.id_rsu AS $ID_COLUMN_RSU " +
                    "FROM $svfPts a RIGHT JOIN $ptsRSUtot b ON a.id_rsu = b.id_rsu " +
                    "GROUP BY b.id_rsu"

            def tObis = System.currentTimeMillis() - to_start

            info "SVF calculation time: ${tObis / 1000} s"


            // The temporary tables are deleted
            datasource.execute "DROP TABLE IF EXISTS $ptsRSUtot, $ptsRSUGrid, $ptsRSUtempo, $ptsRSUbu, " +
                    "$ptsRSUfreeall, $randomSample"

            [outputTableName: outputTableName]
        }
    })
}

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
 * @param rsuBuildingDensityColumn The name of the column where are stored the building density values (within the rsu
 * Table)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess aspectRatio() {
    def final COLUMN_ID_RSU = "id_rsu"
    def final BASE_NAME = "rsu_aspect_ratio"

    return create({
        title "RSU aspect ratio"
        inputs rsuTable: String, rsuFreeExternalFacadeDensityColumn: String, rsuBuildingDensityColumn: String,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, rsuFreeExternalFacadeDensityColumn, rsuBuildingDensityColumn,
              prefixName, datasource ->

            info "Executing RSU aspect ratio"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            datasource.execute """DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS 
                    SELECT $rsuFreeExternalFacadeDensityColumn/(1-$rsuBuildingDensityColumn) AS 
                    rsu_aspect_ratio, $COLUMN_ID_RSU FROM $rsuTable"""

            [outputTableName: outputTableName]
        }
    })
}

/**
 * Script to compute the distribution of projected facade area within a RSU per vertical layer and direction
 * of analysis (ie. wind or sun direction). Note that the method used is an approximation if the RSU split
 * a building into two parts (the facade included within the RSU is counted half).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable The name of the input ITable where are stored the RSU
 * @param buildingTable The name of the input ITable where are stored the buildings
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20,
 * 30, 40, 50])
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
IProcess projectedFacadeAreaDistribution() {
    def final BASE_NAME = "rsu_projected_facade_area_distribution"
    def final GEOMETRIC_COLUMN_RSU = "the_geom"
    def final GEOMETRIC_COLUMN_BU = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final ID_COLUMN_BU = "id_build"
    def final HEIGHT_WALL = "height_wall"

    return create({
        title "RSU projected facade area distribution"
        inputs buildingTable: String, rsuTable: String, listLayersBottom: [0, 10, 20, 30, 40, 50], numberOfDirection: 12,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTable, rsuTable, listLayersBottom, numberOfDirection, prefixName, datasource ->

            info "Executing RSU projected facade area distribution"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            datasource.getSpatialTable(buildingTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(buildingTable).id_build.createIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()

            if (360 % numberOfDirection == 0 && numberOfDirection % 2 == 0) {

                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names
                def buildingIntersection = "building_intersection$uuid"
                def buildingIntersectionExpl = "building_intersection_expl$uuid"
                def buildingFree = "buildingFree$uuid"
                def buildingLayer = "buildingLayer$uuid"
                def buildingFreeExpl = "buildingFreeExpl$uuid"
                def rsuInter = "rsuInter$uuid"
                def finalIndicator = "finalIndicator$uuid"
                def rsuInterRot = "rsuInterRot$uuid"

                // The projection should be performed at the median of the angle interval
                def dirMedDeg = 180 / numberOfDirection
                def dirMedRad = Math.toRadians(dirMedDeg)

                dirMedDeg = Math.round(dirMedDeg)

                // The list that will store the fields name is initialized
                def names = []

                // Common party walls between buildings are calculated
                datasource.execute "CREATE TABLE $buildingIntersection(pk SERIAL, the_geom GEOMETRY, " +
                        "ID_build_a INTEGER, ID_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS " +
                        "(SELECT NULL, ST_INTERSECTION(ST_MAKEVALID(a.$GEOMETRIC_COLUMN_BU), ST_MAKEVALID(b.$GEOMETRIC_COLUMN_BU)), " +
                        "a.$ID_COLUMN_BU, b.$ID_COLUMN_BU, GREATEST(a.$HEIGHT_WALL,b.$HEIGHT_WALL), " +
                        "LEAST(a.$HEIGHT_WALL,b.$HEIGHT_WALL) FROM $buildingTable AS a, $buildingTable AS b " +
                        "WHERE a.$GEOMETRIC_COLUMN_BU && b.$GEOMETRIC_COLUMN_BU AND " +
                        "ST_INTERSECTS(a.$GEOMETRIC_COLUMN_BU, b.$GEOMETRIC_COLUMN_BU) " +
                        "AND a.$ID_COLUMN_BU <> b.$ID_COLUMN_BU)"

                // Common party walls are converted to multilines and then exploded
                datasource.execute "CREATE TABLE $buildingIntersectionExpl(pk SERIAL, the_geom GEOMETRY, " +
                        "ID_build_a INTEGER, ID_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS " +
                        "(SELECT NULL, ST_TOMULTILINE(the_geom), ID_build_a, ID_build_b, z_max, z_min " +
                        "FROM ST_EXPLODE('(SELECT the_geom AS the_geom, ID_build_a, ID_build_b, z_max, z_min " +
                        "FROM $buildingIntersection)'))"

                // Each free facade is stored TWICE (an intersection could be seen from the point of view of two
                // buildings).
                // Facades of isolated buildings are unioned to free facades of non-isolated buildings which are
                // unioned to free intersection facades. To each facade is affected its corresponding free height
                datasource.execute "CREATE INDEX IF NOT EXISTS id_buint" +
                        " ON ${buildingIntersectionExpl}(ID_build_a); " +
                        "CREATE TABLE $buildingFree(pk SERIAL, the_geom GEOMETRY, z_max DOUBLE, z_min DOUBLE) " +
                        "AS (SELECT NULL, ST_TOMULTISEGMENTS(a.the_geom), a.$HEIGHT_WALL, 0 " +
                        "FROM $buildingTable a WHERE a.$ID_COLUMN_BU NOT IN (SELECT ID_build_a " +
                        "FROM $buildingIntersectionExpl)) UNION ALL (SELECT NULL, " +
                        "ST_TOMULTISEGMENTS(ST_DIFFERENCE(ST_TOMULTILINE(a.$GEOMETRIC_COLUMN_BU), " +
                        "ST_UNION(ST_ACCUM(b.the_geom)))), a.$HEIGHT_WALL, 0 FROM $buildingTable a, " +
                        "$buildingIntersectionExpl b WHERE a.$ID_COLUMN_BU=b.ID_build_a " +
                        "GROUP BY b.ID_build_a) UNION ALL (SELECT NULL, ST_TOMULTISEGMENTS(the_geom) " +
                        "AS the_geom, z_max, z_min FROM $buildingIntersectionExpl WHERE ID_build_a<ID_build_b)"

                // The height of wall is calculated for each intermediate level...
                def layerQuery = "CREATE TABLE $buildingLayer AS SELECT pk, the_geom, "
                for (i in 1..(listLayersBottom.size() - 1)) {
                    names[i - 1] = "rsu_projected_facade_area_distribution${listLayersBottom[i - 1]}" +
                            "_${listLayersBottom[i]}"
                    layerQuery += "CASEWHEN(z_max <= ${listLayersBottom[i - 1]}, 0, " +
                            "CASEWHEN(z_min >= ${listLayersBottom[i]}, " +
                            "0, ${listLayersBottom[i] - listLayersBottom[i - 1]}-" +
                            "GREATEST(${listLayersBottom[i]}-z_max,0)" +
                            "-GREATEST(z_min-${listLayersBottom[i - 1]},0))) AS ${names[i - 1]} ,"
                }

                // ...and for the final level
                names[listLayersBottom.size() - 1] = "rsu_projected_facade_area_distribution" +
                        "${listLayersBottom[listLayersBottom.size() - 1]}_"
                layerQuery += "CASEWHEN(z_max >= ${listLayersBottom[listLayersBottom.size() - 1]}, " +
                        "z_max-GREATEST(z_min,${listLayersBottom[listLayersBottom.size() - 1]}), 0) " +
                        "AS ${names[listLayersBottom.size() - 1]} FROM $buildingFree"
                datasource.execute layerQuery

                // Names and types of all columns are then useful when calling sql queries
                def namesAndType = ""
                def onlyNames = ""
                def onlyNamesB = ""
                for (n in names) {
                    namesAndType += " " + n + " double,"
                    onlyNames += " " + n + ","
                    onlyNamesB += " b." + n + ","
                }

                // Free facades are exploded to multisegments
                datasource.execute "CREATE TABLE $buildingFreeExpl(pk SERIAL, the_geom GEOMETRY, $namesAndType) " +
                        "AS (SELECT NULL, the_geom, ${onlyNames[0..-2]} FROM ST_EXPLODE('$buildingLayer'))"

                // Intersections between free facades and rsu geometries are calculated
                datasource.execute "CREATE SPATIAL INDEX IF NOT EXISTS ids_bufre ON $buildingFreeExpl(the_geom); " +
                        "CREATE TABLE $rsuInter(id_rsu INTEGER, the_geom GEOMETRY, $namesAndType) AS " +
                        "(SELECT a.$ID_COLUMN_RSU, ST_INTERSECTION(ST_MAKEVALID(a.$GEOMETRIC_COLUMN_RSU), ST_MAKEVALID(b.the_geom)), " +
                        "${onlyNamesB[0..-2]} FROM $rsuTable a, $buildingFreeExpl b " +
                        "WHERE a.$GEOMETRIC_COLUMN_RSU && b.the_geom " +
                        "AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_RSU, b.the_geom))"

                // Basic informations are stored in the result Table where will be added all fields
                // corresponding to the distribution
                datasource.execute "CREATE TABLE ${finalIndicator}_0 AS SELECT $ID_COLUMN_RSU FROM $rsuTable"


                // The analysis is then performed for each direction ('numberOfDirection' / 2 because calculation
                // is performed for a direction independently of the "toward")
                for (int d = 0; d < numberOfDirection / 2; d++) {
                    def dirDeg = d * 360 / numberOfDirection
                    def dirRad = Math.toRadians(dirDeg)

                    // Define the field name for each of the directions and vertical layers
                    def namesAndTypeDir = ""
                    def onlyNamesDir = ""
                    def onlyNamesDirB = ""
                    for (n in names) {
                        namesAndTypeDir += " " + n + "D" + (dirDeg + dirMedDeg) + " double,"
                        onlyNamesDir += " " + n + "D" + (dirDeg + dirMedDeg) + ","
                        onlyNamesDirB += " b." + n + "D" + (dirDeg + dirMedDeg) + ","
                    }

                    // To calculate the indicator for a new wind direction, the free facades are rotated
                    datasource.execute "DROP TABLE IF EXISTS $rsuInterRot; " +
                            "CREATE TABLE $rsuInterRot(id_rsu INTEGER, the_geom geometry, ${namesAndType[0..-2]})" +
                            " AS (SELECT id_rsu, ST_ROTATE(the_geom,${dirRad + dirMedRad}), " +
                            "${onlyNames[0..-2]} FROM $rsuInter)"

                    // The projected facade area indicator is calculated according to the free facades table
                    // for each vertical layer for this specific direction "d"
                    def calcQuery = ""
                    for (n in names) {
                        calcQuery += "COALESCE(sum((st_xmax(b.the_geom) - st_xmin(b.the_geom))*b.$n)/2,0) AS " +
                                "${n}D${dirDeg + dirMedDeg}, "
                    }
                    datasource.execute "CREATE INDEX IF NOT EXISTS id_rint ON $rsuInterRot(id_rsu); " +
                            "CREATE INDEX IF NOT EXISTS id_fin ON ${finalIndicator}_$d(id_rsu); " +
                            "CREATE TABLE ${finalIndicator}_${d + 1} AS SELECT a.*, ${calcQuery[0..-3]} " +
                            "FROM ${finalIndicator}_$d a LEFT JOIN $rsuInterRot b " +
                            "ON a.id_rsu = b.ID_RSU GROUP BY a.id_rsu"
                }

                datasource.execute "DROP TABLE IF EXISTS $outputTableName; ALTER TABLE " +
                        "${finalIndicator}_${numberOfDirection / 2} RENAME TO $outputTableName"

                // Remove all temporary tables created
                def removeQuery = " ${finalIndicator}_0"
                for (int d = 0; d < numberOfDirection / 2; d++) {
                    removeQuery += ", ${finalIndicator}_$d"
                }
                datasource.execute "DROP TABLE IF EXISTS ${buildingIntersection}, ${buildingIntersectionExpl}, " +
                        "${buildingFree}, ${buildingFreeExpl}, ${rsuInter}, $removeQuery;"
            }

            [outputTableName: outputTableName]
        }
    })
}

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
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20,
 * 30, 40, 50])
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
IProcess roofAreaDistribution() {
    def final GEOMETRIC_COLUMN_RSU = "the_geom"
    def final GEOMETRIC_COLUMN_BU = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final ID_COLUMN_BU = "id_build"
    def final HEIGHT_WALL = "height_wall"
    def final HEIGHT_ROOF = "height_roof"
    def final BASE_NAME = "rsu_roof_area_distribution"

    return create({
        title "RSU roof area distribution"
        inputs rsuTable: String, buildingTable: String, listLayersBottom: [0, 10, 20, 30, 40, 50], prefixName: String,
                datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, buildingTable, listLayersBottom, prefixName, datasource ->

            info "Executing RSU roof area distribution"

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def buildRoofSurfIni = "build_roof_surf_ini$uuid"
            def buildVertRoofInter = "build_vert_roof_inter$uuid"
            def buildVertRoofAll = "buildVertRoofAll$uuid"
            def buildRoofSurfTot = "build_roof_surf_tot$uuid"

            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // Vertical and non-vertical (tilted and horizontal) roof areas are calculated
            datasource.execute "CREATE TABLE $buildRoofSurfIni AS SELECT $GEOMETRIC_COLUMN_BU, $ID_COLUMN_RSU," +
                    "$ID_COLUMN_BU, $HEIGHT_ROOF AS z_max, $HEIGHT_WALL AS z_min, ST_AREA($GEOMETRIC_COLUMN_BU)" +
                    " AS building_area, ST_PERIMETER($GEOMETRIC_COLUMN_BU)+" +
                    "ST_PERIMETER(ST_HOLES($GEOMETRIC_COLUMN_BU))" +
                    " AS building_total_facade_length, $HEIGHT_ROOF-$HEIGHT_WALL" +
                    " AS delta_h, POWER(POWER(ST_AREA($GEOMETRIC_COLUMN_BU),2)+4*" +
                    "ST_AREA($GEOMETRIC_COLUMN_BU)*POWER($HEIGHT_ROOF-$HEIGHT_WALL,2),0.5)" +
                    " AS non_vertical_roof_area," +
                    "POWER(ST_AREA($GEOMETRIC_COLUMN_BU), 0.5)*($HEIGHT_ROOF-$HEIGHT_WALL) AS vertical_roof_area" +
                    " FROM $buildingTable;"


            // Indexes and spatial indexes are created on rsu and building Tables
            datasource.execute "CREATE INDEX IF NOT EXISTS ids_ina ON $buildRoofSurfIni($GEOMETRIC_COLUMN_BU) " +
                    "USING RTREE; " +
                    "CREATE INDEX IF NOT EXISTS id_ina ON $buildRoofSurfIni($ID_COLUMN_BU); " +
                    "CREATE INDEX IF NOT EXISTS id_ina ON $buildRoofSurfIni($ID_COLUMN_RSU); "

            // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
            // and the corresponding area is estimated (only if the building roof does not overpass the building
            // wall of the neighbor)
            datasource.execute "CREATE TABLE $buildVertRoofInter(id_build INTEGER, vert_roof_to_remove DOUBLE) AS " +
                    "(SELECT b.$ID_COLUMN_BU, sum(CASEWHEN(b.building_area>a.building_area, " +
                    "POWER(a.building_area,0.5)*b.delta_h/2, POWER(b.building_area,0.5)*b.delta_h/2)) " +
                    "FROM $buildRoofSurfIni a, $buildRoofSurfIni b WHERE a.$GEOMETRIC_COLUMN_BU && " +
                    "b.$GEOMETRIC_COLUMN_BU AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_BU, b.$GEOMETRIC_COLUMN_BU) " +
                    "AND a.$ID_COLUMN_BU <> b.$ID_COLUMN_BU AND a.z_min >= b.z_max GROUP BY b.$ID_COLUMN_BU);"

            // Indexes and spatial indexes are created on rsu and building Tables
            datasource.execute "CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofInter(id_build);"

            // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
            // and the corresponding area is estimated (only if the building roof does not overpass the building wall
            // of the neighbor)
            datasource.execute "CREATE TABLE $buildVertRoofAll(id_build INTEGER, the_geom GEOMETRY, " +
                    "id_rsu INTEGER, z_max DOUBLE, z_min DOUBLE, delta_h DOUBLE, building_area DOUBLE, " +
                    "building_total_facade_length DOUBLE, non_vertical_roof_area DOUBLE, vertical_roof_area DOUBLE," +
                    " vert_roof_to_remove DOUBLE) AS " +
                    "(SELECT a.$ID_COLUMN_BU, a.$GEOMETRIC_COLUMN_BU, a.$ID_COLUMN_RSU, a.z_max, a.z_min," +
                    "a.delta_h, a.building_area, a.building_total_facade_length, a.non_vertical_roof_area, " +
                    "a.vertical_roof_area, ISNULL(b.vert_roof_to_remove,0) FROM $buildRoofSurfIni a LEFT JOIN " +
                    "$buildVertRoofInter b ON a.$ID_COLUMN_BU=b.$ID_COLUMN_BU);"

            // Indexes and spatial indexes are created on rsu and building Tables
            datasource.execute "CREATE INDEX IF NOT EXISTS ids_bu ON $buildVertRoofAll(the_geom) USING RTREE; " +
                    "CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofAll(id_build); " +
                    "CREATE INDEX IF NOT EXISTS id_rsu ON $buildVertRoofAll(id_rsu);"

            //PEUT-ETRE MIEUX VAUT-IL FAIRE L'INTERSECTION À PART POUR ÉVITER DE LA FAIRE 2 FOIS ICI ?

            // Update the roof areas (vertical and non vertical) taking into account the vertical roofs shared with
            // the neighbor facade and the roof surfaces that are not in the RSU. Note that half of the facade
            // are considered as vertical roofs, the other to "normal roof".
            datasource.execute "CREATE TABLE $buildRoofSurfTot(id_build INTEGER," +
                    "id_rsu INTEGER, z_max DOUBLE, z_min DOUBLE, delta_h DOUBLE, non_vertical_roof_area DOUBLE, " +
                    "vertical_roof_area DOUBLE) AS (SELECT a.id_build, a.id_rsu, a.z_max, a.z_min, a.delta_h, " +
                    "a.non_vertical_roof_area*ST_AREA(ST_INTERSECTION(a.the_geom, b.$GEOMETRIC_COLUMN_RSU))/a.building_area " +
                    "AS non_vertical_roof_area, (a.vertical_roof_area-a.vert_roof_to_remove)*" +
                    "(1-0.5*(1-ST_LENGTH(ST_ACCUM(ST_INTERSECTION(ST_TOMULTILINE(a.the_geom), b.$GEOMETRIC_COLUMN_RSU)))/" +
                    "a.building_total_facade_length)) FROM $buildVertRoofAll a, $rsuTable b " +
                    "WHERE a.id_rsu=b.$ID_COLUMN_RSU GROUP BY b.$GEOMETRIC_COLUMN_RSU, a.id_build, a.id_rsu, a.z_max, a.z_min, a.delta_h);"

            // The roof area is calculated for each level except the last one (> 50 m in the default case)
            def finalQuery = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT id_rsu, "
            def nonVertQuery = ""
            def vertQuery = ""
            for (i in 1..(listLayersBottom.size() - 1)) {
                nonVertQuery += " COALESCE(SUM(CASEWHEN(z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(" +
                        "z_max <= ${listLayersBottom[i]}, CASEWHEN(delta_h=0, non_vertical_roof_area, " +
                        "non_vertical_roof_area*(z_max-GREATEST(${listLayersBottom[i - 1]},z_min))/delta_h), " +
                        "CASEWHEN(z_min < ${listLayersBottom[i]}, non_vertical_roof_area*(${listLayersBottom[i]}-" +
                        "GREATEST(${listLayersBottom[i - 1]},z_min))/delta_h, 0)))),0) AS rsu_non_vert_roof_area" +
                        "${listLayersBottom[i - 1]}_${listLayersBottom[i]},"
                vertQuery += " COALESCE(SUM(CASEWHEN(z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(" +
                        "z_max <= ${listLayersBottom[i]}, CASEWHEN(delta_h=0, 0, " +
                        "vertical_roof_area*POWER((z_max-GREATEST(${listLayersBottom[i - 1]}," +
                        "z_min))/delta_h, 2)), CASEWHEN(z_min < ${listLayersBottom[i]}, " +
                        "CASEWHEN(z_min>${listLayersBottom[i - 1]}, vertical_roof_area*(1-" +
                        "POWER((z_max-${listLayersBottom[i]})/delta_h,2)),vertical_roof_area*(" +
                        "POWER((z_max-${listLayersBottom[i - 1]})/delta_h,2)-POWER((z_max-${listLayersBottom[i]})/" +
                        "delta_h,2))), 0)))),0) AS rsu_vert_roof_area${listLayersBottom[i - 1]}_${listLayersBottom[i]},"
            }
            // The roof area is calculated for the last level (> 50 m in the default case)
            def valueLastLevel = listLayersBottom[listLayersBottom.size() - 1]
            nonVertQuery += " COALESCE(SUM(CASEWHEN(z_max <= $valueLastLevel, 0, CASEWHEN(delta_h=0, non_vertical_roof_area, " +
                    "non_vertical_roof_area*(z_max-GREATEST($valueLastLevel,z_min))/delta_h))),0) AS rsu_non_vert_roof_area" +
                    "${valueLastLevel}_,"
            vertQuery += " COALESCE(SUM(CASEWHEN(z_max <= $valueLastLevel, 0, CASEWHEN(delta_h=0, vertical_roof_area, " +
                    "vertical_roof_area*(z_max-GREATEST($valueLastLevel,z_min))/delta_h))),0) AS rsu_vert_roof_area" +
                    "${valueLastLevel}_,"

            def endQuery = " FROM $buildRoofSurfTot GROUP BY id_rsu;"

            datasource.execute finalQuery + nonVertQuery + vertQuery[0..-2] + endQuery

            datasource.execute "DROP TABLE IF EXISTS ${buildRoofSurfIni}, ${buildVertRoofInter}, " +
                    "${buildRoofSurfTot};"

            [outputTableName: outputTableName]
        }
    })
}


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
 * Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American
 * Meteorological Society 93, no. 12 (2012): 1879-1900.
 * Hanna, Steven R., and Rex E. Britter. Wind flow and vapor cloud dispersion at industrial and urban sites. Vol. 7.
 * John Wiley & Sons, 2010.
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
IProcess effectiveTerrainRoughnessHeight() {
    def final GEOMETRIC_COLUMN = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final BASE_NAME = "rsu_effective_terrain_roughness"

    return create({
        title "RSU effective terrain roughness height"
        inputs rsuTable: String, projectedFacadeAreaName: "rsu_projected_facade_area_distribution",
                geometricMeanBuildingHeightName: String, prefixName: String,
                listLayersBottom: [0, 10, 20, 30, 40, 50], numberOfDirection: 12, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, projectedFacadeAreaName, geometricMeanBuildingHeightName, prefixName, listLayersBottom,
              numberOfDirection, datasource ->

            info "Executing RSU effective terrain roughness height"

            // Processes used for the indicator calculation
            // Some local variables are initialized
            def names = []
            // The projection should be performed at the median of the angle interval
            def dirMedDeg = Math.round(180 / numberOfDirection)

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def lambdaTable = "lambdaTable$uuid"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // The lambda_f indicator is first calculated
            def lambdaQuery = "CREATE TABLE $lambdaTable AS SELECT $ID_COLUMN_RSU, $geometricMeanBuildingHeightName, ("
            for (int i in 1..listLayersBottom.size()) {
                names[i - 1] = "$projectedFacadeAreaName${listLayersBottom[i - 1]}_${listLayersBottom[i]}"
                if (i == listLayersBottom.size()) {
                    names[listLayersBottom.size() - 1] =
                            "$projectedFacadeAreaName${listLayersBottom[listLayersBottom.size() - 1]}_"
                }
                for (int d = 0; d < numberOfDirection / 2; d++) {
                    def dirDeg = d * 360 / numberOfDirection
                    lambdaQuery += "${names[i - 1]}D${dirDeg + dirMedDeg}+"
                }
            }
            lambdaQuery = lambdaQuery[0..-2] + ")/(${numberOfDirection / 2}*ST_AREA($GEOMETRIC_COLUMN)) " +
                    "AS lambda_f FROM $rsuTable"
            datasource.execute lambdaQuery

            // The rugosity z0 is calculated according to the indicator lambda_f (the value of indicator z0 is limited to 3 m)
            datasource.execute "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName " +
                    "AS SELECT $ID_COLUMN_RSU, CASEWHEN(lambda_f < 0.15, CASEWHEN(lambda_f*$geometricMeanBuildingHeightName>3," +
                    "3,lambda_f*$geometricMeanBuildingHeightName), CASEWHEN(0.15*$geometricMeanBuildingHeightName>3,3," +
                    "0.15*$geometricMeanBuildingHeightName)) AS $BASE_NAME FROM $lambdaTable"

            datasource.execute "DROP TABLE IF EXISTS $lambdaTable"

            [outputTableName: outputTableName]
        }
    })
}

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
 * underground) or not (null). Default [0]
 * @param prefixName String use as prefix to name the output table
 * @param angleRangeSize the range size (in °) of each interval angle used to calculate the distribution of road per
 * direction (should be a divisor of 180 - default 30°)
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
IProcess linearRoadOperations() {
    def final OPS = ["rsu_road_direction_distribution", "rsu_linear_road_density"]
    def final GEOMETRIC_COLUMN_RSU = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final GEOMETRIC_COLUMN_ROAD = "the_geom"
    def final Z_INDEX = "zindex"
    def final BASE_NAME = "rsu_road_linear_properties"

    return create({
        title "Operations on the linear of road"
        inputs rsuTable: String, roadTable: String, operations: String[], prefixName: String, angleRangeSize: 30,
                levelConsiderated: [0], datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, roadTable, operations, prefixName, angleRangeSize, levelConsiderated,
              datasource ->

            info "Executing Operations on the linear of road"

            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()
            datasource.getSpatialTable(roadTable).the_geom.createSpatialIndex()

            // Test whether the angleRangeSize is a divisor of 180°
            if (180 % angleRangeSize == 0 && 180 / angleRangeSize > 1) {
                // Test whether the operations filled by the user are OK
                if (operations == null) {
                    error "The parameter operations should not be null"
                } else if (operations.isEmpty()) {
                    error "The parameter operations is empty"
                } else {
                    // The operation names are transformed into lower case
                    operations.replaceAll({ s -> s.toLowerCase() })

                    def opOk = true
                    operations.each { op ->
                        opOk &= OPS.contains(op)
                    }
                    if (opOk) {
                        // To avoid overwriting the output files of this step, a unique identifier is created
                        // Temporary table names
                        def roadInter = "roadInter$uuid"
                        def roadExpl = "roadExpl$uuid"
                        def roadDistrib = "roadDistrib$uuid"
                        def roadDens = "roadDens$uuid"
                        def roadDistTot = "roadDistTot$uuid"
                        def roadDensTot = "roadDensTot$uuid"

                        // The name of the outputTableName is constructed
                        def outputTableName = prefixName + "_" + BASE_NAME

                        //      1. Whatever are the operations to proceed, this step is done the same way
                        // Only some of the roads are selected according to the level they are located
                        // Initialize some parameters
                        def ifZindex = ""
                        def baseFiltering = "a.$GEOMETRIC_COLUMN_RSU && b.$GEOMETRIC_COLUMN_ROAD AND " +
                                "ST_INTERSECTS(a.$GEOMETRIC_COLUMN_RSU,b.$GEOMETRIC_COLUMN_ROAD) "
                        def filtering = baseFiltering
                        def nameDens = []
                        def nameDistrib = []
                        def caseQueryDistrib = ""
                        def caseQueryDens = ""

                        if (levelConsiderated != null) {
                            ifZindex = ", b.$Z_INDEX AS zindex"
                            filtering = ""
                            levelConsiderated.each { lev ->
                                filtering += "$baseFiltering AND b.$Z_INDEX=$lev OR "
                            }
                            filtering = filtering[0..-4]
                        }
                        def selectionQuery = "DROP TABLE IF EXISTS $outputTableName; DROP TABLE IF EXISTS $roadInter; " +
                                "CREATE TABLE $roadInter AS SELECT a.$ID_COLUMN_RSU AS id_rsu, " +
                                "ST_AREA(a.$GEOMETRIC_COLUMN_RSU) AS rsu_area, ST_INTERSECTION(a.$GEOMETRIC_COLUMN_RSU, " +
                                "b.$GEOMETRIC_COLUMN_ROAD) AS the_geom $ifZindex FROM $rsuTable a, $roadTable b " +
                                "WHERE $filtering;"
                        datasource.execute selectionQuery

                        // If all roads are considered at the same level...
                        if (levelConsiderated == null) {
                            nameDens.add("rsu_linear_road_density")
                            caseQueryDens = "SUM(ST_LENGTH(the_geom))/rsu_area AS rsu_linear_road_density "
                            for (int d = angleRangeSize; d <= 180; d += angleRangeSize) {
                                caseQueryDistrib += "SUM(CASEWHEN(azimuth>=${d - angleRangeSize} AND azimuth<$d, length, 0)) AS " +
                                        "rsu_road_direction_distribution_d${d - angleRangeSize}_$d,"
                                nameDistrib.add("rsu_road_direction_distribution_d${d - angleRangeSize}_$d")
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
                                    nameDistrib.add("rsu_road_direction_distribution_h${lev.toString().replaceAll("-", "minus")}" +
                                            "_d${d - angleRangeSize}_$d")
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
                                    "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(the_geom), " +
                                    "ST_ENDPOINT(the_geom))))-180," +
                                    "ROUND(DEGREES(ST_AZIMUTH(ST_STARTPOINT(the_geom), " +
                                    "ST_ENDPOINT(the_geom))))) AS azimuth," +
                                    "ST_LENGTH(the_geom) AS length $ifZindex " +
                                    "FROM ST_EXPLODE('(SELECT ST_TOMULTISEGMENTS(the_geom)" +
                                    " AS the_geom, id_rsu $ifZindex FROM $roadInter)');"
                            // Calculate the road direction for each direction and optionally level
                            def queryDistrib = queryExpl + "CREATE TABLE $roadDistrib AS SELECT id_rsu, " +
                                    caseQueryDistrib[0..-2] +
                                    " FROM $roadExpl GROUP BY id_rsu;" +
                                    "CREATE INDEX IF NOT EXISTS id_d ON $roadDistrib(id_rsu);" +
                                    "DROP TABLE IF EXISTS $roadDistTot; CREATE TABLE $roadDistTot($ID_COLUMN_RSU INTEGER," +
                                    "${nameDistrib.join(" double,")} double) AS (SELECT a.$ID_COLUMN_RSU," +
                                    "COALESCE(b.${nameDistrib.join(",0),COALESCE(b.")},0)  " +
                                    "FROM $rsuTable a LEFT JOIN $roadDistrib b ON a.$ID_COLUMN_RSU=b.id_rsu);"
                            datasource.execute queryDistrib

                            if (!operations.contains("rsu_linear_road_density")) {
                                datasource.execute "ALTER TABLE $roadDistTot RENAME TO $outputTableName"
                            }
                        }

                        // If the rsu linear density should be calculated
                        if (operations.contains("rsu_linear_road_density")) {
                            String queryDensity = "CREATE TABLE $roadDens AS SELECT id_rsu, " + caseQueryDens[0..-2] +
                                    " FROM $roadInter GROUP BY id_rsu;" +
                                    "CREATE INDEX IF NOT EXISTS id_d ON $roadDens(id_rsu);" +
                                    "DROP TABLE IF EXISTS $roadDensTot; CREATE TABLE $roadDensTot($ID_COLUMN_RSU INTEGER," +
                                    "${nameDens.join(" double,")} double) AS (SELECT a.$ID_COLUMN_RSU," +
                                    "COALESCE(b.${nameDens.join(",0),COALESCE(b.")},0) " +
                                    "FROM $rsuTable a LEFT JOIN $roadDens b ON a.$ID_COLUMN_RSU=b.id_rsu)"
                            datasource.execute queryDensity
                            if (!operations.contains("rsu_road_direction_distribution")) {
                                datasource.execute "ALTER TABLE $roadDensTot RENAME TO $outputTableName"
                            }
                        }
                        if (operations.contains("rsu_road_direction_distribution") &&
                                operations.contains("rsu_linear_road_density")) {
                            datasource.execute "CREATE TABLE $outputTableName AS SELECT a.*," +
                                    "b.${nameDens.join(",b.")} FROM $roadDistTot a LEFT JOIN $roadDensTot b " +
                                    "ON a.id_rsu=b.id_rsu"
                        }

                        datasource.execute "DROP TABLE IF EXISTS $roadInter, $roadExpl, $roadDistrib," +
                                "$roadDens, $roadDistTot, $roadDensTot"

                        [outputTableName: outputTableName]

                    } else {
                        error "One of several operations are not valid."
                    }
                }
            } else {
                error "Cannot compute the indicator. The range size (angleRangeSize) should be a divisor of 180°"
            }
        }
    })
}


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
IProcess effectiveTerrainRoughnessClass() {
    def final ID_COLUMN_RSU = "id_rsu"
    def final BASE_NAME = "effective_terrain_roughness_class"

    return create({
        title "RSU effective terrain roughness class"
        inputs datasource: JdbcDataSource, rsuTable: String, effectiveTerrainRoughnessHeight: String, prefixName: String
        outputs outputTableName: String
        run { datasource, rsuTable, effectiveTerrainRoughnessHeight, prefixName ->

            info "Executing RSU effective terrain roughness class"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // Based on the lookup Table of Davenport
            datasource.execute "DROP TABLE IF EXISTS $outputTableName;" +
                    "CREATE TABLE $outputTableName AS SELECT $ID_COLUMN_RSU, " +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.0 OR $effectiveTerrainRoughnessHeight IS NULL, null," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.00035, 1," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.01525, 2," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.065, 3," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.175, 4," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.375, 5," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<0.75, 6," +
                    "CASEWHEN($effectiveTerrainRoughnessHeight<1.5, 7, 8)))))))) AS $BASE_NAME FROM $rsuTable"

            [outputTableName: outputTableName]
        }
    })
}

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
IProcess vegetationFraction() {
    def final geometricColumnRsu = "the_geom"
    def final geometricColumnVeget = "the_geom"
    def final idColumnRsu = "id_rsu"
    def final vegetClass = "height_class"
    def final baseName = "vegetation_fraction"
    def final OP_LOW = "low"
    def final OP_HIGH = "high"
    def final OP_ALL = "all"

    return create({
        title "vegetation fraction"
        inputs rsuTable: String, vegetTable: String, fractionType: String[], prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, vegetTable, fractionType, prefixName, datasource ->

            info "Executing vegetation fraction"

            // The name of the outputTableName is constructed
            String outputTableName = prefixName + "_" + baseName

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def interTable = "interTable$uuid"
            def buffTable = "buffTable$uuid"


            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()
            datasource.getSpatialTable(vegetTable).the_geom.createSpatialIndex()

            // Intersections between vegetation and RSU are calculated
            def interQuery = "DROP TABLE IF EXISTS $interTable; " +
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
            fractionType.replaceAll({ s -> s.toLowerCase() })

            fractionType.each { op ->
                names.add("${op}_vegetation_fraction")
                if (op == OP_LOW || op == OP_HIGH) {
                    buffQuery += "SUM(CASEWHEN($vegetClass = '$op', VEGET_AREA, 0))/RSU_AREA AS ${names[-1]},"
                } else if (op == OP_ALL) {
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

            datasource.execute "DROP TABLE IF EXISTS $interTable, $buffTable"

            [outputTableName: outputTableName]
        }
    })
}

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
IProcess roadFraction() {
    def final GEOMETRIC_COLUMN_RSU = "the_geom"
    def final GEOMETRIC_COLUMN_ROAD = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final Z_INDEX_ROAD = "zindex"
    def final WIDTH_ROAD = "width"
    def final BASE_NAME = "road_fraction"

    return create({
        title "road fraction"
        inputs rsuTable: String, roadTable: String, levelToConsiders: String[], prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, roadTable, levelToConsiders, prefixName, datasource ->

            info "Executing road fraction"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def surfTable = "surfTable$uuid"
            def interTable = "interTable$uuid"
            def buffTable = "buffTable$uuid"

            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(roadTable).the_geom.createSpatialIndex()

            def surfQuery = "DROP TABLE IF EXISTS $surfTable; CREATE TABLE $surfTable AS SELECT " +
                    "ST_BUFFER($GEOMETRIC_COLUMN_ROAD,$WIDTH_ROAD/2,'endcap=flat') AS the_geom," +
                    "$Z_INDEX_ROAD FROM $roadTable;"

            // Intersections between road surfaces and RSU are calculated
            def interQuery = "DROP TABLE IF EXISTS $interTable; " +
                    "CREATE INDEX IF NOT EXISTS ids_v ON $surfTable($GEOMETRIC_COLUMN_ROAD) USING RTREE;" +
                    "CREATE TABLE $interTable AS SELECT b.$ID_COLUMN_RSU, a.$Z_INDEX_ROAD, " +
                    "ST_AREA(b.$GEOMETRIC_COLUMN_RSU) AS RSU_AREA, " +
                    "ST_AREA(ST_INTERSECTION(a.$GEOMETRIC_COLUMN_ROAD, b.$GEOMETRIC_COLUMN_RSU)) AS ROAD_AREA " +
                    "FROM $surfTable a, $rsuTable b WHERE a.$GEOMETRIC_COLUMN_ROAD && b.$GEOMETRIC_COLUMN_RSU AND " +
                    "ST_INTERSECTS(a.$GEOMETRIC_COLUMN_ROAD, b.$GEOMETRIC_COLUMN_RSU);"


            // Road fraction is calculated at RSU scale for different road types (combinations of levels)
            def buffQuery = "DROP TABLE IF EXISTS $buffTable;" +
                    "CREATE INDEX IF NOT EXISTS idi_i ON $interTable($ID_COLUMN_RSU);" +
                    "CREATE INDEX IF NOT EXISTS idt_i ON $interTable($Z_INDEX_ROAD);" +
                    "CREATE TABLE $buffTable AS SELECT $ID_COLUMN_RSU,"
            def names = []
            levelToConsiders.each { name, levels ->
                def conditions = ""
                names.add("${name}_road_fraction")
                levels.each { lev ->
                    conditions += "$Z_INDEX_ROAD=$lev OR "
                }
                buffQuery += "SUM(CASEWHEN(${conditions[0..-4]}, ROAD_AREA, 0))/RSU_AREA AS ${names[-1]},"
            }
            buffQuery = buffQuery[0..-2] + " FROM $interTable GROUP BY $ID_COLUMN_RSU; "

            def finalQuery = "DROP TABLE IF EXISTS $outputTableName; " +
                    "CREATE INDEX IF NOT EXISTS ids_r ON $buffTable($ID_COLUMN_RSU); " +
                    "CREATE TABLE $outputTableName($ID_COLUMN_RSU INTEGER, ${names.join(" DOUBLE DEFAULT 0,")} " +
                    " DOUBLE DEFAULT 0) AS (SELECT a.$ID_COLUMN_RSU, COALESCE(b.${names.join(",0), COALESCE(b.")},0) " +
                    "FROM $rsuTable a LEFT JOIN $buffTable b ON a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU)"

            datasource.execute surfQuery + interQuery + buffQuery + finalQuery
            datasource.execute "DROP TABLE IF EXISTS $surfTable, $interTable, $buffTable"

            [outputTableName: outputTableName]
        }
    })
}

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
IProcess waterFraction() {
    def final GEOMETRIC_COLUMN_RSU = "the_geom"
    def final GEOMETRIC_COLUMN_WATER = "the_geom"
    def final ID_COLUMN_RSU = "id_rsu"
    def final BASE_NAME = "water_fraction"

    return create({
        title "water fraction"
        inputs rsuTable: String, waterTable: String, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, waterTable, prefixName, datasource ->

            info "Executing water fraction"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def buffTable = "buffTable$uuid"


            datasource.getSpatialTable(rsuTable).the_geom.createSpatialIndex()
            datasource.getSpatialTable(rsuTable).id_rsu.createIndex()
            datasource.getSpatialTable(waterTable).the_geom.createSpatialIndex()

            // Intersections between water and RSU are calculated
            def buffQuery = "DROP TABLE IF EXISTS $buffTable; " +
                    "CREATE TABLE $buffTable AS SELECT b.$ID_COLUMN_RSU, " +
                    "SUM(ST_AREA(ST_INTERSECTION(a.$GEOMETRIC_COLUMN_WATER, b.$GEOMETRIC_COLUMN_RSU)))" +
                    "/ST_AREA(b.$GEOMETRIC_COLUMN_RSU) AS water_fraction FROM $waterTable a, $rsuTable b " +
                    "WHERE a.$GEOMETRIC_COLUMN_WATER && b.$GEOMETRIC_COLUMN_RSU AND " +
                    "ST_INTERSECTS(a.$GEOMETRIC_COLUMN_WATER, b.$GEOMETRIC_COLUMN_RSU) GROUP BY b.$ID_COLUMN_RSU;"

            def finalQuery = "DROP TABLE IF EXISTS $outputTableName; " +
                    "CREATE INDEX IF NOT EXISTS ids_r ON $buffTable($ID_COLUMN_RSU); " +
                    "CREATE TABLE $outputTableName($ID_COLUMN_RSU INTEGER, water_fraction DOUBLE DEFAULT 0) AS " +
                    "(SELECT a.$ID_COLUMN_RSU, COALESCE(b.water_fraction,0) FROM $rsuTable a LEFT JOIN $buffTable b ON " +
                    "a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU)"

            datasource.execute buffQuery + finalQuery
            datasource.execute "DROP TABLE IF EXISTS $buffTable"

            [outputTableName: outputTableName]
        }
    })
}

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
IProcess perviousnessFraction() {
    def final ID_COLUMN_RSU = "id_rsu"
    def final OPS = ["pervious_fraction", "impervious_fraction"]
    def final BASE_NAME = "perviousness_fraction"

    return create({
        title "Perviousness fraction"
        inputs rsuTable: String, operationsAndComposition: ["pervious_fraction"  :
                                                                    ["low_vegetation_fraction", "water_fraction"],
                                                            "impervious_fraction": ["road_fraction"]],
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, operationsAndComposition, prefixName, datasource ->

            info "Executing Perviousness fraction"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // The pervious or impervious fractions are calculated
            def query = "DROP TABLE IF EXISTS $outputTableName; " +
                    "CREATE TABLE $outputTableName AS SELECT $ID_COLUMN_RSU, "

            operationsAndComposition.each { indic, land_fractions ->
                if (OPS.contains(indic.toLowerCase())) {
                    land_fractions.each { lf ->
                        query += "$lf +"
                    }
                    query = query[0..-2] + "AS $indic,"
                } else {
                    error "$indic is not a valid name (valid names are in $OPS)."
                }
            }
            query = query[0..-2] + " FROM $rsuTable;"

            datasource.execute query

            [outputTableName: outputTableName]
        }
    })
}


/**
* Process used to compute the free facade fraction within an extended RSU. The free facade fraction is defined as
* the ratio between the free facade area within an extended RSU and the free facade area within an extended RSU +
* the area of the extended RSU. This indicator may be useful to investigate a simple an fast approach to
* calculate the ground sky view factor such as defined by Stewart et Oke (2012): ratio of the
* amount of sky hemisphere visible from ground level to that of an unobstructed hemisphere. Preliminary studies
* have been performed by Bernabé et al. (2015) and Bernard et al. (2018). The calculation needs
* the "building_contiguity", the building wall height, the "building_total_facade_length" values as well as
* a correlation Table between buildings and blocks.
*
*
* @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
* the resulting database will be stored
* @param buildingTable The name of the input ITable where are stored the buildings, the building contiguity values,
* the building total facade length values and the building and rsu id
* @param rsuTable The name of the input ITable where are stored the rsu geometries and the id_rsu
* @param buContiguityColumn The name of the column where are stored the building contiguity values (within the
    * building Table)
* @param buTotalFacadeLengthColumn The name of the column where are stored the building total facade length values
* (within the building Table)
* @param buffDist The size used for RSU extension (buffer size in meters)
* @param prefixName String use as prefix to name the output table
*
* References:
* --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of
* the American Meteorological Society 93, no. 12 (2012): 1879-1900.
* --> Bernabé, A., Musy, M., Andrieu, H., & Calmet, I. (2015). Radiative properties of the urban fabric derived
* from surface form analysis: A simplified solar balance model. Solar Energy, 122, 156-168.
* --> Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. Sky View Factor Calculation in
* Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools. Climate ,
* MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.
*
*
* @return A database table name.
* @author Jérémy Bernard
*/
IProcess extendedFreeFacadeFraction() {
    def final GEOMETRIC_FIELD = "the_geom"
    def final ID_FIELD_RSU = "id_rsu"
    def final HEIGHT_WALL = "height_wall"
    def final BASE_NAME = "rsu_extended_free_facade_fraction"

    return create({
        title "Extended RSU free facade fraction (for SVF fast)"
        inputs buildingTable: String, rsuTable: String, buContiguityColumn: String, buTotalFacadeLengthColumn: String,
                prefixName: String, buffDist: 10, datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTable, rsuTable, buContiguityColumn, buTotalFacadeLengthColumn, prefixName, buffDist, datasource ->

            info "Executing RSU free facade fraction (for SVF fast)"

            // The name of the outputTableName is constructed
            def outputTableName = prefixName + "_" + BASE_NAME

            // Temporary tables are created
            def extRsuTable = "extRsu$uuid"
            def inclBu = "inclBu$uuid"
            def fullInclBu = "fullInclBu$uuid"
            def notIncBu = "notIncBu$uuid"
            def allBu = "allBu$uuid"

            // The RSU area is extended according to a buffer
            datasource.execute "DROP TABLE IF EXISTS $extRsuTable; CREATE TABLE $extRsuTable AS SELECT " +
                    "ST_EXPAND($GEOMETRIC_FIELD, $buffDist) AS $GEOMETRIC_FIELD," +
                    "$ID_FIELD_RSU FROM $rsuTable;"

            // The facade area of buildings being entirely included in the RSU buffer is calculated
            datasource.getSpatialTable(extRsuTable)[GEOMETRIC_FIELD].createSpatialIndex()
            datasource.getSpatialTable(buildingTable)[GEOMETRIC_FIELD].createSpatialIndex()

            datasource.execute "DROP TABLE IF EXISTS $inclBu; CREATE TABLE $inclBu AS SELECT " +
                    "COALESCE(SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$HEIGHT_WALL), 0) AS FAC_AREA," +
                    "b.$ID_FIELD_RSU FROM $buildingTable a, $extRsuTable b WHERE ST_COVERS(b.$GEOMETRIC_FIELD," +
                    "a.$GEOMETRIC_FIELD) GROUP BY b.$ID_FIELD_RSU;"

            // All RSU are feeded with default value
            datasource.getTable(inclBu)[ID_FIELD_RSU].createIndex()
            datasource.getTable(rsuTable)[ID_FIELD_RSU].createIndex()

            datasource.execute "DROP TABLE IF EXISTS $fullInclBu; CREATE TABLE $fullInclBu AS SELECT " +
                    "COALESCE(a.FAC_AREA, 0) AS FAC_AREA, b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD " +
                    "FROM $inclBu a RIGHT JOIN $rsuTable b ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;"

            // The facade area of buildings being partially included in the RSU buffer is calculated
            datasource.execute "DROP TABLE IF EXISTS $notIncBu; CREATE TABLE $notIncBu AS SELECT " +
                    "COALESCE(SUM(ST_LENGTH(ST_INTERSECTION(ST_TOMULTILINE(a.$GEOMETRIC_FIELD)," +
                    " b.$GEOMETRIC_FIELD))*a.$HEIGHT_WALL), 0) " +
                    "AS FAC_AREA, b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD FROM $buildingTable a, $extRsuTable b " +
                    "WHERE ST_OVERLAPS(b.$GEOMETRIC_FIELD, a.$GEOMETRIC_FIELD) GROUP BY b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD;"

            // The facade fraction is calculated
            datasource.getTable(notIncBu)[ID_FIELD_RSU].createIndex()
            datasource.getSpatialTable(fullInclBu)[ID_FIELD_RSU].createIndex()

            datasource.execute "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS " +
                    "SELECT COALESCE((a.FAC_AREA + b.FAC_AREA) /" +
                    "(a.FAC_AREA + b.FAC_AREA + ST_AREA(ST_EXPAND(a.$GEOMETRIC_FIELD, $buffDist)))," +
                    " a.FAC_AREA / (a.FAC_AREA  + ST_AREA(ST_EXPAND(a.$GEOMETRIC_FIELD, $buffDist))))" +
                    "AS rsu_extended_free_facade_fraction, " +
                    "a.$ID_FIELD_RSU, a.$GEOMETRIC_FIELD FROM $fullInclBu a LEFT JOIN $notIncBu b " +
                    "ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;"

            [outputTableName: outputTableName]
        }
    })
}