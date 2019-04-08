package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

/**
 * Process used to compute the sum of all building free facades (roofs are excluded) included in a
 * Reference Spatial Unit (RSU - such as urban blocks) divided by the RSU area. The calculation is performed
 * according to a building Table where are stored the "building_contiguity", the building wall height and
 * the "building_total_facade_length" values as well as a correlation Table between buildings and blocks.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess rsuFreeExternalFacadeDensity() {
return processFactory.create(
        "RSU free external facade density",
        [buildingTable: String,inputColumns:String[],correlationTable: String,
         buContiguityColumn: String, buTotalFacadeLengthColumn: String, outputTableName: String, datasource: JdbcDataSource],
        [outputTableName : String],
        { buildingTable, inputColumns, correlationTable, buContiguityColumn, buTotalFacadeLengthColumn,
          outputTableName, datasource ->
            def geometricFieldRsu = "the_geom"
            def idFieldBu = "id_build"
            def idFieldRsu = "id_rsu"
            def height_wall = "height_wall"

            String query = "CREATE INDEX IF NOT EXISTS id_bua ON $buildingTable($idFieldBu); "+
                            "CREATE INDEX IF NOT EXISTS id_bub ON $correlationTable($idFieldBu); "+
                            "CREATE INDEX IF NOT EXISTS id_blb ON $correlationTable($idFieldRsu); "+
                            "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                            "SELECT SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$height_wall)/"+
                            "st_area(b.$geometricFieldRsu) AS rsu_free_external_facade_density, b.$idFieldRsu "

            if(!inputColumns.isEmpty()){
                query += ", a.${inputColumns.join(",a.")} "
            }

            query += "FROM $buildingTable a, $correlationTable b "+
                        "WHERE a.$idFieldBu = b.$idFieldBu GROUP BY b.$idFieldRsu, b.$geometricFieldRsu;"

            logger.info("Executing $query")
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
static IProcess rsuGroundSkyViewFactor() {
    return processFactory.create(
            "RSU ground sky view factor",
            [rsuTable: String,inputColumns:String[],correlationBuildingTable: String,
             rsuAreaColumn: String, rsuBuildingDensityColumn: String, pointDensity: Double, rayLength: Double,
             numberOfDirection: Double, outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, inputColumns, correlationBuildingTable, rsuAreaColumn, rsuBuildingDensityColumn,
              pointDensity = 0.008, rayLength = 100, numberOfDirection = 60, outputTableName, datasource ->
                def geometricColumnRsu = "the_geom"
                def geometricColumnBu = "the_geom"
                def idColumnRsu = "id_rsu"
                def height_wall = "height_wall"

                // To avoid overwriting the output files of this step, a unique identifier is created
                def uid_out = System.currentTimeMillis()

                // Temporary table names
                def ptsRSUtot = "ptsRSUtot"+uid_out
                def ptsRSUGrid = "ptsRSUGrid"+uid_out
                def ptsRSUtempo = "ptsRSUtempo"+uid_out
                def ptsRSUbu = "ptsRSUbu"+uid_out
                def ptsRSUfreeall = "ptsRSUfreeall"+uid_out
                def randomSample = "randomSample"+uid_out
                def svfPts = "svfPts"+uid_out

                // Other local variables
                // Size of the grid mesh used to sample each RSU (according to the point density, we take a factor 10
                // in order to consider that for some LCZ(7), 90% of the surfaces may be covered by buildings and
                // then will not be used for the SVF calculation)
                def gms = Math.pow((1.0/pointDensity)/10,0.5)

                // Create the needed index on input tables and the table that will contain the SVF calculation points
                datasource.execute(("CREATE INDEX IF NOT EXISTS ids_inI ON $rsuTable($geometricColumnRsu) USING RTREE; "+
                        "CREATE INDEX IF NOT EXISTS ids_inA ON $correlationBuildingTable($geometricColumnBu) USING RTREE; "+
                        "CREATE INDEX IF NOT EXISTS id_inA ON $correlationBuildingTable($idColumnRsu); "+
                        "DROP TABLE IF EXISTS $ptsRSUtot; CREATE TABLE $ptsRSUtot (pk serial, "+
                        "the_geom geometry, id_rsu int)").toString())

                // The points used for the SVF calculation should be selected within each RSU
                datasource.eachRow("SELECT * FROM $rsuTable".toString()) { row ->
                    // A grid of point is created for each RSU
                    datasource.execute(("DROP TABLE IF EXISTS $ptsRSUGrid; CREATE TABLE $ptsRSUGrid(pk SERIAL, the_geom GEOMETRY) AS (SELECT null, "+
                            "the_geom FROM ST_MAKEGRIDPOINTS('${row[geometricColumnRsu]}'::GEOMETRY, $gms, $gms))").toString())
                    // Grid points included inside the RSU are conserved
                    datasource.execute(("CREATE INDEX IF NOT EXISTS ids_temp ON $ptsRSUGrid(the_geom) USING RTREE; "+
                            "DROP TABLE IF EXISTS $ptsRSUtempo; CREATE TABLE $ptsRSUtempo AS SELECT a.pk, a.the_geom, "+
                            "${row[idColumnRsu]} AS id_rsu FROM $ptsRSUGrid a WHERE a.the_geom && '${row[geometricColumnRsu]}' AND "+
                            "ST_INTERSECTS(a.the_geom, '${row[geometricColumnRsu]}')").toString())
                    // The grid points intersecting buildings are identified
                    datasource.execute(("CREATE INDEX IF NOT EXISTS ids_temp ON $ptsRSUtempo(the_geom) USING RTREE; "+
                            "DROP TABLE IF EXISTS $ptsRSUbu; CREATE TABLE $ptsRSUbu AS SELECT a.pk FROM $ptsRSUtempo a, "+
                            "$correlationBuildingTable b WHERE ${row[idColumnRsu]} = b.$idColumnRsu AND "+
                            "a.the_geom && b.$geometricColumnBu AND ST_INTERSECTS(a.the_geom, b.$geometricColumnBu)").toString())
                    // The grid points intersecting buildings are then deleted
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUtempo(pk); "+
                            "CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUbu(pk); DROP TABLE IF EXISTS $ptsRSUfreeall; "+
                            "CREATE TABLE $ptsRSUfreeall(pk SERIAL, the_geom GEOMETRY, id_rsu INT) AS (SELECT null, a.the_geom, a.id_rsu FROM "+
                            "$ptsRSUtempo a LEFT JOIN $ptsRSUbu b ON a.pk=b.pk WHERE b.pk IS NULL)").toString())
                    // A random sample of points (of size corresponding to the point density defined by $pointDensity)
                    // is drawn in order to have the same density of point in each RSU
                    datasource.execute(("DROP TABLE IF EXISTS $randomSample; CREATE TABLE $randomSample AS "+
                            "SELECT pk FROM $ptsRSUfreeall ORDER BY RANDOM() LIMIT "+
                            "(TRUNC(${pointDensity*row[rsuAreaColumn]*(1.0-row[rsuBuildingDensityColumn])})+1);").toString())
                    // The sample of point is inserted into the Table gathering the SVF points used for all RSU
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_temp ON $ptsRSUfreeall(pk); "+
                            "CREATE INDEX IF NOT EXISTS id_temp ON $randomSample(pk); "+
                            "INSERT INTO $ptsRSUtot(pk, the_geom, id_rsu) SELECT NULL, a.the_geom, a.id_rsu "+
                            "FROM $ptsRSUfreeall a, $randomSample b WHERE a.pk = b.pk").toString())
                }

                // The SVF calculation is performed at point scale
                datasource.execute(("CREATE INDEX IF NOT EXISTS ids_pts ON $ptsRSUtot(the_geom) USING RTREE; "+
                        "DROP TABLE IF EXISTS $svfPts; CREATE TABLE $svfPts AS SELECT a.pk, a.id_rsu, "+
                        "ST_SVF(ST_GEOMETRYN(a.the_geom,1), ST_ACCUM(ST_UPDATEZ(b.$geometricColumnBu, b.$height_wall)), "+
                        "$rayLength, $numberOfDirection, 5) AS SVF FROM $ptsRSUtot AS a, $correlationBuildingTable "+
                        "AS b WHERE ST_EXPAND(a.the_geom, $rayLength) && b.$geometricColumnBu AND "+
                        "ST_DWITHIN(b.$geometricColumnBu, a.the_geom, $rayLength) GROUP BY a.the_geom").toString())

                // The result of the SVF calculation is averaged at RSU scale
                datasource.execute(("CREATE INDEX IF NOT EXISTS id_svf ON $svfPts(id_rsu); "+
                        "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                        "SELECT AVG(SVF) AS rsu_ground_sky_view_factor, id_rsu AS $idColumnRsu FROM $svfPts "+
                        "GROUP BY id_rsu").toString())

                // The temporary tables are deleted
                datasource.execute(("DROP TABLE IF EXISTS $ptsRSUtot, $ptsRSUGrid, $ptsRSUtempo, $ptsRSUbu, "+
                        "$ptsRSUfreeall, $randomSample, $svfPts").toString())
            }
    )}

/**
 * Process used to compute the aspect ratio such as defined by Stewart et Oke (2012): mean height-to-width ratio
 * of street canyons (LCZs 1-7), building spacing (LCZs 8-10), and tree spacing (LCZs A - G). A simple approach based
 * on the street canyons assumption is used for the calculation. The sum of facade area within a given RSU area
 * is divided by the area of free surfaces of the given RSU (not covered by buildings). The
 * "rsu_free_external_facade_density" and "rsu_building_density" are used for the calculation.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess rsuAspectRatio() {
    return processFactory.create(
            "RSU aspect ratio",
            [rsuTable: String, inputColumns:String[], rsuFreeExternalFacadeDensityColumn: String,
             rsuBuildingDensityColumn: String, outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, inputColumns, rsuFreeExternalFacadeDensityColumn, rsuBuildingDensityColumn,
              outputTableName, datasource ->

                String query = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS "+
                        "SELECT $rsuFreeExternalFacadeDensityColumn/(1-$rsuBuildingDensityColumn) AS "+
                        "rsu_aspect_ratio "

                if(!inputColumns.isEmpty()){
                    query += ", ${inputColumns.join(",")} "
                }

                query += " FROM $rsuTable"

                logger.info("Executing $query")
                datasource.execute query
                [outputTableName: outputTableName]
            }
    )}

/**
 * Script to compute the distribution of projected facade area within a RSU per vertical layer and direction
 * of analysis (ie. wind or sun direction). Note that the method used is an approximation if the RSU split
 * a building into two parts (the facade included within the RSU is counted half).
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
static IProcess rsuProjectedFacadeAreaDistribution() {
    return processFactory.create(
            "RSU projected facade area distribution",
            [buildingTable: String, rsuTable: String, inputColumns: String[], listLayersBottom: double[], numberOfDirection: Integer,
             outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { buildingTable, rsuTable, inputColumns, listLayersBottom = [0, 10, 20, 30, 40, 50], numberOfDirection = 12,
              outputTableName, datasource ->

                if(180%numberOfDirection==0 & numberOfDirection%2==0){
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
                    def dirMedDeg = 180/numberOfDirection
                    def dirMedRad = Math.toRadians(dirMedDeg)

                    // The list that will store the fields name is initialized
                    def names = []

                    // Indexes and spatial indexes are created on input tables
                    datasource.execute(("CREATE SPATIAL INDEX IF NOT EXISTS ids_ina ON $buildingTable($geometricColumnBu); "+
                            "CREATE SPATIAL INDEX IF NOT EXISTS ids_inb ON $rsuTable($geometricColumnRsu); "+
                            "CREATE INDEX IF NOT EXISTS id_ina ON $buildingTable($idColumnBu); "+
                            "CREATE INDEX IF NOT EXISTS id_inb ON $rsuTable($idColumnRsu);").toString())

                    // Common party walls between buildings are calculated
                    datasource.execute(("CREATE TABLE $buildingIntersection(pk SERIAL, the_geom GEOMETRY, "+
                            "ID_build_a INTEGER, ID_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS "+
                            "(SELECT NULL, ST_INTERSECTION(a.$geometricColumnBu, b.$geometricColumnBu), "+
                            "a.$idColumnBu, b.$idColumnBu, GREATEST(a.$height_wall,b.$height_wall), "+
                            "LEAST(a.$height_wall,b.$height_wall) FROM $buildingTable AS a, $buildingTable AS b "+
                            "WHERE a.$geometricColumnBu && b.$geometricColumnBu AND "+
                            "ST_INTERSECTS(a.$geometricColumnBu, b.$geometricColumnBu) AND a.$idColumnBu <> b.$idColumnBu)").toString())

                    // Common party walls are converted to multilines and then exploded
                    datasource.execute(("CREATE TABLE $buildingIntersectionExpl(pk SERIAL, the_geom GEOMETRY, "+
                            "ID_build_a INTEGER, ID_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS "+
                            "(SELECT NULL, ST_TOMULTILINE(the_geom), ID_build_a, ID_build_b, z_max, z_min "+
                            "FROM ST_EXPLODE('(SELECT the_geom AS the_geom, ID_build_a, ID_build_b, z_max, z_min "+
                            "FROM $buildingIntersection)'))").toString())

                    // Each free facade is stored TWICE (an intersection could be seen from the point of view of two buildings).
                    // Facades of isolated buildings are unioned to free facades of non-isolated buildings which are
                    // unioned to free intersection facades. To each facade is affected its corresponding free height
                    datasource.execute(("CREATE INDEX IF NOT EXISTS id_buint ON ${buildingIntersectionExpl}(ID_build_a); "+
                            "CREATE TABLE $buildingFree(pk SERIAL, the_geom GEOMETRY, z_max DOUBLE, z_min DOUBLE) "+
                            "AS (SELECT NULL, ST_TOMULTISEGMENTS(a.the_geom), a.$height_wall, 0 "+
                            "FROM $buildingTable a WHERE a.$idColumnBu NOT IN (SELECT ID_build_a "+
                            "FROM $buildingIntersectionExpl)) UNION ALL (SELECT NULL, "+
                            "ST_TOMULTISEGMENTS(ST_DIFFERENCE(ST_TOMULTILINE(a.$geometricColumnBu), "+
                            "ST_UNION(ST_ACCUM(b.the_geom)))), a.$height_wall, 0 FROM $buildingTable a, "+
                            "$buildingIntersectionExpl b WHERE a.$idColumnBu=b.ID_build_a "+
                            "GROUP BY b.ID_build_a) UNION ALL (SELECT NULL, ST_TOMULTISEGMENTS(the_geom) "+
                            "AS the_geom, z_max, z_min FROM $buildingIntersectionExpl WHERE ID_build_a<ID_build_b)").toString())

                    // The height of wall is calculated for each level
                    String layerQuery = "CREATE TABLE $buildingLayer AS SELECT pk, the_geom, "
                    for (i in 1..(listLayersBottom.size()-1)){
                        names[i-1]="rsu_projected_facade_area_distribution${listLayersBottom[i-1]}"+
                                "_${listLayersBottom[i]}"
                        layerQuery += "CASEWHEN(z_max <= ${listLayersBottom[i-1]}, 0, CASEWHEN(z_min >= ${listLayersBottom[i]}, "+
                                "0, ${listLayersBottom[i]-listLayersBottom[i-1]}-GREATEST(${listLayersBottom[i]}-z_max,0)"+
                                "-GREATEST(z_min-${listLayersBottom[i-1]},0))) AS ${names[i-1]} ,"
                        }


                    names[listLayersBottom.size()-1]="rsu_projected_facade_area_distribution${listLayersBottom[listLayersBottom.size()-1]}_"
                    layerQuery += "CASEWHEN(z_max >= ${listLayersBottom[listLayersBottom.size()-1]}, "+
                            "z_max-GREATEST(z_min,${listLayersBottom[listLayersBottom.size()-1]}), 0) "+
                            "AS ${names[listLayersBottom.size()-1]} FROM $buildingFree"
                    datasource.execute(layerQuery.toString())

                    // Names and types of all columns are then useful when calling sql queries
                    String namesAndType = ""
                    String onlyNames = ""
                    String onlyNamesB = ""
                    for (n in names){
                        namesAndType += " " + n + " double,"
                        onlyNames += " " + n + ","
                        onlyNamesB += " b." + n + ","
                    }

                    // Free facades are exploded to multisegments
                    datasource.execute(("CREATE TABLE $buildingFreeExpl(pk SERIAL, the_geom GEOMETRY, $namesAndType) AS "+
                            "(SELECT NULL, the_geom, ${onlyNames[0..-2]} FROM ST_EXPLODE('$buildingLayer'))").toString())

                    // Intersections between free facades and rsu geometries are calculated
                    datasource.execute(("CREATE SPATIAL INDEX IF NOT EXISTS ids_bufre ON $buildingFreeExpl(the_geom); "+
                            "CREATE TABLE $rsuInter(id_rsu INTEGER, the_geom GEOMETRY, $namesAndType) AS "+
                            "(SELECT a.$idColumnRsu, ST_INTERSECTION(a.$geometricColumnRsu, b.the_geom), "+
                            "${onlyNamesB[0..-2]} FROM $rsuTable a, $buildingFreeExpl b WHERE a.$geometricColumnRsu && b.the_geom "+
                            "AND ST_INTERSECTS(a.$geometricColumnRsu, b.the_geom))").toString())

                    // Basic informations are stored in the result Table where will be added all fields
                    // corresponding to the distribution
                    datasource.execute(("CREATE TABLE ${finalIndicator}_0 AS SELECT ${inputColumns.join(",")} "+
                            "FROM $rsuTable").toString())


                    // The analysis is then performed for each direction ('numberOfDirection' / 2 because calculation
                    // is performed for a direction independently of the "toward")
                    for (int d=0; d<numberOfDirection/2; d++){
                        def dirDeg = d*360/numberOfDirection
                        def dirRad = Math.toRadians(dirDeg)

                        // Define the field name for each of the directions and vertical layers
                        String namesAndTypeDir = ""
                        String onlyNamesDir = ""
                        String onlyNamesDirB = ""
                        for (n in names){
                            namesAndTypeDir += " " + n + "D" + (dirDeg+dirMedDeg).toString() + " double,"
                            onlyNamesDir += " " + n + "D" + (dirDeg+dirMedDeg).toString() + ","
                            onlyNamesDirB += " b." + n + "D" + (dirDeg+dirMedDeg).toString() + ","
                        }

                        // To calculate the indicator for a new wind direction, the free facades are rotated
                        datasource.execute(("DROP TABLE IF EXISTS $rsuInterRot; CREATE TABLE $rsuInterRot(id_rsu INTEGER, the_geom geometry, "+
                                "${namesAndType[0..-2]}) AS (SELECT id_rsu, ST_ROTATE(the_geom,${dirRad+dirMedRad}), "+
                                "${onlyNames[0..-2]} FROM $rsuInter)").toString())

                        // The projected facade area indicator is calculated according to the free facades table
                        // for each vertical layer for this specific direction "d"
                        String calcQuery = ""
                        for (n in names){
                            calcQuery += "sum((st_xmax(b.the_geom) - st_xmin(b.the_geom))*b.$n)/2 AS "+
                                    "${n}D${dirDeg+dirMedDeg}, "
                        }
                        datasource.execute(("CREATE INDEX IF NOT EXISTS id_rint ON $rsuInterRot(id_rsu); "+
                                "CREATE INDEX IF NOT EXISTS id_fin ON ${finalIndicator}_$d(id_rsu); "+
                                "CREATE TABLE ${finalIndicator}_${d+1} AS SELECT a.*, ${calcQuery[0..-3]} "+
                                "FROM ${finalIndicator}_$d a LEFT JOIN $rsuInterRot b "+
                                "ON a.id_rsu = b.ID_RSU GROUP BY a.id_rsu, a.the_geom").toString())
                    }

                    datasource.execute(("DROP TABLE IF EXISTS $outputTableName; ALTER TABLE "+
                            "${finalIndicator}_${numberOfDirection/2} RENAME TO $outputTableName").toString())

                    // Remove all temporary tables created
                    String removeQuery = " ${finalIndicator}_0"
                    for (int d=0; d<numberOfDirection/2; d++){
                        removeQuery += ", ${finalIndicator}_$d"
                    }
                    datasource.execute(("DROP TABLE IF EXISTS ${buildingIntersection}, ${buildingIntersectionExpl}, "+
                            "${buildingFree}, ${buildingFreeExpl}, ${rsuInter}, $removeQuery;").toString())

                    [outputTableName: outputTableName]
                }
            }
    )}

/**
 * Script to compute the distribution of roof (vertical, horizontal and tilted) area within a RSU per vertical layer.
 * Note that the method used is based on the assumption that all buildings have gable roofs. Since we do not know
 * what is the direction of the gable, we also consider that buildings are square in order to limit the potential
 * calculation error (for example considering that the gable is always oriented parallel to the larger side of the
 * building.
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
static IProcess rsuRoofAreaDistribution() {
    return processFactory.create(
            "RSU roof area distribution",
            [rsuTable: String, correlationBuildingTable: String, inputColumns: String[], listLayersBottom: Double[],
             outputTableName: String, datasource: JdbcDataSource],
            [outputTableName : String],
            { rsuTable, correlationBuildingTable, inputColumns, listLayersBottom = [0, 10, 20, 30, 40, 50],
              outputTableName, datasource ->

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

                // Vertical and non-vertical (tilted and horizontal) roof areas are calculated
                datasource.execute(("CREATE TABLE $buildRoofSurfIni AS SELECT $geometricColumnBu, $idColumnRsu," +
                        "$idColumnBu, $height_roof AS z_max, $height_wall AS z_min, ST_AREA($geometricColumnBu) AS"+
                        " building_area, ST_PERIMETER($geometricColumnBu)+ST_PERIMETER(ST_HOLES($geometricColumnBu))"+
                        " AS building_total_facade_length, $height_roof-$height_wall AS delta_h, POWER(POWER(ST_AREA($geometricColumnBu),2)+4*"+
                        "ST_AREA($geometricColumnBu)*POWER($height_roof-$height_wall,2),0.5) AS non_vertical_roof_area,"+
                        "POWER(ST_AREA($geometricColumnBu), 0.5)*($height_roof-$height_wall) AS vertical_roof_area"+
                        " FROM $correlationBuildingTable;").toString())


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
                datasource.eachRow("SELECT * FROM $buildVertRoofAll".toString()){
                    row ->
                        println(row)
                }
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
                String finalQuery = "CREATE TABLE $outputTableName AS SELECT id_rsu, "
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
                            "CASEWHEN(z_min>${listLayersBottom[i - 1]}, vertical_roof_area*(1-" +
                            "POWER((z_max-${listLayersBottom[i]})/delta_h,2)),vertical_roof_area*(1-" +
                            "POWER((${listLayersBottom[i]}-z_min)/delta_h,2)-POWER((z_max-${listLayersBottom[i]})/" +
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
