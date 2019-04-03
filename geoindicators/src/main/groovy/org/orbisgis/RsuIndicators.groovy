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