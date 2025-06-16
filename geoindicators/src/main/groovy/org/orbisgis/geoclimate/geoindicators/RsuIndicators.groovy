/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.SQLException

import static java.lang.Math.round
import static java.lang.Math.toRadians

@BaseScript Geoindicators geoindicators

/**
 * Process used to compute the sum of all building free facades (roofs are excluded) included in a
 * Reference Spatial Unit (RSU - such as urban blocks) divided by the RSU area. The calculation is performed
 * according to a building Table where are stored the "building_contiguity", the building wall height and
 * the "building_total_facade_length" values as well as a correlation Table between buildings and blocks.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param building The name of the input ITable where are stored the buildings, the building contiguity values,
 * the building total facade length values and the building and rsu id
 * @param rsu The name of the input ITable where are stored the rsu geometries and the id_rsu
 * @param buContiguityColumn The name of the column where are stored the building contiguity values (within the
 * building Table)
 * @param buTotalFacadeLengthColumn The name of the column where are stored the building total facade length values
 * (within the building Table)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String freeExternalFacadeDensity(JdbcDataSource datasource, String building, String rsu, String buContiguityColumn,
                                 String buTotalFacadeLengthColumn, String prefixName) throws Exception {
    try {
        def GEOMETRIC_FIELD_RSU = "the_geom"
        def ID_FIELD_RSU = "id_rsu"
        def HEIGHT_WALL = "height_wall"
        def BASE_NAME = "free_external_facade_density"

        debug "Executing RSU free external facade density"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        datasource.createIndex(building, "id_rsu")
        datasource.createIndex(rsu, "id_rsu")

        def query = """
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName AS 
                    SELECT COALESCE(
                        SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$HEIGHT_WALL)/
                                st_area(b.$GEOMETRIC_FIELD_RSU),0) 
                            AS $BASE_NAME, 
                        b.$ID_FIELD_RSU """

        query += " FROM $building a RIGHT JOIN $rsu b " +
                "ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU GROUP BY b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD_RSU;"

        datasource.execute(query)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the free external facade density at RSU scale", e)
    }
}

/**
 * Process used to compute the sum of all building free facades (roofs are excluded) included in a
 * Reference Spatial Unit (RSU) divided by the RSU area. The calculation is performed more accurately than
 * in the freeExternalFacadeDensity method since in this process only the part of building being in the RSU
 * is considered in the calculation of the indicator.
 *
 * WARNING: WITH THE CURRENT METHOD, IF A BUILDING FACADE FOLLOW THE LINE SEPARATING TWO RSU, IT WILL BE COUNTED FOR BOTH RSU
 * (EVEN THOUGH THIS SITUATION IS PROBABLY ALMOST IMPOSSIBLE WITH REGULAR SQUARE GRID)...
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param building The name of the input ITable where are stored the buildings geometry, the building height,
 * the building and the rsu id
 * @param rsu The name of the input ITable where are stored the rsu geometries and the 'idRsu'
 * @param idRsu the name of the id of the RSU table
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String freeExternalFacadeDensityExact(JdbcDataSource datasource, String building,
                                      String rsu, String idRsu, String prefixName) throws Exception {

    // Temporary table names
    def buildLine = postfix "buildLine"
    def buildLineRsu = postfix "buildLineRsu"
    def sharedLineRsu = postfix "shareLineRsu"
    def onlyBuildRsu = postfix "onlyBuildRsu"
    try {
        def GEOMETRIC_FIELD_RSU = "the_geom"
        def GEOMETRIC_FIELD_BU = "the_geom"
        def ID_FIELD_BU = "id_build"
        def HEIGHT_WALL = "height_wall"
        def FACADE_AREA = "facade_area"
        def RSU_AREA = "rsu_area"
        def BASE_NAME = "free_external_facade_density"

        debug "Executing RSU free external facade density (exact version)"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME


        // Consider facades as touching each other within a snap tolerance
        def snap_tolerance = 0.01

        // 1. Convert the building polygons into lines and create the intersection with RSU polygons
        datasource.createIndex(building, idRsu)
        datasource.createIndex(rsu, idRsu)
        datasource.execute("""
                DROP TABLE IF EXISTS $buildLine;
                CREATE TABLE $buildLine
                    AS SELECT   a.$ID_FIELD_BU, a.$idRsu, ST_AREA(b.$GEOMETRIC_FIELD_RSU) AS $RSU_AREA,
                                ST_CollectionExtract(ST_INTERSECTION(ST_TOMULTILINE(a.$GEOMETRIC_FIELD_BU), b.$GEOMETRIC_FIELD_RSU), 2) AS $GEOMETRIC_FIELD_BU,
                                a.$HEIGHT_WALL
                    FROM $building AS a LEFT JOIN $rsu AS b
                    ON a.$idRsu = b.$idRsu""")

        // 2. Keep only intersected facades within a given distance and calculate their area per RSU
        datasource.createSpatialIndex(buildLine, GEOMETRIC_FIELD_BU)
        datasource.createIndex(buildLine, idRsu)
        datasource.createIndex(buildLine, ID_FIELD_BU)
        datasource.execute("""
                DROP TABLE IF EXISTS $sharedLineRsu;
                CREATE TABLE $sharedLineRsu 
                    AS SELECT   SUM(ST_LENGTH(  ST_INTERSECTION(a.$GEOMETRIC_FIELD_BU, 
                                                                ST_SNAP(b.$GEOMETRIC_FIELD_BU, a.$GEOMETRIC_FIELD_BU, $snap_tolerance)
                                                                )
                                                )
                                    *LEAST(a.$HEIGHT_WALL, b.$HEIGHT_WALL)) AS $FACADE_AREA,
                                a.$idRsu
                    FROM    $buildLine AS a LEFT JOIN $buildLine AS b
                            ON a.$idRsu = b.$idRsu
                    WHERE       a.$GEOMETRIC_FIELD_BU && b.$GEOMETRIC_FIELD_BU AND ST_INTERSECTS(a.$GEOMETRIC_FIELD_BU, 
                                ST_SNAP(b.$GEOMETRIC_FIELD_BU, a.$GEOMETRIC_FIELD_BU, $snap_tolerance)) AND
                                a.$ID_FIELD_BU <> b.$ID_FIELD_BU
                    GROUP BY a.$idRsu;""")

        // 3. Calculates the building facade area within each RSU
        datasource.createIndex(buildLine, idRsu)
        datasource.execute("""
                DROP TABLE IF EXISTS $buildLineRsu;
                CREATE TABLE $buildLineRsu
                    AS SELECT   $idRsu, MIN($RSU_AREA) AS $RSU_AREA,
                                SUM(ST_LENGTH($GEOMETRIC_FIELD_BU) * $HEIGHT_WALL) AS $FACADE_AREA
                    FROM $buildLine
                    GROUP BY $idRsu;""")

        // 4. Calculates the free facade density by RSU (subtract 3 and 2 and divide by RSU area)
        datasource.createIndex(buildLineRsu, idRsu)
        datasource.createIndex(sharedLineRsu, idRsu)
        datasource.execute("""
                DROP TABLE IF EXISTS $onlyBuildRsu;
                CREATE TABLE $onlyBuildRsu
                    AS SELECT   a.$idRsu,
                                a.$FACADE_AREA/a.$RSU_AREA AS $BASE_NAME
                    FROM $buildLineRsu AS a LEFT JOIN $sharedLineRsu AS b
                    ON a.$idRsu = b.$idRsu  WHERE b.$idRsu IS NULL
                    union all
                    SELECT   a.$idRsu,
                                (a.$FACADE_AREA-b.$FACADE_AREA)/a.$RSU_AREA AS $BASE_NAME
                    FROM $buildLineRsu AS a right JOIN $sharedLineRsu AS b
                    ON a.$idRsu = b.$idRsu
            """)


        // 5. Join RSU having no buildings and set their value to 0
        datasource.createIndex(onlyBuildRsu, idRsu)
        datasource.execute("""
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName
                    AS SELECT   a.$idRsu,
                                COALESCE(b.$BASE_NAME, 0) AS $BASE_NAME
                    FROM $rsu AS a LEFT JOIN $onlyBuildRsu AS b
                    ON a.$idRsu = b.$idRsu""")

        // The temporary tables are deleted
        datasource.dropTable(buildLine, buildLineRsu, sharedLineRsu, onlyBuildRsu)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the exact free external facade density at RSU scale", e)
    } finally {
        datasource.dropTable(buildLine, buildLineRsu, sharedLineRsu, onlyBuildRsu)
    }
}

/**
 * Process used to compute the RSU ground sky view factor such as defined by Stewart et Oke (2012): ratio of the
 * amount of sky hemisphere visible from ground level to that of an unobstructed hemisphere. The calculation is
 * based on the ST_SVF function of H2GIS using the following parameters: ray length = 100, number of directions = 60
 * and for a grid resolution of 10 m (standard deviation of the estimate calculated to 0.03 according to
 * Bernard et al. (2018)). The density of points used for the calculation actually depends on building
 * density (higher the building density, lower the density of points). To avoid this phenomenon, we set
 * a constant density of point "point_density" for a given amount of free surfaces (default 0.008,
 * based on the median of Bernard et al. (2018) dataset).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsu The name of the input ITable where are stored the RSU
 * @param id_rsu Unique identifier column name
 * @param correlationBuildingTable The name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
 * @param pointDensity The density of points (nb / free m²) used to calculate the spatial average SVF. Use 0.008f
 * @param rayLength The maximum distance to consider an obstacle as potential sky cover. Use 100D
 * @param numberOfDirection the number of directions considered to calculate the SVF. Use 60
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
 * @author Erwan Bocher
 */
String groundSkyViewFactor(JdbcDataSource datasource, String rsu, String id_rsu, String correlationBuildingTable, float pointDensity,
                           float rayLength, int numberOfDirection, String prefixName) throws Exception {
    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def rsuDiff = postfix "rsuDiff"
    def rsuDiffTot = postfix "rsuDiffTot"
    def multiptsRSU = postfix "multiptsRSU"
    def multiptsRSUtot = postfix "multiptsRSUtot"
    def ptsRSUtot = postfix "ptsRSUtot"
    def pts_order = postfix "pts_order"
    def svfPts = postfix "svfPts"
    def pts_RANG = postfix "pts_RANG"
    try {
        def GEOMETRIC_COLUMN_RSU = "the_geom"
        def GEOMETRIC_COLUMN_BU = "the_geom"
        def ID_COLUMN_RSU = id_rsu
        def HEIGHT_WALL = "height_wall"
        def BASE_NAME = "ground_sky_view_factor"

        debug "Executing RSU ground sky view factor"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        // Create the needed index on input tables and the table that will contain the SVF calculation points
        datasource.createSpatialIndex(rsu, GEOMETRIC_COLUMN_RSU)
        datasource.createIndex(rsu, ID_COLUMN_RSU)

        datasource.createSpatialIndex(correlationBuildingTable, GEOMETRIC_COLUMN_BU)
        datasource.createIndex(correlationBuildingTable, ID_COLUMN_RSU)

        def to_start = System.currentTimeMillis()

        // Create the geometries of buildings and RSU holes included within each RSU
        datasource.execute("""
                DROP TABLE IF EXISTS $rsuDiff, $multiptsRSU, $multiptsRSUtot, $rsuDiffTot,$pts_RANG,$pts_order,$ptsRSUtot, $svfPts, $outputTableName;
                CREATE TABLE $rsuDiff 
                AS (SELECT  CASE WHEN   ST_ISEMPTY(st_difference(a.$GEOMETRIC_COLUMN_RSU, st_makevalid(ST_ACCUM(b.$GEOMETRIC_COLUMN_BU))))
                            THEN        ST_EXTERIORRING(ST_NORMALIZE(a.$GEOMETRIC_COLUMN_RSU))
                            ELSE        st_difference(a.$GEOMETRIC_COLUMN_RSU, st_makevalid(ST_ACCUM(b.$GEOMETRIC_COLUMN_BU)))
                            END         AS the_geom, a.$ID_COLUMN_RSU
                FROM        $rsu a, $correlationBuildingTable b 
                WHERE       a.$GEOMETRIC_COLUMN_RSU && b.$GEOMETRIC_COLUMN_BU AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_RSU, 
                            b.$GEOMETRIC_COLUMN_BU)
                GROUP BY    a.$ID_COLUMN_RSU);
            """)
        datasource.execute("""
                CREATE INDEX ON $rsuDiff ($ID_COLUMN_RSU);
                CREATE TABLE $rsuDiffTot AS 
                SELECT b.$ID_COLUMN_RSU, case when a.$ID_COLUMN_RSU is null then b.the_geom else a.the_geom end as the_geom 
                FROM $rsu as b left join $rsuDiff as a on a.$ID_COLUMN_RSU=b.$ID_COLUMN_RSU;
            """)

        // The points used for the SVF calculation are regularly selected within each RSU. The points are
        // located outside buildings (and RSU holes) and the size of the grid mesh used to sample each RSU
        // (based on the building density + 10%) - if the building density exceeds 90%,
        // the LCZ 7 building density is then set to 90%)
        datasource.execute("""CREATE TABLE $multiptsRSU AS SELECT $ID_COLUMN_RSU, THE_GEOM 
                    FROM  
                    ST_EXPLODE('(SELECT $ID_COLUMN_RSU,
                                case when LEAST(TRUNC($pointDensity*c.rsu_area_free),100)=0 
                                then st_pointonsurface(c.the_geom)
                                else ST_GENERATEPOINTS(c.the_geom, LEAST(TRUNC($pointDensity*c.rsu_area_free),100)) end
                    AS the_geom
                    FROM  (SELECT the_geom, st_area($GEOMETRIC_COLUMN_RSU) 
                                            AS rsu_area_free, $ID_COLUMN_RSU
                                FROM        st_explode(''(select * from $rsuDiffTot)'')  where st_area(the_geom)>0) as c)');""")

        // Need to identify specific points for buildings being RSU (slightly away from the wall on each facade)
        datasource.execute("""  CREATE TABLE $multiptsRSUtot
                                AS SELECT $ID_COLUMN_RSU, THE_GEOM
                                FROM    ST_EXPLODE('(SELECT $ID_COLUMN_RSU, ST_LocateAlong(THE_GEOM, 0.5, 0.01) AS THE_GEOM 
                                            FROM $rsuDiffTot
                                            WHERE ST_DIMENSION(THE_GEOM)=1)')
                            UNION 
                                SELECT $ID_COLUMN_RSU, THE_GEOM
                                FROM $multiptsRSU""")

        datasource.createSpatialIndex(multiptsRSUtot, "the_geom")
        // The SVF calculation is performed at point scale
        datasource.execute("""
                CREATE TABLE $svfPts 
                AS SELECT   a.$ID_COLUMN_RSU, 
                            ST_SVF(ST_GEOMETRYN(a.the_geom,1), ST_ACCUM(ST_UPDATEZ(b.$GEOMETRIC_COLUMN_BU, b.$HEIGHT_WALL)), 
                                   $rayLength, $numberOfDirection, 5) AS SVF
                FROM        $multiptsRSUtot AS a, $correlationBuildingTable AS b 
                WHERE       ST_EXPAND(a.the_geom, $rayLength) && b.$GEOMETRIC_COLUMN_BU AND 
                            ST_DWITHIN(b.$GEOMETRIC_COLUMN_BU, a.the_geom, $rayLength) 
                GROUP BY    a.the_geom""")
        datasource.createIndex(svfPts, ID_COLUMN_RSU)

        // The result of the SVF calculation is averaged at RSU scale
        datasource.execute("""
                CREATE TABLE $outputTableName($ID_COLUMN_RSU integer, $BASE_NAME double) 
                AS          (SELECT a.$ID_COLUMN_RSU, CASE WHEN AVG(b.SVF) is not null THEN AVG(b.SVF) ELSE 1 END
                FROM        $rsu a 
                LEFT JOIN   $svfPts b 
                ON          a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU
                GROUP BY    a.$ID_COLUMN_RSU)""")

        debug "SVF calculation time: ${(System.currentTimeMillis() - to_start) / 1000} s"

        // The temporary tables are deleted
        datasource.dropTable(rsuDiff, rsuDiffTot,
                multiptsRSU, multiptsRSUtot, ptsRSUtot,
                pts_order, svfPts, pts_RANG)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the ground sky view factor at RSU scale", e)
    } finally {
        datasource.dropTable(rsuDiff, rsuDiffTot,
                multiptsRSU, multiptsRSUtot, ptsRSUtot,
                pts_order, svfPts, pts_RANG)
    }
}

/**
 * Process used to compute the aspect ratio such as defined by Stewart et Oke (2012): mean height-to-width ratio
 * of street canyons (LCZs 1-7), building spacing (LCZs 8-10), and tree spacing (LCZs A - G). A simple approach based
 * on the street canyons assumption is used for the calculation. The sum of facade area within a given RSU area
 * is divided by the area of free surfaces of the given RSU (not covered by buildings). The
 * "rsu_free_external_facade_density" and "rsu_building_density" are used for the calculation.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input table and in which
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
String aspectRatio(JdbcDataSource datasource, String rsuTable, String rsuFreeExternalFacadeDensityColumn, String rsuBuildingDensityColumn,
                   prefixName) throws Exception {
    try {
        def COLUMN_ID_RSU = "id_rsu"
        def BASE_NAME = "aspect_ratio"
        debug "Executing RSU aspect ratio"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME
        datasource.execute("""
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName AS 
                    SELECT CASE WHEN $rsuBuildingDensityColumn = 1 
                        THEN null 
                        ELSE 0.5 * ($rsuFreeExternalFacadeDensityColumn/(1-$rsuBuildingDensityColumn)) END 
                    AS $BASE_NAME, $COLUMN_ID_RSU FROM $rsuTable""")

        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the aspect ratio at RSU scale", e)
    }
}

/**
 * Script to compute the distribution of projected facade area within a RSU per vertical layer and direction
 * of analysis (ie. wind or sun direction). Note that the method used is an approximation if the RSU split
 * a building into two parts (the facade included within the RSU is counted half).
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsu The name of the input ITable where are stored the RSU
 * @param building The name of the input ITable where are stored the buildings
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20,
 * 30, 40, 50])
 * @param numberOfDirection the number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 12)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String projectedFacadeAreaDistribution(JdbcDataSource datasource, String building, String rsu, String id_rsu,
                                       List listLayersBottom = [0, 10, 20, 30, 40, 50], int numberOfDirection = 12,
                                       String prefixName) throws Exception {

    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def buildingIntersection = postfix "building_intersection"
    def buildingIntersectionExpl = postfix "building_intersection_expl"
    def buildingFree = postfix "buildingFree"
    def buildingLayer = postfix "buildingLayer"
    def buildingFreeExpl = postfix "buildingFreeExpl"
    def rsuInter = postfix "rsuInter"
    def finalIndicator = postfix "finalIndicator"
    try {
        def BASE_NAME = "projected_facade_area_distribution"
        def GEOMETRIC_COLUMN_RSU = "the_geom"
        def GEOMETRIC_COLUMN_BU = "the_geom"
        def ID_COLUMN_RSU = id_rsu
        def ID_COLUMN_BU = "id_build"
        def HEIGHT_WALL = "height_wall"

        debug "Executing RSU projected facade area distribution"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        datasource.createSpatialIndex(building, "the_geom")
        datasource.createSpatialIndex(rsu, "the_geom")
        datasource.createIndex(building, "id_build")
        datasource.createIndex(rsu, ID_COLUMN_RSU)

        if (360 % numberOfDirection == 0 && numberOfDirection % 2 == 0) {


            // The projection should be performed at the median of the angle interval
            def dirMedDeg = 180 / numberOfDirection
            def dirMedRad = toRadians(dirMedDeg)

            dirMedDeg = round(dirMedDeg)

            // The list that will store the fields name is initialized
            def names = []

            // Common party walls between buildings are calculated
            datasource.execute("""
                    DROP TABLE IF EXISTS $buildingIntersection;
                    CREATE TABLE $buildingIntersection( the_geom GEOMETRY, id_build_a INTEGER, id_build_b INTEGER, z_max DOUBLE, z_min DOUBLE) AS 
                        SELECT ST_CollectionExtract(t.the_geom,2), t.id_build_a , t.id_build_b , t.z_max , t.z_min 
                        FROM (
                            SELECT ST_INTERSECTION(ST_MAKEVALID(a.$GEOMETRIC_COLUMN_BU), 
                                ST_MAKEVALID(b.$GEOMETRIC_COLUMN_BU)) AS the_geom, 
                                a.$ID_COLUMN_BU AS id_build_a, 
                                b.$ID_COLUMN_BU AS id_build_b, 
                                GREATEST(a.$HEIGHT_WALL,b.$HEIGHT_WALL) AS z_max, 
                                LEAST(a.$HEIGHT_WALL,b.$HEIGHT_WALL) AS z_min 
                            FROM $building AS a, $building AS b 
                            WHERE a.$GEOMETRIC_COLUMN_BU && b.$GEOMETRIC_COLUMN_BU 
                            AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_BU, b.$GEOMETRIC_COLUMN_BU) 
                             AND a.$ID_COLUMN_BU <> b.$ID_COLUMN_BU) AS t
            """)

            datasource.createIndex(buildingIntersection, "id_build_a")
            datasource.createIndex(buildingIntersection, "id_build_b")

            // Each free facade is stored TWICE (an intersection could be seen from the point of view of two
            // buildings).
            // Facades of isolated buildings are unioned to free facades of non-isolated buildings which are
            // unioned to free intersection facades. To each facade is affected its corresponding free height
            datasource.execute("""
                    DROP TABLE IF EXISTS $buildingFree;
                    CREATE TABLE $buildingFree (the_geom GEOMETRY, z_max double precision, z_min double precision)
                    AS (SELECT  ST_TOMULTISEGMENTS(a.the_geom) as the_geom, a.$HEIGHT_WALL as z_max, 0  as z_min
                    FROM $building a WHERE a.$ID_COLUMN_BU NOT IN (SELECT ID_build_a 
                    FROM $buildingIntersection)) UNION ALL (SELECT  
                    ST_TOMULTISEGMENTS(ST_DIFFERENCE(ST_TOMULTILINE(a.$GEOMETRIC_COLUMN_BU), 
                    ST_UNION(ST_ACCUM(b.the_geom)))) as the_geom, a.$HEIGHT_WALL as z_max, 0 as z_min FROM $building a, 
                    $buildingIntersection b WHERE a.$ID_COLUMN_BU=b.ID_build_a and st_isempty(b.the_geom)=false
                    GROUP BY b.ID_build_a) UNION ALL (SELECT ST_TOMULTISEGMENTS(the_geom)
                    AS the_geom, z_max, z_min FROM $buildingIntersection WHERE ID_build_a<ID_build_b)""")

            // The height of wall is calculated for each intermediate level...
            def layerQuery = "DROP TABLE IF EXISTS $buildingLayer; " +
                    "CREATE TABLE $buildingLayer AS SELECT the_geom, "
            for (i in 1..(listLayersBottom.size() - 1)) {
                names.add(getDistribIndicName(BASE_NAME, 'H', listLayersBottom[i - 1], listLayersBottom[i]))
                layerQuery += "CASEWHEN(z_max <= ${listLayersBottom[i - 1]}, 0, " +
                        "CASEWHEN(z_min >= ${listLayersBottom[i]}, " +
                        "0, ${listLayersBottom[i] - listLayersBottom[i - 1]}-" +
                        "GREATEST(${listLayersBottom[i]}-z_max,0)" +
                        "-GREATEST(z_min-${listLayersBottom[i - 1]},0))) AS ${names[i - 1]} ,"
            }

            // ...and for the final level
            names.add(getDistribIndicName(BASE_NAME, 'H', listLayersBottom[listLayersBottom.size() - 1], null))
            layerQuery += "CASEWHEN(z_max >= ${listLayersBottom[listLayersBottom.size() - 1]}, " +
                    "z_max-GREATEST(z_min,${listLayersBottom[listLayersBottom.size() - 1]}), 0) " +
                    "AS ${names[listLayersBottom.size() - 1]} FROM $buildingFree"
            datasource layerQuery.toString()

            // Names and types of all columns are then useful when calling sql queries
            def namesAndType = names.inject([]) { result, iter ->
                result += " $iter double precision"
            }.join(",")
            def onlyNamesB = names.inject([]) { result, iter ->
                result += "b.$iter"
            }.join(",")
            def onlyNames = names.join(",")

            datasource.createSpatialIndex(buildingLayer, "the_geom")

            // Intersections between free facades and rsu geometries are calculated
            datasource.execute(""" DROP TABLE IF EXISTS $buildingFreeExpl; 
                    CREATE TABLE $buildingFreeExpl($ID_COLUMN_RSU INTEGER, the_geom GEOMETRY, $namesAndType) AS 
                    (SELECT a.$ID_COLUMN_RSU, ST_INTERSECTION(a.$GEOMETRIC_COLUMN_RSU, ST_TOMULTILINE(b.the_geom)), 
                    ${onlyNamesB} FROM $rsu a, $buildingLayer b 
                    WHERE a.$GEOMETRIC_COLUMN_RSU && b.the_geom 
                    AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_RSU, b.the_geom))""")


            // Intersections  facades are exploded to multisegments
            datasource.execute("""DROP TABLE IF EXISTS $rsuInter; 
                        CREATE TABLE $rsuInter($ID_COLUMN_RSU INTEGER, the_geom GEOMETRY, $namesAndType) 
                        AS (SELECT $ID_COLUMN_RSU, the_geom, ${onlyNames} FROM ST_EXPLODE('$buildingFreeExpl'))""")


            // The analysis is then performed for each direction ('numberOfDirection' / 2 because calculation
            // is performed for a direction independently of the "toward")
            def namesAndTypeDir = []
            def onlyNamesDir = []
            def sumNamesDir = []
            def queryColumns = []
            for (int d = 0; d < numberOfDirection / 2; d++) {
                int dirDeg = d * 360 / numberOfDirection
                def dirRad = toRadians(dirDeg)
                int rangeDeg = 360 / numberOfDirection
                def dirRadMid = dirRad + dirMedRad
                def dirDegMid = dirDeg + dirMedDeg
                // Define the field name for each of the directions and vertical layers
                names.each {
                    namesAndTypeDir += " " + "${getDistribIndicName(it, 'D', dirDeg, dirDeg + rangeDeg)} double precision"
                    queryColumns += """CASE
                            WHEN  a.azimuth-$dirRadMid>PI()/2
                            THEN  a.$it*a.length*COS(a.azimuth-$dirRadMid-PI()/2)/2
                            WHEN  a.azimuth-$dirRadMid<-PI()/2
                            THEN  a.$it*a.length*COS(a.azimuth-$dirRadMid+PI()/2)/2
                            ELSE  a.$it*a.length*ABS(SIN(a.azimuth-$dirRadMid))/2 
                            END AS ${getDistribIndicName(it, 'D', dirDeg, dirDeg + rangeDeg)}"""
                    onlyNamesDir += "${it}_D${dirDeg}_${dirDeg + rangeDeg}"
                    sumNamesDir += "COALESCE(SUM(b.${it}_D${dirDeg}_${dirDeg + rangeDeg}), 0) " +
                            "AS ${getDistribIndicName(it, 'D', dirDeg, dirDeg + rangeDeg)}"
                }
            }
            namesAndTypeDir = namesAndTypeDir.join(",")
            queryColumns = queryColumns.join(",")
            onlyNamesDir = onlyNamesDir.join(",")
            sumNamesDir = sumNamesDir.join(",")

            def query = "DROP TABLE IF EXISTS $finalIndicator; " +
                    "CREATE TABLE $finalIndicator AS SELECT a.$ID_COLUMN_RSU," + queryColumns +
                    " FROM (SELECT $ID_COLUMN_RSU, CASE WHEN ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)) >= PI()" +
                    "THEN ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)) - PI() " +
                    "ELSE ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)) END AS azimuth," +
                    " ST_LENGTH(the_geom) AS length, ${onlyNames} FROM $rsuInter) a"

            datasource query.toString()


            datasource.createIndex(finalIndicator, ID_COLUMN_RSU)
            // Sum area at RSU scale and fill null values with 0
            datasource.execute("""
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE    ${outputTableName} 
                    AS SELECT       a.$ID_COLUMN_RSU, ${sumNamesDir} 
                    FROM            $rsu a LEFT JOIN $finalIndicator b 
                    ON              a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU 
                    GROUP BY        a.$ID_COLUMN_RSU""")

            // Remove all temporary tables created
            datasource.dropTable(buildingIntersection, buildingIntersectionExpl, buildingFree, buildingLayer,
                    buildingFreeExpl, rsuInter, finalIndicator)
        }
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the projected facade area distribution at RSU scale", e)
    } finally {
        datasource.dropTable(buildingIntersection, buildingIntersectionExpl, buildingFree, buildingLayer,
                buildingFreeExpl, rsuInter, finalIndicator)
    }
}

/**
 * Create indicator names for building facade and building roof distribution indicators
 *
 * @param base_name The base name of the indicator
 * @param var_type A prefix corresponding to the type of values to come after ("h" for height range and "d" for direction range)
 * @param lev_bot The bottom limit of the level (for height range or direction range)
 * @param lev_up The upper limit of the level (for height range or direction range)
 *
 * @return Column (indicator) name
 *
 * @author Jérémy Bernard
 */
String getDistribIndicName(String base_name, String var_type, Integer lev_bot, Integer lev_up) {
    String name
    if (lev_up == null) {
        name = "${base_name}_${var_type}${lev_bot}"
    } else {
        name = "${base_name}_${var_type}${lev_bot}_${lev_up}"
    }
    return name
}

/**
 * Script to compute both the roof (vertical, horizontal and tilted)
 * - area within each vertical layer of a RSU.
 * - optionally the density within the RSU independantly of the layer considered
 *
 * WARNING: Note that the method used is based on the assumption that all buildings have gable roofs. Since we do not know
 * what is the direction of the gable, we also consider that buildings are square in order to limit the potential
 * calculation error (for example considering that the gable is always oriented parallel to the larger side of the
 * building.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param building the name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
 * @param rsu the name of the input ITable where are stored the RSU
 * @param prefixName String use as prefix to name the output table
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20,
 * 30, 40, 50])
 * @param density Boolean to set whether or not the roof density should be calculated in addition to the distribution
 * (default true)
 *
 * @return Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
String roofAreaDistribution(JdbcDataSource datasource, String rsu, String building,
                            List listLayersBottom = [0, 10, 20, 30, 40, 50], String prefixName,
                            boolean density = true) throws Exception {
    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def buildRoofSurfIni = postfix "build_roof_surf_ini"
    def buildVertRoofInter = postfix "build_vert_roof_inter"
    def buildVertRoofAll = postfix "buildVertRoofAll"
    def buildRoofSurfTot = postfix "build_roof_surf_tot"
    def optionalTempo = postfix "optionalTempo"
    try {
        def GEOMETRIC_COLUMN_RSU = "the_geom"
        def GEOMETRIC_COLUMN_BU = "the_geom"
        def ID_COLUMN_RSU = "id_rsu"
        def ID_COLUMN_BU = "id_build"
        def HEIGHT_WALL = "height_wall"
        def HEIGHT_ROOF = "height_roof"
        def BASE_NAME = "roof_area_distribution"

        debug "Executing RSU roof area distribution (and optionally roof density)"

        datasource.createSpatialIndex(rsu, "the_geom")
        datasource.createIndex(rsu, "id_rsu")

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        // Vertical and non-vertical (tilted and horizontal) roof areas are calculated
        datasource.execute("""
                DROP TABLE IF EXISTS $buildRoofSurfIni;
                CREATE TABLE $buildRoofSurfIni AS 
                    SELECT $GEOMETRIC_COLUMN_BU, 
                        $ID_COLUMN_RSU,$ID_COLUMN_BU, 
                        $HEIGHT_ROOF AS z_max, 
                        $HEIGHT_WALL AS z_min, 
                        ST_AREA($GEOMETRIC_COLUMN_BU) AS building_area, 
                        ST_PERIMETER($GEOMETRIC_COLUMN_BU)+ST_PERIMETER(ST_HOLES($GEOMETRIC_COLUMN_BU)) AS building_total_facade_length, 
                        $HEIGHT_ROOF-$HEIGHT_WALL AS delta_h, 
                        POWER(POWER(ST_AREA($GEOMETRIC_COLUMN_BU),2)+4*ST_AREA($GEOMETRIC_COLUMN_BU)*POWER($HEIGHT_ROOF-$HEIGHT_WALL,2),0.5) AS non_vertical_roof_area,
                        POWER(ST_AREA($GEOMETRIC_COLUMN_BU), 0.5)*($HEIGHT_ROOF-$HEIGHT_WALL) AS vertical_roof_area 
                    FROM $building;
        """)


        // Indexes and spatial indexes are created on rsu and building Tables
        datasource.execute("""CREATE SPATIAL INDEX IF NOT EXISTS ids_ina ON $buildRoofSurfIni ($GEOMETRIC_COLUMN_BU);
                CREATE INDEX IF NOT EXISTS id_ina ON $buildRoofSurfIni ($ID_COLUMN_BU);""")

        // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
        // and the corresponding area is estimated (only if the building roof does not overpass the building
        // wall of the neighbor)
        datasource.execute("""
                DROP TABLE IF EXISTS $buildVertRoofInter; 
                CREATE TABLE $buildVertRoofInter(id_build INTEGER, vert_roof_to_remove DOUBLE) AS (
                    SELECT b.$ID_COLUMN_BU, 
                        sum(CASEWHEN(b.building_area>a.building_area, 
                            POWER(a.building_area,0.5)*b.delta_h/2, 
                            POWER(b.building_area,0.5)*b.delta_h/2)) 
                    FROM $buildRoofSurfIni a, $buildRoofSurfIni b 
                    WHERE a.$GEOMETRIC_COLUMN_BU && b.$GEOMETRIC_COLUMN_BU 
                    AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_BU, b.$GEOMETRIC_COLUMN_BU) 
                    AND a.$ID_COLUMN_BU <> b.$ID_COLUMN_BU 
                    AND a.z_min >= b.z_max 
                    GROUP BY b.$ID_COLUMN_BU);""")

        // Indexes and spatial indexes are created on rsu and building Tables
        datasource.execute("""CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofInter ($ID_COLUMN_BU);""")

        // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
        // and the corresponding area is estimated (only if the building roof does not overpass the building wall
        // of the neighbor)
        datasource.execute("""
                DROP TABLE IF EXISTS $buildVertRoofAll;
                CREATE TABLE $buildVertRoofAll(
                    id_build INTEGER, 
                    the_geom GEOMETRY, 
                    id_rsu INTEGER, 
                    z_max DOUBLE, 
                    z_min DOUBLE, 
                    delta_h DOUBLE, 
                    building_area DOUBLE, 
                    building_total_facade_length DOUBLE, 
                    non_vertical_roof_area DOUBLE, 
                    vertical_roof_area DOUBLE, 
                    vert_roof_to_remove DOUBLE) 
                AS (SELECT 
                        a.$ID_COLUMN_BU, 
                        a.$GEOMETRIC_COLUMN_BU, 
                        a.$ID_COLUMN_RSU, 
                        a.z_max, 
                        a.z_min,
                        a.delta_h, 
                        a.building_area, 
                        a.building_total_facade_length, 
                        a.non_vertical_roof_area, 
                        a.vertical_roof_area, 
                        IFNULL(b.vert_roof_to_remove,0) 
                    FROM $buildRoofSurfIni a 
                    LEFT JOIN $buildVertRoofInter b 
                    ON a.$ID_COLUMN_BU=b.$ID_COLUMN_BU);""")

        // Indexes and spatial indexes are created on rsu and building Tables
        datasource.execute("""CREATE SPATIAL INDEX IF NOT EXISTS ids_bu ON $buildVertRoofAll (the_geom); 
                    CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofAll (id_build); 
                    CREATE INDEX IF NOT EXISTS id_rsu ON $buildVertRoofAll (id_rsu);""")

        //TODO : PEUT-ETRE MIEUX VAUT-IL FAIRE L'INTERSECTION À PART POUR ÉVITER DE LA FAIRE 2 FOIS ICI ?

        // Update the roof areas (vertical and non vertical) taking into account the vertical roofs shared with
        // the neighbor facade and the roof surfaces that are not in the RSU. Note that half of the facade
        // are considered as vertical roofs, the other to "normal roof".
        datasource.execute("""
                DROP TABLE IF EXISTS $buildRoofSurfTot; 
                CREATE TABLE $buildRoofSurfTot(
                    id_build INTEGER,
                    id_rsu INTEGER, 
                    z_max DOUBLE, 
                    z_min DOUBLE, 
                    delta_h DOUBLE, 
                    non_vertical_roof_area DOUBLE, 
                    vertical_roof_area DOUBLE) 
                AS (SELECT 
                        a.id_build, 
                        a.id_rsu, 
                        a.z_max, 
                        a.z_min, 
                        a.delta_h, 
                        a.non_vertical_roof_area*
                            ST_AREA(ST_INTERSECTION(a.the_geom, b.$GEOMETRIC_COLUMN_RSU))/
                            a.building_area AS non_vertical_roof_area, 
                        (a.vertical_roof_area-a.vert_roof_to_remove)*
                            (1-0.5*(1-ST_LENGTH(ST_ACCUM(ST_INTERSECTION(ST_TOMULTILINE(a.the_geom), b.$GEOMETRIC_COLUMN_RSU)))/
                            a.building_total_facade_length)) 
                    FROM $buildVertRoofAll a, $rsu b 
                    WHERE a.id_rsu=b.$ID_COLUMN_RSU 
                    GROUP BY b.$GEOMETRIC_COLUMN_RSU, a.id_build, a.id_rsu, a.z_max, a.z_min, a.delta_h);""")

        // The roof area is calculated for each level except the last one (> 50 m in the default case)
        def finalQuery = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.id_rsu, "
        def nonVertQuery = ""
        def vertQuery = ""
        for (i in 1..(listLayersBottom.size() - 1)) {
            nonVertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(" +
                    "a.z_max <= ${listLayersBottom[i]}, CASEWHEN(a.delta_h=0, a.non_vertical_roof_area, " +
                    "a.non_vertical_roof_area*(a.z_max-GREATEST(${listLayersBottom[i - 1]},a.z_min))/a.delta_h), " +
                    "CASEWHEN(a.z_min < ${listLayersBottom[i]}, a.non_vertical_roof_area*(${listLayersBottom[i]}-" +
                    "GREATEST(${listLayersBottom[i - 1]},a.z_min))/a.delta_h, 0)))),0) AS " +
                    "${getDistribIndicName('non_vert_roof_area', 'H', listLayersBottom[i - 1], listLayersBottom[i])},"
            vertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(" +
                    "a.z_max <= ${listLayersBottom[i]}, CASEWHEN(a.delta_h=0, 0, " +
                    "a.vertical_roof_area*POWER((a.z_max-GREATEST(${listLayersBottom[i - 1]}," +
                    "a.z_min))/a.delta_h, 2)), CASEWHEN(a.z_min < ${listLayersBottom[i]}, " +
                    "CASEWHEN(a.z_min>${listLayersBottom[i - 1]}, a.vertical_roof_area*(1-" +
                    "POWER((a.z_max-${listLayersBottom[i]})/a.delta_h,2)),a.vertical_roof_area*(" +
                    "POWER((a.z_max-${listLayersBottom[i - 1]})/a.delta_h,2)-POWER((a.z_max-${listLayersBottom[i]})/" +
                    "a.delta_h,2))), 0)))),0) AS ${getDistribIndicName('vert_roof_area', 'H', listLayersBottom[i - 1], listLayersBottom[i])},"
        }
        // The roof area is calculated for the last level (> 50 m in the default case)
        def valueLastLevel = listLayersBottom[listLayersBottom.size() - 1]
        nonVertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= $valueLastLevel, 0, CASEWHEN(a.delta_h=0, a.non_vertical_roof_area, " +
                "a.non_vertical_roof_area*(a.z_max-GREATEST($valueLastLevel,a.z_min))/a.delta_h))),0) AS " +
                "${getDistribIndicName('non_vert_roof_area', 'H', valueLastLevel, null)},"
        vertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= $valueLastLevel, 0, CASEWHEN(a.delta_h=0, a.vertical_roof_area, " +
                "a.vertical_roof_area*(a.z_max-GREATEST($valueLastLevel,a.z_min))/a.delta_h))),0) " +
                "${getDistribIndicName('vert_roof_area', 'H', valueLastLevel, null)},"

        def endQuery = """ FROM $buildRoofSurfTot a RIGHT JOIN $rsu b 
                                    ON a.id_rsu = b.id_rsu GROUP BY b.id_rsu;"""

        datasource.execute(finalQuery.toString() + nonVertQuery.toString() + vertQuery[0..-2].toString() + endQuery.toString())

        // Calculate the roof density if needed
        if (density) {
            def optionalQuery = "ALTER TABLE $outputTableName RENAME TO $optionalTempo;" +
                    "CREATE INDEX IF NOT EXISTS id ON $optionalTempo USING BTREE($ID_COLUMN_RSU);" +
                    "DROP TABLE IF EXISTS $outputTableName; " +
                    "CREATE TABLE $outputTableName AS SELECT a.*, "
            def optionalNonVert = "("
            def optionalVert = "("

            for (i in 1..(listLayersBottom.size() - 1)) {
                optionalNonVert += " a.${getDistribIndicName('non_vert_roof_area', 'H', listLayersBottom[i - 1], listLayersBottom[i])} + "
                optionalVert += "a.${getDistribIndicName('vert_roof_area', 'H', listLayersBottom[i - 1], listLayersBottom[i])} + "
            }
            optionalNonVert += "a.${getDistribIndicName('non_vert_roof_area', 'H', valueLastLevel, null)}) / ST_AREA(b.$GEOMETRIC_COLUMN_RSU)"
            optionalVert += "a.${getDistribIndicName('vert_roof_area', 'H', valueLastLevel, null)}) / ST_AREA(b.$GEOMETRIC_COLUMN_RSU)"
            optionalQuery += "$optionalNonVert AS VERT_ROOF_DENSITY, $optionalVert AS NON_VERT_ROOF_DENSITY" +
                    " FROM $optionalTempo a RIGHT JOIN $rsu b ON a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU;"

            datasource.execute(optionalQuery)
        }

        datasource.dropTable(buildRoofSurfIni, buildVertRoofInter, buildVertRoofAll, buildRoofSurfTot, optionalTempo)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the roof area distribution at RSU scale", e)
    } finally {
        datasource.dropTable(buildRoofSurfIni, buildVertRoofInter, buildVertRoofAll, buildRoofSurfTot, optionalTempo)
    }
}

/**
 * Script to compute the effective terrain roughness length (z0). The method for z0 calculation is based on the
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
 * @param id_rsu Unique identifier column name
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
 * @return Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
String effectiveTerrainRoughnessLength(JdbcDataSource datasource, String rsuTable, String id_rsu,
                                       String projectedFacadeAreaName,
                                       String geometricMeanBuildingHeightName,
                                       List listLayersBottom = [0, 10, 20, 30, 40, 50],
                                       int numberOfDirection = 12, String prefixName) throws Exception {
    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def lambdaTable = postfix "lambdaTable"
    try {
        def GEOMETRIC_COLUMN = "the_geom"
        def ID_COLUMN_RSU = id_rsu
        def BASE_NAME = "effective_terrain_roughness_length"

        debug "Executing RSU effective terrain roughness length"

        // Processes used for the indicator calculation
        // Some local variables are initialized
        def names = []
        // The projection should be performed at the median of the angle interval
        def dirRangeDeg = round(360 / numberOfDirection)
        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        def layerSize = listLayersBottom.size()
        // The lambda_f indicator is first calculated
        def lambdaQuery = "DROP TABLE IF EXISTS $lambdaTable;" +
                "CREATE TABLE $lambdaTable AS SELECT $ID_COLUMN_RSU, $geometricMeanBuildingHeightName, ("
        for (int i in 1..layerSize) {
            //TODO : why an array here and not a variable
            names[i - 1] = "${projectedFacadeAreaName}_H${listLayersBottom[i - 1]}_${listLayersBottom[i]}"
            if (i == layerSize) {
                names[layerSize - 1] =
                        "${projectedFacadeAreaName}_H${listLayersBottom[layerSize - 1]}"
            }
            for (int d = 0; d < numberOfDirection / 2; d++) {
                def dirDeg = d * 360 / numberOfDirection
                lambdaQuery += "${names[i - 1]}_D${dirDeg}_${dirDeg + dirRangeDeg}+"
            }
        }
        lambdaQuery = lambdaQuery[0..-2] + ")/(${numberOfDirection / 2}*ST_AREA($GEOMETRIC_COLUMN)) " +
                "AS lambda_f FROM $rsuTable"
        datasource.execute(lambdaQuery.toString())

        // The rugosity z0 is calculated according to the indicator lambda_f (the value of indicator z0 is limited to 3 m)
        datasource.execute("""DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName 
                    AS SELECT $ID_COLUMN_RSU, CASEWHEN(lambda_f < 0.15, CASEWHEN(lambda_f*$geometricMeanBuildingHeightName>3,
                    3,lambda_f*$geometricMeanBuildingHeightName), CASEWHEN(0.15*$geometricMeanBuildingHeightName>3,3,
                    0.15*$geometricMeanBuildingHeightName)) AS $BASE_NAME FROM $lambdaTable;
            DROP TABLE IF EXISTS $lambdaTable""")

        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the effective terrain roughness length at RSU scale", e)
    } finally {
        datasource.dropTable(lambdaTable)
    }
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
 * @return Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
String linearRoadOperations(JdbcDataSource datasource, String rsuTable, String roadTable, List operations, int angleRangeSize = 30,
                            List levelConsiderated = [0], String prefixName) throws Exception {
    def OPS = ["road_direction_distribution", "linear_road_density"]
    def GEOMETRIC_COLUMN_RSU = "the_geom"
    def ID_COLUMN_RSU = "id_rsu"
    def GEOMETRIC_COLUMN_ROAD = "the_geom"
    def Z_INDEX = "zindex"
    def BASE_NAME = "rsu_road_linear_properties"

    debug "Executing Operations on the linear of road"

    datasource.createSpatialIndex(rsuTable, "the_geom")
    datasource.createIndex(rsuTable, "id_rsu")
    datasource.createSpatialIndex(roadTable, "the_geom")
    datasource.createIndex(roadTable, "zindex")

    // Test whether the angleRangeSize is a divisor of 180°
    if (180 % angleRangeSize == 0 && 180 / angleRangeSize > 1) {
        // Test whether the operations filled by the user are OK
        if (!operations) {
            error "The parameter operations should not be null or empty"
        } else {
            // The operation names are transformed into lower case
            operations.replaceAll { it.toLowerCase() }

            def opOk = true
            operations.each { opOk &= OPS.contains(it) }
            if (opOk) {
                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names
                def roadInter = postfix "roadInter"
                def roadExpl = postfix "roadExpl"
                def roadDistrib = postfix "roadDistrib"
                def roadDens = postfix "roadDens"
                def roadDistTot = postfix "roadDistTot"
                def roadDensTot = postfix "roadDensTot"

                try {
                    // The name of the outputTableName is constructed
                    def outputTableName = prefix prefixName, BASE_NAME

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
                        levelConsiderated.each { filtering += "$baseFiltering AND b.$Z_INDEX=$it OR " }
                        filtering = filtering[0..-4]
                    }
                    datasource.execute("""DROP TABLE IF EXISTS $roadInter; 
                        CREATE TABLE $roadInter AS SELECT a.$ID_COLUMN_RSU AS id_rsu, 
                        ST_AREA(a.$GEOMETRIC_COLUMN_RSU) AS rsu_area, ST_INTERSECTION(a.$GEOMETRIC_COLUMN_RSU, 
                        b.$GEOMETRIC_COLUMN_ROAD) AS the_geom $ifZindex FROM $rsuTable a, $roadTable b 
                        WHERE $filtering;""")

                    // If all roads are considered at the same level...
                    if (!levelConsiderated) {
                        nameDens.add("linear_road_density")
                        caseQueryDens = "SUM(ST_LENGTH(the_geom))/rsu_area AS linear_road_density "
                        for (int d = angleRangeSize; d <= 180; d += angleRangeSize) {
                            caseQueryDistrib += "SUM(CASEWHEN(azimuth>=${d - angleRangeSize} AND azimuth<$d, length, 0)) AS " +
                                    "${getRoadDirIndic(d, angleRangeSize, null)},"
                            nameDistrib.add(getRoadDirIndic(d, angleRangeSize, null))
                        }
                    }
                    // If only certain levels are considered independently
                    else {
                        ifZindex = ", zindex "
                        levelConsiderated.each { lev ->
                            caseQueryDens += "SUM(CASEWHEN(zindex = $lev, ST_LENGTH(the_geom), 0))/rsu_area " +
                                    "AS linear_road_density_h${lev.toString().replaceAll('-', 'minus')},"
                            nameDens.add("linear_road_density_h${lev.toString().replaceAll('-', 'minus')}")
                            for (int d = angleRangeSize; d <= 180; d += angleRangeSize) {
                                caseQueryDistrib += "SUM(CASEWHEN(azimuth>=${d - angleRangeSize} AND azimuth<$d AND " +
                                        "zindex = $lev, length, 0)) AS " +
                                        "${getRoadDirIndic(d, angleRangeSize, lev)},"
                                nameDistrib.add(getRoadDirIndic(d, angleRangeSize, lev))
                            }
                        }
                    }

                    //      2. Depending on the operations to proceed, the queries executed during this step will differ
                    // If the road direction distribution is calculated, explode the roads into segments in order to calculate
                    // their length for each azimuth range
                    if (operations.contains("road_direction_distribution")) {
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
                                "CREATE INDEX IF NOT EXISTS id_d ON $roadDistrib (id_rsu);" +
                                "DROP TABLE IF EXISTS $roadDistTot; CREATE TABLE $roadDistTot($ID_COLUMN_RSU INTEGER," +
                                "${nameDistrib.join(" double precision,")} double precision) AS (SELECT a.$ID_COLUMN_RSU," +
                                "COALESCE(b.${nameDistrib.join(",0),COALESCE(b.")},0)  " +
                                "FROM $rsuTable a LEFT JOIN $roadDistrib b ON a.$ID_COLUMN_RSU=b.id_rsu);"
                        datasource.execute(queryDistrib)

                        if (!operations.contains("linear_road_density")) {
                            datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
                                        ALTER TABLE $roadDistTot RENAME TO $outputTableName""")
                        }
                    }

                    // If the rsu linear density should be calculated
                    if (operations.contains("linear_road_density")) {
                        String queryDensity = "DROP TABLE IF EXISTS $roadDens;" +
                                "CREATE TABLE $roadDens AS SELECT id_rsu, " + caseQueryDens[0..-2] +
                                " FROM $roadInter GROUP BY id_rsu;" +
                                "CREATE INDEX IF NOT EXISTS id_d ON $roadDens (id_rsu);" +
                                "DROP TABLE IF EXISTS $roadDensTot; CREATE TABLE $roadDensTot($ID_COLUMN_RSU INTEGER," +
                                "${nameDens.join(" double,")} double) AS (SELECT a.$ID_COLUMN_RSU," +
                                "COALESCE(b.${nameDens.join(",0),COALESCE(b.")},0) " +
                                "FROM $rsuTable a LEFT JOIN $roadDens b ON a.$ID_COLUMN_RSU=b.id_rsu)"
                        datasource.execute(queryDensity)
                        if (!operations.contains("road_direction_distribution")) {
                            datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
                                        ALTER TABLE $roadDensTot RENAME TO $outputTableName""")
                        }
                    }
                    if (operations.contains("road_direction_distribution") &&
                            operations.contains("linear_road_density")) {
                        datasource.execute("""DROP TABLE if exists $outputTableName; 
                                CREATE INDEX IF NOT EXISTS idx_$roadDistTot ON $roadDistTot (id_rsu);
                                CREATE INDEX IF NOT EXISTS idx_$roadDensTot ON $roadDensTot (id_rsu);
                                CREATE TABLE $outputTableName AS SELECT a.*,
                                b.${nameDens.join(",b.")} FROM $roadDistTot a LEFT JOIN $roadDensTot b 
                                ON a.id_rsu=b.id_rsu""")
                    }
                    datasource.dropTable(roadInter, roadExpl, roadDistrib,
                            roadDens, roadDistTot, roadDensTot)
                    return outputTableName
                } catch (SQLException e) {
                    throw new SQLException("Cannot compute the linear road operations at RSU scale", e)
                } finally {
                    datasource.dropTable(roadInter, roadExpl, roadDistrib,
                            roadDens, roadDistTot, roadDensTot)
                }

            } else {
                throw new SQLException("One of several operations are not valid.")
            }
        }
    } else {
        throw new SQLException("Cannot compute the indicator. The range size (angleRangeSize) should be a divisor of 180°")
    }
}

/**
 * Create indicator names for road distribution indicators
 *
 * @param d The upper limit of the angle considered for road direction (°)
 * @param angleRangeSize the angle range
 * @param lev The level of the roads
 *
 * @return Column (indicator) name
 *
 * @author Jérémy Bernard
 */
String getRoadDirIndic(int d, Integer angleRangeSize, Integer lev) {
    String name
    if (lev == null) {
        name = "road_direction_distribution_d${d - angleRangeSize}_$d"
    } else {
        name = "road_direction_distribution_h${lev.toString().replaceAll("-", "minus")}" +
                "_d${d - angleRangeSize}_$d"
    }

    return name
}

/**
 * Script to compute the effective terrain class from the terrain roughness length (z0).
 * The classes are defined according to the Davenport lookup Table (cf Table 5 in Stewart and Oke, 2012)
 *
 * Warning: the Davenport definition defines a class for a given z0 value. Then there is no definition of the z0 range
 * corresponding to a certain class. Then we have arbitrarily defined the z0 value corresponding to a certain
 * Davenport class as the average of each interval, and the boundary between two classes is defined as the arithmetic
 * average between the z0 values of each class. A definition of the interval based on the profile of class = f(z0)
 * could lead to different results (especially for classes 3, 4 and 5) since f(z0) is clearly non-linear.
 *
 * References:
 * Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American Meteorological Society 93, no. 12 (2012): 1879-1900.
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable the name of the input ITable where are stored the effectiveTerrainRoughnessHeight values
 * @param id_rsu Unique identifier column name
 * @param effectiveTerrainRoughnessLength the field name corresponding to the RSU effective terrain roughness class due
 * to roughness elements (buildings, trees, etc.) (in the rsuTable)
 * @param prefixName String use as prefix to name the output table
 *
 * @return Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
String effectiveTerrainRoughnessClass(JdbcDataSource datasource, String rsu, String id_rsu,
                                      String effectiveTerrainRoughnessLength, String prefixName) throws Exception {
    try {
        def BASE_NAME = "effective_terrain_roughness_class"

        debug "Executing RSU effective terrain roughness class"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        // Based on the lookup Table of Davenport
        datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName AS SELECT $id_rsu, 
                    CASEWHEN($effectiveTerrainRoughnessLength<0.0 OR $effectiveTerrainRoughnessLength IS NULL, null,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.00035, 1,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.01525, 2,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.065, 3,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.175, 4,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.375, 5,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.75, 6,
                    CASEWHEN($effectiveTerrainRoughnessLength<1.5, 7, 8)))))))) AS $BASE_NAME FROM $rsu""")
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the effective terrain roughness classes at RSU scale", e)
    }
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
 * @param building The name of the input ITable where are stored the buildings, the building contiguity values,
 * the building total facade length values and the building and rsu id
 * @param rsu The name of the input ITable where are stored the rsu geometries and the id_rsu
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
 * @author Erwan Bocher
 */
String extendedFreeFacadeFraction(JdbcDataSource datasource, String building, String rsu, String buContiguityColumn,
                                  String buTotalFacadeLengthColumn, float buffDist = 10, String prefixName) throws Exception {

    // Temporary tables are created
    def extRsuTable = postfix "extRsu"
    def inclBu = postfix "inclBu"
    def fullInclBu = postfix "fullInclBu"
    def notIncBu = postfix "notIncBu"
    try {
        def GEOMETRIC_FIELD = "the_geom"
        def ID_FIELD_RSU = "id_rsu"
        def HEIGHT_WALL = "height_wall"
        def BASE_NAME = "extended_free_facade_fraction"

        debug "Executing RSU free facade fraction (for SVF fast)"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

        datasource.execute("""DROP TABLE IF EXISTS $extRsuTable; CREATE TABLE $extRsuTable AS SELECT  
                    ST_BUFFER($GEOMETRIC_FIELD, $buffDist, 2) AS $GEOMETRIC_FIELD,
                    $ID_FIELD_RSU FROM $rsu;""")

        // The facade area of buildings being entirely included in the RSU buffer is calculated
        datasource.createSpatialIndex(extRsuTable, GEOMETRIC_FIELD)
        datasource.createIndex(extRsuTable, ID_FIELD_RSU)
        datasource.createSpatialIndex(building, GEOMETRIC_FIELD)

        datasource.execute("""DROP TABLE IF EXISTS $inclBu; CREATE TABLE $inclBu AS SELECT 
                    COALESCE(SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$HEIGHT_WALL), 0) AS FAC_AREA,
                    b.$ID_FIELD_RSU FROM $building a, $extRsuTable b WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD and ST_COVERS(b.$GEOMETRIC_FIELD,
                    a.$GEOMETRIC_FIELD) GROUP BY b.$ID_FIELD_RSU;""")

        // All RSU are feeded with default value
        datasource.createIndex(inclBu, ID_FIELD_RSU)
        datasource.createIndex(rsu, ID_FIELD_RSU)

        datasource.execute("""DROP TABLE IF EXISTS $fullInclBu; CREATE TABLE $fullInclBu AS SELECT 
                    COALESCE(a.FAC_AREA, 0) AS FAC_AREA, b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD, st_area(b.$GEOMETRIC_FIELD) as rsu_buff_area 
                    FROM $inclBu a RIGHT JOIN $extRsuTable b ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;""")

        // The facade area of buildings being partially included in the RSU buffer is calculated
        datasource.execute("""DROP TABLE IF EXISTS $notIncBu; CREATE TABLE $notIncBu AS SELECT 
                    COALESCE(SUM(ST_LENGTH(ST_INTERSECTION(ST_TOMULTILINE(a.$GEOMETRIC_FIELD),
                     b.$GEOMETRIC_FIELD))*a.$HEIGHT_WALL), 0) 
                    AS FAC_AREA, b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD FROM $building a, $extRsuTable b 
                    WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD and ST_OVERLAPS(b.$GEOMETRIC_FIELD, a.$GEOMETRIC_FIELD) GROUP BY b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD;""")

        // The facade fraction is calculated
        datasource.createIndex(notIncBu, ID_FIELD_RSU)
        datasource.createIndex(fullInclBu, ID_FIELD_RSU)

        datasource.execute("""DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS 
                    SELECT COALESCE((a.FAC_AREA + b.FAC_AREA) /(a.FAC_AREA + b.FAC_AREA + a.rsu_buff_area),
                     a.FAC_AREA / (a.FAC_AREA  + a.rsu_buff_area))
                    AS $BASE_NAME, 
                    a.$ID_FIELD_RSU, a.$GEOMETRIC_FIELD FROM $fullInclBu a LEFT JOIN $notIncBu b 
                    ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;""")

        // Drop intermediate tables
        datasource.execute("DROP TABLE IF EXISTS $extRsuTable, $inclBu, $fullInclBu, $notIncBu;")
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the extended free facade fractions at RSU scale", e)
    } finally {
        datasource.dropTable(extRsuTable, inclBu, fullInclBu, notIncBu)
    }
}

/**
 * This process computes all spatial relations between a set of input layers : building, road, water,
 * vegetation and impervious
 *
 * A zone table aka RSU or a GRID is used to split the input layers and group using the id of zone.
 *
 * The geometry of the input layers are flattened vertically to extract the smallest common geometry.
 * This method allows to identify all common part between the input layers. e.g a vegetation geometry that overlaps
 * a building, a water geometry that overlaps a road...
 *
 * The relations are encoded in table with the following schema
 *
 * area,low_vegetation, high_vegetation,  water, impervious, road,  building, id, id_rsu
 *
 * area : the area of the smallest common geometry
 * low_vegetation : 1 if it is made of otherwise 0
 * high_vegetation : 1 if it is made of otherwise 0
 * water : 1 if it is made of otherwise 0
 * impervious : 1 if it is made of otherwise 0
 * road : 1 if it is made of otherwise 0
 * rail : 1 if it is made of otherwise 0
 * building : 1 if it is made of otherwise 0
 * id : unique identifier
 * id_rsu = rsu identifier
 *
 * Note that the relations are only computed for the zindex = 0
 *
 * @author Erwan Bocher (CNRS)
 *
 * @return a table that stores all spatial relations
 */
String smallestCommunGeometry(JdbcDataSource datasource, String zone, String id_zone,
                              String building, String road, String water, String vegetation,
                              String impervious, String rail, String prefixName) throws Exception {
    //All table names cannot be null or empty
    if (!building && !road && !water && !vegetation && !impervious) {
        throw new IllegalArgumentException("Cannot compute the smallest commun geometry on null input")
    }
    try {
        def BASE_NAME = "RSU_SMALLEST_COMMUN_GEOMETRY"

        debug "Compute the smallest geometries"

        //To avoid column name duplication
        def ID_COLUMN_NAME = postfix "id"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        if (zone && datasource.hasTable(zone)) {
            datasource.createIndex(zone, id_zone)
            datasource.createSpatialIndex(zone, "the_geom")
            def tablesToMerge = [:]
            tablesToMerge += ["$zone": "select ST_ExteriorRing(the_geom) as the_geom, ${id_zone} from $zone"]
            if (road && datasource.hasTable(road) && !datasource.isEmpty(road)) {
                debug "Preparing table : $road"
                datasource.createSpatialIndex(road, "the_geom")
                //Separate road features according the zindex
                def roadTable_zindex0_buffer = postfix "road_zindex0_buffer"
                def road_tmp = postfix "road_zindex0"

                datasource.execute("""DROP TABLE IF EXISTS $roadTable_zindex0_buffer, $road_tmp;
            CREATE TABLE $roadTable_zindex0_buffer as SELECT ST_CollectionExtract(st_intersection(a.the_geom,b.the_geom),2) AS the_geom, 
            a.WIDTH, b.${id_zone}
            FROM $road as a, $zone AS b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom) and a.ZINDEX=0 ;
            CREATE SPATIAL INDEX IF NOT EXISTS ids_$roadTable_zindex0_buffer ON $roadTable_zindex0_buffer(the_geom);
            CREATE TABLE $road_tmp AS SELECT st_union(st_accum(st_buffer(a.the_geom, WIDTH::DOUBLE PRECISION/2,2)))AS the_geom,
            ${id_zone} FROM
            $roadTable_zindex0_buffer AS a group by ${id_zone} ;
            DROP TABLE IF EXISTS $roadTable_zindex0_buffer;
            """)
                tablesToMerge += ["$road_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $road_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }

            if (rail && datasource.hasTable(rail) && !datasource.isEmpty(rail)) {
                debug "Preparing table : $rail"
                datasource.createSpatialIndex(rail, "the_geom")
                //Separate rail features according the zindex
                def railTable_zindex0_buffer = postfix "rail_zindex0_buffer"
                def rail_tmp = postfix "rail_zindex0"
                datasource.execute("""DROP TABLE IF EXISTS $railTable_zindex0_buffer, $rail_tmp;
            CREATE TABLE $railTable_zindex0_buffer as SELECT ST_CollectionExtract(st_intersection(a.the_geom,b.the_geom),3) AS the_geom, 
            a.WIDTH, b.${id_zone}
            FROM $rail as a ,$zone AS b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom) and a.ZINDEX=0 ;
            CREATE SPATIAL INDEX IF NOT EXISTS ids_$railTable_zindex0_buffer ON $railTable_zindex0_buffer(the_geom);
            CREATE TABLE $rail_tmp AS SELECT st_union(st_accum(st_buffer(a.the_geom, WIDTH::DOUBLE PRECISION/2,2))) AS the_geom,
            ${id_zone} FROM
            $railTable_zindex0_buffer AS a GROUP BY ${id_zone};
            DROP TABLE IF EXISTS $railTable_zindex0_buffer;
            """)
                tablesToMerge += ["$rail_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $rail_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }

            if (vegetation && datasource.hasTable(vegetation) && !datasource.isEmpty(vegetation)) {
                debug "Preparing table : $vegetation"
                datasource.createSpatialIndex(vegetation, "the_geom")
                def low_vegetation_rsu_tmp = postfix "low_vegetation_rsu_zindex0"
                def low_vegetation_tmp = postfix "low_vegetation_zindex0"
                def high_vegetation_tmp = postfix "high_vegetation_zindex0"
                datasource.execute("""DROP TABLE IF EXISTS $low_vegetation_tmp, $low_vegetation_rsu_tmp;
                CREATE TABLE $low_vegetation_tmp as select ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom,  b.${id_zone} FROM 
                    $vegetation AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.height_class='low';
                CREATE TABLE $high_vegetation_tmp as select ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom,  b.${id_zone} FROM 
                    $vegetation AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.height_class='high';
                """)
                tablesToMerge += ["$low_vegetation_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $low_vegetation_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                tablesToMerge += ["$high_vegetation_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $high_vegetation_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }

            if (water && datasource.hasTable(water) && !datasource.isEmpty(water)) {
                debug "Preparing table : $water"
                datasource.createSpatialIndex(water, "the_geom")
                def water_tmp = postfix "water_zindex0"
                datasource.execute("""DROP TABLE IF EXISTS $water_tmp;
                CREATE TABLE $water_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_zone} FROM 
                        $water AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom)""")
                tablesToMerge += ["$water_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $water_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }

            if (impervious && datasource.hasTable(impervious) && !datasource.isEmpty(impervious)) {
                debug "Preparing table : $impervious"
                datasource.createSpatialIndex(impervious, "the_geom")
                def impervious_tmp = postfix "impervious_zindex0"
                datasource.execute("""DROP TABLE IF EXISTS $impervious_tmp;
                CREATE TABLE $impervious_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_zone} FROM 
                        $impervious AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom)""")
                tablesToMerge += ["$impervious_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $impervious_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }

            if (building && datasource.hasTable(building) && !datasource.isEmpty(building)) {
                debug "Preparing table : $building"
                datasource.createSpatialIndex(building, "the_geom")
                def building_tmp = postfix "building_zindex0"
                datasource.execute("""DROP TABLE IF EXISTS $building_tmp;
                CREATE TABLE $building_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_zone} FROM 
                        $building AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.zindex=0""")
                tablesToMerge += ["$building_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $building_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }

            //Merging all tables in one
            debug "Grouping all tables in one..."
            if (!tablesToMerge) {
                error "Any features to compute surface fraction statistics"
                return
            }
            def tmp_tables = postfix "tmp_tables_zindex0"
            datasource.execute("""DROP TABLE if exists $tmp_tables;
            CREATE TABLE $tmp_tables(the_geom GEOMETRY, ${id_zone} integer) AS ${tablesToMerge.values().join(' union ')};
               """)

            //Polygonize the input tables
            debug "Generating " +
                    "minimum polygon areas"
            def tmp_point_polygonize = postfix "tmp_point_polygonize_zindex0"
            datasource.execute("""DROP TABLE IF EXISTS $tmp_point_polygonize;
                CREATE INDEX ON $tmp_tables($id_zone);
                CREATE TABLE $tmp_point_polygonize as  select  EXPLOD_ID as ${ID_COLUMN_NAME}, st_pointonsurface(the_geom) as the_geom ,
                st_area(the_geom) as area , ${id_zone}
                 from st_explode ('(select st_polygonize(st_union(st_force2d(
                st_precisionreducer(st_node(st_accum(a.the_geom)), 3)))) as the_geom, ${id_zone} from $tmp_tables as a group by ${id_zone})')""")

            //Create indexes
            datasource.createSpatialIndex(tmp_point_polygonize, "the_geom")
            datasource.createIndex(tmp_point_polygonize, id_zone)

            def final_polygonize = postfix "final_polygonize_zindex0"
            datasource.execute("""
            DROP TABLE IF EXISTS $final_polygonize;
            CREATE TABLE $final_polygonize as select a.AREA , a.the_geom as the_geom, a.${ID_COLUMN_NAME}, b.${id_zone}
            from $tmp_point_polygonize as a, $zone as b
            where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")

            datasource.createSpatialIndex(final_polygonize, "the_geom")
            datasource.createIndex(final_polygonize, id_zone)

            def finalMerge = []
            def tmpTablesToDrop = []
            tablesToMerge.each { entry ->
                debug "Processing table $entry.key"
                def tmptableName = "tmp_stats_$entry.key"
                tmpTablesToDrop << tmptableName
                if (entry.key.startsWith("high_vegetation")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT b.area,0 as low_vegetation, 1 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 0 as building,0 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                } else if (entry.key.startsWith("low_vegetation")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                 CREATE TABLE $tmptableName AS SELECT b.area,1 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 0 as building,0 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and st_intersects(a.the_geom,b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                } else if (entry.key.startsWith("water")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""CREATE TABLE $tmptableName AS SELECT b.area,0 as low_vegetation, 0 as high_vegetation, 1 as water, 0 as impervious, 0 as road,  0 as building,0 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key} as a,
                $final_polygonize as b  where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                } else if (entry.key.startsWith("road")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                    CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 1 as road, 0 as building,0 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                } else if (entry.key.startsWith("rail")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                    CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 0 as building,1 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                } else if (entry.key.startsWith("impervious")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 1 as impervious, 0 as road, 0 as building,0 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                } else if (entry.key.startsWith("building")) {
                    datasource.createSpatialIndex(entry.key, "the_geom")
                    datasource.createIndex(entry.key, id_zone)
                    datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 1 as building,0 as rail, b.${ID_COLUMN_NAME}, b.${id_zone} from ${entry.key}  as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_zone} =b.${id_zone}""")
                    finalMerge.add("SELECT * FROM $tmptableName")
                }
            }
            if (finalMerge) {
                //Do not drop RSU table
                tablesToMerge.remove("$zone")
                def allInfoTableName = postfix "allInfoTableName"
                datasource.execute("""DROP TABLE IF EXISTS $allInfoTableName, $tmp_point_polygonize, $final_polygonize, $tmp_tables, $outputTableName;
                                      CREATE TABLE $allInfoTableName as ${finalMerge.join(' union all ')};
                                      CREATE INDEX ON $allInfoTableName (${ID_COLUMN_NAME});
                                      CREATE INDEX ON $allInfoTableName (${id_zone});
                                      CREATE TABLE $outputTableName AS SELECT MAX(AREA) AS AREA, MAX(LOW_VEGETATION) AS LOW_VEGETATION,
                                                        MAX(HIGH_VEGETATION) AS HIGH_VEGETATION, MAX(WATER) AS WATER,
                                                        MAX(IMPERVIOUS) AS IMPERVIOUS, MAX(ROAD) AS ROAD, 
                                                        MAX(BUILDING) AS BUILDING, MAX(RAIL) AS RAIL, ${id_zone} FROM $allInfoTableName GROUP BY ${ID_COLUMN_NAME}, ${id_zone};
                                      DROP TABLE IF EXISTS ${tablesToMerge.keySet().join(' , ')}, ${allInfoTableName}, ${tmpTablesToDrop.join(",")}""")
            } else {
                datasource.execute("""DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName(AREA DOUBLE PRECISION, 
                                                LOW_VEGETATION INTEGER,
                                                HIGH_VEGETATION INTEGER,
                                                WATER INTEGER,
                                                IMPERVIOUS INTEGER,
                                                ROAD INTEGER,
                                                BUILDING INTEGER,
                                                RAIL INTEGER,
                                                ${id_zone} INTEGER);""")
            }
        } else {
            throw new SQLException("""Cannot compute the smallest geometries""")
        }
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the smallest geometries", e)
    }
}

/**
 * This process computes all surface fractions from building, road, water,
 * vegetation and impervious layers. It also computes the fractions of layers that
 * overlay each other.
 *
 * It is necessary to calculate the smallestCommunGeometry since its output is needed as input of this process.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param rsuTable The name of the input ITable where are stored the rsu geometries and the id_rsu
 * @param id_rsu Name of the rsuTable column (unique identifier)
 * @param spatialRelationsTable The name of the table that stores all spatial relations (output of smallestCommunGeometry)
 * @param superpositions Map where are stored the overlaying layers as keys and the overlapped
 * layers as values. Note that the priority order for the overlapped layers is taken according to the priority variable
 * name and (default ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]])
 * @param priorities List indicating the priority order to set between layers in order to remove potential double count
 * of overlapped layers (for example a geometry containing water and low_vegetation must be either water
 * or either low_vegetation, not both (default ["water", "building", "high_vegetation", "low_vegetation",
 * "road", "impervious"]
 * @param prefixName String use as prefix to name the output table
 *
 * Note that the relations are only computed for the zindex = 0
 *
 * @author Jérémy Bernard (CNRS)
 *
 * @return a table where are stored all surface fraction informations
 */
String surfaceFractions(JdbcDataSource datasource,
                        String rsu, String id_rsu, String spatialRelationsTable,
                        Map superpositions = ["high_vegetation": ["water", "building", "low_vegetation", "rail", "road", "impervious"]],
                        List priorities = ["water", "building", "high_vegetation", "low_vegetation", "rail", "road", "impervious"],
                        String prefixName) throws Exception {
    def BASE_TABLE_NAME = "RSU_SURFACE_FRACTIONS"
    def LAYERS = ["rail", "road", "water", "high_vegetation", "low_vegetation", "impervious", "building"]
    debug "Executing RSU surface fractions computation"

    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_TABLE_NAME

    // Temporary tables are created
    def withoutUndefined = postfix "without_undefined"
    try {
        // Create the indexes on each of the input tables
        datasource.createIndex(rsu, id_rsu)
        datasource.createIndex(spatialRelationsTable, id_rsu)

        // Need to set priority number for future sorting
        def prioritiesMap = [:]
        def i = 0
        priorities.each { val ->
            prioritiesMap[val] = i
            i++
        }

        def query = """DROP TABLE IF EXISTS $withoutUndefined; CREATE TABLE $withoutUndefined AS SELECT b.${id_rsu} """
        def end_query = """ FROM $spatialRelationsTable AS a RIGHT JOIN $rsu b 
                            ON a.${id_rsu}=b.${id_rsu} GROUP BY b.${id_rsu};"""

        if (superpositions) {
            // Calculates the fraction of overlapped layers according to "superpositionsWithPriorities"
            superpositions.each { key, values ->
                // Calculating the overlaying layer when it has no overlapped layer
                def tempoLayers = LAYERS.minus([key])

                query += ", COALESCE(SUM(CASE WHEN a.$key =1 AND a.${tempoLayers.join(" =0 AND a.")} =0 THEN a.area ELSE 0 END),0)/st_area(b.the_geom) AS ${key}_fraction "

                // Calculate each combination of overlapped layer for the current overlaying layer
                def notOverlappedLayers = priorities.minus(values).minus([key])
                // If an non overlapped layer is prioritized, its number should be 0 for the overlapping to happen
                def nonOverlappedQuery = ""
                def positionOverlapping = prioritiesMap."$key"
                if (notOverlappedLayers) {
                    notOverlappedLayers.each { val ->
                        if (positionOverlapping > prioritiesMap.get(val)) {
                            nonOverlappedQuery += " AND a.$val =0 "
                        }
                    }
                }
                def var2Zero = []
                def prioritiesWithoutOverlapping = priorities.minus(key)
                prioritiesWithoutOverlapping.each { val ->
                    if (values.contains(val)) {
                        def var2ZeroQuery = ""
                        if (var2Zero) {
                            var2ZeroQuery = " AND a." + var2Zero.join("=0 AND a.") + " =0 "
                        }
                        query += ", COALESCE(SUM(CASE WHEN a.$key =1 AND a.$val =1 $var2ZeroQuery $nonOverlappedQuery THEN a.area ELSE 0 END),0)/st_area(b.the_geom) AS ${key}_${val}_fraction "

                    }
                    var2Zero.add(val)
                }
            }

            // Calculates the fraction for each individual layer using the "priorities" table and considering
            // already calculated superpositions
            def varAlreadyUsedQuery = ""
            def var2Zero = []
            def overlappingLayers = superpositions.keySet()
            priorities.each { val ->
                def var2ZeroQuery = ""
                if (var2Zero) {
                    var2ZeroQuery = " AND a." + var2Zero.join("=0 AND a.") + " =0 "
                }
                var2Zero.add(val)
                if (!overlappingLayers.contains(val)) {
                    // Overlapping layers should be set to zero when they arrive after the current layer
                    // in order of priority
                    def nonOverlappedQuery = ""
                    superpositions.each { key, values ->
                        def positionOverlapping = prioritiesMap.get(key)
                        if (values.contains(val) & (positionOverlapping > prioritiesMap.get(val))) {
                            nonOverlappedQuery += " AND a.$key =0 "
                        }
                    }
                    query += ", COALESCE(SUM(CASE WHEN a.$val =1 $var2ZeroQuery $varAlreadyUsedQuery $nonOverlappedQuery THEN a.area ELSE 0 END),0)/st_area(b.the_geom) AS ${val}_fraction "

                }
            }
            datasource query.toString() + end_query.toString()

        } else {
            def var2Zero = []
            priorities.each { val ->
                def var2ZeroQuery = ""
                if (var2Zero) {
                    var2ZeroQuery = " AND a." + var2Zero.join("=0 AND a.") + " = 0 "
                }
                var2Zero.add(val)
                query += ", COALESCE(SUM(CASE WHEN a.$val =1 $var2ZeroQuery THEN a.area ELSE 0 END),0)/st_area(b.the_geom) AS ${val}_fraction "
            }
            datasource query.toString() + end_query.toString()

        }
        // Calculates the fraction of land without defined surface
        def allCols = datasource.getColumnNames(withoutUndefined)
        def allFractionCols = allCols.minus(id_rsu.toUpperCase())
        datasource.execute(""" DROP TABLE IF EXISTS $outputTableName;
                           CREATE TABLE $outputTableName
                                AS SELECT *, 1-(${allFractionCols.join("+")}) AS UNDEFINED_FRACTION
                                FROM $withoutUndefined""")

        // Drop intermediate tables
        datasource.execute("DROP TABLE IF EXISTS $withoutUndefined;")

        //Cache the table name to re-use it
        cacheTableName(BASE_TABLE_NAME, outputTableName)
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute surface fractions", e)
    } finally {
        datasource.dropTable(withoutUndefined)
    }
}

/**
 * Process used to compute the sum of all building facades (free facades and roofs) included in a
 * Reference Spatial Unit (RSU) divided by the RSU area. The calculation is actually simply the sum
 * of two other indicators (building fraction and free external facade density)...
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param facadeDensityTable The name of the input ITable where are stored the facade density and the rsu id
 * @param buildingFractionTable The name of the input ITable where are stored the building fraction and the rsu id
 * @param facDensityColumn The name of the column where are stored the facade density values (within the
 * 'facadeDensityTable' Table)
 * @param buFractionColumn The name of the column where are stored the building fraction values (within the
 * 'buildingFractionTable' Table)
 * @param idRsu the name of the id of the RSU table
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String buildingSurfaceDensity(JdbcDataSource datasource, String facadeDensityTable,
                              String buildingFractionTable, String facDensityColumn, String buFractionColumn,
                              String idRsu, String prefixName) throws Exception {
    try {
        def BASE_NAME = "building_surface_fraction"

        debug "Executing building surface density"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, BASE_NAME

        // Sum free facade density and building fraction...
        datasource.createIndex(facadeDensityTable, idRsu)
        datasource.createIndex(buildingFractionTable, idRsu)
        datasource.execute("""
                DROP TABLE IF EXISTS $outputTableName;
                CREATE TABLE $outputTableName
                    AS SELECT   a.$idRsu, 
                                a.$buFractionColumn + b.$facDensityColumn AS BUILDING_SURFACE_DENSITY
                    FROM $buildingFractionTable AS a LEFT JOIN $facadeDensityTable AS b
                    ON a.$idRsu = b.$idRsu""")

        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute building surface density at RSU scale", e)
    }
}

/**
 * Script to compute the distribution of (only) horizontal roof area fraction for each layer of the canopy. If the height
 * roof and height wall differ, take the average value at building height. Note that this process first cut the buildings
 * according to RSU in order to calculate the exact distribution.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param building the name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
 * @param rsu the name of the input ITable where are stored the RSU
 * @param idRsu the name of the id of the RSU table
 * @param prefixName String use as prefix to name the output table
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default
 * [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50])
 * @param cutBuilding set to true if the building layer must be cuted by the rsu layer
 *
 * @return Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
String roofFractionDistributionExact(JdbcDataSource datasource, String rsu, String building,
                                     String idRsu, List listLayersBottom = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
                                     boolean cutBuilding = true, String prefixName) throws Exception {
    def GEOMETRIC_COLUMN_RSU = "the_geom"
    def GEOMETRIC_COLUMN_BU = "the_geom"
    def ID_COLUMN_BU = "id_build"
    def HEIGHT_WALL = "height_wall"
    def HEIGHT_ROOF = "height_roof"
    def BUILDING_AREA = "building_area"
    def BUILD_HEIGHT = "BUILD_HEIGHT"
    def BASE_NAME = "roof_fraction_distribution"

    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_NAME

    debug "Executing RSU roof area distribution (and optionally roof density)"

    // To avoid overwriting the output files of this step, a unique identifier is created
    // Temporary table names
    def buildInter = building
    def rsuBuildingArea = postfix "rsu_building_area"
    def buildFracH = postfix "roof_frac_H"
    def bufferTable = postfix "buffer_table"

    if (cutBuilding) {
        buildInter = postfix "build_inter"
        // 1. Create the intersection between buildings and RSU polygons
        datasource.createIndex(building, ID_COLUMN_BU)
        datasource.createIndex(rsu, idRsu)
        datasource.execute("""
                DROP TABLE IF EXISTS $buildInter;
                CREATE TABLE $buildInter
                    AS SELECT   a.$ID_COLUMN_BU, a.$idRsu,
                                ST_INTERSECTION(a.$GEOMETRIC_COLUMN_BU, b.$GEOMETRIC_COLUMN_RSU) AS $GEOMETRIC_COLUMN_BU,
                                (a.$HEIGHT_WALL + a.$HEIGHT_ROOF) / 2 AS $BUILD_HEIGHT
                    FROM $building AS a LEFT JOIN $rsu AS b
                    ON a.$idRsu = b.$idRsu""")
    }

    // 2. Calculate the total building roof area within each RSU
    datasource.createIndex(buildInter, idRsu)
    datasource.execute("""
                DROP TABLE IF EXISTS $rsuBuildingArea;
                CREATE TABLE $rsuBuildingArea
                    AS SELECT   $idRsu, SUM(ST_AREA($GEOMETRIC_COLUMN_BU)) AS $BUILDING_AREA
                    FROM $buildInter
                    GROUP BY $idRsu""")

    // 3. Calculate the fraction of roof for each level of the canopy (defined by 'listLayersBottom') except the last
    datasource.createIndex(buildInter, BUILD_HEIGHT)
    datasource.createIndex(rsuBuildingArea, idRsu)
    def tab_H = [:]
    def indicToJoin = [:]
    for (i in 1..(listLayersBottom.size() - 1)) {
        def layer_top = listLayersBottom[i]
        def layer_bottom = listLayersBottom[i - 1]
        def indicNameH = getDistribIndicName(BASE_NAME, 'H', layer_bottom, layer_top).toString()
        tab_H[i - 1] = "${buildFracH}_$layer_bottom"
        datasource.execute("""
                DROP TABLE IF EXISTS $bufferTable;
                CREATE TABLE $bufferTable
                    AS SELECT   a.$idRsu, 
                                CASE WHEN a.$BUILDING_AREA = 0
                                THEN 0
                                ELSE SUM(ST_AREA(b.$GEOMETRIC_COLUMN_BU)) / a.$BUILDING_AREA
                                END AS $indicNameH
                    FROM $rsuBuildingArea AS a LEFT JOIN $buildInter AS b
                    ON a.$idRsu = b.$idRsu
                    WHERE b.$BUILD_HEIGHT >= $layer_bottom AND b.$BUILD_HEIGHT < $layer_top
                    GROUP BY b.$idRsu""")
        // Fill missing values with 0
        datasource.createIndex(bufferTable, idRsu)
        datasource.execute("""
                    DROP TABLE IF EXISTS ${tab_H[i - 1]};
                    CREATE TABLE ${tab_H[i - 1]}
                    AS SELECT   a.$idRsu, 
                                COALESCE(b.$indicNameH, 0) AS $indicNameH
                    FROM $rsu AS a LEFT JOIN $bufferTable AS b
                    ON a.$idRsu = b.$idRsu""")
        // Declare this layer to the layer to join at the end
        indicToJoin.put(tab_H[i - 1], idRsu)
    }

    // 4. Calculate the fraction of roof for the last level of the canopy
    def layer_bottom = listLayersBottom[listLayersBottom.size() - 1]
    def indicNameH = getDistribIndicName(BASE_NAME, 'H', layer_bottom, null).toString()
    tab_H[listLayersBottom.size() - 1] = "${buildFracH}_$layer_bottom"
    datasource.execute("""
            DROP TABLE IF EXISTS $bufferTable;
            CREATE TABLE $bufferTable
                AS SELECT   a.$idRsu, 
                            CASE WHEN a.$BUILDING_AREA = 0
                            THEN 0
                            ELSE SUM(ST_AREA(b.$GEOMETRIC_COLUMN_BU)) / a.$BUILDING_AREA
                            END AS $indicNameH
                FROM $rsuBuildingArea AS a LEFT JOIN $buildInter AS b
                ON a.$idRsu = b.$idRsu
                WHERE b.$BUILD_HEIGHT >= $layer_bottom
                GROUP BY b.$idRsu""")
    // Fill missing values with 0
    datasource.createIndex(bufferTable, idRsu)
    datasource.execute("""
                    DROP TABLE IF EXISTS ${tab_H[listLayersBottom.size() - 1]};
                    CREATE TABLE ${tab_H[listLayersBottom.size() - 1]}
                    AS SELECT   a.$idRsu, 
                                COALESCE(b.$indicNameH, 0) AS $indicNameH
                    FROM $rsu AS a LEFT JOIN $bufferTable AS b
                    ON a.$idRsu = b.$idRsu""")
    // Declare this layer to the layer to join at the end
    indicToJoin.put(tab_H[listLayersBottom.size() - 1], idRsu)

    // 5. Join all layers in one table
    def joinGrids = Geoindicators.DataUtils.joinTables(datasource, indicToJoin, outputTableName)
    if (!joinGrids) {
        info "Cannot merge all indicators in RSU table $outputTableName."
        return
    }

    datasource.execute("""DROP TABLE IF EXISTS $buildInter, $rsuBuildingArea, $bufferTable,
                    ${tab_H.values().join(",")}""")

    return outputTableName
}


/**
 * Process used to compute the frontal area index for different combinations of direction / canopy levels. The frontal
 * area index for a given set of direction / level is defined as the projected area of facade facing the direction
 * of analysis divided by the horizontal surface of analysis and divided by the height of the layer
 *
 * WARNING: WITH THE CURRENT METHOD, IF A BUILDING FACADE FOLLOW THE LINE SEPARATING TWO RSU, IT WILL BE COUNTED FOR BOTH RSU
 * (EVEN THOUGH THIS SITUATION IS PROBABLY ALMOST IMPOSSIBLE WITH REGULAR SQUARE GRID)...
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input Table and in which
 * the resulting database will be stored
 * @param building The name of the input ITable where are stored the buildings geometry, the building height,
 * the building and the rsu id
 * @param rsu The name of the input ITable where are stored the rsu geometries and the 'idRsu'
 * @param idRsu the name of the id of the RSU table
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default
 * [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50])
 * @param numberOfDirection the number of directions used for the calculation - according to the method used it should
 * be divisor of 360 AND a multiple of 2 (default 12)
 * @param distributionAsIndex set the value to false to avoid normalizing the distribution by the area of the input rsu
 * and the range of the bottom layers
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 * TODO :  https://github.com/orbisgis/geoclimate/issues/848
 */
String frontalAreaIndexDistribution(JdbcDataSource datasource, String building, String rsu,
                                    String idRsu, List listLayersBottom = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
                                    int numberOfDirection = 12, boolean distributionAsIndex = true, String prefixName) throws Exception {
    def GEOMETRIC_FIELD_RSU = "the_geom"
    def GEOMETRIC_FIELD_BU = "the_geom"
    def ID_FIELD_BU = "id_build"
    def HEIGHT_WALL = "height_wall"
    def BASE_NAME = "frontal_area_index_distribution"

    debug "Executing RSU frontal area index distribution"

    // The name of the outputTableName is constructed
    def outputTableName = prefix prefixName, BASE_NAME

    boolean  buildingIsEmpty =  datasource.isEmpty(building)
    if (360 % numberOfDirection == 0 && numberOfDirection % 2 == 0) {

        // Temporary table names
        def buildLine = postfix "buildLine"
        def allLinesRsu = postfix "all_lines_rsu"
        def buildFracH = postfix "build_frac_H"
        def bufferTable = postfix "buffer_table"

        // Consider facades as touching each other within a snap tolerance
        def snap_tolerance = 0.01

        // 1. Convert the building polygons into lines and create the intersection with RSU polygons
        datasource.createIndex(building, idRsu)
        datasource.createIndex(rsu, idRsu)
        datasource.execute("""
                    DROP TABLE IF EXISTS $buildLine;
                    CREATE TABLE $buildLine
                        AS SELECT   a.$ID_FIELD_BU, a.$idRsu,
                                    ST_CollectionExtract(ST_INTERSECTION(ST_TOMULTILINE(a.$GEOMETRIC_FIELD_BU), b.$GEOMETRIC_FIELD_RSU),2) AS $GEOMETRIC_FIELD_BU,
                                    a.$HEIGHT_WALL
                        FROM $building AS a LEFT JOIN $rsu AS b
                        ON a.$idRsu = b.$idRsu""")

        // 2. Keep only intersected facades within a given distance and calculate their length, height and azimuth
        datasource.createSpatialIndex(buildLine, GEOMETRIC_FIELD_BU)
        datasource.createIndex(buildLine, idRsu)
        datasource.createIndex(buildLine, ID_FIELD_BU)
        datasource.execute("""
                    DROP TABLE IF EXISTS $allLinesRsu;
                    CREATE TABLE $allLinesRsu 
                        AS SELECT   -ST_LENGTH($GEOMETRIC_FIELD_BU) AS LENGTH,
                                    ST_AZIMUTH(ST_STARTPOINT($GEOMETRIC_FIELD_BU), ST_ENDPOINT($GEOMETRIC_FIELD_BU)) AS AZIMUTH,
                                    $idRsu,
                                    $HEIGHT_WALL,
                                    $ID_FIELD_BU
                        FROM ST_EXPLODE('(SELECT  ST_TOMULTISEGMENTS(ST_INTERSECTION(a.$GEOMETRIC_FIELD_BU, 
                                                                                       ST_SNAP(b.$GEOMETRIC_FIELD_BU, 
                                                                                               a.$GEOMETRIC_FIELD_BU,
                                                                                               $snap_tolerance))) AS $GEOMETRIC_FIELD_BU,
                                                    LEAST(a.$HEIGHT_WALL, b.$HEIGHT_WALL) AS $HEIGHT_WALL,
                                                    a.$idRsu, a.$ID_FIELD_BU
                                            FROM    $buildLine AS a LEFT JOIN $buildLine AS b
                                                    ON a.$idRsu = b.$idRsu
                                            WHERE       a.$GEOMETRIC_FIELD_BU && b.$GEOMETRIC_FIELD_BU AND ST_INTERSECTS(a.$GEOMETRIC_FIELD_BU, 
                                                        ST_SNAP(b.$GEOMETRIC_FIELD_BU, a.$GEOMETRIC_FIELD_BU, $snap_tolerance)) AND
                                                        a.$ID_FIELD_BU <> b.$ID_FIELD_BU)')
                        WHERE ST_DIMENSION($GEOMETRIC_FIELD_BU) = 1
                        UNION ALL
                        SELECT  ST_LENGTH($GEOMETRIC_FIELD_BU) AS LENGTH,
                                ST_AZIMUTH(ST_STARTPOINT($GEOMETRIC_FIELD_BU), ST_ENDPOINT($GEOMETRIC_FIELD_BU)) AS AZIMUTH,
                                $idRsu,
                                $HEIGHT_WALL,
                                $ID_FIELD_BU
                        FROM ST_EXPLODE('(SELECT    ST_TOMULTISEGMENTS($GEOMETRIC_FIELD_BU) AS $GEOMETRIC_FIELD_BU,
                                                    $HEIGHT_WALL,
                                                    $idRsu, $ID_FIELD_BU
                                          FROM $buildLine)')
                        WHERE ST_LENGTH($GEOMETRIC_FIELD_BU) > 0;""")

        // 3. Make the calculations for all directions of each level except the highest one
        def dirQueryVertFrac = [:]
        def dirQueryDiv = [:]
        def angleRangeRad = 2 * Math.PI / numberOfDirection
        def angleRangeDeg = 360 / numberOfDirection
        def tab_H = [:]
        def indicToJoin = [:]
        datasource.createIndex(rsu, idRsu)
        for (i in 1..(listLayersBottom.size() - 1)) {
            def layer_top = listLayersBottom[i]
            def layer_bottom = listLayersBottom[i - 1]
            def deltaH = layer_top - layer_bottom
            tab_H[i - 1] = "${buildFracH}_$layer_bottom"

            // Define queries and indic names
            def dirList = [:]
            (0..numberOfDirection - 1).each { dirList[it] = (it + 0.5) * angleRangeRad }
            dirList.each { k, v ->
                // Indicator name
                def indicName = "FRONTAL_AREA_INDEX_H${layer_bottom}_${layer_top}_D${k * angleRangeDeg}_${(k + 1) * angleRangeDeg}"
                // Define query to sum the projected facade for buildings and shared facades
                if (distributionAsIndex) {
                    dirQueryVertFrac[k] = """
                                                CASE WHEN $v > AZIMUTH AND $v-AZIMUTH < PI()
                                                    THEN    CASE WHEN $HEIGHT_WALL >= $layer_top
                                                                THEN LENGTH*SIN($v-AZIMUTH)
                                                                ELSE LENGTH*SIN($v-AZIMUTH)*($HEIGHT_WALL-$layer_bottom)/$deltaH
                                                                END
                                                    ELSE    CASE WHEN $v - AZIMUTH < -PI()
                                                            THEN    CASE WHEN $HEIGHT_WALL >= $layer_top
                                                                    THEN LENGTH*SIN($v+2*PI()-AZIMUTH)
                                                                    ELSE LENGTH*SIN($v+2*PI()-AZIMUTH)*($HEIGHT_WALL-$layer_bottom)/$deltaH
                                                                    END
                                                            ELSE 0
                                                            END
                                                    END AS $indicName"""
                    dirQueryDiv[k] = """COALESCE(SUM(b.$indicName)/ST_AREA(a.$GEOMETRIC_FIELD_RSU), 0) AS $indicName"""
                } else {
                    dirQueryVertFrac[k] = """
                                                CASE WHEN $v > AZIMUTH AND $v-AZIMUTH < PI()
                                                    THEN    CASE WHEN $HEIGHT_WALL >= $layer_top
                                                                THEN LENGTH*SIN($v-AZIMUTH)
                                                                ELSE LENGTH*SIN($v-AZIMUTH)*($HEIGHT_WALL-$layer_bottom)
                                                                END
                                                    ELSE    CASE WHEN $v - AZIMUTH < -PI()
                                                            THEN    CASE WHEN $HEIGHT_WALL >= $layer_top
                                                                    THEN LENGTH*SIN($v+2*PI()-AZIMUTH)
                                                                    ELSE LENGTH*SIN($v+2*PI()-AZIMUTH)*($HEIGHT_WALL-$layer_bottom)
                                                                    END
                                                            ELSE 0
                                                            END
                                                    END AS $indicName"""
                    dirQueryDiv[k] = """COALESCE(SUM(b.$indicName), 0) AS $indicName"""
                }
            }
            // Calculates projected surfaces for buildings and shared facades
            datasource.execute("""
                        DROP TABLE IF EXISTS $bufferTable;
                        CREATE TABLE $bufferTable
                            AS SELECT   $idRsu, 
                                        ${dirQueryVertFrac.values().join(",")}
                            FROM $allLinesRsu
                            WHERE $HEIGHT_WALL > $layer_bottom""")
            // Fill missing values with 0
            datasource.createIndex(bufferTable, idRsu)
            datasource.execute("""
                        DROP TABLE IF EXISTS ${tab_H[i - 1]};
                        CREATE TABLE ${tab_H[i - 1]}
                            AS SELECT   a.$idRsu, 
                                        ${dirQueryDiv.values().join(",")}
                            FROM $rsu AS a LEFT JOIN $bufferTable AS b
                            ON a.$idRsu = b.$idRsu
                            GROUP BY a.$idRsu""")
            // Declare this layer to the layer to join at the end
            indicToJoin.put(tab_H[i - 1], idRsu)
        }

        // 4. Make the calculations for the last level
        def layer_bottom = listLayersBottom[listLayersBottom.size() - 1]
        // Get the maximum building height
        def layer_top
        if(buildingIsEmpty){
             layer_top =1
        }else {
            datasource.firstRow("SELECT CAST(MAX($HEIGHT_WALL) AS INTEGER) +1 AS MAXH FROM $building").MAXH
        }
        def deltaH = layer_top - layer_bottom
        tab_H[listLayersBottom.size() - 1] = "${buildFracH}_$layer_bottom"

        // Define queries and indic names
        def dirList = [:]
        (0..numberOfDirection - 1).each { dirList[it] = (it + 0.5) * angleRangeRad }
        dirList.each { k, v ->
            // Indicator name
            def indicName = "FRONTAL_AREA_INDEX_H${layer_bottom}_${layer_top}_D${k * angleRangeDeg}_${(k + 1) * angleRangeDeg}"
            if (!distributionAsIndex) {
                indicName = "FRONTAL_AREA_INDEX_H${layer_bottom}_D${k * angleRangeDeg}_${(k + 1) * angleRangeDeg}"
            }
            // Define query to calculate the vertical fraction of projected facade for buildings and shared facades
            dirQueryVertFrac[k] = """
                                            CASE WHEN $v-AZIMUTH > 0 AND $v-AZIMUTH < PI()
                                                THEN LENGTH*SIN($v-AZIMUTH)*($HEIGHT_WALL-$layer_bottom)/$deltaH
                                                ELSE    CASE WHEN $v - AZIMUTH < -PI()
                                                        THEN LENGTH*SIN($v+2*PI()-AZIMUTH)*($HEIGHT_WALL-$layer_bottom)/$deltaH
                                                        ELSE 0
                                                        END
                                            END AS $indicName"""
            dirQueryDiv[k] = """COALESCE(SUM(b.$indicName)/ST_AREA(a.$GEOMETRIC_FIELD_RSU), 0) AS $indicName"""
        }
        // Calculates projected surfaces for buildings and shared facades
        datasource.execute("""
                        DROP TABLE IF EXISTS $bufferTable;
                        CREATE TABLE $bufferTable
                            AS SELECT   $idRsu, 
                                        ${dirQueryVertFrac.values().join(",")}
                            FROM $allLinesRsu
                            WHERE $HEIGHT_WALL > $layer_bottom""")
        // Fill missing values with 0
        datasource.createIndex(bufferTable, idRsu)
        datasource.execute("""
                        DROP TABLE IF EXISTS ${tab_H[listLayersBottom.size() - 1]};
                        CREATE TABLE ${tab_H[listLayersBottom.size() - 1]}
                            AS SELECT   a.$idRsu, 
                                        ${dirQueryDiv.values().join(",")}
                            FROM $rsu AS a LEFT JOIN $bufferTable AS b
                            ON a.$idRsu = b.$idRsu
                            GROUP BY a.$idRsu""")
        // Declare this layer to the layer to join at the end
        indicToJoin.put(tab_H[listLayersBottom.size() - 1], idRsu)

        // 5. Join all layers in one table
        def joinGrids = Geoindicators.DataUtils.joinTables(datasource, indicToJoin, outputTableName)
        if (!joinGrids) {
            info "Cannot merge all indicators in RSU table $outputTableName."
            return
        }

        // The temporary tables are deleted
        datasource.execute("""DROP TABLE IF EXISTS $buildLine, $allLinesRsu,
                       $bufferTable, ${tab_H.values().join(",")}""")
    }

    return outputTableName
}

/**
 * Disaggregate a set of population values to the rsu units
 * Update the input rsu table to add new population columns
 * @param rsu the building table
 * @param population the spatial unit that contains the population to distribute
 * @param populationColumns the list of the columns to disaggregate
 * @return the input RSU table with the new population columns
 *
 * @author Erwan Bocher, CNRS
 */
String rsuPopulation(JdbcDataSource datasource, String rsu, String population, List populationColumns = []) throws Exception {
    def BASE_NAME = "rsu_with_population"
    def ID_RSU = "id_rsu"
    def ID_POP = "id_pop"

    debug "Computing rsu population"

    // The name of the outputTableName is constructed
    def outputTableName = postfix BASE_NAME

    //Indexing table
    datasource.createSpatialIndex(rsu, "the_geom")
    datasource.createSpatialIndex(population, "the_geom")
    def popColumns = []
    def sum_popColumns = []
    if (populationColumns) {
        datasource.getColumnNames(population).each { col ->
            if (!["the_geom", "id_pop"].contains(col.toLowerCase()
            ) && populationColumns.contains(col.toLowerCase())) {
                popColumns << "b.$col"
                sum_popColumns << "sum((a.area_rsu * $col)/b.sum_area_rsu) as $col"
            }
        }
    } else {
        warn "Please set a list one column that contain population data to be disaggregated"
        return
    }

    //Filtering the rsu to get only the geometries that intersect the population table
    def inputRsuTableName_pop = postfix rsu
    datasource.execute("""
                drop table if exists $inputRsuTableName_pop;
                CREATE TABLE $inputRsuTableName_pop AS SELECT (ST_AREA(ST_INTERSECTION(a.the_geom, st_force2D(b.the_geom))))  as area_rsu, a.$ID_RSU, 
                b.id_pop, ${popColumns.join(",")} from
                $rsu as a, $population as b where a.the_geom && b.the_geom and
                st_intersects(a.the_geom, b.the_geom);
                create index on $inputRsuTableName_pop ($ID_RSU);
                create index on $inputRsuTableName_pop ($ID_POP);
            """)

    def inputRsuTableName_pop_sum = postfix "rsu_pop_sum"
    def inputRsuTableName_area_sum = postfix "rsu_area_sum"
    //Aggregate population values
    datasource.execute("""drop table if exists $inputRsuTableName_pop_sum, $inputRsuTableName_area_sum;
            create table $inputRsuTableName_area_sum as select id_pop, sum(area_rsu) as sum_area_rsu
            from $inputRsuTableName_pop group by $ID_POP;
            create index on $inputRsuTableName_area_sum($ID_POP);
            create table $inputRsuTableName_pop_sum 
            as select a.$ID_RSU, ${sum_popColumns.join(",")} 
            from $inputRsuTableName_pop as a, $inputRsuTableName_area_sum as b where a.$ID_POP=b.$ID_POP group by $ID_RSU;
            CREATE INDEX ON $inputRsuTableName_pop_sum ($ID_RSU);
            DROP TABLE IF EXISTS $outputTableName;
            CREATE TABLE $outputTableName AS SELECT a.*, ${popColumns.join(",")} from $rsu a  
            LEFT JOIN $inputRsuTableName_pop_sum  b on a.$ID_RSU=b.$ID_RSU;
            drop table if exists $inputRsuTableName_pop,$inputRsuTableName_pop_sum, $inputRsuTableName_area_sum ;""")

    return outputTableName
}

/**
 * This process is used to compute a ground layer.
 *
 * All GIS layers prepared by GeoClimate can be set as input : building, road, water,
 * vegetation and impervious
 *
 * The geometry of the input layers are flattened vertically to extract the smallest common geometry.
 * This method allows to identify all common part between the input layers. e.g a vegetation geometry that overlaps
 * a building, a water geometry that overlaps a road...
 *
 * A zones table must be set to split the input layers in small units and perform a fast chain.
 *
 * The result produces a geometry table with the following schema
 *
 * area,low_vegetation, high_vegetation,  water, impervious, road,  building, id_ground
 *
 * area : the area of the smallest common geometry
 * low_vegetation : any type values stored in vegetation table, null otherwise
 * high_vegetation : any type values stored in vegetation table, null otherwise
 * water : any type values stored in vegetation table, null otherwise
 * impervious : any type values stored in vegetation table, null otherwise
 * road : any type values stored in vegetation table, null otherwise
 * building : any type values stored in vegetation table, null otherwise
 * id_ground= unique identifier
 *
 * Note that the relations are only computed for the zindex = 0
 *
 * @author Erwan Bocher (CNRS)
 *
 * @return a table that stores all spatial relations and the smallest common geometry
 */
String groundLayer(JdbcDataSource datasource, String zone, String id_zone,
                   String building, String road, String water, String vegetation,
                   String impervious, List priorities = ["building", "road", "water", "high_vegetation", "low_vegetation", "impervious"]) throws Exception {

    if (!id_zone) {
        error "The id_zone identifier cannot be null or empty"
        return
    }
    if (!priorities) {
        error """Please an order to process the layers using the following array \n 
                ["building", "road", "water", "high_vegetation", "low_vegetation", "impervious"]"""
        return
    }
    def BASE_NAME = "GROUND_GEOMETRY"

    debug "Compute the smallest geometries"

    //To avoid column name duplication
    def ID_COLUMN_NAME = "ID_GROUND"

    // The name of the outputTableName is constructed
    def outputTableName = postfix BASE_NAME

    if (zone && datasource.hasTable(zone)) {
        datasource.createIndex(zone, id_zone)
        datasource.createSpatialIndex(zone, "the_geom")
        def tablesToMerge = [:]
        tablesToMerge += ["$zone": "select ST_ExteriorRing(the_geom) as the_geom, ${id_zone} from $zone"]
        if (road && datasource.hasTable(road) && priorities.contains("road")) {
            debug "Preparing table : $road"
            datasource.createSpatialIndex(road, "the_geom")
            //Separate road features according the zindex
            def roadTable_zindex0_buffer = postfix "road_zindex0_buffer"
            def road_tmp = postfix "road_zindex0"
            datasource.execute("""DROP TABLE IF EXISTS $roadTable_zindex0_buffer, $road_tmp;
            CREATE TABLE $roadTable_zindex0_buffer as SELECT st_buffer(the_geom, WIDTH::DOUBLE PRECISION/2, 2)
            AS the_geom, surface as type
            FROM $road  where ZINDEX=0 ;
            CREATE SPATIAL INDEX IF NOT EXISTS ids_$roadTable_zindex0_buffer ON $roadTable_zindex0_buffer(the_geom);
            CREATE TABLE $road_tmp AS SELECT ST_CollectionExtract(st_intersection(st_union(st_accum(a.the_geom)),b.the_geom),3) AS the_geom, b.${id_zone}, a.type FROM
            $roadTable_zindex0_buffer AS a, $zone AS b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom) GROUP BY b.${id_zone}, a.type;
            DROP TABLE IF EXISTS $roadTable_zindex0_buffer;
            """)
            tablesToMerge += ["$road_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $road_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
        }

        if (vegetation && datasource.hasTable(vegetation)) {
            debug "Preparing table : $vegetation"
            datasource.createSpatialIndex(vegetation, "the_geom")
            def low_vegetation_tmp = postfix "low_vegetation_zindex0"
            def high_vegetation_tmp = postfix "high_vegetation_zindex0"
            if (priorities.contains("low_vegetation")) {
                datasource.execute("""DROP TABLE IF EXISTS $low_vegetation_tmp; 
                CREATE TABLE $low_vegetation_tmp as select ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom,  b.${id_zone}, a.type FROM 
                    $vegetation AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.height_class='low'; """)
                tablesToMerge += ["$low_vegetation_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $low_vegetation_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]

            }
            if (priorities.contains("high_vegetation")) {
                datasource.execute("""DROP TABLE IF EXISTS  $high_vegetation_tmp;
                CREATE TABLE $high_vegetation_tmp as select ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom,  b.${id_zone}, a.type FROM 
                    $vegetation AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.height_class='high';
                """)
                tablesToMerge += ["$high_vegetation_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $high_vegetation_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
            }
        }

        if (water && datasource.hasTable(water) && priorities.contains("water")) {
            debug "Preparing table : $water"
            datasource.createSpatialIndex(water, "the_geom")
            def water_tmp = postfix "water_zindex0"
            datasource.execute("""DROP TABLE IF EXISTS $water_tmp;
                CREATE TABLE $water_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_zone}, 'water' as type FROM 
                        $water AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom)""")
            tablesToMerge += ["$water_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $water_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
        }

        if (impervious && datasource.hasTable(impervious) && priorities.contains("impervious")) {
            debug "Preparing table : $impervious"
            datasource.createSpatialIndex(impervious, "the_geom")
            def impervious_tmp = postfix "impervious_zindex0"
            datasource.execute("""DROP TABLE IF EXISTS $impervious_tmp;
                CREATE TABLE $impervious_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_zone},  'impervious' as type FROM 
                        $impervious AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom)""")
            tablesToMerge += ["$impervious_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $impervious_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
        }

        if (building && datasource.hasTable(building) && priorities.contains("building")) {
            debug "Preparing table : $building"
            datasource.createSpatialIndex(building, "the_geom")
            def building_tmp = postfix "building_zindex0"
            datasource.execute("""DROP TABLE IF EXISTS $building_tmp;
                CREATE TABLE $building_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_zone}, a.type FROM 
                        $building AS a, $zone AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.zindex=0""")
            tablesToMerge += ["$building_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_zone} from $building_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
        }

        //Merging all tables in one
        debug "Grouping all tables in one..."
        if (!tablesToMerge) {
            error "Any features to compute surface fraction statistics"
            return
        }
        def tmp_tables = postfix "tmp_tables_zindex0"
        datasource.execute("""DROP TABLE if exists $tmp_tables;
            CREATE TABLE $tmp_tables(the_geom GEOMETRY, ${id_zone} integer) AS ${tablesToMerge.values().join(' union ')};
               """)

        //Polygonize the input tables
        debug "Generating " +
                "minimum polygon areas"
        def final_polygonize = postfix "tmp_point_polygonize_zindex0"
        datasource.execute("""DROP TABLE IF EXISTS $final_polygonize;
                CREATE INDEX ON $tmp_tables($id_zone);
                CREATE TABLE $final_polygonize as  select  CAST((row_number() over()) as Integer) as ${ID_COLUMN_NAME}, the_geom ,
                st_area(the_geom) as area 
                 from st_explode ('(select st_polygonize(st_union(st_force2d(
                st_precisionreducer(st_node(st_accum(a.the_geom)), 3)))) as the_geom from $tmp_tables as a group by ${id_zone})')""")

        //Create indexes
        datasource.createSpatialIndex(final_polygonize, "the_geom")
        datasource.createIndex(final_polygonize, ID_COLUMN_NAME)

        def finalMerge = []
        def tmpTablesToDrop = []
        tablesToMerge.each { entry ->
            debug "Processing table $entry.key"
            def tmptableName = "tmp_stats_$entry.key"
            tmpTablesToDrop << tmptableName
            if (entry.key.startsWith("high_vegetation")) {
                datasource.createSpatialIndex(entry.key, "the_geom")
                datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT st_area(a.the_geom) as area,'high_vegetation' as layer, a.type, ${priorities.findIndexOf { it == "high_vegetation" }} as priority, b.${ID_COLUMN_NAME} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, st_pointonsurface(b.the_geom))""")
                finalMerge.add("SELECT * FROM $tmptableName")
            } else if (entry.key.startsWith("low_vegetation")) {
                datasource.createSpatialIndex(entry.key, "the_geom")
                datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                 CREATE TABLE $tmptableName AS SELECT st_area(a.the_geom) as area,'low_vegetation' as layer, a.type, ${priorities.findIndexOf { it == "low_vegetation" }} as priority, b.${ID_COLUMN_NAME} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and st_intersects(a.the_geom,st_pointonsurface(b.the_geom))""")
                finalMerge.add("SELECT * FROM $tmptableName")
            } else if (entry.key.startsWith("water")) {
                datasource.createSpatialIndex(entry.key, "the_geom")
                datasource.execute("""CREATE TABLE $tmptableName AS SELECT st_area(a.the_geom) as area,'water' as layer, a.type,${priorities.findIndexOf { it == "water" }} as priority, b.${ID_COLUMN_NAME} from ${entry.key} as a,
                $final_polygonize as b  where a.the_geom && b.the_geom and st_intersects(a.the_geom, st_pointonsurface(b.the_geom))""")
                finalMerge.add("SELECT * FROM $tmptableName")
            } else if (entry.key.startsWith("road")) {
                datasource.createSpatialIndex(entry.key, "the_geom")
                datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                    CREATE TABLE $tmptableName AS SELECT st_area(a.the_geom) as area, 'road' as layer, a.type,${priorities.findIndexOf { it == "road" }} as priority, b.${ID_COLUMN_NAME} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, st_pointonsurface(b.the_geom))""")
                finalMerge.add("SELECT * FROM $tmptableName")
            } else if (entry.key.startsWith("impervious")) {
                datasource.createSpatialIndex(entry.key, "the_geom")
                datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT st_area(a.the_geom) as area, 'impervious' as layer, 'impervious' as type,${priorities.findIndexOf { it == "impervious" }} as priority, b.${ID_COLUMN_NAME} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, st_pointonsurface(b.the_geom))""")
                finalMerge.add("SELECT * FROM $tmptableName")
            } else if (entry.key.startsWith("building")) {
                datasource.createSpatialIndex(entry.key, "the_geom")
                datasource.execute("""DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT st_area(a.the_geom) as area, 'building' as layer, a.type, ${priorities.findIndexOf { it == "building" }} as priority, b.${ID_COLUMN_NAME} from ${entry.key}  as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, st_pointonsurface(b.the_geom))""")
                finalMerge.add("SELECT * FROM $tmptableName")
            }
        }
        if (finalMerge) {
            //Do not drop RSU table
            tablesToMerge.remove("$zone")
            def allInfoTableName = postfix "allInfoTableName"
            def groupedLandTypes = postfix("grouped_land_type")
            datasource.execute("""DROP TABLE IF EXISTS  $allInfoTableName,$groupedLandTypes , $tmp_tables, $outputTableName;
                                      CREATE TABLE $allInfoTableName as ${finalMerge.join(' union all ')};""")
            datasource.execute("""
                                      CREATE INDEX ON $allInfoTableName (${ID_COLUMN_NAME});
                                    CREATE TABLE $groupedLandTypes as select distinct ${ID_COLUMN_NAME}, first_value(type) over(partition by ${ID_COLUMN_NAME} order by priority, area) as type, first_value(layer) over(partition by ${ID_COLUMN_NAME} order by priority, area) as layer
                                    FROM $allInfoTableName;
                                   """)
            datasource.execute("""CREATE INDEX ON $groupedLandTypes ($ID_COLUMN_NAME);
                    CREATE TABLE $outputTableName as SELECT a.$ID_COLUMN_NAME, a.the_geom,  b.* EXCEPT($ID_COLUMN_NAME) FROM $final_polygonize as a left join $groupedLandTypes as b 
                on a.$ID_COLUMN_NAME= b.$ID_COLUMN_NAME;""")
            datasource.execute("""DROP TABLE IF EXISTS $final_polygonize, ${tablesToMerge.keySet().join(' , ')}, ${allInfoTableName}, ${groupedLandTypes}, ${tmpTablesToDrop.join(",")}""")

        }

    } else {
        error """Cannot compute the unified ground layer"""
    }
    return outputTableName
}

/**
 * Process used to compute the average street width needed by models such as TARGET. A simple approach based
 * on the street canyons assumption is used for the calculation. The area weighted mean building height is divided
 * by the aspect ratio (defined as the sum of facade area within a given RSU area divided by the area of free
 * surfaces of the given RSU (not covered by buildings). The "avg_height_roof_area_weighted",
 * "rsu_free_external_facade_density" and "rsu_building_density" are used for the calculation.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...) where are stored the input table and in which
 * the resulting database will be stored
 * @param rsuTable The name of the input ITable where are stored the RSU
 * @param avgHeightRoofAreaWeightedColumn The name of the column where are stored the area weighted mean building height
 * values (within the rsu Table)
 * @param aspectRatioColumn The name of the column where are stored the aspect ratio values (within the rsu Table)
 * @param prefixName String use as prefix to name the output table
 *
 * @return A database table name.
 * @author Jérémy Bernard
 */
String streetWidth(JdbcDataSource datasource, String rsuTable, String avgHeightRoofAreaWeightedColumn,
                   String aspectRatioColumn, prefixName) throws Exception {
    try {
        def COLUMN_ID_RSU = "id_rsu"
        def BASE_NAME = "street_width"
        debug "Executing RSU street width"

        // The name of the outputTableName is constructed
        def outputTableName = prefix prefixName, "rsu_" + BASE_NAME
        datasource.execute("""
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName AS 
                    SELECT CASE WHEN $aspectRatioColumn = 0
                        THEN null 
                        ELSE $avgHeightRoofAreaWeightedColumn / $aspectRatioColumn END 
                    AS $BASE_NAME, $COLUMN_ID_RSU FROM $rsuTable""")

        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("Cannot compute the street width at RSU scale", e)
    }
}