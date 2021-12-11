package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.orbisdata.datamanager.jdbc.*
import org.orbisgis.orbisdata.processmanager.api.IProcess


import static java.lang.Math.*

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
    return create {
        title "RSU free external facade density"
        id "freeExternalFacadeDensity"
        inputs buildingTable: String, rsuTable: String, buContiguityColumn: String, buTotalFacadeLengthColumn: String,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTable, rsuTable, buContiguityColumn, buTotalFacadeLengthColumn, prefixName, datasource ->

            def GEOMETRIC_FIELD_RSU = "the_geom"
            def ID_FIELD_RSU = "id_rsu"
            def HEIGHT_WALL = "height_wall"
            def BASE_NAME = "free_external_facade_density"

            debug "Executing RSU free external facade density"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            datasource."$buildingTable".id_rsu.createIndex()
            datasource."$rsuTable".id_rsu.createIndex()

            def query = """
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName AS 
                    SELECT COALESCE(
                        SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$HEIGHT_WALL)/
                                st_area(b.$GEOMETRIC_FIELD_RSU),0) 
                            AS $BASE_NAME, 
                        b.$ID_FIELD_RSU """

            query += " FROM $buildingTable a RIGHT JOIN $rsuTable b " +
                    "ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU GROUP BY b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD_RSU;"

            datasource query.toString()

            [outputTableName: outputTableName]
        }
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
 * @param rsuTable The name of the input ITable where are stored the RSU
 * @param correlationBuildingTable The name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
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
 * @author Erwan Bocher
 */
IProcess groundSkyViewFactor() {
    return create {
        title "RSU ground sky view factor"
        id "groundSkyViewFactor"
        inputs rsuTable: String, correlationBuildingTable: String, pointDensity: 0.008D,
                rayLength: 100D, numberOfDirection: 60, prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, correlationBuildingTable, pointDensity, rayLength, numberOfDirection,
              prefixName, datasource ->

            def GEOMETRIC_COLUMN_RSU = "the_geom"
            def GEOMETRIC_COLUMN_BU = "the_geom"
            def ID_COLUMN_RSU = "id_rsu"
            def HEIGHT_WALL = "height_wall"
            def BASE_NAME = "ground_sky_view_factor"

            debug "Executing RSU ground sky view factor"

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

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            // Create the needed index on input tables and the table that will contain the SVF calculation points
            datasource."$rsuTable"."$GEOMETRIC_COLUMN_RSU".createSpatialIndex()
            datasource."$rsuTable"."$ID_COLUMN_RSU".createIndex()

            datasource."$correlationBuildingTable"."$GEOMETRIC_COLUMN_BU".createSpatialIndex()
            datasource."$correlationBuildingTable"."$ID_COLUMN_RSU".createIndex()

            def to_start = System.currentTimeMillis()

            // Create the geometries of buildings and RSU holes included within each RSU
            datasource """
                DROP TABLE IF EXISTS $rsuDiff, $multiptsRSU, $multiptsRSUtot, $rsuDiffTot,$pts_RANG,$pts_order,$ptsRSUtot, $svfPts, $outputTableName;
                CREATE TABLE $rsuDiff 
                AS (SELECT  CASE WHEN   st_difference(a.$GEOMETRIC_COLUMN_RSU, st_makevalid(ST_ACCUM(b.$GEOMETRIC_COLUMN_BU)))='POLYGON EMPTY'
                            THEN        ST_EXTERIORRING(ST_NORMALIZE(a.$GEOMETRIC_COLUMN_RSU))
                            ELSE        st_difference(a.$GEOMETRIC_COLUMN_RSU, st_makevalid(ST_ACCUM(b.$GEOMETRIC_COLUMN_BU)))
                            END         AS the_geom, a.$ID_COLUMN_RSU
                FROM        $rsuTable a, $correlationBuildingTable b 
                WHERE       a.$GEOMETRIC_COLUMN_RSU && b.$GEOMETRIC_COLUMN_BU AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_RSU, 
                            b.$GEOMETRIC_COLUMN_BU)
                GROUP BY    a.$ID_COLUMN_RSU);
            """.toString()
            datasource """
                CREATE INDEX ON $rsuDiff USING BTREE($ID_COLUMN_RSU);
                CREATE TABLE $rsuDiffTot AS 
                SELECT b.$ID_COLUMN_RSU, case when a.$ID_COLUMN_RSU is null then b.the_geom else a.the_geom end as the_geom 
                FROM $rsuTable as b left join $rsuDiff as a on a.$ID_COLUMN_RSU=b.$ID_COLUMN_RSU;
            """.toString()

            // The points used for the SVF calculation are regularly selected within each RSU. The points are
            // located outside buildings (and RSU holes) and the size of the grid mesh used to sample each RSU
            // (based on the building density + 10%) - if the building density exceeds 90%,
            // the LCZ 7 building density is then set to 90%)
            datasource """CREATE TABLE $multiptsRSU AS SELECT $ID_COLUMN_RSU, THE_GEOM 
                    FROM  
                    ST_EXPLODE('(SELECT $ID_COLUMN_RSU,
                                case when LEAST(TRUNC($pointDensity*c.rsu_area_free),100)=0 
                                then st_pointonsurface(c.the_geom)
                                else ST_GENERATEPOINTS(c.the_geom, LEAST(TRUNC($pointDensity*c.rsu_area_free),100)) end
                    AS the_geom
                    FROM  (SELECT the_geom, st_area($GEOMETRIC_COLUMN_RSU) 
                                            AS rsu_area_free, $ID_COLUMN_RSU
                                FROM        st_explode(''(select * from $rsuDiffTot)'')  where st_area(the_geom)>0) as c)');""".toString()

            // Need to identify specific points for buildings being RSU (slightly away from the wall on each facade)
            datasource """  CREATE TABLE $multiptsRSUtot
                                AS SELECT $ID_COLUMN_RSU, THE_GEOM
                                FROM    ST_EXPLODE('(SELECT $ID_COLUMN_RSU, ST_LocateAlong(THE_GEOM, 0.5, 0.01) AS THE_GEOM 
                                            FROM $rsuDiffTot
                                            WHERE ST_DIMENSION(THE_GEOM)=1)')
                            UNION 
                                SELECT $ID_COLUMN_RSU, THE_GEOM
                                FROM $multiptsRSU""".toString()

            datasource."$multiptsRSUtot".the_geom.createSpatialIndex()
            // The SVF calculation is performed at point scale
            datasource """
                CREATE TABLE $svfPts 
                AS SELECT   a.$ID_COLUMN_RSU, 
                            ST_SVF(ST_GEOMETRYN(a.the_geom,1), ST_ACCUM(ST_UPDATEZ(st_force3D(b.$GEOMETRIC_COLUMN_BU), b.$HEIGHT_WALL)), 
                                   $rayLength, $numberOfDirection, 5) AS SVF
                FROM        $multiptsRSUtot AS a, $correlationBuildingTable AS b 
                WHERE       ST_EXPAND(a.the_geom, $rayLength) && b.$GEOMETRIC_COLUMN_BU AND 
                            ST_DWITHIN(b.$GEOMETRIC_COLUMN_BU, a.the_geom, $rayLength) 
                GROUP BY    a.the_geom""".toString()
            datasource."$svfPts"."$ID_COLUMN_RSU".createIndex()

            // The result of the SVF calculation is averaged at RSU scale
            datasource """
                CREATE TABLE $outputTableName(id_rsu integer, $BASE_NAME double) 
                AS          (SELECT a.$ID_COLUMN_RSU, CASE WHEN AVG(b.SVF) is not null THEN AVG(b.SVF) ELSE 1 END
                FROM        $rsuTable a 
                LEFT JOIN   $svfPts b 
                ON          a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU
                GROUP BY    a.$ID_COLUMN_RSU)""".toString()

            def tObis = System.currentTimeMillis() - to_start

            debug "SVF calculation time: ${tObis / 1000} s"

            // The temporary tables are deleted
            datasource "DROP TABLE IF EXISTS $rsuDiff, $ptsRSUtot, $multiptsRSU, $rsuDiffTot,$pts_order,$multiptsRSUtot, $svfPts".toString()

            [outputTableName: outputTableName]
        }
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
IProcess aspectRatio() {
    return create {
        title "RSU aspect ratio"
        id "aspectRatio"
        inputs rsuTable: String, rsuFreeExternalFacadeDensityColumn: String, rsuBuildingDensityColumn: String,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, rsuFreeExternalFacadeDensityColumn, rsuBuildingDensityColumn,
              prefixName, datasource ->

            def COLUMN_ID_RSU = "id_rsu"
            def BASE_NAME = "aspect_ratio"

            debug "Executing RSU aspect ratio"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            datasource """
                DROP TABLE IF EXISTS $outputTableName; 
                CREATE TABLE $outputTableName AS 
                    SELECT CASE WHEN $rsuBuildingDensityColumn = 1 
                        THEN null 
                        ELSE 0.5 * ($rsuFreeExternalFacadeDensityColumn/(1-$rsuBuildingDensityColumn)) END 
                    AS $BASE_NAME, $COLUMN_ID_RSU FROM $rsuTable""".toString()

            [outputTableName: outputTableName]
        }
    }
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
 * @return A database table name.
 * @author Jérémy Bernard
 */
IProcess projectedFacadeAreaDistribution() {
    return create {
        title "RSU projected facade area distribution"
        id "projectedFacadeAreaDistribution"
        inputs buildingTable: String, rsuTable: String, listLayersBottom: [0, 10, 20, 30, 40, 50], numberOfDirection: 12,
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTable, rsuTable, listLayersBottom, numberOfDirection, prefixName, datasource ->

            def BASE_NAME = "projected_facade_area_distribution"
            def GEOMETRIC_COLUMN_RSU = "the_geom"
            def GEOMETRIC_COLUMN_BU = "the_geom"
            def ID_COLUMN_RSU = "id_rsu"
            def ID_COLUMN_BU = "id_build"
            def HEIGHT_WALL = "height_wall"

            debug "Executing RSU projected facade area distribution"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            datasource."$buildingTable".the_geom.createSpatialIndex()
            datasource."$rsuTable".the_geom.createSpatialIndex()
            datasource."$buildingTable".id_build.createIndex()
            datasource."$rsuTable".id_rsu.createIndex()

            if (360 % numberOfDirection == 0 && numberOfDirection % 2 == 0) {

                // To avoid overwriting the output files of this step, a unique identifier is created
                // Temporary table names
                def buildingIntersection = postfix "building_intersection"
                def buildingIntersectionExpl = postfix "building_intersection_expl"
                def buildingFree = postfix "buildingFree"
                def buildingLayer = postfix "buildingLayer"
                def buildingFreeExpl = postfix "buildingFreeExpl"
                def rsuInter = postfix "rsuInter"
                def finalIndicator = postfix "finalIndicator"

                // The projection should be performed at the median of the angle interval
                def dirMedDeg = 180 / numberOfDirection
                def dirMedRad = toRadians(dirMedDeg)

                dirMedDeg = round(dirMedDeg)

                // The list that will store the fields name is initialized
                def names = []

                // Common party walls between buildings are calculated
                datasource """
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
                            FROM $buildingTable AS a, $buildingTable AS b 
                            WHERE a.$GEOMETRIC_COLUMN_BU && b.$GEOMETRIC_COLUMN_BU 
                            AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_BU, b.$GEOMETRIC_COLUMN_BU) 
                             AND a.$ID_COLUMN_BU <> b.$ID_COLUMN_BU) AS t
            """.toString()

                datasource."$buildingIntersection".id_build_a.createIndex()
                datasource."$buildingIntersection".id_build_b.createIndex()


                // Each free facade is stored TWICE (an intersection could be seen from the point of view of two
                // buildings).
                // Facades of isolated buildings are unioned to free facades of non-isolated buildings which are
                // unioned to free intersection facades. To each facade is affected its corresponding free height
                datasource """
                    DROP TABLE IF EXISTS $buildingFree;
                    CREATE TABLE $buildingFree(the_geom GEOMETRY, z_max DOUBLE, z_min DOUBLE) 
                    AS (SELECT  ST_TOMULTISEGMENTS(a.the_geom), a.$HEIGHT_WALL, 0 
                    FROM $buildingTable a WHERE a.$ID_COLUMN_BU NOT IN (SELECT ID_build_a 
                    FROM $buildingIntersection)) UNION ALL (SELECT  
                    ST_TOMULTISEGMENTS(ST_DIFFERENCE(ST_TOMULTILINE(a.$GEOMETRIC_COLUMN_BU), 
                    ST_UNION(ST_ACCUM(b.the_geom)))), a.$HEIGHT_WALL, 0 FROM $buildingTable a, 
                    $buildingIntersection b WHERE a.$ID_COLUMN_BU=b.ID_build_a 
                    GROUP BY b.ID_build_a) UNION ALL (SELECT ST_TOMULTISEGMENTS(the_geom) 
                    AS the_geom, z_max, z_min FROM $buildingIntersection WHERE ID_build_a<ID_build_b)""".toString()

                // The height of wall is calculated for each intermediate level...
                def layerQuery = "DROP TABLE IF EXISTS $buildingLayer; " +
                        "CREATE TABLE $buildingLayer AS SELECT the_geom, "
                for (i in 1..(listLayersBottom.size() - 1)) {
                    names[i - 1] = "${BASE_NAME}_H${listLayersBottom[i - 1]}" +
                            "_${listLayersBottom[i]}"
                    layerQuery += "CASEWHEN(z_max <= ${listLayersBottom[i - 1]}, 0, " +
                            "CASEWHEN(z_min >= ${listLayersBottom[i]}, " +
                            "0, ${listLayersBottom[i] - listLayersBottom[i - 1]}-" +
                            "GREATEST(${listLayersBottom[i]}-z_max,0)" +
                            "-GREATEST(z_min-${listLayersBottom[i - 1]},0))) AS ${names[i - 1]} ,"
                }

                // ...and for the final level
                names[listLayersBottom.size() - 1] = "$BASE_NAME" +
                        "_H${listLayersBottom[listLayersBottom.size() - 1]}"
                layerQuery += "CASEWHEN(z_max >= ${listLayersBottom[listLayersBottom.size() - 1]}, " +
                        "z_max-GREATEST(z_min,${listLayersBottom[listLayersBottom.size() - 1]}), 0) " +
                        "AS ${names[listLayersBottom.size() - 1]} FROM $buildingFree"
                datasource layerQuery.toString()

                // Names and types of all columns are then useful when calling sql queries
                def namesAndType = names.inject([]) { result, iter ->
                    result += " $iter double"
                }.join(",")
                def onlyNamesB = names.inject([]) { result, iter ->
                    result += "b.$iter"
                }.join(",")
                def onlyNames = names.join(",")

                datasource."$buildingLayer".the_geom.createSpatialIndex()

                // Intersections between free facades and rsu geometries are calculated
                datasource """ DROP TABLE IF EXISTS $buildingFreeExpl; 
                    CREATE TABLE $buildingFreeExpl(id_rsu INTEGER, the_geom GEOMETRY, $namesAndType) AS 
                    (SELECT a.$ID_COLUMN_RSU, ST_INTERSECTION(a.$GEOMETRIC_COLUMN_RSU, ST_TOMULTILINE(b.the_geom)), 
                    ${onlyNamesB} FROM $rsuTable a, $buildingLayer b 
                    WHERE a.$GEOMETRIC_COLUMN_RSU && b.the_geom 
                    AND ST_INTERSECTS(a.$GEOMETRIC_COLUMN_RSU, b.the_geom))""".toString()


                // Intersections  facades are exploded to multisegments
                datasource """DROP TABLE IF EXISTS $rsuInter; 
                        CREATE TABLE $rsuInter(id_rsu INTEGER, the_geom GEOMETRY, $namesAndType) 
                        AS (SELECT id_rsu, the_geom, ${onlyNames} FROM ST_EXPLODE('$buildingFreeExpl'))""".toString()


                // The analysis is then performed for each direction ('numberOfDirection' / 2 because calculation
                // is performed for a direction independently of the "toward")
                def namesAndTypeDir = []
                def onlyNamesDir = []
                def sumNamesDir = []
                def queryColumns = []
                for (int d = 0; d < numberOfDirection / 2; d++) {
                    def dirDeg = d * 360 / numberOfDirection
                    def dirRad = toRadians(dirDeg)
                    def rangeDeg = 360 / numberOfDirection
                    def dirRadMid = dirRad + dirMedRad
                    def dirDegMid = dirDeg + dirMedDeg
                    // Define the field name for each of the directions and vertical layers
                    names.each {
                        namesAndTypeDir += " " + it + "_D${dirDeg}_${dirDeg + rangeDeg} double"
                        queryColumns += """CASE
                            WHEN  a.azimuth-$dirRadMid>PI()/2
                            THEN  a.$it*a.length*COS(a.azimuth-$dirRadMid-PI()/2)/2
                            WHEN  a.azimuth-$dirRadMid<-PI()/2
                            THEN  a.$it*a.length*COS(a.azimuth-$dirRadMid+PI()/2)/2
                            ELSE  a.$it*a.length*ABS(SIN(a.azimuth-$dirRadMid))/2 
                            END AS ${it}_D${dirDeg}_${dirDeg + rangeDeg}"""
                        onlyNamesDir += "${it}_D${dirDeg}_${dirDeg + rangeDeg}"
                        sumNamesDir += "COALESCE(SUM(b.${it}_D${dirDeg}_${dirDeg + rangeDeg}), 0) " +
                                "AS ${it}_D${dirDeg}_${dirDeg + rangeDeg} "
                    }
                }
                namesAndTypeDir = namesAndTypeDir.join(",")
                queryColumns = queryColumns.join(",")
                onlyNamesDir = onlyNamesDir.join(",")
                sumNamesDir = sumNamesDir.join(",")

                def query = "DROP TABLE IF EXISTS $finalIndicator; " +
                        "CREATE TABLE $finalIndicator AS SELECT a.id_rsu," + queryColumns +
                        " FROM (SELECT id_rsu, CASE WHEN ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)) >= PI()" +
                        "THEN ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)) - PI() " +
                        "ELSE ST_AZIMUTH(ST_STARTPOINT(the_geom), ST_ENDPOINT(the_geom)) END AS azimuth," +
                        " ST_LENGTH(the_geom) AS length, ${onlyNames} FROM $rsuInter) a"

                datasource query.toString()


                datasource."$finalIndicator".id_rsu.createIndex()
                // Sum area at RSU scale and fill null values with 0
                datasource """
                    DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE    ${outputTableName} 
                    AS SELECT       a.id_rsu, ${sumNamesDir} 
                    FROM            $rsuTable a LEFT JOIN $finalIndicator b 
                    ON              a.id_rsu = b.id_rsu 
                    GROUP BY        a.id_rsu""".toString()

                // Remove all temporary tables created
                datasource """DROP TABLE IF EXISTS $buildingIntersection, $buildingIntersectionExpl, 
                        $buildingFree, $buildingFreeExpl, $buildingLayer, $rsuInter, $finalIndicator;""".toString()
            }

            [outputTableName: outputTableName]
        }
    }
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
 * @param buildingTable the name of the input ITable where are stored the buildings and the relationships
 * between buildings and RSU
 * @param rsuTable the name of the input ITable where are stored the RSU
 * @param prefixName String use as prefix to name the output table
 * @param listLayersBottom the list of height corresponding to the bottom of each vertical layers (default [0, 10, 20,
 * 30, 40, 50])
 * @param density Boolean to set whether or not the roof density should be calculated in addition to the distribution
 * (default true)
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
IProcess roofAreaDistribution() {
    return create {
        title "RSU roof area distribution"
        id "roofAreaDistribution"
        inputs rsuTable: String, buildingTable: String, listLayersBottom: [0, 10, 20, 30, 40, 50], prefixName: String,
                datasource: JdbcDataSource, density: true
        outputs outputTableName: String
        run { rsuTable, buildingTable, listLayersBottom, prefixName, datasource, density ->

            def GEOMETRIC_COLUMN_RSU = "the_geom"
            def GEOMETRIC_COLUMN_BU = "the_geom"
            def ID_COLUMN_RSU = "id_rsu"
            def ID_COLUMN_BU = "id_build"
            def HEIGHT_WALL = "height_wall"
            def HEIGHT_ROOF = "height_roof"
            def BASE_NAME = "roof_area_distribution"

            debug "Executing RSU roof area distribution (and optionally roof density)"

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def buildRoofSurfIni = postfix "build_roof_surf_ini"
            def buildVertRoofInter = postfix "build_vert_roof_inter"
            def buildVertRoofAll = postfix "buildVertRoofAll"
            def buildRoofSurfTot = postfix "build_roof_surf_tot"
            def optionalTempo = postfix "optionalTempo"

            datasource."$rsuTable".the_geom.createSpatialIndex()
            datasource."$rsuTable".id_rsu.createIndex()

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            // Vertical and non-vertical (tilted and horizontal) roof areas are calculated
            datasource """
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
                    FROM $buildingTable;
        """.toString()


            // Indexes and spatial indexes are created on rsu and building Tables
            datasource """CREATE INDEX IF NOT EXISTS ids_ina ON $buildRoofSurfIni USING RTREE($GEOMETRIC_COLUMN_BU);
                CREATE INDEX IF NOT EXISTS id_ina ON $buildRoofSurfIni ($ID_COLUMN_BU);""".toString()

            // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
            // and the corresponding area is estimated (only if the building roof does not overpass the building
            // wall of the neighbor)
            datasource """
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
                    GROUP BY b.$ID_COLUMN_BU);""".toString()

            // Indexes and spatial indexes are created on rsu and building Tables
            datasource """CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofInter ($ID_COLUMN_BU);""".toString()

            // Vertical roofs that are potentially in contact with the facade of a building neighbor are identified
            // and the corresponding area is estimated (only if the building roof does not overpass the building wall
            // of the neighbor)
            datasource """
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
                    ON a.$ID_COLUMN_BU=b.$ID_COLUMN_BU);""".toString()

            // Indexes and spatial indexes are created on rsu and building Tables
            datasource """CREATE INDEX IF NOT EXISTS ids_bu ON $buildVertRoofAll USING RTREE(the_geom); 
                    CREATE INDEX IF NOT EXISTS id_bu ON $buildVertRoofAll (id_build); 
                    CREATE INDEX IF NOT EXISTS id_rsu ON $buildVertRoofAll (id_rsu);""".toString()

            //TODO : PEUT-ETRE MIEUX VAUT-IL FAIRE L'INTERSECTION À PART POUR ÉVITER DE LA FAIRE 2 FOIS ICI ?

            // Update the roof areas (vertical and non vertical) taking into account the vertical roofs shared with
            // the neighbor facade and the roof surfaces that are not in the RSU. Note that half of the facade
            // are considered as vertical roofs, the other to "normal roof".
            datasource """
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
                    FROM $buildVertRoofAll a, $rsuTable b 
                    WHERE a.id_rsu=b.$ID_COLUMN_RSU 
                    GROUP BY b.$GEOMETRIC_COLUMN_RSU, a.id_build, a.id_rsu, a.z_max, a.z_min, a.delta_h);""".toString()

            // The roof area is calculated for each level except the last one (> 50 m in the default case)
            def finalQuery = "DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.id_rsu, "
            def nonVertQuery = ""
            def vertQuery = ""
            for (i in 1..(listLayersBottom.size() - 1)) {
                nonVertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(" +
                        "a.z_max <= ${listLayersBottom[i]}, CASEWHEN(a.delta_h=0, a.non_vertical_roof_area, " +
                        "a.non_vertical_roof_area*(a.z_max-GREATEST(${listLayersBottom[i - 1]},a.z_min))/a.delta_h), " +
                        "CASEWHEN(a.z_min < ${listLayersBottom[i]}, a.non_vertical_roof_area*(${listLayersBottom[i]}-" +
                        "GREATEST(${listLayersBottom[i - 1]},a.z_min))/a.delta_h, 0)))),0) AS non_vert_roof_area_H" +
                        "${listLayersBottom[i - 1]}_${listLayersBottom[i]},"
                vertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= ${listLayersBottom[i - 1]}, 0, CASEWHEN(" +
                        "a.z_max <= ${listLayersBottom[i]}, CASEWHEN(a.delta_h=0, 0, " +
                        "a.vertical_roof_area*POWER((a.z_max-GREATEST(${listLayersBottom[i - 1]}," +
                        "a.z_min))/a.delta_h, 2)), CASEWHEN(a.z_min < ${listLayersBottom[i]}, " +
                        "CASEWHEN(a.z_min>${listLayersBottom[i - 1]}, a.vertical_roof_area*(1-" +
                        "POWER((a.z_max-${listLayersBottom[i]})/a.delta_h,2)),a.vertical_roof_area*(" +
                        "POWER((a.z_max-${listLayersBottom[i - 1]})/a.delta_h,2)-POWER((a.z_max-${listLayersBottom[i]})/" +
                        "a.delta_h,2))), 0)))),0) AS vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]},"
            }
            // The roof area is calculated for the last level (> 50 m in the default case)
            def valueLastLevel = listLayersBottom[listLayersBottom.size() - 1]
            nonVertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= $valueLastLevel, 0, CASEWHEN(a.delta_h=0, a.non_vertical_roof_area, " +
                    "a.non_vertical_roof_area*(a.z_max-GREATEST($valueLastLevel,a.z_min))/a.delta_h))),0) AS non_vert_roof_area_H" +
                    "${valueLastLevel},"
            vertQuery += " COALESCE(SUM(CASEWHEN(a.z_max <= $valueLastLevel, 0, CASEWHEN(a.delta_h=0, a.vertical_roof_area, " +
                    "a.vertical_roof_area*(a.z_max-GREATEST($valueLastLevel,a.z_min))/a.delta_h))),0) AS vert_roof_area_H" +
                    "${valueLastLevel},"

            def endQuery = """ FROM $buildRoofSurfTot a RIGHT JOIN $rsuTable b 
                                    ON a.id_rsu = b.id_rsu GROUP BY b.id_rsu;"""

            datasource finalQuery.toString() + nonVertQuery.toString() + vertQuery[0..-2].toString() + endQuery.toString()

            // Calculate the roof density if needed
            if (density) {
                def optionalQuery = "ALTER TABLE $outputTableName RENAME TO $optionalTempo;" +
                        "CREATE INDEX IF NOT EXISTS id ON $optionalTempo USING BTREE($ID_COLUMN_RSU);" +
                        "DROP TABLE IF EXISTS $outputTableName; " +
                        "CREATE TABLE $outputTableName AS SELECT a.*, "
                def optionalNonVert = "("
                def optionalVert = "("

                for (i in 1..(listLayersBottom.size() - 1)) {
                    optionalNonVert += " a.non_vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]} + "
                    optionalVert += "a.vert_roof_area_H${listLayersBottom[i - 1]}_${listLayersBottom[i]} + "
                }
                optionalNonVert += "a.non_vert_roof_area_H$valueLastLevel) / ST_AREA(b.$GEOMETRIC_COLUMN_RSU)"
                optionalVert += "a.vert_roof_area_H${valueLastLevel}) / ST_AREA(b.$GEOMETRIC_COLUMN_RSU)"
                optionalQuery += "$optionalNonVert AS VERT_ROOF_DENSITY, $optionalVert AS NON_VERT_ROOF_DENSITY" +
                        " FROM $optionalTempo a RIGHT JOIN $rsuTable b ON a.$ID_COLUMN_RSU = b.$ID_COLUMN_RSU;"

                datasource optionalQuery.toString()
            }

            datasource """DROP TABLE IF EXISTS $buildRoofSurfIni, $buildVertRoofInter, 
                    $buildVertRoofAll, $buildRoofSurfTot, $optionalTempo;""".toString()

            [outputTableName: outputTableName]
        }
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
IProcess effectiveTerrainRoughnessLength() {
    return create {
        title "RSU effective terrain roughness height"
        id "effectiveTerrainRoughnessLength"
        inputs rsuTable: String, projectedFacadeAreaName: "projected_facade_area_distribution",
                geometricMeanBuildingHeightName: String, prefixName: String,
                listLayersBottom: [0, 10, 20, 30, 40, 50], numberOfDirection: 12, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, projectedFacadeAreaName, geometricMeanBuildingHeightName, prefixName, listLayersBottom,
              numberOfDirection, datasource ->

            def GEOMETRIC_COLUMN = "the_geom"
            def ID_COLUMN_RSU = "id_rsu"
            def BASE_NAME = "effective_terrain_roughness_length"

            debug "Executing RSU effective terrain roughness length"

            // Processes used for the indicator calculation
            // Some local variables are initialized
            def names = []
            // The projection should be performed at the median of the angle interval
            def dirRangeDeg = round(360 / numberOfDirection)

            // To avoid overwriting the output files of this step, a unique identifier is created
            // Temporary table names
            def lambdaTable = postfix "lambdaTable"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            // The lambda_f indicator is first calculated
            def lambdaQuery = "DROP TABLE IF EXISTS $lambdaTable;" +
                    "CREATE TABLE $lambdaTable AS SELECT $ID_COLUMN_RSU, $geometricMeanBuildingHeightName, ("
            for (int i in 1..listLayersBottom.size()) {
                names[i - 1] = "${projectedFacadeAreaName}_H${listLayersBottom[i - 1]}_${listLayersBottom[i]}"
                if (i == listLayersBottom.size()) {
                    names[listLayersBottom.size() - 1] =
                            "${projectedFacadeAreaName}_H${listLayersBottom[listLayersBottom.size() - 1]}"
                }
                for (int d = 0; d < numberOfDirection / 2; d++) {
                    def dirDeg = d * 360 / numberOfDirection
                    lambdaQuery += "${names[i - 1]}_D${dirDeg}_${dirDeg + dirRangeDeg}+"
                }
            }
            lambdaQuery = lambdaQuery[0..-2] + ")/(${numberOfDirection / 2}*ST_AREA($GEOMETRIC_COLUMN)) " +
                    "AS lambda_f FROM $rsuTable"
            datasource lambdaQuery.toString()

            // The rugosity z0 is calculated according to the indicator lambda_f (the value of indicator z0 is limited to 3 m)
            datasource """DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName 
                    AS SELECT $ID_COLUMN_RSU, CASEWHEN(lambda_f < 0.15, CASEWHEN(lambda_f*$geometricMeanBuildingHeightName>3,
                    3,lambda_f*$geometricMeanBuildingHeightName), CASEWHEN(0.15*$geometricMeanBuildingHeightName>3,3,
                    0.15*$geometricMeanBuildingHeightName)) AS $BASE_NAME FROM $lambdaTable;
            DROP TABLE IF EXISTS $lambdaTable""".toString()

            [outputTableName: outputTableName]
        }
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
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
IProcess linearRoadOperations() {
    return create {
        title "Operations on the linear of road"
        id "linearRoadOperations"
        inputs rsuTable: String, roadTable: String, operations: String[], prefixName: String, angleRangeSize: 30,
                levelConsiderated: [0], datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, roadTable, operations, prefixName, angleRangeSize, levelConsiderated,
              datasource ->

            def OPS = ["road_direction_distribution", "linear_road_density"]
            def GEOMETRIC_COLUMN_RSU = "the_geom"
            def ID_COLUMN_RSU = "id_rsu"
            def GEOMETRIC_COLUMN_ROAD = "the_geom"
            def Z_INDEX = "zindex"
            def BASE_NAME = "rsu_road_linear_properties"

            debug "Executing Operations on the linear of road"

            datasource."$rsuTable".the_geom.createSpatialIndex()
            datasource."$rsuTable".id_rsu.createIndex()
            datasource."$roadTable".the_geom.createSpatialIndex()
            datasource."$roadTable".zindex.createIndex()

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
                        def selectionQuery = "DROP TABLE IF EXISTS $roadInter; " +
                                "CREATE TABLE $roadInter AS SELECT a.$ID_COLUMN_RSU AS id_rsu, " +
                                "ST_AREA(a.$GEOMETRIC_COLUMN_RSU) AS rsu_area, ST_INTERSECTION(a.$GEOMETRIC_COLUMN_RSU, " +
                                "b.$GEOMETRIC_COLUMN_ROAD) AS the_geom $ifZindex FROM $rsuTable a, $roadTable b " +
                                "WHERE $filtering;"
                        datasource selectionQuery.toString()

                        // If all roads are considered at the same level...
                        if (!levelConsiderated) {
                            nameDens.add("linear_road_density")
                            caseQueryDens = "SUM(ST_LENGTH(the_geom))/rsu_area AS linear_road_density "
                            for (int d = angleRangeSize; d <= 180; d += angleRangeSize) {
                                caseQueryDistrib += "SUM(CASEWHEN(azimuth>=${d - angleRangeSize} AND azimuth<$d, length, 0)) AS " +
                                        "road_direction_distribution_d${d - angleRangeSize}_$d,"
                                nameDistrib.add("road_direction_distribution_d${d - angleRangeSize}_$d")
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
                                            "road_direction_distribution_h${lev.toString().replaceAll("-", "minus")}" +
                                            "_d${d - angleRangeSize}_$d,"
                                    nameDistrib.add("road_direction_distribution_h${lev.toString().replaceAll("-", "minus")}" +
                                            "_d${d - angleRangeSize}_$d")
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
                                    "CREATE INDEX IF NOT EXISTS id_d ON $roadDistrib USING BTREE(id_rsu);" +
                                    "DROP TABLE IF EXISTS $roadDistTot; CREATE TABLE $roadDistTot($ID_COLUMN_RSU INTEGER," +
                                    "${nameDistrib.join(" double,")} double) AS (SELECT a.$ID_COLUMN_RSU," +
                                    "COALESCE(b.${nameDistrib.join(",0),COALESCE(b.")},0)  " +
                                    "FROM $rsuTable a LEFT JOIN $roadDistrib b ON a.$ID_COLUMN_RSU=b.id_rsu);"
                            datasource queryDistrib.toString()

                            if (!operations.contains("linear_road_density")) {
                                datasource """DROP TABLE IF EXISTS $outputTableName;
                                        ALTER TABLE $roadDistTot RENAME TO $outputTableName""".toString()
                            }
                        }

                        // If the rsu linear density should be calculated
                        if (operations.contains("linear_road_density")) {
                            String queryDensity = "DROP TABLE IF EXISTS $roadDens;" +
                                    "CREATE TABLE $roadDens AS SELECT id_rsu, " + caseQueryDens[0..-2] +
                                    " FROM $roadInter GROUP BY id_rsu;" +
                                    "CREATE INDEX IF NOT EXISTS id_d ON $roadDens USING BTREE(id_rsu);" +
                                    "DROP TABLE IF EXISTS $roadDensTot; CREATE TABLE $roadDensTot($ID_COLUMN_RSU INTEGER," +
                                    "${nameDens.join(" double,")} double) AS (SELECT a.$ID_COLUMN_RSU," +
                                    "COALESCE(b.${nameDens.join(",0),COALESCE(b.")},0) " +
                                    "FROM $rsuTable a LEFT JOIN $roadDens b ON a.$ID_COLUMN_RSU=b.id_rsu)"
                            datasource queryDensity
                            if (!operations.contains("road_direction_distribution")) {
                                datasource """DROP TABLE IF EXISTS $outputTableName;
                                        ALTER TABLE $roadDensTot RENAME TO $outputTableName""".toString()
                            }
                        }
                        if (operations.contains("road_direction_distribution") &&
                                operations.contains("linear_road_density")) {
                            datasource """DROP TABLE if exists $outputTableName; 
                                CREATE INDEX IF NOT EXISTS idx_$roadDistTot ON $roadDistTot USING BTREE(id_rsu);
                                CREATE INDEX IF NOT EXISTS idx_$roadDensTot ON $roadDensTot USING BTREE(id_rsu);
                                CREATE TABLE $outputTableName AS SELECT a.*,
                                b.${nameDens.join(",b.")} FROM $roadDistTot a LEFT JOIN $roadDensTot b 
                                ON a.id_rsu=b.id_rsu""".toString()
                        }

                        datasource """DROP TABLE IF EXISTS $roadInter, $roadExpl, $roadDistrib,
                                    $roadDens, $roadDistTot, $roadDensTot""".toString()

                        [outputTableName: outputTableName]

                    } else {
                        error "One of several operations are not valid."
                    }
                }
            } else {
                error "Cannot compute the indicator. The range size (angleRangeSize) should be a divisor of 180°"
            }
        }
    }
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
 * @param effectiveTerrainRoughnessLength the field name corresponding to the RSU effective terrain roughness class due
 * to roughness elements (buildings, trees, etc.) (in the rsuTable)
 * @param prefixName String use as prefix to name the output table
 *
 * @return outputTableName Table name in which the rsu id and their corresponding indicator value are stored
 *
 * @author Jérémy Bernard
 */
IProcess effectiveTerrainRoughnessClass() {
    return create {
        title "RSU effective terrain roughness class"
        id "effectiveTerrainRoughnessClass"
        inputs datasource: JdbcDataSource, rsuTable: String, effectiveTerrainRoughnessLength: String, prefixName: String
        outputs outputTableName: String
        run { datasource, rsuTable, effectiveTerrainRoughnessLength, prefixName ->

            def ID_COLUMN_RSU = "id_rsu"
            def BASE_NAME = "effective_terrain_roughness_class"

            debug "Executing RSU effective terrain roughness class"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            // Based on the lookup Table of Davenport
            datasource """DROP TABLE IF EXISTS $outputTableName;
                    CREATE TABLE $outputTableName AS SELECT $ID_COLUMN_RSU, 
                    CASEWHEN($effectiveTerrainRoughnessLength<0.0 OR $effectiveTerrainRoughnessLength IS NULL, null,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.00035, 1,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.01525, 2,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.065, 3,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.175, 4,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.375, 5,
                    CASEWHEN($effectiveTerrainRoughnessLength<0.75, 6,
                    CASEWHEN($effectiveTerrainRoughnessLength<1.5, 7, 8)))))))) AS $BASE_NAME FROM $rsuTable""".toString()

            [outputTableName: outputTableName]
        }
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
    return create {
        title "Extended RSU free facade fraction (for SVF fast)"
        id "extendedFreeFacadeFraction"
        inputs buildingTable: String, rsuTable: String, buContiguityColumn: String, buTotalFacadeLengthColumn: String,
                prefixName: String, buffDist: 10, datasource: JdbcDataSource
        outputs outputTableName: String
        run { buildingTable, rsuTable, buContiguityColumn, buTotalFacadeLengthColumn, prefixName, buffDist, datasource ->

            def GEOMETRIC_FIELD = "the_geom"
            def ID_FIELD_RSU = "id_rsu"
            def HEIGHT_WALL = "height_wall"
            def BASE_NAME = "extended_free_facade_fraction"

            debug "Executing RSU free facade fraction (for SVF fast)"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, "rsu_" + BASE_NAME

            // Temporary tables are created
            def extRsuTable = postfix "extRsu"
            def inclBu = postfix "inclBu"
            def fullInclBu = postfix "fullInclBu"
            def notIncBu = postfix "notIncBu"

            // The RSU area is extended according to a buffer
            datasource """DROP TABLE IF EXISTS $extRsuTable; CREATE TABLE $extRsuTable AS SELECT  
                    ST_BUFFER($GEOMETRIC_FIELD, $buffDist, 'quad_segs=2') AS $GEOMETRIC_FIELD,
                    $ID_FIELD_RSU FROM $rsuTable;""".toString()

            // The facade area of buildings being entirely included in the RSU buffer is calculated
            datasource."$extRsuTable"."$GEOMETRIC_FIELD".createSpatialIndex()
            datasource."$extRsuTable"."$ID_FIELD_RSU".createIndex()
            datasource."$buildingTable"."$GEOMETRIC_FIELD".createSpatialIndex()

            datasource """DROP TABLE IF EXISTS $inclBu; CREATE TABLE $inclBu AS SELECT 
                    COALESCE(SUM((1-a.$buContiguityColumn)*a.$buTotalFacadeLengthColumn*a.$HEIGHT_WALL), 0) AS FAC_AREA,
                    b.$ID_FIELD_RSU FROM $buildingTable a, $extRsuTable b WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD and ST_COVERS(b.$GEOMETRIC_FIELD,
                    a.$GEOMETRIC_FIELD) GROUP BY b.$ID_FIELD_RSU;""".toString()

            // All RSU are feeded with default value
            datasource."$inclBu"."$ID_FIELD_RSU".createIndex()
            datasource."$rsuTable"."$ID_FIELD_RSU".createIndex()

            datasource """DROP TABLE IF EXISTS $fullInclBu; CREATE TABLE $fullInclBu AS SELECT 
                    COALESCE(a.FAC_AREA, 0) AS FAC_AREA, b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD, st_area(b.$GEOMETRIC_FIELD) as rsu_buff_area 
                    FROM $inclBu a RIGHT JOIN $extRsuTable b ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;""".toString()

            // The facade area of buildings being partially included in the RSU buffer is calculated
            datasource """DROP TABLE IF EXISTS $notIncBu; CREATE TABLE $notIncBu AS SELECT 
                    COALESCE(SUM(ST_LENGTH(ST_INTERSECTION(ST_TOMULTILINE(a.$GEOMETRIC_FIELD),
                     b.$GEOMETRIC_FIELD))*a.$HEIGHT_WALL), 0) 
                    AS FAC_AREA, b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD FROM $buildingTable a, $extRsuTable b 
                    WHERE a.$GEOMETRIC_FIELD && b.$GEOMETRIC_FIELD and ST_OVERLAPS(b.$GEOMETRIC_FIELD, a.$GEOMETRIC_FIELD) GROUP BY b.$ID_FIELD_RSU, b.$GEOMETRIC_FIELD;""".toString()

            // The facade fraction is calculated
            datasource."$notIncBu"."$ID_FIELD_RSU".createIndex()
            datasource."$fullInclBu"."$ID_FIELD_RSU".createIndex()

            datasource """DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS 
                    SELECT COALESCE((a.FAC_AREA + b.FAC_AREA) /(a.FAC_AREA + b.FAC_AREA + a.rsu_buff_area),
                     a.FAC_AREA / (a.FAC_AREA  + a.rsu_buff_area))
                    AS $BASE_NAME, 
                    a.$ID_FIELD_RSU, a.$GEOMETRIC_FIELD FROM $fullInclBu a LEFT JOIN $notIncBu b 
                    ON a.$ID_FIELD_RSU = b.$ID_FIELD_RSU;""".toString()

            // Drop intermediate tables
            datasource "DROP TABLE IF EXISTS $extRsuTable, $inclBu, $fullInclBu, $notIncBu;".toString()

            [outputTableName: outputTableName]
        }
    }
}

/**
 * This process computes all spatial relations by USR between a set of input layers : building, road, water,
 * vegetation and impervious
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
IProcess smallestCommunGeometry() {
    return create {
        title "RSU surface features"
        id "smallestCommunGeometry"
        inputs rsuTable: String, id_rsu : "id_rsu" , buildingTable: "", roadTable: "", waterTable: "", vegetationTable: "",
                imperviousTable: "", prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, id_rsu, buildingTable, roadTable, waterTable, vegetationTable,
              imperviousTable, prefixName, datasource ->
            //All table names cannot be null or empty
            if(!buildingTable && !roadTable && !waterTable && !vegetationTable && !imperviousTable){
                return
            }
            def BASE_NAME = "RSU_SMALLEST_COMMUN_GEOMETRY"

            debug "Compute the smallest geometries"

            //To avoid column name duplication
            def ID_COLUMN_NAME =  postfix "id"

            // The name of the outputTableName is constructed
            def outputTableName = prefix prefixName, BASE_NAME

            if (rsuTable && datasource.hasTable(rsuTable)) {
                datasource."$rsuTable"."$id_rsu".createIndex()
                datasource."$rsuTable".the_geom.createSpatialIndex()
                def tablesToMerge = [:]
                tablesToMerge += ["$rsuTable": "select ST_ExteriorRing(the_geom) as the_geom, ${id_rsu} from $rsuTable"]
                if (roadTable && datasource.hasTable(roadTable)) {
                    debug "Preparing table : $roadTable"
                    datasource."$roadTable".the_geom.createSpatialIndex()
                    //Separate road features according the zindex
                    def roadTable_zindex0_buffer = postfix "road_zindex0_buffer"
                    def road_tmp = postfix "road_zindex0"
                    datasource """DROP TABLE IF EXISTS $roadTable_zindex0_buffer, $road_tmp;
            CREATE TABLE $roadTable_zindex0_buffer as SELECT st_buffer(the_geom, WIDTH::DOUBLE PRECISION/2)
            AS the_geom
            FROM $roadTable  where ZINDEX=0 ;
            CREATE INDEX IF NOT EXISTS ids_$roadTable_zindex0_buffer ON $roadTable_zindex0_buffer USING RTREE(the_geom);
            CREATE TABLE $road_tmp AS SELECT ST_CollectionExtract(st_intersection(st_union(st_accum(a.the_geom)),b.the_geom),3) AS the_geom, b.${id_rsu} FROM
            $roadTable_zindex0_buffer AS a, $rsuTable AS b WHERE a.the_geom && b.the_geom AND st_intersects(a.the_geom, b.the_geom) GROUP BY b.${id_rsu};
            DROP TABLE IF EXISTS $roadTable_zindex0_buffer;
            """.toString()
                    tablesToMerge += ["$road_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_rsu} from $road_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                }

                if (vegetationTable && datasource.hasTable(vegetationTable)) {
                    debug "Preparing table : $vegetationTable"
                    datasource."$vegetationTable".the_geom.createSpatialIndex()
                    def low_vegetation_rsu_tmp = postfix "low_vegetation_rsu_zindex0"
                    def low_vegetation_tmp = postfix "low_vegetation_zindex0"
                    def high_vegetation_tmp = postfix "high_vegetation_zindex0"
                    def high_vegetation_rsu_tmp = postfix "high_vegetation_zindex0"
                    datasource """DROP TABLE IF EXISTS $low_vegetation_tmp, $low_vegetation_rsu_tmp;
                CREATE TABLE $low_vegetation_rsu_tmp as select st_union(st_accum(a.the_geom)) as the_geom,  b.${id_rsu} FROM 
                    $vegetationTable AS a, $rsuTable AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.height_class='low' group by b.${id_rsu};
                CREATE INDEX ON $low_vegetation_rsu_tmp(${id_rsu});
                CREATE TABLE $low_vegetation_tmp AS SELECT ST_CollectionExtract(st_buffer(st_intersection(a.the_geom, b.the_geom),0),3) AS the_geom, b.${id_rsu} FROM 
                        $low_vegetation_rsu_tmp AS a, $rsuTable AS b WHERE a.${id_rsu}=b.${id_rsu} group by b.${id_rsu};
                        DROP TABLE IF EXISTS $high_vegetation_tmp,$high_vegetation_rsu_tmp;
                CREATE TABLE $high_vegetation_rsu_tmp as select st_union(st_accum(a.the_geom)) as the_geom,  b.${id_rsu} FROM 
                    $vegetationTable AS a, $rsuTable AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.height_class='high' group by b.${id_rsu};
                CREATE INDEX ON $high_vegetation_rsu_tmp(${id_rsu});
                CREATE TABLE $high_vegetation_tmp AS SELECT ST_CollectionExtract(st_buffer(st_intersection(a.the_geom, b.the_geom),0),3) AS the_geom, b.${id_rsu} FROM 
                        $high_vegetation_rsu_tmp AS a, $rsuTable AS b WHERE a.${id_rsu}=b.${id_rsu} group by b.${id_rsu};
                DROP TABLE $low_vegetation_rsu_tmp, $high_vegetation_rsu_tmp;""".toString()
                    tablesToMerge += ["$low_vegetation_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_rsu} from $low_vegetation_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                    tablesToMerge += ["$high_vegetation_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_rsu} from $high_vegetation_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                }

                if (waterTable && datasource.hasTable(waterTable)) {
                    debug "Preparing table : $waterTable"
                    datasource."$waterTable".the_geom.createSpatialIndex()
                    def water_tmp = postfix "water_zindex0"
                    datasource """DROP TABLE IF EXISTS $water_tmp;
                CREATE TABLE $water_tmp AS SELECT ST_CollectionExtract(st_buffer(st_intersection(a.the_geom, b.the_geom),0),3) AS the_geom, b.${id_rsu} FROM 
                        $waterTable AS a, $rsuTable AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom)""".toString()
                    tablesToMerge += ["$water_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_rsu} from $water_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                }

                if (imperviousTable && datasource.hasTable(imperviousTable)) {
                    debug "Preparing table : $imperviousTable"
                    datasource."$imperviousTable".the_geom.createSpatialIndex()
                    def impervious_tmp = postfix "impervious_zindex0"
                    datasource """DROP TABLE IF EXISTS $impervious_tmp;
                CREATE TABLE $impervious_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_rsu} FROM 
                        $imperviousTable AS a, $rsuTable AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom)""".toString()
                    tablesToMerge += ["$impervious_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_rsu} from $impervious_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                }

                if (buildingTable && datasource.hasTable(buildingTable)) {
                    debug "Preparing table : $buildingTable"
                    datasource."$buildingTable".the_geom.createSpatialIndex()
                    def building_tmp = postfix "building_zindex0"
                    datasource """DROP TABLE IF EXISTS $building_tmp;
                CREATE TABLE $building_tmp AS SELECT ST_CollectionExtract(st_intersection(a.the_geom, b.the_geom),3) AS the_geom, b.${id_rsu} FROM 
                        $buildingTable AS a, $rsuTable AS b WHERE a.the_geom && b.the_geom 
                        AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.zindex=0""".toString()
                    tablesToMerge += ["$building_tmp": "select ST_ToMultiLine(the_geom) as the_geom, ${id_rsu} from $building_tmp WHERE ST_ISEMPTY(THE_GEOM)=false"]
                }

                //Merging all tables in one
                debug "Grouping all tables in one..."
                if (!tablesToMerge) {
                    error "Any features to compute surface fraction statistics"
                    return
                }
                def tmp_tables = postfix "tmp_tables_zindex0"
                datasource """DROP TABLE if exists $tmp_tables;
            CREATE TABLE $tmp_tables(the_geom GEOMETRY, ${id_rsu} integer) AS ${tablesToMerge.values().join(' union ')};
               """.toString()

                //Polygonize the input tables
                debug "Generating " +
                        "minimum polygon areas"
                def tmp_point_polygonize = postfix "tmp_point_polygonize_zindex0"
                datasource """DROP TABLE IF EXISTS $tmp_point_polygonize;
                CREATE TABLE $tmp_point_polygonize as  select  EXPLOD_ID as ${ID_COLUMN_NAME}, st_pointonsurface(st_force2D(the_geom)) as the_geom ,
                st_area(the_geom) as area , ${id_rsu}
                 from st_explode ('(select st_polygonize(st_union(st_force2d(
                st_precisionreducer(st_node(st_accum(st_force2d(a.the_geom))), 3)))) as the_geom, ${id_rsu} from $tmp_tables as a group by ${id_rsu})')""".toString()

                //Create indexes
                datasource."$tmp_point_polygonize".the_geom.createSpatialIndex()
                datasource."$tmp_point_polygonize"."${id_rsu}".createIndex()

                def final_polygonize = postfix "final_polygonize_zindex0"
                datasource """
            DROP TABLE IF EXISTS $final_polygonize;
            CREATE TABLE $final_polygonize as select a.AREA , a.the_geom as the_geom, a.${ID_COLUMN_NAME}, b.${id_rsu}
            from $tmp_point_polygonize as a, $rsuTable as b
            where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()

                datasource."$final_polygonize".the_geom.createSpatialIndex()
                datasource."$final_polygonize"."${id_rsu}".createIndex()

                def finalMerge = []
                def tmpTablesToDrop = []
                tablesToMerge.each { entry ->
                    debug "Processing table $entry.key"
                    def tmptableName = "tmp_stats_$entry.key"
                    tmpTablesToDrop<<tmptableName
                    if (entry.key.startsWith("high_vegetation")) {
                        datasource."$entry.key".the_geom.createSpatialIndex()
                        datasource."$entry.key"."${id_rsu}".createIndex()
                        datasource """DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT b.area,0 as low_vegetation, 1 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 0 as building, b.${ID_COLUMN_NAME}, b.${id_rsu} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()
                        finalMerge.add("SELECT * FROM $tmptableName")
                    } else if (entry.key.startsWith("low_vegetation")) {
                        datasource."$entry.key".the_geom.createSpatialIndex()
                        datasource."$entry.key"."${id_rsu}".createIndex()
                        datasource """DROP TABLE IF EXISTS $tmptableName;
                 CREATE TABLE $tmptableName AS SELECT b.area,1 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 0 as building, b.${ID_COLUMN_NAME}, b.${id_rsu} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and st_intersects(a.the_geom,b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()
                        finalMerge.add("SELECT * FROM $tmptableName")
                    } else if (entry.key.startsWith("water")) {
                        datasource."$entry.key".the_geom.createSpatialIndex()
                        datasource."$entry.key"."${id_rsu}".createIndex()
                        datasource """CREATE TABLE $tmptableName AS SELECT b.area,0 as low_vegetation, 0 as high_vegetation, 1 as water, 0 as impervious, 0 as road,  0 as building, b.${ID_COLUMN_NAME}, b.${id_rsu} from ${entry.key} as a,
                $final_polygonize as b  where a.the_geom && b.the_geom and st_intersects(a.the_geom, b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()
                        finalMerge.add("SELECT * FROM $tmptableName")
                    } else if (entry.key.startsWith("road")) {
                        datasource."$entry.key".the_geom.createSpatialIndex()
                        datasource """DROP TABLE IF EXISTS $tmptableName;
                    CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 1 as road, 0 as building, b.${ID_COLUMN_NAME}, b.${id_rsu} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()
                        finalMerge.add("SELECT * FROM $tmptableName")
                    } else if (entry.key.startsWith("impervious")) {
                        datasource."$entry.key".the_geom.createSpatialIndex()
                        datasource."$entry.key"."${id_rsu}".createIndex()
                        datasource """DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 1 as impervious, 0 as road, 0 as building, b.${ID_COLUMN_NAME}, b.${id_rsu} from ${entry.key} as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()
                        finalMerge.add("SELECT * FROM $tmptableName")
                    } else if (entry.key.startsWith("building")) {
                        datasource."$entry.key".the_geom.createSpatialIndex()
                        datasource."$entry.key"."${id_rsu}".createIndex()
                        datasource """DROP TABLE IF EXISTS $tmptableName;
                CREATE TABLE $tmptableName AS SELECT b.area, 0 as low_vegetation, 0 as high_vegetation, 0 as water, 0 as impervious, 0 as road, 1 as building, b.${ID_COLUMN_NAME}, b.${id_rsu} from ${entry.key}  as a,
                $final_polygonize as b where a.the_geom && b.the_geom and ST_intersects(a.the_geom, b.the_geom) AND a.${id_rsu} =b.${id_rsu}""".toString()
                        finalMerge.add("SELECT * FROM $tmptableName")
                    }
                }
                if (finalMerge) {
                    //Do not drop RSU table
                    tablesToMerge.remove("$rsuTable")
                    def allInfoTableName = postfix "allInfoTableName"
                    datasource """DROP TABLE IF EXISTS $allInfoTableName, $tmp_point_polygonize, $final_polygonize, $tmp_tables, $outputTableName;
                                      CREATE TABLE $allInfoTableName as ${finalMerge.join(' union all ')};
                                      CREATE INDEX ON $allInfoTableName (${ID_COLUMN_NAME});
                                      CREATE INDEX ON $allInfoTableName (${id_rsu});
                                      CREATE TABLE $outputTableName AS SELECT MAX(AREA) AS AREA, MAX(LOW_VEGETATION) AS LOW_VEGETATION,
                                                        MAX(HIGH_VEGETATION) AS HIGH_VEGETATION, MAX(WATER) AS WATER,
                                                        MAX(IMPERVIOUS) AS IMPERVIOUS, MAX(ROAD) AS ROAD, 
                                                        MAX(BUILDING) AS BUILDING, ${id_rsu} FROM $allInfoTableName GROUP BY ${ID_COLUMN_NAME}, ${id_rsu};
                                      DROP TABLE IF EXISTS ${tablesToMerge.keySet().join(' , ')}, ${allInfoTableName}, ${tmpTablesToDrop.join(",")}""".toString()
                }

            } else {
                error """Cannot compute the smallest geometries"""
            }
            [outputTableName: outputTableName]
        }
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
IProcess surfaceFractions() {
    return create {
        title "RSU surface fractions"
        id "surfaceFractions"
        inputs rsuTable: String, id_rsu: "id_rsu", spatialRelationsTable: String,
                superpositions: ["high_vegetation": ["water", "building", "low_vegetation", "road", "impervious"]],
                priorities: ["water", "building", "high_vegetation", "low_vegetation", "road", "impervious"],
                prefixName: String, datasource: JdbcDataSource
        outputs outputTableName: String
        run { rsuTable, id_rsu, spatialRelationsTable, superpositions, priorities,
              prefixName, datasource ->

            def BASE_TABLE_NAME ="RSU_SURFACE_FRACTIONS"
            def LAYERS = ["road", "water", "high_vegetation", "low_vegetation", "impervious", "building"]
            debug "Executing RSU surface fractions computation"

            // The name of the outputTableName is constructed
            def outputTableName = postfix( BASE_TABLE_NAME)

            // Create the indexes on each of the input tables
            datasource."$rsuTable"."$id_rsu".createIndex()
            datasource."$spatialRelationsTable"."$id_rsu".createIndex()

            // Need to set priority number for future sorting
            def prioritiesMap = [:]
            def i = 0
            priorities.each { val ->
                prioritiesMap[val] = i
                i++
            }

            def query = """DROP TABLE IF EXISTS $outputTableName; CREATE TABLE $outputTableName AS SELECT b.${id_rsu} """
            def end_query = """ FROM $spatialRelationsTable AS a RIGHT JOIN $rsuTable b 
                            ON a.${id_rsu}=b.${id_rsu} GROUP BY b.${id_rsu};"""

            if(superpositions){
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

            }else{
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
            //Cache the table name to re-use it
            cacheTableName(BASE_TABLE_NAME, outputTableName)

            [outputTableName: outputTableName]
        }
    }
}
