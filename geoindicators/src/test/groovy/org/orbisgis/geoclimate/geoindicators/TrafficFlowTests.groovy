package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.geoclimate.Geoindicators

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.orbisgis.data.H2GIS.open

class TrafficFlowTests {

    @TempDir
    static File folder
    private static def h2GIS

    @BeforeAll
    static void beforeAll() {
        h2GIS = open(folder.getAbsolutePath()+File.separator+"trafficFlowTests;AUTO_SERVER=TRUE")
    }

    @Test
    void roadTrafficTest() {
        h2GIS.execute """drop table if exists road_test ;
        CREATE TABLE road_test (id_road int, the_geom geometry, width float, zindex int, crossing varchar(30), type varchar(30), maxspeed integer, direction integer, surface varchar);
        INSERT INTO road_test VALUES (1, 'LINESTRING(120 60, 120 -10)'::GEOMETRY, 10, 0, null,  'motorway', 110, 1, null),
        (2, 'LINESTRING (86 19, 170 20)'::GEOMETRY, 5, 0, null,  'highway', 50, 3, null),
        (3, 'LINESTRING (93 53, 149 54, 145 -5)'::GEOMETRY, 5, 0, null,  'highway', 50, 1, null),
        (4, 'LINESTRING (85 60, 85 -1, 155 1, 148 54, 92 50, 96 -12, 119 -11, 117 -4, 78 -5)'::GEOMETRY, 10, 0, null,  'highway', 90, 1, null),
        (5, 'LINESTRING (20 100, 25 100, 25 120, 20 120)'::GEOMETRY, 6, 0, null,  'highway',50, 1, null),
        (6, 'LINESTRING (50 105, 47 99)'::GEOMETRY, 6, -1, null,  'highway', 50, 3, null);"""
        def traffic = Geoindicators.RoadIndicators.build_road_traffic()
        traffic.execute([
                datasource : h2GIS,
                inputTableName: "ROAD_TEST",
                epsg: 2154])

        assertEquals 6 , h2GIS.getTable(traffic.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${traffic.results.outputTableName} where road_type is not null").count==6
    }
}
