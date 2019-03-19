package org.orbisgis

import org.orbisgis.datamanager.h2gis.H2GIS
import static org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class BuildingIndicatorsTests {

    @Test
    void testGeometryProperties() {

        def h2GIS = H2GIS.open([databaseName: './target/buildingdb'])
        h2GIS.execute("""
                DROP TABLE IF EXISTS spatial_table, geom_properties;
                CREATE TABLE spatial_table (id int, the_geom point);
                INSERT INTO spatial_table VALUES (1, 'POINT(10 10)'::GEOMETRY), (2, 'POINT(1 1)'::GEOMETRY);
        """)
       def  p =  Geoclimate.BuildingIndicators.geometryProperties()
       p.execute([inputTableName: "spatial_table", inputFields:["the_geom"], operations:["st_issimple","st_area"], outputTableName : "geom_properties",datasource:h2GIS])
        assert p.results.outputTableName == "geom_properties"

        h2GIS.getTable("geom_properties").eachRow {
            row -> println("$row.the_geom  : $row.issimple  :  $row.area")
        }
    }
}
