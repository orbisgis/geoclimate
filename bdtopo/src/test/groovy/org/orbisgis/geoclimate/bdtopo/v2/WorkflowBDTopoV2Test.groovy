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
package org.orbisgis.geoclimate.bdtopo.v2

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.bdtopo.BDTopo
import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class WorkflowBDTopoV2Test extends WorkflowAbstractTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    public static File folder

    @Override
    ArrayList getFileNames() {
        return ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "TRONCON_VOIE_FERREE",
                "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
                "SURFACE_ROUTE", "SURFACE_ACTIVITE"]
    }


    @Override
    int getVersion() {
        return 2
    }

    @Override
    String getDBFolderPath(){
        return folder.absolutePath
    }


    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}"
    }

    @Override
    String getInseeCode() {
        return "12174"
    }

    @Test
    void testFormatData() {
        String dataFolder = getDataFolderPath()
        String outputFolder = getDBFolderPath()+"bdtopo_"+getInseeCode()
        def bdTopoParameters = [
                "description" : "Full workflow configuration file",
                "geoclimatedb": [
                        "folder": outputFolder,
                        "name"  : "testFormatedData_${getInseeCode()};AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [getInseeCode()]],
                "output"      : [
                        "folder": ["path": outputFolder]],
                "parameters"  :
                        ["distance": 0]
        ]

        Map process = BDTopo.workflow(bdTopoParameters, getVersion())
        assertNotNull(process)
        Map resultFiles = getResultFiles(outputFolder)
        H2GIS h2GIS = H2GIS.open(outputFolder + File.separator + "testFormatedData_${getInseeCode()};AUTO_SERVER=TRUE")
        resultFiles.each {
            h2GIS.load(it.value, it.key, true)
        }
        //Check the data
        //Building
        int count = h2GIS.getRowCount("building")
        List cols = ["ID_BUILD", "ID_SOURCE", "HEIGHT_WALL", "HEIGHT_ROOF", "NB_LEV", "TYPE", "MAIN_USE", "ROOF_SHAPE", "ZINDEX", "THE_GEOM"]
        assertTrue h2GIS.getColumnNames("building").intersect(cols).size() == cols.size()
        assertEquals(0, h2GIS.firstRow("SELECT COUNT(*) as count FROM building where HEIGHT_WALL = 0 OR HEIGHT_ROOF = 0 OR NB_LEV = 0").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM building where TYPE IS NOT NULL OR  MAIN_USE IS NOT NULL").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM building where ZINDEX BETWEEN -4 AND 4").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM building where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)

        //Road
        count = h2GIS.getRowCount("road")
        cols = ["ID_ROAD", "ID_SOURCE", "WIDTH", "TYPE", "SURFACE", "SIDEWALK", "CROSSING", "MAXSPEED", "DIRECTION", "ZINDEX", "THE_GEOM"]
        assertTrue h2GIS.getColumnNames("road").intersect(cols).size() == cols.size()
        assertEquals(0, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where WIDTH = 0 ").count)
        assertEquals(24, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where crossing in ('bridge', 'crossing')").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where TYPE IS NOT NULL OR SIDEWALK is not null").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where MAXSPEED !=0 OR MAXSPEED>= -1").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where DIRECTION !=0 OR DIRECTION>= -1").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where ZINDEX BETWEEN -4 AND 4").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM road where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)


        //Rail
        count = h2GIS.getRowCount("rail")
        cols = ["THE_GEOM", "ID_RAIL",
                "ID_SOURCE", "TYPE", "CROSSING", "ZINDEX", "WIDTH", "USAGE"]
        assertTrue h2GIS.getColumnNames("rail").intersect(cols).size() == cols.size()
        assertEquals(0, h2GIS.firstRow("SELECT COUNT(*) as count FROM rail where WIDTH = 0 ").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM rail where type is not null").count)
        assertEquals(2, h2GIS.firstRow("SELECT COUNT(*) as count FROM rail where crossing is not null").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM rail where ZINDEX BETWEEN -4 AND 4").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM rail where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)


        //Vegetation
        count = h2GIS.getRowCount("vegetation")
        cols = ["THE_GEOM", "ID_VEGET", "ID_SOURCE", "TYPE", "HEIGHT_CLASS", "ZINDEX"]
        assertTrue h2GIS.getColumnNames("vegetation").intersect(cols).size() == cols.size()
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM vegetation where type is not null").count)
        assertEquals(665, h2GIS.firstRow("SELECT COUNT(*) as count FROM vegetation where height_class ='high'").count)
        assertEquals(2, h2GIS.firstRow("SELECT COUNT(*) as count FROM vegetation where height_class = 'low'").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM vegetation where ZINDEX BETWEEN 0 AND 1 ").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM vegetation where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)


        //Water
        count = h2GIS.getRowCount("water")
        cols = ["THE_GEOM", "ID_WATER", "ID_SOURCE", "TYPE", "ZINDEX"]
        assertTrue h2GIS.getColumnNames("water").intersect(cols).size() == cols.size()
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM water where type is not null").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM water where ZINDEX BETWEEN 0 AND 1 ").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM water where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)


        //Impervious areas
        count = h2GIS.getRowCount("impervious")
        cols = ["THE_GEOM", "ID_IMPERVIOUS", "TYPE"]
        assertTrue h2GIS.getColumnNames("impervious").intersect(cols).size() == cols.size()
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM impervious where type is not null").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM impervious where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)


        //Urban areas
        count = h2GIS.getRowCount("urban_areas")
        cols = ["THE_GEOM", "ID_URBAN", "ID_SOURCE", "TYPE"]
        assertTrue h2GIS.getColumnNames("urban_areas").intersect(cols).size() == cols.size()
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM urban_areas where type is not null").count)
        assertEquals(count, h2GIS.firstRow("SELECT COUNT(*) as count FROM urban_areas where ST_ISEMPTY(THE_GEOM)=false OR THE_GEOM IS NOT NULL").count)

        h2GIS.deleteClose()
    }

    @Test
    void testClip2() {
        String dataFolder = getDataFolderPath()
        def bbox = [ 6359905.15, 663566.1107794361, 6360305.15, 663966.1107794361 ]
        def bdTopoParameters = [
                "description" : "Clip option test",
                "geoclimatedb": [
                        "folder": getDBFolderPath(),
                        "name"  : "testclip2;AUTO_SERVER=TRUE",
                        "delete": false
                ],
                "input"       : [
                        "folder"   : dataFolder,
                        "locations": [bbox]],
                "output"      : [
                        "folder": ["path": getDBFolderPath()],
                        "domain":"zone_extended"],
                "parameters"  :
                        ["distance"        : 100,
                         rsu_indicators    : [
                                 "indicatorUse": ["LCZ", "TEB", "UTRF"]]
                         ,
                         "grid_indicators" : [
                                 "x_size"    : 100,
                                 "y_size"    : 100,
                                 "indicators": ["LAND_TYPE_FRACTION"]
                                 ,"domain":"zone_extended"
                         ],
                         "road_traffic"    : true,
                         "noise_indicators": [
                                 "ground_acoustic": true
                         ]]
        ]
        BDTopo.workflow(bdTopoParameters, getVersion())

        H2GIS h2gis = H2GIS.open("${getDBFolderPath() + File.separator}testclip2;AUTO_SERVER=TRUE")
        def building = "building"
        def zone = "zone"
        h2gis.load(getDBFolderPath() + File.separator + "bdtopo_" + getVersion() + "_" + bbox.join("_") + File.separator +"building.fgb", building, true)
        h2gis.load(getDBFolderPath() + File.separator + "bdtopo_" + getVersion() + "_" + bbox.join("_") + File.separator +"zone.fgb", zone, true)

        assertTrue h2gis.firstRow("select count(*) as count from $building where HEIGHT_WALL>0 and HEIGHT_ROOF>0").count > 0
        h2gis.execute("""DROP TABLE IF EXISTS building_out;
        CREATE TABLE building_out as SELECT a.* FROM  $building a LEFT JOIN $zone b
                ON a.the_geom && b.the_geom and ST_INTERSECTS(a.the_geom, b.the_geom)
                WHERE b.the_geom IS NULL;""")
        assertEquals(10, h2gis.getRowCount("building_out"))
        def grid_indicators = "grid_indicators"
        h2gis.load(getDBFolderPath() + File.separator + "bdtopo_" + getVersion() + "_" + bbox.join("_") + File.separator +"grid_indicators.fgb", grid_indicators, true)
        h2gis.execute("""DROP TABLE IF EXISTS grid_out;
        CREATE TABLE grid_out as SELECT a.* FROM  $grid_indicators a LEFT JOIN $zone b
                ON a.the_geom && b.the_geom and ST_INTERSECTS(st_centroid(a.the_geom), b.the_geom)
                WHERE b.the_geom IS NULL;""")
        assertEquals(20, h2gis.getRowCount("grid_out"))

        h2gis.dropTable("building_out", "grid_out")
        h2gis.deleteClose()
    }
}