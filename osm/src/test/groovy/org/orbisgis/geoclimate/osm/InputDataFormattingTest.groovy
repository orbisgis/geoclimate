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
package org.orbisgis.geoclimate.osm

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators

import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.*

class InputDataFormattingTest {

    @TempDir
    static File folder

    static H2GIS h2GIS

    @BeforeAll
    static void loadDb() {
        h2GIS = H2GIS.open(folder.getAbsolutePath() + File.separator + "osm_inputDataFormattingTest;AUTO_SERVER=TRUE;")
    }

    @Test
    void formatHeightRoof() {
        def heightPattern = Pattern.compile("((?:\\d+\\/|(?:\\d+|^|\\s)\\.)?\\d+)\\s*([^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?(?:\\/[^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?)*)?", Pattern.CASE_INSENSITIVE)
        assertEquals(6, InputDataFormatting.getHeightRoof("6", heightPattern))
        assertEquals(6, InputDataFormatting.getHeightRoof("6 m", heightPattern))
        assertEquals(6, InputDataFormatting.getHeightRoof("6m", heightPattern))
        assertEquals(3.3528f, InputDataFormatting.getHeightRoof("11'", heightPattern))
        assertEquals(3.4544f, InputDataFormatting.getHeightRoof("11'4''", heightPattern))
        assertEquals(3.4544f, InputDataFormatting.getHeightRoof("11 '4''", heightPattern))
        assertEquals(3.4544f, InputDataFormatting.getHeightRoof("11 '4 ''", heightPattern))
        assertEquals(0.1016f, InputDataFormatting.getHeightRoof("4''", heightPattern))
    }

    @Test
    void formattingGISLayers() {
        def epsg = 2154
        Map extractData = OSM.InputDataLoading.createGISLayers(
                h2GIS, new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(), epsg)

        assertEquals 1038, h2GIS.getTable(extractData.building).rowCount
        assertEquals 211, h2GIS.getTable(extractData.road).rowCount
        assertEquals 44, h2GIS.getTable(extractData.rail).rowCount
        assertEquals 136, h2GIS.getTable(extractData.vegetation).rowCount
        assertEquals 10, h2GIS.getTable(extractData.water).rowCount
        assertEquals 47, h2GIS.getTable(extractData.impervious).rowCount
        assertEquals 11, h2GIS.getTable(extractData.urban_areas).rowCount
        assertEquals 0, h2GIS.getTable(extractData.coastline).rowCount

        //Buildings
        Map buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building)
        String building = buildingLayers.building
        assertNotNull h2GIS.getTable(building).save(new File(folder, "osm_building_formated.shp").absolutePath, true)
        assertEquals 1038, h2GIS.getTable(building).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where NB_LEV is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where NB_LEV<0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_WALL is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_WALL<0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_ROOF is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_ROOF<0".toString()).count == 0

        //Format urban areas
        String urbanAreas = OSM.InputDataFormatting.formatUrbanAreas(h2GIS, extractData.urban_areas)
        assertNotNull h2GIS.getTable(urbanAreas).save(new File(folder, "osm_urban_areas.shp").absolutePath, true)

        //Improve building type
        buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building, null, urbanAreas)
        String buiding_imp = buildingLayers.building
        assertNotNull h2GIS.getTable(buiding_imp).save(new File(folder, "osm_building_formated_type.shp").absolutePath, true)
        def rows = h2GIS.rows("select type from ${buiding_imp} where id_build=158 or id_build=982".toString())
        assertEquals(2, rows.size())
        assertTrue(rows.type == ['residential', 'residential'])

        rows = h2GIS.rows("select type from ${buiding_imp} where id_build=881 or id_build=484 or id_build=610".toString())

        assertEquals(3, rows.size())
        assertTrue(rows.type == ['industrial', 'industrial', 'industrial'])


        //Roads
        String road = OSM.InputDataFormatting.formatRoadLayer(h2GIS, extractData.road)
        assertNotNull h2GIS.getTable(road).save(new File(folder, "osm_road_formated.shp").absolutePath, true)
        assertEquals 145, h2GIS.getTable(road).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${road} where WIDTH is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${road} where WIDTH<=0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${road} where CROSSING IS NOT NULL".toString()).count == 7

        //Rails
        String rails = OSM.InputDataFormatting.formatRailsLayer(h2GIS, extractData.rail)
        assertNotNull h2GIS.getTable(rails).save(new File(folder, "osm_rails_formated.shp").absolutePath, true)
        assertEquals 41, h2GIS.getTable(rails).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${rails} where CROSSING IS NOT NULL".toString()).count == 8

        //Vegetation
        String vegetation = OSM.InputDataFormatting.formatVegetationLayer(h2GIS, extractData.vegetation)
        assertNotNull h2GIS.getTable(vegetation).save(new File(folder, "osm_vegetation_formated.shp").absolutePath, true)
        assertEquals 140, h2GIS.getTable(vegetation).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${vegetation} where type is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${vegetation} where HEIGHT_CLASS is null".toString()).count == 0

        //Hydrography
        String water = OSM.InputDataFormatting.formatWaterLayer(h2GIS, extractData.water)
        assertNotNull h2GIS.getTable(water).save(new File(folder, "osm_hydro_formated.shp").absolutePath, true)
        assertEquals 10, h2GIS.getTable(water).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${water} where type = 'sea'").count == 0

        //Impervious surfaces
        String impervious = OSM.InputDataFormatting.formatImperviousLayer(h2GIS, extractData.impervious)
        assertNotNull h2GIS.getTable(impervious).save(new File(folder, "osm_impervious_formated.shp").absolutePath, true)
        assertEquals 45, h2GIS.getTable(impervious).rowCount

        //Sea/Land mask
        String sea_land_mask = OSM.InputDataFormatting.formatSeaLandMask(h2GIS, extractData.coastline)
        assertEquals(0, h2GIS.getTable(sea_land_mask).getRowCount())

        //Build traffic data
        String road_traffic = Geoindicators.RoadIndicators.build_road_traffic(h2GIS, road)

        assertNotNull h2GIS.getTable(road_traffic).save(new File(folder, "osm_road_traffic.shp").absolutePath, true)
        assertEquals 138, h2GIS.getTable(road_traffic).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${road_traffic} where road_type is not null").count == 138

        def road_traffic_data = h2GIS.firstRow("select * from ${road_traffic} where road_type = 'Collecting roads' and direction =3 limit 1")

        def expectedFlow = [ROAD_TYPE   : 'Collecting roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 53, DAY_HV_HOUR: 6, DAY_LV_SPEED: 50,
                            DAY_HV_SPEED: 50, NIGHT_LV_HOUR: 12, NIGHT_HV_HOUR: 0, NIGHT_LV_SPEED: 50, NIGHT_HV_SPEED: 50,
                            EV_LV_HOUR  : 47, EV_HV_HOUR: 3, EV_LV_SPEED: 50, EV_HV_SPEED: 50]
        road_traffic_data.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic_data = h2GIS.firstRow("select * from ${road_traffic} where road_type = 'Dead-end roads' and direction = 1 limit 1")

        expectedFlow = [ROAD_TYPE   : 'Dead-end roads', SURFACE: null, PAVEMENT: 'NL05', DIRECTION: 1, DAY_LV_HOUR: 7, DAY_HV_HOUR: 0, DAY_LV_SPEED: 30,
                        DAY_HV_SPEED: 30, NIGHT_LV_HOUR: 2, NIGHT_HV_HOUR: 0, NIGHT_LV_SPEED: 30, NIGHT_HV_SPEED: 30,
                        EV_LV_HOUR  : 6, EV_HV_HOUR: 0, EV_LV_SPEED: 30, EV_HV_SPEED: 30]
        road_traffic_data.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic_data = h2GIS.firstRow("select * from ${road_traffic} where road_type = 'Service roads' limit 1")

        expectedFlow = [ROAD_TYPE   : 'Service roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 28, DAY_HV_HOUR: 1, DAY_LV_SPEED: 50,
                        DAY_HV_SPEED: 50, NIGHT_LV_HOUR: 6, NIGHT_HV_HOUR: 0, NIGHT_LV_SPEED: 50, NIGHT_HV_SPEED: 50,
                        EV_LV_HOUR  : 25, EV_HV_HOUR: 1, EV_LV_SPEED: 50, EV_HV_SPEED: 50]
        road_traffic_data.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic_data = h2GIS.firstRow("select * from ${road_traffic} where road_type = 'Small main roads' limit 1")

        expectedFlow = [ROAD_TYPE   : 'Small main roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 99, DAY_HV_HOUR: 18, DAY_LV_SPEED: 30,
                        DAY_HV_SPEED: 30, NIGHT_LV_HOUR: 24, NIGHT_HV_HOUR: 1, NIGHT_LV_SPEED: 30, NIGHT_HV_SPEED: 30,
                        EV_LV_HOUR  : 90, EV_HV_HOUR: 10, EV_LV_SPEED: 30, EV_HV_SPEED: 30]
        road_traffic_data.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic_data = h2GIS.firstRow("select * from ${road_traffic} where road_type = 'Main roads' limit 1")

        expectedFlow = [ROAD_TYPE   : 'Main roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 475, DAY_HV_HOUR: 119, DAY_LV_SPEED: 50,
                        DAY_HV_SPEED: 50, NIGHT_LV_HOUR: 80, NIGHT_HV_HOUR: 9, NIGHT_LV_SPEED: 50, NIGHT_HV_SPEED: 50,
                        EV_LV_HOUR  : 227, EV_HV_HOUR: 40, EV_LV_SPEED: 50, EV_HV_SPEED: 50]
        road_traffic_data.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }
        assertEquals(1, h2GIS.firstRow("select count(*) as count from ${road_traffic} where PAVEMENT = 'NL08'").count)

    }

    @Test
    void extractSeaLandTest() {
        def epsg = 32629
        def osmBbox = [52.08484801362273, -10.75003575696209, 52.001518013622736, -10.66670575696209]
        def geom = org.orbisgis.geoclimate.osmtools.OSMTools.Utilities.geometryFromNominatim(osmBbox)
        Map extractData = OSM.InputDataLoading.createGISLayers(h2GIS, new File(this.class.getResource("sea_land_data.osm").toURI()).getAbsolutePath(), epsg)

        def zoneEnvelopeTableName = "zone_envelope_sea_land"
        h2GIS.execute("""drop table if exists $zoneEnvelopeTableName;
         create table $zoneEnvelopeTableName as select st_transform(st_geomfromtext('$geom', ${geom.getSRID()}), $epsg) as the_geom""".toString())

        //Test coastline
        assertEquals(3, h2GIS.getTable(extractData.coastline).getRowCount())

        //Sea/Land mask
        String inputSeaLandTableName = OSM.InputDataFormatting.formatSeaLandMask(h2GIS, extractData.coastline, zoneEnvelopeTableName)
        assertEquals(2, h2GIS.getTable(inputSeaLandTableName).getRowCount())
        assertTrue h2GIS.firstRow("select count(*) as count from ${inputSeaLandTableName} where type='land'").count == 1
        h2GIS.getTable(inputSeaLandTableName).save(new File(folder, "osm_sea_land.fgb").getAbsolutePath(), true)
    }

    @Test
    void osmFileGISBuildingCheckHeight2() {
        //OSM URL https://www.openstreetmap.org/way/79083537
        //negative building:levels
        def epsg = 2154
        Map extractData = OSM.InputDataLoading.createGISLayers(h2GIS, new File(this.class.getResource("osmBuildingCheckHeigh.osm").toURI()).getAbsolutePath(), epsg)

        //Buildings
        Map buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building, extractData.zone_Envelope)
        String format = buildingLayers.building
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where 1=1").count > 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where NB_LEV is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where NB_LEV<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where HEIGHT_WALL is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where HEIGHT_WALL<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where HEIGHT_ROOF is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format} where HEIGHT_ROOF<0").count == 0
    }

    @Test
    void formattingGISBuildingLayer() {
        def epsg = 2154
        Map extractData = OSM.InputDataLoading.createGISLayers(h2GIS, new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(), epsg)

        assertEquals 1038, h2GIS.getTable(extractData.building).rowCount
        assertEquals 211, h2GIS.getTable(extractData.road).rowCount
        assertEquals 44, h2GIS.getTable(extractData.rail).rowCount
        assertEquals 136, h2GIS.getTable(extractData.vegetation).rowCount
        assertEquals 10, h2GIS.getTable(extractData.water).rowCount
        assertEquals 47, h2GIS.getTable(extractData.impervious).rowCount

        //Buildings with estimation state
        Map buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building)
        String buildingLayer = buildingLayers.building
        assertNotNull h2GIS.getTable(buildingLayer).save(new File(folder, "osm_building_formated.shp").absolutePath, true)
        assertEquals 1038, h2GIS.getTable(buildingLayer).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where NB_LEV is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where NB_LEV<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where NB_LEV=0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_WALL is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_WALL<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_ROOF is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_ROOF<0").count == 0
        assertEquals 1033, h2GIS.getTable(buildingLayers.building_estimated).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayers.building} join ${buildingLayers.building_estimated} using (id_build, id_source) where 1=1").count == 1033

        //Buildings without estimation state
        buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building)
        assertEquals 1038, h2GIS.getTable(buildingLayers.building).rowCount
        assertEquals 1033, h2GIS.getTable(buildingLayers.building_estimated).rowCount
    }


    @Disabled
    @Test
    //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {

        def directory = "/tmp/geoclimate"

        File file = new File(directory)
        if (!file.exists()) {
            file.mkdir()
        }

        def h2GIS = H2GIS.open("${file.absolutePath + File.separator}osm_gislayers;AUTO_SERVER=TRUE".toString())
        def zoneToExtract = "Marseille"

        Map extractData = OSM.InputDataLoading.extractAndCreateGISLayers(h2GIS, zoneToExtract)

        String formatedPlaceName = zoneToExtract.join("-").trim().split("\\s*(,|\\s)\\s*").join("_");

        if(!formatedPlaceName){
            formatedPlaceName=zoneToExtract
        }

        if (extractData.zone != null) {
            //Zone
            def epsg = h2GIS.getSpatialTable(extractData.zone).srid
            h2GIS.getTable(extractData.zone).save("${file.absolutePath + File.separator}osm_zone_${formatedPlaceName}.fgb", true)

            //Zone envelope
            h2GIS.getTable(extractData.zone_envelope).save("${file.absolutePath + File.separator}osm_zone_envelope_${formatedPlaceName}.fgb", true)

            //Urban Areas
            def inputUrbanAreas = OSM.InputDataFormatting.formatUrbanAreas(h2GIS,
                    extractData.urban_areas,extractData.zone)
            h2GIS.save(inputUrbanAreas,"${file.absolutePath + File.separator}osm_urban_areas_${formatedPlaceName}.fgb", true)

            println("Urban areas formatted")

            //Buildings
            h2GIS.save(extractData.building,"${file.absolutePath + File.separator}building_${formatedPlaceName}.fgb", true)
            def inputBuildings = OSM.InputDataFormatting.formatBuildingLayer(h2GIS,
                     extractData.building,extractData.zone,null)
            h2GIS.save(inputBuildings.building,"${file.absolutePath + File.separator}osm_building_${formatedPlaceName}.fgb", true)

            println("Building formatted")

            //Roads
            def inputRoadTableName = OSM.InputDataFormatting.formatRoadLayer( h2GIS,extractData.road, extractData.zone_envelope)
            h2GIS.save(inputRoadTableName,"${file.absolutePath + File.separator}osm_road_${formatedPlaceName}.fgb", true)

            println("Road formatted")

            //Rails
            def inputRailTableName = OSM.InputDataFormatting.formatRailsLayer( h2GIS,extractData.rail, extractData.zone_envelope)
            h2GIS.save(inputRailTableName,"${file.absolutePath + File.separator}osm_rail_${formatedPlaceName}.fgb", true)

            println("Rail formatted")

            //Vegetation
            def inputVegetationTableName = OSM.InputDataFormatting.formatVegetationLayer(
                    h2GIS,extractData.vegetation,extractData.zone_envelope)
            h2GIS.save(inputVegetationTableName,"${file.absolutePath + File.separator}osm_vegetation_${formatedPlaceName}.fgb", true)

            println("Vegetation formatted")

            //Hydrography
            def inputWaterTableName = OSM.InputDataFormatting.formatWaterLayer(h2GIS, extractData.water, extractData.zone_envelope)

            //Impervious
            String imperviousTable = OSM.InputDataFormatting.formatImperviousLayer(h2GIS, extractData.impervious,
                    extractData.zone_envelope)
            h2GIS.save(imperviousTable,"${file.absolutePath + File.separator}osm_impervious_${formatedPlaceName}.fgb", true)

            println("Impervious formatted")

            //Save coastlines to debug
            h2GIS.save(extractData.coastline,"${file.absolutePath + File.separator}osm_coastlines_${formatedPlaceName}.fgb", true)


            //Sea/Land mask
            def inputSeaLandTableName = OSM.InputDataFormatting.formatSeaLandMask(h2GIS, extractData.coastline,
                    extractData.zone_envelope, inputWaterTableName)
            h2GIS.save(inputSeaLandTableName,"${file.absolutePath + File.separator}osm_sea_land_${formatedPlaceName}.fgb", true)

            println("Sea land mask formatted")

            //Save it after sea/land mask because the water table can be modified
            h2GIS.save(inputWaterTableName,"${file.absolutePath + File.separator}osm_water_${formatedPlaceName}.fgb", true)

        } else {
            assertTrue(false)
        }
    }

    //This test is used for debug purpose
    @Test
    @Disabled
    void createGISFormatLayersTestIntegration() {
        Map gISLayers = OSM.InputDataLoading.createGISLayers(h2GIS, "/tmp/map.osm", 2154)

        //Format Roads
        def road = OSM.InputDataFormatting.formatRoadLayer(h2GIS, gISLayers.road)
        h2GIS.getTable(road).save("/tmp/formated_osm_road.shp", true)
    }
}
