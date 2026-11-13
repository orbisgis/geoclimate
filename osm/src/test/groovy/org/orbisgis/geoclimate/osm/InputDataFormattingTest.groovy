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

import groovy.json.JsonSlurper
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
        assertEquals 358, h2GIS.getTable(extractData.road).rowCount
        assertEquals 44, h2GIS.getTable(extractData.rail).rowCount
        assertEquals 136, h2GIS.getTable(extractData.vegetation).rowCount
        assertEquals 8, h2GIS.getTable(extractData.water).rowCount
        assertEquals 45, h2GIS.getTable(extractData.impervious).rowCount
        assertEquals 11, h2GIS.getTable(extractData.urban_areas).rowCount
        assertEquals 0, h2GIS.getTable(extractData.coastline).rowCount

        //Buildings
        Map buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building)
        String building = buildingLayers.building
        assertNotNull h2GIS.getTable(building).save(new File(folder, "osm_building_formated.fgb").absolutePath, true)
        assertEquals 1036, h2GIS.getTable(building).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where NB_LEV is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where NB_LEV<0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_WALL is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_WALL<0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_ROOF is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${building} where HEIGHT_ROOF<0".toString()).count == 0

        //Format urban areas
        String urbanAreas = OSM.InputDataFormatting.formatUrbanAreas(h2GIS, extractData.urban_areas)
        assertNotNull h2GIS.getTable(urbanAreas).save(new File(folder, "osm_urban_areas.fgb").absolutePath, true)

        //Improve building type
        buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building, null, urbanAreas)
        String buiding_imp = buildingLayers.building
        assertNotNull h2GIS.getTable(buiding_imp).save(new File(folder, "osm_building_formated_type.fgb").absolutePath, true)
        def rows = h2GIS.rows("select type from ${buiding_imp} where id_build=158 or id_build=982".toString())
        assertEquals(2, rows.size())
        assertTrue(rows.type == ['residential', 'undefined'])

        rows = h2GIS.rows("select type from ${buiding_imp} where id_build=881 or id_build=484 or id_build=610".toString())

        assertEquals(3, rows.size())
        assertTrue(rows.type == ['industrial', 'undefined', 'industrial'])


        //Roads
        String road = OSM.InputDataFormatting.formatRoadLayer(h2GIS, extractData.road)
        assertNotNull h2GIS.getTable(road).save(new File(folder, "osm_road_formated.fgb").absolutePath, true)
        assertEquals 290, h2GIS.getTable(road).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${road} where WIDTH is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${road} where WIDTH<=0".toString()).count == 0
        assertEquals(12, h2GIS.firstRow("select count(*) as count from ${road} where CROSSING IS NOT NULL".toString()).count)
        assertEquals(17,h2GIS.firstRow("select count(*) as count from ${road} where LANES IS NOT NULL".toString()).count)

        //Rails
        String rails = OSM.InputDataFormatting.formatRailsLayer(h2GIS, extractData.rail)
        assertNotNull h2GIS.getTable(rails).save(new File(folder, "osm_rails_formated.fgb").absolutePath, true)
        assertEquals 41, h2GIS.getTable(rails).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${rails} where CROSSING IS NOT NULL".toString()).count == 8

        //Vegetation
        String vegetation = OSM.InputDataFormatting.formatVegetationLayer(h2GIS, extractData.vegetation)
        assertNotNull h2GIS.getTable(vegetation).save(new File(folder, "osm_vegetation_formated.fgb").absolutePath, true)
        assertEquals 137, h2GIS.getTable(vegetation).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${vegetation} where type is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${vegetation} where HEIGHT_CLASS is null".toString()).count == 0

        //Hydrography
        String water = OSM.InputDataFormatting.formatWaterLayer(h2GIS, extractData.water)
        assertNotNull h2GIS.getTable(water).save(new File(folder, "osm_hydro_formated.fgb").absolutePath, true)
        def waterCount = h2GIS.getTable(water).rowCount
        assertEquals 8, waterCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${water} where INTERMITTENT is not null").count == waterCount

        //Impervious surfaces
        String impervious = OSM.InputDataFormatting.formatImperviousLayer(h2GIS, extractData.impervious)
        assertNotNull h2GIS.getTable(impervious).save(new File(folder, "osm_impervious_formated.fgb").absolutePath, true)
        assertEquals 37, h2GIS.getTable(impervious).rowCount

        //Sea/Land mask
        String sea_land_mask = OSM.InputDataFormatting.formatSeaLandMask(h2GIS, extractData.coastline)
        assertEquals(0, h2GIS.getTable(sea_land_mask).getRowCount())

        //Build traffic data
        String road_traffic = Geoindicators.RoadIndicators.build_road_traffic(h2GIS, road)

        assertNotNull h2GIS.getTable(road_traffic).save(new File(folder, "osm_road_traffic.fgb").absolutePath, true)
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
        assertEquals 358, h2GIS.getTable(extractData.road).rowCount
        assertEquals 44, h2GIS.getTable(extractData.rail).rowCount
        assertEquals 136, h2GIS.getTable(extractData.vegetation).rowCount
        assertEquals 8, h2GIS.getTable(extractData.water).rowCount
        assertEquals 45, h2GIS.getTable(extractData.impervious).rowCount

        //Buildings with estimation state
        Map buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building)
        String buildingLayer = buildingLayers.building
        assertNotNull h2GIS.getTable(buildingLayer).save(new File(folder, "osm_building_formated.fgb").absolutePath, true)
        assertEquals 1036, h2GIS.getTable(buildingLayer).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where NB_LEV is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where NB_LEV<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where NB_LEV=0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_WALL is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_WALL<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_ROOF is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${buildingLayer} where HEIGHT_ROOF<0").count == 0
        assertEquals 1031, h2GIS.getTable(buildingLayers.building_updated).rowCount
        assertEquals(1031, h2GIS.firstRow("select count(*) as count from ${buildingLayers.building} join ${buildingLayers.building_updated} using (id_build) where 1=1").count)

        //Buildings without estimation state
        buildingLayers = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building)
        assertEquals 1036, h2GIS.getTable(buildingLayers.building).rowCount
        assertEquals 1031, h2GIS.getTable(buildingLayers.building_updated).rowCount
    }


    @Disabled
    @Test
    //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {
        def directory = "/tmp/geoclimate"
        def zoneToExtract = "Redon"
        //zoneToExtract = [48.889780, 2.226381, 48.891529, 2.230699]
        //zoneToExtract = [51.328681, 1.195128, 51.331121, 1.199162]

        // Layers to format : keep only the ones you want,
        // or override them with the JVM option -Dlayers=building,road
        def allLayers = ["urban_areas", "building", "road", "rail", "vegetation",
                         "water", "impervious", "sea_land_mask"]
        def layers = (System.getProperty("layers")?.split(",")*.trim() ?: allLayers) as Set
        def unknown = layers - allLayers
        assertTrue(unknown.isEmpty(), "Unknown layers : $unknown, available layers : $allLayers".toString())
        // The sea/land mask needs the formatted water table
        if ("sea_land_mask" in layers) {
            layers << "water"
        }

        File folder = new File(directory)
        folder.mkdirs()
        def h2GIS = H2GIS.open("${folder.absolutePath + File.separator}osm_gislayers;AUTO_SERVER=TRUE".toString())

        Map extractData = OSM.InputDataLoading.extractAndCreateGISLayers(h2GIS, zoneToExtract)
        assertNotNull(extractData.zone)

        //Zone
        saveTable(h2GIS, extractData.zone, folder, "zone")
        saveTable(h2GIS, extractData.zone_envelope, folder, "zone_envelope")

        //Urban Areas
        String urbanAreas = ""
        if ("urban_areas" in layers) {
            long start = System.currentTimeMillis()
            urbanAreas = OSM.InputDataFormatting.formatUrbanAreas(h2GIS, extractData.urban_areas, extractData.zone)
            logFormat(h2GIS, "Urban areas", start, urbanAreas)
            saveTable(h2GIS, urbanAreas, folder, "urban_areas")
        }

        //Buildings
        if ("building" in layers) {
            long start = System.currentTimeMillis()
            String building = OSM.InputDataFormatting.formatBuildingLayer(h2GIS, extractData.building,
                    extractData.zone, urbanAreas).building
            logFormat(h2GIS, "Building", start, building)
            saveTable(h2GIS, building, folder, "building")
        }

        //Roads
        if ("road" in layers) {
            long start = System.currentTimeMillis()
            String road = OSM.InputDataFormatting.formatRoadLayer(h2GIS, extractData.road, extractData.zone_envelope)
            logFormat(h2GIS, "Road", start, road)
            saveTable(h2GIS, road, folder, "road")
        }

        //Rails
        if ("rail" in layers) {
            long start = System.currentTimeMillis()
            String rail = OSM.InputDataFormatting.formatRailsLayer(h2GIS, extractData.rail, extractData.zone_envelope)
            logFormat(h2GIS, "Rail", start, rail)
            saveTable(h2GIS, rail, folder, "rail")
        }

        //Vegetation
        if ("vegetation" in layers) {
            long start = System.currentTimeMillis()
            String vegetation = OSM.InputDataFormatting.formatVegetationLayer(h2GIS, extractData.vegetation,
                    extractData.zone_envelope)
            logFormat(h2GIS, "Vegetation", start, vegetation)
            saveTable(h2GIS, vegetation, folder, "vegetation")
        }

        //Hydrography
        String water = null
        if ("water" in layers) {
            saveTable(h2GIS, extractData.water, folder, "water_osm")
            long start = System.currentTimeMillis()
            water = OSM.InputDataFormatting.formatWaterLayer(h2GIS, extractData.water, extractData.zone_envelope)
            logFormat(h2GIS, "Water", start, water)
        }

        //Impervious
        if ("impervious" in layers) {
            long start = System.currentTimeMillis()
            String impervious = OSM.InputDataFormatting.formatImperviousLayer(h2GIS, extractData.impervious,
                    extractData.zone_envelope)
            logFormat(h2GIS, "Impervious", start, impervious)
            saveTable(h2GIS, impervious, folder, "impervious")
        }

        //Sea/Land mask
        if ("sea_land_mask" in layers) {
            //Save coastlines to debug
            saveTable(h2GIS, extractData.coastline, folder, "coastlines")
            long start = System.currentTimeMillis()
            String seaLand = OSM.InputDataFormatting.formatSeaLandMask(h2GIS, extractData.coastline,
                    extractData.zone_envelope, water)
            logFormat(h2GIS, "Sea land mask", start, seaLand)
            saveTable(h2GIS, seaLand, folder, "sea_land_mask")
        }

        //Save it after sea/land mask because the water table can be modified
        if (water) {
            saveTable(h2GIS, water, folder, "water")
        }
    }

    /**
     * Print the time
     */
    private String logFormat(H2GIS h2GIS, String name, long start, String table) {
        def count = table ? h2GIS.getRowCount(table) : 0
        println "${name} formatted in ${(System.currentTimeMillis() - start) / 1000} s ($count rows)"
        return table
    }

    /**
     * Save a table in a FlatGeobuf file in the given folder
     */
    private void saveTable(H2GIS h2GIS, String table, File folder, String fileName) {
        h2GIS.save(table, new File(folder, "${fileName}.fgb").absolutePath, true)
    }

    //This test is used for debug purpose
    @Test
    @Disabled
    void createGISFormatLayersTestIntegration() {
        Map gISLayers = OSM.InputDataLoading.createGISLayers(h2GIS, "/tmp/map.osm", 2154)
        //Format Roads
        def road = OSM.InputDataFormatting.formatRoadLayer(h2GIS, gISLayers.road)
        h2GIS.getTable(road).save("/tmp/formated_osm_road.fgb", true)
    }

    @Test
    void parseFloat() {
        def heightPattern = Pattern.compile("((?:\\d+\\/|(?:\\d+|^|\\s)\\.)?\\d+)\\s*([^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?(?:\\/[^\\s\\d+\\-.,:;^\\/]+(?:\\^\\d+(?:\$|(?=[\\s:;\\/])))?)*)?", Pattern.CASE_INSENSITIVE)
        assertEquals(0, InputDataFormatting.getHeightRoof("II OSK 1559/12", heightPattern))
    }

    @Test
    void mappingBuildingModelTest() {
        JsonSlurper jsonSlurper = new JsonSlurper()
        Map buildingParameters = jsonSlurper.parse(InputDataFormatting.class.getResourceAsStream("buildingParams.json")) as Map

        HashSet allOSMTypes = new HashSet()
        HashSet allOSMUses = new HashSet()
        Map mappingTypeAndUse = buildingParameters.type as Map
        mappingTypeAndUse.each { it ->
            def type_use = it.key.split(":")
            if (type_use.size() == 2) {
                allOSMTypes.add(type_use[0].trim())
                def main_use = type_use[1].trim()
                allOSMUses.add(main_use)
            } else {
                def type_and_main_use = it.key.trim()
                allOSMUses.add(type_and_main_use)
                allOSMTypes.add(type_and_main_use)
            }
        }

        Map types_uses_for_model = Geoindicators.WorkflowGeoIndicators.getHeightModelTypesMapping()

        Map typesToMap = types_uses_for_model.types as Map
        Map usesToMap = types_uses_for_model.uses as Map
        HashSet<String> allModelTypes = new HashSet<>()
        typesToMap.each {it->
            allModelTypes.add(it.key)
            List inputTypes = it.value
            if(inputTypes){
                allModelTypes.addAll(inputTypes)
            }
        }

        HashSet<String> allModelUses = new HashSet<>()
        usesToMap.each {it->
            allModelUses.add(it.key)
            List inputUses = it.value
            if(inputUses){
                allModelUses.addAll(inputUses)
            }
        }

        assertTrue(!(allOSMTypes - allModelTypes))
        assertTrue(!(allOSMUses - allModelUses))

    }

    @Test
    void mappingUrbanAreasModelTest() {
        JsonSlurper jsonSlurper = new JsonSlurper()
        Map buildingParameters = jsonSlurper.parse(InputDataFormatting.class.getResourceAsStream("urbanAreasParams.json")) as Map

        HashSet allOSMTypes = new HashSet()
        HashSet allOSMUses = new HashSet()
        Map mappingTypeAndUse = buildingParameters.type as Map
        mappingTypeAndUse.each { it ->
            def type_use = it.key.split(":")
            if (type_use.size() == 2) {
                allOSMTypes.add(type_use[0].trim())
                def main_use = type_use[1].trim()
                allOSMUses.add(main_use)
            } else {
                def type_and_main_use = it.key.trim()
                allOSMUses.add(type_and_main_use)
                allOSMTypes.add(type_and_main_use)
            }
        }

        Map types_uses_for_model = Geoindicators.WorkflowGeoIndicators.getHeightModelTypesMapping()

        Map typesToMap = types_uses_for_model.types as Map
        Map usesToMap = types_uses_for_model.uses as Map
        HashSet<String> allModelTypes = new HashSet<>()
        typesToMap.each {it->
            allModelTypes.add(it.key)
            List inputTypes = it.value
            if(inputTypes){
                allModelTypes.addAll(inputTypes)
            }
        }

        HashSet<String> allModelUses = new HashSet<>()
        usesToMap.each {it->
            allModelUses.add(it.key)
            List inputUses = it.value
            if(inputUses){
                allModelUses.addAll(inputUses)
            }
        }
        assertTrue(!(allOSMTypes - allModelTypes))
        assertTrue(!(allOSMUses - allModelUses))
    }
}
