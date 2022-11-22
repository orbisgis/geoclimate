package org.orbisgis.geoclimate.osm

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.data.H2GIS
import org.orbisgis.process.api.IProcess

import java.util.regex.Pattern

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

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
        IProcess extractData = OSM.InputDataLoading.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg       : epsg])

        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        assertEquals 211, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 47, h2GIS.getTable(extractData.results.imperviousTableName).rowCount
        assertEquals 6, h2GIS.getTable(extractData.results.urbanAreasTableName).rowCount
        assertEquals 0, h2GIS.getTable(extractData.results.coastlineTableName).rowCount

        //Buildings
        IProcess format = OSM.InputDataFormatting.formatBuildingLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg          : epsg,
                jsonFilename  : null])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_building_formated.shp").absolutePath, true)
        assertEquals 1038, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0".toString()).count == 0

        //Format urban areas
        format = OSM.InputDataFormatting.formatUrbanAreas()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.urbanAreasTableName,
                epsg          : epsg])
        def urbanAreas = format.results.outputTableName
        assertNotNull h2GIS.getTable(urbanAreas).save(new File(folder, "osm_urban_areas.shp").absolutePath, true)


        //Improve building type
        format = OSM.InputDataFormatting.formatBuildingLayer()
        format.execute([
                datasource         : h2GIS,
                inputTableName     : extractData.results.buildingTableName,
                epsg               : epsg,
                urbanAreasTableName: urbanAreas])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_building_formated_type.shp").absolutePath, true)
        def rows = h2GIS.rows("select type from ${format.results.outputTableName} where id_build=158 or id_build=982".toString())
        assertEquals(2, rows.size())
        assertTrue(rows.type == ['residential', 'residential'])

        rows = h2GIS.rows("select type from ${format.results.outputTableName} where id_build=881 or id_build=484 or id_build=610".toString())
        assertEquals(3, rows.size())
        assertTrue(rows.type == ['light_industry', 'light_industry', 'light_industry'])


        //Roads
        format = OSM.InputDataFormatting.formatRoadLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.roadTableName,
                epsg          : epsg,
                jsonFilename  : null])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_road_formated.shp").absolutePath, true)
        assertEquals 146, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH<=0".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where CROSSING IS NOT NULL".toString()).count == 7

        def formatedRoadTable = format.results.outputTableName
        //Rails
        format = OSM.InputDataFormatting.formatRailsLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.railTableName,
                epsg          : epsg,
                jsonFilename  : null])

        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_rails_formated.shp").absolutePath, true)
        assertEquals 41, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where CROSSING IS NOT NULL".toString()).count == 8


        //Vegetation
        format = OSM.InputDataFormatting.formatVegetationLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.vegetationTableName,
                epsg          : epsg,
                jsonFilename  : null
        ])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_vegetation_formated.shp").absolutePath, true)
        assertEquals 140, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where type is null".toString()).count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_CLASS is null".toString()).count == 0

        //Hydrography
        format = OSM.InputDataFormatting.formatHydroLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.hydroTableName,
                epsg          : epsg])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_hydro_formated.shp").absolutePath, true)
        assertEquals 10, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where type = 'sea'").count == 0

        //Impervious surfaces
        format = OSM.InputDataFormatting.formatImperviousLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.imperviousTableName,
                epsg          : epsg])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_impervious_formated.shp").absolutePath, true)
        assertEquals 47, h2GIS.getTable(format.results.outputTableName).rowCount

        //Sea/Land mask
        format = OSM.InputDataFormatting.formatSeaLandMask()
        format.execute([
                datasource                : h2GIS,
                inputTableName            : extractData.results.outputCoastlineTableName,
                inputZoneEnvelopeTableName: "",
                epsg                      : epsg])
        assertEquals(0, h2GIS.getTable(format.results.outputTableName).getRowCount())

        //Build traffic data
        format = Geoindicators.RoadIndicators.build_road_traffic()
        format.execute([
                datasource    : h2GIS,
                inputTableName: formatedRoadTable,
                epsg          : epsg])

        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_road_traffic.shp").absolutePath, true)
        assertEquals 138, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where road_type is not null").count == 138

        def road_traffic = h2GIS.firstRow("select * from ${format.results.outputTableName} where road_type = 'Collecting roads' and direction =3 limit 1")

        def expectedFlow = [ROAD_TYPE   : 'Collecting roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 53, DAY_HV_HOUR: 6, DAY_LV_SPEED: 50,
                            DAY_HV_SPEED: 50, NIGHT_LV_HOUR: 12, NIGHT_HV_HOUR: 0, NIGHT_LV_SPEED: 50, NIGHT_HV_SPEED: 50,
                            EV_LV_HOUR  : 47, EV_HV_HOUR: 3, EV_LV_SPEED: 50, EV_HV_SPEED: 50]
        road_traffic.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic = h2GIS.firstRow("select * from ${format.results.outputTableName} where road_type = 'Dead-end roads' and direction = 1 limit 1")

        expectedFlow = [ROAD_TYPE   : 'Dead-end roads', SURFACE: null, PAVEMENT: 'NL05', DIRECTION: 1, DAY_LV_HOUR: 7, DAY_HV_HOUR: 0, DAY_LV_SPEED: 30,
                        DAY_HV_SPEED: 30, NIGHT_LV_HOUR: 2, NIGHT_HV_HOUR: 0, NIGHT_LV_SPEED: 30, NIGHT_HV_SPEED: 30,
                        EV_LV_HOUR  : 6, EV_HV_HOUR: 0, EV_LV_SPEED: 30, EV_HV_SPEED: 30]
        road_traffic.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic = h2GIS.firstRow("select * from ${format.results.outputTableName} where road_type = 'Service roads' limit 1")

        expectedFlow = [ROAD_TYPE   : 'Service roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 28, DAY_HV_HOUR: 1, DAY_LV_SPEED: 50,
                        DAY_HV_SPEED: 50, NIGHT_LV_HOUR: 6, NIGHT_HV_HOUR: 0, NIGHT_LV_SPEED: 50, NIGHT_HV_SPEED: 50,
                        EV_LV_HOUR  : 25, EV_HV_HOUR: 1, EV_LV_SPEED: 50, EV_HV_SPEED: 50]
        road_traffic.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic = h2GIS.firstRow("select * from ${format.results.outputTableName} where road_type = 'Small main roads' limit 1")

        expectedFlow = [ROAD_TYPE   : 'Small main roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 99, DAY_HV_HOUR: 18, DAY_LV_SPEED: 30,
                        DAY_HV_SPEED: 30, NIGHT_LV_HOUR: 24, NIGHT_HV_HOUR: 1, NIGHT_LV_SPEED: 30, NIGHT_HV_SPEED: 30,
                        EV_LV_HOUR  : 90, EV_HV_HOUR: 10, EV_LV_SPEED: 30, EV_HV_SPEED: 30]
        road_traffic.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }

        road_traffic = h2GIS.firstRow("select * from ${format.results.outputTableName} where road_type = 'Main roads' limit 1")

        expectedFlow = [ROAD_TYPE   : 'Main roads', SURFACE: 'asphalt', PAVEMENT: 'NL05', DIRECTION: 3, DAY_LV_HOUR: 475, DAY_HV_HOUR: 119, DAY_LV_SPEED: 50,
                        DAY_HV_SPEED: 50, NIGHT_LV_HOUR: 80, NIGHT_HV_HOUR: 9, NIGHT_LV_SPEED: 50, NIGHT_HV_SPEED: 50,
                        EV_LV_HOUR  : 227, EV_HV_HOUR: 40, EV_LV_SPEED: 50, EV_HV_SPEED: 50]
        road_traffic.each { it ->
            if (expectedFlow.get(it.key)) {
                assertEquals(expectedFlow.get(it.key), it.value)
            }
        }
        assertEquals(1, h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where PAVEMENT = 'NL08'").count)

    }

    @Test
    void extractSeaLandTest(TestInfo testInfo) {
        def epsg = 32629
        def osmBbox = [52.08484801362273, -10.75003575696209, 52.001518013622736, -10.66670575696209]
        def geom = Utilities.geometryFromNominatim(osmBbox)
        IProcess extractData = OSM.InputDataLoading.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("sea_land_data.osm").toURI()).getAbsolutePath(),
                epsg       : epsg])

        def zoneEnvelopeTableName = "zone_envelope_sea_land"
        h2GIS.execute("""drop table if exists $zoneEnvelopeTableName;
         create table $zoneEnvelopeTableName as select st_transform(st_geomfromtext('$geom', ${geom.getSRID()}), $epsg) as the_geom""".toString())

        //Test coastline
        assertEquals(1, h2GIS.getTable(extractData.results.coastlineTableName).getRowCount())

        //Sea/Land mask
        def format = OSM.InputDataFormatting.formatSeaLandMask()
        format.execute([
                datasource                : h2GIS,
                inputTableName            : extractData.results.coastlineTableName,
                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                epsg                      : epsg])
        def inputSeaLandTableName = format.results.outputTableName;
        assertEquals(2, h2GIS.getTable(inputSeaLandTableName).getRowCount())
        assertTrue h2GIS.firstRow("select count(*) as count from ${inputSeaLandTableName} where type='land'").count == 1

        h2GIS.getTable(inputSeaLandTableName).save(new File(folder, "osm_sea_land.geojson").absolutePath, true)

    }

    @Test
    void osmFileGISBuildingCheckHeight2() {
        //OSM URL https://www.openstreetmap.org/way/79083537
        //negative building:levels
        def epsg = 2154
        IProcess extractData = OSM.InputDataLoading.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("osmBuildingCheckHeigh.osm").toURI()).getAbsolutePath(),
                epsg       : epsg])

        //Buildings
        IProcess format = OSM.InputDataFormatting.formatBuildingLayer()
        format.execute([
                datasource                : h2GIS,
                inputTableName            : extractData.results.buildingTableName,
                inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                epsg                      : epsg])

        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where 1=1").count > 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count == 0

    }

    @Test
    void formattingGISBuildingLayer() {
        def epsg = 2154
        IProcess extractData = OSM.InputDataLoading.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg       : epsg])

        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        assertEquals 211, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 47, h2GIS.getTable(extractData.results.imperviousTableName).rowCount

        //Buildings with estimation state
        IProcess format = OSM.InputDataFormatting.formatBuildingLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg          : epsg,
                jsonFilename  : null,
                estimateHeight: true])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save(new File(folder, "osm_building_formated.shp").absolutePath, true)
        assertEquals 1038, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count == 0
        assertEquals 1038, h2GIS.getTable(format.results.outputEstimateTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputEstimateTableName} where ESTIMATED = false").count == 4
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} join ${format.results.outputEstimateTableName} using (id_build, id_source) where 1=1").count == 1038

        //Buildings without estimation state
        format = OSM.InputDataFormatting.formatBuildingLayer()
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg          : epsg,
                jsonFilename  : null])
        assertEquals 1038, h2GIS.getTable(format.results.outputTableName).rowCount
        assertEquals 1038, h2GIS.getTable(format.results.outputEstimateTableName).rowCount
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

        def h2GIS = H2GIS.open("${file.absolutePath + File.separator}osmdb_gislayers;AUTO_SERVER=TRUE".toString())

        //def zoneToExtract ="Shanghai, Chine"
        def zoneToExtract = "École Lycée Joliot-Curie,Rennes"
        zoneToExtract = "New York"
        zoneToExtract = "Québec, Québec (Agglomération), Capitale-Nationale, Québec, Canada"
        //zoneToExtract = "Bucarest"
        zoneToExtract = "Helsinki"
        //zoneToExtract ="Göteborgs Stad"
        //zoneToExtract = "Londres, Grand Londres, Angleterre, Royaume-Uni"
        zoneToExtract = "Vannes"
        //zoneToExtract="rezé"
        //zoneToExtract = "Brest"

        //river Göta älv
        zoneToExtract = [57.6753, 11.7982, 57.6955, 11.8656]
        //Taal Crater Lake
        //zoneToExtract =[13.4203,120.2165,14.5969 , 122.0293]
        //Le Havre
        zoneToExtract = [49.4370, -0.0230, 49.5359, 0.2053,]
        IProcess extractData = OSM.InputDataLoading.extractAndCreateGISLayers()
        extractData.execute([
                datasource   : h2GIS,
                zoneToExtract: zoneToExtract])

        String formatedPlaceName = zoneToExtract.join("-").trim().split("\\s*(,|\\s)\\s*").join("_");


        if (extractData.results.zone != null) {
            //Zone
            def epsg = h2GIS.getSpatialTable(extractData.results.zone).srid
            h2GIS.getTable(extractData.results.zone).save("${file.absolutePath + File.separator}osm_zone_${formatedPlaceName}.geojson", true)

            //Zone envelope
            h2GIS.getTable(extractData.results.zoneEnvelopeTableName).save("${file.absolutePath + File.separator}osm_zone_envelope_${formatedPlaceName}.geojson", true)
            IProcess format

            //Urban Areas
            /*format = OSM.InputDataFormatting.formatUrbanAreas()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.urbanAreasTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg])
            def urbanAreasTableName = format.results.outputTableName;
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_urban_areas_${formatedPlaceName}.geojson", true)

            //Buildings
            format = OSM.InputDataFormatting.formatBuildingLayer()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.buildingTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg,
                    urbanAreasTableName       : urbanAreasTableName])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_${formatedPlaceName}.geojson", true)
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count == 0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count == 0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count == 0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count == 0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count == 0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count == 0


            //Roads
            format = OSM.InputDataFormatting.formatRoadLayer()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.roadTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_${formatedPlaceName}.geojson", true)
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where width is null or width <= 0").count == 0

            //Rails
            format = OSM.InputDataFormatting.formatRailsLayer()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.railTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_${formatedPlaceName}.geojson", true)


            //Vegetation
            format = OSM.InputDataFormatting.formatVegetationLayer()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.vegetationTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_${formatedPlaceName}.geojson", true)*/


            //Hydrography
            format = OSM.InputDataFormatting.formatHydroLayer()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.hydroTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg])
            def inputWaterTableName = format.results.outputTableName
            h2GIS.getTable(inputWaterTableName).save("${file.absolutePath + File.separator}osm_hydro_${formatedPlaceName}.geojson", true)

            //Impervious
            /*format = OSM.InputDataFormatting.formatImperviousLayer()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.imperviousTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg                      : epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_${formatedPlaceName}.geojson", true)*/


            //Sea/Land mask
            format = OSM.InputDataFormatting.formatSeaLandMask()
            format.execute([
                    datasource                : h2GIS,
                    inputTableName            : extractData.results.coastlineTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    inputWaterTableName       : inputWaterTableName,
                    epsg                      : epsg])
            def inputSeaLandTableName = format.results.outputTableName;
            h2GIS.getTable(inputSeaLandTableName).save("${file.absolutePath + File.separator}osm_sea_land_${formatedPlaceName}.geojson", true)

            //Merge Sea/Land mask and Water layers
            format = OSM.InputDataFormatting.mergeWaterAndSeaLandTables()
            format.execute([
                    datasource           : h2GIS,
                    inputSeaLandTableName: inputSeaLandTableName, inputWaterTableName: inputWaterTableName,
                    epsg                 : epsg])
            h2GIS.getTable(format.results.outputTableName).save("${file.absolutePath + File.separator}osm_water_sea_${formatedPlaceName}.geojson", true)

        } else {
            assertTrue(false)
        }
    }

    //This test is used for debug purpose
    @Test
    @Disabled
    void createGISFormatLayersTestIntegration() {
        IProcess process = OSM.InputDataLoading.createGISLayers()
        def osmfile = "/tmp/map.osm"
        process.execute([
                datasource : h2GIS,
                osmFilePath: osmfile,
                epsg       : 2154])

        //Format Roads
        def format = OSM.InputDataFormatting.formatRoadLayer()
        format.execute([
                datasource                : h2GIS,
                inputTableName            : process.results.roadTableName,
                inputZoneEnvelopeTableName: process.results.zoneEnvelopeTableName,
                epsg                      : 2154])
        h2GIS.getTable(format.results.outputTableName).save("/tmp/formated_osm_road.shp", true)
    }
}
