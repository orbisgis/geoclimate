package org.orbisgis.orbisprocess.geoclimate.osm

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.orbisgis.orbisanalysis.osm.utils.Utilities
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue

class FormattingForAbstractModelTests {


    @Test
   void formattingGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        def epsg =2154
        IProcess extractData = OSM.createGISLayers
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :epsg])

        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        assertEquals 211, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 45, h2GIS.getTable(extractData.results.imperviousTableName).rowCount
        assertEquals 6, h2GIS.getTable(extractData.results.urbanAreasTableName).rowCount
        assertEquals 0, h2GIS.getTable(extractData.results.coastlineTableName).rowCount

        //Buildings
        IProcess format = OSM.formatBuildingLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg: epsg,
                jsonFilename: null])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp", true)
        assertEquals 1040, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count==0

        //Format urban areas
        format = OSM.formatUrbanAreas
        format.execute([
                datasource : h2GIS,
                inputTableName:  extractData.results.urbanAreasTableName,
                epsg: epsg])
        def urbanAreas = format.results.outputTableName
        assertNotNull h2GIS.getTable(urbanAreas).save("./target/osm_urban_areas.shp", true)


        //Improve building type
        format = OSM.formatBuildingLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg: epsg,
                urbanAreasTableName: urbanAreas])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated_type.shp", true)
        def rows = h2GIS.rows("select type from ${format.results.outputTableName} where id_build=158 or id_build=982")
        assertEquals(2, rows.size())
        assertTrue( rows.type==['residential','residential'])

        rows = h2GIS.rows("select type from ${format.results.outputTableName} where id_build=881 or id_build=484 or id_build=610")
        assertEquals(3, rows.size())
        assertTrue( rows.type==['light_industry','light_industry','light_industry'])


        //Roads
        format = OSM.formatRoadLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.roadTableName,
                epsg: epsg,
                jsonFilename: null])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_formated.shp", true)
        assertEquals 157, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH<=0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where CROSSING IS NOT NULL").count==7

        //Rails
        format = OSM.formatRailsLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.railTableName,
                epsg: epsg,
                jsonFilename: null])

        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_formated.shp", true)
        assertEquals 41, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where CROSSING IS NOT NULL").count==8


        //Vegetation
        format = OSM.formatVegetationLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.vegetationTableName,
                epsg: epsg,
                jsonFilename: null
        ])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_formated.shp", true)
        assertEquals 140, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where type is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_CLASS is null").count==0


        //Hydrography
        format = OSM.formatHydroLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.hydroTableName,
                epsg: epsg])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_formated.shp", true)
        assertEquals 10, h2GIS.getTable(format.results.outputTableName).rowCount

        //Impervious surfaces
        format = OSM.formatImperviousLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.imperviousTableName,
                epsg: epsg])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_formated.shp", true)
        assertEquals 45, h2GIS.getTable(format.results.outputTableName).rowCount

        //Sea/Land mask
        format = OSM.formatSeaLandMask
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.outputCoastlineTableName,
                inputZoneEnvelopeTableName: "",
                epsg: epsg])
        assertEquals(0, h2GIS.getTable(format.results.outputTableName).getRowCount())
    }

    @Test
    void extractSeaLandTest(TestInfo testInfo) {
        def h2GIS = H2GIS.open('./target/sea_land;AUTO_SERVER=TRUE')
        def epsg =32629
        def osmBbox = [52.08484801362273, -10.75003575696209, 52.001518013622736, -10.66670575696209]
        def geom = Utilities.geometryFromNominatim(osmBbox)
        IProcess extractData = OSM.createGISLayers
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("sea_land_data.osm").toURI()).getAbsolutePath(),
                epsg :epsg])

        def zoneEnvelopeTableName = "zone_envelope_sea_land"
        h2GIS.execute("""drop table if exists $zoneEnvelopeTableName;
         create table $zoneEnvelopeTableName as select st_transform(st_geomfromtext('$geom', ${geom.getSRID()}), $epsg) as the_geom""")

        //Test coastline
        assertEquals(1, h2GIS.getTable(extractData.results.coastlineTableName).getRowCount())

        //Sea/Land mask
        def format = OSM.formatSeaLandMask
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.coastlineTableName,
                inputZoneEnvelopeTableName: zoneEnvelopeTableName,
                epsg: epsg])
        def inputSeaLandTableName = format.results.outputTableName;
        assertEquals(2, h2GIS.getTable(inputSeaLandTableName).getRowCount())
        assertTrue h2GIS.firstRow("select count(*) as count from ${inputSeaLandTableName} where type='land'").count==1

        h2GIS.getTable(inputSeaLandTableName).save("./target/osm_sea_land_${testInfo.getDisplayName()}.geojson", true)


    }

    @Disabled
    @Test //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb_gislayers;AUTO_SERVER=TRUE')

        //def zoneToExtract ="Shanghai, Chine"
        def zoneToExtract ="École Lycée Joliot-Curie,Rennes"
        zoneToExtract = "New York"
        zoneToExtract = "Québec, Québec (Agglomération), Capitale-Nationale, Québec, Canada"
        //zoneToExtract = "Bucarest"
        zoneToExtract="Helsinki"
        //zoneToExtract ="Göteborgs Stad"
        //zoneToExtract = "Londres, Grand Londres, Angleterre, Royaume-Uni"
        //zoneToExtract="Vannes"
        //zoneToExtract="rezé"
        zoneToExtract = "Brest"

        IProcess extractData = OSM.extractAndCreateGISLayers
        extractData.execute([
                datasource : h2GIS,
                zoneToExtract:zoneToExtract ])

        String formatedPlaceName = zoneToExtract.trim().split("\\s*(,|\\s)\\s*").join("_");


        if(extractData.results.zoneTableName!=null) {
            //Zone
            def epsg = h2GIS.getSpatialTable(extractData.results.zoneTableName).srid
            h2GIS.getTable(extractData.results.zoneTableName).save("./target/osm_zone_${formatedPlaceName}.geojson", true)

            //Zone envelope
            h2GIS.getTable(extractData.results.zoneEnvelopeTableName).save("./target/osm_zone_envelope_${formatedPlaceName}.geojson", true)


            //Urban Areas
            IProcess format  = OSM.formatUrbanAreas
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.urbanAreasTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            def urbanAreasTableName = format.results.outputTableName;
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_urban_areas_${formatedPlaceName}.geojson", true)

            //Buildings
            format = OSM.formatBuildingLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.buildingTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg,
                    urbanAreasTableName : urbanAreasTableName])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_${formatedPlaceName}.geojson", true)
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count==0


            //Roads
            format = OSM.formatRoadLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.roadTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_${formatedPlaceName}.geojson", true)
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where width is null or width <= 0").count==0

            //Rails
            format = OSM.formatRailsLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.railTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_${formatedPlaceName}.geojson", true)


            //Vegetation
            format = OSM.formatVegetationLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.vegetationTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_${formatedPlaceName}.geojson", true)


            //Hydrography
            format = OSM.formatHydroLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.hydroTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            def inputWaterTableName = format.results.outputTableName
            h2GIS.getTable(inputWaterTableName).save("./target/osm_hydro_${formatedPlaceName}.geojson", true)

            //Impervious
            format = OSM.formatImperviousLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.imperviousTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_${formatedPlaceName}.geojson", true)


            //Sea/Land mask
            format = OSM.formatSeaLandMask
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.coastlineTableName,
                    inputZoneEnvelopeTableName: extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            def inputSeaLandTableName = format.results.outputTableName;
            h2GIS.getTable(inputSeaLandTableName).save("./target/osm_sea_land_${formatedPlaceName}.geojson", true)

            //Merge Sea/Land mask and Water layers
            format = OSM.mergeWaterAndSeaLandTables
            format.execute([
                    datasource : h2GIS,
                    inputSeaLandTableName: inputSeaLandTableName,inputWaterTableName: inputWaterTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_water_sea_${formatedPlaceName}.geojson", true)

        }else {
            assertTrue(false)
        }
    }

    @Test
    void apiOSMGISBuildingCheckHeight1() {
        //OSM URL https://www.openstreetmap.org/way/227927910
        def zoneToExtract =  [48.87644088590647,2.3938433825969696,48.877258515821225,2.3952582478523254]
        createGISLayersCheckHeight(zoneToExtract)
    }

    @Test
    void apiOSMGISBuildingCheckHeight2() {
        //OSM URL https://www.openstreetmap.org/way/79083537
        //negative building:levels
        def zoneToExtract =  [48.82043541804379,2.364395409822464,48.82125396297273,2.36581027507782]
        createGISLayersCheckHeight(zoneToExtract)
    }



    /**
     * Method to check value from  the building layer
     * @param zoneToExtract
     */
    void createGISLayersCheckHeight(def zoneToExtract) {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')

        IProcess extractData = OSM.extractAndCreateGISLayers
        extractData.execute([
                datasource : h2GIS,
                zoneToExtract:zoneToExtract ])

        if(extractData.results.zoneTableName!=null) {
            def epsg = h2GIS.getSpatialTable(extractData.results.zoneTableName).srid

            //Buildings
            IProcess format = OSM.formatBuildingLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.buildingTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])

            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where 1=1").count>0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count==0
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count==0

        }else {
            assertTrue(false)
        }
    }

    @Test
    void formattingGISBuildingLayer() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        def epsg = 2154
        IProcess extractData = OSM.createGISLayers
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg       : epsg])

        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        assertEquals 211, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 45, h2GIS.getTable(extractData.results.imperviousTableName).rowCount

        //Buildings with estimation state
        IProcess format = OSM.formatBuildingLayer
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg          : epsg,
                jsonFilename  : null,
                estimateHeight : true])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp", true)
        assertEquals 1040, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count == 0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count == 0
        assertEquals 1040, h2GIS.getTable(format.results.outputEstimateTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputEstimateTableName} where ESTIMATED = false").count == 4
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} join ${format.results.outputEstimateTableName} using (id_build, id_source) where 1=1").count == 1040

        //Buildings without estimation state
        format = OSM.formatBuildingLayer
        format.execute([
        datasource    : h2GIS,
        inputTableName: extractData.results.buildingTableName,
        epsg          : epsg,
        jsonFilename  : null])
        assertEquals 1040, h2GIS.getTable(format.results.outputTableName).rowCount
        assertEquals 1040, h2GIS.getTable(format.results.outputEstimateTableName).rowCount
    }

}
