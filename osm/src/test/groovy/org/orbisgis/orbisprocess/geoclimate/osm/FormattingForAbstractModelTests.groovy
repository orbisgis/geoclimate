package org.orbisgis.orbisprocess.geoclimate.osm

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager

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
        assertEquals 44, h2GIS.getTable(extractData.results.imperviousTableName).rowCount
        assertEquals 6, h2GIS.getTable(extractData.results.urbanAreasTableName).rowCount

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
        assertTrue( rows.type==['industrial','industrial','industrial'])


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
        assertEquals 44, h2GIS.getTable(format.results.outputTableName).rowCount

    }

    @Disabled
    @Test //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')

        //def zoneToExtract ="Shanghai, Chine"
        def zoneToExtract ="École Lycée Joliot-Curie,Rennes"
        zoneToExtract = "New York"
        zoneToExtract = "Québec, Québec (Agglomération), Capitale-Nationale, Québec, Canada"
<<<<<<< HEAD
        zoneToExtract = "Paimpol"
=======
        //zoneToExtract = "Bucarest"
        zoneToExtract="Helsinki"
>>>>>>> e04930d576c4ce986be0a38bc9c45137695103fc
        //zoneToExtract = "Londres, Grand Londres, Angleterre, Royaume-Uni"
        //zoneToExtract="Cliscouet, Vannes"
        //zoneToExtract="rezé"

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


            //Buildings
            IProcess format = OSM.formatBuildingLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.buildingTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
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
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_${formatedPlaceName}.geojson", true)

            //Impervious
            format = OSM.formatImperviousLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.imperviousTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_${formatedPlaceName}.geojson", true)
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
        assertEquals 44, h2GIS.getTable(extractData.results.imperviousTableName).rowCount

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
        jsonFilename  : null,
        estimateHeight : false])
        assertEquals 1040, h2GIS.getTable(format.results.outputTableName).rowCount
        assertEquals "", format.results.outputEstimateTableName

    }

}
