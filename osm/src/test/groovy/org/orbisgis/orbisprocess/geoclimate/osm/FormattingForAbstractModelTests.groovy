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
        /**
        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        assertEquals 198, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 43, h2GIS.getTable(extractData.results.imperviousTableName).rowCount
        */
        //Buildings
        IProcess format = OSM.formatBuildingLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg: epsg,
                jsonFilename: null])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp")
        assertEquals 1040, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<HEIGHT_WALL").count==0

        /**
        //Check value for  specific features
        //TODO: to be fixed
        def res =  h2GIS.firstRow("select type,  nb_lev, height_wall, height_roof from ${format.results.outputTableName} where ID_SOURCE='w122539595'")
        assertEquals("church", res.type)
        assertEquals(1, res.nb_lev)
        assertEquals(3, res.height_wall)
        assertEquals(3, res.height_roof)

        res =  h2GIS.firstRow("select type,  nb_lev, height_wall, height_roof from ${format.results.outputTableName} where ID_SOURCE='w122535997'")
        assertEquals("building", res.type)
        assertEquals(1, res.nb_lev)
        assertEquals(6, res.height_wall)
        assertEquals(6, res.height_roof)*/

        //Roads
        format = OSM.formatRoadLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.roadTableName,
                epsg: epsg,
                jsonFilename: null])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_formated.shp")
        assertEquals 197, h2GIS.getTable(format.results.outputTableName).rowCount
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

        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_formated.shp")
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
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_formated.shp")
        assertEquals 140, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where type is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_CLASS is null").count==0


        //Hydrography
        format = OSM.formatHydroLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.hydroTableName,
                epsg: epsg])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_formated.shp")
        assertEquals 10, h2GIS.getTable(format.results.outputTableName).rowCount

        //Impervious surfaces
        format = OSM.formatImperviousLayer
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.imperviousTableName,
                epsg: epsg])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_formated.shp")
        assertEquals 43, h2GIS.getTable(format.results.outputTableName).rowCount


    }

    @Disabled
    @Test //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')

        //def zoneToExtract ="Shanghai, Chine"
        def zoneToExtract ="École Lycée Joliot-Curie,Rennes"
        zoneToExtract = "New York"
        zoneToExtract = "Québec, Québec (Agglomération), Capitale-Nationale, Québec, Canada"
        zoneToExtract = "Paimpol"
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
            h2GIS.getTable(extractData.results.zoneTableName).save("./target/osm_zone_${formatedPlaceName}.geojson")

            //Zone envelope
            h2GIS.getTable(extractData.results.zoneEnvelopeTableName).save("./target/osm_zone_envelope_${formatedPlaceName}.geojson")


            //Buildings
            IProcess format = OSM.formatBuildingLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.buildingTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_${formatedPlaceName}.geojson")
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
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_${formatedPlaceName}.geojson")
            assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where width is null or width <= 0").count==0

            //Rails
            format = OSM.formatRailsLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.railTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_${formatedPlaceName}.geojson")


            //Vegetation
            format = OSM.formatVegetationLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.vegetationTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_${formatedPlaceName}.geojson")


            //Hydrography
            format = OSM.formatHydroLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.hydroTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_${formatedPlaceName}.geojson")

            //Impervious
            format = OSM.formatImperviousLayer
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.imperviousTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_${formatedPlaceName}.geojson")
        }else {
            assertTrue(false)
        }
    }
    @Disabled
    @Test
    void apiOSMGISBuildingCheckHeight1() {
        //OSM URL https://www.openstreetmap.org/way/227927910
        def zoneToExtract =  [48.87644088590647,2.3938433825969696,48.877258515821225,2.3952582478523254]
        createGISLayersCheckHeight(zoneToExtract)
    }
    @Disabled
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
        assertEquals 198, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 43, h2GIS.getTable(extractData.results.imperviousTableName).rowCount

        //Buildings with estimation state
        IProcess format = OSM.formatBuildingLayer
        format.execute([
                datasource    : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg          : epsg,
                jsonFilename  : null,
                estimateHeight : true])
        assertNotNull h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp")
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
/*
        IProcess format = OSM.formatBuildingLayer
        format.execute([
        datasource    : h2GIS,
        inputTableName: extractData.results.buildingTableName,
        epsg          : epsg,
        jsonFilename  : null,
        estimateHeight : false])
        assertEquals 1040, h2GIS.getTable(format.results.outputTableName).rowCount
        assertEquals "", format.results.outputEstimateTableName
*/
    }

}
