package org.orbisgis.orbisprocess.geoclimate.preparedata.osm


import org.junit.jupiter.api.Test
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.*

class FormattingForAbstractModelTests {

    @Test
   void formattingGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :2154])

        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount
        assertEquals 198, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 135, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 10, h2GIS.getTable(extractData.results.hydroTableName).rowCount
        assertEquals 43, h2GIS.getTable(extractData.results.imperviousTableName).rowCount

        def epsg = extractData.results.epsg

        //Buildings
        IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg: epsg,
                jsonFilename: null])
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp")
        assertEquals 1044, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count==0

        //Roads
        format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.roadTableName,
                epsg: epsg,
                jsonFilename: null])
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_formated.shp")
        assertEquals 197, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where CROSSING IS NOT NULL").count==7



        //Rails
        format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.railTableName,
                epsg: epsg,
                jsonFilename: null])

        h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_formated.shp")
        assertEquals 41, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where CROSSING IS NOT NULL").count==8


        //Vegetation
        format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.vegetationTableName,
                epsg: epsg,
                jsonFilename: null
        ])
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_formated.shp")
        assertEquals 140, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where type is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_CLASS is null").count==0


        //Hydrography
        format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.hydroTableName,
                epsg: epsg])
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_formated.shp")
        assertEquals 10, h2GIS.getTable(format.results.outputTableName).rowCount

        //Impervious surfaces
        format = PrepareData.FormattingForAbstractModel.formatImperviousLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.imperviousTableName,
                epsg: epsg])
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_impervious_formated.shp")
        assertEquals 43, h2GIS.getTable(format.results.outputTableName).rowCount


    }

    //@Test //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')

        //def placeName ="Shanghai, Chine"
        def placeName ="École Lycée Joliot-Curie,Rennes"
        placeName = "New York"
        placeName = "Québec, Québec (Agglomération), Capitale-Nationale, Québec, Canada"
        placeName = "Paimpol"
        //placeName = "Londres, Grand Londres, Angleterre, Royaume-Uni"

        IProcess extractData = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
        extractData.execute([
                datasource : h2GIS,
                placeName:placeName ])

        String formatedPlaceName = placeName.trim().split("\\s*(,|\\s)\\s*").join("_");


        if(extractData.results.zoneTableName!=null) {
            //Zone
            h2GIS.getTable(extractData.results.zoneTableName).save("./target/osm_zone_${formatedPlaceName}.geojson")

            //Zone envelope
            h2GIS.getTable(extractData.results.zoneEnvelopeTableName).save("./target/osm_zone_envelope_${formatedPlaceName}.geojson")


            def epsg = extractData.results.epsg

            //Buildings
            IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
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
            format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.roadTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_${formatedPlaceName}.geojson")


            //Rails
            format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.railTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_${formatedPlaceName}.geojson")


            //Vegetation
            format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.vegetationTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_${formatedPlaceName}.geojson")


            //Hydrography
            format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.hydroTableName,
                    inputZoneEnvelopeTableName :extractData.results.zoneEnvelopeTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_${formatedPlaceName}.geojson")

            //Impervious
            format = PrepareData.FormattingForAbstractModel.formatImperviousLayer()
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

}


