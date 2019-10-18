package org.orbisgis.osm

import groovy.json.JsonSlurper
import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess
import groovy.json.JsonOutput

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
        assertEquals 360, h2GIS.getTable(extractData.results.roadTableName).rowCount
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount
        assertEquals 128, h2GIS.getTable(extractData.results.vegetationTableName).rowCount
        assertEquals 8, h2GIS.getTable(extractData.results.hydroTableName).rowCount


        def epsg = extractData.results.epsg


        //Buildings
        IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName,
                epsg: epsg,
                jsonFilename: null])
        assertEquals 1038, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where NB_LEV<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_WALL<0").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_ROOF<0").count==0

        h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp")

        //Roads
        format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.roadTableName,
                epsg: epsg,
                jsonFilename: null])

        assertEquals 360, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where WIDTH<0").count==0

        h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_formated.shp")


        //Rails
        format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.railTableName,
                epsg: epsg,
                jsonFilename: null])

        assertEquals 44, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_formated.shp")


        //Vegetation
        format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.vegetationTableName,
                epsg: epsg,
                jsonFilename: null
        ])

        assertEquals 128, h2GIS.getTable(format.results.outputTableName).rowCount
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where type is null").count==0
        assertTrue h2GIS.firstRow("select count(*) as count from ${format.results.outputTableName} where HEIGHT_CLASS is null").count==0

        h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_formated.shp")

        //Hydrography
        format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.hydroTableName,
                epsg: epsg])

        assertEquals 8, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_formated.shp")

    }

    //@Test //enable it to test data extraction from the overpass api
    void extractCreateFormatGISLayers() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')

        //def placeName ="Shanghai, Chine"
        def placeName ="École Lycée Joliot-Curie,Rennes"

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
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_${formatedPlaceName}.geojson")


            //Rails
            format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.railTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_${formatedPlaceName}.geojson")


            //Vegetation
            format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.vegetationTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_${formatedPlaceName}.geojson")


            //Hydrography
            format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
            format.execute([
                    datasource : h2GIS,
                    inputTableName: extractData.results.hydroTableName,
                    epsg: epsg])
            h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_${formatedPlaceName}.geojson")

        }else {
            assertTrue(false)
        }
    }

}


