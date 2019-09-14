
package org.orbisgis.osm


import org.junit.jupiter.api.Test
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess

import static org.junit.jupiter.api.Assertions.*

class FormattingForAbstractModelTests {

    @Test
   void transformBuildingsTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 1038, h2GIS.getTable(extractData.results.buildingTableName).rowCount

        IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.buildingTableName])
        assertEquals 1038, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_building_formated.shp")
    }

    @Test
    void transformRoadsTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 360, h2GIS.getTable(extractData.results.roadTableName).rowCount

        IProcess format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.roadTableName])

        assertEquals 360, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_road_formated.shp")
    }

    @Test
    void transformRailsTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 44, h2GIS.getTable(extractData.results.railTableName).rowCount

        IProcess format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.railTableName])

        assertEquals 44, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_rails_formated.shp")
    }


    @Test
    void transformVegetationTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 128, h2GIS.getTable(extractData.results.vegetationTableName).rowCount

        IProcess format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.vegetationTableName])

        assertEquals 128, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_vegetation_formated.shp")
    }

    @Test
    void transformHydroTest() {
        def h2GIS = H2GIS.open('./target/osmdb;AUTO_SERVER=TRUE')
        IProcess extractData = PrepareData.OSMGISLayers.createGISLayers()
        extractData.execute([
                datasource : h2GIS,
                osmFilePath: new File(this.class.getResource("redon.osm").toURI()).getAbsolutePath(),
                epsg :2154])
        assertEquals 8, h2GIS.getTable(extractData.results.hydroTableName).rowCount

        IProcess format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
        format.execute([
                datasource : h2GIS,
                inputTableName: extractData.results.hydroTableName])

        assertEquals 8, h2GIS.getTable(format.results.outputTableName).rowCount
        h2GIS.getTable(format.results.outputTableName).save("./target/osm_hydro_formated.shp")
    }
}


