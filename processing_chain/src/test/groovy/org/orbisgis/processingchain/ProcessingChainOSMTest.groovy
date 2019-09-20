package org.orbisgis.processingchain

import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.geoindicators.DataUtils
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.geoindicators.Geoindicators
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertTrue
import static org.junit.jupiter.api.Assertions.assertEquals


class ProcessingChainOSMTest extends ChainProcessMainTest {



    @Test
    void osmToRSU() {
        String directory ="./target/osm_processchain_geoindicators"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def h2GIS = H2GIS.open(dirFile.absolutePath+File.separator+'osm_chain_db;AUTO_SERVER=TRUE')
        def placeName = "Cliscouet, vannes"
        IProcess process = ProcessingChain.PrepareOSM.buildGeoclimateLayers()

        process.execute([datasource: h2GIS, placeName :placeName, distance: 0])

        def prefixName  = placeName.trim().split("\\s*(,|\\s)\\s*").join("_");

        // Create the RSU
        def prepareRSUData = Geoindicators.SpatialUnits.prepareRSUData()
        def createRSU = Geoindicators.SpatialUnits.createRSU()
        if (prepareRSUData([datasource        : h2GIS,
                             zoneTable         : process.getResults().outputZone,
                             roadTable         : process.getResults().outputRoad,
                             railTable         : process.getResults().outputRail,
                             vegetationTable   : process.getResults().outputVeget,
                             hydrographicTable : process.getResults().outputHydro,
                             prefixName        : prefixName])) {
            def saveTables = Geoindicators.DataUtils.saveTablesAsFiles()

            saveTables.execute( [inputTableNames: process.getResults().values()
                                 , directory: directory, datasource: h2GIS])

            if (createRSU([datasource    : h2GIS,
                            inputTableName: prepareRSUData.results.outputTableName,
                           inputZoneTableName :process.getResults().outputZone,
                            prefixName    : prefixName])) {
                h2GIS.getTable(createRSU.results.outputTableName).save(dirFile.absolutePath+File.separator+"${prefixName}.geojson")
            }

        }
    }

    @Test
    void osmGeoIndicatorsFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        boolean saveResults = true
        String directory ="./target/osm_processchain_geoindicators"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osm_chain_db;AUTO_SERVER=TRUE")

        String zoneTableName="zone"
        String buildingTableName="building"
        String roadTableName="road"
        String railTableName="rails"
        String vegetationTableName="veget"
        String hydrographicTableName="hydro"
        def indicatorUse = ["URBAN_TYPOLOGY", "LCZ", "TEB"]

        datasource.load(urlBuilding, buildingTableName)
        datasource.load(urlRoad, roadTableName)
        datasource.load(urlRail, railTableName)
        datasource.load(urlVeget, vegetationTableName)
        datasource.load(urlHydro, hydrographicTableName)
        datasource.load(urlZone, zoneTableName)

        //Run tests
        osmGeoIndicators(directory, datasource, zoneTableName, buildingTableName,roadTableName,null,vegetationTableName,
                hydrographicTableName,saveResults, indicatorUse)

    }

    //@Test
    void osmGeoIndicatorsFromApi() {
        String directory ="./target/osm_processchain_full"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osm_chain_db;AUTO_SERVER=TRUE")

        //Extract and transform OSM data
        def placeName = "Cliscouet, Vannes"

        IProcess prepareOSMData = ProcessingChain.PrepareOSM.buildGeoclimateLayers()

        process.execute([datasource: h2GIS, placeName :placeName, distance: 0])

        String buildingTableName = prepareOSMData.getResults().outputBuilding

        String roadTableName = prepareOSMData.getResults().outputRoad

        String railTableName = prepareOSMData.getResults().outputRail

        String hydrographicTableName = prepareOSMData.getResults().outputHydro

        String vegetationTableName = prepareOSMData.getResults().outputVeget

        String zoneTableName = prepareOSMData.getResults().outputZone

        if(saveResults){
            println("Saving OSM GIS layers")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles()
            saveTables.execute( [inputTableNames: [buildingTableName,roadTableName,railTableName,hydrographicTableName,
                                                   vegetationTableName,zoneTableName]
                                 , directory: dirFile.absolutePath, datasource: datasource])
        }

        String indicatorUse = ["TEB", "URBAN_TYPOLOGY", "LCZ"]

        // Run LCZ
        calcLcz(directory, datasource, zoneTableName, buildingTableName,
                roadTableName, railTableName, vegetationTableName,
                hydrographicTableName, saveResults )

        //Run tests
        osmGeoIndicators(dirFile.absolutePath, datasource, zoneTableName, buildingTableName,roadTableName,railTableName,vegetationTableName,
                hydrographicTableName,saveResults, indicatorUse)

    }

    @Test
    void osmLczFromTestFiles() {
        String urlBuilding = new File(getClass().getResource("BUILDING.geojson").toURI()).absolutePath
        String urlRoad= new File(getClass().getResource("ROAD.geojson").toURI()).absolutePath
        String urlRail = new File(getClass().getResource("RAIL.geojson").toURI()).absolutePath
        String urlVeget = new File(getClass().getResource("VEGET.geojson").toURI()).absolutePath
        String urlHydro = new File(getClass().getResource("HYDRO.geojson").toURI()).absolutePath
        String urlZone = new File(getClass().getResource("ZONE.geojson").toURI()).absolutePath

        boolean saveResults = true
        String directory ="./target/osm_processchain_lcz"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osmchain_lcz;AUTO_SERVER=TRUE")

        String zoneTableName="zone"
        String buildingTableName="building"
        String roadTableName="road"
        String railTableName="rails"
        String vegetationTableName="veget"
        String hydrographicTableName="hydro"

        datasource.load(urlBuilding, buildingTableName)
        datasource.load(urlRoad, roadTableName)
        datasource.load(urlRail, railTableName)
        datasource.load(urlVeget, vegetationTableName)
        datasource.load(urlHydro, hydrographicTableName)
        datasource.load(urlZone, zoneTableName)

        //Run tests
        calcLcz(directory, datasource, zoneTableName, buildingTableName,roadTableName,null,vegetationTableName,
                hydrographicTableName,saveResults, false)
    }


}
