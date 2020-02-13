package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators
import groovy.json.*

import static org.junit.jupiter.api.Assertions.*

class ProcessingChainOSMTest extends ChainProcessAbstractTest {

    @Disabled
    @Test
    void osmToRSU() {
        String directory ="./target/osm_processchain_geoindicators"

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def h2GIS = H2GIS.open(dirFile.absolutePath+File.separator+'osm_chain_db;AUTO_SERVER=TRUE')
        def zoneToExtract = "Pont de veyle"
        IProcess process = ProcessingChain.PrepareOSM.buildGeoclimateLayers()

        process.execute([datasource: h2GIS, zoneToExtract :zoneToExtract, distance: 0])

        def prefixName  = zoneToExtract.trim().split("\\s*(,|\\s)\\s*").join("_");

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
        def prefixName = ""
        def indicatorUse = ["URBAN_TYPOLOGY", "LCZ", "TEB"]
        def svfSimplified = false

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


        datasource.load(urlBuilding, buildingTableName)
        datasource.load(urlRoad, roadTableName)
        datasource.load(urlRail, railTableName)
        datasource.load(urlVeget, vegetationTableName)
        datasource.load(urlHydro, hydrographicTableName)
        datasource.load(urlZone, zoneTableName)

        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath+File, datasource, zoneTableName, buildingTableName,roadTableName,
                null,vegetationTableName, hydrographicTableName,saveResults, svfSimplified, indicatorUse, prefixName)

    }

    @Disabled
    @Test
    void osmGeoIndicatorsFromApi() {
        String directory ="./target/osm_processchain_full"
        boolean saveResults = true
        def prefixName = ""
        def svfSimplified = false

        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()

        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"osm_chain_db;AUTO_SERVER=TRUE")

        //Extract and transform OSM data
        def zoneToExtract = "Rezé"

        IProcess prepareOSMData = ProcessingChain.PrepareOSM.buildGeoclimateLayers()

        prepareOSMData.execute([datasource: datasource, zoneToExtract :zoneToExtract, distance: 0])

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

        def indicatorUse = ["TEB", "URBAN_TYPOLOGY", "LCZ"]

        //Run tests
        geoIndicatorsCalc(dirFile.absolutePath, datasource, zoneTableName, buildingTableName,roadTableName,railTableName,vegetationTableName,
                hydrographicTableName,saveResults, svfSimplified,indicatorUse,  prefixName)

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

        datasource.load(urlBuilding, buildingTableName, true)
        datasource.load(urlRoad, roadTableName, true)
        datasource.load(urlRail, railTableName, true)
        datasource.load(urlVeget, vegetationTableName, true)
        datasource.load(urlHydro, hydrographicTableName, true)
        datasource.load(urlZone, zoneTableName, true)

        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        IProcess geodindicators = ProcessingChain.Workflow.GeoIndicators()
        assertTrue geodindicators.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: railTableName, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ["LCZ"],
                mapOfWeights: mapOfWeights)


        assertTrue(datasource.getTable(geodindicators.results.outputTableBuildingIndicators).rowCount>0)
        assertNull(geodindicators.results.outputTableBlockIndicators)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuIndicators).rowCount>0)
        assertTrue(datasource.getTable(geodindicators.results.outputTableRsuLcz).rowCount>0)

    }


    @Disabled
    @Test
    void testOSMWorkflowFromPlaceName() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE")
        IProcess process = ProcessingChain.Workflow.OSM()
        def placeName = "Paris"
        if(process.execute(datasource: datasource, zoneToExtract: placeName)){
            process.getResults().values().each { it ->
                if(datasource.hasTable(it)){
                    datasource.save(it, "/tmp/${placeName}_${it}.shp")
                }
            }
        }
    }

    @Test
    void testOSMWorkflowFromBbox() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE")
        IProcess process = ProcessingChain.Workflow.OSM()
        if(process.execute(datasource: datasource, zoneToExtract: [38.89557963573336,-77.03930318355559,38.89944983078282,-77.03364372253417])) {
            process.getResults().values().each { it ->
                assertTrue datasource.hasTable(it)
            }
        }
    }

    @Test
    void testOSMWorkflowFromBadBbox1() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE")
        IProcess process = ProcessingChain.Workflow.OSM()
        assertFalse(process.execute(datasource: datasource, zoneToExtract: [-3.0961382389068604, -3.1055688858032227,48.77155634881654,]))
    }

    @Test
    void testOSMWorkflowFromBadBbox2() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        H2GIS datasource = H2GIS.open(dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE")
        IProcess process = ProcessingChain.Workflow.OSM()
        assertFalse(process.execute(datasource: datasource, zoneToExtract: []))
    }

    @Test
    void testOSMLCZ() {
        String directory ="./target/geoclimate_chain"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parmeters = [
            "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
            "geoclimatedb" : [
                "path" : "${dirFile.absolutePath+File.separator+"geoclimate_chain_db;AUTO_SERVER=TRUE"}",
                "delete" :true
            ],
            "input" : [
                "osm" : ["romainville"]],
            "output" :[
                "folder" : "$directory"],
            "parameters":
            ["distance" : 1000,
                "indicatorUse": ["LCZ"],
                "svfSimplified": false,
                "prefixName": "",
                "mapOfWeights":
                ["sky_view_factor": 1,
                    "aspect_ratio": 1,
                    "building_surface_fraction": 1,
                    "impervious_surface_fraction" : 1,
                    "pervious_surface_fraction": 1,
                    "height_of_roughness_elements": 1,
                    "terrain_roughness_class": 1  ],
                "hLevMin": 3,
                "hLevMax": 15,
                "hThresholdLev2": 10
            ]
        ]
        def json = JsonOutput.toJson(osm_parmeters)
        def configFilePath =  directory+File.separator+"osmConfigFile.json"
        File configFile = new File(configFilePath)
        if(configFile.exists()){
            configFile.delete()
        }
        configFile.write(json)
        IProcess process = ProcessingChain.Workflow.OSM()
        assertTrue(process.execute(configurationFile: configFilePath))
    }

    @Test //Integration tests
    @Disabled
    void testOSMConfigurationFile() {
        def configFile = getClass().getResource("config/osm_workflow_placename_folderoutput.json").toURI()
        configFile =getClass().getResource("config/osm_workflow_envelope_folderoutput.json").toURI()
        IProcess process = ProcessingChain.Workflow.OSM()
        assertTrue(process.execute(configurationFile: configFile))
    }
}
