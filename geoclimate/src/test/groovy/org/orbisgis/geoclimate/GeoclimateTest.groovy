package org.orbisgis.geoclimate

import groovy.json.JsonOutput
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import picocli.CommandLine

import java.util.regex.Pattern

/**
 * Test class dedicated to {@link Geoclimate}.
 *
 * @author Erwan Bocher (CNRS 2020)
 * @author Sylvain PALOMINOS (UBS chaire GEOTERA)
 */
class GeoclimateTest {

    @Test
    void runCLITest() {
        def app = new Geoclimate()
        def cmd = new CommandLine(app)
        def sw = new StringWriter()
        cmd.setOut(new PrintWriter(sw))
        def exitCode = cmd.execute("-c")
        assert 2 == exitCode
        exitCode = cmd.execute("-w", "-f")
        assert 2 == exitCode
        exitCode = cmd.execute("-w orbisgis", "-f")
        assert 2 == exitCode
        exitCode = cmd.execute("-w osm", "-f")
        assert 2 == exitCode
        exitCode = cmd.execute("-w osm", "-f  /tmp/conf.json")
        assert 1 == exitCode
    }

    @Test
    void propertiesTest() {
        assert "0.0.2-SNAPSHOT" == Geoclimate.version
        assert Pattern.compile("^\\d{4}-\\d{2}-\\d{2}").matcher(Geoclimate.build).matches()
    }

    @Disabled
    @Test
    void runCLIWorkflow() {
        def directory ="./target/geoclimate_cli"
        def dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osmParameters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["Pont de veyle"]],
                "output" :[
                        "folder" : "$directory"],
                "parameters":
                        [
                                "indicatorUse": ["TEB"],
                                "svfSimplified": true,
                                "hLevMin": 3,
                                "hLevMax": 15,
                                "hThresholdLev2": 10
                        ]
        ]

        def json = JsonOutput.toJson(osmParameters)
        def configFile = File.createTempFile("osmConfigFile",".json")
        if(configFile.exists()){
            configFile.delete()
        }
        configFile.write(json)

        def app = new Geoclimate()
        def cmd = new CommandLine(app)
        def sw = new StringWriter()
        cmd.setOut(new PrintWriter(sw))
        def exitCode = cmd.execute("-w osm", "-f $configFile")
        assert 2 == exitCode
    }


    @Disabled
    @Test
    void runWorkflow() {
        def directory ="./target/geoclimate_cli"
        def dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osmParameters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE",
                        "delete" :true
                ],
                "input" : [
                        "osm" : ["Pont de veyle"]],
                "output" :[
                        "folder" : "$directory"],
                "parameters":[
                        "rsu_indicators":[
                                "indicatorUse": ["LCZ"],
                                "svfSimplified": false,
                                "estimateHeight":false
                        ],
                        "grid_indicators": [
                                "x_size": 1000,
                                "y_size": 1000,
                                "indicators": ["BUILDING_FRACTION","BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION","WATER_FRACTION","VEGETATION_FRACTION",
                                               "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "LCZ_FRACTION"]
                        ]
                ]
        ]
        def json = JsonOutput.toJson(osmParameters)
        def configFile = File.createTempFile("osmConfigFile",".json")
        if(configFile.exists()){
            configFile.delete()
        }
        configFile.write(json)

        Geoclimate.OSM.workflow.execute(configurationFile: configFile.absolutePath)
    }
}
