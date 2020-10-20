package org.orbisgis.orbisprocess.geoclimate

import groovy.json.JsonOutput
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import picocli.CommandLine

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class GeoclimateCLITest {


    @Test
    void runCLI() {
        Geoclimate app = new Geoclimate();
        CommandLine cmd = new CommandLine(app);
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        int exitCode = cmd.execute("-c");
        assertEquals(2, exitCode);
        exitCode = cmd.execute("-w", "-f");
        assertEquals(2, exitCode);
        exitCode = cmd.execute("-w orbisgis", "-f");
        assertEquals(2, exitCode);
        exitCode = cmd.execute("-w osm", "-f");
        assertEquals(2, exitCode);
        exitCode = cmd.execute("-w osm", "-f  /tmp/conf.json");
        assertEquals(1, exitCode);
    }

    @Disabled
    @Test
    void runCLIWorkflow() {
        String directory ="./target/geoclimate_cli"
        File dirFile = new File(directory)
        dirFile.delete()
        dirFile.mkdir()
        def osm_parameters = [
                "description" :"Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb" : [
                        "folder" : "${dirFile.absolutePath}",
                        "name" : "geoclimate_chain_db;AUTO_SERVER=TRUE",
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
        def configFile = createOSMConfigFile(osm_parameters)
        Geoclimate app = new Geoclimate();
        CommandLine cmd = new CommandLine(app);
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        int exitCode = cmd.execute("-w osm", "-f $configFile");
        assertEquals(2, exitCode);
    }

    /**
     * Create a configuration file
     * @param osmParameters
     * @return
     */
    def createOSMConfigFile(def osmParameters){
        def json = JsonOutput.toJson(osmParameters)
        File configFile = File.createTempFile("osmConfigFile",".json")
        if(configFile.exists()){
            configFile.delete()
        }
        configFile.write(json)
        return configFile.absolutePath
    }
}
