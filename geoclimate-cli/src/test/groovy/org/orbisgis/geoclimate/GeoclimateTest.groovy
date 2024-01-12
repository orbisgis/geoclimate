/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
package org.orbisgis.geoclimate

import groovy.json.JsonOutput
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import picocli.CommandLine

import java.util.regex.Pattern

/**
 * Test class dedicated to {@link Geoclimate}.
 *
 * @author Erwan Bocher (CNRS 2020)
 * @author Sylvain PALOMINOS (UBS chaire GEOTERA 2020)
 */
class GeoclimateTest {

    @TempDir
    static File folder

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
        exitCode = cmd.execute("-w osm", "-f  /tmp/conf.json", "-l")
        assert 2 == exitCode
        exitCode = cmd.execute("-w osm", "-f  /tmp/conf.json", "-l debug")
        assert 1 == exitCode
    }

    @Test
    void propertiesTest() {
        assert "1.0.0" == Geoclimate.version
        assert Pattern.compile("^\\d{4}-\\d{2}-\\d{2}").matcher(Geoclimate.build).matches()
    }

    @Test
    void runCLIWorkflow() {
        def osmParameters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultst in a folder",
                "geoclimatedb": [
                        "folder": folder.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": ["Pont de veyle"]],
                "output"      : [
                        "folder": folder.absolutePath],
                "parameters"  :
                        [
                                "indicatorUse" : ["TEB"],
                                "svfSimplified": true,
                                "hLevMin"      : 3,
                        ]
        ]

        def json = JsonOutput.toJson(osmParameters)
        def configFile = File.createTempFile("osmConfigFile", ".json")
        if (configFile.exists()) {
            configFile.delete()
        }
        configFile.write(json)

        def app = new Geoclimate()
        def cmd = new CommandLine(app)
        def sw = new StringWriter()
        cmd.setOut(new PrintWriter(sw))
        def exitCode = cmd.execute("-w osm", "-f $configFile", "-l OFF")
        assert 0 == exitCode
    }


    @Disabled
    @Test
    void runOSMWorkflow() {
        def osmParameters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultset in a folder",
                "geoclimatedb": [
                        "folder": folder.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "locations": ["Pont de veyle"]],
                "output"      : [
                        "folder": folder.absolutePath],
                "parameters"  : [
                        "rsu_indicators" : [
                                "indicatorUse"  : ["LCZ"],
                                "svfSimplified" : false,
                                "estimateHeight": false
                        ],
                        "grid_indicators": [
                                "x_size"    : 1000,
                                "y_size"    : 1000,
                                "indicators": ["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                                               "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "LCZ_FRACTION"]
                        ]
                ]
        ]
        Geoclimate.OSM.workflow(osmParameters)
    }

    @Disabled
    @Test
    void runBDTopoWorkflow() {
        def wParameters = [
                "description" : "Example of configuration file to run the OSM workflow and store the resultset in a folder",
                "geoclimatedb": [
                        "folder": folder.absolutePath,
                        "name"  : "geoclimate_chain_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder": ["path"     : "../geoclimate/bdtopo/src/test/resources/org/orbisgis/geoclimate/bdtopo/v2.sample_12174",
                                   "locations": ["12174"]]],
                "output"      : [
                        "folder": folder.absolutePath],
                "parameters"  : [
                        "rsu_indicators" : [
                                "indicatorUse": ["LCZ"]
                        ],
                        "grid_indicators": [
                                "x_size"    : 1000,
                                "y_size"    : 1000,
                                "indicators": ["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                                               "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "LCZ_FRACTION"]
                        ]
                ]
        ]
        Geoclimate.BDTopo.workflow.v2(wParameters)
    }
}
