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

import org.orbisgis.geoclimate.bdtopo.BDTopo
import picocli.CommandLine

import java.util.concurrent.Callable

/**
 * Root access point to the Geoclimate processes.
 *
 * @author Erwan Bocher (CNRS 2020-2023)
 * @author Sylvain Palominos (UBS chaire GEOTERA 2020)
 */
@CommandLine.Command(name = "Geoclimate",
        sortOptions = false,
        version = "0.0.2",
        mixinStandardHelpOptions = true,
        description = "Simple command line tool to run Geoclimate algorithms",
        header =
["  ___  ____  _____  ___  __    ____  __  __    __   ____  ____ ",
 " / __)( ___)(  _  )/ __)(  )  (_  _)(  \\/  )  /__\\ (_  _)( ___)",
 "( (_-. )__)  )(_)(( (__  )(__  _)(_  )    (  /(__)\\  )(   )__) ",
 " \\___/(____)(_____)\\___)(____)(____)(_/\\/\\_)(__)(__)(__) (____)"] )

class Geoclimate implements Callable<Integer> {

    public static final def SUCCESS_CODE = 0
    public static final def PROCESS_FAIL_CODE = 1
    public static final def PROCESS_INVALID_CODE = 2

    public static def PROPS

    @CommandLine.Option(names = ['-w'],
            defaultValue = "OSM",
            required = true,
            description = "Name of workflow :  OSM (default) BDTOPO_V2 or BDTOPO_V3")
    String workflow

    @CommandLine.Option(names = ["-f"],
            arity = "1",
            required = true,
            description = "The configuration file used to set up the workflow")
    String configFile

    @CommandLine.Option(names = ["-l"],
            required = false,
            description = "Use it to manage the log level. Allowed values are : INFO, DEBUG, TRACE, OFF\"\n  ")
    String verbose


    @Override
    Integer call() {
        if (verbose) {
            Geoindicators.WorkflowUtilities.setLoggerLevel(verbose.trim())
        } else {
            Geoindicators.WorkflowUtilities.setLoggerLevel("INFO")
        }
        if (workflow.trim().equalsIgnoreCase("OSM")) {
            println("The OSM workflow has been started.\nPlease wait...")
            def success = org.orbisgis.geoclimate.osm.OSM.WorkflowOSM.workflow().execute(input: configFile.trim())
            if (success) {
                println("The OSM workflow has been successfully executed")
                return SUCCESS_CODE
            } else {
                println("Cannot execute the OSM workflow")
                return PROCESS_FAIL_CODE
            }
        } else if (workflow.trim().equalsIgnoreCase("BDTOPO_V2")) {
            println("The BDTOPO_V2 workflow has been started.\nPlease wait...")
            def success = BDTopo.v2(configFile.trim())
            if (success) {
                println("The BDTOPO_V2 workflow has been successfully executed")
                return SUCCESS_CODE
            } else {
                println("Cannot execute the BDTOPO_V2 workflow")
                return PROCESS_FAIL_CODE
            }
        } else if (workflow.trim().equalsIgnoreCase("BDTOPO_V3")) {
            println("The BDTOPO_V3 workflow has been started.\nPlease wait...")
            def success = BDTopo.v3(configFile.trim())
            if (success) {
                println("The BDTOPO_V3 workflow has been successfully executed")
                return SUCCESS_CODE
            } else {
                println("Cannot execute the BDTOPO_V3 workflow")
                return PROCESS_FAIL_CODE
            }
        } else {
            System.out.println("Invalid workflow name. Supported values are OSM (default), BDTOPO_V2 or BDTOPO_V3")
            return PROCESS_INVALID_CODE
        }
    }

    /**
     * Run the Picocli command(s).
     *
     * @param args Geoclimate client arguments.
     */
    static void main(String[] args) {
        def executionCode = new CommandLine(new Geoclimate()).execute(args)
        if (executionCode != SUCCESS_CODE) {
            System.exit(executionCode)
        }
    }

    static def $static_propertyMissing(String name) {
        if (!PROPS) {
            PROPS = new Properties()
            PROPS.load(Geoclimate.getResourceAsStream("geoclimate.properties"))
        }
        return PROPS.get(name)
    }
}