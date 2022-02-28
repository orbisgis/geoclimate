package org.orbisgis.geoclimate

import picocli.CommandLine

import java.util.concurrent.Callable

/**
 * Root access point to the Geoclimate processes.
 *
 * @author Erwan Bocher (CNRS 2020)
 * @author Sylvain Palominos (UBS chaire GEOTERA)
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
 " \\___/(____)(_____)\\___)(____)(____)(_/\\/\\_)(__)(__)(__) (____)"])

class Geoclimate implements Callable<Integer> {

    public static final def SUCCESS_CODE = 0
    public static final def PROCESS_FAIL_CODE = 1
    public static final def PROCESS_INVALID_CODE = 2


    /**
     * Shortcut to run the OSM workflow
     */
    static class OSM {
        def static workflow = org.orbisgis.geoclimate.osm.OSM.WorkflowOSM.workflow()
    }

    /**
     * Shortcut to run the BDTopo_V2 workflow
     */
    static class BDTopo_V2 {
        def static workflow = org.orbisgis.geoclimate.bdtopo_v2.BDTopo_V2.WorkflowBDTopo_V2.workflow()
    }



    public static def PROPS

    /**
     * Set the logger for all the processes.
     *
     * @param logger Logger to use in the processes.
     */
    static void setLogger(def logger){
        OSM.logger = logger
        BDTopo_V2.logger = logger
    }

    @CommandLine.Option(names = ['-w'],
            defaultValue = "OSM",
            required = true,
            description = "Name of workflow :  OSM (default) or BDTOPO_V2.2")
    String workflow

    @CommandLine.Option(names = ["-f" ],
            arity = "1",
            required = true,
            description = "The configuration file used to set up the workflow")
    String configFile

    @Override
    Integer call() {
        if (workflow.trim().equalsIgnoreCase("OSM")) {
            def success = OSM.workflow.execute(input: configFile.trim())
            if (success) {
                println("The OSM workflow has been successfully executed")
                return SUCCESS_CODE
            } else {
                println("Cannot execute the OSM workflow")
                return PROCESS_FAIL_CODE
            }
        } else if (workflow.trim().equalsIgnoreCase("BDTOPO_V2.2")) {
            def success = BDTopo_V2.workflow.execute(input: configFile.trim())
            if (success) {
                println("The BDTOPO_V2.2 workflow has been successfully executed")
                return SUCCESS_CODE
            } else {
                println("Cannot execute the BDTOPO_V2.2 workflow")
                return PROCESS_FAIL_CODE
            }
        } else {
            System.out.println("Invalid workflow name. Supported values are OSM (default) or BDTOPO_V2.2")
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
        if(executionCode != SUCCESS_CODE) {
            System.exit(executionCode)
        }
    }

    static def $static_propertyMissing(String name) {
        if(!PROPS) {
            PROPS = new Properties()
            PROPS.load(Geoclimate.getResourceAsStream("geoclimate.properties"))
        }
        return PROPS.get(name)
    }
}