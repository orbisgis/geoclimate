package org.orbisgis.orbisprocess.geoclimate

import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.*
import org.orbisgis.orbisprocess.geoclimate.processingchain.*
import org.orbisgis.orbisprocess.geoclimate.osm.*
import org.orbisgis.orbisprocess.geoclimate.bdtopo_v2.*
import picocli.CommandLine

import java.util.concurrent.Callable

/**
 * Root access point to the Geoindicators processes.
 */
@CommandLine.Command(name = "Geoclimate",sortOptions = false, version = "0.1",
        mixinStandardHelpOptions = true, // add --help and --version options
        description = "Simple command line tool to run Geoclimate algorithms",
        header =
["  ___  ____  _____  ___  __    ____  __  __    __   ____  ____ ",
 " / __)( ___)(  _  )/ __)(  )  (_  _)(  \\/  )  /__\\ (_  _)( ___)",
 "( (_-. )__)  )(_)(( (__  )(__  _)(_  )    (  /(__)\\  )(   )__) ",
 " \\___/(____)(_____)\\___)(____)(____)(_/\\/\\_)(__)(__)(__) (____)"])

class Geoclimate implements Callable<Integer> {

    public static def GeoIndicatorsChain  = new GeoIndicatorsChain()
    public static def DataUtils  = new DataUtils()
    public static def BuildingIndicators = new BuildingIndicators()
    public static def RsuIndicators = new RsuIndicators()
    public static def BlockIndicators = new BlockIndicators()
    public static def GenericIndicators = new GenericIndicators()
    public static def SpatialUnits = new SpatialUnits()
    public static def TypologyClassification = new TypologyClassification()
    public static def OSM = new OSM()
    public static def BDTOPO_V2 = new BDTopo_V2()

    /**
     * Set the logger for all the processes.
     *
     * @param logger Logger to use in the processes.
     */
    static void setLogger(def logger){
        OSM_Utils.logger = logger
        BDTopo_V2_Utils.logger = logger
        ProcessingChain.logger = logger
        Geoindicators.logger = logger
    }

    @CommandLine.Option(names = ['-w'], defaultValue = "OSM", required = true,description = 'Name of workflow :  OSM (default) or bdtopo_v2.2')
    String workflow

    @CommandLine.Option(names = ["-f" ],arity = "1", required = true,  description = "The configuration file used to set up the workflow")
    String configFile;

    @Override
    public Integer  call()  {
        if(workflow.trim().equalsIgnoreCase("osm")){
            IProcess process = org.orbisgis.orbisprocess.geoclimate.Geoclimate.OSM.workflow
            def success = process.execute(configurationFile: configFile.trim())
            if(success){
                System.out.println("The OSM workflow has been successfully executed")
                return 0;
            }else{
                System.out.println("Cannot execute the OSM workflow")
                return 1;
            }
        }
        else if(workflow.trim().equalsIgnoreCase("bdtopo_v2.2")){
            IProcess process = org.orbisgis.orbisprocess.geoclimate.Geoclimate.BDTOPO_V2.workflow
            def success =  process.execute(configurationFile: configFile.trim())
            if(success){
                System.out.println("The bdtopo_v2.2 workflow has been successfully executed")
                return 0
            }else{
                System.out.println("Cannot execute the bdtopo_v2.2 workflow")
                return 1
            }
        }
        else{
            System.out.println("Invalid workflow name. Supported values are OSM (default) or bdtopo_v2.2")
            return 2
        }
        System.out.println("Any workflow to run")
        return 1
    }

    /**
     * Run the Picocli command(s)
     * @param args
     */
    public static void main(String[] args) {
        System.exit(new CommandLine(new Geoclimate()).execute(args))
    }
}