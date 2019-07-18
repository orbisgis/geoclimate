package org.orbisgis.geoindicators

import org.orbisgis.processmanager.GroovyProcessFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Root access point to the Geoindicators processes.
 */
abstract class Geoindicators extends GroovyProcessFactory {

    public static Logger logger = LoggerFactory.getLogger(Geoindicators.class)

    static def uuid(){UUID.randomUUID().toString().replaceAll("-", "_")}


    public static BuildingIndicators = new BuildingIndicators()
    public static RsuIndicators = new RsuIndicators()
    public static BlockIndicators = new BlockIndicators()
    public static GenericIndicators = new GenericIndicators()
    public static SpatialUnits = new SpatialUnits()
    public static DataUtils = new DataUtils()
    public static TypologyClassification = new TypologyClassification()

}
