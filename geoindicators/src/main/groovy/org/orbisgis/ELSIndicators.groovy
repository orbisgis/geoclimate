package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate
/**
 * This process is a pure non-sense
 * @param input Input string
 * @return a string
 * @author Elisabeth Le Saux Wiederhold
 */
static IProcess getELSIndic() {
    return processFactory.create(
            "getELSIndic",
            [input:String],
            [output:String],
            { input ->
                logger.info(input)
                [output: input]
            }
    )
}