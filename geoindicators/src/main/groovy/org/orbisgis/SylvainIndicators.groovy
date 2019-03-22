package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

static IProcess getSimpleIndicator(){
    return processFactory.create('title',
            [input:String],
            [output:String],
            { input ->
                logger.info(input)
                [output: input]
            })
}