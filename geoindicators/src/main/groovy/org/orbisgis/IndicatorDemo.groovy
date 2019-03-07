package org.orbisgis

import groovy.transform.BaseScript
import org.orbisgis.datamanagerapi.dataset.ITable
import org.orbisgis.processmanagerapi.IProcess

@BaseScript Geoclimate geoclimate

// Create a new process
static IProcess getDemoProcess() {
    return processFactory.create(
            "Demo process",
            [inputA: ITable],
            [outputA: String],
            { inputA ->
                [outputA: inputA.columnNames]
            }
    )
}