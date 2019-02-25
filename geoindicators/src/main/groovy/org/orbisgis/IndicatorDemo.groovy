package org.orbisgis

import org.orbisgis.processmanager.ProcessFactory
import org.orbisgis.processmanagerapi.IProcess
import org.orbisgis.datamanagerapi.dataset.ITable

class IndicatorDemo {

    final ProcessFactory processFactory = new ProcessFactory()

    // Create a new process
    public IProcess demoProcess = processFactory.create(
            "Demo process",
            [inputA: ITable],
            [outputA : String],
            { inputA  ->
                [outputA: inputA.columnNames] }
    )

}
