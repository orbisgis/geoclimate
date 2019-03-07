package org.orbisgis

import org.h2.engine.Database
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

    /*******************/
    /** building_area **/
    /*******************/
    /**
     * Script to compute the footprint area of each building stored in a given Table.
     *
     * INPUTS (the user has to specify - mandatory):
     *  - inputA: (String) the name of the input ITable where are stored the buildings
     *  - inputB: (String) the name of the output ITable
     *  - inputC: (String) the name of the field (within the outputA ITable) where will be stored the building area values
     *  - inputD: (String) the field name corresponding to the building geometry (in the inputA ITable)
     *  - inputE: (Database) the Database object where is stored the inputA ITable
     *
     * OUTPUTS (the user can recover):
     *  - outputA: (String) the name of the output ITable (which is actually equal to inputB...)
     *
     * References:
     * Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
     * Steiniger, S., Lange, T., Burghardt, D., Weibel, R., 2008. An approach for the classification of urban building structures based on discriminant analysis techniques. Trans. GIS 12, 31–59.
     */
    // Definition of the inputs, of the outputs and of the methods used for the indicator calculation
    public IProcess buildingArea = processFactory.create(
            "Building area calculation",
            [inputA: String, inputB: String, inputC: String, inputD: String, inputE: Database],
            [outputA : String],
            { inputA, inputB, inputC, inputD, inputE ->


                // Processes used for the indicator calculation
                inputE.execute("DROP TABLE IF EXISTS $inputB; CREATE TABLE $inputB AS SELECT ST_AREA($inputD) AS $inputC FROM $inputA".toString())


                [outputA: inputB] }
    )

}
