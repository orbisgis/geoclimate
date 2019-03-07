package org.orbisgis

import org.orbisgis.processmanager.ProcessManager
import org.orbisgis.processmanagerapi.IProcessFactory

abstract class Geoclimate extends Script {

    public static IProcessFactory processFactory = ProcessManager.getProcessManager().factory("geoclimate")

    public static IndicatorDemo = new IndicatorDemo()
}
