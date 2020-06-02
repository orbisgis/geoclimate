package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager


/**
 * Script to manage and access to the BDTOPO processes
 */
@BaseScript GroovyProcessManager pm

register([AbstractTablesInitialization,
          BDTopoGISLayers,
          InputDataFormatting,
          PrepareBDTopo,
          WorkflowBDTopo_V2])
