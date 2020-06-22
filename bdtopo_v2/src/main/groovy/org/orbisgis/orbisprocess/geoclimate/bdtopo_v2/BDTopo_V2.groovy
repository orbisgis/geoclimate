package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import groovy.transform.Field
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager


/**
 * Script to manage and access to the BDTOPO processes
 */
@BaseScript GroovyProcessManager pm

@Field static AbstractTablesInitialization abstractTablesInitialization
@Field static BDTopoGISLayers bDTopoGISLayers
@Field static InputDataFormatting inputDataFormatting
@Field static PrepareBDTopo prepareBDTopo
@Field static WorkflowBDTopo_V2 workflowBDTopo_V2

abstractTablesInitialization = register(AbstractTablesInitialization)
bDTopoGISLayers = register(BDTopoGISLayers)
inputDataFormatting = register(InputDataFormatting)
prepareBDTopo = register(PrepareBDTopo)
workflowBDTopo_V2 = register(WorkflowBDTopo_V2)