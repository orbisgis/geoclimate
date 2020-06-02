package org.orbisgis.orbisprocess.geoclimate.osm

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager

/**
 * Main class to access to the OSM processes
 */
@BaseScript GroovyProcessManager pm

register([FormattingForAbstractModel,
          OSMGISLayers,
          PrepareOSM,
          WorkflowOSM])
