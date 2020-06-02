package org.orbisgis.orbisprocess.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.processmanager.process.GroovyProcessManager

@BaseScript GroovyProcessManager pm
register([BlockIndicators,
          BuildingIndicators,
          DataUtils,
          GenericIndicators,
          RsuIndicators,
          SpatialUnits,
          TypologyClassification])