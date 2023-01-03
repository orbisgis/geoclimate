/*
 * Bundle WorldPopTools is part of the GeoClimate tool
 *
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies .
 * GeoClimate is developed by the GIS group of the DECIDE team of the
 * Lab-STICC CNRS laboratory, see <http://www.lab-sticc.fr/>.
 *
 * The GIS group of the DECIDE team is located at :
 *
 * Laboratoire Lab-STICC – CNRS UMR 6285
 * Equipe DECIDE
 * UNIVERSITÉ DE BRETAGNE-SUD
 * Institut Universitaire de Technologie de Vannes
 * 8, Rue Montaigne - BP 561 56017 Vannes Cedex
 *
 * WorldPopTools is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019-2021 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * WorldPopTools is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * WorldPopTools is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * WorldPopTools. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <https://github.com/orbisgis/geoclimate>
 * or contact directly:
 * info_at_ orbisgis.org
 */

package org.orbisgis.geoclimate.worldpoptools

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import org.orbisgis.process.GroovyProcessFactory
import org.slf4j.LoggerFactory

/**
 * Main script to access to all WorldPop utilities
 *
 * WorldPopTools offers utilities to extract data from the https://www.worldpop.org project
 *
 *
 * @author Erwan Bocher CNRS LAB-STICC
 */

abstract class WorldPopTools extends GroovyProcessFactory {

    def static Extract = new WorldPopExtract()

    public static def logger

    WorldPopTools(){
        logger = LoggerFactory.getLogger(WorldPopTools.class)
        var context = (LoggerContext) LoggerFactory.getILoggerFactory()
        context.getLogger(WorldPopTools.class).setLevel(Level.INFO)
    }

    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def debug = { obj -> logger.debug(obj.toString()) }
}