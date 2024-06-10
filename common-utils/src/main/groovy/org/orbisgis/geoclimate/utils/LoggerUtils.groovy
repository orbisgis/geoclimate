package org.orbisgis.geoclimate.utils

import org.slf4j.Logger

/**
 * GeoClimate is a geospatial processing toolbox for environmental and climate studies
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>.
 *
 * This code is part of the GeoClimate project. GeoClimate is free software;
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * GeoClimate is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult:
 * <a href="https://github.com/orbisgis/geoclimate">https://github.com/orbisgis/geoclimate</a>
 *
 */
class LoggerUtils {

    static Logger logger

    static LOGLEVEL_KEY = "org.orbisgis.geoclimate.loglevel"

    private LoggerUtils() throws IOException {
        String level = System.getProperty(LOGLEVEL_KEY)
        if (level) {
            setLoggerLevel(level)
        } else {
            setLoggerLevel("info")
        }
        logger = org.slf4j.LoggerFactory.getLogger("GeoClimate")
    }

    private static Logger getLogger() {
        if (logger == null) {
            try {
                new LoggerUtils()
            } catch (IOException e) {
                e.printStackTrace()
            }
        }
        return logger
    }

    static info(def message) {
        getLogger().info(message.toString())
    }

    static warn(def message) {
        getLogger().warn(message.toString())
    }

    static error(def message) {
        getLogger().error(message.toString())
    }

    static debug(def message) {
        getLogger().debug(message.toString())
    }

    static trace(def message) {
        getLogger().trace(message.toString())
    }

    /**
     * Utility class to change log level for all loggers
     *
     */
    static void setLoggerLevel(String loggerLevel) {
        if (loggerLevel) {
            if (loggerLevel.equalsIgnoreCase("INFO")) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", loggerLevel.toLowerCase())
            } else if (loggerLevel.equalsIgnoreCase("DEBUG")) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", loggerLevel.toLowerCase())
            } else if (loggerLevel.equalsIgnoreCase("TRACE")) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", loggerLevel.toLowerCase())
            } else if (loggerLevel.equalsIgnoreCase("OFF")) {
                System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", loggerLevel.toLowerCase())
            } else {
                throw new RuntimeException("Invalid log level. Allowed values are : INFO, DEBUG, TRACE, OFF")
            }
        }
    }

}
