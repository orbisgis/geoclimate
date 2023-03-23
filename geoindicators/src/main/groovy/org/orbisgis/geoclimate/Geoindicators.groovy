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
package org.orbisgis.geoclimate

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import org.orbisgis.geoclimate.geoindicators.*
import org.orbisgis.geoclimate.utils.AbstractScript
import org.slf4j.LoggerFactory

abstract class Geoindicators extends AbstractScript {
    public static def logger

    Geoindicators() {
        super(LoggerFactory.getLogger(Geoindicators.class))
        var context = (LoggerContext) LoggerFactory.getILoggerFactory()
        context.getLogger(Geoindicators.class).setLevel(Level.INFO)
    }

    //Processes
    public static BuildingIndicators = new BuildingIndicators()
    public static RsuIndicators = new RsuIndicators()
    public static BlockIndicators = new BlockIndicators()
    public static GenericIndicators = new GenericIndicators()
    public static SpatialUnits = new SpatialUnits()
    public static DataUtils = new DataUtils()
    public static TypologyClassification = new TypologyClassification()
    public static RoadIndicators = new RoadIndicators()
    public static PopulationIndicators = new PopulationIndicators()
    public static GridIndicators = new GridIndicators()
    public static NoiseIndicators = new NoiseIndicators()

    public static WorkflowUtilities = new WorkflowUtilities()

    //Cache the XStream models
    public static Map cacheModels = [:]

    //The whole chain to run the geoindicators
    public static WorkflowGeoIndicators = new WorkflowGeoIndicators()
    static Properties GEOCLIMATE_PROPERTIES

    //Utility methods
    static def getUuid() {
        UUID.randomUUID().toString().replaceAll("-", "_")
    }

    static def getOutputTableName(prefixName, baseName) {
        if (!prefixName) {
            return baseName
        } else {
            return prefixName + "_" + baseName
        }
    }

    static def uuid = { getUuid() }

    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def debug = { obj -> logger.debug(obj.toString()) }


    /**
     * Return a list of cached table names
     * @return
     */
    static def isTableCacheEnable() {
        return Boolean.parseBoolean(System.getProperty("GEOCLIMATE_CACHE")) ? true : false
    }

    /**
     * Return a list of cached table names
     * @return
     */
    static def enableTableCache() {
        return System.setProperty("GEOCLIMATE_CACHE", "true")
    }

    /**
     * Return a list of cached table names
     * @return
     */
    static def getCachedTableNames() {
        return System.properties.findAll { it.key.startsWith("GEOCLIMATE_TABLE") }.collect { key, value -> value }
    }

    /**
     * Remove from the list of table names the cached tables
     * @return
     */
    static def removeAllCachedTableNames(def tableNames) {
        if (isTableCacheEnable()) {
            tableNames.removeAll(getCachedTableNames())
        }
        return tableNames
    }

    /**
     * Return the cached table name from its identifier
     *
     * @return
     */
    static def getCachedTableName(tableIdentifier) {
        return System.getProperty(tableIdentifier)
    }

    /**
     * Add a table if the System properties to cache it
     * The table is prefixed with the name GEOCLIMATE_
     *
     * @return
     */
    static void cacheTableName(baseTableName, outputTableName) {
        if (isTableCacheEnable()) {
            System.setProperty("GEOCLIMATE_TABLE_" + baseTableName, outputTableName)
        }
    }

    /**
     * Clean the System properties that stores intermediate table names
     * @return
     */
    static void clearTablesCache() {
        System.properties.removeAll { it.key.startsWith("GEOCLIMATE") }
    }

    /**
     * Return the current GeoClimate version
     * @return
     */
    static def version() {
        return geoclimate_property("version")
    }

    /**
     * Return the current GeoClimate build number
     * @return
     */
    static def buildNumber() {
        return geoclimate_property("build")
    }

    /**
     * Return geoclimate properties
     * @param name
     * @return
     */
    static def geoclimate_property(String name) {
        if (!GEOCLIMATE_PROPERTIES) {
            GEOCLIMATE_PROPERTIES = new Properties()
            GEOCLIMATE_PROPERTIES.load(Geoindicators.getResourceAsStream("geoclimate.properties"))
        }
        return GEOCLIMATE_PROPERTIES.get(name)
    }

    /**
     * Return a XStream model in the cache
     * @param modelName
     * @return
     */
    static def getModel(modelName) {
        return cacheModels.get(modelName)
    }

    /**
     * Put a XStream model in the cache
     * @param modelName
     * @param xstream object
     * @return
     */
    static void putModel(String modelName, Object xsStreamModel) {
        cacheModels.put(modelName, xsStreamModel)
    }

    /**
     * Clear the cache models
     * @return
     */
    static clearCacheModels() {
        cacheModels.clear()
    }
}