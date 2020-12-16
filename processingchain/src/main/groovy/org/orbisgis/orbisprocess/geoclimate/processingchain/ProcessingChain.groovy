package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.orbisgis.orbisdata.processmanager.process.GroovyProcessFactory
import org.orbisgis.orbisprocess.geoclimate.geoindicators.DataUtils
import org.slf4j.LoggerFactory

/**
 * This class contains all references to the group of chains used by GeoClimate
 */
abstract class ProcessingChain extends GroovyProcessFactory {
    public static def logger = LoggerFactory.getLogger(ProcessingChain.class)

    public static GeoIndicatorsChain  = new GeoIndicatorsChain()
    public static DataUtils  = new DataUtils()
    public static FormatingDataChain  = new FormatingDataChain()

    //Utility methods
    static def uuid = { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def getUuid() { UUID.randomUUID().toString().replaceAll("-", "_") }
    static def info = { obj -> logger.info(obj.toString()) }
    static def warn = { obj -> logger.warn(obj.toString()) }
    static def error = { obj -> logger.error(obj.toString()) }
    static def getOutputTableName(prefixName, baseName){
        if (!prefixName){
            return baseName
        }
        else{
            return prefixName + "_" + baseName
        }
    }

    /**
     * Return a list of cached table names
     * @return
     */
    static def enableTableCache(){
        return System.setProperty("GEOCLIMATE_CACHE","true")
    }

    /**
     * Return a list of cached table names
     * @return
     */
    static def isTableCacheEnable(){
        return System.getProperty("GEOCLIMATE_CACHE")?true:false
    }

    /**
     * Return a list of cached table names
     * @return
     */
    static def getCachedTableNames(){
        return System.properties.findAll{it.key.startsWith("GEOCLIMATE_TABLE")}.collect{key, value -> value}
    }

    /**
     * Remove from the list of table names the cached tables
     * @return
     */
    static def removeAllCachedTableNames(def tableNames){
        if(isTableCacheEnable()) {
            tableNames.removeAll(getCachedTableNames())
        }
        return tableNames
    }

    /**
     * Return the cached table name from its identifier
     *
     * @return
     */
    static def getCachedTableName(tableIdentifier){
        return System.getProperty(tableIdentifier)
    }

    /**
     * Add a table if the System properties to cache it
     * The table is prefixed with the name GEOCLIMATE_
     *
     * @return
     */
    static void cacheTableName(baseTableName, outputTableName){
        if(isTableCacheEnable()) {
            System.setProperty("GEOCLIMATE_TABLE_" + baseTableName, outputTableName)
        }
    }

    /**
     * Clean the System properties that stores intermediate table names
     * @return
     */
    static  void clearTablesCache(){
        System.properties.removeAll {it.key.startsWith("GEOCLIMATE")}
    }
}