package org.orbisgis.geoclimate.utils

import ch.qos.logback.classic.Logger

abstract class AbstractScript extends Script {

    public Logger logger

    AbstractScript(Logger logger) {
        this.logger = logger
    }

    static String uuid() { UUID.randomUUID().toString().replaceAll("-", "_") }

    void info(String message){
        logger.info(message)
    }

    void warn(String message){
        logger.warn(message)
    }

    void error(String message){
        logger.error(message)
    }

    void debug(String message){
        logger.debug(message)
    }

    /**
     * Postfix the given String with '_' and an UUID..
     *
     * @param name String to postfix
     * @return The postfix String
     */
    static String postfix(String name) {
        return name + "_" + UUID.randomUUID().toString().replaceAll("-", "_")
    }

    /**
     * Prefix the given String with '_' and an UUID.
     *
     * @param name String to prefix
     * @return The prefixed String
     */
    static String prefix(String name) {
        return UUID.randomUUID().toString().replaceAll("-", "_") + "_" + name
    }

    /**
     * Prefix the given String with the given prefix.
     *
     * @param prefix Prefix
     * @param name String to prefix
     * @return The prefixed String
     */
    static String prefix(String prefix, String name) {
        return prefix == null || prefix.isEmpty() ? name : prefix + "_" + name
    }

}
