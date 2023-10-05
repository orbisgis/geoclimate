package org.orbisgis.geoclimate.utils

import org.slf4j.Logger

abstract class AbstractScript extends Script {

    public Logger logger


    AbstractScript(Class aClass) {
        this.logger = org.slf4j.LoggerFactory.getLogger(aClass.toString())
    }


    static String uuid() { UUID.randomUUID().toString().replaceAll("-", "_") }

    void info(def message) {
        logger.info( message.toString())
    }

    void warn(def message) {
        logger.warn(message.toString())
    }

    void error(def message) {
        logger.error(message.toString())
    }

    void debug(def message) {
        logger.debug(message.toString())
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
     * Postfix the given String with the given postfix.
     *
     * @param postfix Postfix
     * @param name String to postfix
     * @return The postfix String
     */
    static String postfix(String name, String postfix) {
        return postfix == null || postfix.isEmpty() ? name : name + "_" + postfix;
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
