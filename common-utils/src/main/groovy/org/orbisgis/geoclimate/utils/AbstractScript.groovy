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
package org.orbisgis.geoclimate.utils

abstract class AbstractScript extends Script {

    AbstractScript() {
    }

    static String uuid() { UUID.randomUUID().toString().replaceAll("-", "_") }

    static void info(def message) {
        LoggerUtils.info(message.toString())
    }

    static void warn(def message) {
        LoggerUtils.warn(message.toString())
    }

    static void error(def message) {
        LoggerUtils.error(message.toString())
    }

    static void debug(def message) {
        LoggerUtils.debug(message.toString())
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
