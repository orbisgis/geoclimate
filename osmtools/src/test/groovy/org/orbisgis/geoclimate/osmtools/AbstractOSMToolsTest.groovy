/*
 * Bundle OSMTools is part of the GeoClimate tool
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
 * OSMTools is distributed under LGPL 3 license.
 *
 * Copyright (C) 2019-2021 CNRS (Lab-STICC UMR CNRS 6285)
 *
 *
 * OSMTools is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * OSMTools is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * OSMTools. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <https://github.com/orbisgis/geoclimate>
 * or contact directly:
 * info_at_ orbisgis.org
 */
package org.orbisgis.geoclimate.osmtools


import org.locationtech.jts.io.WKTReader

import static org.junit.jupiter.api.Assertions.assertEquals

/**
 * Abstract for OSM tests. It contains some utilities methods and static variable in order to simplify test write.
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
abstract class AbstractOSMToolsTest {

    /**WKTReader*/
    protected static def wktReader = new WKTReader();

    /** Regex for the string UUID. */
    protected static def uuidRegex = "[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}"

    /** Generation of string {@link UUID}.*/
    protected static final def uuid() { UUID.randomUUID().toString().replaceAll("-", "_") }

    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link GString}
     * @param actual Actual {@link GString}
     */
    protected static void assertGStringEquals(GString expected, GString actual) {
        assertEquals(expected.toString(), actual.toString());
    }


    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link String}
     * @param actual Actual {@link GString}
     */
    protected static void assertGStringEquals(String expected, GString actual) {
        assertEquals(expected, actual.toString());
    }

    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link GString}
     * @param actual Actual {@link String}
     */

    protected static void assertGStringEquals(GString expected, String actual) {
        assertEquals(expected.toString(), actual);
    }

    /**
     * Implementation of the {@link org.junit.jupiter.api.Assertions#assertEquals(String, String)} method to take into
     * account GString
     *
     * @param expected Expected {@link String}
     * @param actual Actual {@link String}
     */

    protected static void assertGStringEquals(String expected, String actual) {
        assertEquals(expected.toString(), actual);
    }

    /**
     * Create a sample of OSM data
     *
     * @param ds Datasource where the data should be created.
     * @param prefix Prefix of the OSM tables.
     */
    protected void createData(def ds, def prefix) {
        ds.execute """CREATE TABLE ${prefix}_node_tag (id_node int, tag_key varchar, tag_value varchar);
       INSERT INTO ${prefix}_node_tag VALUES(1, 'building', 'house'),(1, 'material', 'concrete'),
       (2, 'material', 'concrete'),(3, 'water', 'lake'),(4, 'water', 'lake'),(4, 'building', 'house');
       CREATE TABLE ${prefix}_way_tag (id_way int, tag_key varchar, tag_value varchar);
       INSERT INTO ${prefix}_way_tag VALUES(1, 'building', 'house'),(1, 'material', 'concrete')
       ,(1, 'water', 'lake');
       CREATE TABLE ${prefix}_relation_tag (id_relation int, tag_key varchar, tag_value varchar);
       INSERT INTO ${prefix}_relation_tag VALUES(1, 'building', 'house')
       ,(1, 'material', 'concrete'),(1, 'water', 'lake');
       
       CREATE TABLE ${prefix}_node(the_geom geometry, id_node int);
       INSERT INTO ${prefix}_node VALUES('POINT(0 0)', 1),('POINT(10 0)', 2),
       ('POINT(0 10)', 3),('POINT(10 10)', 4);

       CREATE TABLE ${prefix}_way_node(id_way int, id_node int, node_order int);
       INSERT INTO ${prefix}_way_node VALUES(1, 1, 1),(1, 2, 2),(1, 3, 3),
       (1, 4, 4),(1, 1, 5);

       CREATE TABLE ${prefix}_way(id_way int);
       INSERT INTO ${prefix}_way VALUES(1);

        CREATE TABLE ${prefix}_relation(id_relation int);
       INSERT INTO ${prefix}_relation VALUES(1);

        CREATE TABLE ${prefix}_way_member(id_relation int, id_way int, role varchar);
        INSERT INTO ${prefix}_way_member VALUES(1, 1, 'outer');""".toString()
    }
}
