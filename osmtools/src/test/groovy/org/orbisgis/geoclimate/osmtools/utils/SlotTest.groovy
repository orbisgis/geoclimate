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
package org.orbisgis.geoclimate.osmtools.utils

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

import java.text.SimpleDateFormat
import java.time.ZoneId

import static org.junit.jupiter.api.Assertions.assertEquals

/**
 * Test class for {@link Utilities}
 *
 * @author Erwan Bocher (CNRS)
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class SlotTest {

    private static format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    private static local = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    @BeforeAll
    static void beforeAll(){
        format.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"))
        local.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()))
    }

    /**
     * Test the {@link Slot#Slot(java.lang.String)} method.
     */
    @Test
    void slotTest(){
        def date = "2019-10-11T13:31:39Z"
        def slot = new Slot("Slot available after: $date, in 3 seconds.")
        assertEquals 3, slot.waitSeconds
        assertEquals format.parse(date), slot.availibility
        slot = new Slot("Slot available after: $date, in 3 seconds.".toString())
        assertEquals 3, slot.waitSeconds
        assertEquals format.parse(date), slot.availibility
    }

    /**
     * Test the {@link Slot#toString()} method.
     */
    @Test
    void toStringTest(){
        def date = "2019-10-11T13:31:39Z"
        def slot = new Slot("Slot available after: $date, in 3 seconds.")
        assertEquals "Slot available after: ${local.format(format.parse(date))}, in 3 seconds.".toString(), slot.toString()
    }
}
