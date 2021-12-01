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

import org.junit.jupiter.api.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.ZoneId

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

/**
 * Test class dedicated to the {@link OverpassStatus} class.
 *
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */
class OverpassStatusTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverpassStatusTest)

    private static final CONNECTION_ID = "3265774337"
    private static final CURRENT_TIME = "2019-10-11T13:31:36Z"
    private static final RATE_LIMIT = 3
    private static final SLOT_AVAILABLE = 1
    private static final SLOT_AVAILABLE_AFTER_DATE = "2019-10-11T13:31:39Z"
    private static final SLOT_AVAILABLE_AFTER_TIME = "3"
    private static final PID = "31941"
    private static final SPACE_LIMIT = "536870912"
    private static final TIME_LIMIT = "10"
    private static final START_TIME = "2019-10-11T13:31:35Z"

    private static format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    private static current = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    @BeforeAll
    static final void beforeAll(){
        format.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"))
        current.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()))
    }

    @BeforeEach
    final void beforeEach(TestInfo testInfo){
        LOGGER.info("@ ${testInfo.testMethod.get().name}()")
    }

    @AfterEach
    final void afterEach(TestInfo testInfo){
        LOGGER.info("# ${testInfo.testMethod.get().name}()")
    }

    /**
     * Test the parsing of the {@link String} overpass server status representation.
     */
    @Test
    void overpassStatusTest(){
        def testStatus = "Connected as: $CONNECTION_ID\n" +
                "Current time: $CURRENT_TIME\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "$SLOT_AVAILABLE slots available now.\n" +
                "Slot available after: $SLOT_AVAILABLE_AFTER_DATE, in $SLOT_AVAILABLE_AFTER_TIME seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$TIME_LIMIT\t$START_TIME"
        def expected = "Connected as: $CONNECTION_ID\n" +
                "Current time: ${current.format(format.parse(CURRENT_TIME))}\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "$SLOT_AVAILABLE slots available now.\n" +
                "Slot available after: ${current.format(format.parse(SLOT_AVAILABLE_AFTER_DATE))}, in $SLOT_AVAILABLE_AFTER_TIME seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$TIME_LIMIT\t${current.format(format.parse(START_TIME))}"
        def status = new OverpassStatus(testStatus)
        assertEquals expected.toString(), status.toString()

        assertEquals CONNECTION_ID, status.connectionId.toString()
        assertEquals current.format(format.parse(CURRENT_TIME)), current.format(status.time)
        assertEquals RATE_LIMIT, status.slotLimit
        assertEquals SLOT_AVAILABLE, status.slotAvailable

        assertEquals 1, status.slots.size()
        Slot slot = status.slots[0]
        assertEquals format.parse(SLOT_AVAILABLE_AFTER_DATE), slot.availibility
        assertEquals SLOT_AVAILABLE_AFTER_TIME, slot.waitSeconds.toString()

        assertEquals 1, status.runningQueries.size()
        RunningQuery runningQuery = status.runningQueries[0]
        assertEquals PID, runningQuery.pid.toString()
        assertEquals SPACE_LIMIT, runningQuery.spaceLimit.toString()
        assertEquals TIME_LIMIT, runningQuery.timeLimit.toString()
        assertEquals format.parse(START_TIME), runningQuery.startTime
    }

    /**
     * Test the {@link OverpassStatus#wait(int)} method
     */
    @Test
    void waitTest(){
        def serverFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)
        serverFormat.setTimeZone(TimeZone.getTimeZone("Etc/GMT+0"))
        def current = new Date()
        def ds5 = 5
        def ds10 = 10
        def in5Sec = Date.from(current.toInstant().plusSeconds(ds5))
        def in10Sec = Date.from(current.toInstant().plusSeconds(ds10))
        long time1 = System.currentTimeSeconds()
        def slotBeforeQuery = "Connected as: $CONNECTION_ID\n" +
                "Current time: ${serverFormat.format(current)}\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "0 slots available now.\n" +
                "Slot available after: ${serverFormat.format(in5Sec)}, in $ds5 seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$TIME_LIMIT\t${serverFormat.format(in10Sec)}"
        new OverpassStatus(slotBeforeQuery).waitForSlot(15)
        assertTrue ds5 <= System.currentTimeSeconds() - time1
        assertTrue ds10 > System.currentTimeSeconds() - time1


        current = new Date()
        def ds3 = 3
        def ds8 = 8
        def bef3Sec = Date.from(current.toInstant().minusSeconds(ds3))
        def in8Sec = Date.from(current.toInstant().plusSeconds(ds8))
        long time2 = System.currentTimeSeconds()
        def queryBeforeSlot = "Connected as: $CONNECTION_ID\n" +
                "Current time: ${serverFormat.format(current)}\n" +
                "Rate limit: $RATE_LIMIT\n" +
                "0 slots available now.\n" +
                "Slot available after: ${serverFormat.format(in8Sec)}, in $ds8 seconds.\n" +
                "Currently running queries (pid, space limit, time limit, start time):\n" +
                "$PID\t$SPACE_LIMIT\t$ds8\t${serverFormat.format(bef3Sec)}"
        new OverpassStatus(queryBeforeSlot).waitForQueryEnd(15)
        assertTrue ds8-ds3 <= System.currentTimeSeconds() - time2
        assertTrue ds8 > System.currentTimeSeconds() - time2
    }
}