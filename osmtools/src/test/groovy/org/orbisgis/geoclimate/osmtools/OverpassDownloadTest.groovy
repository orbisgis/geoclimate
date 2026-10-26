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
package org.orbisgis.geoclimate.osmtools

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.geoclimate.osmtools.utils.Utilities

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream

import static org.junit.jupiter.api.Assertions.*

/**
 * Test the download of the Overpass data against a local fake Overpass server,
 * so no query is sent to the real Overpass servers.
 */
class OverpassDownloadTest {

    static final String OSM_DATA = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="Overpass API">
  <node id="1" lat="43.7" lon="7.29"/>
</osm>"""

    @TempDir
    File folder

    HttpServer server
    AtomicInteger calls = new AtomicInteger()
    /** Answers returned by the fake server, one per call, the last one is repeated */
    List<Closure> answers = []

    long previousDelay

    @BeforeEach
    void startServer() {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
        server.createContext("/api/interpreter", { HttpExchange exchange ->
            int call = calls.getAndIncrement()
            def answer = answers[Math.min(call, answers.size() - 1)]
            answer(exchange)
            exchange.close()
        })
        server.createContext("/api/status", { HttpExchange exchange ->
            send(exchange, 200, "Rate limit: 2\n1 slots available now.\n")
            exchange.close()
        })
        server.start()
        System.setProperty("OVERPASS_ENDPOINT", "http://localhost:${server.address.port}/api")
        previousDelay = Utilities.OVERPASS_RETRY_DELAY
        Utilities.OVERPASS_RETRY_DELAY = 10
    }

    @AfterEach
    void stopServer() {
        server.stop(0)
        System.clearProperty("OVERPASS_ENDPOINT")
        System.clearProperty("OVERPASS_MAX_ATTEMPTS")
        Utilities.OVERPASS_RETRY_DELAY = previousDelay
    }

    static void send(HttpExchange exchange, int code, String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(code, bytes.length)
        exchange.responseBody.withStream { it.write(bytes) }
    }

    static String readGzip(File file) {
        return new GZIPInputStream(new FileInputStream(file)).getText("UTF-8")
    }

    @Test
    void retryAfterTooManyRequests() {
        answers = [{ send(it, 429, "Too many requests") },
                   { send(it, 504, "Gateway timeout") },
                   { send(it, 200, OSM_DATA) }]
        def file = new File(folder, "data.osm.gz")
        assertTrue OSMTools.Utilities.executeOverPassQuery("node(1);out;", file)
        assertEquals 3, calls.get()
        //The server did not gzip the answer, the file must be gzipped anyway
        assertEquals OSM_DATA, readGzip(file)
        //No temporary file left
        assertEquals 1, folder.listFiles().length
    }

    @Test
    void serverStatus() {
        def status = OSMTools.Utilities.getServerStatus()
        assertNotNull status
        assertTrue status.contains("slots available now")
    }

    @Test
    void noRetryOnBadRequest() {
        answers = [{ send(it, 400, "<p><strong style=\"color:#FF0000\">Error</strong>: line 1: parse error</p>") }]
        def file = new File(folder, "data.osm.gz")
        assertFalse OSMTools.Utilities.executeOverPassQuery("bad query", file)
        assertEquals 1, calls.get()
        assertFalse file.exists()
    }

    @Test
    void incompleteAnswerIsNotKept() {
        System.setProperty("OVERPASS_MAX_ATTEMPTS", "2")
        answers = [{
                       send(it, 200, OSM_DATA.replace("</osm>",
                               "<remark> runtime error: Query timed out in \"query\" at line 1 after 25 seconds. </remark>\n</osm>"))
                   }]
        def file = new File(folder, "data.osm.gz")
        assertFalse OSMTools.Utilities.executeOverPassQuery("node(1);out;", file)
        assertEquals 2, calls.get()
        //Nothing must be cached
        assertEquals 0, folder.listFiles().length
    }

    @Test
    void fallbackEndpoint() {
        System.setProperty("OVERPASS_MAX_ATTEMPTS", "1")
        answers = [{ send(it, 200, OSM_DATA) }]
        def port = server.address.port
        //The main endpoint does not answer, the fallback one is used
        System.setProperty("OVERPASS_ENDPOINT", "http://localhost:1/api")
        System.setProperty("OVERPASS_FALLBACK_ENDPOINTS", "http://localhost:${port}/api/")
        try {
            def file = new File(folder, "data.osm")
            assertTrue OSMTools.Utilities.executeOverPassQuery("node(1);out;", file)
            assertEquals OSM_DATA, file.getText("UTF-8")
        } finally {
            System.clearProperty("OVERPASS_FALLBACK_ENDPOINTS")
        }
    }
}