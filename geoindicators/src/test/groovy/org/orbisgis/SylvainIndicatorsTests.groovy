package org.orbisgis

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals

class SylvainIndicatorsTests {

    @Test
    void simpleTest(){
        def process = Geoclimate.SylvainIndicators.getSimpleIndicator()

        process.execute([input: "Hello"])
        process.results.each { result ->
            assertEquals "Hello", result.value
        }
    }
}
