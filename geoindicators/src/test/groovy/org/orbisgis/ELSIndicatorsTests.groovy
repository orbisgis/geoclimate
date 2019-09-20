package org.orbisgis

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals

class ELSIndicatorsTests {

    @Test
    void simpleTest(){
        def process = Geoclimate.ELSIndicators.getELSIndic()
        process.execute([input:"a"])
        process.results.each {result ->
             assertEquals "a", result.value
        }
    }
}
