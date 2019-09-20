package org.orbisgis

process = Geoclimate.ELSIndicators.getELSIndic()
process.execute([input:"a"])
process.results.each {result ->
    println result.value
}
