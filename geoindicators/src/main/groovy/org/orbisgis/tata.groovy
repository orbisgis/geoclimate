package org.orbisgis

process = Geoclimate.SylvainIndicators.getSimpleIndicator()

process.execute([input: "Hello"])
process.results.each { result ->
    println(result.value)
}