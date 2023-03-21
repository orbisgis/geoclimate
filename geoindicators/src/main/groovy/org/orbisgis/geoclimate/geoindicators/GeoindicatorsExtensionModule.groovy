package org.orbisgis.geoclimate.geoindicators

import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS

/**
 * A module to extend on the fly some classes
 */

    /**
     * Utilities to save a geometry to a file
     * @param geometry input geometry
     * @param h2GIS input h2GIS database
     * @param filePath path for the file
     * @return
     */
    static String save(Geometry geometry, H2GIS h2GIS, String filePath) {
        return h2GIS.save("(SELECT ST_GEOMFROMTEXT('${geometry}',${geometry.getSRID()}) as the_geom, CAST(1 as integer) as id)", filePath, true)
    }
