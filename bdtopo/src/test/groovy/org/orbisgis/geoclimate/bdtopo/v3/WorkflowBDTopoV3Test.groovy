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
package org.orbisgis.geoclimate.bdtopo.v3


import org.junit.jupiter.api.*
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class WorkflowBDTopoV3Test extends WorkflowAbstractTest {


    public WorkflowBDTopoV3Test(){
        new File("/tmp/test_bd").mkdir()
        folder = new File("/tmp/test_bd")
    }
    @Override
    int getVersion() {
        return 3
    }

    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}"
    }

    @Override
    String getInseeCode() {
        return "12174"
    }

    @Override
    ArrayList getFileNames() {
        return ["COMMUNE", "BATIMENT", "ZONE_D_ACTIVITE_OU_D_INTERET", "TERRAIN_DE_SPORT", "CIMETIERE",
                "PISTE_D_AERODROME", "RESERVOIR", "CONSTRUCTION_SURFACIQUE", "EQUIPEMENT_DE_TRANSPORT",
                "TRONCON_DE_ROUTE", "TRONCON_DE_VOIE_FERREE", "SURFACE_HYDROGRAPHIQUE",
                "ZONE_DE_VEGETATION", "AERODROME"]
    }
}
