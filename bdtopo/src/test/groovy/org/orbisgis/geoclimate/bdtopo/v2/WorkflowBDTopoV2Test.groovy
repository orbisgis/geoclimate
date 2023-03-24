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
package org.orbisgis.geoclimate.bdtopo.v2


import org.orbisgis.geoclimate.bdtopo.WorkflowAbstractTest

class WorkflowBDTopoV2Test extends WorkflowAbstractTest {


    @Override
    ArrayList getFileNames() {
        return ["COMMUNE", "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL", "BATI_REMARQUABLE",
                "ROUTE", "SURFACE_EAU", "ZONE_VEGETATION", "TRONCON_VOIE_FERREE",
                "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE",
                "SURFACE_ROUTE", "SURFACE_ACTIVITE"]
    }

    @Override
    int getVersion() {
        return 2
    }

    @Override
    String getFolderName() {
        return "sample_${getInseeCode()}"
    }

    @Override
    String getInseeCode() {
        return "12174"
    }
}