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
package org.orbisgis.geoclimate.bdtopo

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

import static org.junit.jupiter.api.Assertions.assertNull

class WorkflowParametersTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    static File folder

    @Test
    void workflowWrongMapOfWeights() {
        def bdTopoParameters = [
                "description" : "Example of configuration file to run the BDTopo workflow and store the results in a folder",
                "geoclimatedb": [
                        "folder": folder.absolutePath,
                        "name"  : "bdtopo_workflow_db;AUTO_SERVER=TRUE",
                        "delete": true
                ],
                "input"       : [
                        "folder": ["path"     : ".../processingChain",
                                   "locations": ["56350"]]],
                "output"      : [
                        "database":
                                ["user"    : "sa",
                                 "password": "sa",
                                 "url"     : "jdbc:h2://${folder.absolutePath + File.separator + "geoclimate_chain_db_output;AUTO_SERVER=TRUE"}",
                                 "tables"  : ["building_indicators": "building_indicators",
                                              "block_indicators"   : "block_indicators",
                                              "rsu_indicators"     : "rsu_indicators",
                                              "rsu_lcz"            : "rsu_lcz",
                                              "zone"               : "zone"]]],
                "parameters"  :
                        ["distance"    : 0,
                         "hLevMin"     : 3,
                         rsu_indicators: [
                                 "indicatorUse" : ["LCZ", "TEB", "UTRF"],
                                 "svfSimplified": true,
                                 "mapOfWeights" :
                                         ["sky_view_factor"             : 1,
                                          "aspect_ratio"                : 1,
                                          "building_surface_fraction"   : 1,
                                          "impervious_surface_fraction" : 1,
                                          "pervious_surface_fraction"   : 1,
                                          "height_of_roughness_elements": 1,
                                          "terrain_roughness_class"     : 1]]
                        ]
        ]
        assertNull BDTopo.v2(bdTopoParameters)
        assertNull BDTopo.v3(bdTopoParameters)
    }
}
