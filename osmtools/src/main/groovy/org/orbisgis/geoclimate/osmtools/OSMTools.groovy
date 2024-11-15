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


import org.orbisgis.geoclimate.osmtools.utils.TransformUtils
import org.orbisgis.geoclimate.osmtools.utils.Utilities
import org.orbisgis.geoclimate.utils.AbstractScript

/**
 * Main script to access to all processes used to extract, transform and save OSM data as GIS layers.
 *
 * @author Erwan Bocher CNRS LAB-STICC
 * @author Elisabeth Le Saux UBS LAB-STICC
 * @author Sylvain PALOMINOS (UBS LAB-STICC 2019)
 */

abstract class OSMTools extends AbstractScript {
    public static Loader Loader = new Loader()
    public static Transform Transform = new Transform()
    public static Utilities Utilities = new Utilities()
    public static TransformUtils TransformUtils = new TransformUtils()

    OSMTools() {
    }
}