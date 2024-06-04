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
package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.h2.util.StringUtils
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.geoclimate.Geoindicators

import java.sql.SQLException

@BaseScript Geoindicators geoindicators


/**
 * This process computes a ground acoustic table.
 *
 * The ground table acoustic is based on a set of layers : building, road, water, vegetation  water and impervious.
 *
 * All layers are merged to find the smallest common geometry.
 *
 * The g coefficient is defined according the type of the surface.
 *
 * The type of surface is retrieved from the input layers.
 *
 * An array defined the priority order to set between layers in order to remove potential double count
 * of overlapped layers.
 *
 * The building and road geometries are not retained in the ground acoustic table.
 *
 *
 * The schema of the ground acoustic table should be defined with 5 columns :
 *
 * id_ground : unique identifier
 * the_geom : polygon or multipolygon
 * g : acoustic absorption coefficient
 * type : the type of the surface
 * layer : the layer selected to identify the surface
 *
 * See https://hal.archives-ouvertes.fr/hal-00985998/document
 * @return
 */
String groundAcousticAbsorption(JdbcDataSource datasource, String zone, String id_zone,
                                String building, String road, String water, String vegetation,
                                String impervious, String jsonFilename = "", boolean unknownArea = false) throws Exception {
    //Ground layer name
    String ground
    try {
        def outputTableName = postfix("GROUND_ACOUSTIC")
        datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_ground serial,G float, type VARCHAR, layer VARCHAR);""".toString()

        def paramsDefaultFile = this.class.getResourceAsStream("ground_acoustic_absorption.json")
        def absorption_params = Geoindicators.DataUtils.parametersMapping(jsonFilename, paramsDefaultFile)
        def default_absorption = absorption_params.default_g
        def g_absorption = absorption_params.g
        def layer_priorities = absorption_params.layer_priorities
        String filter = " where layer not in('building','road') "
        if (unknownArea) {
            filter += " or layer is null"
        }
        ground = Geoindicators.RsuIndicators.groundLayer(datasource, zone, id_zone,
                building, road, water, vegetation,
                impervious, layer_priorities)
        if (ground) {
            int rowcount = 1
            datasource.withBatch(100) { stmt ->
                datasource.eachRow("SELECT the_geom, TYPE, layer FROM $ground $filter".toString()) { row ->
                    String type = row.type
                    def layer = row.layer
                    float g_coeff = default_absorption as float
                    if (type) {
                        g_coeff = g_absorption.get(type)
                    }
                    Geometry geom = row.the_geom
                    def epsg = geom.getSRID()
                    stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${geom}',$epsg), ${rowcount++},${g_coeff}, ${StringUtils.quoteStringSQL(type)}, ${StringUtils.quoteStringSQL(layer)})".toString()
                }
            }
            datasource.dropTable(ground)
        }
        debug('Ground acoustic transformation finishes')
        return outputTableName
    } catch (SQLException e) {
        throw new SQLException("", e)
    } finally {
        datasource.dropTable(ground)
    }
}

