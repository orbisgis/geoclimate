package org.orbisgis.geoclimate.geoindicators

import groovy.transform.BaseScript
import org.locationtech.jts.geom.Geometry
import org.orbisgis.data.H2GIS
import org.orbisgis.geoclimate.Geoindicators
import org.orbisgis.process.api.IProcess

@BaseScript Geoindicators geoindicators


/**
 *
 * See https://hal.archives-ouvertes.fr/hal-00985998/document
 * @return
 */
IProcess groundAcousticAbsorption() {
    return create {
        title "Compute a ground acoustic absorption table"
        inputs zone: String, id_zone: String, building: "", road: "", water: "", vegetation: "",
                impervious: "", datasource: H2GIS, jsonFilename: ""
        outputs ground: String
        run { zone, id_zone, building, road, water, vegetation, impervious, H2GIS datasource, jsonFilename ->

            IProcess process = Geoindicators.RsuIndicators.groundLayer()
            if(process.execute(["zone" : zone, "id_zone": id_zone,
                                        building: building, road: road, vegetation: vegetation, water: water, datasource: datasource])){

                def outputTableName = postfix("GROUND_ACOUSTIC")
                datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_ground serial,G float, type VARCHAR);""".toString()
                def paramsDefaultFile = this.class.getResourceAsStream("ground_acoustic_absorption.json")

                def absorption_params = Geoindicators.DataUtils.parametersMapping(jsonFilename, paramsDefaultFile)
                def default_absorption = absorption_params.default_g
                def g_absorption = absorption_params.g
                def layer_priorities = absorption_params.layer_priorities

                def ground = process.results.ground
                int rowcount = 1
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow("SELECT the_geom, LOW_VEGETATION_TYPE, HIGH_VEGETATION_TYPE, WATER_TYPE, IMPERVIOUS_TYPE FROM $ground where BUILDING_TYPE IS NULL AND ROAD_TYPE IS NULL".toString()) { row ->

                        float g_coeff
                        def best_types
                        def layer_name
                        layer_priorities.eachWithIndex {layer ->
                            def tmp_val =row."${layer}_TYPE".toLowerCase()
                            if(tmp_val){
                                best_types=tmp_val
                                layer_name=layer
                                return
                            }
                        }
                        //There is no type
                        if(!best_types){
                            g_coeff = default_absorption as float
                        }else{
                            g_coeff= g_absorption.get(layer_name)
                        }

                        //TODO for futur improvments, let's find the best coefficient regarding internal priorities
                        /*def listTypes = best_types.split(",") as Set
                        if (listTypes.size() == 1) {
                            def mapping = g_absorption.get(best_types)
                            if (mapping) {
                                g_coeff = mapping
                            }
                        } else {
                            type = weight_values.subMap(matching_bdtopo_values.subMap(listTypes).values()).max { it.key }.key
                        }*/
                        Geometry geom = row.the_geom
                        def epsg = geom.getSRID()
                        stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${geom}',$epsg), ${rowcount++},${g_coeff}, '${layer_name}')".toString()

                    }
                }
                debug('Ground acoustic transformation finishes')
                [outputTableName: outputTableName]
            }
        }

    }}

