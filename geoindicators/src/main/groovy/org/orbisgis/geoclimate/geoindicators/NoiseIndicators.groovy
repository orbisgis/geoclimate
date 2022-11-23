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
        outputs ground_acoustic: String
        run { zone, id_zone, building, road, water, vegetation, impervious, H2GIS datasource, jsonFilename ->

            def paramsDefaultFile = this.class.getResourceAsStream("ground_acoustic_absorption.json")
            def absorption_params = Geoindicators.DataUtils.parametersMapping(jsonFilename, paramsDefaultFile)
            def default_absorption = absorption_params.default_g
            def g_absorption = absorption_params.g
            def layer_priorities = absorption_params.layer_priorities

            IProcess process = Geoindicators.RsuIndicators.groundLayer()
            if(process.execute(["zone" : zone, "id_zone": id_zone,
                                        "building": building, "road": road, "vegetation": vegetation,
                                "water": water,"impervious":impervious, datasource: datasource])){
                def outputTableName = postfix("GROUND_ACOUSTIC")
                datasource.execute """ drop table if exists $outputTableName;
                CREATE TABLE $outputTableName (THE_GEOM GEOMETRY, id_ground serial,G float, type VARCHAR);""".toString()

                def ground = process.results.ground
                int rowcount = 1
                datasource.withBatch(100) { stmt ->
                    datasource.eachRow("SELECT the_geom, TYPE FROM $ground where layer NOT in ('building' ,'road')".toString()) { row ->
                        def type = row.type
                        float g_coeff
                        //There is no type
                        if(!type){
                            g_coeff = default_absorption as float
                        }else{
                            g_coeff= g_absorption.get(type) as float
                        }
                        Geometry geom = row.the_geom
                        def epsg = geom.getSRID()
                        stmt.addBatch "insert into $outputTableName values(ST_GEOMFROMTEXT('${geom}',$epsg), ${rowcount++},${g_coeff}, '${type}')".toString()
                  }
                }
                debug('Ground acoustic transformation finishes')
                [ground_acoustic: outputTableName]
            }
        }

    }}

