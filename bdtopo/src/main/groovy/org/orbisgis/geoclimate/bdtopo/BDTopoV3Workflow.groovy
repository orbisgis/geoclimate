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

import groovy.transform.BaseScript
import org.h2gis.functions.io.utility.IOMethods
import org.h2gis.postgis_jts.PostGISDBFactory
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.orbisgis.data.H2GIS
import org.orbisgis.data.jdbc.JdbcDataSource

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

/**
 * Workflow to prepare data and building geoclimate indicators with the BDTopo 3.0
 */
@BaseScript AbstractBDTopoWorkflow bdTopoWorkflow


@Override
def filterLinkedShapeFiles(def location, float distance, LinkedHashMap inputTables,
                           int sourceSRID, int inputSRID, H2GIS h2gis_datasource) {
    def formatting_geom = "the_geom"
    if (sourceSRID == 0 && sourceSRID != inputSRID) {
        formatting_geom = "st_setsrid(the_geom, $inputSRID) as the_geom"
    } else if (sourceSRID >= 0 && sourceSRID == inputSRID) {
        formatting_geom = "st_transform(the_geom, $inputSRID) as the_geom"
    }

    String outputTableName = "COMMUNE"
    //Let's process the data by location
    //Check if code is a string or a bbox
    //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
    if (location in Collection) {
        debug "Loading in the H2GIS database $outputTableName"
        def communeColumns = h2gis_datasource.getColumnNames(inputTables.commune)
        if(communeColumns.contains("INSEE_COM")) {
            if(location.size()==3){
                if(location[2]<100){
                    error("The distance to create a bbox from a point must be greater than 100 meters")
                    return
                }
                location = BDTopoUtils.bbox(location[0], location[1],location[2])
            }

            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as  SELECT
                    ST_INTERSECTION(the_geom, ST_MakeEnvelope(${location[1]},${location[0]},${location[3]},${location[2]}, $sourceSRID)) as the_geom, INSEE_COM AS CODE_INSEE  from ${inputTables.commune} where the_geom 
                    && ST_MakeEnvelope(${location[1]},${location[0]},${location[3]},${location[2]}, $sourceSRID) """.toString())
        }
        else {
            error "Cannot find a column insee_com or code_insee to filter the commune"
            return
        }
    } else if (location instanceof String) {
        debug "Loading in the H2GIS database $outputTableName"
        def communeColumns = h2gis_datasource.getColumnNames(inputTables.commune)
        if(communeColumns.contains("INSEE_COM")){
        h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT $formatting_geom, 
            INSEE_COM AS CODE_INSEE FROM ${inputTables.commune} WHERE INSEE_COM='$location' or lower(nom)='${location.toLowerCase()}'""".toString())
        }
        else {
            error "Cannot find a column insee_com or code_insee to filter the commune"
            return
        }

    }
    else {
        debug "Invalid location"
        return false
    }
    def count = h2gis_datasource."$outputTableName".rowCount
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM

        if (inputTables.batiment) {
            //Extract batiment
            outputTableName = "BATIMENT"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, 
                NATURE,	USAGE1,NB_ETAGES, HAUTEUR,
	            Z_MIN_TOIT, Z_MAX_TOIT 
	            FROM ${inputTables.batiment}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY 
                AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)""".toString())
        }

        if (inputTables.troncon_de_route) {
            //Extract route
            outputTableName = "troncon_de_route"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; 
                CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, LARGEUR, POS_SOL, SENS,
                IMPORTANCE, CL_ADMIN, NAT_RESTR FROM ${inputTables.troncon_de_route}  
                WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY 
                AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) 
                AND NATURE NOT IN ('Bac ou liaison maritime', 'Escalier')
                """.toString())
        } else {
            error "The troncon_de_route table must be provided"
            return
        }

        if (inputTables.troncon_de_voie_ferree) {
            //Extract troncon_de_voie_ferree
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "troncon_de_voie_ferree"
            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; 
                CREATE TABLE $outputTableName as SELECT ID,$formatting_geom, NATURE, LARGEUR, NB_VOIES, POS_SOL FROM ${inputTables.troncon_de_voie_ferree}  
                WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY 
                AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)""".toString())
        }

        if (inputTables.surface_hydrographique) {
            //Extract surface_hydrographique
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "surface_hydrographique"
            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as 
                SELECT ID, $formatting_geom, NATURE, POS_SOL FROM ${inputTables.surface_hydrographique}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY 
                AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)""".toString())
        }

        if (inputTables.zone_de_vegetation) {
            //Extract zone_vegetation
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "zone_de_vegetation"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName  AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.zone_de_vegetation}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.terrain_de_sport) {
            //Extract terrain_de_sport
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "terrain_de_sport"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE, NAT_DETAIL  FROM ${inputTables.terrain_de_sport}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport'".toString())

        }

        if (inputTables.construction_surfacique) {
            //Extract construction_surfacique
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier')".toString())
        }

        if (inputTables.equipement_de_transport) {
            //Extract equipement_de_transport
            outputTableName = "equipement_de_transport"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom,NATURE  FROM ${inputTables.equipement_de_transport}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.zone_d_activite_ou_d_interet) {
            //Extract zone_d_activite_ou_d_interet
            outputTableName = "zone_d_activite_ou_d_interet"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, CATEGORIE, NATURE, FICTIF 
                FROM ${inputTables.zone_d_activite_ou_d_interet}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND (CATEGORIE='Administratif' OR CATEGORIE='Enseignement' OR CATEGORIE='Santé')""".toString())
        }
        //Extract PISTE_D_AERODROME
        if (inputTables.piste_d_aerodrome) {
            outputTableName = "piste_d_aerodrome"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName  AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.piste_d_aerodrome}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        //Extract RESERVOIR
        if (inputTables.reservoir) {
            outputTableName = "RESERVOIR"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE, HAUTEUR  FROM ${inputTables.reservoir}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        //Extract aerodrome
        if (inputTables.aerodrome) {
            outputTableName = "aerodrome"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.aerodrome}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        //Extract cimetiere
        if (inputTables.cimetiere) {
            outputTableName = "cimetiere"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.cimetiere}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        //Extract limite_terre_mer
        if (inputTables.limite_terre_mer) {
            outputTableName = "limite_terre_mer"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NIVEAU  FROM ${inputTables.limite_terre_mer}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        return true

    } else {
        error "Cannot find any commune with the insee code : $location"
        return
    }
}

@Override
Integer loadDataFromPostGIS(Object input_database_properties, Object code, Object distance, Object inputTables, Object inputSRID, H2GIS h2gis_datasource) {
    def commune_location = inputTables.commune
    if (!commune_location) {
        error "The commune table must be specified to run Geoclimate"
        return
    }
    PostGISDBFactory dataSourceFactory = new PostGISDBFactory()
    Connection sourceConnection = null
    try {
        Properties props = new Properties()
        input_database_properties.forEach(props::put)
        DataSource ds = dataSourceFactory.createDataSource(props)
        sourceConnection = ds.getConnection()
    } catch (SQLException e) {
        error("Cannot connect to the database to import the data ")
    }

    if (sourceConnection == null) {
        error("Cannot connect to the database to import the data ")
        return
    }

    //Check if the commune table exists
    if (!JDBCUtilities.tableExists(sourceConnection, commune_location)) {
        error("The commune table doesn't exist")
        return
    }

    //Find the SRID of the commune table
    def commune_srid = GeometryTableUtilities.getSRID(sourceConnection, commune_location)
    if (commune_srid <= 0) {
        error("The commune table doesn't have any SRID")
        return
    }
    if (commune_srid == 0 && inputSRID) {
        commune_srid = inputSRID
    } else if (commune_srid <= 0) {
        warn "Cannot find a SRID value for the layer commune.\n" +
                "Please set a valid OGC prj or use the parameter srid to force it."
        return null
    }

    String outputTableName = "COMMUNE"
    //Let's process the data by location
    //Check if code is a string or a bbox
    //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
    if (code in Collection) {
        def communeColumns = h2gis_datasource.getColumnNames(commune_location)
        if(communeColumns.contains("INSEE_COM")) {
            if(code.size()==3){
                if(code[2]<100){
                    error("The distance to create a bbox from a point must be greater than 100 meters")
                    return
                }
                code = BDTopoUtils.bbox(code[0], code[1],code[2])
            }
            String inputTableName = """(SELECT
                    ST_INTERSECTION(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)) as the_geom, INSEE_COM as CODE_INSEE   from $commune_location where 
                    st_setsrid(the_geom, $commune_srid) 
                    && ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid) and
                    st_intersects(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)))""".toString()
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 100)
        }else {
            error "Cannot find a column insee_com or code_insee to filter the commune"
            return
        }
    } else if (code instanceof String) {
        def communeColumns = h2gis_datasource.getColumnNames(commune_location)
        if(communeColumns.contains("insee_com")){
            String inputTableName = "(SELECT st_setsrid(the_geom, $commune_srid) as the_geom, INSEE_COM as CODE_INSEE FROM $commune_location WHERE INSEE_COM='$code' or lower(nom)='${code.toLowerCase()}')"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }
        else {
            error "Cannot find a column insee_com to filter the commune"
            return
        }
    }
    def count = h2gis_datasource."$outputTableName".rowCount
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM
        def outputTableNameBatiInd = "BATIMENT"
        if (inputTables.batiment) {
            //Extract bati_indifferencie
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, HAUTEUR, USAGE1, NB_ETAGES, Z_MIN_TOIT, Z_MAX_TOIT FROM ${inputTables.batiment}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiInd"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiInd, -1, 1000)
        }
        def outputTableNameRoad = "troncon_de_route"
        if (inputTables.troncon_de_route) {
            //Extract route
            def inputTableName = """(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, 
            FRANCHISST, SENS, IMPORTANCE, CL_ADMIN, NAT_RESTR  FROM ${inputTables.troncon_de_route}  
            WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY 
            AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY)
            AND NATURE NOT IN ('Bac ou liaison maritime', 'Escalier'))""".toString()
            debug "Loading in the H2GIS database $outputTableNameRoad"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameRoad, -1, 1000)
        } else {
            error "The route table must be provided"
            return
        }

        if (inputTables.troncon_de_voie_ferree) {
            //Extract troncon_voie_ferree
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, NB_VOIES, POS_SOL, FRANCHISST FROM ${inputTables.troncon_de_voie_ferree}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "troncon_de_voie_ferree"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.surface_hydrographique) {
            //Extract surface_eau
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom FROM ${inputTables.surface_hydrographique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "surface_hydrographique"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.zone_de_vegetation) {
            //Extract zone_vegetation
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.zone_de_vegetation}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "ZONE_VEGETATION"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.terrain_de_sport) {
            //Extract terrain_sport
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE,NAT_DETAIL  FROM ${inputTables.terrain_de_sport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport')"
            outputTableName = "terrain_de_sport"
            debug "Loading in the H2GIS database $outputTableName"

            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.construction_surfacique) {
            //Extract construction_surfacique
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier'))"
            outputTableName = "construction_surfacique"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.equipement_de_transport) {
            //Extract surface_route
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom,NATURE  FROM ${inputTables.equipement_de_transport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "equipement_de_transport"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.zone_d_activite_ou_d_interet) {
            //Extract zone_d_activite_ou_d_interet
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, CATEGORIE, ORIGINE  FROM ${inputTables.zone_d_activite_ou_d_interet}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "zone_d_activite_ou_d_interet"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }
        //Extract piste_d_aerodrome
        if (inputTables.piste_d_aerodrome) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.piste_d_aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "piste_d_aerodrome"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        //Extract aerodrome
        if (inputTables.aerodrome) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "aerodrome"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        //Extract RESERVOIR
        if (inputTables.reservoir) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR  FROM ${inputTables.reservoir}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "RESERVOIR"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        //Extract RESERVOIR
        if (inputTables.cimetiere) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.cimetiere}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "cimetiere"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        sourceConnection.close()

        return commune_srid

    } else {
        error "Cannot find any commune with the insee code : $code"
        return
    }

    return false
}

@Override
List getInputTables() {
    return ["commune", "batiment", "zone_d_activite_ou_d_interet", "terrain_de_sport", "cimetiere",
            "piste_d_aerodrome", "reservoir", "construction_surfacique", "equipement_de_transport",
            "troncon_de_route", "troncon_de_voie_ferree", "surface_hydrographique",
            "zone_de_vegetation", "aerodrome", "limite_terre_mer"]
}

@Override
int getVersion() {
    return 3
}

@Override
Map formatLayers(JdbcDataSource datasource, Map layers, float distance, float hLevMin) {
    if (!hLevMin) {
        hLevMin = 3
    }
    if (!datasource) {
        error "The database to store the BD Topo data doesn't exist"
        return
    }
    info "Formating BDTopo GIS layers"

    //Prepare the existing bdtopo data in the local database
    def importPreprocess = BDTopo.InputDataLoading.loadV3(datasource, layers, distance)

    if (!importPreprocess) {
        error "Cannot prepare the BDTopo data."
        return
    }
    def zoneTable = importPreprocess.zone
    def urbanAreas = importPreprocess.urban_areas

    //Format impervious
    def finalImpervious = BDTopo.InputDataFormatting.formatImperviousLayer(datasource,
            importPreprocess.impervious)

    //Format building
    def finalBuildings = BDTopo.InputDataFormatting.formatBuildingLayer(datasource,
            importPreprocess.building, zoneTable,
            urbanAreas, hLevMin)

    //Format roads
    def finalRoads = BDTopo.InputDataFormatting.formatRoadLayer(datasource,
            importPreprocess.road,
            zoneTable)

    //Format rails
    def finalRails = BDTopo.InputDataFormatting.formatRailsLayer(datasource,
            importPreprocess.rail,
            zoneTable)


    //Format vegetation
    def finalVeget = BDTopo.InputDataFormatting.formatVegetationLayer(datasource,
            importPreprocess.vegetation,
            zoneTable)

    //Format water
    def finalHydro = BDTopo.InputDataFormatting.formatHydroLayer(datasource,
            importPreprocess.water,
            zoneTable)

    info "All layers have been formatted"

    return ["building"  : finalBuildings, "road": finalRoads, "rail": finalRails, "water": finalHydro,
            "vegetation": finalVeget, "impervious": finalImpervious, "urban_areas": urbanAreas, "zone": zoneTable]

}