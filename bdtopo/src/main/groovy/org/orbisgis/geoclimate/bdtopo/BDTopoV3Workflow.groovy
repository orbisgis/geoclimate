package org.orbisgis.geoclimate.bdtopo

import org.h2gis.functions.io.utility.IOMethods
import org.h2gis.postgis_jts.PostGISDBFactory
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.orbisgis.data.H2GIS

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

/**
 * Workflow to building geoclimate indicators with the BDTopo 3.0
 */
class BDTopoV3Workflow extends AbstractBDTopoWorkflow{


    @Override
    boolean loadDataFromPostGIS(Object input_database_properties, Object code, Object distance, Object inputTables, Object inputSRID, H2GIS h2gis_datasource) {
        def commune_location = inputTables.commune
        if (!commune_location) {
            logger.error "The commune table must be specified to run Geoclimate"
            return
        }
        PostGISDBFactory dataSourceFactory = new PostGISDBFactory();
        Connection sourceConnection = null;
        try {
            Properties props = new Properties();
            input_database_properties.forEach(props::put);
            DataSource ds = dataSourceFactory.createDataSource(props);
            sourceConnection = ds.getConnection()
        } catch (SQLException e) {
            logger.error("Cannot connect to the database to import the data ");
        }

        if (sourceConnection == null) {
            logger.error("Cannot connect to the database to import the data ");
            return
        }

        //Check if the commune table exists
        if (!JDBCUtilities.tableExists(sourceConnection, commune_location)) {
            logger.error("The commune table doesn't exist");
            return
        }

        //Find the SRID of the commune table
        def commune_srid = GeometryTableUtilities.getSRID(sourceConnection, commune_location)
        if (commune_srid <= 0) {
            logger.error("The commune table doesn't have any SRID");
            return
        }
        if (commune_srid == 0 && inputSRID) {
            commune_srid = inputSRID
        } else if (commune_srid <= 0) {
            logger.warn "Cannot find a SRID value for the layer commune.\n" +
                    "Please set a valid OGC prj or use the parameter srid to force it."
            return null
        }

        String outputTableName = "COMMUNE"
        //Let's process the data by location
        //Check if code is a string or a bbox
        //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
        if (code in Collection) {
            //def tmp_insee = code.join("_")
            String inputTableName = """(SELECT
                    ST_INTERSECTION(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)) as the_geom, CODE_INSEE  from $commune_location where 
                    st_setsrid(the_geom, $commune_srid) 
                    && ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid) and
                    st_intersects(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)))""".toString()
            logger.debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 10);
        } else if (code instanceof String) {
            String inputTableName = "(SELECT st_setsrid(the_geom, $commune_srid) as the_geom, CODE_INSEE FROM $commune_location WHERE CODE_INSEE='$code' or lower(nom)='${code.toLowerCase()}')"
            logger.debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
        }
        def count = h2gis_datasource."$outputTableName".rowCount
        if (count > 0) {
            //Compute the envelope of the extracted area to extract the thematic tables
            def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM
            def outputTableNameBatiInd = "BATIMENT"
            if (inputTables.batiment) {
                //Extract bati_indifferencie
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, HAUTEUR, USAGE1, NB_ETAGES, Z_MIN_TOIT, Z_MAX_TOIT FROM ${inputTables.batiment}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                logger.debug "Loading in the H2GIS database $outputTableNameBatiInd"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiInd, -1, 1000);
            }
            def outputTableNameRoad = "troncon_de_route"
            if (inputTables.troncon_de_route) {
                //Extract route
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST, SENS FROM ${inputTables.troncon_de_route}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                logger.debug "Loading in the H2GIS database $outputTableNameRoad"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameRoad, -1, 1000);
            } else {
                error "The route table must be provided"
                return
            }

            if (inputTables.troncon_de_voie_ferree) {
                //Extract troncon_voie_ferree
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTables.troncon_de_voie_ferree}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "troncon_de_voie_ferree"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.surface_hydrographique) {
                //Extract surface_eau
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom FROM ${inputTables.surface_hydrographique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "surface_hydrographique"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.zone_de_vegetation) {
                //Extract zone_vegetation
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.zone_de_vegetation}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "ZONE_VEGETATION"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.terrain_de_sport) {
                //Extract terrain_sport
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE,NAT_DETAIL  FROM ${inputTables.terrain_de_sport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport')"
                outputTableName = "terrain_de_sport"
                logger.debug "Loading in the H2GIS database $outputTableName"

                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.construction_surfacique) {
                //Extract construction_surfacique
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier'))"
                outputTableName = "construction_surfacique"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.equipement_de_transport) {
                //Extract surface_route
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom,NATURE  FROM ${inputTables.equipement_de_transport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "equipement_de_transport"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.zone_d_activite_ou_d_interet) {
                //Extract zone_d_activite_ou_d_interet
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, CATEGORIE, ORIGINE  FROM ${inputTables.zone_d_activite_ou_d_interet}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "zone_d_activite_ou_d_interet"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }
            //Extract piste_d_aerodrome
            if (inputTables.piste_d_aerodrome) {
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.piste_d_aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "piste_d_aerodrome"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            //Extract aerodrome
            if (inputTables.aerodrome) {
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "aerodrome"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            //Extract RESERVOIR
            if (inputTables.reservoir) {
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR  FROM ${inputTables.reservoir}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "RESERVOIR"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            //Extract RESERVOIR
            if (inputTables.cimetiere) {
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.cimetiere}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "cimetiere"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            sourceConnection.close();

            return commune_srid

        } else {
            logger.error "Cannot find any commune with the insee code : $code"
            return
        }

        return false
    }

    @Override
    Map getInputTables() {
        return ["commune", "batiment", "zone_d_activite_ou_d_interet", "terrain_de_sport,cimetiere",
                "piste_d_aerodrome", "reservoir", "construction_surfacique", "equipement_de_transport",
                "troncon_de_route", "troncon_de_voie_ferree", "surface_hydrographique",
                "zone_de_vegetation", "aerodrome", "limite_terre_mer"]
    }

    @Override
    int getVersion() {
        return 3
    }
}
