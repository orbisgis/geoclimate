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
 * Workflow to prepare data and building geoclimate indicators with the BDTopo 2.2
 */
@BaseScript AbstractBDTopoWorkflow bdTopoWorkflow


@Override
Integer loadDataFromPostGIS(Object input_database_properties, Object code, Object distance, Object inputTables,
                            Object inputSRID, H2GIS h2gis_datasource) throws Exception {
    def commune_location = inputTables.commune
    if (!commune_location) {
        throw new Exception("The commune table must be specified to run Geoclimate")
    }
    PostGISDBFactory dataSourceFactory = new PostGISDBFactory()
    Connection sourceConnection = null
    try {
        Properties props = new Properties()
        input_database_properties.forEach(props::put)
        DataSource ds = dataSourceFactory.createDataSource(props)
        sourceConnection = ds.getConnection()
    } catch (SQLException e) {
        throw new SQLException("Cannot connect to the database to import the data ")
    }
    if (sourceConnection == null) {
        throw new Exception("Cannot connect to the database to import the data ")
    }

    //Check if the commune table exists
    if (!JDBCUtilities.tableExists(sourceConnection, commune_location)) {
        throw new Exception("The commune table doesn't exist")
    }

    //Find the SRID of the commune table
    def commune_srid = GeometryTableUtilities.getSRID(sourceConnection, commune_location)
    if (commune_srid <= 0) {
        throw new Exception("The commune table doesn't have any SRID")
    }
    if (commune_srid == 0 && inputSRID) {
        commune_srid = inputSRID
    } else if (commune_srid <= 0) {
        throw new Exception("Cannot find a SRID value for the layer commune.\n" +
                "Please set a valid OGC prj or use the parameter srid to force it.")
    }

    String outputTableName = "COMMUNE"
    //Let's process the data by location
    //Check if code is a string or a bbox
    //The zone is a osm bounding box represented by ymin,xmin , ymax,xmax,
    if (code in Collection) {
        if (code.size() == 3) {
            if (code[2] < 100) {
                throw new Exception("The distance to create a bbox from a point must be greater than 100 meters")
            }
            code = BDTopoUtils.bbox(code[0], code[1], code[2])
        }
        String inputTableName = """(SELECT
                    ST_INTERSECTION(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)) as the_geom, CODE_INSEE  from $commune_location where 
                    st_setsrid(the_geom, $commune_srid) 
                    && ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid) and
                    st_intersects(st_setsrid(the_geom, $commune_srid), ST_MakeEnvelope(${code[1]},${code[0]},${code[3]},${code[2]}, $commune_srid)))""".toString()
        debug "Loading in the H2GIS database $outputTableName"
        IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 10)
    } else if (code instanceof String) {
        String inputTableName = "(SELECT st_setsrid(the_geom, $commune_srid) as the_geom, CODE_INSEE FROM $commune_location WHERE CODE_INSEE='$code' or lower(nom)='${code.toLowerCase()}')"
        debug "Loading in the H2GIS database $outputTableName"
        IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
    }
    def count = h2gis_datasource.getRowCount(outputTableName)
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM
        def outputTableNameBatiInd = "BATI_INDIFFERENCIE"
        if (inputTables.bati_indifferencie) {
            //Extract bati_indifferencie
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, HAUTEUR FROM ${inputTables.bati_indifferencie}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiInd"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiInd, -1, 1000)
        }
        def outputTableNameBatiIndus = "BATI_INDUSTRIEL"
        if (inputTables.bati_industriel) {
            //Extract bati_industriel
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR FROM ${inputTables.bati_industriel}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiIndus"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiIndus, -1, 1000)
        }
        def outputTableNameBatiRem = "BATI_REMARQUABLE"
        if (inputTables.bati_remarquable) {
            //Extract bati_remarquable
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR FROM ${inputTables.bati_remarquable}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            debug "Loading in the H2GIS database $outputTableNameBatiRem"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiRem, -1, 1000)
        }
        def outputTableNameRoad = "ROUTE"
        if (inputTables.route) {
            //Extract route
            def inputTableName = """(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, 
            FRANCHISST, SENS, IMPORTANCE, CL_ADMIN FROM ${inputTables.route}  WHERE 
            st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY 
            AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY)
            AND NATURE NOT IN ('Bac auto', 'Bac piéton', 'Escalier'))""".toString()
            debug "Loading in the H2GIS database $outputTableNameRoad"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameRoad, -1, 1000)
        } else {
            throw new Exception("The route table must be provided")
        }

        if (inputTables.troncon_voie_ferree) {
            //Extract troncon_voie_ferree
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, NB_VOIES, POS_SOL, FRANCHISST FROM ${inputTables.troncon_voie_ferree}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "TRONCON_VOIE_FERREE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.surface_eau) {
            //Extract surface_eau
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom , NATURE FROM ${inputTables.surface_eau}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_EAU"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.zone_vegetation) {
            //Extract zone_vegetation
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.zone_vegetation}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "ZONE_VEGETATION"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.terrain_sport) {
            //Extract terrain_sport
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.terrain_sport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport')"
            outputTableName = "TERRAIN_SPORT"
            debug "Loading in the H2GIS database $outputTableName"

            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.construction_surfacique) {
            //Extract construction_surfacique
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier'))"
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.surface_route) {
            //Extract surface_route
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom,NATURE  FROM ${inputTables.surface_route}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }

        if (inputTables.surface_activite) {
            //Extract surface_activite
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, CATEGORIE, ORIGINE  FROM ${inputTables.surface_activite}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "SURFACE_ACTIVITE"
            debug "Loading in the H2GIS database $outputTableName"
            IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000)
        }
        //Extract PISTE_AERODROME
        if (inputTables.piste_aerodrome) {
            def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.piste_aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
            outputTableName = "PISTE_AERODROME"
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
        sourceConnection.close()
        return commune_srid

    } else {
        throw new Exception("Cannot find any commune with the insee code : $code".toString())
    }
}

@Override
List getInputTables() {
    return ["commune",
            "bati_indifferencie", "bati_industriel",
            "bati_remarquable", "route",
            "troncon_voie_ferree", "surface_eau",
            "terrain_sport", "construction_surfacique",
            "surface_route", "surface_activite",
            "piste_aerodrome", "reservoir", "zone_vegetation"]
}

@Override
int getVersion() {
    return 2
}

@Override
Map formatLayers(JdbcDataSource datasource, Map layers, float distance, float hLevMin) throws Exception {
    if (!hLevMin) {
        hLevMin = 3
    }
    if (!datasource) {
        throw new Exception("The database to store the BDTopo data doesn't exist")
    }
    info "Formating BDTopo GIS layers"
    //Prepare the existing bdtopo data in the local database
    def importPreprocess = BDTopo.InputDataLoading.loadV2(datasource, layers, distance)

    if (!importPreprocess) {
        throw new Exception("Cannot prepare the BDTopo data.")
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

    debug "End of the BDTopo extract transform process."

    info "All layers have been formatted"

    return ["building"  : finalBuildings, "road": finalRoads, "rail": finalRails, "water": finalHydro,
            "vegetation": finalVeget, "impervious": finalImpervious, "urban_areas": urbanAreas, "zone": zoneTable]

}

@Override
def filterLinkedShapeFiles(def location, float distance, LinkedHashMap inputTables,
                           int sourceSRID, int inputSRID, H2GIS h2gis_datasource) throws Exception {
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
        if (location.size() == 3) {
            if (location[2] < 100) {
                throw new Exception("The distance to create a bbox from a point must be greater than 100 meters")
            }
            location = BDTopoUtils.bbox(location[0], location[1], location[2])
        }
        debug "Loading in the H2GIS database $outputTableName"
        h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as  SELECT
                    ST_INTERSECTION(the_geom, ST_MakeEnvelope(${location[1]},${location[0]},${location[3]},${location[2]}, $sourceSRID)) as the_geom, CODE_INSEE  from ${inputTables.commune} where the_geom 
                    && ST_MakeEnvelope(${location[1]},${location[0]},${location[3]},${location[2]}, $sourceSRID) """.toString())
    } else if (location instanceof String) {
        debug "Loading in the H2GIS database $outputTableName"
        h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT $formatting_geom, 
            CODE_INSEE FROM ${inputTables.commune} WHERE CODE_INSEE='$location' or lower(nom)='${location.toLowerCase()}'""".toString())
    } else {
        throw new Exception("Invalid location data type. Please set a text value or a collection of coordinates to specify a bbox")
    }
    def count = h2gis_datasource.getRowCount(outputTableName)
    if (count > 0) {
        //Compute the envelope of the extracted area to extract the thematic tables
        def geomToExtract = h2gis_datasource.firstRow("SELECT ST_EXPAND(ST_UNION(ST_ACCUM(the_geom)), ${distance}) AS THE_GEOM FROM $outputTableName".toString()).THE_GEOM

        if (inputTables.bati_indifferencie) {
            //Extract bati_indifferencie
            outputTableName = "BATI_INDIFFERENCIE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, HAUTEUR FROM ${inputTables.bati_indifferencie}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.bati_industriel) {
            //Extract bati_industriel
            outputTableName = "BATI_INDUSTRIEL"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, HAUTEUR FROM ${inputTables.bati_industriel}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.bati_remarquable) {
            //Extract bati_remarquable
            outputTableName = "BATI_REMARQUABLE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, HAUTEUR FROM ${inputTables.bati_remarquable}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.route) {
            //Extract route
            outputTableName = "ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("""DROP TABLE IF EXISTS $outputTableName ; 
            CREATE TABLE $outputTableName as SELECT ID, $formatting_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST, SENS,
            IMPORTANCE, CL_ADMIN FROM ${inputTables.route}  
            WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY 
            AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)
            AND NATURE NOT IN ('Bac auto', 'Bac piéton', 'Escalier')""".toString())
        } else {
            throw new Exception("The route table must be provided")
        }

        if (inputTables.troncon_voie_ferree) {
            //Extract troncon_voie_ferree
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "TRONCON_VOIE_FERREE"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID,$formatting_geom, NATURE, LARGEUR,NB_VOIES, POS_SOL, FRANCHISST FROM ${inputTables.troncon_voie_ferree}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.surface_eau) {
            //Extract surface_eau
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "SURFACE_EAU"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName as SELECT ID, $formatting_geom , NATURE FROM ${inputTables.surface_eau}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.zone_vegetation) {
            //Extract zone_vegetation
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "ZONE_VEGETATION"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName  AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.zone_vegetation}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())

        }

        if (inputTables.terrain_sport) {
            //Extract terrain_sport
            debug "Loading in the H2GIS database $outputTableName"
            outputTableName = "TERRAIN_SPORT"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.terrain_sport}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport'".toString())

        }

        if (inputTables.construction_surfacique) {
            //Extract construction_surfacique
            outputTableName = "CONSTRUCTION_SURFACIQUE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier')".toString())
        }

        if (inputTables.surface_route) {
            //Extract surface_route
            outputTableName = "SURFACE_ROUTE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom,NATURE  FROM ${inputTables.surface_route}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        if (inputTables.surface_activite) {
            //Extract surface_activite
            outputTableName = "SURFACE_ACTIVITE"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, CATEGORIE  FROM ${inputTables.surface_activite}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY) AND (CATEGORIE='Administratif' OR CATEGORIE='Enseignement' OR CATEGORIE='Santé')".toString())
        }
        //Extract PISTE_AERODROME
        if (inputTables.piste_aerodrome) {
            outputTableName = "PISTE_AERODROME"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName  AS SELECT ID, $formatting_geom, NATURE  FROM ${inputTables.piste_aerodrome}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }

        //Extract RESERVOIR
        if (inputTables.reservoir) {
            outputTableName = "RESERVOIR"
            debug "Loading in the H2GIS database $outputTableName"
            h2gis_datasource.execute("DROP TABLE IF EXISTS $outputTableName ; CREATE TABLE $outputTableName AS SELECT ID, $formatting_geom, NATURE, HAUTEUR  FROM ${inputTables.reservoir}  WHERE the_geom && 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(the_geom, 'SRID=$sourceSRID;$geomToExtract'::GEOMETRY)".toString())
        }
        return true

    } else {
        throw new Exception("Cannot find any commune with the insee code : $location".toString())
    }
}

