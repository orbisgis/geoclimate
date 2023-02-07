package org.orbisgis.geoclimate.bdtopo

import org.h2gis.functions.io.utility.IOMethods
import org.h2gis.postgis_jts.PostGISDBFactory
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.JDBCUtilities
import org.orbisgis.data.H2GIS
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

class BDTopoV2Workflow extends AbstractBDTopoWorkflow {


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
            def outputTableNameBatiInd = "BATI_INDIFFERENCIE"
            if (inputTables.bati_indifferencie) {
                //Extract bati_indifferencie
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, HAUTEUR FROM ${inputTables.bati_indifferencie}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                logger.debug "Loading in the H2GIS database $outputTableNameBatiInd"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiInd, -1, 1000);
            }
            def outputTableNameBatiIndus = "BATI_INDUSTRIEL"
            if (inputTables.bati_industriel) {
                //Extract bati_industriel
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR FROM ${inputTables.bati_industriel}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                logger.debug "Loading in the H2GIS database $outputTableNameBatiIndus"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiIndus, -1, 1000);
            }
            def outputTableNameBatiRem = "BATI_REMARQUABLE"
            if (inputTables.bati_remarquable) {
                //Extract bati_remarquable
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, HAUTEUR FROM ${inputTables.bati_remarquable}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                logger.debug "Loading in the H2GIS database $outputTableNameBatiRem"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameBatiRem, -1, 1000);
            }
            def outputTableNameRoad = "ROUTE"
            if (inputTables.route) {
                //Extract route
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST, SENS FROM ${inputTables.route}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                logger.debug "Loading in the H2GIS database $outputTableNameRoad"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableNameRoad, -1, 1000);
            } else {
                error "The route table must be provided"
                return
            }

            if (inputTables.troncon_voie_ferree) {
                //Extract troncon_voie_ferree
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE, LARGEUR, POS_SOL, FRANCHISST FROM ${inputTables.troncon_voie_ferree}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "TRONCON_VOIE_FERREE"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.surface_eau) {
                //Extract surface_eau
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom FROM ${inputTables.surface_eau}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "SURFACE_EAU"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.zone_vegetation) {
                //Extract zone_vegetation
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.zone_vegetation}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "ZONE_VEGETATION"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.terrain_sport) {
                //Extract terrain_sport
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.terrain_sport}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND NATURE='Piste de sport')"
                outputTableName = "TERRAIN_SPORT"
                logger.debug "Loading in the H2GIS database $outputTableName"

                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.construction_surfacique) {
                //Extract construction_surfacique
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.construction_surfacique}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY) AND (NATURE='Barrage' OR NATURE='Ecluse' OR NATURE='Escalier'))"
                outputTableName = "CONSTRUCTION_SURFACIQUE"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.surface_route) {
                //Extract surface_route
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom,NATURE  FROM ${inputTables.surface_route}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "SURFACE_ROUTE"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }

            if (inputTables.surface_activite) {
                //Extract surface_activite
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, CATEGORIE, ORIGINE  FROM ${inputTables.surface_activite}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "SURFACE_ACTIVITE"
                logger.debug "Loading in the H2GIS database $outputTableName"
                IOMethods.exportToDataBase(sourceConnection, inputTableName, h2gis_datasource.getConnection(), outputTableName, -1, 1000);
            }
            //Extract PISTE_AERODROME
            if (inputTables.piste_aerodrome) {
                def inputTableName = "(SELECT ID, st_setsrid(the_geom, $commune_srid) as the_geom, NATURE  FROM ${inputTables.piste_aerodrome}  WHERE st_setsrid(the_geom, $commune_srid) && 'SRID=$commune_srid;$geomToExtract'::GEOMETRY AND ST_INTERSECTS(st_setsrid(the_geom, $commune_srid), 'SRID=$commune_srid;$geomToExtract'::GEOMETRY))"
                outputTableName = "PISTE_AERODROME"
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

            sourceConnection.close();

            return commune_srid

        } else {
            logger.error "Cannot find any commune with the insee code : $code"
            return
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
    Map loadAndFormatData2(JdbcDataSource datasource, Map layers, float distance, float hLevMin ) {
        if (!hLevMin) {
            hLevMin = 3
        }
        if (!datasource) {
            logger.error "The database to store the BD Topo data doesn't exist"
            return
        }
        //Prepare the existing bdtopo data in the local database
        def importPreprocess = BDTopo.InputDataLoading.loadV2(datasource, layers, distance)

        if(!importPreprocess) {
            logger.error "Cannot prepare the BDTopo data."
            return
        }
        def zoneTable = importPreprocess.zone
        def urbanAreas = importPreprocess.urban_areas

        //Format impervious
        def processFormatting = BDTopo.InputDataFormatting.formatImperviousLayer(datasource,
                                   importPreprocess.impervious)
        def finalImpervious = processFormatting.outputTableName

        //Format building
        processFormatting = BDTopo.InputDataFormatting.formatBuildingLayer( datasource,
                                    importPreprocess.building,zoneTable,
                                    urbanAreas, hLevMin)
        def finalBuildings = processFormatting.outputTableName

        //Format roads
        processFormatting = BDTopo.InputDataFormatting.formatRoadLayer(datasource,
                                    importPreprocess.road,
                                   zoneTable)
        def finalRoads = processFormatting.outputTableName

        //Format rails
        processFormatting = BDTopo.InputDataFormatting.formatRailsLayer( datasource,
                                   importPreprocess.rail,
                                   zoneTable)
        def finalRails = processFormatting.outputTableName


        //Format vegetation
        processFormatting = BDTopo.InputDataFormatting.formatVegetationLayer(datasource,
                                    importPreprocess.vegetation,
                                   zoneTable)
        def finalVeget = processFormatting.outputTableName

        //Format water
        processFormatting = BDTopo.InputDataFormatting.formatHydroLayer(datasource,
                                    importPreprocess.water,
                                   zoneTable)
        def finalHydro = processFormatting.outputTableName

        logger.debug "End of the BDTopo extract transform process."

        return ["building": finalBuildings, "road": finalRoads, "rail": finalRails, "water": finalHydro,
                "vegetation"   : finalVeget, "impervious": finalImpervious, "urban_areas": urbanAreas, "zone": zoneTable]

    }




/** This method creates the Table from the Abstract model and feed them with the BDTopo data according to some
 * defined rules (i.e. certain types of buildings or vegetation are transformed into a generic type in the abstract
 * model). Then default values are set (or values are corrected) and the quality of the input data is assessed and
 * returned into a statistic table for each layer. For further information, cf. each of the four processes used in the
 * mapper.
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 * @param distBuffer The distance (exprimed in meter) used to compute the buffer area around the ZONE
 * @param distance The distance (exprimed in meter) used to compute the extended area around the ZONE
 * @param idZone The Zone id
 * @param tableZoneName The table name in which the zone area is stored
 * @param tableBuildIndifName The table name in which the undifferentiated ("Indifférencié" in french) buildings are stored
 * @param tableBuildIndusName The table name in which the industrial buildings are stored
 * @param tableBuildRemarqName The table name in which the remarkable ("Remarquable" in french) buildings are stored
 * @param tableRoadName The table name in which the roads are stored
 * @param tableRailName The table name in which the rail ways are stored
 * @param tableHydroName The table name in which the hydrographic areas are stored
 * @param tableVegetName The table name in which the vegetation areas are stored
 * @param tableImperviousSportName The table name in which the impervious sport areas are stored
 * @param tableImperviousBuildSurfName The table name in which the impervious surfacic buildings are stored
 * @param tableImperviousRoadSurfName The table name in which the impervious road areas are stored
 * @param tableImperviousActivSurfName The table name in which the impervious activities areas are stored
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param inputSRID to force the SRID of the input data
 *
 * @return outputBuilding Table name in which the (ready to be used in the GeoIndicators part) buildings are stored
 * @return outputRoad Table name in which the (ready to be used in the GeoIndicators part) roads are stored
 * @return outputRail Table name in which the (ready to be used in the GeoIndicators part) rail ways are stored
 * @return outputHydro Table name in which the (ready to be used in the GeoIndicators part) hydrographic areas are stored
 * @return outputVeget Table name in which the (ready to be used in the GeoIndicators part) vegetation areas are stored
 * @return outputImpervious Table name in which the (ready to be used in the GeoIndicators part) impervious areas are stored
 * @return outputUrbanAreas Table name in which the (ready to be used in the GeoIndicators part) urban areas are stored
 * @return outputZone Table name in which the (ready to be used in the GeoIndicators part) zone is stored
 *
 */
    def loadAndFormatData(
            JdbcDataSource datasource,
            float distance = 1000,
            String tableCommuneName = "",
            String tableBuildIndifName = "",
            String tableBuildIndusName = "",
            String tableBuildRemarqName = "",
            String tableRoadName = "",
            String tableRailName = "",
            String tableHydroName = "",
            String tableVegetName = "",
            String tableImperviousSportName = "",
            String tableImperviousBuildSurfName = "",
            String tableImperviousRoadSurfName = "",
            String tableImperviousActivSurfName = "",
            String tablePiste_AerodromeName = "",
            String tableReservoirName = "",
            int hLevMin = 3) {


    }
}

