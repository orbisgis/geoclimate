package org.orbisgis.geoclimate.bdtopo_v2

import groovy.transform.BaseScript
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.orbisgis.data.jdbc.JdbcDataSource
import org.orbisgis.process.api.IProcess

@BaseScript BDTopo_V2 BDTopo_V2

/**
 * This script prepares the BDTopo data already imported in H2GIS database for a specific ZONE
 *
 * @param datasource A connexion to a database (H2GIS), in which the data to process will be stored
 * @param commune The table name that represents the commune to process
 * @param batiment The table name in which the buildings are stored
 * @param tableRoadName The table name in which the roads are stored
 * @param tableRailName The table name in which the rail ways are stored
 * @param tableHydroName The table name in which the hydrographic areas are stored
 * @param tableVegetName The table name in which the vegetation areas are stored
 * @param tableImperviousSportName The table name in which the impervious sport areas are stored
 * @param tableImperviousBuildSurfName The table name in which the building impervious surfaces are stored
 * @param tableImperviousRoadSurfName The table name in which the impervious road areas are stored
 * @param tableImperviousActivSurfName The table name in which the impervious activities areas are stored
 * @param distance The distance (expressed in meter) used to compute the extended area around the ZONE
 * @param inputSRID to force the SRID of the input data
 *
 * @return building Table name in which the (ready to feed the GeoClimate model) buildings are stored
 * @return road Table name in which the (ready to feed the GeoClimate model) roads are stored
 * @return rail Table name in which the (ready to feed the GeoClimate model) rail ways are stored
 * @return water Table name in which the (ready to feed the GeoClimate model) hydrographic areas are stored
 * @return vegetation Table name in which the (ready to feed the GeoClimate model) vegetation areas are stored
 * @return impervious Table name in which the (ready to feed the GeoClimate model) impervious areas are stored
 * @return zone Table name in which the (ready to feed the GeoClimate model) zone is stored
 */
IProcess prepareBDTopo3Data() {
    return create {
        title 'Prepare the BDTopo 3.0 data already imported in the H2GIS database'
        id "prepare"
        inputs datasource: JdbcDataSource,
                layers: [],
                distance: 1000
        outputs building: String,
                road: String,
                rail: String,
                water: String,
                vegetation: String,
                impervious: String,
                urban_areas: String,
                zone: String,
                coastline: String
        run { datasource, List layers, distance ->
            if (!layers) {
                error "Please set a valid list of layers"
                return
            }
            debug('Import the BDTopo data')

            // If the Commune table is empty, then the process is stopped
            if (!layers*.toLowerCase().contains("commune")) {
                error 'The process has been stopped since the table Commnune is empty'
                return
            }

            // -------------------------------------------------------------------------------
            // Control the SRIDs from input tables
            def allowedLayers = ["commune", "batiment", "zone_d_activite_ou_d_interet", "terrain_de_sport,cimetiere",
                                 "piste_d_aerodrome", "reservoir", "construction_surfacique", "equipement_de_transport",
                                 "troncon_de_route", "troncon_de_voie_ferree", "surface_hydrographique",
                                 "zone_de_vegetation", "aerodrome", "limite_terre_mer"]

            def srid = -1
            def tablesExist = []
            def con = datasource.getConnection()
            // For each tables in the list, we check the SRID and compare to the srid variable. If different, the process is stopped
            for (String name : layers) {
                if (name) {
                    def nameLCase = name.toLowerCase()
                    if (nameLCase in allowedLayers) {
                        if (datasource.hasTable(name)) {
                            def hasRow = datasource.firstRow("select 1 as id from ${name} limit 1".toString())
                            if (hasRow) {
                                tablesExist << nameLCase
                                def currentSrid = GeometryTableUtilities.getSRID(con, TableLocation.parse(name, datasource.getDataBaseType()))
                                if (srid == -1) {
                                    srid = currentSrid
                                } else {
                                    if (currentSrid == 0) {
                                        error "The process has been stopped since the table $name has a no SRID"
                                        return
                                    } else if (currentSrid > 0 && srid != currentSrid) {
                                        error "The process has been stopped since the table $name has a different SRID from the others"
                                        return
                                    }
                                }
                            }
                        }
                    }

                }
            }

            // -------------------------------------------------------------------------------
            // Check if the input files are present
            // If the following tables does not exists, we create corresponding empty tables
            if (!tablesExist.contains("batiment")) {
                batiment = "BATIMENT"
                datasource.execute("""DROP TABLE IF EXISTS batiment; 
                CREATE TABLE batiment (THE_GEOM geometry(polygon, $srid), 
                ID CHARACTER VARYING(24),
	            NATURE CHARACTER VARYING(34),
	            USAGE1 CHARACTER VARYING(22),
	            USAGE2 CHARACTER VARYING(22),
	            LEGER CHARACTER VARYING(3),
	            ETAT CHARACTER VARYING(15),
	            NB_LOGTS INTEGER,
	            NB_ETAGES INTEGER,
	            MAT_MURS CHARACTER VARYING(2),
	            MAT_TOITS CHARACTER VARYING(2),
	            HAUTEUR DOUBLE PRECISION,
	            Z_MIN_SOL DOUBLE PRECISION,
	            Z_MIN_TOIT DOUBLE PRECISION,
	            Z_MAX_TOIT DOUBLE PRECISION,
	            Z_MAX_SOL DOUBLE PRECISION);""".toString())
            }

            if (!tablesExist.contains("troncon_de_route")) {
                datasource.execute("""DROP TABLE IF EXISTS TRONCON_DE_ROUTE;  
                CREATE TABLE TRONCON_DE_ROUTE (THE_GEOM geometry(linestring, $srid), ID varchar, LARGEUR DOUBLE PRECISION, NATURE varchar, POS_SOL integer, FRANCHISST varchar, SENS varchar);""".toString())
            }
            if (!tablesExist.contains("troncon_de_voie_ferree")) {
                datasource.execute("""DROP TABLE IF EXISTS troncon_de_voie_ferree;  
                CREATE TABLE troncon_de_voie_ferree (THE_GEOM geometry(linestring, $srid), ID varchar, NATURE varchar, POS_SOL integer, FRANCHISST varchar);""".toString())
            }
            if (!tablesExist.contains("surface_hydrographique")) {
                datasource.execute("""DROP TABLE IF EXISTS surface_hydrographique;  
                CREATE TABLE surface_hydrographique (THE_GEOM geometry(polygon, $srid), ID varchar);""".toString())
            }
            if (!tablesExist.contains("zone_de_vegetation")) {
                datasource.execute("""DROP TABLE IF EXISTS zone_de_vegetation; 
                CREATE TABLE zone_de_vegetation (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);""".toString())
            }
            if (!tablesExist.contains("terrain_de_sport")) {
                datasource.execute("""DROP TABLE IF EXISTS TERRAIN_DE_SPORT; 
                CREATE TABLE TERRAIN_DE_SPORT (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar, NAT_DETAIL varchar);""".toString())
            }
            if (!tablesExist.contains("construction_surfacique")) {
                datasource.execute("""DROP TABLE IF EXISTS construction_surfacique; 
                CREATE TABLE construction_surfacique (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);""".toString())
            }
            if (!tablesExist.contains("equipement_de_transport")) {
                datasource.execute("""DROP TABLE IF EXISTS equipement_de_transport; 
                CREATE TABLE equipement_de_transport (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);""".toString())
            }
            if (!tablesExist.contains("zone_d_activite_ou_d_interet")) {
                datasource.execute("""DROP TABLE IF EXISTS zone_d_activite_ou_d_interet; 
                CREATE TABLE zone_d_activite_ou_d_interet (THE_GEOM geometry(polygon, $srid), 
                ID varchar, CATEGORIE varchar, ORIGINE varchar);""".toString())
            }
            if (!tablesExist.contains("piste_d_aerodrome")) {
                datasource.execute("""DROP TABLE IF EXISTS piste_d_aerodrome; 
                CREATE TABLE piste_d_aerodrome (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);""".toString())
            }

            if (!tablesExist.contains("aerodrome")) {
                datasource.execute("""DROP TABLE IF EXISTS aerodrome; 
                CREATE TABLE aerodrome (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);""".toString())
            }

            if (!tablesExist.contains("reservoir")) {
                datasource.execute("""DROP TABLE IF EXISTS reservoir; 
                CREATE TABLE reservoir (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar, HAUTEUR integer);""".toString())
            }

            if (!tablesExist.contains("cimetiere")) {
                datasource.execute("""DROP TABLE IF EXISTS cimetiere; 
                CREATE TABLE cimetiere (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar, HAUTEUR integer);""".toString())
            }

            //Here we prepare the BDTopo data

            //1- Create (spatial) indexes if not already exists on the input layers
            datasource.execute("""
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_COMMUNE ON commune (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATIMENT ON BATIMENT (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_troncon_de_route ON troncon_de_route (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_troncon_de_voie_ferree ON troncon_de_voie_ferree (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_surface_hydrographique ON surface_hydrographique (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_zone_de_vegetation ON zone_de_vegetation (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_terrain_de_sport ON terrain_de_sport (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_construction_surfacique ON construction_surfacique (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_equipement_de_transport ON equipement_de_transport (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_zone_d_activite_ou_d_interet ON zone_d_activite_ou_d_interet (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_piste_d_aerodrome ON piste_d_aerodrome (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_aerodrome ON aerodrome (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_cimetiere ON cimetiere (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_reservoir ON reservoir (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_cimetiere ON cimetiere (the_geom)
            """.toString())

            //2- Preparation of the study area (zone_xx)

            def zoneTable = postfix("ZONE")
            datasource.execute("""
            DROP TABLE IF EXISTS $zoneTable;
            CREATE TABLE $zoneTable AS SELECT ST_FORCE2D(the_geom) as the_geom, INSEE_COM AS ID_ZONE  FROM commune;
            CREATE SPATIAL INDEX IF NOT EXISTS idx_geom_ZONE ON $zoneTable (the_geom);
            -- Create a bbox around the input commune
            DROP TABLE IF EXISTS ZONE_EXTENDED;
            CREATE TABLE ZONE_EXTENDED AS SELECT ST_EXPAND(the_geom, $distance) as the_geom FROM $zoneTable;
            CREATE SPATIAL INDEX ON ZONE_EXTENDED (the_geom);
                """.toString())

            // 3. Prepare the Building table that are in the study area
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_BUILDING;
            CREATE TABLE INPUT_BUILDING 
            AS 
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, 
            a.* EXCEPT(the_geom) FROM BATIMENT a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
            """.toString())

            //4. Prepare the Road table (from the layer "troncon_de_route") that are in the study area (ZONE_BUFFER)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_ROAD;
            CREATE TABLE INPUT_ROAD 
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.* EXCEPT(the_geom) FROM troncon_de_route a, ZONE_EXTENDED 
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())


            //5. Prepare the Rail table (from the layer "troncon_de_voie_ferree") that are in the study area (ZONE)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_RAIL;
            CREATE TABLE INPUT_RAIL
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.* EXCEPT(the_geom)  FROM troncon_de_voie_ferree a, $zoneTable
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())


            //6. Prepare the Hydrography table (from the layer "surface_hydrographique") that are in the study area (ZONE_EXTENDED)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_HYDRO;
            CREATE TABLE INPUT_HYDRO (THE_GEOM geometry, ID_SOURCE varchar(24), ZINDEX integer, TYPE varchar)
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 0, a.NATURE FROM surface_hydrographique a, ZONE_EXTENDED 
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0
            union all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 0, a.NATURE  FROM terrain_de_sport a, ZONE_EXTENDED 
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) 
            and NATURE = 'Bassin de natation';
            """.toString())


            //7. Prepare the Vegetation table (from the layers "zone_de_vegetation", "piste_d_aerodrome", "terrain_de_sport") that are in the study area (ZONE_EXTENDED)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_VEGET;
            CREATE TABLE INPUT_VEGET (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0 FROM zone_de_vegetation a, ZONE_EXTENDED b 
            WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0
            FROM piste_d_aerodrome a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en herbe'
             UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NAT_DETAIL, 0
            FROM terrain_de_sport a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NAT_DETAIL in ('Terrain de football', 'Terrain de rugby')
            ;
            """.toString())


            //8. Prepare the Urban areas
            def input_urban_areas = postfix "INPUT_URBAN_AREAS"
            datasource.execute(""" DROP TABLE IF EXISTS TMP_SURFACE_ACTIVITE;
            CREATE TABLE TMP_SURFACE_ACTIVITE (THE_GEOM geometry, ID varchar(24), NATURE VARCHAR) 
            AS SELECT ST_FORCE2D(ST_MAKEVALID(ST_CollectionExtract(ST_INTERSECTION(a.THE_GEOM, B.THE_GEOM), 3))) as the_geom, a.ID,            
            CASE WHEN a.CATEGORIE = 'Administratif ou militaire' 
            AND a.NATURE IN ('Administration centrale de l''Etat' , 'Aire d''accueil des gens du voyage' ,
            'Autre service déconcentré de l''Etat' , 'Borne', 'Capitainerie','Caserne de pompiers' , 'Divers public ou administratif', 
            'Etablissement extraterritorial' , 'Etablissement pénitentiaire', 'Hôtel de collectivité' , 'Hôtel de département' , 
            'Hôtel de région' , 'Mairie' , 'Maison forestière',  'Palais de justice' , 'Police' , 'Poste' , 'Préfecture' , 
            'Préfecture de région' , 'Siège d''EPCI' , 'Sous-préfecture') THEN 'government'
            WHEN a.CATEGORIE = 'Administratif ou militaire'   AND a.NATURE IN ('Borne frontière', 'Camp militaire non clos', 'Caserne', 
            'Caserne de pompiers' ,'Champ de tir' ,  'Enceinte militaire',  'Gendarmerie' ,  'Ouvrage militaire', 'Surveillance maritime') THEN 'military'
            WHEN a.CATEGORIE= 'Science et enseignement' THEN 'education'
            WHEN a.CATEGORIE='Santé' THEN 'healthcare' 
            WHEN a.CATEGORIE ='Culture et loisirs' THEN 'entertainment_arts_culture'
            WHEN a.CATEGORIE ='Transport' THEN 'transportation'
            WHEN a.CATEGORIE ='Industriel et commercial' and a.NATURE IN ( 'Divers commercial' , 'Marché') THEN 'commercial'
            WHEN a.CATEGORIE ='Industriel et commercial' 
            and a.NATURE IN ('Aquaculture', 'Carrière' ,'Centrale électrique' , 'Déchèterie' , 
            'Divers agricole'  , 'Divers industriel', 'Elevage' , 'Mine' , 'Usine' , 'Zone industrielle') THEN 'industrial'
            WHEN a.CATEGORIE ='Gestion des eaux' THEN 'industrial'
            WHEN a.CATEGORIE ='Sport' THEN 'sport'
            WHEN a.CATEGORIE = 'Religieux' THEN 'religious'
            ELSE 'unknown' END AS TYPE
            FROM zone_d_activite_ou_d_interet a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.FICTIF='Non';
            DROP TABLE IF EXISTS $input_urban_areas;
            CREATE TABLE $input_urban_areas (THE_GEOM geometry, ID_SOURCE varchar(24),TYPE VARCHAR, id_urban INTEGER)
            AS SELECT THE_GEOM, ID, NATURE, EXPLOD_ID AS id_urban 
            FROM ST_EXPLODE('(select * from TMP_SURFACE_ACTIVITE where NATURE != ''unknown'')') ;
            """.toString())


            //9. Prepare the Impervious areas table (from the layers "terrain_de_sport", "CONSTRUCTION_SURFACIQUE",
            // "equipement_de_transport" and "PISTE_AERODROME") that are in the study area (ZONE)

            def input_impervious = postfix "INPUT_IMPERVIOUS"
            datasource.execute("""
            DROP TABLE IF EXISTS $input_impervious;
            CREATE TABLE $input_impervious (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE VARCHAR, id_impervious INTEGER) AS        
            SELECT the_geom, ID,TYPE,CAST((row_number() over()) as Integer) from (      
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'sport' as type
            FROM terrain_de_sport a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NAT_DETAIL not in ('Terrain de football', 'Terrain de rugby')
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 
            'building' as type
            FROM CONSTRUCTION_SURFACIQUE a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE in ('Barrage','Ecluse','Dalle')
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'transport' as type 
            FROM equipement_de_transport a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT  the_geom, ID,NATURE
            FROM TMP_SURFACE_ACTIVITE where NATURE != 'unknown'
            UNION all
            SELECT  the_geom, ID,'cemetery'
            FROM cimetiere
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'transport' as type
            FROM piste_d_aerodrome a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en dur') as foo;
            """.toString())

            //10. Prepare the coastline table (from the layer "limite_terre_mer") that are in the study area (ZONE)
            String coastline
            if (tablesExist.contains("limite_terre_mer")) {
                coastline = "INPUT_COASTLINE"
                datasource.execute("""
            DROP TABLE IF EXISTS $coastline;
            CREATE TABLE $coastline
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.* EXCEPT(the_geom)  FROM limite_terre_mer a, $zoneTable
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.NIVEAU = 'Plus hautes eaux';
            """.toString())
            }


            //11. Clean tables
            datasource.execute("""
            DROP TABLE IF EXISTS  ZONE_EXTENDED,TMP_SURFACE_ACTIVITE; """.toString())
            debug('The BDTopo 3.0 data has been prepared')
            return [building   : "INPUT_BUILDING",
                    road       : "INPUT_ROAD",
                    rail       : "INPUT_RAIL", water: "INPUT_HYDRO",
                    vegetation : "INPUT_VEGET", impervious: "INPUT_IMPERVIOUS",
                    urban_areas: "INPUT_URBAN_AREAS",
                    coastline  : coastline, zone: zoneTable
            ]
        }
    }
}


