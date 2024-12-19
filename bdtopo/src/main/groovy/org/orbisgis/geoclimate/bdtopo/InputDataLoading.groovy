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
import org.h2gis.utilities.GeometryTableUtilities
import org.h2gis.utilities.TableLocation
import org.orbisgis.data.jdbc.JdbcDataSource


@BaseScript BDTopo bdTopo

/**
 * This script prepares the BDTopo V2.0 data already imported in H2GIS database for a specific ZONE
 *
 * @param datasource A connexion to a database (H2GIS), in which the data to process will be stored
 * @param commune The table name that represents the commune to process
 * @param bati_indifferencie The table name in which the undifferentiated ("Indifférencié" in french) buildings are stored
 * @param bati_industriel The table name in which the industrial buildings are stored
 * @param bati_remarquable The table name in which the remarkable ("Remarquable" in french) buildings are stored
 * @param route The table name in which the roads are stored
 * @param troncon_voie_ferree The table name in which the rail ways are stored
 * @param surface_eau The table name in which the hydrographic areas are stored
 * @param zone_vegetation The table name in which the vegetation areas are stored
 * @param terrain_sport The table name in which the impervious sport areas are stored
 * @param construction_surfacique The table name in which the building impervious surfaces are stored
 * @param surface_route The table name in which the impervious road areas are stored
 * @param surface_activite The table name in which the impervious activities areas are stored
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
 * @return urban_areas Table name in which the (ready to feed the GeoClimate model) urban areas are stored
 */
def loadV2(
        JdbcDataSource datasource, Map layers = ["commune"            : "",
                                                 "bati_indifferencie" : "", "bati_industriel": "",
                                                 "bati_remarquable"   : "", "route": "",
                                                 "troncon_voie_ferree": "", "surface_eau": "",
                                                 "terrain_sport"      : "", "construction_surfacique": "",
                                                 "surface_route"      : "", "surface_activite": "",
                                                 "piste_aerodrome"    : "", "reservoir": "", "zone_vegetation": ""],
        float distance = 1000) throws Exception {

    debug('Import the BDTopo data')

    def commune = layers.commune
    // If the Commune table is empty, then the process is stopped
    if (!commune) {
        throw new IllegalArgumentException('The process has been stopped since the table Commnune is empty')
    }
    debug('Import the BDTopo data')


    // -------------------------------------------------------------------------------
    // Control the SRIDs from input tables
    def srid = -1
    def tablesExist = [:]
    def con = datasource.getConnection()
    // For each tables in the list, we check the SRID and compare to the srid variable. If different, the process is stopped
    layers.each { layer ->
        String name = layer.getValue()
        if (name) {
            if (datasource.hasTable(name)) {
                def hasRow = datasource.firstRow("select 1 as id from ${name} limit 1")
                if (hasRow) {
                    tablesExist.put(layer.key, name)
                    def currentSrid = GeometryTableUtilities.getSRID(con, TableLocation.parse(name, datasource.getDataBaseType()))
                    if (srid == -1) {
                        srid = currentSrid
                    } else {
                        if (currentSrid == 0) {
                            throw new IllegalArgumentException("The process has been stopped since the table $name has a no SRID")
                        } else if (currentSrid > 0 && srid != currentSrid) {
                            throw new IllegalArgumentException("The process has been stopped since the table $name has a different SRID from the others")
                        }
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------------
    // Check if the input files are present

    // If the COMMUNE table does not exist or is empty, then the process is stopped
    if (!tablesExist.get("commune")) {
        throw new IllegalArgumentException('The process has been stopped since the table commune does not exist or is empty')
    }

    // If the following tables does not exists, we create corresponding empty tables
    String bati_indifferencie = tablesExist.get("bati_indifferencie")
    if (!bati_indifferencie) {
        bati_indifferencie = "bati_indifferencie"
        datasource.execute("""DROP TABLE IF EXISTS $bati_indifferencie; CREATE TABLE $bati_indifferencie (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer);""".toString())
    }
    String bati_industriel = tablesExist.get("bati_industriel")
    if (!bati_industriel) {
        bati_industriel = "bati_industriel"
        datasource.execute("DROP TABLE IF EXISTS $bati_industriel; CREATE TABLE $bati_industriel (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);".toString())
    }
    String bati_remarquable = tablesExist.get("bati_remarquable")
    if (!bati_remarquable) {
        bati_remarquable = "bati_remarquable"
        datasource.execute("DROP TABLE IF EXISTS $bati_remarquable;  CREATE TABLE $bati_remarquable (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);".toString())
    }
    String route = tablesExist.get("route")
    if (!route) {
        route = "route"
        datasource.execute("""DROP TABLE IF EXISTS $route; 
        CREATE TABLE $route (THE_GEOM geometry(linestring, $srid), ID varchar, 
        LARGEUR DOUBLE PRECISION, NATURE varchar, POS_SOL integer, FRANCHISST varchar, SENS varchar,
        IMPORTANCE VARCHAR, CL_ADMIN VARCHAR, NB_VOIES INTEGER);""".toString())
    }
    String troncon_voie_ferree = tablesExist.get("troncon_voie_ferree")
    if (!troncon_voie_ferree) {
        troncon_voie_ferree = "troncon_voie_ferree"
        datasource.execute("DROP TABLE IF EXISTS $troncon_voie_ferree;  CREATE TABLE $troncon_voie_ferree (THE_GEOM geometry(linestring, $srid), ID varchar, NATURE varchar, POS_SOL integer, FRANCHISST varchar,LARGEUR DOUBLE PRECISION, NB_VOIES INTEGER);".toString())
    }
    String surface_eau = tablesExist.get("surface_eau")
    if (!surface_eau) {
        surface_eau = "surface_eau"
        datasource.execute("DROP TABLE IF EXISTS $surface_eau;  CREATE TABLE $surface_eau (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE VARCHAR);".toString())
    }
    String zone_vegetation = tablesExist.get("zone_vegetation")
    if (!zone_vegetation) {
        zone_vegetation = "zone_vegetation"
        datasource.execute("DROP TABLE IF EXISTS $zone_vegetation; CREATE TABLE $zone_vegetation (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
    }
    String terrain_sport = tablesExist.get("terrain_sport")
    if (!terrain_sport) {
        terrain_sport = "terrain_sport"
        datasource.execute("DROP TABLE IF EXISTS $terrain_sport; CREATE TABLE $terrain_sport (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
    }
    String construction_surfacique = tablesExist.get("construction_surfacique")
    if (!construction_surfacique) {
        construction_surfacique = "construction_surfacique"
        datasource.execute("DROP TABLE IF EXISTS $construction_surfacique; CREATE TABLE $construction_surfacique (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
    }
    String surface_route = tablesExist.get("surface_route")
    if (!surface_route) {
        surface_route = "surface_route"
        datasource.execute("DROP TABLE IF EXISTS $surface_route; CREATE TABLE $surface_route (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
    }
    String surface_activite = tablesExist.get("surface_activite")
    if (!surface_activite) {
        surface_activite = "surface_activite"
        datasource.execute("DROP TABLE IF EXISTS $surface_activite; CREATE TABLE $surface_activite (THE_GEOM geometry(polygon, $srid), ID varchar, CATEGORIE varchar, ORIGINE varchar);".toString())
    }
    String piste_aerodrome = tablesExist.get("piste_aerodrome")
    if (!piste_aerodrome) {
        piste_aerodrome = "piste_aerodrome"
        datasource.execute("DROP TABLE IF EXISTS $piste_aerodrome; CREATE TABLE $piste_aerodrome (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);".toString())
    }
    String reservoir = tablesExist.get("reservoir")
    if (!reservoir) {
        reservoir = "reservoir"
        datasource.execute("DROP TABLE IF EXISTS $reservoir; CREATE TABLE $reservoir (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar, HAUTEUR integer);".toString())
    }

    //Here we prepare the BDTopo data

    //1- Create (spatial) indexes if not already exists on the input layers

    datasource.execute("""
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_COMMUNE ON $commune (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDIFFERENCIE ON $bati_indifferencie (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDUSTRIEL ON $bati_industriel (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_REMARQUABLE ON $bati_remarquable (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ROUTE ON $route (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_TRONCON_VOIE_FERREE ON $troncon_voie_ferree (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_EAU ON $surface_eau (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ZONE_VEGETATION ON $zone_vegetation (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_TERRAIN_SPORT ON $terrain_sport (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_CONSTRUCTION_SURFACIQUE ON $construction_surfacique (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_ROUTE ON $surface_route (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_ACTIVITE ON $surface_activite (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_PISTE_AERODROME ON $piste_aerodrome (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_RESERVOIR ON $reservoir (the_geom);
            """.toString())

    //2- Preparation of the study area (zone_xx)
    def zoneTable = postfix("ZONE")
    datasource.execute("""
            DROP TABLE IF EXISTS $zoneTable;
            CREATE TABLE $zoneTable AS SELECT ST_FORCE2D(the_geom) as the_geom, CODE_INSEE AS ID_ZONE  FROM $commune;
            CREATE SPATIAL INDEX IF NOT EXISTS idx_geom_ZONE ON $zoneTable (the_geom);
            -- Generation of a rectangular area (bbox) around the studied commune
            DROP TABLE IF EXISTS ZONE_EXTENDED;
            CREATE TABLE ZONE_EXTENDED AS SELECT ST_EXPAND(the_geom, $distance) as the_geom FROM $zoneTable;
            CREATE SPATIAL INDEX ON ZONE_EXTENDED (the_geom);
                """.toString())

    // 3. Prepare the Building table (from the layers "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL" and "BATI_REMARQUABLE") that are in the study area (ZONE_BUFFER)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_BUILDING;
            CREATE TABLE INPUT_BUILDING (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL FLOAT, TYPE varchar, MAIN_USE varchar, HEIGHT_ROOF FLOAT)
            AS 
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.HAUTEUR, 'Résidentiel' , null, null FROM $bati_indifferencie a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.HAUTEUR>=0
            union all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.HAUTEUR, a.NATURE , null, null FROM $bati_industriel a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0
            union all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.HAUTEUR, a.NATURE , null, null FROM $bati_remarquable a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0
            union all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID
                    as id_source, a.HAUTEUR , 'Industrie lourde' , null, null FROM $reservoir a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.NATURE='Réservoir industriel' and a.HAUTEUR>0;

            """.toString())

    //4. Prepare the Road table (from the layer "ROUTE") that are in the study area (ZONE_BUFFER)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_ROAD;
            CREATE TABLE INPUT_ROAD (THE_GEOM geometry, ID_SOURCE varchar(24), WIDTH DOUBLE PRECISION, TYPE varchar, ZINDEX integer, CROSSING varchar,
             DIRECTION varchar,  RANK INTEGER, ADMIN_SCALE VARCHAR, NB_VOIES INTEGER)
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.LARGEUR, a.NATURE, 
            a.POS_SOL, a.FRANCHISST, a.SENS , 
            CASE WHEN a.IMPORTANCE IN ('1', '2', '3', '4', '5') THEN CAST (a.IMPORTANCE AS INTEGER) ELSE NULL END ,
            a.CL_ADMIN, A.NB_VOIES
            FROM $route a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())

    //5. Prepare the Rail table (from the layer "TRONCON_VOIE_FERREE") that are in the study area (ZONE)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_RAIL;
            CREATE TABLE INPUT_RAIL (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer, CROSSING varchar, WIDTH DOUBLE PRECISION)
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, a.POS_SOL, a.FRANCHISST, 
            CASE WHEN a.NB_VOIES = 0 THEN 1.435 ELSE 1.435 * a.NB_VOIES END FROM $troncon_voie_ferree a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())


    //6. Prepare the Hydrography table (from the layer "SURFACE_EAU") that are in the study area (ZONE_EXTENDED)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_HYDRO;
            CREATE TABLE INPUT_HYDRO (THE_GEOM geometry, ID_SOURCE varchar(24), ZINDEX integer, TYPE VARCHAR)
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 0, a.NATURE   FROM $surface_eau a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
            """.toString())

    //7. Prepare the Vegetation table (from the layer "ZONE_VEGETATION") that are in the study area (ZONE_EXTENDED)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_VEGET;
            CREATE TABLE INPUT_VEGET (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0 FROM $zone_vegetation a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0
            FROM $piste_aerodrome a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en herbe';
            """.toString())

    //8. Prepare the Urban areas
    def input_urban_areas = postfix "INPUT_URBAN_AREAS"
    datasource.execute(""" DROP TABLE IF EXISTS TMP_SURFACE_ACTIVITE;
            CREATE TABLE TMP_SURFACE_ACTIVITE (THE_GEOM geometry, ID varchar(24), NATURE VARCHAR) AS 
            SELECT ST_FORCE2D(ST_MAKEVALID(ST_CollectionExtract(ST_INTERSECTION(a.THE_GEOM, B.THE_GEOM), 3))) as the_geom, a.ID,
            CASE WHEN a.CATEGORIE ='Administratif' THEN 'government'
            WHEN a.CATEGORIE= 'Enseignement' THEN 'education'
            WHEN a.CATEGORIE='Santé' THEN 'healthcare' 
            WHEN a.CATEGORIE ='Culture et loisirs' THEN 'entertainment_arts_culture'
            WHEN a.CATEGORIE ='Transport' THEN 'transport'
            WHEN a.CATEGORIE ='Industriel ou commercial' THEN 'commercial'
            WHEN a.CATEGORIE ='Gestion des eaux' THEN 'industrial'
            WHEN a.CATEGORIE ='Sport' THEN 'sport'
            ELSE 'unknown' END
            FROM $surface_activite a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom);
            DROP TABLE IF EXISTS $input_urban_areas;
            CREATE TABLE $input_urban_areas (THE_GEOM geometry, ID_SOURCE varchar(24),TYPE VARCHAR, id_urban INTEGER)
            AS SELECT THE_GEOM, ID, 
            NATURE AS TYPE , EXPLOD_ID AS id_urban FROM ST_EXPLODE('(SELECT * FROM TMP_SURFACE_ACTIVITE WHERE NATURE !=''unknown'')');
            """.toString())

    //9. Prepare the Impervious areas table (from the layers "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE", "SURFACE_ROUTE" and "SURFACE_ACTIVITE") that are in the study area (ZONE)
    def input_impervious = postfix "INPUT_IMPERVIOUS"
    datasource.execute("""
            DROP TABLE IF EXISTS $input_impervious;
            CREATE TABLE $input_impervious (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE VARCHAR, id_impervious INTEGER) AS        
            SELECT the_geom, ID,TYPE,CAST((row_number() over()) as Integer) from (      
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 
            'sport' as type
            FROM $terrain_sport a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE in ('Piste de sport', 'Indifférencié')
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'building' as type
            FROM $construction_surfacique a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE in ('Barrage','Ecluse','Dalle de protection')
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'transport' as type 
            FROM $surface_route a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT  the_geom, ID,NATURE
            FROM TMP_SURFACE_ACTIVITE
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'transport' as type
            FROM $piste_aerodrome a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en dur') as foo;
            """.toString())

    //10. Clean tables
    datasource.execute("""
            DROP TABLE IF EXISTS  ZONE_EXTENDED,TMP_SURFACE_ACTIVITE; """.toString())
    debug('The BDTopo data has been prepared')
    return [building   : "INPUT_BUILDING", road: "INPUT_ROAD",
            rail       : "INPUT_RAIL", water: "INPUT_HYDRO",
            vegetation : "INPUT_VEGET", impervious: input_impervious,
            urban_areas: input_urban_areas, zone: zoneTable
    ]
}

/**
 * This script prepares the BDTopo V3 data already imported in H2GIS database for a specific ZONE
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
Map loadV3(JdbcDataSource datasource,
           Map layers = ["commune"           : "", "batiment": "", "zone_d_activite_ou_d_interet": "", "terrain_de_sport": "", "cimetiere": "",
                         "piste_d_aerodrome" : "", "reservoir": "", "construction_surfacique": "", "equipement_de_transport": "",
                         "troncon_de_route"  : "", "troncon_de_voie_ferree": "", "surface_hydrographique": "",
                         "zone_de_vegetation": "", "aerodrome": "", "limite_terre_mer": ""],
           float distance = 1000) throws Exception {
    if (!layers) {
        throw new IllegalArgumentException("Please set a valid list of layers")
    }
    debug('Import the BDTopo data')

    def commune = layers.commune
    // If the Commune table is empty, then the process is stopped
    if (!commune) {
        throw new IllegalArgumentException('The process has been stopped since the table Commnune is empty')
    }

    // -------------------------------------------------------------------------------
    // Control the SRIDs from input tables

    def srid = -1
    def tablesExist = [:]
    def con = datasource.getConnection()
    // For each tables in the list, we check the SRID and compare to the srid variable. If different, the process is stopped
    layers.each { layer ->
        String name = layer.getValue()
        if (name) {
            if (datasource.hasTable(name)) {
                def hasRow = datasource.firstRow("select 1 as id from ${name} limit 1".toString())
                if (hasRow) {
                    tablesExist.put(layer.key, name)
                    def currentSrid = GeometryTableUtilities.getSRID(con, TableLocation.parse(name, datasource.getDataBaseType()))
                    if (srid == -1) {
                        srid = currentSrid
                    } else {
                        if (currentSrid == 0) {
                            throw new IllegalArgumentException("The process has been stopped since the table $name has a no SRID")
                        } else if (currentSrid > 0 && srid != currentSrid) {
                            throw new IllegalArgumentException("The process has been stopped since the table $name has a different SRID from the others")
                        }
                    }
                }
            }
        }
    }

    if (!tablesExist.get("commune")) {
        throw new IllegalArgumentException('The process has been stopped since the table zone does not exist or is empty')
    }

    // -------------------------------------------------------------------------------
    // Check if the input files are present
    // If the following tables does not exists, we create corresponding empty tables
    String batiment = tablesExist.get("batiment")
    if (!batiment) {
        batiment = "BATIMENT"
        datasource.execute("""DROP TABLE IF EXISTS $batiment; 
                CREATE TABLE $batiment (THE_GEOM geometry(polygon, $srid), 
                ID CHARACTER VARYING(24),
	            NATURE CHARACTER VARYING(34),
	            USAGE1 CHARACTER VARYING(22)
	            NB_ETAGES INTEGER
	            HAUTEUR FLOAT,
	            Z_MIN_TOIT FLOAT,
	            Z_MAX_TOIT FLOAT);""".toString())
    }

    String troncon_de_route = tablesExist.get("troncon_de_route")
    if (!troncon_de_route) {
        troncon_de_route = "troncon_de_route"
        datasource.execute("""DROP TABLE IF EXISTS $troncon_de_route;  
                CREATE TABLE $troncon_de_route (THE_GEOM geometry(linestring, $srid), ID varchar, 
                LARGEUR DOUBLE PRECISION, NATURE varchar, POS_SOL integer, FRANCHISST varchar, SENS varchar, 
                 IMPORTANCE VARCHAR, CL_ADMIN VARCHAR, NAT_RESTR VARCHAR, NB_VOIES INTEGER);""".toString())
    }

    String troncon_de_voie_ferree = tablesExist.get("troncon_de_voie_ferree")
    if (!troncon_de_voie_ferree) {
        troncon_de_voie_ferree = "troncon_de_voie_ferree"
        datasource.execute("""DROP TABLE IF EXISTS $troncon_de_voie_ferree;  
                CREATE TABLE $troncon_de_voie_ferree (THE_GEOM geometry(linestring, $srid), ID varchar, NATURE varchar,
                POS_SOL integer, FRANCHISST varchar, LARGEUR DOUBLE PRECISION, NB_VOIES INTEGER);""".toString())
    }

    String surface_hydrographique = tablesExist.get("surface_hydrographique")
    if (!surface_hydrographique) {
        surface_hydrographique = "surface_hydrographique"
        datasource.execute("""DROP TABLE IF EXISTS $surface_hydrographique;  
                CREATE TABLE $surface_hydrographique (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar,POS_SOL integer);""".toString())
    }

    String zone_de_vegetation = tablesExist.get("zone_de_vegetation")
    if (!zone_de_vegetation) {
        zone_de_vegetation = "zone_de_vegetation"
        datasource.execute("""DROP TABLE IF EXISTS $zone_de_vegetation; 
                CREATE TABLE $zone_de_vegetation (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);""".toString())
    }
    String terrain_de_sport = tablesExist.get("terrain_de_sport")
    if (!terrain_de_sport) {
        terrain_de_sport = "terrain_de_sport"
        datasource.execute("""DROP TABLE IF EXISTS $terrain_de_sport; 
                CREATE TABLE $terrain_de_sport (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar, NAT_DETAIL varchar);""".toString())
    }
    String construction_surfacique = tablesExist.get("construction_surfacique")
    if (!construction_surfacique) {
        construction_surfacique = "construction_surfacique"
        datasource.execute("""DROP TABLE IF EXISTS $construction_surfacique; 
                CREATE TABLE $construction_surfacique (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);""".toString())
    }
    String equipement_de_transport = tablesExist.get("equipement_de_transport")
    if (!equipement_de_transport) {
        equipement_de_transport = "equipement_de_transport"
        datasource.execute("""DROP TABLE IF EXISTS $equipement_de_transport; 
                CREATE TABLE $equipement_de_transport (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);""".toString())
    }
    String zone_d_activite_ou_d_interet = tablesExist.get("zone_d_activite_ou_d_interet")
    if (!zone_d_activite_ou_d_interet) {
        zone_d_activite_ou_d_interet = "zone_d_activite_ou_d_interet"
        datasource.execute("""DROP TABLE IF EXISTS $zone_d_activite_ou_d_interet; 
                CREATE TABLE $zone_d_activite_ou_d_interet (THE_GEOM geometry(polygon, $srid), 
                ID varchar, CATEGORIE varchar, NATURE varchar, FICTIF varchar);""".toString())
    }
    String piste_d_aerodrome = tablesExist.get("piste_d_aerodrome")
    if (!piste_d_aerodrome) {
        piste_d_aerodrome = "piste_d_aerodrome"
        datasource.execute("""DROP TABLE IF EXISTS $piste_d_aerodrome; 
                CREATE TABLE $piste_d_aerodrome (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);""".toString())
    }

    String aerodrome = tablesExist.get("aerodrome")
    if (!aerodrome) {
        aerodrome = "aerodrome"
        datasource.execute("""DROP TABLE IF EXISTS $aerodrome; 
                CREATE TABLE $aerodrome (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);""".toString())
    }

    String reservoir = tablesExist.get("reservoir")
    if (!reservoir) {
        reservoir = "reservoir"
        datasource.execute("""DROP TABLE IF EXISTS $reservoir; 
                CREATE TABLE $reservoir (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar, HAUTEUR integer);""".toString())
    }

    String cimetiere = tablesExist.get("cimetiere")
    if (!cimetiere) {
        cimetiere = "cimetiere"
        datasource.execute("""DROP TABLE IF EXISTS $cimetiere; 
                CREATE TABLE $cimetiere (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);""".toString())
    }

    //Here we prepare the BDTopo data

    //1- Create (spatial) indexes if not already exists on the input layers
    datasource.execute("""
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_COMMUNE ON $commune (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATIMENT ON $batiment(the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_troncon_de_route ON $troncon_de_route (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_troncon_de_voie_ferree ON $troncon_de_voie_ferree (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_surface_hydrographique ON $surface_hydrographique (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_zone_de_vegetation ON $zone_de_vegetation (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_terrain_de_sport ON $terrain_de_sport (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_construction_surfacique ON $construction_surfacique (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_equipement_de_transport ON $equipement_de_transport (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_zone_d_activite_ou_d_interet ON $zone_d_activite_ou_d_interet (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_piste_d_aerodrome ON $piste_d_aerodrome (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_aerodrome ON $aerodrome (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_reservoir ON $reservoir (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_cimetiere ON $cimetiere (the_geom)
            """.toString())

    //2- Preparation of the study area (zone_xx)

    def zoneTable = postfix("ZONE")
    datasource.execute("""
            DROP TABLE IF EXISTS $zoneTable;
            CREATE TABLE $zoneTable AS SELECT ST_FORCE2D(the_geom) as the_geom, CODE_INSEE AS ID_ZONE  FROM $commune;
            CREATE SPATIAL INDEX IF NOT EXISTS idx_geom_ZONE ON $zoneTable (the_geom);
            -- Create a bbox around the input commune
            DROP TABLE IF EXISTS ZONE_EXTENDED;
            CREATE TABLE ZONE_EXTENDED AS SELECT ST_EXPAND(the_geom, $distance) as the_geom FROM $zoneTable;
            CREATE SPATIAL INDEX ON ZONE_EXTENDED (the_geom);
                """.toString())

    // 3. Prepare the Building table that are in the study area
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_BUILDING;
            CREATE TABLE INPUT_BUILDING (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL float, HEIGHT_ROOF float, TYPE varchar, MAIN_USE varchar)
            AS 
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, 
            a.ID, a.HAUTEUR, 
            CASE WHEN a.Z_MAX_TOIT IS NOT NULL AND a.Z_MIN_TOIT IS NOT NULL THEN
            a.Z_MAX_TOIT-a.Z_MIN_TOIT + a.HAUTEUR else null end as HEIGHT_ROOF, 
            CASE WHEN a.NATURE='Indifférenciée' and a.USAGE1 != 'Indifférencié' THEN a.USAGE1
            WHEN a.NATURE='Indifférenciée' and a.USAGE1 = 'Indifférencié' THEN 'Bâtiment'
            WHEN a.NATURE= 'Industriel, agricole ou commercial' and a.USAGE1 != 'Indifférencié' then a.USAGE1
            else a.NATURE end AS type, 
            CASE WHEN a.USAGE1 = 'Indifférencié' and a.NATURE='Indifférenciée' then 'Bâtiment'
            WHEN a.USAGE1 = 'Indifférencié' and  a.NATURE!='Indifférenciée' then a.NATURE
            else a.USAGE1 end as main_use FROM $batiment a, ZONE_EXTENDED b 
            WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
            """.toString())

    //4. Prepare the Road table (from the layer "troncon_de_route") that are in the study area (ZONE_BUFFER)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_ROAD;
            CREATE TABLE INPUT_ROAD (THE_GEOM geometry, ID_SOURCE varchar(24), WIDTH DOUBLE PRECISION, 
            TYPE varchar,  ZINDEX integer, CROSSING varchar, DIRECTION varchar,
            RANK INTEGER, ADMIN_SCALE VARCHAR, NB_VOIES INTEGER)
            AS SELECT  ST_FORCE2D(a.THE_GEOM) as the_geom, a.ID, a.LARGEUR, 
            CASE WHEN a.NAT_RESTR = 'Piste cyclable' then  a.NAT_RESTR else a.NATURE end,  
            CASE WHEN a.POS_SOL='Gué ou radier' THEN 0 ELSE CAST(a.POS_SOL AS INT ) END AS POS_SOL, 
            CASE WHEN a.POS_SOL in ('1', '2', '3', '4') THEN 'Pont' 
            WHEN a.POS_SOL='Gué ou radier' THEN a.POS_SOL
            else null end , 
            CASE WHEN a.SENS='Double sens' THEN 'Double'
            WHEN  a.SENS='Sens direct'  THEN 'Direct'
            WHEN a.SENS='Sens inverse' THEN 'Inverse'
            ELSE null END AS SENS,
            CASE WHEN a.IMPORTANCE IN ('1', '2', '3', '4', '5', '6') THEN CAST (a.IMPORTANCE AS INTEGER) ELSE NULL END ,
            a.CL_ADMIN, a.NB_VOIES
            FROM $troncon_de_route a, 
            ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) 
            and a.POS_SOL not in ('-4' , '-3' ,'-2' ,'-1');
            """.toString())

    //5. Prepare the Rail table (from the layer "troncon_de_voie_ferree") that are in the study area (ZONE)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_RAIL;
            CREATE TABLE INPUT_RAIL (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer, CROSSING varchar, WIDTH DOUBLE PRECISION)
            AS SELECT ST_FORCE2D(a.THE_GEOM) as the_geom, a.ID, a.NATURE, 
            a.POS_SOL, 
            CASE WHEN a.POS_SOL in ('1', '2', '3', '4') THEN 'Pont' else null end, 
        CASE WHEN a.NB_VOIES = 0 THEN 1.435 ELSE 1.435 * a.NB_VOIES END FROM $troncon_de_voie_ferree a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())

    //6. Prepare the Hydrography table (from the layer "surface_hydrographique") that are in the study area (ZONE_EXTENDED)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_HYDRO;
            CREATE TABLE INPUT_HYDRO (THE_GEOM geometry, ID_SOURCE varchar(24), ZINDEX integer, TYPE varchar)
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 0, a.NATURE FROM $surface_hydrographique a, ZONE_EXTENDED 
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0
            and a.NATURE not in ('Conduit buse', 'Conduit forcé', 'Marais', 'Glacier névé')
            union all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 0, a.NATURE  FROM $terrain_de_sport a, ZONE_EXTENDED 
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) 
            and NATURE = 'Bassin de natation';
            """.toString())

    //7. Prepare the Vegetation table (from the layers "zone_de_vegetation", "piste_d_aerodrome", "terrain_de_sport") that are in the study area (ZONE_EXTENDED)
    datasource.execute("""
            DROP TABLE IF EXISTS INPUT_VEGET;
            CREATE TABLE INPUT_VEGET (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0 FROM $zone_de_vegetation a, ZONE_EXTENDED b 
            WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0
            FROM $piste_d_aerodrome a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en herbe'
             UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NAT_DETAIL, 0
            FROM $terrain_de_sport a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NAT_DETAIL in ('Terrain de football', 'Terrain de rugby')
            UNION ALL
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE , 0 FROM $surface_hydrographique a, ZONE_EXTENDED 
            b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0
            and a.NATURE  in ('Marais')            ;
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
            WHEN a.CATEGORIE ='Transport' THEN 'transport'
            WHEN a.CATEGORIE ='Industriel et commercial' and a.NATURE IN ( 'Divers commercial' , 'Marché') THEN 'commercial'
            WHEN a.CATEGORIE ='Industriel et commercial' 
            and a.NATURE IN ('Aquaculture', 'Carrière' ,'Centrale électrique' , 'Déchèterie' , 
            'Divers agricole'  , 'Divers industriel', 'Elevage' , 'Mine' , 'Usine' , 'Zone industrielle') THEN 'industrial'
            WHEN a.CATEGORIE ='Gestion des eaux' THEN 'industrial'
            WHEN a.CATEGORIE ='Sport' THEN 'sport'
            WHEN a.CATEGORIE = 'Religieux' THEN 'religious'
            ELSE 'unknown' END AS TYPE
            FROM $zone_d_activite_ou_d_interet a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
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
            FROM $terrain_de_sport a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NAT_DETAIL not in ('Terrain de football', 'Terrain de rugby')
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 
            'building' as type
            FROM $construction_surfacique a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE in ('Barrage','Ecluse','Dalle')
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'transport' as type 
            FROM $equipement_de_transport a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT  the_geom, ID,NATURE
            FROM TMP_SURFACE_ACTIVITE where NATURE != 'unknown'
            UNION all
            SELECT  ST_FORCE2D(the_geom) as the_geom, ID,'cemetery'
            FROM $cimetiere
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 'transport' as type
            FROM $piste_d_aerodrome a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en dur') as foo;
            """.toString())

    //10. Prepare the coastline table (from the layer "limite_terre_mer") that are in the study area (ZONE)
    String coastline
    String limite_terre_mer = tablesExist.get("limite_terre_mer")
    if (limite_terre_mer) {
        coastline = "INPUT_COASTLINE"
        datasource.execute("""
            DROP TABLE IF EXISTS $coastline;
            CREATE TABLE $coastline
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.* EXCEPT(the_geom)  FROM $limite_terre_mer a, $zoneTable
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
            vegetation : "INPUT_VEGET", impervious: input_impervious,
            urban_areas: input_urban_areas,
            coastline  : coastline, zone: zoneTable
    ]
}