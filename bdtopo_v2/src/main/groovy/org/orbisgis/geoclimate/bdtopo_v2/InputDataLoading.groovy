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
 * @param tableCommuneName The table name that represents the commune to process
 * @param tableBuildIndifName The table name in which the undifferentiated ("Indifférencié" in french) buildings are stored
 * @param tableBuildIndusName The table name in which the industrial buildings are stored
 * @param tableBuildRemarqName The table name in which the remarkable ("Remarquable" in french) buildings are stored
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
 * @return outputBuildingName Table name in which the (ready to feed the GeoClimate model) buildings are stored
 * @return outputRoadName Table name in which the (ready to feed the GeoClimate model) roads are stored
 * @return outputRailName Table name in which the (ready to feed the GeoClimate model) rail ways are stored
 * @return outputHydroName Table name in which the (ready to feed the GeoClimate model) hydrographic areas are stored
 * @return outputVegetName Table name in which the (ready to feed the GeoClimate model) vegetation areas are stored
 * @return outputImperviousName Table name in which the (ready to feed the GeoClimate model) impervious areas are stored
 * @return outputZoneName Table name in which the (ready to feed the GeoClimate model) zone is stored
 */
IProcess prepareBDTopoData() {
    return create {
        title 'Prepare the BDTopo data already imported in the H2GIS database'
        id "importPreprocess"
        inputs datasource: JdbcDataSource,
                tableCommuneName: "",
                tableBuildIndifName: "",
                tableBuildIndusName: "",
                tableBuildRemarqName: "",
                tableRoadName: "",
                tableRailName: "",
                tableHydroName: "",
                tableVegetName: "",
                tableImperviousSportName: "",
                tableImperviousBuildSurfName: "",
                tableImperviousRoadSurfName: "",
                tableImperviousActivSurfName: "",
                tablePiste_AerodromeName: "",
                tableReservoirName: "",
                distance: 1000
        outputs outputBuildingName: String,
                outputRoadName: String,
                outputRailName: String,
                outputHydroName: String,
                outputVegetName: String,
                outputImperviousName: String,
                outputUrbanAreas:String,
                outputZoneName: String
        run { datasource, tableCommuneName, tableBuildIndifName, tableBuildIndusName,
              tableBuildRemarqName, tableRoadName, tableRailName, tableHydroName, tableVegetName,
              tableImperviousSportName, tableImperviousBuildSurfName, tableImperviousRoadSurfName,
              tableImperviousActivSurfName, tablePiste_AerodromeName, tableReservoirName, distance ->

            debug('Import the BDTopo data')

            // If the Commune table is empty, then the process is stopped
            if (!tableCommuneName) {
                error 'The process has been stopped since the table Commnune is empty'
                return
            }

            // -------------------------------------------------------------------------------
            // Control the SRIDs from input tables
            def list = [tableCommuneName, tableBuildIndifName, tableBuildIndusName, tableBuildRemarqName,
                        tableRoadName, tableRailName, tableHydroName, tableVegetName,
                        tableImperviousSportName, tableImperviousBuildSurfName,
                        tableImperviousRoadSurfName, tableImperviousActivSurfName,
                        tablePiste_AerodromeName, tableReservoirName]
            def srid = -1
            def tablesExist = []
                def con = datasource.getConnection()
                // For each tables in the list, we check the SRID and compare to the srid variable. If different, the process is stopped
                for (String name : list) {
                    if (name) {
                        if (datasource.hasTable(name)) {
                            def hasRow = datasource.firstRow("select 1 as id from ${name} limit 1".toString())
                            if (hasRow) {
                                tablesExist << name
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

            // -------------------------------------------------------------------------------
            // Check if the input files are present

            // If the COMMUNE table does not exist or is empty, then the process is stopped
            if (!tablesExist.contains(tableCommuneName)) {
                error 'The process has been stopped since the table zone does not exist or is empty'
                return
            }

            // If the following tables does not exists, we create corresponding empty tables
            if (!tablesExist.contains(tableBuildIndifName)) {
                tableBuildIndifName = "BATI_INDIFFERENCIE"
                datasource.execute("DROP TABLE IF EXISTS $tableBuildIndifName; CREATE TABLE $tableBuildIndifName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer);".toString())
            }
            if (!tablesExist.contains(tableBuildIndusName)) {
                tableBuildIndusName = "BATI_INDUSTRIEL"
                datasource.execute("DROP TABLE IF EXISTS $tableBuildIndusName; CREATE TABLE $tableBuildIndusName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableBuildRemarqName)) {
                tableBuildRemarqName = "BATI_REMARQUABLE"
                datasource.execute("DROP TABLE IF EXISTS $tableBuildRemarqName;  CREATE TABLE $tableBuildRemarqName (THE_GEOM geometry(polygon, $srid), ID varchar, HAUTEUR integer, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableRoadName)) {
                tableRoadName = "ROUTE"
                datasource.execute("DROP TABLE IF EXISTS $tableRoadName;  CREATE TABLE $tableRoadName (THE_GEOM geometry(linestring, $srid), ID varchar, LARGEUR DOUBLE PRECISION, NATURE varchar, POS_SOL integer, FRANCHISST varchar, SENS varchar);".toString())
            }
            if (!tablesExist.contains(tableRailName)) {
                tableRailName = "TRONCON_VOIE_FERREE"
                datasource.execute("DROP TABLE IF EXISTS $tableRailName;  CREATE TABLE $tableRailName (THE_GEOM geometry(linestring, $srid), ID varchar, NATURE varchar, POS_SOL integer, FRANCHISST varchar);".toString())
            }
            if (!tablesExist.contains(tableHydroName)) {
                tableHydroName = "SURFACE_EAU"
                datasource.execute("DROP TABLE IF EXISTS $tableHydroName;  CREATE TABLE $tableHydroName (THE_GEOM geometry(polygon, $srid), ID varchar);".toString())
            }
            if (!tablesExist.contains(tableVegetName)) {
                tableVegetName = "ZONE_VEGETATION"
                datasource.execute("DROP TABLE IF EXISTS $tableVegetName; CREATE TABLE $tableVegetName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousSportName)) {
                tableImperviousSportName = "TERRAIN_SPORT"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousSportName; CREATE TABLE $tableImperviousSportName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousBuildSurfName)) {
                tableImperviousBuildSurfName = "CONSTRUCTION_SURFACIQUE"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousBuildSurfName; CREATE TABLE $tableImperviousBuildSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousRoadSurfName)) {
                tableImperviousRoadSurfName = "SURFACE_ROUTE"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousRoadSurfName; CREATE TABLE $tableImperviousRoadSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, NATURE varchar);".toString())
            }
            if (!tablesExist.contains(tableImperviousActivSurfName)) {
                tableImperviousActivSurfName = "SURFACE_ACTIVITE"
                datasource.execute("DROP TABLE IF EXISTS $tableImperviousActivSurfName; CREATE TABLE $tableImperviousActivSurfName (THE_GEOM geometry(polygon, $srid), ID varchar, CATEGORIE varchar, ORIGINE varchar);".toString())
            }
            if (!tablesExist.contains(tablePiste_AerodromeName)) {
                tablePiste_AerodromeName = "PISTE_AERODROME"
                datasource.execute("DROP TABLE IF EXISTS $tablePiste_AerodromeName; CREATE TABLE $tablePiste_AerodromeName (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar);".toString())
            }

            if (!tablesExist.contains(tableReservoirName)) {
                tableReservoirName = "RESERVOIR"
                datasource.execute("DROP TABLE IF EXISTS $tableReservoirName; CREATE TABLE $tableReservoirName (THE_GEOM geometry(polygon, $srid), ID varchar,  NATURE varchar, HAUTEUR integer);".toString())
            }

            //Here we prepare the BDTopo data

            //1- Create (spatial) indexes if not already exists on the input layers

            datasource.execute("""
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDIFFERENCIE ON $tableCommuneName (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDIFFERENCIE ON BATI_INDIFFERENCIE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_INDUSTRIEL ON BATI_INDUSTRIEL (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_BATI_REMARQUABLE ON BATI_REMARQUABLE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ROUTE ON ROUTE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_TRONCON_VOIE_FERREE ON TRONCON_VOIE_FERREE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_EAU ON SURFACE_EAU (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_ZONE_VEGETATION ON ZONE_VEGETATION (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_TERRAIN_SPORT ON TERRAIN_SPORT (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_CONSTRUCTION_SURFACIQUE ON CONSTRUCTION_SURFACIQUE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_ROUTE ON SURFACE_ROUTE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_SURFACE_ACTIVITE ON SURFACE_ACTIVITE (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_PISTE_AERODROME ON PISTE_AERODROME (the_geom);
            CREATE SPATIAL INDEX  IF NOT EXISTS idx_geom_RESERVOIR ON RESERVOIR (the_geom);
            """.toString())

            //2- Preparation of the study area (zone_xx)

            def zoneTable = postfix("ZONE")
            datasource.execute("""
            DROP TABLE IF EXISTS $zoneTable;
            CREATE TABLE $zoneTable AS SELECT ST_FORCE2D(the_geom) as the_geom, CODE_INSEE AS ID_ZONE  FROM $tableCommuneName;
            CREATE SPATIAL INDEX IF NOT EXISTS idx_geom_ZONE ON $zoneTable (the_geom);
            -- Generation of a rectangular area (bbox) around the studied commune
            DROP TABLE IF EXISTS ZONE_EXTENDED;
            CREATE TABLE ZONE_EXTENDED AS SELECT ST_EXPAND(the_geom, $distance) as the_geom FROM $zoneTable;
            CREATE SPATIAL INDEX ON ZONE_EXTENDED (the_geom);
                """.toString())

            // 3. Prepare the Building table (from the layers "BATI_INDIFFERENCIE", "BATI_INDUSTRIEL" and "BATI_REMARQUABLE") that are in the study area (ZONE_BUFFER)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_BUILDING;
            CREATE TABLE INPUT_BUILDING (THE_GEOM geometry, ID_SOURCE varchar(24), HEIGHT_WALL integer, TYPE varchar)
            AS 
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.HAUTEUR, 'Résidentiel', FROM BATI_INDIFFERENCIE a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.HAUTEUR>=0
            union all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.HAUTEUR, a.NATURE FROM BATI_INDUSTRIEL a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0
            union all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.HAUTEUR, a.NATURE FROM BATI_REMARQUABLE a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)  and a.HAUTEUR>=0
            union all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID
                    as id_source, a.HAUTEUR , 'heavy_industry' FROM RESERVOIR a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.NATURE='Réservoir industriel' and a.HAUTEUR>0;

            """.toString())

            //4. Prepare the Road table (from the layer "ROUTE") that are in the study area (ZONE_BUFFER)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_ROAD;
            CREATE TABLE INPUT_ROAD (THE_GEOM geometry, ID_SOURCE varchar(24), WIDTH DOUBLE PRECISION, TYPE varchar, SURFACE varchar, SIDEWALK varchar, ZINDEX integer, CROSSING varchar, SENS varchar)
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.LARGEUR, a.NATURE, '', '', a.POS_SOL, a.FRANCHISST, a.SENS FROM ROUTE a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())

            //5. Prepare the Rail table (from the layer "TRONCON_VOIE_FERREE") that are in the study area (ZONE)

            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_RAIL;
            CREATE TABLE INPUT_RAIL (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer, CROSSING varchar)
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, a.POS_SOL, a.FRANCHISST FROM TRONCON_VOIE_FERREE a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom) and a.POS_SOL>=0;
            """.toString())


            //6. Prepare the Hydrography table (from the layer "SURFACE_EAU") that are in the study area (ZONE_EXTENDED)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_HYDRO;
            CREATE TABLE INPUT_HYDRO (THE_GEOM geometry, ID_SOURCE varchar(24), ZINDEX integer)
            AS SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, 0  FROM SURFACE_EAU a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom);
            """.toString())

            //7. Prepare the Vegetation table (from the layer "ZONE_VEGETATION") that are in the study area (ZONE_EXTENDED)
            datasource.execute("""
            DROP TABLE IF EXISTS INPUT_VEGET;
            CREATE TABLE INPUT_VEGET (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE varchar, ZINDEX integer)
            AS SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0 FROM ZONE_VEGETATION a, ZONE_EXTENDED b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE, 0
            FROM PISTE_AERODROME a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en herbe';
            ;
            """.toString())

            //8. Prepare the Urban areas
            def input_urban_areas = postfix "INPUT_URBAN_AREAS"
            datasource.execute(""" DROP TABLE IF EXISTS TMP_SURFACE_ACTIVITE;
            CREATE TABLE TMP_SURFACE_ACTIVITE (THE_GEOM geometry, ID varchar(24), NATURE VARCHAR) AS 
            SELECT ST_FORCE2D(ST_MAKEVALID(ST_CollectionExtract(ST_INTERSECTION(a.THE_GEOM, B.THE_GEOM), 3))) as the_geom, a.ID,a.CATEGORIE
            FROM SURFACE_ACTIVITE a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom);
            DROP TABLE IF EXISTS $input_urban_areas;
            CREATE TABLE $input_urban_areas (THE_GEOM geometry, ID_SOURCE varchar(24),TYPE VARCHAR, id_urban INTEGER)
            AS SELECT THE_GEOM, ID, 
            CASE WHEN NATURE ='Administratif' THEN 'government'
            WHEN NATURE= 'Enseignement' THEN 'education'
            WHEN NATURE='Santé' THEN 'healthcare' 
            WHEN NATURE ='Culture et loisirs' THEN 'entertainment_arts_culture'
            WHEN NATURE ='Transport' THEN 'transportation'
            WHEN NATURE ='Industriel ou commercial' THEN 'commercial'
            WHEN NATURE ='Gestion des eaux' THEN 'industrial'
            WHEN NATURE ='Sport' THEN 'sport'
            ELSE 'unknown' END AS TYPE
            , EXPLOD_ID AS id_urban FROM ST_EXPLODE('TMP_SURFACE_ACTIVITE') where NATURE IN('Administratif', 'Enseignement',
            'Santé','Culture et loisirs','Transport','Industriel ou commercial','Gestion des eaux'
            ,'Sport');
            """.toString())

            //9. Prepare the Impervious areas table (from the layers "TERRAIN_SPORT", "CONSTRUCTION_SURFACIQUE", "SURFACE_ROUTE" and "SURFACE_ACTIVITE") that are in the study area (ZONE)
            def input_impervious = postfix "INPUT_IMPERVIOUS"
            datasource.execute("""
            DROP TABLE IF EXISTS $input_impervious;
            CREATE TABLE $input_impervious (THE_GEOM geometry, ID_SOURCE varchar(24), TYPE VARCHAR, id_impervious INTEGER) AS        
            SELECT the_geom, ID,TYPE,CAST((row_number() over()) as Integer) from (      
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE as type
            FROM TERRAIN_SPORT a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE in ('Piste de sport', 'Indifférencié')
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE
            FROM CONSTRUCTION_SURFACIQUE a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom) AND a.NATURE in ('Barrage','Ecluse','Dalle de protection')
            UNION all
            SELECT ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE 
            FROM SURFACE_ROUTE a, $zoneTable b WHERE a.the_geom && b.the_geom AND ST_INTERSECTS(a.the_geom, b.the_geom)
            UNION all
            SELECT  the_geom, ID,NATURE
            FROM TMP_SURFACE_ACTIVITE
            UNION all
            SELECT  ST_FORCE2D(ST_MAKEVALID(a.THE_GEOM)) as the_geom, a.ID, a.NATURE
            FROM PISTE_AERODROME a, $zoneTable b WHERE a.the_geom && b.the_geom AND 
            ST_INTERSECTS(a.the_geom, b.the_geom)
            and a.NATURE = 'Piste en dur') as foo;
            """.toString())

            //10. Clean tables
            datasource.execute("""
            DROP TABLE IF EXISTS  ZONE_EXTENDED,TMP_SURFACE_ACTIVITE; """.toString())
            debug('The BDTopo data has been prepared')
             return    [outputBuildingName: "INPUT_BUILDING", outputRoadName: "INPUT_ROAD",
                        outputRailName: "INPUT_RAIL", outputHydroName   : "INPUT_HYDRO",
                        outputVegetName: "INPUT_VEGET", outputImperviousName: input_impervious,
                        outputUrbanAreas : input_urban_areas, outputZoneName    : zoneTable
                ]
            }
    }
}


