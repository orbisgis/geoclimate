package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.datamanager.h2gis.H2GIS
import org.orbisgis.processmanagerapi.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue


class ChainProcessMainTest {

    private static H2GIS datasource

    public static Logger logger = LoggerFactory.getLogger(ChainProcessMainTest.class)

    // Indicator list (at RSU scale) for each type of use
    public static listNames = [
            "TEB"           : ["VERT_ROOF_DENSITY", "NON_VERT_ROOF_DENSITY", "BUILDING_AREA_FRACTION",
                               "LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_FRACTION",
                               "ALL_VEGETATION_FRACTION", "ROAD_DIRECTION_DISTRIBUTION_H0_D0_30", "ROAD_DIRECTION_DISTRIBUTION_H0_D60_90",
                               "ROAD_DIRECTION_DISTRIBUTION_H0_D90_120", "ROAD_DIRECTION_DISTRIBUTION_H0_D120_150",
                               "ROAD_DIRECTION_DISTRIBUTION_H0_D150_180", "ROAD_DIRECTION_DISTRIBUTION_H0_D30_60",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D0_30", "PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D0_30",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D0_30", "PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D0_30",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D0_30", "PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D0_30",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D30_60", "PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D30_60",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D30_60", "PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D30_60",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D30_60", "PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D30_60",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D60_90", "PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D60_90",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D60_90", "PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D60_90",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D60_90", "PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D60_90",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D90_120", "PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D90_120",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D90_120", "PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D90_120",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D90_120", "PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D90_120",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D120_150", "PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D120_150",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D120_150", "PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D120_150",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D120_150", "PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D120_150",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H0_10_D150_180", "PROJECTED_FACADE_AREA_DISTRIBUTION_H10_20_D150_180",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H20_30_D150_180", "PROJECTED_FACADE_AREA_DISTRIBUTION_H30_40_D150_180",
                               "PROJECTED_FACADE_AREA_DISTRIBUTION_H40_50_D150_180", "PROJECTED_FACADE_AREA_DISTRIBUTION_H50_D150_180",
                               "NON_VERT_ROOF_AREA_H0_10", "NON_VERT_ROOF_AREA_H10_20", "NON_VERT_ROOF_AREA_H20_30",
                               "NON_VERT_ROOF_AREA_H30_40", "NON_VERT_ROOF_AREA_H40_50", "NON_VERT_ROOF_AREA_H50",
                               "VERT_ROOF_AREA_H0_10", "VERT_ROOF_AREA_H10_20", "VERT_ROOF_AREA_H20_30", "VERT_ROOF_AREA_H30_40",
                               "VERT_ROOF_AREA_H40_50", "VERT_ROOF_AREA_H50", "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH"],
            "URBAN_TYPOLOGY": ["AREA", "BUILDING_AREA_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                               "AVG_NUMBER_BUILDING_NEIGHBOR", "AVG_HEIGHT_ROOF_AREA_WEIGHTED",
                               "STD_HEIGHT_ROOF_AREA_WEIGHTED", "BUILDING_NUMBER_DENSITY", "BUILDING_VOLUME_DENSITY",
                               "BUILDING_VOLUME_DENSITY", "AVG_VOLUME", "GROUND_ROAD_FRACTION",
                               "OVERGROUND_ROAD_FRACTION", "GROUND_LINEAR_ROAD_DENSITY", "WATER_FRACTION",
                               "ALL_VEGETATION_FRACTION", "GEOM_AVG_HEIGHT_ROOF", "BUILDING_FLOOR_AREA_DENSITY",
                               "AVG_MINIMUM_BUILDING_SPACING", "MAIN_BUILDING_DIRECTION", "BUILDING_DIRECTION_UNIQUENESS",
                               "BUILDING_DIRECTION_INEQUALITY"],
            "LCZ"           : ["BUILDING_AREA_FRACTION", "ASPECT_RATIO", "GROUND_SKY_VIEW_FACTOR", "PERVIOUS_FRACTION",
                               "IMPERVIOUS_FRACTION", "GEOM_AVG_HEIGHT_ROOF", "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS"]]

    // Extra columns at RSU scale
    public static listColBasic = ["ID_RSU", "THE_GEOM"]

    // Column names in the LCZ Table
    public static listColLcz = ["LCZ1", "LCZ2", "MIN_DISTANCE", "PSS"]

    // Indicator lists for urban typology use at building and block scales
    public static listUrbTyp =
            ["Bu": ["THE_GEOM", "ID_RSU", "ID_BUILD", "ID_BLOCK", "NB_LEV", "ZINDEX", "MAIN_USE", "TYPE", "ID_SOURCE",
                    "HEIGHT_ROOF", "HEIGHT_WALL", "PERIMETER", "AREA", "VOLUME", "FLOOR_AREA", "TOTAL_FACADE_LENGTH", "COMMON_WALL_FRACTION",
                    "CONTIGUITY", "AREA_CONCAVITY", "FORM_FACTOR", "RAW_COMPACTNESS", "PERIMETER_CONVEXITY",
                    "MINIMUM_BUILDING_SPACING", "NUMBER_BUILDING_NEIGHBOR", "ROAD_DISTANCE", "LIKELIHOOD_LARGE_BUILDING"],
             "Bl": ["THE_GEOM", "ID_RSU", "ID_BLOCK", "AREA", "FLOOR_AREA", "VOLUME", "HOLE_AREA_DENSITY", "MAIN_BUILDING_DIRECTION",
                    "BUILDING_DIRECTION_UNIQUENESS", "BUILDING_DIRECTION_INEQUALITY", "CLOSINGNESS", "NET_COMPACTNESS",
                    "AVG_HEIGHT_ROOF_AREA_WEIGHTED", "STD_HEIGHT_ROOF_AREA_WEIGHTED"]]

    public static File directory = new File("./target/osm_workflow")

    public static zoneTableName = "ZONE"
    public static buildingTableName = "BUILDING"
    public static roadTableName = "ROAD"
    public static railTableName = "RAIL"
    public static vegetationTableName = "VEGET"
    public static hydrographicTableName = "HYDRO"

    @BeforeAll
    static void init() {
        File directory = new File("./target/osm_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")

        // Names of the input tables downloaded from OpenStreetMap
        datasource.load(ProcessingChain.class.getResource("BUILDING.geojson"), buildingTableName, true)
        datasource.load(ProcessingChain.class.getResource("ROAD.geojson"), roadTableName, true)
        datasource.load(ProcessingChain.class.getResource("RAIL.geojson"), railTableName, true)
        datasource.load(ProcessingChain.class.getResource("VEGET.geojson"), vegetationTableName, true)
        datasource.load(ProcessingChain.class.getResource("HYDRO.geojson"), hydrographicTableName, true)
        datasource.load(ProcessingChain.class.getResource("ZONE.geojson"), zoneTableName, true)
    }


    @Test
    void OSMGeoIndicatorsTest() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        String placeName = "Cliscouet, vannes"
        def distance = 0
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["LCZ", "URBAN_TYPOLOGY", "TEB"]

        IProcess OSMGeoIndicatorsCompute_i = ProcessingChain.Workflow.OSM()
        assertTrue OSMGeoIndicatorsCompute_i.execute([datasource   : datasource, placeName: placeName,
                                                      distance     : distance, indicatorUse: ind_i,
                                                      svfSimplified: svfSimplified, prefixName: prefixName,
                                                      mapOfWeights : mapOfWeights])

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(OSMGeoIndicatorsCompute_i.getResults().buildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(OSMGeoIndicatorsCompute_i.results.blockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(OSMGeoIndicatorsCompute_i.results.rsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(OSMGeoIndicatorsCompute_i.results.rsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, OSMGeoIndicatorsCompute_i.results.rsuLcz)
        }
    }


    @Test
    void GeoIndicatorsTest1() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["LCZ"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: null, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest2() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["URBAN_TYPOLOGY"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: null, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest3() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["URBAN_TYPOLOGY", "TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: null, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest4() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: null, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest5() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["LCZ", "TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: null, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest6() {
        datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["URBAN_TYPOLOGY", "LCZ"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: zoneTableName,
                buildingTable: buildingTableName, roadTable: roadTableName,
                railTable: null, vegetationTable: vegetationTableName,
                hydrographicTable: hydrographicTableName, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).getColumnNames().sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).getColumnNames().sort())
        }
        def expectListRsuTempo = listColBasic
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).getColumnNames()
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).getColumnNames().sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    /**
     * A method to compute geomorphological indicators
     * @param directory
     * @param datasource
     * @param zoneTableName
     * @param buildingTableName
     * @param roadTableName
     * @param railTableName
     * @param vegetationTableName
     * @param hydrographicTableName
     * @param saveResults
     * @param indicatorUse
     */
    void geoIndicatorsCalc(String directory, JdbcDataSource datasource, String zoneTableName, String buildingTableName,
                           String roadTableName, String railTableName, String vegetationTableName,
                           String hydrographicTableName, boolean saveResults, boolean svfSimplified = false, indicatorUse,
                           String prefixName = "") {
        //Create spatial units and relations : building, block, rsu
        IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
        assertTrue spatialUnits.execute([datasource       : datasource, zoneTable: zoneTableName, buildingTable: buildingTableName,
                                         roadTable        : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                                         hydrographicTable: hydrographicTableName, surface_vegetation: 100000,
                                         surface_hydro    : 2500, distance: 0.01, prefixName: prefixName])

        String relationBuildings = spatialUnits.getResults().outputTableBuildingName
        String relationBlocks = spatialUnits.getResults().outputTableBlockName
        String relationRSU = spatialUnits.getResults().outputTableRsuName

        if (saveResults) {
            logger.info("Saving spatial units")
            IProcess saveTables = ProcessingChain.DataUtils.saveTablesAsFiles()
            saveTables.execute([inputTableNames: spatialUnits.getResults().values()
                                , directory    : directory, datasource: datasource])
        }

        def maxBlocks = datasource.firstRow("select max(id_block) as max from ${relationBuildings}".toString())
        def countBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
        assertEquals(countBlocks.count, maxBlocks.max)


        def maxRSUBlocks = datasource.firstRow("select count(distinct id_block) as max from ${relationBuildings} where id_rsu is not null".toString())
        def countRSU = datasource.firstRow("select count(*) as count from ${relationBlocks} where id_rsu is not null".toString())
        assertEquals(countRSU.count, maxRSUBlocks.max)

        //Compute building indicators
        def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
        assertTrue computeBuildingsIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: relationBuildings,
                                                       inputRoadTableName    : roadTableName,
                                                       indicatorUse          : indicatorUse,
                                                       prefixName            : prefixName])
        String buildingIndicators = computeBuildingsIndicators.getResults().outputTableName
        if (saveResults) {
            logger.info("Saving building indicators")
            datasource.save(buildingIndicators, directory + File.separator + "${buildingIndicators}.geojson")
        }

        //Check we have the same number of buildings
        def countRelationBuilding = datasource.firstRow("select count(*) as count from ${relationBuildings}".toString())
        def countBuildingIndicators = datasource.firstRow("select count(*) as count from ${buildingIndicators}".toString())
        assertEquals(countRelationBuilding.count, countBuildingIndicators.count)

        //Compute block indicators
        if (indicatorUse.contains("URBAN_TYPOLOGY")) {
            def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
            assertTrue computeBlockIndicators.execute([datasource            : datasource,
                                                       inputBuildingTableName: buildingIndicators,
                                                       inputBlockTableName   : relationBlocks,
                                                       prefixName            : prefixName])
            String blockIndicators = computeBlockIndicators.getResults().outputTableName
            if (saveResults) {
                logger.info("Saving block indicators")
                datasource.save(blockIndicators, directory + File.separator + "${blockIndicators}.geojson")
            }
            //Check if we have the same number of blocks
            def countRelationBlocks = datasource.firstRow("select count(*) as count from ${relationBlocks}".toString())
            def countBlocksIndicators = datasource.firstRow("select count(*) as count from ${blockIndicators}".toString())
            assertEquals(countRelationBlocks.count, countBlocksIndicators.count)
        }

        //Compute RSU indicators
        def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
        assertTrue computeRSUIndicators.execute([datasource       : datasource,
                                                 buildingTable    : buildingIndicators,
                                                 rsuTable         : relationRSU,
                                                 vegetationTable  : vegetationTableName,
                                                 roadTable        : roadTableName,
                                                 hydrographicTable: hydrographicTableName,
                                                 indicatorUse     : indicatorUse,
                                                 prefixName       : prefixName,
                                                 svfSimplified    : svfSimplified])
        String rsuIndicators = computeRSUIndicators.getResults().outputTableName
        if (saveResults) {
            logger.info("Saving RSU indicators")
            datasource.save(rsuIndicators, directory + File.separator + "${rsuIndicators}.geojson")
        }

        //Check if we have the same number of RSU
        def countRelationRSU = datasource.firstRow("select count(*) as count from ${relationRSU}".toString())
        def countRSUIndicators = datasource.firstRow("select count(*) as count from ${rsuIndicators}".toString())
        assertEquals(countRelationRSU.count, countRSUIndicators.count)
    }
}