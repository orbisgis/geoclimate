package org.orbisgis.orbisprocess.geoclimate.processingchain


import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class GeoIndicatorsChainTest {

    public static Logger logger = LoggerFactory.getLogger(GeoIndicatorsChainTest.class)

    // Indicator list (at RSU scale) for each type of use
    public static listNames = [
            "TEB"           : ["VERT_ROOF_DENSITY", "NON_VERT_ROOF_DENSITY",
                               "ROAD_DIRECTION_DISTRIBUTION_H0_D0_30", "ROAD_DIRECTION_DISTRIBUTION_H0_D60_90",
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
            "URBAN_TYPOLOGY": ["AREA", "ASPECT_RATIO", "BUILDING_TOTAL_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                               "VEGETATION_FRACTION_URB", "LOW_VEGETATION_FRACTION_URB", "HIGH_VEGETATION_IMPERVIOUS_FRACTION_URB",
                               "HIGH_VEGETATION_PERVIOUS_FRACTION_URB", "ROAD_FRACTION_URB", "IMPERVIOUS_FRACTION_URB",
                               "AVG_NUMBER_BUILDING_NEIGHBOR", "AVG_HEIGHT_ROOF_AREA_WEIGHTED",
                               "STD_HEIGHT_ROOF_AREA_WEIGHTED", "BUILDING_NUMBER_DENSITY", "BUILDING_VOLUME_DENSITY",
                               "BUILDING_VOLUME_DENSITY", "AVG_VOLUME", "GROUND_LINEAR_ROAD_DENSITY",
                               "GEOM_AVG_HEIGHT_ROOF", "BUILDING_FLOOR_AREA_DENSITY",
                               "AVG_MINIMUM_BUILDING_SPACING", "MAIN_BUILDING_DIRECTION", "BUILDING_DIRECTION_UNIQUENESS",
                               "BUILDING_DIRECTION_EQUALITY", "AREA_FRACTION_INDUSTRIAL", "FLOOR_AREA_FRACTION_RESIDENTIAL"],
            "LCZ"           : ["BUILDING_FRACTION_LCZ", "ASPECT_RATIO", "GROUND_SKY_VIEW_FACTOR", "PERVIOUS_FRACTION_LCZ",
                               "IMPERVIOUS_FRACTION_LCZ", "GEOM_AVG_HEIGHT_ROOF", "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH", "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS",
                               "HIGH_VEGETATION_FRACTION_LCZ", "LOW_VEGETATION_FRACTION_LCZ", "WATER_FRACTION_LCZ", "AREA_FRACTION_INDUSTRIAL",
                                "FLOOR_AREA_FRACTION_RESIDENTIAL"]]

    // Basic columns at RSU scale
    public static listColBasic = ["ID_RSU", "THE_GEOM"]

    // Indicators common to each indicator use
    public static listColCommon = ["LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_FRACTION",
                                   "BUILDING_FRACTION", "WATER_FRACTION", "ROAD_FRACTION", "IMPERVIOUS_FRACTION",
                                   "HIGH_VEGETATION_LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_WATER_FRACTION",
                                   "HIGH_VEGETATION_ROAD_FRACTION", "HIGH_VEGETATION_IMPERVIOUS_FRACTION",
                                   "HIGH_VEGETATION_BUILDING_FRACTION"]

    // Column names in the LCZ Table
    public static listColLcz = ["LCZ1", "LCZ2", "LCZ_EQUALITY_VALUE", "LCZ_UNIQUENESS_VALUE", "MIN_DISTANCE"]

    // Indicator lists for urban typology use at building and block scales
    public static listUrbTyp =
            ["Bu": ["THE_GEOM", "ID_RSU", "ID_BUILD", "ID_BLOCK", "NB_LEV", "ZINDEX", "MAIN_USE", "TYPE", "ID_SOURCE",
                    "HEIGHT_ROOF", "HEIGHT_WALL", "PERIMETER", "AREA", "VOLUME", "FLOOR_AREA", "TOTAL_FACADE_LENGTH", "COMMON_WALL_FRACTION",
                    "CONTIGUITY", "AREA_CONCAVITY", "FORM_FACTOR", "RAW_COMPACTNESS", "PERIMETER_CONVEXITY",
                    "MINIMUM_BUILDING_SPACING", "NUMBER_BUILDING_NEIGHBOR", "ROAD_DISTANCE", "LIKELIHOOD_LARGE_BUILDING"],
             "Bl": ["THE_GEOM", "ID_RSU", "ID_BLOCK", "AREA", "FLOOR_AREA", "VOLUME", "HOLE_AREA_DENSITY", "MAIN_BUILDING_DIRECTION",
                    "BUILDING_DIRECTION_UNIQUENESS", "BUILDING_DIRECTION_EQUALITY", "CLOSINGNESS", "NET_COMPACTNESS",
                    "AVG_HEIGHT_ROOF_AREA_WEIGHTED", "STD_HEIGHT_ROOF_AREA_WEIGHTED"]]

    /**
     * Method to init the tables
     * @param datasource
     * @return
     */
    def initTables(def datasource){
        datasource.load(ProcessingChain.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(ProcessingChain.class.getResource("ROAD.geojson"), "ROAD", true)
        datasource.load(ProcessingChain.class.getResource("RAIL.geojson"), "RAIL", true)
        datasource.load(ProcessingChain.class.getResource("VEGET.geojson"), "VEGET", true)
        datasource.load(ProcessingChain.class.getResource("HYDRO.geojson"), "HYDRO", true)
        datasource.load(ProcessingChain.class.getResource("ZONE.geojson"), "ZONE", true)
        return [zoneTable: "ZONE", buildingTable: "BUILDING", roadTable: "ROAD",
                railTable: "RAIL", vegetationTable: "VEGET", hydrographicTable: "HYDRO"]
    }


    @Test
    void GeoIndicatorsTest1() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]
        def ind_i = ["LCZ"]
        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        checkRSUIndicators(datasource,GeoIndicatorsCompute_i.results.outputTableRsuIndicators, false)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest2() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        def ind_i = ["URBAN_TYPOLOGY"]
        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        checkRSUIndicators(datasource,GeoIndicatorsCompute_i.results.outputTableRsuIndicators, false)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns

        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest3() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        def ind_i = ["URBAN_TYPOLOGY", "TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        checkRSUIndicators(datasource,GeoIndicatorsCompute_i.results.outputTableRsuIndicators, false)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest4() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        def ind_i = ["TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        checkRSUIndicators(datasource,GeoIndicatorsCompute_i.results.outputTableRsuIndicators, false)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest5() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        def ind_i = ["LCZ", "TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        checkRSUIndicators(datasource,GeoIndicatorsCompute_i.results.outputTableRsuIndicators, false)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest6() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        def prefixName = ""
        def mapOfWeights = ["sky_view_factor"             : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                            "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                            "height_of_roughness_elements": 1, "terrain_roughness_length": 1]

        def ind_i = ["URBAN_TYPOLOGY", "LCZ"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        checkRSUIndicators(datasource,GeoIndicatorsCompute_i.results.outputTableRsuIndicators, false)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    @Test
    void GeoIndicatorsTest7() {
        File directory = new File("./target/geoindicators_workflow")
        H2GIS datasource = H2GIS.open(directory.absolutePath + File.separator + "osm_chain_db;AUTO_SERVER=TRUE")
        def inputTableNames = initTables(datasource)
        boolean svfSimplified = false
        boolean lczRandomForest = true
        def prefixName = ""
        def ind_i = ["LCZ"]
        def modelPath = "../models/LCZ_OSM_RF_1_0.model"

        IProcess GeoIndicatorsCompute_i = ProcessingChain.GeoIndicatorsChain.computeAllGeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName, lczRandomForest: lczRandomForest,
                lczRfModelPath: modelPath)

        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + ind_i.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuIndicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (ind_i.contains("LCZ")) {
            assertEquals(["ID_RSU", "LCZ1","THE_GEOM"], datasource.getTable(GeoIndicatorsCompute_i.results.outputTableRsuLcz).columns.sort())
        } else {
            assertEquals(null, GeoIndicatorsCompute_i.results.outputTableRsuLcz)
        }
    }

    /**
     * Method to check the result for the RSU indicators table
     * Please add new checks here
     */
    def checkRSUIndicators(def datasource, def rsuIndicatorsTableName, def save){
        //Check road_fraction > 0
        def countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE ROAD_FRACTION>0".toString())
        assertEquals(216, countResult.count)

        //Check building_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE BUILDING_FRACTION>0".toString())
        assertEquals(83, countResult.count)

        //Check high_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_fraction>0".toString())
        assertEquals(12, countResult.count)

        //Check high_vegetation_water_fraction > 0
         countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_water_fraction>0".toString())
        assertEquals(0, countResult.count)

        //Check high_vegetation_building_fraction > 0
         countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_building_fraction>0".toString())
        assertEquals(1, countResult.count)

        //Check high_vegetation_low_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_low_vegetation_fraction>0".toString())
        assertEquals(0, countResult.count)

        //Check high_vegetation_road_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_road_fraction>0".toString())
        assertEquals(12, countResult.count)

        //Check high_vegetation_impervious_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_impervious_fraction>0".toString())
        assertEquals(0, countResult.count)

        //Check water_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE water_fraction>0".toString())
        assertEquals(2, countResult.count)

        //Check low_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE low_vegetation_fraction>0".toString())
        assertEquals(47, countResult.count)

        //Check low_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE impervious_fraction>0".toString())
        assertEquals(0, countResult.count)

        if(save){
            datasource.getTable(tableName).save("${directory.absolutePath}${File.separator}${rsuIndicatorsTableName}.geojson")
        }
    }

}
