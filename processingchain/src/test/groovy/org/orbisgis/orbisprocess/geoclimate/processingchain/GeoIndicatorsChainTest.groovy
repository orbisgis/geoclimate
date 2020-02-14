package org.orbisgis.orbisprocess.geoclimate.processingchain

import org.junit.jupiter.api.Test
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertTrue

class GeoIndicatorsChainTest extends ChainProcessAbstractTest{

    public static Logger logger = LoggerFactory.getLogger(GeoIndicatorsChainTest.class)

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
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["LCZ"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic
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
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["URBAN_TYPOLOGY"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic
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
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["URBAN_TYPOLOGY", "TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic
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
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic
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
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["LCZ", "TEB"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic
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
                            "height_of_roughness_elements": 1, "terrain_roughness_class": 1]

        def ind_i = ["URBAN_TYPOLOGY", "LCZ"]

        IProcess GeoIndicatorsCompute_i = ProcessingChain.Workflow.GeoIndicators()
        assertTrue GeoIndicatorsCompute_i.execute(datasource: datasource, zoneTable: inputTableNames.zoneTable,
                buildingTable: inputTableNames.buildingTable, roadTable: inputTableNames.roadTable,
                railTable: inputTableNames.railTable, vegetationTable: inputTableNames.vegetationTable,
                hydrographicTable: inputTableNames.hydrographicTable, indicatorUse: ind_i,
                svfSimplified: svfSimplified, prefixName: prefixName,
                mapOfWeights: mapOfWeights)

        if (ind_i.contains("URBAN_TYPOLOGY")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(GeoIndicatorsCompute_i.getResults().outputTableBuildingIndicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(GeoIndicatorsCompute_i.results.outputTableBlockIndicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic
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
}
