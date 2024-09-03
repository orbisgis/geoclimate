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
package org.orbisgis.geoclimate.geoindicators

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.orbisgis.data.H2GIS
import org.orbisgis.data.dataframe.DataFrame
import org.orbisgis.geoclimate.Geoindicators
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*
import static org.orbisgis.data.H2GIS.open

class WorkflowGeoIndicatorsTest {

    @TempDir
    static File folder

    public static Logger logger = LoggerFactory.getLogger(WorkflowGeoIndicatorsTest.class)


    public static listNames
    // Basic columns at RSU scale
    public static listColBasic
    // Indicators common to each indicator use
    public static listColCommon
    // Column names in the LCZ Table
    public static listColLcz
    // Indicator lists for urban typology use at building and block scales
    public static listUrbTyp
    // Indicator list for testing sum of all fractions
    public static listBuildTypTeb
    public static listBuildTypLcz
    public static listFloorBuildTypLcz
    public static listFloorBuildTypTeb

    public static H2GIS datasource
    public static def inputTableNames

    public
    @BeforeAll
    static void beforeAll() {
        initParameters()
        datasource = open(folder.getAbsolutePath() + File.separator + "workflowGeoIndicatorsTest;AUTO_SERVER=TRUE")
        assertNotNull(datasource)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ROAD.geojson"), "ROAD", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("RAIL.geojson"), "RAIL", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("VEGET.geojson"), "VEGET", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("HYDRO.geojson"), "HYDRO", true)
        inputTableNames = [zoneTable: "ZONE", buildingTable: "BUILDING", roadTable: "ROAD",
                           railTable: "RAIL", vegetationTable: "VEGET", hydrographicTable: "HYDRO"]
    }

    // Indicator list (at RSU scale) for each type of use
    /**
     * Init some parameters to run the tests
     */
    static void initParameters() {
        Map parameters = Geoindicators.WorkflowGeoIndicators.getParameters()
        // Indicator list (at RSU scale) for each type of use
        listBuildTypTeb = []
        listBuildTypLcz = []
        listFloorBuildTypLcz = []
        listFloorBuildTypTeb = []
        for (type in parameters.buildingAreaTypeAndCompositionTeb.keySet()) {
            listBuildTypTeb.add("AREA_FRACTION_${type}".toString().toUpperCase())
        }
        for (type in parameters.floorAreaTypeAndCompositionTeb.keySet()) {
            listFloorBuildTypTeb.add("FLOOR_AREA_FRACTION_${type}".toString().toUpperCase())
        }
        for (type in parameters.buildingAreaTypeAndCompositionLcz.keySet()) {
            listBuildTypLcz.add("AREA_FRACTION_${type}".toString().toUpperCase())
        }
        for (type in parameters.floorAreaTypeAndCompositionLcz.keySet()) {
            listFloorBuildTypLcz.add("FLOOR_AREA_FRACTION_${type}".toString().toUpperCase())
        }

        // Indicator list (at RSU scale) for each road direction
        List listRoadDir = []
        for (int d = parameters.angleRangeSizeRoDirection; d <= 180; d += parameters.angleRangeSizeRoDirection) {
            listRoadDir.add(Geoindicators.RsuIndicators.getRoadDirIndic(d, parameters.angleRangeSizeRoDirection, 0).toString().toUpperCase())
        }

        // Indicator list (at RSU scale) for each facade direction and height (projected facade distrib)
        // and also for height only (vert and non vert roof density)
        List listFacadeDistrib = []
        List listRoofDensDistrib = []
        int rangeDeg = 360 / parameters.facadeDensNumberOfDirection
        for (int i in 0..parameters.facadeDensListLayersBottom.size() - 1) {
            Integer h_bot = parameters.facadeDensListLayersBottom[i]
            Integer h_up
            if (h_bot == parameters.facadeDensListLayersBottom[-1]) {
                h_up = null
            } else {
                h_up = parameters.facadeDensListLayersBottom[i + 1]
            }
            // Create names for vert and non vert roof density
            listRoofDensDistrib.add(Geoindicators.RsuIndicators.getDistribIndicName("vert_roof_area", 'h', h_bot, h_up).toString().toUpperCase())
            listRoofDensDistrib.add(Geoindicators.RsuIndicators.getDistribIndicName("non_vert_roof_area", 'h', h_bot, h_up).toString().toUpperCase())

            // Create names for facade density
            String name_h = Geoindicators.RsuIndicators.getDistribIndicName("projected_facade_area_distribution", 'h', h_bot, h_up).toString().toUpperCase()
            for (int d = 0; d < parameters.facadeDensNumberOfDirection / 2; d++) {
                int d_bot = d * 360 / parameters.facadeDensNumberOfDirection
                int d_up = d_bot + rangeDeg
                listFacadeDistrib.add(Geoindicators.RsuIndicators.getDistribIndicName(name_h, 'd', d_bot, d_up).toString().toUpperCase())
            }
        }
        // Indicator list (at RSU scale) for each building height level
        List listHeightDistrib = []
        for (int i in 0..parameters.buildHeightListLayersBottom.size() - 1) {
            Integer h_bot = parameters.buildHeightListLayersBottom[i]
            Integer h_up
            if (h_bot == parameters.buildHeightListLayersBottom[-1]) {
                h_up = null
            } else {
                h_up = parameters.buildHeightListLayersBottom[i + 1]
            }
            listHeightDistrib.add(Geoindicators.RsuIndicators.getDistribIndicName("roof_fraction_distribution", 'h', h_bot, h_up).toString().toUpperCase())
        }
        listNames = [
                "TEB" : ["VERT_ROOF_DENSITY", "NON_VERT_ROOF_DENSITY"] +
                        listRoadDir + listFacadeDistrib + listRoofDensDistrib + listBuildTypTeb + listHeightDistrib +
                        ["AVG_HEIGHT_ROOF", "STD_HEIGHT_ROOF", "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH"],
                "UTRF": ["AREA", "ASPECT_RATIO", "BUILDING_TOTAL_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                         "VEGETATION_FRACTION_UTRF", "LOW_VEGETATION_FRACTION_UTRF", "HIGH_VEGETATION_IMPERVIOUS_FRACTION_UTRF",
                         "HIGH_VEGETATION_PERVIOUS_FRACTION_UTRF", "ROAD_FRACTION_UTRF", "IMPERVIOUS_FRACTION_UTRF",
                         "AVG_NUMBER_BUILDING_NEIGHBOR", "AVG_HEIGHT_ROOF_AREA_WEIGHTED",
                         "STD_HEIGHT_ROOF_AREA_WEIGHTED", "BUILDING_NUMBER_DENSITY", "BUILDING_VOLUME_DENSITY",
                         "BUILDING_VOLUME_DENSITY", "AVG_VOLUME", "GROUND_LINEAR_ROAD_DENSITY",
                         "GEOM_AVG_HEIGHT_ROOF", "BUILDING_FLOOR_AREA_DENSITY",
                         "AVG_MINIMUM_BUILDING_SPACING", "MAIN_BUILDING_DIRECTION", "BUILDING_DIRECTION_UNIQUENESS",
                         "BUILDING_DIRECTION_EQUALITY"],
                "LCZ" : ["BUILDING_FRACTION_LCZ", "ASPECT_RATIO", "GROUND_SKY_VIEW_FACTOR", "PERVIOUS_FRACTION_LCZ",
                         "IMPERVIOUS_FRACTION_LCZ", "GEOM_AVG_HEIGHT_ROOF", "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH", "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS",
                         "HIGH_VEGETATION_FRACTION_LCZ", "LOW_VEGETATION_FRACTION_LCZ", "WATER_FRACTION_LCZ"] + listBuildTypLcz]

        // Basic columns at RSU scale
        listColBasic = ["ID_RSU", "THE_GEOM"]

        // Indicators common to each indicator use
        listColCommon = ["LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_FRACTION",
                         "BUILDING_FRACTION", "WATER_FRACTION", "ROAD_FRACTION", "IMPERVIOUS_FRACTION",
                         "HIGH_VEGETATION_LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_WATER_FRACTION",
                         "HIGH_VEGETATION_ROAD_FRACTION", "HIGH_VEGETATION_IMPERVIOUS_FRACTION",
                         "HIGH_VEGETATION_BUILDING_FRACTION", "UNDEFINED_FRACTION", "BUILDING_FLOOR_AREA_DENSITY"]

        // Column names in the LCZ Table
        listColLcz = ["LCZ_PRIMARY", "LCZ_SECONDARY", "LCZ_EQUALITY_VALUE", "LCZ_UNIQUENESS_VALUE", "MIN_DISTANCE"]

        // Indicator lists for urban typology use at building and block scales
        listUrbTyp =
                ["Bu": ["THE_GEOM", "ID_RSU", "ID_BUILD", "ID_BLOCK", "NB_LEV", "ZINDEX", "MAIN_USE", "TYPE", "ID_SOURCE",
                        "HEIGHT_ROOF", "HEIGHT_WALL", "PERIMETER", "AREA", "VOLUME", "FLOOR_AREA", "TOTAL_FACADE_LENGTH", "COMMON_WALL_FRACTION",
                        "CONTIGUITY", "AREA_CONCAVITY", "FORM_FACTOR", "RAW_COMPACTNESS", "PERIMETER_CONVEXITY",
                        "MINIMUM_BUILDING_SPACING", "NUMBER_BUILDING_NEIGHBOR", "ROAD_DISTANCE", "LIKELIHOOD_LARGE_BUILDING"],
                 "Bl": ["THE_GEOM", "ID_RSU", "ID_BLOCK", "AREA", "FLOOR_AREA", "VOLUME", "HOLE_AREA_DENSITY", "MAIN_BUILDING_DIRECTION",
                        "BUILDING_DIRECTION_UNIQUENESS", "BUILDING_DIRECTION_EQUALITY", "CLOSINGNESS", "NET_COMPACTNESS",
                        "AVG_HEIGHT_ROOF_AREA_WEIGHTED", "STD_HEIGHT_ROOF_AREA_WEIGHTED"]]

    }

    @Test
    void GeoIndicatorsTest1() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)
        def indicatorUse = ["LCZ", "UTRF", "TEB"]
        def prefixName = ""
        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "", "", "", "", "",
                ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)

        datasource.save(geoIndicatorsCompute_i.rsu_indicators, "/tmp/rsu.geojson", true)
        assertNotNull(geoIndicatorsCompute_i)
        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)
        assertEquals(listUrbTyp.Bu.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.building_indicators).sort())
        assertEquals(listUrbTyp.Bl.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.block_indicators).sort())


        List expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        List expectListRsu = expectListRsuTempo.toUnique()
        List realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        def expectListLczTempo = listColLcz
        expectListLczTempo = expectListLczTempo + listColBasic
        def expectListLcz = expectListLczTempo.sort()
        assertEquals(expectListLcz, datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort())

        def dfRsu = DataFrame.of(datasource.getTable(geoIndicatorsCompute_i.rsu_indicators))
        assertEquals dfRsu.nrows(), dfRsu.omitNullRows().nrows()
        def dfBuild = DataFrame.of(datasource.getTable(geoIndicatorsCompute_i.building_indicators))
        dfBuild = dfBuild.drop("ID_RSU")
        assertEquals dfBuild.nrows(), dfBuild.omitNullRows().nrows()
        def dfBlock = DataFrame.of(datasource.getTable(geoIndicatorsCompute_i.block_indicators))
        dfBlock = dfBlock.drop("ID_RSU")
        assertEquals dfBlock.nrows(), dfBlock.omitNullRows().nrows()

        // Test that the sum of all building fractions is 100% for both LCZ and TEB building types
        if (listBuildTypTeb) {
            def sum_afrac_teb = datasource.firstRow("SELECT AVG(${listBuildTypTeb.join("+")}) AS SUM_FRAC FROM ${"$geoIndicatorsCompute_i.rsu_indicators"} WHERE BUILDING_DIRECTION_UNIQUENESS <> -1")
            assertEquals sum_afrac_teb.SUM_FRAC, 1.0, 0.01
        }
        if (listBuildTypLcz) {
            def sum_afrac_lcz = datasource.firstRow("SELECT AVG(${listBuildTypLcz.join("+")}) AS SUM_FRAC FROM ${"$geoIndicatorsCompute_i.rsu_indicators"} WHERE BUILDING_DIRECTION_UNIQUENESS <> -1")
            assertEquals sum_afrac_lcz.SUM_FRAC, 1.0, 0.01
        }
        if (listFloorBuildTypLcz) {
            def sum_fafrac_lcz = datasource.firstRow("SELECT AVG(${listFloorBuildTypLcz.join("+")}) AS SUM_FRAC FROM ${"$geoIndicatorsCompute_i.rsu_indicators"} WHERE BUILDING_DIRECTION_UNIQUENESS <> -1")
            assertEquals sum_fafrac_lcz.SUM_FRAC, 1.0, 0.01
        }
        if (listFloorBuildTypTeb) {
            def sum_fafrac_teb = datasource.firstRow("SELECT AVG(${listFloorBuildTypTeb.join("+")}) AS SUM_FRAC FROM ${"$geoIndicatorsCompute_i.rsu_indicators"} WHERE BUILDING_DIRECTION_UNIQUENESS <> -1")
            assertEquals sum_fafrac_teb.SUM_FRAC, 1.0, 0.01
        }
    }

    @Test
    void GeoIndicatorsTest2() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)
        def prefixName = ""
        def indicatorUse = ["UTRF"]
        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", "",
                ["indicatorUse": indicatorUse, svfSimplified: false, "utrfModelName": "UTRF_BDTOPO_V2_RF_2_2.model"], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.building_indicators).sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.block_indicators).sort())
            // Check that the sum of proportion (or building area) for each RSU is equal to 1
            def colUtrfArea = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_utrf_area)
            colUtrfArea = colUtrfArea.minus(["ID_RSU", "THE_GEOM", "TYPO_MAJ", "TYPO_SECOND", "UNIQUENESS_VALUE"])
            def countSumAreaEqual1 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_area}
                                                                    WHERE ${colUtrfArea.join("+")}>0.99 AND ${colUtrfArea.join("+")}<1.01""")
            def countSumAreaRemove0 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_floor_area}
                                                                    WHERE ${colUtrfArea.join("+")}>0""")
            assertEquals countSumAreaRemove0.NB, countSumAreaEqual1.NB

            // Check that the sum of proportion (or building floor area) for each RSU is equal to 1
            def colUtrfFloorArea = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_utrf_floor_area)

            // Test that the TYPO_SECOND is inside the RSU UTRF table
            assertEquals 1, colUtrfFloorArea.count("TYPO_SECOND")

            colUtrfFloorArea = colUtrfFloorArea.minus(["ID_RSU", "THE_GEOM", "TYPO_MAJ", "TYPO_SECOND", "UNIQUENESS_VALUE"])
            def countSumFloorAreaEqual1 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_floor_area}
                                                                    WHERE ${colUtrfFloorArea.join("+")}>0.99 AND ${colUtrfFloorArea.join("+")}<1.01""")
            def countSumFloorAreaRemove0 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_floor_area}
                                                                    WHERE ${colUtrfFloorArea.join("+")}>0""")
            assertEquals countSumFloorAreaRemove0.NB, countSumFloorAreaEqual1.NB

            // Check that all buildings being in the zone have a value different than 0 (0 being no value)
            def dfBuild = DataFrame.of(datasource.getTable(geoIndicatorsCompute_i.building_utrf))
            def nbNull = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                            FROM ${geoIndicatorsCompute_i.building_utrf}
                                                            WHERE I_TYPO = 'unknown'""")
            assertTrue dfBuild.nrows() > 0
            assertEquals 0, nbNull.NB
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)

        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest3() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = false
        def prefixName = ""
        def indicatorUse = ["UTRF", "TEB"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "", "",
                "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.building_indicators).sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.block_indicators).sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest4() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = false
        def prefixName = ""

        def indicatorUse = ["TEB"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        datasource.save(geoIndicatorsCompute_i.rsu_indicators, "/tmp/test4.geojson", true)
        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.building_indicators).sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.block_indicators).sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest5() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = false
        def prefixName = ""
        def indicatorUse = ["LCZ", "TEB"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.building_indicators).sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.block_indicators).sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest6() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)

        def prefixName = ""

        def indicatorUse = ["UTRF", "LCZ"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators
                .computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                        inputTableNames.buildingTable, inputTableNames.roadTable,
                        inputTableNames.railTable, inputTableNames.vegetationTable,
                        inputTableNames.hydrographicTable, "", "", "",
                        "", "",
                        ["indicatorUse": indicatorUse, "svfSimplified": false],
                        prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.building_indicators).sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getColumnNames(geoIndicatorsCompute_i.block_indicators).sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest7() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = true
        def prefixName = ""
        def indicatorUse = ["LCZ"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getColumnNames(geoIndicatorsCompute_i.rsu_indicators)
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            assertEquals("ID_RSU,LCZ_EQUALITY_VALUE,LCZ_PRIMARY,LCZ_SECONDARY,LCZ_UNIQUENESS_VALUE,MIN_DISTANCE,THE_GEOM", datasource.getColumnNames(geoIndicatorsCompute_i.rsu_lcz).sort().join(","))
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void rasterizeIndicators1() {
        datasource.execute("""
        DROP TABLE IF EXISTS building;
        CREATE TABLE BUILDING (id_build int, id_block int, id_rsu int, zindex int, the_geom geometry, height_wall float, height_roof float, type varchar, pop double precision );
        INSERT INTO BUILDING VALUES (1, 1, 1, 0, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'::GEOMETRY, 5, 10, 'office', 100);
        """.toString())
        String grid = Geoindicators.WorkflowGeoIndicators.createGrid(datasource, datasource.getExtent("building"), 10, 10, 0)
        assertNotNull(grid)
        String grid_indicators = Geoindicators.WorkflowGeoIndicators.rasterizeIndicators(datasource, grid, [],
                "building", null, null, null, null, null,
                null, null, null)
        assertNull(grid_indicators)
        def list_indicators = ["BUILDING_FRACTION", "BUILDING_HEIGHT", "BUILDING_POP",
                               "BUILDING_TYPE_FRACTION", "WATER_FRACTION", "VEGETATION_FRACTION",
                               "ROAD_FRACTION", "IMPERVIOUS_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                               "BUILDING_HEIGHT_WEIGHTED", "BUILDING_SURFACE_DENSITY",
                               "SEA_LAND_FRACTION", "ASPECT_RATIO", "SVF",
                               "HEIGHT_OF_ROUGHNESS_ELEMENTS", "TERRAIN_ROUGHNESS_CLASS"]
        grid_indicators = Geoindicators.WorkflowGeoIndicators.rasterizeIndicators(datasource, grid, list_indicators,
                "building", null, null, null, null, null,
                null, null, null)
        assertNotNull(grid_indicators)
        assertEquals(1, datasource.getRowCount(grid_indicators))
        def rows = datasource.firstRow("select * from $grid_indicators".toString())
        assertEquals(1d, rows.BUILDING_FRACTION)
        assertEquals(0d, rows.HIGH_VEGETATION_FRACTION)
        assertEquals(0d, rows.IMPERVIOUS_FRACTION)
        assertEquals(0d, rows.LOW_VEGETATION_FRACTION)
        assertEquals(0d, rows.ROAD_FRACTION)
        assertEquals(0d, rows.WATER_FRACTION)
        assertEquals(0d, rows.UNDEFINED_FRACTION)
        assertEquals(100d, rows.SUM_POP)
        assertEquals(10d, rows.AVG_HEIGHT_ROOF)
        assertEquals(0d, rows.STD_HEIGHT_ROOF)
        assertTrue(10d - rows.GEOM_AVG_HEIGHT_ROOF < 0.0001)
        assertEquals(10d, rows.AVG_HEIGHT_ROOF_AREA_WEIGHTED)
        assertEquals(0d, rows.STD_HEIGHT_ROOF_AREA_WEIGHTED)
        assertEquals(2d, rows.FREE_EXTERNAL_FACADE_DENSITY)
        assertEquals(3d, rows.BUILDING_SURFACE_DENSITY)
        assertTrue(1.5 - rows.EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH < 0.001)
        assertEquals(8, rows.EFFECTIVE_TERRAIN_ROUGHNESS_CLASS)
        assertNull(rows.ASPECT_RATIO)
        assertTrue(0.5 - rows.SVF < 0.1)
        assertEquals(1d, rows.TYPE_OFFICE)
    }


    /**
     * Method to check the result for the RSU indicators table
     * Please add new checks here
     */
    def checkRSUIndicators(def datasource, def rsuIndicatorsTableName) {
        //Check road_fraction > 0
        def countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE ROAD_FRACTION>0".toString())
        assertEquals(176, countResult.count)

        //Check building_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE BUILDING_FRACTION>0".toString())
        assertEquals(73, countResult.count)

        //Check high_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_fraction>0".toString())
        assertEquals(24, countResult.count)

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
        assertEquals(20, countResult.count)

        //Check high_vegetation_impervious_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_impervious_fraction>0".toString())
        assertEquals(0, countResult.count)

        //Check water_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE water_fraction>0".toString())
        assertEquals(2, countResult.count)

        //Check low_vegetation_fraction > 0.001
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE low_vegetation_fraction>0.001".toString())
        assertEquals(49, countResult.count)

        //Check impervious_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE impervious_fraction>0".toString())
        assertEquals(0, countResult.count)
    }

    @Test
    void GeoClimateProperties() {
        assertNotNull Geoindicators.version()
        assertNotNull Geoindicators.buildNumber()
    }

    @Disabled
    @Test
    void test() {
        datasource.load("/tmp/road_inter.geojson", "road", true)
        datasource.load("/tmp/rsu_table.geojson", "rsu", true)
        datasource.createSpatialIndex("road")
        datasource.createSpatialIndex("rsu")
        datasource.execute("""
        select st_intersection(a.the_geom, b.the_geom) as the_geom from road as a, rsu as b where a.the_geom && b.the_geom 
        and st_intersects(a.the_geom, b.the_geom);
        """.toString())
    }

    @Test
    void sprawlIndicators1() {
        //Data for test
        datasource.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid, 104 AS LCZ_PRIMARY FROM 
        ST_MakeGrid('POLYGON((0 0, 2500 0, 2500 2500, 0 0))'::GEOMETRY, 250, 250);
        --Center cell urban
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 5 AND id_col = 5;
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 6 AND id_col = 4; 
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 6 AND id_col = 5;
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 6 AND id_col = 6; 
        UPDATE grid SET LCZ_PRIMARY= 1 WHERE id_row = 5 AND id_col = 6; 
        """)
        def results = Geoindicators.WorkflowGeoIndicators.sprawlIndicators(datasource, "grid", "id_grid",  ["URBAN_SPRAWL_AREAS", "URBAN_SPRAWL_DISTANCES", "URBAN_SPRAWL_COOL_DISTANCES"],
               250/2 )
        def urban_cool_areas = results.urban_cool_areas
        def urban_sprawl_areas = results.urban_sprawl_areas
        assertEquals(312500, datasource.firstRow("select st_area(the_geom) as area from $urban_sprawl_areas").area, 0.0001)
        assertEquals(0, datasource.getRowCount(urban_cool_areas))
    }


    @Test
    void sprawlIndicators2() {
        //Data for test only vegetation
        datasource.execute("""
        --Grid values
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid AS SELECT  * EXCEPT(ID), id as id_grid, 104 AS LCZ_PRIMARY FROM 
        ST_MakeGrid('POLYGON((0 0, 2500 0, 2500 2500, 0 0))'::GEOMETRY, 250, 250);
        """.toString())
        def results = Geoindicators.WorkflowGeoIndicators.sprawlIndicators(datasource, "grid", "id_grid",  ["URBAN_SPRAWL_AREAS", "URBAN_SPRAWL_DISTANCES", "URBAN_SPRAWL_COOL_DISTANCES"],
                250/2 )
        def urban_cool_areas = results.urban_cool_areas
        def urban_sprawl_areas = results.urban_sprawl_areas
        assertTrue(datasource.isEmpty(urban_sprawl_areas))
        assertNull(urban_cool_areas)

    }


}
