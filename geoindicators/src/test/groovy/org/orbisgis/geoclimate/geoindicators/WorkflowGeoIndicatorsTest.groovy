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

    // Indicator list (at RSU scale) for each type of use
    public static listNames = [
            "TEB" : ["VERT_ROOF_DENSITY", "NON_VERT_ROOF_DENSITY",
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
            "UTRF": ["AREA", "ASPECT_RATIO", "BUILDING_TOTAL_FRACTION", "FREE_EXTERNAL_FACADE_DENSITY",
                     "VEGETATION_FRACTION_UTRF", "LOW_VEGETATION_FRACTION_UTRF", "HIGH_VEGETATION_IMPERVIOUS_FRACTION_UTRF",
                     "HIGH_VEGETATION_PERVIOUS_FRACTION_UTRF", "ROAD_FRACTION_UTRF", "IMPERVIOUS_FRACTION_UTRF",
                     "AVG_NUMBER_BUILDING_NEIGHBOR", "AVG_HEIGHT_ROOF_AREA_WEIGHTED",
                     "STD_HEIGHT_ROOF_AREA_WEIGHTED", "BUILDING_NUMBER_DENSITY", "BUILDING_VOLUME_DENSITY",
                     "BUILDING_VOLUME_DENSITY", "AVG_VOLUME", "GROUND_LINEAR_ROAD_DENSITY",
                     "GEOM_AVG_HEIGHT_ROOF", "BUILDING_FLOOR_AREA_DENSITY",
                     "AVG_MINIMUM_BUILDING_SPACING", "MAIN_BUILDING_DIRECTION", "BUILDING_DIRECTION_UNIQUENESS",
                     "BUILDING_DIRECTION_EQUALITY", "AREA_FRACTION_LIGHT_INDUSTRY", "FLOOR_AREA_FRACTION_RESIDENTIAL"],
            "LCZ" : ["BUILDING_FRACTION_LCZ", "ASPECT_RATIO", "GROUND_SKY_VIEW_FACTOR", "PERVIOUS_FRACTION_LCZ",
                     "IMPERVIOUS_FRACTION_LCZ", "GEOM_AVG_HEIGHT_ROOF", "EFFECTIVE_TERRAIN_ROUGHNESS_LENGTH", "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS",
                     "HIGH_VEGETATION_FRACTION_LCZ", "LOW_VEGETATION_FRACTION_LCZ", "WATER_FRACTION_LCZ", "AREA_FRACTION_LIGHT_INDUSTRY",
                     "FLOOR_AREA_FRACTION_RESIDENTIAL"]]

    // Basic columns at RSU scale
    public static listColBasic = ["ID_RSU", "THE_GEOM"]

    // Indicators common to each indicator use
    public static listColCommon = ["LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_FRACTION",
                                   "BUILDING_FRACTION", "WATER_FRACTION", "ROAD_FRACTION", "IMPERVIOUS_FRACTION",
                                   "HIGH_VEGETATION_LOW_VEGETATION_FRACTION", "HIGH_VEGETATION_WATER_FRACTION",
                                   "HIGH_VEGETATION_ROAD_FRACTION", "HIGH_VEGETATION_IMPERVIOUS_FRACTION",
                                   "HIGH_VEGETATION_BUILDING_FRACTION", "UNDEFINED_FRACTION"]

    // Column names in the LCZ Table
    public static listColLcz = ["LCZ_PRIMARY", "LCZ_SECONDARY", "LCZ_EQUALITY_VALUE", "LCZ_UNIQUENESS_VALUE", "MIN_DISTANCE"]

    // Indicator lists for urban typology use at building and block scales
    public static listUrbTyp =
            ["Bu": ["THE_GEOM", "ID_RSU", "ID_BUILD", "ID_BLOCK", "NB_LEV", "ZINDEX", "MAIN_USE", "TYPE", "ID_SOURCE",
                    "HEIGHT_ROOF", "HEIGHT_WALL", "PERIMETER", "AREA", "VOLUME", "FLOOR_AREA", "TOTAL_FACADE_LENGTH", "COMMON_WALL_FRACTION",
                    "CONTIGUITY", "AREA_CONCAVITY", "FORM_FACTOR", "RAW_COMPACTNESS", "PERIMETER_CONVEXITY",
                    "MINIMUM_BUILDING_SPACING", "NUMBER_BUILDING_NEIGHBOR", "ROAD_DISTANCE", "LIKELIHOOD_LARGE_BUILDING"],
             "Bl": ["THE_GEOM", "ID_RSU", "ID_BLOCK", "AREA", "FLOOR_AREA", "VOLUME", "HOLE_AREA_DENSITY", "MAIN_BUILDING_DIRECTION",
                    "BUILDING_DIRECTION_UNIQUENESS", "BUILDING_DIRECTION_EQUALITY", "CLOSINGNESS", "NET_COMPACTNESS",
                    "AVG_HEIGHT_ROOF_AREA_WEIGHTED", "STD_HEIGHT_ROOF_AREA_WEIGHTED"]]

    public static H2GIS datasource
    public static def inputTableNames

    @BeforeAll
    static void beforeAll() {
        datasource = open(folder.getAbsolutePath() + File.separator + "workflowGeoIndicatorsTest;AUTO_SERVER=TRUE")
        assertNotNull(datasource)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ROAD.geojson"), "ROAD", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("RAIL.geojson"), "RAIL", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("VEGET.geojson"), "VEGET", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("HYDRO.geojson"), "HYDRO", true)
        inputTableNames = [zoneTable: "ZONE", buildingTable: "BUILDING", roadTable: "ROAD",
                           railTable: "RAIL", vegetationTable: "VEGET", hydrographicTable: "HYDRO"]
    }


    @Test
    void GeoIndicatorsTest1() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)
        def indicatorUse = ["LCZ", "UTRF", "TEB"]
        def prefixName = ""
        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "", "", "", "",
                ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)
        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)
        assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(geoIndicatorsCompute_i.building_indicators).columns.sort())
        assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(geoIndicatorsCompute_i.block_indicators).columns.sort())


        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        def expectListLczTempo = listColLcz
        expectListLczTempo = expectListLczTempo + listColBasic
        def expectListLcz = expectListLczTempo.sort()
        assertEquals(expectListLcz, datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort())

        def dfRsu = DataFrame.of(datasource."$geoIndicatorsCompute_i.rsu_indicators")
        assertEquals dfRsu.nrows(), dfRsu.omitNullRows().nrows()
        def dfBuild = DataFrame.of(datasource."$geoIndicatorsCompute_i.building_indicators")
        dfBuild = dfBuild.drop("ID_RSU")
        assertEquals dfBuild.nrows(), dfBuild.omitNullRows().nrows()
        def dfBlock = DataFrame.of(datasource."$geoIndicatorsCompute_i.block_indicators")
        dfBlock = dfBlock.drop("ID_RSU")
        assertEquals dfBlock.nrows(), dfBlock.omitNullRows().nrows()

    }

    @Test
    void GeoIndicatorsTest2() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)
        def prefixName = ""
        def indicatorUse = ["UTRF"]
        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "",
                ["indicatorUse": indicatorUse, svfSimplified: false, "utrfModelName": "UTRF_BDTOPO_V2_RF_2_2.model"], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(geoIndicatorsCompute_i.building_indicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(geoIndicatorsCompute_i.block_indicators).columns.sort())
            // Check that the sum of proportion (or building area) for each RSU is equal to 1
            def utrfArea = datasource."$geoIndicatorsCompute_i.rsu_utrf_area"
            def colUtrfArea = utrfArea.getColumns()
            colUtrfArea = colUtrfArea.minus(["ID_RSU", "THE_GEOM", "TYPO_MAJ", "TYPO_SECOND", "UNIQUENESS_VALUE"])
            def countSumAreaEqual1 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_area}
                                                                    WHERE ${colUtrfArea.join("+")}>0.99 AND ${colUtrfArea.join("+")}<1.01""")
            def countSumAreaRemove0 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_floor_area}
                                                                    WHERE ${colUtrfArea.join("+")}>0""")
            assertEquals countSumAreaRemove0.NB, countSumAreaEqual1.NB

            // Check that the sum of proportion (or building floor area) for each RSU is equal to 1
            def utrfFloorArea = datasource."$geoIndicatorsCompute_i.rsu_utrf_floor_area"
            def colUtrfFloorArea = utrfFloorArea.getColumns()
            colUtrfFloorArea = colUtrfFloorArea.minus(["ID_RSU", "THE_GEOM", "TYPO_MAJ", "TYPO_SECOND", "UNIQUENESS_VALUE"])
            def countSumFloorAreaEqual1 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_floor_area}
                                                                    WHERE ${colUtrfFloorArea.join("+")}>0.99 AND ${colUtrfFloorArea.join("+")}<1.01""")
            def countSumFloorAreaRemove0 = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                                    FROM ${geoIndicatorsCompute_i.rsu_utrf_floor_area}
                                                                    WHERE ${colUtrfFloorArea.join("+")}>0""")
            assertEquals countSumFloorAreaRemove0.NB, countSumFloorAreaEqual1.NB

            // Check that all buildings being in the zone have a value different than 0 (0 being no value)
            def dfBuild = DataFrame.of(datasource."$geoIndicatorsCompute_i.building_utrf")
            def nbNull = datasource.firstRow("""SELECT COUNT(*) AS NB 
                                                            FROM ${geoIndicatorsCompute_i.building_utrf}
                                                            WHERE I_TYPO = 'unknown'""")
            assertTrue dfBuild.nrows() > 0
            assertEquals 0, nbNull.NB
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns

        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest3() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = false
        def prefixName = ""
        def indicatorUse = ["UTRF", "TEB"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(geoIndicatorsCompute_i.building_indicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(geoIndicatorsCompute_i.block_indicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest4() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = false
        def prefixName = ""

        def indicatorUse = ["TEB"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(geoIndicatorsCompute_i.building_indicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(geoIndicatorsCompute_i.block_indicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest5() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = false
        def prefixName = ""
        def indicatorUse = ["LCZ", "TEB"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(geoIndicatorsCompute_i.building_indicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(geoIndicatorsCompute_i.block_indicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest6() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)

        def prefixName = ""

        def indicatorUse = ["UTRF", "LCZ"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators
                .computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                        inputTableNames.buildingTable, inputTableNames.roadTable,
                        inputTableNames.railTable, inputTableNames.vegetationTable,
                        inputTableNames.hydrographicTable, "", "", "", "",
                        ["indicatorUse": indicatorUse, "svfSimplified": false],
                        prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        checkRSUIndicators(datasource, geoIndicatorsCompute_i.rsu_indicators)

        if (indicatorUse.contains("UTRF")) {
            assertEquals(listUrbTyp.Bu.sort(), datasource.getTable(geoIndicatorsCompute_i.building_indicators).columns.sort())
            assertEquals(listUrbTyp.Bl.sort(), datasource.getTable(geoIndicatorsCompute_i.block_indicators).columns.sort())
        }
        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            def expectListLczTempo = listColLcz
            expectListLczTempo = expectListLczTempo + listColBasic
            def expectListLcz = expectListLczTempo.sort()
            assertEquals(expectListLcz, datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort())
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    @Test
    void GeoIndicatorsTest7() {
        //Reload the building table because the original table is updated with the block and rsu identifiers
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("BUILDING.geojson"), "BUILDING", true)
        datasource.load(WorkflowGeoIndicatorsTest.class.getResource("ZONE.geojson"), "ZONE", true)
        boolean svfSimplified = true
        def prefixName = ""
        def indicatorUse = ["LCZ"]

        Map geoIndicatorsCompute_i = Geoindicators.WorkflowGeoIndicators.computeAllGeoIndicators(datasource, inputTableNames.zoneTable,
                inputTableNames.buildingTable, inputTableNames.roadTable,
                inputTableNames.railTable, inputTableNames.vegetationTable,
                inputTableNames.hydrographicTable, "",
                "", "", "", ["indicatorUse": indicatorUse, svfSimplified: false], prefixName)
        assertNotNull(geoIndicatorsCompute_i)

        def expectListRsuTempo = listColBasic + listColCommon
        expectListRsuTempo = (expectListRsuTempo + indicatorUse.collect { listNames[it] }).flatten()
        def expectListRsu = expectListRsuTempo.toUnique()
        def realListRsu = datasource.getTable(geoIndicatorsCompute_i.rsu_indicators).columns
        // We test that there is no missing indicators in the RSU table
        for (i in expectListRsu) {
            assertTrue realListRsu.contains(i)
        }
        if (indicatorUse.contains("LCZ")) {
            assertEquals("ID_RSU,LCZ_EQUALITY_VALUE,LCZ_PRIMARY,LCZ_SECONDARY,LCZ_UNIQUENESS_VALUE,MIN_DISTANCE,THE_GEOM", datasource.getTable(geoIndicatorsCompute_i.rsu_lcz).columns.sort().join(","))
        } else {
            assertEquals(null, geoIndicatorsCompute_i.rsu_lcz)
        }
    }

    /**
     * Method to check the result for the RSU indicators table
     * Please add new checks here
     */
    def checkRSUIndicators(def datasource, def rsuIndicatorsTableName) {
        //Check road_fraction > 0
        def countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE ROAD_FRACTION>0".toString())
        assertEquals(184, countResult.count)

        //Check building_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE BUILDING_FRACTION>0".toString())
        assertEquals(70, countResult.count)

        //Check high_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE high_vegetation_fraction>0".toString())
        assertEquals(18, countResult.count)

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

        //Check low_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE low_vegetation_fraction>0".toString())
        assertEquals(50, countResult.count)

        //Check low_vegetation_fraction > 0
        countResult = datasource.firstRow("select count(*) as count from ${rsuIndicatorsTableName} WHERE impervious_fraction>0".toString())
        assertEquals(0, countResult.count)
    }

    @Test
    void GeoClimateProperties() {
        assert "0.0.2-SNAPSHOT" == Geoindicators.version()
        assertNotNull Geoindicators.buildNumber()
    }

}
