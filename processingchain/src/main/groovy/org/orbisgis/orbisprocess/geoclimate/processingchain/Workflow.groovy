package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.transform.BaseScript
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess
import org.orbisgis.orbisprocess.geoclimate.geoindicators.Geoindicators

@BaseScript ProcessingChain processingChain


/**
 * Load the BDTopo layers from a folder and compute geoindicators (by default the ones needed for the LCZ classification,
 * for the urban typology classification and for the TEB model). Note that the LCZ classification is performed but
 * should not be trusted now.
 *
 * @param datasource A connection to a database
 * @param inputFolder The path of the folder that must contains the BDTopo V2 shapefiles
 * @param distance The integer value to expand the envelope of zone when recovering the data from OSM
 * (some objects may be badly truncated if they are not within the envelope)
 * @param indicatorUse List of geoindicator types to compute (default ["LCZ", "URBAN_TYPOLOGY", "TEB"]
 *                  --> "LCZ" : compute the indicators needed for the LCZ classification (Stewart et Oke, 2012)
 *                  --> "URBAN TYPOLOGY" : compute the indicators needed for the urban typology classification (Bocher et al., 2017)
 *                  --> "TEB" : compute the indicators needed for the Town Energy Balance model
 * @param svfSimplified A boolean indicating whether or not the simplified version of the SVF should be used. This
 * version is faster since it is based on a simple relationship between ground SVF calculated at RSU scale and
 * facade density (Bernard et al. 2018).
 * @param prefixName A prefix used to name the output table (default ""). Could be useful in case the user wants to
 * investigate the sensibility of the chain to some input parameters
 * @param mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 * @param outputFolder The path of the folder to store the results as geojson files. By default, the files are stored
 * in the subfolder "results" in the inputFolder
 *
 * @return the path of the folder that contains the geojson files
 *
 *
 * References:
 * --> Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 * --> Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. Sky View Factor Calculation in
 * Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools. Climate ,
 * MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American
 * Meteorological Society 93, no. 12 (2012): 1879-1900.
 *
 */
def BDTOPO_V2() {
    create({
        title "Create all geoindicators from BDTopo data"
        inputs datasource: JdbcDataSource, inputFolder: String, distance: 1000,indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"],
                svfSimplified:false, prefixName: "",
                mapOfWeights : ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                                "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                                "height_of_roughness_elements": 1, "terrain_roughness_class": 1],
                hLevMin : 3, hLevMax: 15, hThresholdLev2: 10,
                outputFolder:""
        outputs outputFolder: String
        run {datasource, inputFolder, distance,indicatorUse, svfSimplified, prefixName, mapOfWeights, hLevMin, hLevMax, hThresholdLev2,outputFolder ->

            if(inputFolder){
                def outputDir = new File(outputFolder)
                def folder = new File(inputFolder)
                if(folder.isDirectory()){
                    def shapeFiles  = []
                    folder.traverse(type: groovy.io.FileType.FILES, nameFilter: ~/.*\.shp/) { File shapeFile ->
                        shapeFiles << shapeFile.getAbsolutePath()
                    }
                    if(!outputFolder){
                        outputDir=new File("$inputFolder${File.separator}results")
                        if(!outputDir.exists()){
                            outputDir.mkdir()
                        }
                    }
                    else {
                        if(!outputDir.isDirectory()){
                            error "Please set a valid output folder"
                            return null
                        }
                    }
                    //Looking for IRIS_GE shape file
                    if(shapeFiles.find{ it.toLowerCase().endsWith("iris_ge.shp")}) {
                        //Load the shapefiles
                        shapeFiles.each { shp ->
                            datasource.load(shp, true)
                        }
                        //Prepare iteration on each insee code
                        def inseeCodes = []
                        datasource.eachRow("select distinct insee_com from IRIS_GE group by insee_com ;") { row ->
                            inseeCodes << row.insee_com
                        }

                        //Let's run the BDTopo process for each insee code
                        def prepareBDTopoData = ProcessingChain.PrepareBDTopo.prepareBDTopo()
                        int nbAreas = inseeCodes.size();
                        info "$nbAreas areas will be processed"
                        inseeCodes.eachWithIndex { code, index->
                            info "Starting to process insee code $code"
                             if(prepareBDTopoData.execute([datasource                 : datasource,
                                                        tableIrisName              : 'IRIS_GE', tableBuildIndifName: 'BATI_INDIFFERENCIE',
                                                        tableBuildIndusName        : 'BATI_INDUSTRIEL', tableBuildRemarqName: 'BATI_REMARQUABLE',
                                                        tableRoadName              : 'ROUTE', tableRailName: 'TRONCON_VOIE_FERREE',
                                                        tableHydroName             : 'SURFACE_EAU', tableVegetName: 'ZONE_VEGETATION',
                                                        tableImperviousSportName   : 'TERRAIN_SPORT', tableImperviousBuildSurfName: 'CONSTRUCTION_SURFACIQUE',
                                                        tableImperviousRoadSurfName: 'SURFACE_ROUTE', tableImperviousActivSurfName: 'SURFACE_ACTIVITE',
                                                        distBuffer                 : 500, expand: distance, idZone: code,
                                                        hLevMin                    : hLevMin, hLevMax: hLevMax, hThresholdLev2: hThresholdLev2
                        ])){

                             String buildingTableName = prepareBDTopoData.getResults().outputBuilding

                             String roadTableName = prepareBDTopoData.getResults().outputRoad

                             String railTableName = prepareBDTopoData.getResults().outputRail

                             String hydrographicTableName = prepareBDTopoData.getResults().outputHydro

                             String vegetationTableName = prepareBDTopoData.getResults().outputVeget

                             String zoneTableName = prepareBDTopoData.getResults().outputZone

                             IProcess geoIndicators = GeoIndicators()
                             if (!geoIndicators.execute( datasource          : datasource,           zoneTable       : zoneTableName,
                                     buildingTable       : buildingTableName,    roadTable       : roadTableName,
                                     railTable           : railTableName,        vegetationTable : vegetationTableName,
                                     hydrographicTable   : hydrographicTableName,indicatorUse    : indicatorUse,
                                     svfSimplified       : svfSimplified,        prefixName      : "zone_$code",
                                     mapOfWeights        : mapOfWeights)) {
                                 error "Cannot build the geoindicators for the area $code"
                             }
                             else{
                                 def tablesToSave = geoIndicators.getResults().collect {
                                     if (it.value){
                                         datasource.save(it.value,"${outputDir.getAbsolutePath()}${File.separator}${it.value}.geojson")
                                         datasource.execute "DROP TABLE IF EXISTS ${it.value};"
                                        }
                                     }
                                 info "${code} has been processed"
                             }

                         }
                            info "Number of areas processed ${index+1} on $nbAreas"
                    }

                    }
                    else{
                        error "The input folder must be contains the iris_ge.shp file"
                        return null
                    }
                }
                else{
                    error "The input folder must be a directory"
                    return null
                }
                return [outputFolder : outputDir.getAbsolutePath()]
            }
        }
    })

}

/**
 * Extract OSM data from a place name and compute geoindicators (by default the ones needed for the LCZ classification,
 * for the urban typology classification and for the TEB model). Note that the LCZ classification is performed but
 * should not be trusted now.
 *
 * @param datasource A connection to a database
 * @param zoneToExtract A zone to extract. Can be, a name of the place (neighborhood, city, etc. - cf https://wiki.openstreetmap.org/wiki/Key:level)
 * or a bounding box specified as a JTS envelope
 * @param distance The integer value to expand the envelope of zone when recovering the data from OSM
 * (some objects may be badly truncated if they are not within the envelope)
 * @param indicatorUse List of geoindicator types to compute (default ["LCZ", "URBAN_TYPOLOGY", "TEB"]
 *                  --> "LCZ" : compute the indicators needed for the LCZ classification (Stewart et Oke, 2012)
 *                  --> "URBAN TYPOLOGY" : compute the indicators needed for the urban typology classification (Bocher et al., 2017)
 *                  --> "TEB" : compute the indicators needed for the Town Energy Balance model
 * @param svfSimplified A boolean indicating whether or not the simplified version of the SVF should be used. This
 * version is faster since it is based on a simple relationship between ground SVF calculated at RSU scale and
 * facade density (Bernard et al. 2018).
 * @param prefixName A prefix used to name the output table (default ""). Could be useful in case the user wants to
 * investigate the sensibility of the chain to some input parameters
 * @param mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
 * of the map) for the LCZ classification step (default : all values to 1)
 * @param hLevMin Minimum building level height
 * @param hLevMax Maximum building level height
 * @param hThresholdLev2 Threshold on the building height, used to determine the number of levels
 *
 *
 * @return 10 tables : zoneTable , zoneEnvelopeTableName, buildingTable,
 * roadTable, railTable, vegetationTable,
 * hydrographicTable, buildingIndicators,
 * blockIndicators, rsuIndicators, rsuLcz
 *
 *
 * References:
 * --> Bocher, E., Petit, G., Bernard, J., & Palominos, S. (2018). A geoprocessing framework to compute
 * urban indicators: The MApUCE tools chain. Urban climate, 24, 153-174.
 * --> Jérémy Bernard, Erwan Bocher, Gwendall Petit, Sylvain Palominos. Sky View Factor Calculation in
 * Urban Context: Computational Performance and Accuracy Analysis of Two Open and Free GIS Tools. Climate ,
 * MDPI, 2018, Urban Overheating - Progress on Mitigation Science and Engineering Applications, 6 (3), pp.60.
 * --> Stewart, Ian D., and Tim R. Oke. "Local climate zones for urban temperature studies." Bulletin of the American
 * Meteorological Society 93, no. 12 (2012): 1879-1900.
 *
 */
def OSM() {
    return create({
        title "Create all geoindicators from OSM data"
        inputs datasource: JdbcDataSource, zoneToExtract: Object, distance: 0,indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"],
                svfSimplified:false, prefixName: "",
                mapOfWeights : ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                                 "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                                 "height_of_roughness_elements": 1, "terrain_roughness_class": 1],
                hLevMin : 3, hLevMax: 15, hThresholdLev2: 10
        outputs zoneTable: String, zoneEnvelopeTableName: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, buildingIndicators: String,
                blockIndicators: String,
                rsuIndicators: String, rsuLcz: String
        run { datasource, zoneToExtract, distance, indicatorUse, svfSimplified, prefixName, mapOfWeights, hLevMin, hLevMax, hThresholdLev2 ->

                IProcess prepareOSMData = ProcessingChain.PrepareOSM.buildGeoclimateLayers()

                if (!prepareOSMData.execute([datasource: datasource, zoneToExtract: zoneToExtract, distance: distance, hLevMin: hLevMin,
                                             hLevMax   : hLevMax, hThresholdLev2: hThresholdLev2])) {
                    error "Cannot extract the GIS layers from the zone to extract  ${zoneToExtract}"
                    return null
                }

                String buildingTableName = prepareOSMData.getResults().outputBuilding

                String roadTableName = prepareOSMData.getResults().outputRoad

                String railTableName = prepareOSMData.getResults().outputRail

                String hydrographicTableName = prepareOSMData.getResults().outputHydro

                String vegetationTableName = prepareOSMData.getResults().outputVeget

                String zoneTableName = prepareOSMData.getResults().outputZone

                String zoneEnvelopeTableName = prepareOSMData.getResults().outputZoneEnvelope

                IProcess geoIndicators = GeoIndicators()
                if (!geoIndicators.execute(datasource: datasource, zoneTable: zoneTableName,
                        buildingTable: buildingTableName, roadTable: roadTableName,
                        railTable: railTableName, vegetationTable: vegetationTableName,
                        hydrographicTable: hydrographicTableName, indicatorUse: indicatorUse,
                        svfSimplified: svfSimplified, prefixName: prefixName,
                        mapOfWeights: mapOfWeights)) {
                    error "Cannot build the geoindicators"
                    return null
                }

                return [zoneTable         : zoneTableName, zoneEnvelopeTableName: zoneEnvelopeTableName, buildingTable: buildingTableName,
                        roadTable         : roadTableName, railTable: railTableName, vegetationTable: vegetationTableName,
                        hydrographicTable : hydrographicTableName,
                        buildingIndicators: geoIndicators.results.outputTableBuildingIndicators,
                        blockIndicators   : geoIndicators.results.outputTableBlockIndicators,
                        rsuIndicators     : geoIndicators.results.outputTableRsuIndicators,
                        rsuLcz            : geoIndicators.results.outputTableRsuLcz]
        }
    })
}

/**
 * Compute all geoindicators at the 3 scales :
 * building, block and RSU
 * Compute also the LCZ classification and the urban typology
 *
 * @return 4 tables outputTableBuildingIndicators, outputTableBlockIndicators, outputTableRsuIndicators,
 * outputTableRsuLcz . The first three tables contains the geoindicators and the last table the LCZ classification.
 * This table can be empty if the user decides not to calculate it.
 *
 */
def GeoIndicators() {
    def final COLUMN_ID_RSU = "id_rsu"
    def final GEOMETRIC_COLUMN = "the_geom"

    return create({
        title "Compute all geoindicators"
        inputs datasource: JdbcDataSource, zoneTable: String, buildingTable: String,
                roadTable: String, railTable: String, vegetationTable: String,
                hydrographicTable: String, surface_vegetation: 100000, surface_hydro: 2500,
                distance: 0.01, indicatorUse: ["LCZ", "URBAN_TYPOLOGY", "TEB"], svfSimplified:false, prefixName: "",
                mapOfWeights: ["sky_view_factor" : 1, "aspect_ratio": 1, "building_surface_fraction": 1,
                               "impervious_surface_fraction" : 1, "pervious_surface_fraction": 1,
                               "height_of_roughness_elements": 1, "terrain_roughness_class": 1]
        outputs outputTableBuildingIndicators: String, outputTableBlockIndicators: String,
                outputTableRsuIndicators: String, outputTableRsuLcz:String
        run { datasource, zoneTable, buildingTable, roadTable, railTable, vegetationTable, hydrographicTable,
              surface_vegetation, surface_hydro, distance,indicatorUse,svfSimplified, prefixName, mapOfWeights ->
            info "Start computing the geoindicators..."

            // Temporary tables are created
            def lczIndicTable = "LCZ_INDIC_TABLE$uuid"

            // Output Lcz table name is set to null in case LCZ indicators are not calculated
            def rsuLcz = null

            //Create spatial units and relations : building, block, rsu
            IProcess spatialUnits = ProcessingChain.BuildSpatialUnits.createUnitsOfAnalysis()
            if (!spatialUnits.execute([datasource       : datasource,           zoneTable           : zoneTable,
                                       buildingTable    : buildingTable,        roadTable           : roadTable,
                                       railTable        : railTable,            vegetationTable     : vegetationTable,
                                       hydrographicTable: hydrographicTable,    surface_vegetation  : surface_vegetation,
                                       surface_hydro    : surface_hydro,        distance            : distance,
                                       prefixName       : prefixName,
                                       indicatorUse:indicatorUse])) {
                error "Cannot create the spatial units"
                return null
            }
            String relationBuildings = spatialUnits.getResults().outputTableBuildingName
            String relationBlocks = spatialUnits.getResults().outputTableBlockName
            String relationRSU = spatialUnits.getResults().outputTableRsuName

            //Compute building indicators
            def computeBuildingsIndicators = ProcessingChain.BuildGeoIndicators.computeBuildingsIndicators()
            if (!computeBuildingsIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: relationBuildings,
                                                     inputRoadTableName    : roadTable,
                                                     indicatorUse          : indicatorUse,
                                                     prefixName            : prefixName])) {
                error "Cannot compute the building indicators"
                return null
            }

            def buildingIndicators = computeBuildingsIndicators.results.outputTableName

            //Compute block indicators
            def blockIndicators = null
            if(indicatorUse*.toUpperCase().contains("URBAN_TYPOLOGY")){
                def computeBlockIndicators = ProcessingChain.BuildGeoIndicators.computeBlockIndicators()
                if (!computeBlockIndicators.execute([datasource            : datasource,
                                                     inputBuildingTableName: buildingIndicators,
                                                     inputBlockTableName   : relationBlocks,
                                                     prefixName            : prefixName])) {
                    error "Cannot compute the block indicators"
                    return null
                }
                blockIndicators = computeBlockIndicators.results.outputTableName
            }

            //Compute RSU indicators
            def computeRSUIndicators = ProcessingChain.BuildGeoIndicators.computeRSUIndicators()
            if (!computeRSUIndicators.execute([datasource       : datasource,
                                               buildingTable    : buildingIndicators,
                                               rsuTable         : relationRSU,
                                               vegetationTable  : vegetationTable,
                                               roadTable        : roadTable,
                                               hydrographicTable: hydrographicTable,
                                               indicatorUse     : indicatorUse,
                                               svfSimplified    : svfSimplified,
                                               prefixName       : prefixName])) {
                error "Cannot compute the RSU indicators"
                return null
            }
            info "All geoindicators have been computed"

            // If the LCZ indicators should be calculated, we only affect a LCZ class to each RSU
            if(indicatorUse.contains("LCZ")){
                def lczIndicNames = ["GEOM_AVG_HEIGHT_ROOF"             : "HEIGHT_OF_ROUGHNESS_ELEMENTS",
                                     "BUILDING_AREA_FRACTION"           : "BUILDING_SURFACE_FRACTION",
                                     "ASPECT_RATIO"                     : "ASPECT_RATIO",
                                     "GROUND_SKY_VIEW_FACTOR"           : "SKY_VIEW_FACTOR",
                                     "PERVIOUS_FRACTION"                : "PERVIOUS_SURFACE_FRACTION",
                                     "IMPERVIOUS_FRACTION"              : "IMPERVIOUS_SURFACE_FRACTION",
                                     "EFFECTIVE_TERRAIN_ROUGHNESS_CLASS": "TERRAIN_ROUGHNESS_CLASS"]

                // Get into an other table the ID, geometry column and the 7 indicators useful for LCZ classification
                datasource.execute "DROP TABLE IF EXISTS $lczIndicTable;" +
                        "CREATE TABLE $lczIndicTable AS SELECT $COLUMN_ID_RSU, $GEOMETRIC_COLUMN, " +
                        "${lczIndicNames.keySet().join(",")} FROM ${computeRSUIndicators.results.outputTableName}"

                // Rename the indicators in order to be consistent with the LCZ ones
                def queryReplaceNames = ""

                lczIndicNames.each { oldIndic, newIndic ->
                    queryReplaceNames += "ALTER TABLE $lczIndicTable ALTER COLUMN $oldIndic RENAME TO $newIndic;"
                }
                datasource.execute "$queryReplaceNames"

                // The classification algorithm is called
                def classifyLCZ = Geoindicators.TypologyClassification.identifyLczType()
                if(!classifyLCZ([rsuLczIndicators   : lczIndicTable,
                                 normalisationType  : "AVG",
                                 mapOfWeights       : mapOfWeights,
                                 prefixName         : prefixName,
                                 datasource         : datasource,
                                 prefixName         : prefixName])){
                    info "Cannot compute the LCZ classification."
                    return
                }
                rsuLcz = classifyLCZ.results.outputTableName
            }

            datasource.execute "DROP TABLE IF EXISTS $lczIndicTable;"


            return [outputTableBuildingIndicators: computeBuildingsIndicators.getResults().outputTableName,
                    outputTableBlockIndicators   : blockIndicators,
                    outputTableRsuIndicators     : computeRSUIndicators.getResults().outputTableName,
                    outputTableRsuLcz   : rsuLcz]
        }
    })
}
