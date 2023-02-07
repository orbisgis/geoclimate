package org.orbisgis.geoclimate.bdtopo

/**
 * Main module for BDTopo processing tasks
 */
class BDTopo extends BDTopoUtils{

    public static InputDataLoading = new InputDataLoading()
    public static InputDataFormatting = new InputDataFormatting()


    /**
     * BDTopo workflow processing chain.
     *
     * The parameters of the processing chain is defined
     * from a configuration file or a Map.
     * The configuration file is stored in a json format
     *
     * @param input The path of the configuration file or a Map
     *
     * The input file or the Map supports the following entries *
     *
     * {
     * [OPTIONAL ENTRY] "description" :"A description for the configuration file"
     *
     * [OPTIONAL ENTRY] "geoclimatedb" : { // Local H2GIS database used to run the processes
     *                                    // A default db is build when this entry is not specified
     *       "folder" : "/tmp/", //The folder to store the database
     *       "name" : "geoclimate_db;AUTO_SERVER=TRUE" // A name for the database
     *       "delete" :false
     *     },
     * [ONE ENTRY REQUIRED]   "input" : {
     *         "folder": "path of the folder that contains the BD Topo layers as shapefile",
     *             "locations":["56260"] //list of insee code, with a comma separator
     *             "srid :  2154}, //SRID to force the projection
     *          "database": {
     *             "user": "-",
     *             "password": "-",
     *             "url": "jdbc:postgresql://", //JDBC url to connect with the database
     *             //List of BDTOPO tables required to compute the geoclimate indicators
     *             "tables": {
     *                 "commune":"commune",
     *                 "bati_indifferencie":"ign_bdtopo_2017.bati_indifferencie",
     *                 "bati_industriel":"ign_bdtopo_2017.bati_industriel",
     *                 "bati_remarquable":"ign_bdtopo_2017.bati_remarquable",
     *                 "route":"ign_bdtopo_2017.route",
     *                 "troncon_voie_ferree":"ign_bdtopo_2017.troncon_voie_ferree",
     *                 "surface_eau":"ign_bdtopo_2017.surface_eau",
     *                 "zone_vegetation":"ign_bdtopo_2017.zone_vegetation",
     *                 "terrain_sport":"ign_bdtopo_2017.terrain_sport",
     *                 "construction_surfacique":"ign_bdtopo_2017.construction_surfacique",
     *                 "surface_route":"ign_bdtopo_2017.surface_route",
     *                 "surface_activite":"ign_bdtopo_2017.surface_activite"
     *                 "population":"insee.population"} }
     *             }
     *             ,
     *  [OPTIONAL ENTRY]  "output" :{ //If not output is set the results are keep in the local database
     *             "folder" : "/tmp/myResultFolder" //tmp folder to store the computed layers in a geojson format,
     *             "database": { //database parameters to store the computed layers
     *                  "user": "-",
     *                  "password": "-",
     *                  "url": "jdbc:postgresql://", //JDBC url to connect with the database
     *                  "tables": { //table names to store the result layers. Create the table if it doesn't exist
     *                      "building_indicators":"building_indicators",
     *                      "block_indicators":"block_indicators",
     *                      "rsu_indicators":"rsu_indicators",
     *                      "rsu_lcz":"rsu_lcz",
     *                      "zones":"zones"} }
     *     },
     *     ,
     *   [OPTIONAL ENTRY]  "parameters":
     *     {"distance" : 1000,
     *     "prefixName": "",
     *          rsu_indicators:{
     *         "indicatorUse": ["LCZ", "UTRF", "TEB"],
     *         "svfSimplified": false, *
     *         "mapOfWeights":
     *         {"sky_view_factor"                : 4,
     *          "aspect_ratio"                   : 3,
     *          "building_surface_fraction"      : 8,
     *          "impervious_surface_fraction"    : 0,
     *          "pervious_surface_fraction"      : 0,
     *           "height_of_roughness_elements"   : 6,
     *           "terrain_roughness_length"       : 0.5},
     *         "hLevMin": 3,
     *         "hLevMax": 15,
     *         "hThresho2": 10
     *         }
     *     }
     *     }
     * The parameters entry tag contains all geoclimate chain parameters.
     * When a parameter is not specificied a default value is set.
     *
     * - distance The integer value to expand the envelope of zone when recovering the data
     * (some objects may be badly truncated if they are not within the envelope)
     * - indicatorUse List of geoindicator types to compute (default ["LCZ", "UTRF", "TEB"]
     *                  --> "LCZ" : compute the indicators needed for the LCZ classification (Stewart et Oke, 2012)
     *                  --> "UTRF" : compute the indicators needed for the urban typology classification (Bocher et al., 2017)
     *                  --> "TEB" : compute the indicators needed for the Town Energy Balance model
     * - svfSimplified A boolean indicating whether or not the simplified version of the SVF should be used. This
     * version is faster since it is based on a simple relationship between ground SVF calculated at RSU scale and
     * facade density (Bernard et al. 2018).
     * - prefixName A prefix used to name the output table (default ""). Could be useful in case the user wants to
     * investigate the sensibility of the chain to some input parameters
     * - mapOfWeights Values that will be used to increase or decrease the weight of an indicator (which are the key
     * of the map) for the LCZ classification step (default : all values to 1)
     * - hLevMin Minimum building level height
     *
     * @return
     * a map with the name of zone and a list of the output tables computed and stored in the local database,
     * otherwise throw an error.
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
    static Map workflow(Map args) {
        if (args.version) {
            if (args.version == 2) {
                BDTopoV2Workflow bdtopo_v2_workflow = new BDTopoV2Workflow()
                return bdtopo_v2_workflow.execute(input: args.input)
            } else if (args.version == 3) {
                BDTopoV3Workflow bdTopoV3Workflow = new BDTopoV3Workflow()
                return bdTopoV3Workflow.execute(input: args.input)
            } else {
                error "Unsupported version. Set number 2 or 3"
                return null
            }
        }
    }

    static Map v2(Map args) {
        BDTopoV2Workflow bdtopo_v2_workflow = new BDTopoV2Workflow()
        return bdtopo_v2_workflow.execute(input: args.input)
    }

    static Map v3(Map args) {
        BDTopoV3Workflow bdtopo_v3_workflow = new BDTopoV3Workflow()
        return bdtopo_v3_workflow.execute(input: args.input)
    }

}
