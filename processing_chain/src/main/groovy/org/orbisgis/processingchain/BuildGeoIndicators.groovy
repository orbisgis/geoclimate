package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

@BaseScript ProcessingChain processingChain

/**
 * Compute the geoindicators at building scale
 *
 * @return
 */
public static IProcess computeBuildingsIndicators(){
    processFactory.create("Compute the geoindicators at building scale", [datasource: JdbcDataSource,
                                                                          inputBuildingTableName: String,
                                                                          inputRoadTableName : String,
                                                                          saveResults : boolean],
            [outputTableName: String], { datasource, buildingTableName, inputRoadTableName, saveResults ->
                //TODO change
                String buildingPrefixName = "test"

                IProcess computeGeometryProperties =  org.orbisgis.GenericIndicators.geometryProperties()
                computeGeometryProperties.execute([inputTableName: inputBuildingTableName,inputFields:String["id_build", "the_geom"],operations:["st_length","st_perimeter","st_area"]
                                                   , prefixName: buildingPrefixName, datasource: datasource])

                def buildTableGeometryProperties =  computeGeometryProperties.results.outputTableName

                IProcess computeSizeProperties = org.orbisgis.BuildingIndicators.sizeProperties()
                computeSizeProperties.execute([inputBuildingTableName: inputBuildingTableName,
                                               operations:["building_volume", "building_floor_area", "building_total_facade_length"]
                                               , prefixName: buildingPrefixName, datasource: datasource])

                def buildTableSizeProperties = computeSizeProperties.getResults.outputTableName


                IProcess computeFormProperties =  org.orbisgis.BuildingIndicators.formProperties()
                computeFormProperties.execute([inputBuildingTableName: inputBuildingTableName,operations: ["building_concavity","building_form_factor",
                                               "building_raw_compacity", "building_convexhull_perimeter_density"]
                                               , prefixName: buildingPrefixName, datasource: datasource])

                def buildTableFormProperties =     computeFormProperties.results.outputTableName


                IProcess computeNeighborsProperties =  org.orbisgis.BuildingIndicators.neighborsProperties()
                computeNeighborsProperties.execute([inputBuildingTableName: inputBuildingTableName, operations: ["building_contiguity","building_common_wall_fraction",
                                                    "building_number_building_neighbor"]
                                                    , prefixName: buildingPrefixName, datasource: datasource])
                def buildTableComputeNeighborsProperties = computeNeighborsProperties.results.outputTableName

                IProcess computeMinimumBuildingSpacing =  org.orbisgis.BuildingIndicators.minimumBuildingSpacing()
                computeMinimumBuildingSpacing.execute([inputBuildingTableName: inputBuildingTableName,bufferDist: 100
                                                       , prefixName: buildingPrefixName, datasource: datasource])

                def buildTableComputeMinimumBuildingSpacing =  computeMinimumBuildingSpacing.results.outputTableName

                IProcess computeRoadDistance = org.orbisgis.BuildingIndicators.roadDistance()
                computeRoadDistance.execute([inputBuildingTableName: inputBuildingTableName, inputRoadTableName: inputRoadTableName, bufferDist: 100
                                             , prefixName: buildingPrefixName, datasource: datasource])

                def buildTableComputeRoadDistance = computeRoadDistance.results.outputTableName


                IProcess computeLikelihoodLargeBuilding =   org.orbisgis.BuildingIndicators.likelihoodLargeBuilding()
                computeLikelihoodLargeBuilding.execute([inputBuildingTableName: inputBuildingTableName, nbOfBuildNeighbors: buildTableComputeNeighborsProperties, prefixName: buildingPrefixName, datasource: datasource])

                def buildTableComputeLikelihoodLargeBuilding = computeLikelihoodLargeBuilding.results.outputTableName


                IProcess buildingTableJoin = org.orbisgis.DataUtils.joinTables()
                buildingTableJoin.execute([inputTableNamesWithId: [buildTableGeometryProperties : id_build,buildTableSizeProperties:id_build,buildTableFormProperties:id_build,
                                                             buildTableComputeNeighborsProperties:id_build,buildTableComputeMinimumBuildingSpacing:id_build,buildTableComputeRoadDistance:id_build,
                                                             buildTableComputeLikelihoodLargeBuilding:id_build]
                                           , prefixName: buildingPrefixName, datasource: datasource])

                [outputTableName:buildingTableJoin.results.outputTableName]

    })

}