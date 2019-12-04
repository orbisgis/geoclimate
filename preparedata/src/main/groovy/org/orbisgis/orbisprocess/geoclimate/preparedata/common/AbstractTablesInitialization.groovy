
package org.orbisgis.orbisprocess.geoclimate.preparedata.common

import groovy.transform.BaseScript
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess

@BaseScript PrepareData prepareData

/**
 * This process initialize the abstract tables in which the objects type and parameters (for buildings, roads, rails
 * hydrographic areas and vegetation areas) are stored
 *
 * @param datasource A connexion to a database (H2GIS, PostGIS, ...), in which the data to process will be stored
 *
 * @return outputBuildingAbstractUseType The name of the table in which the abstract building's use and type are stored
 * @return outputBuildingAbstractParameters The name of the table in which the abstract building's parameters are stored
 * @return outputRoadAbstractType The name of the table in which the abstract road's types are stored
 * @return outputRoadAbstractSurface The name of the table in which the abstract road's surfaces are stored
 * @return outputRoadAbstractParameters The name of the table in which the abstract road's parameters are stored
 * @return outputRoadAbstractCrossing The name of the table in which the abstract road's crossing are stored
 * @return outputRailAbstractType The name of the table in which the abstract rail's types stored
 * @return outputRailAbstractCrossing The name of the table in which the abstract rail's crossing are stored
 * @return outputVegetAbstractType The name of the table in which the abstract vegetation's types are stored
 * @return outputVegetAbstractParameters The name of the table in which the abstract vegetation's parameters are stored
 */

IProcess initParametersAbstract(){
    return create({
        title 'Initialize the abstract and parameter tables'
        inputs datasource: JdbcDataSource
        outputs outputBuildingAbstractUseType: String, outputBuildingAbstractParameters: String,
                outputRoadAbstractType: String, outputRoadAbstractSurface: String, outputRoadAbstractParameters: String,
                outputRoadAbstractCrossing: String, outputRailAbstractType: String, outputRailAbstractCrossing: String,
                outputVegetAbstractType: String, outputVegetAbstractParameters: String
        run { JdbcDataSource datasource ->
            logger.info('Executing the parametersAndAbstractTables.sql script')
            def buildingAbstractUseType = 'BUILDING_ABSTRACT_USE_TYPE'
            def buildingAbstractParam = 'BUILDING_ABSTRACT_PARAMETERS'
            def roadAbstractType = 'ROAD_ABSTRACT_TYPE'
            def roadAbstractSurface = 'ROAD_ABSTRACT_SURFACE'
            def roadAbstractParam = 'ROAD_ABSTRACT_PARAMETERS'
            def roadAbstractCrossing = 'ROAD_ABSTRACT_CROSSING'
            def railAbstractType = 'RAIL_ABSTRACT_TYPE'
            def railAbstractCrossing = 'RAIL_ABSTRACT_CROSSING'
            def vegetAbstractType = 'VEGET_ABSTRACT_TYPE'
            def vegetAbstractParam = 'VEGET_ABSTRACT_PARAMETERS'

            datasource.executeScript(getClass().getResourceAsStream('parametersAndAbstractTables.sql'),
                    [BUILDING_ABSTRACT_USE_TYPE  : buildingAbstractUseType,
                     BUILDING_ABSTRACT_PARAMETERS: buildingAbstractParam,
                     ROAD_ABSTRACT_TYPE          : roadAbstractType,
                     ROAD_ABSTRACT_SURFACE       : roadAbstractSurface,
                     ROAD_ABSTRACT_PARAMETERS    : roadAbstractParam,
                     ROAD_ABSTRACT_CROSSING      : roadAbstractCrossing,
                     RAIL_ABSTRACT_TYPE          : railAbstractType,
                     RAIL_ABSTRACT_CROSSING      : railAbstractCrossing,
                     VEGET_ABSTRACT_TYPE         : vegetAbstractType,
                     VEGET_ABSTRACT_PARAMETERS   : vegetAbstractParam
                    ])

            logger.info('The parametersAndAbstractTables.sql script has been executed')

            [outputBuildingAbstractUseType   : buildingAbstractUseType,
             outputBuildingAbstractParameters: buildingAbstractParam,
             outputRoadAbstractType          : roadAbstractType,
             outputRoadAbstractSurface       : roadAbstractSurface,
             outputRoadAbstractParameters    : roadAbstractParam,
             outputRoadAbstractCrossing      : roadAbstractCrossing,
             outputRailAbstractType          : railAbstractType,
             outputRailAbstractCrossing      : railAbstractCrossing,
             outputVegetAbstractType         : vegetAbstractType,
             outputVegetAbstractParameters   : vegetAbstractParam
            ]
        }
    })
}
