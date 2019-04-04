
package org.orbisgis.common

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.processmanagerapi.IProcess

import javax.lang.model.element.NestingKind

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
 * @return outputRailAbstractType The name of the table in which the abstract rail's types stored
 * @return outputVegetAbstractType The name of the table in which the abstract vegetation's types are stored
 * @return outputVegetAbstractParameters The name of the table in which the abstract vegetation's parameters are stored
 */

static IProcess initParametersAbstract(){
    return processFactory.create(
            'Initialize the abstract and parameter tables',
            [datasource: JdbcDataSource],
            [outputBuildingAbstractUseType: String, outputBuildingAbstractParameters: String,
             outputRoadAbstractType: String, outputRoadAbstractSurface: String, outputRoadAbstractParameters: String,
             outputRailAbstractType: String, outputVegetAbstractType: String, outputVegetAbstractParameters: String
            ],
            {JdbcDataSource datasource ->
                logger.info('Executing the parametersAndAbstractTables.sql script')
                def uuid = UUID.randomUUID().toString().replaceAll('-', '_')
                def buildingAbstractUseType = 'BUILDING_ABSTRACT_USE_TYPE_' + uuid
                def buildingAbstractParam = 'BUILDING_ABSTRACT_PARAMETERS_' + uuid
                def roadAbstractType = 'ROAD_ABSTRACT_TYPE_' + uuid
                def roadAbstractSurface = 'ROAD_ABSTRACT_SURFACE_' + uuid
                def roadAbstractParam = 'ROAD_ABSTRACT_PARAMETERS_' + uuid
                def railAbstractType = 'RAIL_ABSTRACT_TYPE_' + uuid
                def vegetAbstractType = 'VEGET_ABSTRACT_TYPE_' + uuid
                def vegetAbstractParam = 'VEGET_ABSTRACT_PARAMETERS_' + uuid

                datasource.executeScript(this.class.getResource('parametersAndAbstractTables.sql').toString(),
                        [BUILDING_ABSTRACT_USE_TYPE: buildingAbstractUseType,
                         BUILDING_ABSTRACT_PARAMETERS: buildingAbstractParam,
                         ROAD_ABSTRACT_TYPE: roadAbstractType,
                         ROAD_ABSTRACT_SURFACE: roadAbstractSurface,
                         ROAD_ABSTRACT_PARAMETERS: roadAbstractParam,
                         RAIL_ABSTRACT_TYPE: railAbstractType,
                         VEGET_ABSTRACT_TYPE: vegetAbstractType,
                         VEGET_ABSTRACT_PARAMETERS: vegetAbstractParam
                        ])

                logger.info('The parametersAndAbstractTables.sql script has been executed')

                [outputBuildingAbstractUseType: 'BUILDING_ABSTRACT_USE_TYPE',
                 outputBuildingAbstractParameters: 'BUILDING_ABSTRACT_PARAMETERS',
                 outputRoadAbstractType: 'ROAD_ABSTRACT_TYPE', outputRoadAbstractSurface: 'ROAD_ABSTRACT_SURFACE',
                 outputRoadAbstractParameters: 'ROAD_ABSTRACT_PARAMETERS',
                 outputRailAbstractType: 'RAIL_ABSTRACT_TYPE', outputVegetAbstractType: 'VEGET_ABSTRACT_TYPE',
                 outputVegetAbstractParameters: 'VEGET_ABSTRACT_PARAMETERS'
                ]
            }
    )
}
