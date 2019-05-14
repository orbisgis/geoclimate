package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.processmanager.ProcessMapper

@BaseScript PrepareData prepareData


/**
 *
 * @return
 */
public static ProcessMapper PrepareOSMMapper(){
    def initParametersAbstract = org.orbisgis.common.AbstractTablesInitialization.initParametersAbstract()
    def inputDataFormatting = org.orbisgis.common.InputDataFormatting.inputDataFormatting()

    def loadInitialData = org.orbisgis.osm.OSMGISLayers.loadInitialData()
    def prepareBuildings = org.orbisgis.osm.OSMGISLayers.prepareBuildings()
    def prepareRoads = org.orbisgis.osm.OSMGISLayers.prepareRoads()
    def prepareRails = org.orbisgis.osm.OSMGISLayers.prepareRails()
    def prepareVeget = org.orbisgis.osm.OSMGISLayers.prepareVeget()
    def prepareHydro = org.orbisgis.osm.OSMGISLayers.prepareHydro()

    def transformBuildings = org.orbisgis.osm.FormattingForAbstractModel.transformBuildings()
    def transformRoads = org.orbisgis.osm.FormattingForAbstractModel.transformRoads()
    def transformRails = org.orbisgis.osm.FormattingForAbstractModel.transformRails()
    def transformVeget = org.orbisgis.osm.FormattingForAbstractModel.transformVeget()
    def transformHydro = org.orbisgis.osm.FormattingForAbstractModel.transformHydro()


    ProcessMapper mapper = new ProcessMapper()
    // FROM loadInitialData ...
    // ... to prepareXXX
    mapper.link(outDatasource : loadInitialData, datasource : prepareBuildings)
    mapper.link(outDatasource : loadInitialData, datasource : prepareRoads)
    mapper.link(outDatasource : loadInitialData, datasource : prepareRails)
    mapper.link(outDatasource : loadInitialData, datasource : prepareVeget)
    mapper.link(outDatasource : loadInitialData, datasource : prepareHydro)

    // FROM loadInitialData ...
    // ... to transformXXX
    mapper.link(outDatasource : loadInitialData, datasource : transformBuildings)
    mapper.link(outDatasource : loadInitialData, datasource : transformRoads)
    mapper.link(outDatasource : loadInitialData, datasource : transformRails)
    mapper.link(outDatasource : loadInitialData, datasource : transformVeget)
    mapper.link(outDatasource : loadInitialData, datasource : transformHydro)

    // FROM loadInitialData ...
    // ... to dataFormatting
    mapper.link(outputZone : loadInitialData, inputZone : inputDataFormatting)
    mapper.link(outputZoneNeighbors : loadInitialData, inputZoneNeighbors : inputDataFormatting)
    mapper.link(outDatasource : loadInitialData, datasource : inputDataFormatting)

    // FROM loadInitialData ...
    // ... to initParametersAbstract
    mapper.link(outDatasource : loadInitialData, datasource : initParametersAbstract)

    // FROM loadInitialData ...
    // ... to inputDataFormatting
    mapper.link(outDatasource : loadInitialData, datasource : inputDataFormatting)
    mapper.link(outputZoneName : loadInitialData, inputZone : inputDataFormatting)
    mapper.link(outputZoneNeighborsName : loadInitialData, inputZoneNeighbors : inputDataFormatting)

    // FROM prepareXXX ...
    // ... to transformXXX
    mapper.link(buildingTableName : prepareBuildings, inputTableName : transformBuildings)
    mapper.link(roadTableName : prepareRoads, inputTableName : transformRoads)
    mapper.link(railTableName : prepareRails, inputTableName : transformRails)
    mapper.link(vegetTableName : prepareVeget, inputTableName : transformVeget)
    mapper.link(hydroTableName : prepareHydro, inputTableName : transformHydro)


    // FROM initParametersAbstract ...
    // ...to inputDataFormatting

    mapper.link(outputBuildingAbstractParameters : initParametersAbstract, buildingAbstractParameters : inputDataFormatting)
    mapper.link(outputRoadAbstractParameters : initParametersAbstract, roadAbstractParameters : inputDataFormatting)
    mapper.link(outputVegetAbstractParameters : initParametersAbstract, vegetAbstractParameters : inputDataFormatting)
    mapper.link(outputBuildingAbstractUseType : initParametersAbstract, buildingAbstractUseType : inputDataFormatting)
    mapper.link(outputRoadAbstractType : initParametersAbstract, roadAbstractType : inputDataFormatting)
    mapper.link(outputRailAbstractType : initParametersAbstract, railAbstractType : inputDataFormatting)
    mapper.link(outputVegetAbstractType : initParametersAbstract, vegetAbstractType : inputDataFormatting)

    // FROM transformXXX ...
    // ... to inputDataFormatting
    mapper.link(outputTableName : transformBuildings, inputBuilding : inputDataFormatting)
    mapper.link(outputTableName : transformRoads, inputRoad : inputDataFormatting)
    mapper.link(outputTableName : transformRails, inputRail : inputDataFormatting)
    mapper.link(outputTableName : transformVeget, inputVeget : inputDataFormatting)
    mapper.link(outputTableName : transformHydro, inputHydro : inputDataFormatting)

    return mapper
}

