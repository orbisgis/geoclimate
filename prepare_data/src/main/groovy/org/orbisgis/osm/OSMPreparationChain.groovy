package org.orbisgis.osm

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.processmanager.ProcessMapper

@BaseScript PrepareData prepareData


/**
 *
 * @return
 */
public static ProcessMapper OSMPreparationMapper(){
    def loadInitialData = OSMGISLayers.loadInitialData()
    def prepareBuildings = OSMGISLayers.prepareBuildings()
    def prepareRoads = OSMGISLayers.prepareRoads()
    def prepareRails = OSMGISLayers.prepareRails()
    def prepareVeget = OSMGISLayers.prepareVeget()
    def prepareHydro = OSMGISLayers.prepareHydro()

    def transformBuildings = FormattingForAbstractModel.transformBuildings()
    def transformRoads = FormattingForAbstractModel.transformRoads()
    def transformRails = FormattingForAbstractModel.transformRails()
    def transformVeget = FormattingForAbstractModel.transformVeget()
    def transformHydro = FormattingForAbstractModel.transformHydro()


    ProcessMapper mapper = new ProcessMapper()
    mapper.link(outDatasource : loadInitialData, datasource : prepareBuildings)
    mapper.link(outDatasource : loadInitialData, datasource : prepareRoads)
    mapper.link(outDatasource : loadInitialData, datasource : prepareRails)
    mapper.link(outDatasource : loadInitialData, datasource : prepareVeget)
    mapper.link(outDatasource : loadInitialData, datasource : prepareHydro)
    mapper.link(buildingTableName : prepareBuildings, inputTableName : transformBuildings)
    mapper.link(outDatasource : loadInitialData, datasource : transformBuildings)
    mapper.link(roadTableName : prepareRoads, inputTableName : transformRoads)
    mapper.link(outDatasource : loadInitialData, datasource : transformRoads)
    mapper.link(railTableName : prepareRails, inputTableName : transformRails)
    mapper.link(outDatasource : loadInitialData, datasource : transformRails)
    mapper.link(vegetTableName : prepareVeget, inputTableName : transformVeget)
    mapper.link(outDatasource : loadInitialData, datasource : transformVeget)
    mapper.link(hydroTableName : prepareHydro, inputTableName : transformHydro)
    mapper.link(outDatasource : loadInitialData, datasource : transformHydro)


    return mapper
}

