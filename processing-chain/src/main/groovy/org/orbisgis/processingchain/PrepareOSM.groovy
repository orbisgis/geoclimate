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

