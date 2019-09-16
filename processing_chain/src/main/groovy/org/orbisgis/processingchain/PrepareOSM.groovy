package org.orbisgis.processingchain

import groovy.transform.BaseScript
import org.orbisgis.PrepareData
import org.orbisgis.common.AbstractTablesInitialization
import org.orbisgis.common.InputDataFormatting
import org.orbisgis.datamanager.JdbcDataSource
import org.orbisgis.osm.FormattingForAbstractModel
import org.orbisgis.osm.OSMGISLayers
import org.orbisgis.processmanagerapi.IProcess

@BaseScript ProcessingChain processingChain


/**
 * This process chains a set of subprocesses to extract and transform the OSM data into
 * the geoclimate model
 *
 * @param datasource a connection to the database where the result files should be stored
 * @param osmTablesPrefix The prefix used for naming the 11 OSM tables build from the OSM file
 * @param placeName the name of the place to extract
 * @param distance The integer value to expand the envelope of zone
 * @return
 */
def prepareOSM() {
    return create({
        title "Extract and transform OSM data to the Geoclimate model"
        inputs datasource: JdbcDataSource,
                placeName: String,
                distance: 500,
                hLevMin: 3,
                hLevMax: 15,
                hThresholdLev2: 10
        outputs outputBuilding: String, outputRoad: String, outputRail: String, outputHydro: String, outputVeget: String, outputZone: String
        run { datasource, placeName, distance, hLevMin, hLevMax, hThresholdLev2 ->

            if (datasource == null) {
                error "Cannot access to the database to store the osm data"
                return
            }

            info "Building OSM GIS layers"
            IProcess process = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
            if (process.execute([
                    datasource: datasource,
                    placeName : placeName])) {


                info "OSM GIS layers created"

                Map res = process.getResults()

                def buildingTableName = res.buildingTableName
                def roadTableName = res.roadTableName
                def railTableName = res.railTableName
                def vegetationTableName = res.vegetationTableName
                def hydroTableName = res.hydroTableName
                def zoneTableName = res.zoneTableName
                def zoneEnvelopeTableName = res.zoneEnvelopeTableName

                info "Formating OSM GIS layers"
                if (buildingTableName != null) {
                    IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: buildingTableName])
                }
                if (roadTableName != null) {
                    IProcess format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: roadTableName])
                }

                if (railTableName != null) {
                    IProcess format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: railTableName])
                }

                if (vegetationTableName != null) {
                    IProcess format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: vegetationTableName])
                }

                if (hydroTableName != null) {
                    IProcess format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: hydroTableName])
                }

                info "OSM GIS layers formated"


                [outputBuilding: finalBuildings, outputRoad: finalRoads,
                 outputRail    : finalRails, outputHydro: finalHydro, outputVeget: finalVeget, outputZone: finalZone,
                 outputStats   : [finalOutputBuildingStatZone, finalOutputBuildingStatZoneBuff, finalOutputRoadStatZone,
                                  finalOutputRoadStatZoneBuff, finalOutputRailStatZone, finalOutputHydroStatZone, finalOutputHydroStatZoneExt,
                                  finalOutputVegetStatZone, finalOutputVegetStatZoneExt]]

            }
        }

    })

}

