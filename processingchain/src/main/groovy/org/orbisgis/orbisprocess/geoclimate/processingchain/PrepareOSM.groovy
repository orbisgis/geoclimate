package org.orbisgis.orbisprocess.geoclimate.processingchain

import groovy.transform.BaseScript
import org.orbisgis.orbisprocess.geoclimate.preparedata.PrepareData
import org.orbisgis.orbisdata.datamanager.jdbc.JdbcDataSource
import org.orbisgis.orbisdata.processmanager.api.IProcess

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
IProcess buildGeoclimateLayers() {
    return create({
        title "Extract and transform OSM data to the Geoclimate model"
        inputs datasource: JdbcDataSource,
                placeName: String,
                distance: 500,
                hLevMin: 3,
                hLevMax: 15,
                hThresholdLev2: 10
        outputs outputBuilding: String, outputRoad: String, outputRail: String,
                outputHydro: String, outputVeget: String, outputImpervious: String, outputZone: String, outputZoneEnvelope: String
        run { datasource, placeName, distance, hLevMin, hLevMax, hThresholdLev2 ->

            if (datasource == null) {
                error "Cannot access to the database to store the osm data"
                return
            }

            info "Building OSM GIS layers"
            IProcess process = PrepareData.OSMGISLayers.extractAndCreateGISLayers()
            if (process.execute([datasource: datasource, placeName : placeName,
                    distance: distance])) {

                info "OSM GIS layers created"

                Map res = process.getResults()

                def buildingTableName = res.buildingTableName
                def roadTableName = res.roadTableName
                def railTableName = res.railTableName
                def vegetationTableName = res.vegetationTableName
                def hydroTableName = res.hydroTableName
                def imperviousTableName = res.imperviousTableName
                def zoneTableName = res.zoneTableName
                def zoneEnvelopeTableName = res.zoneEnvelopeTableName
                def epsg = res.epsg
                if(zoneTableName!=null) {
                    info "Formating OSM GIS layers"
                    IProcess format = PrepareData.FormattingForAbstractModel.formatBuildingLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: buildingTableName,
                            inputZoneEnvelopeTableName : zoneEnvelopeTableName,
                            epsg:epsg])
                    buildingTableName = format.results.outputTableName

                    format = PrepareData.FormattingForAbstractModel.formatRoadLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: roadTableName,
                            inputZoneEnvelopeTableName : zoneEnvelopeTableName,
                            epsg:epsg])
                    roadTableName = format.results.outputTableName


                    format = PrepareData.FormattingForAbstractModel.formatRailsLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: railTableName,
                            inputZoneEnvelopeTableName : zoneEnvelopeTableName,
                            epsg:epsg])
                    railTableName = format.results.outputTableName

                    format = PrepareData.FormattingForAbstractModel.formatVegetationLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: vegetationTableName,
                            inputZoneEnvelopeTableName : zoneEnvelopeTableName,
                            epsg:epsg])
                    vegetationTableName = format.results.outputTableName

                    format = PrepareData.FormattingForAbstractModel.formatHydroLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: hydroTableName,
                            inputZoneEnvelopeTableName : zoneEnvelopeTableName,
                            epsg:epsg])
                    hydroTableName = format.results.outputTableName

                    format = PrepareData.FormattingForAbstractModel.formatImperviousLayer()
                    format.execute([
                            datasource    : datasource,
                            inputTableName: imperviousTableName,
                            inputZoneEnvelopeTableName : zoneEnvelopeTableName,
                            epsg:epsg])
                    imperviousTableName = format.results.outputTableName

                    info "OSM GIS layers formated"

                }

                [outputBuilding: buildingTableName, outputRoad: roadTableName,
                 outputRail    : railTableName, outputHydro: hydroTableName,
                 outputVeget: vegetationTableName, outputImpervious: imperviousTableName,
                 outputZone: zoneTableName,outputZoneEnvelope:zoneEnvelopeTableName]

            }
        }

    })

}

