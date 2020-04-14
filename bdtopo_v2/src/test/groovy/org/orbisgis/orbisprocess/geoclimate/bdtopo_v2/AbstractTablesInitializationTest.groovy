package org.orbisgis.orbisprocess.geoclimate.bdtopo_v2

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.DisabledIfSystemProperty
import org.orbisgis.orbisdata.datamanager.jdbc.h2gis.H2GIS
import org.orbisgis.orbisprocess.geoclimate.preparedata.BDTopo_v2

import static org.junit.jupiter.api.Assertions.*

class AbstractTablesInitializationTest {

    @Test
    @DisabledIfSystemProperty(named = "data.bd.topo", matches = "false")
    void initParametersAbstract(){
        H2GIS h2GISDatabase = H2GIS.open("./target/h2gis_abstract_tables_${UUID.randomUUID()};AUTO_SERVER=TRUE", "sa", "")
        def process = BDTopo_v2.AbstractTablesInitialization.initParametersAbstract()
        assertTrue process.execute([datasource: h2GISDatabase])
        process.getResults().each {entry ->
            assertNotNull h2GISDatabase.getTable(entry.getValue())
        }

        // Check if the BUILDING_ABSTRACT_USE_TYPE table has the correct number of columns and rows
        def tableName = process.getResults().outputBuildingAbstractUseType
        assertNotNull(tableName)
        def table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(36, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the BUILDING_ABSTRACT_PARAMETERS table has the correct number of columns and rows
        tableName = process.getResults().outputBuildingAbstractParameters
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(36, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('INTEGER', table.columnType('NB_LEV'))
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.NB_LEV)
            assertNotEquals('', row.NB_LEV)
        }

        // Check if the ROAD_ABSTRACT_TYPE table has the correct number of columns and rows
        tableName = process.getResults().outputRoadAbstractType
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(16, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the ROAD_ABSTRACT_SURFACE table has the correct number of columns and rows
        tableName = process.getResults().outputRoadAbstractSurface
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(14, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the ROAD_ABSTRACT_PARAMETERS table has the correct number of columns and rows
        tableName = process.getResults().outputRoadAbstractParameters
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(16, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('INTEGER', table.columnType('MIN_WIDTH'))
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.MIN_WIDTH)
            assertNotEquals('', row.MIN_WIDTH)
        }

        // Check if the ROAD_ABSTRACT_CROSSING table has the correct number of columns and rows
        tableName = process.getResults().outputRoadAbstractCrossing
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(3, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_CROSSING'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        table.eachRow { row ->
            assertNotNull(row.ID_CROSSING)
            assertNotEquals('', row.ID_CROSSING)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the RAIL_ABSTRACT_TYPE table has the correct number of columns and rows
        tableName = process.getResults().outputRailAbstractType
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(7, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the RAIL_ABSTRACT_CROSSING table has the correct number of columns and rows
        tableName = process.getResults().outputRailAbstractCrossing
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(3, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_CROSSING'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        table.eachRow { row ->
            assertNotNull(row.ID_CROSSING)
            assertNotEquals('', row.ID_CROSSING)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the VEGET_ABSTRACT_TYPE table has the correct number of columns and rows
        tableName = process.getResults().outputVegetAbstractType
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(4, table.columnCount)
        assertEquals(13, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('DEFINITION'))
        assertEquals('VARCHAR', table.columnType('SOURCE'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.DEFINITION)
            assertNotEquals('', row.DEFINITION)
            assertNotNull(row.SOURCE)
        }

        // Check if the VEGET_ABSTRACT_PARAMETERS table has the correct number of columns and rows
        tableName = process.getResults().outputVegetAbstractParameters
        assertNotNull(tableName)
        table = h2GISDatabase.getTable(tableName)
        assertNotNull(table)
        assertEquals(3, table.columnCount)
        assertEquals(13, table.rowCount)
        // Check if the column types are correct
        assertEquals('INTEGER', table.columnType('ID_TYPE'))
        assertEquals('VARCHAR', table.columnType('TERM'))
        assertEquals('VARCHAR', table.columnType('HEIGHT_CLASS'))
        // For each rows, check if the fields contains null or empty values
        table.eachRow { row ->
            assertNotNull(row.ID_TYPE)
            assertNotEquals('', row.ID_TYPE)
            assertNotNull(row.TERM)
            assertNotEquals('', row.TERM)
            assertNotNull(row.HEIGHT_CLASS)
            assertNotEquals('', row.HEIGHT_CLASS)
        }
    }
}