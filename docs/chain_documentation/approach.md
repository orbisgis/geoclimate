# Approach

The Geoclimate library is a collection of spatial processes (algorithms) that allows to compute indicators as form factor, densities, 
sky view factor... based on vector GIS layers.
Each indicator is computed for a specific spatial unit. 
A spatial unit corresponds to geometry area (POLYGON or MULTIPOLYGON). It qualifies 

The indicators are collected .. faire classification

The vector GIS layers used by the algorithms must feed an input data model. 
This model is organized around a set of thematic layers: building, road, rail, hydrography, vegetation. 
Each layer follows a dictionary of values that define the name and the type of columns and their constraints 
(min, max value, allowed string value to describe a feature ...). 
The input abstract model guarantees the structure of the data sent to the Geoclimate process while the input data formatting is performed to correct input values that would overpass the defined constraints. 

- [Input data model](./chain_documentation/input_data_model.md)
- [Spatial units](./chain_documentation/spatial_units/spatial_units.md)
- [Indicators](./chain_documentation/indicators/indicators.md)
- [Classifications](./chain_documentation/classsifications/classifications.md)  





