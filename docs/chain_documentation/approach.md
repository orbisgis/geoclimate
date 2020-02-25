# Approach

The Geoclimate library is a collection of spatial processes (algorithms) that allows to compute indicators as form factor, densities, 
sky view factor... based on vector GIS layers. 
Each indicator is computed and collected for a specific spatial unit. 
Indicators made part of statistical classifications to categorize, extract shared properties or organization for a studied area. 

The vector GIS layers used by the algorithms must feed an input data model. 
This model is organized around a set of thematic layers: building, road, rail, hydrography, vegetation. 
Each layer follows a dictionary of values that define the name and the type of columns and their constraints 
(min, max value, allowed string value to describe a feature ...). 
The input data model guarantees the structure of the data sent to the Geoclimate processes while the input data formatting is performed to correct input values that would overpass the defined constraints. 

- [Input data model](./chain_documentation/input_data_model.md)
- [Spatial units](./chain_documentation/spatial_units/spatial_units.md)
- [Indicators](./chain_documentation/indicators/indicators.md)
- [Classifications](./chain_documentation/classsifications/classifications.md)  





