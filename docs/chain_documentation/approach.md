# Approach

The Geoclimate library is a collection of spatial processes (algorithms) that allows to compute indicators such as form factor, densities, sky view factor... based on vector GIS layers. 

Computed for a specific spatial unit, each indicators are then used in statistical classifications to categorize and extract shared properties or organizations for a studied area.

To do so, the vector GIS layers used by the algorithms must feed an input data model. This model is organized around a set of thematic layers (building, road, railway, hydrography, vegetation)  which must follow a dictionary of values defining the name and type of columns as well as their constraints (min / max value, allowed string value to describe a feature, ...). 

The input data model guarantees the structure of the data sent to the Geoclimate processes while the input data formatting is performed to correct input values that would overpass the defined constraints. 

This approach can be summarized through the main bricks illustrated in the diagram below.



![approach](../resources/images/chain_documentation/approach.png)



Remark: The process has been made so that the steps follow one another, but it is also possible to use the bricks *à la carte* according to the data already available to the user and his needs.



To go in deep with these aspects, please visit the following pages:

- [Input data model](./input_data_model.md)
- [Spatial units](./spatial_units/spatial_units.md)
- [Indicators](./indicators/indicators.md)
- [Classifications](./classsifications/classifications.md)





