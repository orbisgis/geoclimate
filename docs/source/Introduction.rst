Introduction
============

**Local climate** is affected by key factors such as the type of land surface (vegetation, water, building, impervious, etc.) or the size, the shape, the use and the distribution of the buildings. Thus it is necessary to describe accurately the land fabric in order to better understand climate processes.

Geoclimate computes geospatial indicators which can be currently used:

- for modeling purpose: to create the input data needed by parametric urban climate models such as Town Energy Balance (`TEB <https://github.com/teb-model/teb>`_),
- for planning purpose: to qualify urban tissues according to **climate-related classifications** such as the *urban typology* presented in `Bocher et al. (2017) <https://hal.archives-ouvertes.fr/hal-01730717v2>`_ or the Local Climate Zones (LCZ).

Geoclimate performs indicator computation at three spatial unit scales, a spatial unit being a POLYGON or MULTIPOLYGON geometry:

1. **building** scale, defined as a collection of features that represent 3D objects with walls and a roof,
2. **block** scale, defined as a set of buildings touching each other (at least one point in common) or as an isolated building,
3. Reference Spatial Unit (**RSU**) scale, being the elementary unit to characterize all the characteristics of a piece of land (not only related to buildings but also to vegetation, water, etc.). It can be defined in different ways:
   
   - Topographical Unit (TU): it is a continuous and homogeneous way to divide the space using topographic constraints: road and railway center lines, vegetation, impervious and water areas, administrative boundaries.

Overall, more than **100 urban indicators** are currently calculated (e.g. compactness and road distance at building scale, volume at block scale, building fraction, mean sky view factor at RSU scale, ...). Note that Geoclimate has first been developed for climate studies but many indicators could also be useful to analyze landscape ecology, land use, habitat conservation planning or any environmental or territory applications.

.. include:: _Footer.rst
