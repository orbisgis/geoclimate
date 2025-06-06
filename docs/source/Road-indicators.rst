Road Indicators
===============

GeoClimate offers a processing chain to compute road traffic indicators based on the table Tool 2.5 (WG-AEN method) described in the `Good Practice Guide for Strategic Noise Mapping and the Production of Associated Data on Noise Exposure Version 2 (13th January 2006) <http://sicaweb.cedex.es/docs/documentacion/Good-Practice-Guide-for-Strategic-Noise-Mapping.pdf>`_.

The chain uses the GeoClimate road layer and a configuration file that contains information to establish relations between the WG-AEN referential and the road layer features:

- a mapping from WG-AEN road types to the road layer types (*WG type*)
- the CNOSSOS-EU pavement codes according to the surface values available in the road layer (*WG pavement*)
- the Tool 2.5 flow data by WG-AEN road types for the 3 periods: day, night, and evening (*WG data flow*)
- the maximum speed value according to the WG-AEN road types (*WG maxspeed*)

.. image:: https://raw.githubusercontent.com/orbisgis/geoclimate/master/docs/resources/images/geoclimate_processing_road_traffic.png
   :alt: GeoClimate road traffic processing

*Figure 3. Processing steps to compute a road traffic flow based on WG-AEN referential*

For each road geometry, the road layer generic fields ``type``, ``surface``, ``oneway``, and ``maxspeed`` are mapped to the WG type, pavement, and direction as defined in the WG-AEN referential. These values are then intersected with the WG data flow to compute the number of light and heavy vehicles per hour for the 3 time periods:

- **Day** (06:00–18:00)
- **Evening (ev)** (18:00–22:00)
- **Night** (22:00–06:00)

The 16 resulting indicators are stored in the ``road_indicators`` table:

- The WG-AEN road type
- The pavement code
- The direction of the road section:

  - 1 = one-way road section, traffic flows in the same direction as the slope definition
  - 2 = one-way road section, traffic flows in the opposite direction to the slope definition
  - 3 = bidirectional traffic flow (split between uphill and downhill)

- The number of light vehicles per hour for **day**
- The number of heavy vehicles per hour for **day**
- The light vehicle speed for **day**
- The heavy vehicle speed for **day**
- The number of light vehicles per hour for **night**
- The number of heavy vehicles per hour for **night**
- The light vehicle speed for **night**
- The heavy vehicle speed for **night**
- The number of light vehicles per hour for **evening**
- The number of heavy vehicles per hour for **evening**
- The light vehicle speed for **evening**
- The heavy vehicle speed for **evening**
- The slope (in %) of the road section

.. include:: _Footer.rst
