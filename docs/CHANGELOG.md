## Changelog for v1.3.0


- Force bbox geometry coordinates to float
- Fix Sonatype publishing
- Improve OSMTools : automatically retry on 429, 502, 503, 504 and network errors, with an increasing delay (configurable via OVERPASS_MAX_ATTEMPTS).
- Change rowColl to be false when not specified
- Add a new parameter, angle to workflow of grid indicators
