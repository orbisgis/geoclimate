## Changelog for v1.3.0


- Force bbox geometry coordinates to float
- Fix Sonatype publishing
- Improve OSMTools : automatically retry on 429, 502, 503, 504 and network errors, with an increasing delay (configurable via OVERPASS_MAX_ATTEMPTS).
- Grid indicators : add the `angle` parameter (degrees, counter-clockwise) to compute a rotated grid, based on the H2GIS `ST_MakeGrid` rotation. A rotated grid can only be saved in `fgb`
- Grid indicators : `rowCol` is now `false` by default, `x_size` and `y_size` are the cell size in meters (#1131)
- Grid indicators : the TARGET checks (square cells, size in meters) are only applied when TARGET is requested, the `asc` output requires square and not rotated cells
- Grid indicators : the urban sprawl distance and the TARGET resolution use the real cell size of the grid
- Fix the `lcz_lod` range check
