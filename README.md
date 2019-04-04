# geoclimate
Repository to share tools and scripts for geoclimate projects

### Good practices

 - The temporary tables should respect the pattern : `tableName_UUID` with `-` replaced by `_` if needed.
 - Index should be create using the Postgresql syntax : `CREATE INDEX IF EXISTS indexName ON table(columnName) USING RTREE`.