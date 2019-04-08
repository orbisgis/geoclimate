# geoclimate
Repository to share tools and scripts for geoclimate projects

### Good practices

 - The temporary tables should respect the pattern : `tableName_UUID` with `-` replaced by `_` if needed.
 - Index should be create using the Postgresql syntax : `CREATE INDEX IF EXISTS indexName ON table(columnName) USING RTREE`.
 - The processes should be documented with a description of the process followed by the inputs with `@param` and then the outputs with `@return`. As example :
    ``` java
    /**
    * Description of my process
    * @param inputName Input description
    * @param inputName Input description
    * @return outputName Output description
    */
    ```