## Incremental Script

Generic ETL Utility to pull data from any data source to Snowflake or any Target Source.

### docs:
https://akhilpatlolla.github.io/Generic_ETL_Utility_AWS_GLUE/

#### Notes
1. You need you have all the respective JDBC connection drives in s3 and add those jar's to dependency path.
2. Add connection and other required permission in order to access JDBC connection 
3. SSM parameter config object <br>
     abcdefghijk = {
       "type":"mysql|redshift|oracle|sqlserver|postgresql",<br>
       "user":"username",<br>
       "multipleStatements":true,<br>
       "password":"********",<br>
       "port":5432|3306,<br>
       "host":"p360-prod.mlprivate.net",<br>
       "database":"program360"
     }<br>
4. Command like parameter for the job  
    --db1 = abcdefghijk

