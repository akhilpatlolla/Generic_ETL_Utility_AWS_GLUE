import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql.functions import *
from py4j.java_gateway import java_import

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


## @params: [JOB_NAME, URL, ACCOUNT, WAREHOUSE, DB, SCHEMA, USERNAME, PASSWORD]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'URL', 'ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD','ROLE'])

sc = SparkContext()
glueContext = GlueContext(sc)
sqlContext = SQLContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
java_import(spark._jvm, "net.snowflake.spark.snowflake") 

spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
"sfURL" : args['URL'],
"sfAccount" : args['ACCOUNT'],
"sfUser" : args['USERNAME'],
"sfPassword" : args['PASSWORD'],
"sfDatabase" : args['DB'],
"sfSchema" : args['SCHEMA'],
"sfWarehouse" : args['WAREHOUSE'],
"sfRole" : args['ROLE']
}

def getLastUpdated_timestamp(targetTable, timestamp_column):
    """
    Calls snoowflake connection and gets last updated timestamp for table.
    
    parameters
        @targetTable:       string Table in Snowflake.
        @timestamp_column:  string Timestamp coloumn name
    rerutn 
        timestamp timestamp 
    """
    df = sqlContext.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option('query','select max({time}) as {time} from {table}'.format(table = targetTable, time = timestamp_column)).load()
    return df.collect()[0][timestamp_column]
    
def write_to_s3(dynamic_dataFrame, bucket, key, table_name):
    """
    Writes the dataframe to s3 bucket
    
    parameters
        @dataframe: dataframe   python spark dataframe after all the ETL operations
        @bucket:    string      destination bucket name
        @key:       string      destination sub folder if any.
        @table_name:string      Table Name
    return
        @save_time string 
    """ 
    print('writing dataframe to file')
    save_time = '{}/{}/{}'.format(table_name,
                                    str(datetime.now().date()),
                                    table_name+'_'+str(datetime.now().time()).split('.')[0].replace(":","_"))
    uploaded_dataframe = glueContext.write_dynamic_frame.from_options(frame = dynamic_dataFrame, connection_type = "s3", connection_options = {"path": 's3://{}/{}/{}'.format(bucket,key,save_time) }, format = "csv")
    return save_time

def deleteRecords(target_table, timestamp_column):
    """
    deletes the records in snowflake table.
    
    parameters
        @target_table:      string 
        @timestamp_column:  string 
    returns 
        None
    """
    Delete_Query = \
        'DELETE from {table_name} where {time_stamp} in (select max({time_stamp}) as {time_stamp} from {table_name})'.format(
            time_stamp = timestamp_column,
            table_name = target_table )
    try:
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions , Delete_Query)
        print('Deleted entries successfully ')
    except :
        print('Error executing the delete query')









