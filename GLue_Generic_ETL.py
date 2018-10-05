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
    deletes the records in snowflake table to most recent records and backfill all the transactions created.
    
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

def truncateTable(target_table):
    """
    Truncates the table

    parameters
        @target_table:      string
    returns 
        None
    """
    try:
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions , 'truncate {}'.format(target_table))
        print('{} truncated sucessfully'.format(target_table))
    except:
        print('Error Truncating the Table {}'.format(target_table))

def ETL_Utility(obj):
    for table_name in obj:
        connection_type     = obj[table_name]['connection_type']
        target_table        = obj[table_name]['target_table']
        timestamp_coloumn   = obj[table_name]['timestamp_coloumn']
        coloumns_to_delete  = obj[table_name]['coloumns_to_delete']
        bucket              = obj[table_name]['bucket']
        key                 = obj[table_name]['key']
        url                 = obj[table_name]['url']
        properties          = {'user':obj[table_name]['user'], 'password': obj[table_name]['password']}
        primary_key         = obj[table_name]['primary_key']
        list_columns        = obj[table_name]['list_columns']
        snowflake_stage     = obj[table_name]['stage']
        
        try:
            ETL_operations      = list(obj[table_name]['operations'].keys())
        except AttributeError:
            ETL_operations      = []    

        print('Creating Dynamic Data Frame')
        connection_string = {"url": url, "user":properties["user"], "password": properties["password"],"dbtable": table_name } 
        dynamic_dataFrame = glueContext.create_dynamic_frame_from_options(connection_type, connection_options=connection_string, format=None, format_options={}, transformation_ctx="")
        print('Dynamic Schema :')
        dynamic_dataFrame.printSchema()
        print('dynamic Frame count :',dynamic_dataFrame.count())
        print('Dropping Fields :', coloumns_to_delete )
        dynamic_dataFrame = dynamic_dataFrame.drop_fields(coloumns_to_delete.split(','))
        print('Dynamic schema after Dropping Fields ')

        for operation in ETL_operations:
            if operation is 'int_to_bool':
                dataFrame = int_to_bool(dataFrame, obj[table_name]['operations']['int_to_bool'])
            if operation is 'truncate':
                truncateTable(obj[table_name]['operations']['truncate'])

        deleteRecords(target_table, timestamp_coloumn)   
        dynamic_dataFrame.printSchema()
        latestmodified = getLastUpdated_timestamp(target_table,timestamp_coloumn)
        print("Last Updated Time : ",latestmodified)   
        Date_Condition = ''

        if latestmodified is not None:
            Date_Condition = "where {time_stamp} >= '{last_date}'".format(time_stamp=timestamp_coloumn, last_date=latestmodified.isoformat())
            print("Date Condition is ", Date_Condition)
            dynamic_dataFrame = Filter.apply(frame = dynamic_dataFrame, f = lambda x: x[timestamp_coloumn] >= latestmodified )
            # local = dynamic_dataFrame.toDF()
            # local = local.filter(local[timestamp_coloumn] >= latestmodified)
            # print("After the filter count is ",local.count() )
            dynamic_dataFrame = DynamicFrame.fromDF(local, glueContext, "dynamic_dataFrame")
            print("final conversion dynamic frame count is ",dynamic_dataFrame.count() )

                                
        print('Uploading to S3, Updated data fields with {} rows '.format(dynamic_dataFrame.count()))
        M_time = write_to_s3(dynamic_dataFrame, bucket, key, table_name)
        print('Uploaded to s3 at',M_time)
        snowflakeMerge(table_name, target_table,Date_Condition,snowflake_stage,primary_key,list_columns, M_time, coloumns_to_delete)


obj = {
        @@template@@
        # 'source_table':{
        #     'connection_type':'mysql',
        #     'url':'jdbc:mysql://host:3306/schema',
        #     'user':'username',
        #     'password':'password',
        #     'target_table':'target_table',
        #     'primary_key':'pk',
        #     'list_columns':'pk, timestamp column, other columns',
        #     'bucket':'s3_bucket_name',
        #     'key':'path_to_dump',
        #     'stage':'@STAGE_LOCATION_SNOWFLAKE',
        #     'timestamp_coloumn':'timestamp_column',
        #     'coloumns_to_delete':'string seperated by "," ',
        #     'operations':{'ETL_Operation':'Value','truncate':'target_table_name'}
        # }
}

ETL_Utility(obj)

job.commit()