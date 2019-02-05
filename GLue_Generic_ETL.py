import sys
import boto3
import json
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


args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'JOB_TYPE', 'DB1', 'DB2'])
session = boto3.Session(region_name='us-east-1')
ssm = session.client('ssm')
sc = SparkContext()
glueContext = GlueContext(sc)
sqlContext = SQLContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
java_import(spark._jvm, "net.snowflake.spark.snowflake")
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(
    spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = json.loads(ssm.get_parameter(
    Name=args['DB2'], WithDecryption=True)['Parameter']['Value'])

obj = {
    'source_table': {
        'ssmKey': args['DB1'],
        'target_table': 'target_table_name',
        'primary_key': 'pk',
        'timestamp_column': 'timestamp_column',
        'columns_to_omit': 'string seperated by "," ',
        'operations': {'ETL_Operation': 'Value', 'truncate': 'target_table_name'}
    }
}


# added the etl utility.
def etlUtility(obj):
    for table_name in obj:
        res = json.loads(ssm.get_parameter(
            Name=obj[table_name]['ssmKey'], WithDecryption=True)['Parameter']['Value'])
        connection_type = res['type']
        target_table = obj[table_name]['target_table']
        timestamp_column = obj[table_name]['timestamp_column']
        columns_to_omit = obj[table_name]['columns_to_omit']
        url = 'jdbc:'+res['type']+'://'+res['host'] + \
            ':'+str(res['port'])+'/'+res['database']
        properties = {'user': res['user'], 'password': res['password']}
        primary_key = obj[table_name]['primary_key']
        # s3 Config
        bucket = 'S3 BUCKET'
        s3_stage = '@STAGE_CONFIG'+res['database']
        key = '{}/s3_path_directories/{}'.format(
            args['JOB_TYPE'], res['database'])

        try:
            ETL_operations = list(obj[table_name]['operations'].keys())
        except AttributeError:
            ETL_operations = []

        print('====>{}<===='.format(table_name))
        print('Creating Dynamic Data Frame')
        connection_string = {
            "url": url, "user": properties["user"], "password": properties["password"], "dbtable": table_name}
        dynamic_dataFrame = glueContext.create_dynamic_frame_from_options(
            connection_type, connection_options=connection_string, format=None, format_options={}, transformation_ctx="")
        print('Dynamic Schema :')
        dynamic_dataFrame.printSchema()
        print('dynamic Frame count :', dynamic_dataFrame.count())
        print('Dropping Fields :', columns_to_omit)
        dynamic_dataFrame = dynamic_dataFrame.drop_fields(
            columns_to_omit.split(','))
        print('Dynamic schema after Dropping Fields ')

        list_columns = get_list_columns_create(dynamic_dataFrame, target_table)

        for operation in ETL_operations:
            if operation is 'int_to_bool':
                dataFrame = int_to_bool(
                    dataFrame, obj[table_name]['operations']['int_to_bool'])
            if operation is 'truncate':
                truncateTable(obj[table_name]['operations']['truncate'])

        deleteRecords(target_table, timestamp_column)
        dynamic_dataFrame.printSchema()

        latestmodified = getLastUpdated_timestamp(
            target_table, timestamp_column)
        print("Last Updated Time : ", latestmodified)

        if latestmodified is not None:
            print("Last modified Date Condition is ",
                  latestmodified.isoformat())
            local = dynamic_dataFrame.toDF()
            local = local.filter(local[timestamp_column] >= latestmodified)
            print("After the filter count is ", local.count())
            dynamic_dataFrame = DynamicFrame.fromDF(
                local, glueContext, "dynamic_dataFrame")
            print("final conversion dynamic frame count is ",
                  dynamic_dataFrame.count())

        print('Uploading to S3, Updated data fields with {} rows '.format(
            dynamic_dataFrame.count()))
        M_time = write_to_s3(dynamic_dataFrame, bucket, key, table_name)
        print('Uploaded to s3 at', M_time)
        snowflakeMerge(table_name, target_table, s3_stage,
                       primary_key, list_columns, M_time)
        #push_to_cloudwatch_metrics()

def get_mapping(spark_datatype):
    """
        mapping spark data points to the snowflake datatypes.
    parameters:
        @spark_datatype:        string spark dataframe datatype
    return:
        associated snowflake datatype for the data in spark dataframe. 
    """
    spark_mapping = {
        'number': ['IntegerType', 'LongType', 'ShortType'],
        'double': ['FloatType', 'DecimalType', 'DoubleType'],
        'string': ['StringType'],
        'boolean': ['BooleanType'],
        'date': ['DateType'],
        'timestamp_ntz': ['TimestampType']
    }
    return list(spark_mapping.keys())[[True if spark_datatype in i else False for i in list(spark_mapping.values())].index(True)]


def get_list_columns_create(dynamic_dataFrame, target_table):
    """
    parameters
        @dynamic_dataFrame:     string Glue Dynamic dataframe 
        @target_table:          string Table in Snowflake.
    return
        string of columns 
    """
    dataFrame = dynamic_dataFrame.toDF()
    dataframe_types = dict((f.name, f.dataType)
                           for f in dataFrame.schema.fields)
    column_list = ['{} {}'.format(i, get_mapping(
        str(dataframe_types[i]).split('(')[0])) for i in dataframe_types.keys()]
    column_list.append('merge_ts timestamp_ltz')
    CREATE_TABLE_QUERY = 'CREATE TABLE IF NOT EXISTS  {} ( {} )'.format(
        target_table, ', '.join(column_list)).upper()
    spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        sfOptions, CREATE_TABLE_QUERY)
    return ', '.join(dataFrame.columns).upper()


def getLastUpdated_timestamp(targetTable, timestamp_column):
    """
    Calls snowflake connection and gets last updated timestamp for table.

    parameters
        @targetTable:       string Table in Snowflake.
        @timestamp_column:  string Timestamp column name
    return 
        timestamp timestamp 
    """
    df = sqlContext.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option(
        'query', 'select max({time}) as {time} from {table}'.format(table=targetTable, time=timestamp_column)).load()
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
                                  table_name+'_'+str(datetime.now().time()).split('.')[0].replace(":", "_"))
    uploaded_dataframe = glueContext.write_dynamic_frame.from_options(frame=dynamic_dataFrame, connection_type="s3", connection_options={
                                                                      "path": 's3://{}/{}/{}'.format(bucket, key, save_time)}, format="csv")
    return save_time


def snowflakeMerge(table_name, target_table, s3_stage, P_KEY, list_columns, M_time):
    """
    Merge the staged changes to snowflake.

    parameters
        @target_table:    string target table name in snowflake
        @s3_Stage:       string stage value
        @P_KEY:          string primary key value
        @list_columns:   list   list of columns in target table
    return
        None
    """
#     merge case for stage
    stage = ''.join(['${} as {}, '.format(index+1, i)
                     for index, i in enumerate(list_columns.split(','))]).strip(', ')

#     merge case for update case
    update = ''.join(list_columns.split()).split(',')
    for pk in ''.join(P_KEY.split()).split(','):
        update.remove(pk)
    update_columns = ''.join(['{0} = temp.{0}, '.format(i)
                              for i in update]).strip(', ')
#     merge case for primary key
    check_condition = ' and '.join([' {t}.{v} = temp.{v} '.format(
        t=target_table, v=i) for i in P_KEY.split(',')])
    MERGE_QUERY = """
    MERGE INTO {TABLE} USING
    (select 
        {stage_columns} from  {stage}/{M_time_path}  ( file_format => 'DB_PRD.PUBLIC.CSV_WITH_HEADER')
    ) temp ON {check} 
        WHEN MATCHED THEN UPDATE SET 
            {update_condition}, MERGE_TS = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN 
        INSERT
            ( {columns}, MERGE_TS) 
        VALUES
           ( {temp_list}, CURRENT_TIMESTAMP);
    """.format(stage=s3_stage,
               M_time_path=M_time,
               stage_table=table_name,
               TABLE=target_table,
               stage_columns=stage,
               check=check_condition,
               update_condition=update_columns,
               columns=list_columns,
               temp_list='temp.'+list_columns.replace(',', ',temp.'),
               date=str(datetime.now().date()),
               timestamp=table_name+'_'+str(datetime.now().time()).split('.')[0].replace(":", "_"))
    try:
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sfOptions, MERGE_QUERY)
    except:
        print('Error executing the merge statement')


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
            time_stamp=timestamp_column,
            table_name=target_table)
    try:
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sfOptions, Delete_Query)
        print('Deleted entries successfully ')
    except:
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
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sfOptions, 'TRUNCATE {}'.format(target_table))
    except:
        print('Error executing the truncate query')


etlUtility(obj)
job.commit()
