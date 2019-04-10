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
    sys.argv, ['DB1', 'DB2', 'JOB_TYPE'])
session = boto3.Session(region_name='us-east-1')
cloudwatch = session.client('cloudwatch')
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
    Name=args['SNOWFLAKE'], WithDecryption=True)['Parameter']['Value'])

obj = {
    'source_table': {
        'ssmKey': args['DB1'],
        'target_table': 'target_table_name',
        'primary_key': 'pk',
        'timestamp_column': 'timestamp_column',
        'truncate': True|False,
        'operations': {'columns_to_omit': 'CSV Values'}
    }
}

class ConnectionConfig:
    bucket = 'aws-glue-scripts-270063804057-us-east-1'
    def __init__(self, ssmKey):
        res = json.loads(ssm.get_parameter(
                Name=ssmKey, WithDecryption=True)['Parameter']['Value'])
        self.connection_type = res['type']
        url = 'jdbc:'+res['type']+'://'+res['host'] + \
            ':'+str(res['port'])+'/'+res['database']
        self.properties = {'user': res['user'], 'password': res['password'],'url':url}
        self.snowflake_stage = '@public.ETL_STAGE/'+res['database']
        self.key = '{}/etl/{}'.format(args['JOB_TYPE'], res['database'])

class Table(ConnectionConfig):
    def __init__(self,source,destination,primary_key, ssmKey,timestamp_column='',truncate=False,operations={}):
        ConnectionConfig.__init__(self, ssmKey)
        self.table_name = source
        self.destination = destination
        self.primary_key=primary_key
        self.primary_key = primary_key
        self.timestamp_column = timestamp_column
        self.ops = type("ops", (object,), operations)
        self.truncate = truncate
    def build_dynamic_dataframe(self):
        print('>>>Table : {}<<<'.format(self.table_name))
        print('Creating Dynamic Data Frame')
        connection_string = self.properties
        connection_string['dbtable'] = self.table_name
        self.dynamic_dataFrame = glueContext.create_dynamic_frame_from_options(
            self.connection_type, 
            connection_options=connection_string, 
            format=None, format_options={}, 
            transformation_ctx="")
        print('Dataframe Schema :')
        self.dynamic_dataFrame.printSchema()
    def perform_etl(self):
        print('in function')
        for i in list(filter(lambda x:not x.startswith('__'), self.ops.__dict__.keys())):
            print('Inside loop')
            if(hasattr(self,i)):getattr(self,i)(self.ops.__dict__[i])
            print('After loop')
    def columns_to_omit(self,parameters):
            print('Dropping Fields :', parameters)
            self.dynamic_dataFrame = dynamic_dataFrame.drop_fields(
                parameters.split(','))
    def create_list_columns(self):
        spark_df = self.dynamic_dataFrame.toDF()
        dataframe_types = dict((f.name, f.dataType)
                            for f in spark_df.schema.fields)
        self.list_columns = ', '.join(spark_df.columns).upper()
        #     sf_create(dataframe_types)
        # def sf_create(self,dataframe_types):
        column_list = ['{} {}'.format(i, get_mapping(
            str(dataframe_types[i]).split('(')[0])) for i in dataframe_types.keys()]
        column_list.append('merge_ts timestamp_ltz')
        CREATE_TABLE_QUERY = 'CREATE TABLE IF NOT EXISTS  {} ( {} )'.format(
            self.destination, ', '.join(column_list)).upper()
        spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            sfOptions, CREATE_TABLE_QUERY)
    def filter_incremental(self):
        if len(self.timestamp_column) > 0:
            deleteRecords(self.destination, self.timestamp_column)
            latestmodified = getMaxValue(self.destination, self.timestamp_column)
            if (latestmodified is not None):
                print('Last modified Date Condition is ',
                    latestmodified.isoformat())
                local = self.dynamic_dataFrame.toDF()
                self.dynamic_dataFrame = \
                    DynamicFrame.fromDF(local.filter(local[self.timestamp_column]
                                                    >= latestmodified), glueContext, 'dynamic_dataFrame')
        elif (len(self.primary_key.split(',')) == 1):
            max_id = getMaxValue(self.destination,  self.primary_key)
            print('max id : ', max_id)
            if (max_id is not None):
                local = self.dynamic_dataFrame.toDF()
                self.dynamic_dataFrame = \
                    DynamicFrame.fromDF(local.filter(local[self.primary_key]
                                                    > int(max_id)), glueContext, 'dynamic_dataFrame')

        elif (not self.truncate):
            # exception.append((Exception('Multiple Primary Keys passed or no Timestamp column')))
            print('Multiple Primary Keys passed or no Timestamp column')
 
# added the etl utility.
def etlUtility(obj):
    global_metrics = []
    cumulative_metric = {}
    exception = []
    source_tables =  [Table(
        table, obj[table]['target_table'], obj[table]['primary_key'], obj[table]['ssmKey'],
        obj[table]['timestamp_column'], obj[table]['truncate'], obj[table]['operations'],
        ) for table in obj]

    for x in source_tables:
        local_metrics = {}
        x.build_dynamic_dataframe()

        local_metrics['source'] = x.dynamic_dataFrame.count()
        print('dynamic Frame count :', local_metrics['source'])

        x.perform_etl()
        x.create_list_columns()
        if(x.truncate):truncateTable(x.destination)
        x.filter_incremental()
        local_metrics['incremental_pull_count'] = x.dynamic_dataFrame.count()
        print('Uploading to S3, Updated data fields with {} rows '.format(
            local_metrics['incremental_pull_count']))
        s3_path = write_to_s3(x)
        print('Uploaded to s3 at', s3_path)
        snowflakeMerge(x, s3_path)
        local_metrics['destination'] = getSnowflake_count(x.destination)
        local_metrics['difference'] = local_metrics['source'] - local_metrics['destination']

        print('Snowflake merge completed. pushed {} rows.'.format(
            local_metrics['incremental_pull_count']))

        for i in local_metrics.keys():
            if i in cumulative_metric.keys():
                cumulative_metric[i] += local_metrics[i]
            else:
                cumulative_metric[i] = local_metrics[i]

        global_metrics.append([m.getMetric() for m in [Metric(
            key, local_metrics[key], Dimension(x.destination)) for key in local_metrics.keys()]])
        print('Published metric to cloudwatch for {} table.'.format(x.table_name))
    
    
    for metric in global_metrics:
        cloudwatch.put_metric_data(MetricData=metric, Namespace='WAREHOUSE')

    # for e in exception:
    #     if None not in e:
    #         raise(e[1])

    cloudwatch.put_metric_data(MetricData=[m.getMetric() for m in [Metric(
        key, cumulative_metric[key],
        Dimension('TOTAL')) for key in cumulative_metric.keys()]],
        Namespace='WAREHOUSE')

    print('Published Cumulative Metric. Job Successfully Ended.')
 
def snowflakeMerge(table, s3_path):
    """
    Merge the staged changes to snowflake.
    parameters
        @table_name:      string    table name
        @target_table:    string    target table name in snowflake
        @snowflake_stage: string    stage value
        @P_KEY:           string    primary key value
        @list_columns:    list      list of columns in target table
        @s3_path:          time      upload time
    return
        None
    """
    # merge case for stage
    stage = ''.join(['${} as {}, '.format(index+1, i)
                     for index, i in enumerate(table.list_columns.split(','))]).strip(', ')

    # merge case for update case
    update = ''.join(table.list_columns.split()).split(',')
    for pk in ''.join(table.primary_key.split()).split(','):
        update.remove(pk)
    update_columns = ''.join(['{0} = temp.{0}, '.format(i)
                              for i in update]).strip(', ')
    # merge case for primary key
    check_condition = ' and '.join([' {t}.{v} = temp.{v} '.format(
        t=table.destination, v=i) for i in table.primary_key.split(',')])
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
    """.format(stage=table.snowflake_stage,
               M_time_path=s3_path,
               stage_table=table.table_name,
               TABLE=table.destination,
               stage_columns=stage,
               check=check_condition,
               update_condition=update_columns,
               columns=table.list_columns,
               temp_list='temp.'+table.list_columns.replace(',', ',temp.'),
               date=str(datetime.now().date()),
               timestamp=table.table_name+'_'+str(datetime.now().time()).split('.')[0].replace(":", "_"))
    spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        sfOptions, MERGE_QUERY)
def write_to_s3(table):
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
    save_time = '{}/{}/{}'.format(table.table_name,
                                  str(datetime.now().date()),
                                  table.table_name+'_'+str(datetime.now().time()).split('.')[0].replace(":", "_"))
    uploaded_dataframe = glueContext.write_dynamic_frame.from_options(
        frame=table.dynamic_dataFrame, 
        connection_type="s3", 
        connection_options=
        {"path": 's3://{}/{}/{}'.format(table.bucket, table.key, save_time)}, 
        format="csv")
    return save_time
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
class Dimension:
    def __init__(self, Value, Name='Glue '+args['JOB_TYPE']):
        self.Name = Name
        self.Value = Value

    def getName(self):
        return self.Name

    def getValue(self):
        return self.Value

    def getDimension(self):
        return dict({'Name': self.Name, 'Value': self.Value})
class Metric(Dimension):
    def __init__(self, MetricName, Value, Dimensions, Unit='Count'):
        self.MetricName = MetricName
        self.Unit = Unit
        self.Value = Value
        self.Dimensions = Dimensions

    def getMetric(self):
        return dict({
            'MetricName': self.MetricName,
            'Dimensions': [self.Dimensions.getDimension()],
            'Unit': self.Unit,
            'Value': self.Value
        })
def getSnowflake_count(targetTable):
    """
    Calls snowflake connection and gets counts for table.

    parameters
        @targetTable:       string Table in Snowflake.
    return 
        integer number of rows
    """
    df = sqlContext.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option(
        'query', 'select count(*) as count from {table}'.format(table=targetTable)).load()
    return df.collect()[0]['COUNT']
def getMaxValue(targetTable, column):
    """
    Calls snowflake connection and gets last updated timestamp for table.

    parameters
        @targetTable:       string Table in Snowflake.
        @column:  string Timestamp column name
    return 
        timestamp timestamp 
    """
    df = sqlContext.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option(
        'query', 'select max({c}) as {c} from {table}'.format(table=targetTable, c=column)).load()
    return df.collect()[0][column]
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
    spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        sfOptions, 'truncate {}'.format(target_table))
etlUtility(obj)
job.commit()
