import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from awsglue.transforms import *
from awsglue.gluetypes import *
import boto3
import json
import botocore
import os
import datetime
from typing import List, Dict

# Start GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Get Args
args = getResolvedOptions(sys.argv, ["data"])
print("Init Arguments",args)

# Setup Job Execute
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variables declaration 
data_interface = json.loads(args["data"])
bucket_raw = data_interface["bucket_raw"]
database_name_raw = data_interface["database_name_raw"]
bucket_artifacts = data_interface["bucket_artifacts"]
key_artifacts = data_interface["key_artifacts"]


def s3_read_file(bucket, key):
    s3 = boto3.client('s3')
    file = s3.get_object(Bucket=bucket, Key=key)
    file = file['Body'].read()
    filebody = file.decode('utf-8', errors='replace')
    return filebody 

# Get schema table
data_schema = s3_read_file(bucket_artifacts, key_artifacts)
data_schema = json.loads(data_schema)
print("schema table", data_schema)

def format_schema_type(column_type):
    if str.lower(column_type)== "string":
            return StringType()
    if str.lower(column_type)== "integer":
            return IntegerType()
    if str.lower(column_type)== "double":
            return DoubleType()
    return StringType()

def glue_spark_schema(data_schema):
    data_columns = data_schema["columns"]
    struct_type_lst = []
    
    for columns in data_columns:
        row_type = Field(columns["Name"],format_schema_type(columns["Type"]))
        struct_type_lst.append(row_type)
    return StructType(struct_type_lst)


def data_create_dynamicframe(glue_schema):
    bucket_landing = data_interface["bucket_landing"]
    key_landing_move = data_interface["key_landing_move"]

    connection_type = "s3"
    connection_options = {"paths": [ f"s3://{bucket_landing}/{key_landing_move}"]}
    format_options = {"withSchema": json.dumps(glue_schema.jsonValue()), "separator": data_schema["field.delim"]
            }

    glue_raw = glueContext.create_dynamic_frame.from_options(
                    connection_type = connection_type, 
                    connection_options = connection_options, 
                    format="csv", 
                    format_options = format_options, 
                    transformation_ctx="")

    return glue_raw

# Create Glue schema table 
print('Create Glue schema table ')
glue_schema = glue_spark_schema(data_schema)

print('Create Glue Data from landing-move ')
glue_raw = data_create_dynamicframe(glue_schema)


# Local variable
partitions_key = [ partition["Name"] for partition in data_schema["partitions_key"]]


def append_partitions(glue_raw):
    
    def AddDateCol_year(r):  
        r["pt_year"] = data_interface["pt_year"]
        return r 
    def AddDateCol_month(r):  
        r["pt_month"] = data_interface["pt_month"]
        return r 
    def AddDateCol_day(r):  
        r["pt_day"] = data_interface["pt_day"]
        return r 
    def AddDateCol_time(r):  
        r["pt_time"] = data_interface["pt_time"]
        return r 
    
    if "pt_year" in partitions_key:
        glue_raw = Map.apply(frame = glue_raw, f = AddDateCol_year)
    if "pt_month" in partitions_key:
        glue_raw = Map.apply(frame = glue_raw, f = AddDateCol_month)
    if "pt_day" in partitions_key:
        glue_raw = Map.apply(frame = glue_raw, f = AddDateCol_day)
    if "pt_time" in partitions_key:
        glue_raw = Map.apply(frame = glue_raw, f = AddDateCol_time)
    
    return glue_raw


def append_audit(glue_raw):
    def AddDateCol_process_code(r):  
        r["process_code"] = data_interface["uuid_file"]
        return r 
    def AddDateCol_uuid(r):  
        r["uuid_file"] = data_interface["process_code"]
        return r
    audit_key = [{"Name":"process_code", "Type":"string"},
            {"Name":"uuid_file", "Type":"string"}]
    data_schema["columns"] = data_schema["columns"]+audit_key

    glue_raw = Map.apply(frame = glue_raw, f = AddDateCol_process_code)
    glue_raw = Map.apply(frame = glue_raw, f = AddDateCol_uuid)
    
    return glue_raw

def data_sort_columns(glue_raw):
    column_total = [col["Name"] for col in data_schema["columns"]]
    partitions_key = [col["Name"] for col in data_schema["partitions_key"]] 
    column_total+= partitions_key
    return glue_raw.select_fields(column_total)

# Add auditoria columns 
print('Add auditoria columns ')
glue_raw = append_audit(glue_raw)

# Add paritions columns 
print('Add paritions column ')
glue_raw = append_partitions(glue_raw)

# Sort columns 
print('Sort columns ')
glue_raw = data_sort_columns(glue_raw)

# Declare Boto3.Glue utils
class Glueutils:
    
    def __init__(self, data_schema, data_interface, bucket_raw):
        self.glue = boto3.client('glue')
        self.data_schema = data_schema
        self.data_interface = data_interface
        self.database_key = "database"
        self.bucket_raw = bucket_raw
        
        self.GLUE_TABLE_FORMATS = {
            'csv': {
                'Input': 'org.apache.hadoop.mapred.TextInputFormat',
                'Output': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'Serde': {
                    'Lib': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                    'Params': {
                        'separatorChar': ','
                    }
                },
                'Prefix': '',
                'Key': lambda prefix, table: f"{self.database_key}/{prefix}/{table}",
                'Bucket': self.bucket_raw
            },
            'tsv': {
                'Input': 'org.apache.hadoop.mapred.TextInputFormat',
                'Output': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'Serde': {
                    'Lib': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Params': {
                        'field.delim': '\t'
                    }
                },
                'Prefix': '',
                'Key': lambda prefix, table: f"{self.database_key}/{prefix}/{table}",
                'Bucket': self.bucket_raw
            }
        }

        self.DEFAULT_PARTITION_KEYS = self.data_schema["partitions_key"]


    def table_spec(self, table_name: str, file_type: str, s3_path: str, columns: List[str], delimiter: Dict[str, dict]):
        """
        Returns a valid table spec for use with Glue CreateTable API
        """

        formats = self.GLUE_TABLE_FORMATS[file_type]

        return {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Compressed': True,
                'Location': s3_path,
                'InputFormat': formats['Input'],
                'OutputFormat': formats['Output'],
                'SerdeInfo': {
                    'SerializationLibrary': formats['Serde']['Lib'],
                    'Parameters': formats['Serde']['Params']
                }
            },
            'PartitionKeys': self.DEFAULT_PARTITION_KEYS,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'EXTERNAL': 'TRUE',
                'classification': file_type,
                'creationDate': datetime.datetime.utcnow().isoformat()
            }
        }

    def table_spec_parquet(self, table_name: str, s3_path: str, columns: List[str]):
        """
        Returns a valid table spec for use with Glue CreateTable API
        """
        return {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Compressed': True,
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                    'Parameters':{
                        'serialization.format': '1'
                    }
                },
            'Parameters': {
                'classification': 'parquet',
                'compressionType': 'none'
                }
            },
            'PartitionKeys': self.DEFAULT_PARTITION_KEYS,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'EXTERNAL': 'TRUE',
                'classification': 'parquet',
                'creationDate': datetime.datetime.utcnow().isoformat()
            }
        }
    
    def table_exists(self, database: str, name: str) -> bool:
        """
        Check if given table exists in the Glue catalog
        """
        resp = self.glue.get_tables(DatabaseName=database, Expression=f".*{name}.*")
        return len(resp['TableList']) > 0


    def create_table(self, system: str, interface: str, file_type: str, database_name: str):
        """
        Create table for given system and interface, if it exists
        Return name and warehouse path info for the table
        """
        source = self.data_interface["source"]
        interface = self.data_interface["interface"]
        table_name = self.data_schema["table_name"]
        table_name = f"{interface}_{table_name}"
        
        formats = self.GLUE_TABLE_FORMATS[file_type]
        # table_name = f"{formats['Prefix']}{system}_{interface}"
        bucket = formats['Bucket']
        key = f"{self.database_key}/{source}/{interface}/{table_name}"
        s3_path = f"s3://{bucket}/{key}"

        if self.table_exists(database_name, table_name):
            print('tabla ya existe')
            return table_name, bucket, key

        # prefix_schema = 'work/schemas'
        bucket_schema = bucket
        interface_schema = interface


        columns = self.data_schema["columns"]
        delimiter = self.data_schema["field.delim"]
        # revisar table_spec, para que se envie el formato correcto de parquet

        table = self.table_spec_parquet(table_name, s3_path, columns)

        try:
            self.glue.create_table(DatabaseName=database_name, TableInput=table)
        except self.glue.exceptions.AlreadyExistsException:
            print(f"{table_name} ya existe")

        return table_name, bucket, key


    def get_glue_table(self, database_name: str, table_name: str):
        try:
            response = self.glue.get_table(DatabaseName=database_name, Name=table_name)

            print(response)
            return response['Table']
        except glue.exceptions.EntityNotFoundException:
            raise ValueError(f'Error:   {database_name}.{table_name} no existe')
            return None


    def create_partition(self, database_name, table_name, new_partition):
        try:
            print('lm: agregar nueva particion: ', end='')
            response = self.glue.create_partition(DatabaseName=database_name,
                                             TableName=table_name,
                                             PartitionInput=new_partition)
            print('Okey particion agregada:')
        except glue.exceptions.AlreadyExistsException as e:
            print('lm:excepcion Particion ya existente', e)
            

            
# Create Glue catalog table
print('Create Glue catalog table')
GlueObj = Glueutils(data_schema, data_interface, bucket_raw)
table_name, table_bucket, table_key = GlueObj.create_table(system=data_interface["source"],
                                interface= data_interface["interface"],
                                file_type= data_schema["field.type"],
                                database_name= database_name_raw)

raw_path_file = f"s3://{table_bucket}/{table_key}"
print(raw_path_file)

# Local partitions variables
pt_year = data_interface['pt_year']
pt_month =data_interface['pt_month']
pt_day =data_interface['pt_day']
pt_time =data_interface['pt_time']

partition = f"pt_year={pt_year}/pt_month={pt_month}/pt_day={pt_day}/pt_time={pt_time}"

print(f"{raw_path_file}/{partition}")

# Purge partitions repositorie
print('Purge partitions repositorie')
glueContext.purge_s3_path(f"{raw_path_file}/{partition}", options={"retentionPeriod":0})

# Create data with partitions
print('Create data with partitions')
glueContext.write_dynamic_frame.from_catalog(
        frame=glue_raw,
        database = database_name_raw, 
        table_name = table_name, 
        transformation_ctx = database_name_raw, 
        additional_options={"partitionKeys": partitions_key})


# Create Partition in Data Catalog
print('Create Partition in Data Catalog')
table = GlueObj.get_glue_table(database_name_raw, table_name)

print('Config new partition')
new_partition = {
    'Values': [pt_year, pt_month, pt_day, pt_time],
    'StorageDescriptor': {
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'Parameters':{
                'serialization.format': '1'
            }
        },
        'Columns': data_schema["columns"],
        'Location': f"{raw_path_file}/{partition}"
    }
}

print('Add new partitions ')
response = GlueObj.create_partition(database_name =database_name_raw,
                                      table_name= table_name,
                                      new_partition=new_partition)

print('Finish ')

job.commit()
