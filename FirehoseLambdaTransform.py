'''
This script will handle the auto partitioning of S3 folder space to be conducive for Athena.
With this, we will not have to create multiple copies of the same data or copy s3 files to the new folder.
'''

from __future__ import print_function

import base64
import re
import datetime
from datetime import datetime
import boto3
import os
import sys

curr_region  = os.environ['AWS_DEFAULT_REGION'] #gets the current region of the execution

firehose_client  = boto3.client('firehose', curr_region)
glue_client = boto3.client('glue', curr_region)

partition_spec = {
  "StorageDescriptor": {},
  "Values": []
}

def describe_stream(delivery_stream_arn):
    '''function to get the firehose stream configuration'''
    
    print("Starting to retrieve configuration for delivery stream arn : {}".format(delivery_stream_arn))
    
    stream_configuration = {}
    try:
        stream_arn_elements = delivery_stream_arn.split("/")
        stream_name = stream_arn_elements[-1]

        regex = re.compile(r'(\d{4})-(\d{2})-(\d{2})T(\d{2})') #regex to match the date regex of the file

        curr_datetime = datetime.utcnow()
        iso_date = curr_datetime.isoformat() #get the isoformat of the current datetime

        date_elements = regex.findall(iso_date)

        response = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name)
        
        if response:
            stream = response["DeliveryStreamDescription"]["Destinations"][0]
            configuration = stream["ExtendedS3DestinationDescription"]["DataFormatConversionConfiguration"]["SchemaConfiguration"]
            db_name = configuration["DatabaseName"]
            table_name = configuration["TableName"]
            bucket_arn_split = stream["ExtendedS3DestinationDescription"]["BucketARN"].split(":")
            bucket = bucket_arn_split[-1]
            bucket_prefix = stream["ExtendedS3DestinationDescription"]["Prefix"]

            stream_configuration["db_name"] = db_name
            stream_configuration["table_name"] = table_name
            stream_configuration["partitions"] = [date_elements[0][0], date_elements[0][1], date_elements[0][2], date_elements[0][3]]
            
            return stream_configuration
        else:
            raise
    except Exception as e:
        print("Errored out to get Firehose delivery configuration for arn : {} with error : {}".format(delivery_stream_arn,e))
    
        return stream_configuration

def get_partition_spec(params):
    '''function to get partition specifications from Glue table which will be used to add partition to the Glue table catalog'''
    
    print("Starting to get partition specification for table {} in database {}".format(params["table_name"],params["db_name"]))
    
    glue_partition_specs = {}
    
    try:
        response = glue_client.get_table(DatabaseName=params['db_name'],Name=params['table_name'])
        if response:
            partition_spec['StorageDescriptor'] = response['Table']['StorageDescriptor']
            partition_spec['StorageDescriptor']['Location'] = response['Table']['StorageDescriptor']['Location'] + params["partitions"][0]+"/"+params["partitions"][1]+"/"+params["partitions"][2]+"/"+params["partitions"][3]+"/"
            partition_spec['Values'] = params['partitions']
            
            glue_partition_specs['db_name'] = params['db_name']
            glue_partition_specs['table_name'] = params['table_name']
            glue_partition_specs['partition'] = partition_spec
            
            return glue_partition_specs
        
        else:
            raise
            
    except Exception as e:
        print("Errroed out to get table configuration from Glue for table {} in database : {} with error : {}".format(params['table_name'],params['db_name'],e))
        
        return glue_partition_specs
    
    
def add_partition(params):
    '''function to add partition to table in Glue catalog'''
    
    try:
        print("Starting to add partition to table {} in database".format(params['table_name'],params['db_name']))
        response = glue_client.create_partition(DatabaseName=params['db_name'],TableName=params['table_name'],PartitionInput=params['partition']) #response is empty dictionary
        
    except Exception as e:
        print("Exception occured while creating paritions for table {} in database {} with error : {}".format(params['table_name'],params['db_name'],e))
        
    
def lambda_handler(event, context):
    output = []

    deliver_stream_arn = event['deliveryStreamArn']
    
    stream_config_data  = describe_stream(deliver_stream_arn)
    
    partition_spec = None
    
    print(stream_config_data)
    if stream_config_data:
        print("Succesfully retrieved stream configuration data")
    
        partition_specs = get_partition_spec(stream_config_data)
    else:
        print ("Could not get stream configuration")
        sys.exit(1)
        
    if partition_specs:
        print("Successfully retrieved partition specifications")
    
        add_partition(partition_specs)
    else:
        sys.exit(1)

    for record in event['records']:
        payload = base64.b64decode(record['data'])

        # Do custom processing on the payload here
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload)
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}