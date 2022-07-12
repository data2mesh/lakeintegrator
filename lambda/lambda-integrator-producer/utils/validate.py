import json
import boto3
import os

s3 = boto3.client('s3')

def validate_key(key_file):
    response = { "statusCode": 200, "body": "suceesfull"}
    
    is_valid, response = validate_lenght(response, key_file)
    if not is_valid:
        return is_valid, response, None

    list_key = key_file.split("/")
    data_struct = {
        "objectS3": key_file,
        "source" : list_key[1],
        "interface" : list_key[2],
        "file_name" : list_key[3]
    }

    is_valid, response = validate_source(response, data_struct["source"])
    if not is_valid:
        return is_valid, response, None

    is_valid, response = validate_interface(response, data_struct["source"], data_struct["interface"])
    if not is_valid:
        return is_valid, response, None

    return is_valid, response, data_struct

def validate_lenght(response, key_file):
    if len(key_file)==4:
        response["statusCode"] = 411
        response["body"] = "Key route Lenght invalid"
        return False, response
    return True, response

def validate_source(response, source):
    bucket = os.environ["BUCKET_ARTIFACTS"]
    key = os.environ["KEY_ARTIFACTS"]

    file = s3.get_object(Bucket=bucket, Key=key)
    file = file['Body'].read()
    config_source = file.decode('utf-8', errors='replace')
    config_source = json.loads(config_source)
    list_sources = config_source["dev"]["sources"]
    if not source in list_sources:
        response["statusCode"] = 405
        response["body"] = "Source not allowed"
        return False, response

    return True, response

def validate_interface(response, source, interface):
    return True, response