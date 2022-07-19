import json
import boto3
import botocore
import os
from utils.validate import validate_key
from utils.generate_token import generate_token
import random
from utils.step_function import StepFunctionsManager
    
def lambda_handler(event, context):
    print(f'S3 Evento:  {event}')

    if isinstance(event, str):
        event = json.loads(event)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f'Bucket: {bucket}, Key: {key}')

    is_valid, response, data_struct = validate_key(key)
    if not is_valid:
            return response

    copy_source = {"Bucket": bucket, "Key": key} 
    data_struct["objectS3"] = key

    audit_token = generate_token()
    data_struct.update(audit_token)

    data_struct = move_raw(copy_source, data_struct)
    data_struct["bucket_raw"] = os.environ["BUCKET_RAW"]
    data_struct["bucket_artifacts"] = os.environ["BUCKET_ARTIFACTS"]
    data_struct["dlk_database_raw"] = os.environ["DLK_DATABASE_RAW"]
    data_struct["dlk_database_analytics"] = os.environ["DLK_DATABASE_ANALYTIC"]

    print("data_struct : ",data_struct)
    
    state_machine_arn = os.environ["STATE_MACHINE_ARN"]
    
    trigger_stepfunction(data_struct, state_machine_arn)
    
    print("")

    data_message = {
            'MessageBody': json.dumps(data_struct),
            'MessageGroupId': data_struct['uuid_file'],
             'MessageDeduplicationId': data_struct['process_code']
        }

    return response


def trigger_stepfunction(data_message, state_machine_arn):
    try:
        print("Invoke execution step")
        sf_manager = StepFunctionsManager(state_machine_arn)
        prefix = "lambda-producer"
        print("Start execution step")
        sf_manager.execute_step_functions(data_message, prefix)
    except Exception as e:
        print('Exception ocurred: {}'.format(e))
        raise e


def move_raw(copy_source, data_struct):

    bucket_move = os.environ["BUCKET_MOVE"]
    key_move = os.environ["KEY_MOVE"]
    key_move = os.path.join(key_move, data_struct["source"])
    key_move = os.path.join(key_move, data_struct["process_code"])
    key_move = os.path.join(key_move, data_struct["uuid_file"])
    key_move = os.path.join(key_move, f"{data_struct['interface']}_{data_struct['file_name']}")
    data_struct["objectS3Move"]: key_move
    
    s3 = boto3.resource('s3')
    s3.meta.client.copy(copy_source, bucket_move, key_move)

    return data_struct
