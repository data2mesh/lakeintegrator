import json
import boto3
import botocore
import os
from utils.validate import validate_key
from utils.generate_token import generate_token
import random

    
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

    print(data_struct)

    data_message = {
            'MessageBody': json.dumps(data_struct),
            'MessageGroupId': data_struct['uuid_file'],
             'MessageDeduplicationId': data_struct['process_code']
        }

    url_queue_sandbox = os.environ["SQS_QUEUE_SANDBOX"]

    print("sns data_message: ", data_message)
    send_message_sqs(data_message, url_queue_sandbox)


    return response

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


def send_message_sqs(data_message, url_queue):
    client = boto3.client('sqs')
    response = {}
    try:
        
        resp = client.send_message(
            QueueUrl = url_queue,
            MessageBody = data_message['MessageBody'],
            MessageGroupId = data_message['MessageGroupId'],
            MessageDeduplicationId = data_message['MessageDeduplicationId']
        )
    except botocore.exceptions.ClientError as e:
        print("Failed send the queue: " + str(e))