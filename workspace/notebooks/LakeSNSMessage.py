import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json
import botocore
import os

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


sns_client = boto3.client('sns', 'us-east-1')

def publish_message(message_hash, topic_arn, message_type, subject):
    """
    this method publishes messages to SNS
    :param message_hash: actual message
    :param topic_arn: sns topic arn
    :param options_hash: hash code
    :return: HTTP status code of the request
    """
    message = json.dumps(message_hash)
    #subject = self.get_subject(message)
    sent_message = False
    
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject,
            MessageStructure='String',
            MessageAttributes={
                    'message_type': {
                        'DataType': 'String',
                        'StringValue': message_type
                    }
            }
        )
        print("Publish message response: "+json.dumps(response))
        sent_message = True
    except botocore.exceptions.ClientError as e:
        print("Failed while publishing the data to SNS topic: "+ str(e))

    return sent_message

topic_arn = "arn:aws:sns:us-east-1:279224823134:sns-integrator-notify-alert"
message_hash = "mensaje desde GlueJob"
message_type = "String"
subject = 'GlueJobMensaje'
publish_message(message_hash, topic_arn, message_type, subject)

job.commit()