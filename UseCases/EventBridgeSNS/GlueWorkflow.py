import boto3
from botocore.exceptions import ClientError

def start_a_workflow(workflow_name)
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      response = glue_client.start_workflow_run(Name=workflow_name)
      return response
   except ClientError as e:
      raise Exception("boto3 client error in start_a_workflow: " + e.__str__())
   except Exception as e:
      raise Exception("Unexpected error in start_a_workflow: " + e.__str__())
print(start_a_workflow("test-daily"))
