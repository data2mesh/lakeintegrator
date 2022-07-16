import boto3
import botocore
import json


class StepFunctionsManager:

    def __init__(self, state_mach_arn):
        self.client = boto3.client('stepfunctions')
        self.state_mach_arn = state_mach_arn

    def execute_step_functions(self, message, prefix):
        try:
            print('sf message uuid: {}'.format(message['uuid']))
            response = self.client.start_execution(
                stateMachineArn=self.state_mach_arn,
                name='{}-{}'.format(prefix,str(message['uuid'])),
                input=json.dumps(message)
            )
            return True

        except botocore.exceptions.ClientError as e:
            print('Exception: {}'.format(e))
            return False


    def get_running_sf_ids(self):
        try:
            response = self.client.list_executions(
            stateMachineArn=self.state_mach_arn, statusFilter='RUNNING', maxResults=50)
            
            print(response)
            executions = response['executions']
            
            running_ids = map(lambda elem: elem['executionArn'].split(':')[-1], executions)
            return running_ids
        except Exception as e:
            print("Exception: {}".format(e))
            raise e

    def get_currently_running_step_func(self, max_executions_allowed):

        response = self.client.list_executions(
            stateMachineArn=self.state_mach_arn,
            statusFilter='RUNNING',
            maxResults=(2 * int(max_executions_allowed))
        )

        print(response)
        executions = response['executions']
        try:
            if (not executions):
                total_running_executions_ids = []
            else:
                total_running_executions_ids = map(lambda elem: elem['executionArn'].split(':')[-1], executions)
        except Exception as e:
            print("Exception occurred while getting the execution ids of step functions: " + str(e))

        return total_running_executions_ids 