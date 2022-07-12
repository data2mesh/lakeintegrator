import json

def lambda_handler(event, context):
    print(event)
    # TODO implement
    
    if isinstance(event, str):
        event = json.loads(event)
        
    body =  event["Records"][0]["body"]
    data = json.loads(body) 
    print("La data recibida es: ", data)    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda CONSUMER!')
    }