import boto3
import os
import uuid

def lambda_handler(event, context):

    record = str(uuid.uuid4())
    
    # Definining Input Paramters 
    
    speech = event["voice"]
    text = event["text"]

    print('Generating new DynamoDB record, with ID: ' + record)
    print('Input Text: ' + text)
    print('Selected voice: ' + speech)

    # DynamoDB Record Creation
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['DB_TABLE_NAME'])
    table.put_item(
        Item={
            'id' : record,
            'text' : text,
            'voice' : speech,
            'status' : 'IN PROCESS'
        }
    )

    # Sending SNS Publishing Information
    
    client = boto3.client('sns')
    client.publish(
        TopicArn = os.environ['SNS_TOPIC'],
        Message = record
    )

    return record
