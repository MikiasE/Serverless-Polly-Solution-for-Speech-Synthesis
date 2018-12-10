import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):

	# Grabs DynamoDB Post ID
	
    post = event["postId"]

	# Stage DynamoDB synthesis table
	
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['DB_TABLE_NAME'])
	
	# Will return all items from the table if * or an individual one based on a query
	
    if post=="*":
        items = table.scan()
    else:
        items = table.query(
            KeyConditionExpression=Key('id').eq(post)
        )

    return items["Items"]
