import boto3
import os
from contextlib import closing
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):

    post = event["Records"][0]["Sns"]["Message"]
    print "Speech Synthesis function. The Post ID in DynamoDB is: " + post

    # Retrieving post information from "synthesis" table in DynamoDB
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['DB_TABLE_NAME'])
    postItem = table.query(
        KeyConditionExpression=Key('id').eq(post)
    )

    text = postItem["Items"][0]["text"]
    voice = postItem["Items"][0]["voice"]
    total = text

    # Dividing text into blocks of 2500 characters to not overload Polly's 3000 char limit per invocation.
    
    textBlocks = []
    while (len(total) > 2600):
        begin = 0
        end = total.find(".", 2500)

        if (end == -1):
            end = total.find(" ", 2500)

        textBlock = total[begin:end]
        total = total[end:]
        textBlocks.append(textBlock)
    textBlocks.append(total)

    # Invoke Polly API for each text block
    
    polly = boto3.client('polly')
    for textBlock in textBlocks:
        response = polly.synthesize_speech(
            OutputFormat='mp3',
            Text = textBlock,
            VoiceId = voice
        )

        # Save audio/concatenate audio
        
        if "AudioStream" in response:
            with closing(response["AudioStream"]) as stream:
                output = os.path.join("/tmp/", post)
                with open(output, "a") as file:
                    file.write(stream.read())

    # Stage S3 bucket
    
    s3 = boto3.client('s3')
    s3.upload_file('/tmp/' + post,
      os.environ['BUCKET_NAME'],
      post + ".mp3")
    s3.put_object_acl(ACL='public-read',
      Bucket=os.environ['BUCKET_NAME'],
      Key= post + ".mp3")

    location = s3.get_bucket_location(Bucket=os.environ['BUCKET_NAME'])
    region = location['LocationConstraint']

    if region is None:
        url_beginning = "https://s3.amazonaws.com/"
    else:
        url_beginning = "https://s3-" + str(region) + ".amazonaws.com/"

    url = url_beginning \
            + str(os.environ['BUCKET_NAME']) \
            + "/" \
            + str(post) \
            + ".mp3"

    # Updating DynamoDB item
    
    response = table.update_item(
        Key={'id':post},
          UpdateExpression=
            "SET #statusAtt = :statusValue, #urlAtt = :urlValue",
          ExpressionAttributeValues=
            {':statusValue': 'UPDATED', ':urlValue': url},
        ExpressionAttributeNames=
          {'#statusAtt': 'status', '#urlAtt': 'url'},
    )

    return
