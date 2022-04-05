// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
    
import json
import boto3
import botocore
import math

# Create SQS client
sqs = boto3.client('sqs')

# Create DynanoDB link
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Subscriber_table')

# generate random floating point values
from random import seed
from random import randint

# seed random number generator
seed(1)

def lambda_handler(event,context):

    try:
        # Receive message from SQS queue
        payload =event['Records'][0]['body']
        receiptHandle=event['Records'][0]['receiptHandle']

        # Extract csv content of the payload
        l = payload.split(',')
        event_timestamp = l[0]
        event_sensorid = l[1]
        event_type = l[2]
        event_status = l[3]
        event_number_failure_15m = l[4]
        event_failure_rate_15m = l[5]
        event_number_failure_60m = l[6]
        event_failure_rate_60m = l[7]
        IEFS_ID = l[8]


        # update DynamoDB Table
        
        incident = []
        feedback = []
        engagement = []
        sentiment = []
        
        try:
            table.update_item(
                Key={'PhoneNumber':event_sensorid},
                UpdateExpression="SET latest_sentiment= :var1",
                ExpressionAttributeValues={
                    ":var1":int(10)
                },
                ReturnValues="UPDATED_NEW",
                ConditionExpression='attribute_not_exists(latest_sentiment)'
            )
        except botocore.exceptions.ClientError as e:
         # Ignore the ConditionalCheckFailedException, bubble up
        # other exceptions.
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                pass
        
        
        if event_status=='FAIL':
            table.update_item(
                Key={'PhoneNumber':event_sensorid},
                UpdateExpression="SET latest_change_time= :var1, latest_change_type= :var2, incident= list_append(if_not_exists(incident, :empty_list), :var3)",
                ExpressionAttributeValues={
                    ":var1":int(event_timestamp),
                    ":var2":str(event_type),
                    ":var3": [{"timestamp":event_timestamp,"incident_type":event_type,"failure_rate_1h":event_failure_rate_15m,"failure_rate_6h":event_failure_rate_60m, "cell":event_number_failure_60m}],
                    ":empty_list":[]
                },
                ReturnValues="UPDATED_NEW"
            )
            print(event_sensorid)
        elif event_status=='FEEDBACK':
            table.update_item(
                Key={'PhoneNumber':event_sensorid},
                UpdateExpression="SET latest_change_time= :var1, latest_change_type= :var2, feedback= list_append(if_not_exists(feedback, :empty_list), :var3)",
                ExpressionAttributeValues={
                    ":var1":int(event_timestamp),
                    ":var2":str(event_type),
                    ":var3": [{"timestamp":event_timestamp,"score":event_number_failure_15m,"cell":event_number_failure_60m,"IEFS":IEFS_ID}],
                    ":empty_list":[]
                },
                ReturnValues="UPDATED_NEW"
            )
        elif event_status=='ENGAGEMENT':
            table.update_item(
                Key={'PhoneNumber':event_sensorid},
                UpdateExpression="SET latest_change_time= :var1, latest_change_type= :var2, engagement= list_append(if_not_exists(engagement, :empty_list), :var3), latest_engagement_time= :var4",
                ExpressionAttributeValues={
                    ":var1":int(event_timestamp),
                    ":var2":str(event_type),
                    ":var3": [{"timestamp":event_timestamp,"type":event_type,"cell":event_number_failure_60m,"IEFS":IEFS_ID}],
                    ":var4":int(event_timestamp),
                    ":empty_list":[]
                },
                ReturnValues="UPDATED_NEW"
            )
        elif event_status=='SENTIMENT':
            table.update_item(
                Key={'PhoneNumber':event_sensorid},
                UpdateExpression="SET latest_change_time= :var1, latest_change_type= :var2, latest_sentiment= :var3, sentiment= list_append(if_not_exists(sentiment, :empty_list), :var4)",
                ExpressionAttributeValues={
                    ":var1":int(event_timestamp),
                    ":var2":str(event_type),
                    ":var3":int(event_number_failure_15m),
                    ":var4": [{"timestamp":event_timestamp,"type":event_type, "change":event_number_failure_60m,"cell":event_failure_rate_60m,"IEFS":IEFS_ID}],
                    ":empty_list":[]
                },
                ReturnValues="UPDATED_NEW"
            )     
        return event_type
        sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receiptHandle)

    except Exception as e:
        print(e)
        raise e
