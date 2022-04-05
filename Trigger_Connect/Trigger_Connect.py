# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
    
import json
import boto3

# Define the client to interact with Amazon Connect
client = boto3.client('connect')

def lambda_handler(event, context):
    
    message=event['Records'][0]['body']
    receiptHandle=event['Records'][0]['receiptHandle']
    
    l = message.split(',')
    event_time=l[0]
    sensorid=l[1]
    event_type=l[2]
    event_status=l[3]
    failure_rate_1h=l[5]
    cell=l[6]
    failure_rate_6h=l[7]
    IEFS_ID=l[8]

    response = client.start_outbound_voice_contact(
    DestinationPhoneNumber=str(sensorid),
    # add your ContactFlowIDnumber
    ContactFlowId=ContactFlowIDnumber,
    # add your InstanceIDnumber
    InstanceId=InstanceIDnumber,
    SourcePhoneNumber='ConnectSourcePhoneNumber',
    Attributes={
        "cell":cell
        }
    )
    
    return response
