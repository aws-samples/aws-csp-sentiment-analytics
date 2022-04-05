# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
    
import json
import boto3
import time
import datetime

s3=boto3.resource('s3')

def lambda_handler(event,context):

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
    cell = l[6]
    event_failure_rate_60m = l[7]
    IEFS_ID = l[8]

    # Upload the file

    file_name=event_sensorid+'_'+event_timestamp+'.csv'
    s3_object_path='engagement/'+file_name

    timestamp = datetime.datetime.fromtimestamp(int(event_timestamp))
    date_time=timestamp.strftime('%Y-%m-%d %H:%M:%S')
    date_time=date_time+'.123'


    string=date_time+','+event_sensorid+',engagement_incident_driven,ENGAGEMENT,0,0,'+cell+',0,'+IEFS_ID+'\n'+'0'+','+event_sensorid+',engagement_incident_driven,ENGAGEMENT,0,0,0,0,'+IEFS_ID+'\n'
    encoded_string= string.encode("utf-8")

    object = s3.Object(bucket_name,s3_object_path)
    object.put(Body=encoded_string)

    return date_time
