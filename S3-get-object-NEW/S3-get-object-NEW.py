# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import urllib.parse
import boto3
import csv
import time

print('Loading function')

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')

def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    queue = sqs.Queue(queue_url)
    
    try:
        file = s3.Object(bucket, key)
        payload = file.get()['Body'].read().decode('utf-8')
        payload_csv = csv.reader(payload.split('\n'))
        line =[]
        split_line=[]
        for row in payload_csv:
            message = str(row).replace('[', '').replace(']', '').replace('\'', '').replace('"', '')
            split_line=message.split(',')
            date_time=str(split_line[0])
            if date_time=='0':
                break
            if date_time=='':
                break
            pattern = '%Y-%m-%d %H:%M:%S.%f'
            epoch = int(time.mktime(time.strptime(date_time, pattern)))
            split_line[0]=epoch

            message=str(split_line[0])+','+split_line[1]+','+split_line[2]+','+split_line[3]+','+split_line[4]+','+split_line[5]+','+split_line[6]+','+split_line[7]+','+split_line[8]
            message_no_space=message.replace(' ', '')
            line.append(message_no_space)
            message_group_id=(str(split_line[1]).replace(' ',""))
            insert = queue.send_message(MessageBody=message_no_space,MessageGroupId=message_group_id)
        return message_no_space  

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
