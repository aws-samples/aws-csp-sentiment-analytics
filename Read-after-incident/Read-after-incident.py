# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Subscriber_table')
sqs = boto3.resource('sqs')

def lambda_handler(event, context):
    for record in event['Records']:
        sensorid = record['dynamodb']['Keys']['PhoneNumber']['S']
        item={}
        item=table.get_item(Key={'PhoneNumber':sensorid})
        item=item.get('Item')
        if item is None:
            break
        else:
            last_incident_time=item.get('latest_change_time')
            last_incident_time=str(last_incident_time)
            last_incident_type=item.get('latest_change_type')
            last_incident_type=str(last_incident_type)
            incident_list=[]
            incident_item={}
            incident_list=item.get('incident')
            failure_rate_1h=0
            failure_rate_6h=0
            iteration=0
            if last_incident_type=='engagement_incident_driven': 
                return last_incident_type
            if last_incident_type=='feedback':
                return last_incident_type
            for index in incident_list:
                iteration=iteration+1
                incident_item.update(index)
                if str(incident_item.get('incident_type')) == last_incident_type:
                    if incident_item.get('timestamp') == last_incident_time:
                        failure_rate_6h=incident_item.get('failure_rate_6h')
                        failure_rate_1h=incident_item.get('failure_rate_1h')
                        cell=incident_item.get('cell')
                        cell=str(cell)
                        iteration=iteration-1
                        break
            IEFS_ID=last_incident_time
            
            if last_incident_type == 'CALL_DROP':
                if float(failure_rate_1h) > call_drop_threshold:
                    message_sqs=last_incident_time+','+sensorid+','+last_incident_type+',ENGAGEMENT,0,'+failure_rate_1h+','+cell+',0,'+IEFS_ID
                    queue = sqs.Queue(update_eng_queue_url)
                    insert = queue.send_message(MessageBody=message_sqs)
                    #write message to SQS queue
                    queue = sqs.Queue(trigger_eng_queue_url)
                    message_group_id=sensorid
                    insert = queue.send_message(MessageBody=message_sqs,MessageGroupId=message_group_id)
                                    
            if last_incident_type == 'CALL_QUALITY':
                if float(failure_rate_1h) > call_quality_threshold:
                    
                    #write message to SQS queue 
                    message_sqs=last_incident_time+','+sensorid+','+last_incident_type+',ENGAGEMENT,0,'+failure_rate_1h+','+cell+',0,'+IEFS_ID
                    queue = sqs.Queue(update_eng_queue_url)
                    insert = queue.send_message(MessageBody=message_sqs)
                    #write message to SQS queue
                    queue = sqs.Queue(trigger_eng_queue_url)
                    message_group_id=sensorid
                    insert = queue.send_message(MessageBody=message_sqs,MessageGroupId=message_group_id)                    
                    
        
            if last_incident_type == 'LOW_BITRATE':
                if float(failure_rate_6h) > low_bitrate_threshold:
    
                    #write message to SQS queue 
                    message_sqs=last_incident_time+','+sensorid+','+last_incident_type+',ENGAGEMENT,0,0,'+cell+','+failure_rate_6h+','+IEFS_ID
                    queue = sqs.Queue(update_eng_queue_url)
                    insert = queue.send_message(MessageBody=message_sqs)
                    #write message to SQS queue
                    queue = sqs.Queue(trigger_eng_queue_url)
                    message_group_id=sensorid
                    insert = queue.send_message(MessageBody=message_sqs,MessageGroupId=message_group_id)
                    
            
            if last_incident_type == 'VIDEO_STALLING':
                if float(failure_rate_6h) > video_stalling_threshold:
                    
                    #write message to SQS queue 
                    message_sqs=last_incident_time+','+sensorid+','+last_incident_type+',ENGAGEMENT,0,0,'+cell+','+failure_rate_6h+','+IEFS_ID
                    queue = sqs.Queue(update_eng_queue_url)
                    insert = queue.send_message(MessageBody=message_sqs)
                    #write message to SQS queue
                    queue = sqs.Queue(trigger_eng_queue_url)
                    message_group_id=sensorid
                    insert = queue.send_message(MessageBody=message_sqs,MessageGroupId=message_group_id)
        

    return 'Successfully processed {} records.'.format(len(event['Records']))
