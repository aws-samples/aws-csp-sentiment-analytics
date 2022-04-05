# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
    
import json
import boto3
import time
import datetime

s3=boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Subscriber_table')

def lambda_handler(event,context):

    # Receive message from Connect
    telnum=event['Details']['ContactData']['CustomerEndpoint']['Address']
    score=event['Details']['ContactData']['Attributes']['impactscore']
    cell=event['Details']['ContactData']['Attributes']['cell']
        
    prefix=str(telnum).replace("+","")
    now=str(time.time())
    now=now[0:10]
    
    score=str(score)
    
    timestamp = datetime.datetime.fromtimestamp(int(now))
    date_time=timestamp.strftime('%Y-%m-%d %H:%M:%S')
    date_time=date_time+'.123'

#update feedback
    string_feedback=date_time+','+prefix+',feedback,FEEDBACK,'+score+',0,'+cell+',0,'+now+'\n'+'0,'+prefix+',feedback,FEEDBACK,'+score+',0,0,0,'+now+'\n'
    encoded_string_feedback= string_feedback.encode("utf-8")

    file_name_feedback=prefix+'_'+now+'_feedback.csv'
    s3_object_path_feedback='feedbacks/'+file_name_feedback

    object = s3.Object(bucket_name,s3_object_path_feedback)
    object.put(Body=encoded_string_feedback)

    
    #read current sentiment
    item={}
    item=table.get_item(Key={'PhoneNumber':prefix})
    item=item.get('Item')

    current_sentiment=item.get('latest_sentiment')
    
    #update sentiment 
    score=int(score)
    if score==1:
        sentiment=current_sentiment
        change=0
    elif score<=3:
        sentiment=current_sentiment-1
        change=-1
    else:
        sentiment=current_sentiment-2
        change=-2
    
    sentiment=str(sentiment)
    current_sentiment=str(current_sentiment)
    change=str(change)
            
    string_sentiment=date_time+','+prefix+',sentiment_feedback_driven,SENTIMENT,'+sentiment+','+current_sentiment+','+change+','+cell+','+now+'\n'+'0,'+prefix+',sentiment_feedback_driven,SENTIMENT,'+sentiment+','+current_sentiment+','+change+',0,'+now+'\n'
    encoded_string_sentiment= string_sentiment.encode("utf-8")
    
    file_name_sentiment=prefix+'_'+now+'_sentiment.csv'    
    s3_object_path_sentiment='sentiment/'+file_name_sentiment
    
    object = s3.Object(bucket_name,s3_object_path_sentiment)
    object.put(Body=encoded_string_sentiment)
    
    return encoded_string_feedback+encoded_string_sentiment
