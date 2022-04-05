The  Lambda application "Read-after-incident" serves the following functions: 

1. it is triggered by a DynamoDB stream following a table update
2. it parses the content of the stream
3. it decides whether to trigger the subscriber engagement phase, and if yes
4. it writes a message to two SQS queues.

The following variables must be entered in the Environment variables of the Lambda function on https://aws.amazon.com/console/.
- call_drop_threshold
- call_quality_threshold
- low_bitrate_threshold
- video_stalling_threshold
- trigger_eng_queue_url
- update_eng_queue_url
