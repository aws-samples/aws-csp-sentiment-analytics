The  Lambda application "write_engagement_to_S3" serves the following functions: 

1. it is triggered by a message being written on a SQS queue
2. put an object into S3.

The variable bucket_name must be entered in the Environment variables of the Lambda function on https://aws.amazon.com/console/.
