The Lambda application "Collect_feedback_from_Connect" serves the following functions: 

1. it is triggered by an Amazon Connect Contact Flow
2. it reads the latest sentiment from DynamoDB
3. it puts two objects in the event bucket

The variable bucket_name must be entered in the Environment variables of the Lambda function on https://aws.amazon.com/console/.
