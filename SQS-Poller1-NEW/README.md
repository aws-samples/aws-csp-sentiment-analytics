The Lambda application "SQS-Poller1-NEW" serves the following functions: 

1. it is triggered by message being available on SQS queue
2. it parses the content
3. it updates a DynamoDB table

The variable queue_url must be entered in the Environment variables of the Lambda function on https://aws.amazon.com/console/.
