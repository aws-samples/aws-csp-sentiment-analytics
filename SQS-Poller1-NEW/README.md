# The Lambda application "SQS-Poller1-NEW" serves the following functions: 1. it is triggered by message being available on SQS queue, 2. it parses the content, 3. it updates a DynamoDB table
# The variable queue_url must be updated with the SQS queue incident_queue.fifo URL.
