The  Lambda application "Read-after-incident" serves the following functions: 

1. it is triggered by a DynamoDB stream following a table update
2. it parses the content of the stream
3. it decides whether to trigger the subscriber engagement phase, and if yes
4. it writes a message to two SQS queues.

The variable update_eng_queue_url must be updated with the SQS engagement_update_queue URL. The variable trigger_eng_queue_url must be updated with the SQS engagement_trigger_queue.fifo URL.
