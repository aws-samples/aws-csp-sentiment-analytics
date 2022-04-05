The Lambda application "Collect_feedback_from_Connect" serves the following functions: 

1. it is triggered by an Amazon Connect Contact Flow
2. it reads the latest sentiment from DynamoDB
3. it puts two objectes in the event bucket

Variable bucket_name must be updated with the name of the event bucket
