The  Lambda application "Trigger_Connect" serves the following functions: 

1. it is triggered by a message being available on a SQS queue, 
2. it triggers an Amazon Connect Contact Flow.

ContactFlowId, InstanceId, SourcePhoneNumber must be updated with the Amazon Connect instance being created.
