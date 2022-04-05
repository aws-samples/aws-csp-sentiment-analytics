# The  Lambda application "S3-get-object-NEW" serves the following functions: 1. it is triggered by a S3 notification following a new object being put on S3, 2. it parses the content of the object, 3. it writes messages to a SQS queue
# The variable queue_url must be updated with the the SQS queue incident_queue.fifo URL.
