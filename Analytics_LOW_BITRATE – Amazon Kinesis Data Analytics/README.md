# The  SQL function "Analytics_LOW_BITRATE" processes the input data stream in real-time and reports statistics and other relevant information to an output stream.
# The SQL application performs the following steps:
#   •	It isolates incident records from all event records
#   •	On an individual subscriber-basis (MSISDN), it calculates the incident ratio over a 15 minutes window and the incident ratio over a 60 minutes window
#   •	It includes record attributes and calculated incident metrics in an output in-application stream 
# Any other record structure can be supported by easily modifying the Amazon Kinesis Data Analytics SQL application code.
