// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

CREATE OR REPLACE STREAM "All_Events_Stream" (
"sequence_number" varchar(128), 
"time_of_incident" timestamp, 
"phone_number" integer,
"type" varchar(16),
"count_15m" float,
"count_1h" float,
"cell" integer,
"status" varchar(4));

CREATE OR REPLACE PUMP "All_Events_Stream_Pump" AS INSERT INTO "All_Events_Stream"
 SELECT STREAM 
 "SEQUENCE_NUMBER" as "sequence_number", 
 ROWTIME as "time_of_incident",
 "phonenumber" as "phone_number",
 "type" as "type",
 count("status") OVER (PARTITION BY "phonenumber" RANGE INTERVAL '15' MINUTE PRECEDING) as "count_15m",
 count("status") OVER (PARTITION BY "phonenumber" RANGE INTERVAL '60' MINUTE PRECEDING) as "count_1h",
 "cell" as "cell",
 "status" as "status"
FROM "SOURCE_SQL_STREAM_001";


CREATE OR REPLACE STREAM "Failure_Events_Stream" (
"sequence_number" varchar(128), 
"time_of_incident" timestamp, 
"phone_number" integer,
"type" varchar(16),
"count_15m" float,
"count_1h" float, 
"cell" integer,
"status" varchar(4));

CREATE OR REPLACE PUMP "Failure_Events_Stream_Pump" AS INSERT INTO "Failure_Events_Stream"
 SELECT STREAM
 "SEQUENCE_NUMBER" as "sequence_number", 
 ROWTIME,
 "phonenumber",
 "type",
 count("status") OVER (PARTITION BY "phonenumber" RANGE INTERVAL '15' MINUTE PRECEDING) as "count_15m",
 count("status") OVER (PARTITION BY "phonenumber" RANGE INTERVAL '60' MINUTE PRECEDING) as "count_1h",
 "cell" as "cell",
 "status"
FROM "SOURCE_SQL_STREAM_001" where "status" = 'FAIL';


CREATE OR REPLACE STREAM "Joined_Stream" (
"seq_num" varchar(128), 
"time_of_incident" timestamp, 
"phone_number" integer,
"type" varchar(16),
"status" varchar(4), 
"count_15m_FAIL" float, 
"count_15m_ALL" float,
"count_1h_FAIL" float, 
"count_1h_ALL" float,
"cell" integer);

CREATE OR REPLACE PUMP "Joined_Stream_Pump" AS INSERT INTO "Joined_Stream"
 SELECT STREAM 
 e."sequence_number" as "seq_num",
 e."time_of_incident" as "time_of_incident", 
 e."phone_number" as "phone_number",
 e."type" as "type",
 e."status" as "status",
 f."count_15m" as "count_15m_FAIL",
 e."count_15m" as "count_15m_ALL",
 f."count_1h" as "count_1h_FAIL",
 e."count_1h" as "count_1h_ALL",
 e."cell" as "cell"
FROM "All_Events_Stream" OVER (RANGE INTERVAL '1' MINUTE PRECEDING) AS e
JOIN "Failure_Events_Stream" AS f
ON e."sequence_number" = f."sequence_number";

CREATE OR REPLACE STREAM "Output_Stream" (
"time_of_incident" timestamp, 
"phone_number" bigint,
"type" varchar(16),
"status" varchar(4),
"number_failures_15m" float,
"fail_rate_15m" float,
"cell" integer,
"fail_rate_1h" float,
"IEFS" integer);

CREATE OR REPLACE PUMP "Output_Stream_Pump" AS INSERT INTO "Output_Stream"
 SELECT STREAM 
 "time_of_incident" as "time_of_incident", 
 ("phone_number") as "phone_number", 
 "type" as "type",
 "status" as "status",
 "count_15m_FAIL" as "number_failures_15m",
 ("count_15m_FAIL" / "count_15m_ALL") as "fail_rate_15m",
  "cell" as "cell",
 ("count_1h_FAIL" / "count_1h_ALL") as "fail_rate_1h",
 0 as "IEFS"
FROM "Joined_Stream" where "status" = 'FAIL';
