/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

parser grammar IdentifierParser;

options { tokenVocab=SqlLexer; }

identifier
     : keyWords
     | DURATION_LITERAL
     | ID
     | QUOTED_ID
     ;


// List of keywords, new keywords that can be used as identifiers should be added into this list. For example, 'not' is an identifier but can not be used as an identifier in node name.

keyWords
    : ADD
    | AFTER
    | ALIAS
    | ALIGN
    | ALIGNED
    | ALL
    | ALTER
    | AND
    | ANY
    | APPEND
    | AS
    | ASC
    | ATTRIBUTES
    | BEFORE
    | BEGIN
    | BETWEEN
    | BLOCKED
    | BOUNDARY
    | BY
    | CACHE
    | CHILD
    | CLEAR
    | CLUSTER
    | CONCAT
    | CONDITION
    | CONFIGNODES
    | CONFIGURATION
    | CONNECTOR
    | CONTAIN
    | CONTAINS
    | CONTINUOUS
    | COUNT
    | CQ
    | CQS
    | CREATE
    | DATA
    | DATABASE
    | DATABASES
    | DATANODEID
    | DATANODES
    | DATA_REPLICATION_FACTOR
    | DATA_REGION_GROUP_NUM
    | DEACTIVATE
    | DEBUG
    | DELETE
    | DESC
    | DESCRIBE
    | DETAILS
    | DEVICE
    | DEVICEID
    | DEVICES
    | DISABLE
    | DISCARD
    | DROP
    | ELAPSEDTIME
    | END
    | ENDTIME
    | EVERY
    | EXPLAIN
    | EXTRACTOR
    | FALSE
    | FILL
    | FILE
    | FIRST
    | FLUSH
    | FOR
    | FROM
    | FULL
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GROUP
    | HAVING
    | IN
    | INDEX
    | INFO
    | INSERT
    | INTO
    | IS
    | KILL
    | LABEL
    | LAST
    | LATEST
    | LEVEL
    | LIKE
    | LIMIT
    | LINEAR
    | LINK
    | LIST
    | LOAD
    | LOCAL
    | LOCK
    | MERGE
    | METADATA
    | MIGRATE
    | NAN
    | NODEID
    | NODES
    | NONE
    | NOT
    | NOW
    | NULL
    | NULLS
    | OF
    | OFF
    | OFFSET
    | ON
    | OR
    | ORDER
    | ONSUCCESS
    | PARTITION
    | PASSWORD
    | PATHS
    | PIPE
    | PIPES
    | PIPESINK
    | PIPESINKS
    | PIPESINKTYPE
    | POLICY
    | PREVIOUS
    | PREVIOUSUNTILLAST
    | PRIVILEGES
    | PROCESSLIST
    | PROCESSOR
    | PROPERTY
    | PRUNE
    | QUERIES
    | QUERY
    | QUERYID
    | RANGE
    | READONLY
    | REGEXP
    | REGION
    | REGIONID
    | REGIONS
    | REMOVE
    | RENAME
    | RESAMPLE
    | RESOURCE
    | REVOKE
    | ROLE
    | RUNNING
    | SCHEMA
    | SCHEMA_REPLICATION_FACTOR
    | SCHEMA_REGION_GROUP_NUM
    | SELECT
    | SERIESSLOTID
    | SESSION
    | SET
    | SETTLE
    | SGLEVEL
    | SHOW
    | SLIMIT
    | SOFFSET
    | STORAGE
    | START
    | STARTTIME
    | STATEFUL
    | STATELESS
    | STATEMENT
    | STOP
    | SYSTEM
    | TAGS
    | TASK
    | TEMPLATE
    | TEMPLATES
    | TIMEOUT
    | TIMESERIES
    | TIMEPARTITION
    | TIME_PARTITION_INTERVAL
    | TIMESLOTID
    | TO
    | TOLERANCE
    | TOP
    | TRACING
    | TRIGGER
    | TRIGGERS
    | TRUE
    | TTL
    | UNLINK
    | UNLOAD
    | UNSET
    | UPDATE
    | UPSERT
    | URI
    | USED
    | USER
    | USING
    | VALUES
    | VARIABLES
    | VARIATION
    | VERIFY
    | VERSION
    | WATERMARK_EMBEDDING
    | WHERE
    | WITH
    | WITHOUT
    | WRITABLE
    | PRIVILEGE_VALUE
    ;