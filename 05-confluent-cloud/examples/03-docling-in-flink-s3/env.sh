# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# Example 03 — every name it creates, all derived from NS.
#
# Read env.sh in example 02 first: this is that example with a different front
# door. Where 02 has a topic you type URLs into, 03 has a private S3 bucket,
# an SQS queue its object notifications go to, and Confluent's managed SQS
# Source connector turning those into records. From the conversion onwards it
# is example 02, and from the chunk topic onwards it is example 01.
#
# The AWS side — bucket, queue, notification, IAM user — is not named here.
# scripts/aws.sh creates it and writes what it made into this example's .env
# (S3_BUCKET, S3_REGION, S3_PREFIX, SQS_QUEUE_URL and the reader key pair);
# AWS.md is the long version. The SQL and the connector config refer to those
# names, and rendering fails loudly until they are set.

EXAMPLE_TITLE="03 — Docling in Flink, from a private S3 bucket"

# --- what this example is made of -----------------------------------------

PY_UDF_DIR="common/udf"
ARTIFACT_NAME="${NS}-ex03-udf"

# The Java artifact gains a second function here: dc_presign, beside
# dc_convert. One JAR, two CREATE FUNCTIONs.
JAVA_UDF_DIR="common/udf-java"
JAVA_ARTIFACT_NAME="${NS}-ex03-udf-java"

# --- connections ------------------------------------------------------------
#
# Example 02's two — the Docling service, and the S3 host its results are
# presigned on — plus the bucket the documents are in. That third one is never
# called: dc_presign only *reads* it, for the endpoint (which says the bucket
# and the region) and the token (the reader key pair, as id:secret). It is a
# connection anyway because a connection is the one place a UDF can read a
# secret from without it being in the text of a statement.

DOCLING_CONNECTION="${NS}_ex03_docling"
DOWNLOAD_CONNECTION="${NS}_ex03_artifacts"
BUCKET_CONNECTION="${NS}_ex03_bucket"

DOWNLOAD_ENDPOINT="${DOWNLOAD_ENDPOINT:-https://s3.us-east-1.amazonaws.com}"

CONNECTION_VARS="DOCLING_CONNECTION DOWNLOAD_CONNECTION BUCKET_CONNECTION"

# --- the connector ---------------------------------------------------------
#
# terraform/main.tf's confluent_connector, created by ./setup.sh apply.
# Fully managed, billed by the hour while it runs, removed by destroy.

SQS_CONNECTOR="${NS}-ex03-sqs-source"
CONNECTOR_VARS="SQS_CONNECTOR"

# --- topics ----------------------------------------------------------------

# What the connector writes: one record per SQS message, the S3 event JSON as
# a string in its Body. scripts/backfill.py's messages land here too, by the
# same road.
OBJECTS_TOPIC="${NS}.ex03.objects"
CHUNKS_TOPIC="${NS}.ex03.chunks"
# One row per object Docling was asked to convert, failures included — the
# paid work, kept for good, as in example 02.
CONVERSIONS_TOPIC="${NS}.ex03.conversions"
CONVERSIONS_RETENTION="0"

ENRICHED_TOPIC="${NS}.ex03.chunks.enriched"
QUARANTINE_TOPIC="${NS}.ex03.pii.quarantine"
REJECTED_TOPIC="${NS}.ex03.quality.rejected"

TOPIC_VARS="OBJECTS_TOPIC CONVERSIONS_TOPIC CHUNKS_TOPIC ENRICHED_TOPIC QUARANTINE_TOPIC REJECTED_TOPIC"

# Every one of them is created by its CREATE TABLE, in ./setup.sh apply — the
# objects topic before the connector starts, because terraform/main.tf has the
# connector wait for the tables.

# --- Flink objects ----------------------------------------------------------

APP_NAME="${NS}-ex03"

VIEW_OBJECTS="${NS}_ex03_objects"              # S3 events -> {bucket, key, etag, size}
VIEW_CONVERTED="${NS}_ex03_converted"          # new objects, presigned, through dc_convert
VIEW_CONVERTED_ALL="${NS}_ex03_converted_all"  # ...plus the conversions of earlier deploys
VIEW_PREPARED="${NS}_ex03_prepared"
VIEW_GUARDED="${NS}_ex03_guarded"
VIEW_VARS="VIEW_OBJECTS VIEW_CONVERTED VIEW_CONVERTED_ALL VIEW_PREPARED VIEW_GUARDED"

FN_IS_CHUNK="${NS}_ex03_is_chunk"
FN_PREPARE="${NS}_ex03_prepare"
FN_GUARD="${NS}_ex03_guard"
FN_CONVERT="${NS}_ex03_convert"
FN_CHUNKS="${NS}_ex03_chunks"
FN_PRESIGN="${NS}_ex03_presign"
FUNCTION_VARS="FN_IS_CHUNK FN_PREPARE FN_GUARD FN_CONVERT FN_CHUNKS FN_PRESIGN"

# --- deploy -------------------------------------------------------------------
#
# As in example 02: a replaced convert statement reads the objects topic from
# where it ends at the deploy (no object converted twice), and rebuilds the
# chunks from the conversions topic up to where *that* ended.
DEPLOY_OFFSET_VARS="OBJECTS_FROM_OFFSETS:OBJECTS_TOPIC CONVERSIONS_UNTIL_OFFSETS:CONVERSIONS_TOPIC"
OBJECTS_FROM_OFFSETS="${OBJECTS_FROM_OFFSETS:-partition:0,offset:0}"
CONVERSIONS_UNTIL_OFFSETS="${CONVERSIONS_UNTIL_OFFSETS:-partition:0,offset:0}"

# --- conversion knobs -------------------------------------------------------

DOCLING_FORMAT="md"

# How long a presigned URL works for, in seconds. Docling fetches the object
# within seconds of being handed it, and the URL is recorded in the
# conversion on its topic — so short. It expires; it is never a secret worth
# rotating.
PRESIGN_TTL="${PRESIGN_TTL:-600}"
