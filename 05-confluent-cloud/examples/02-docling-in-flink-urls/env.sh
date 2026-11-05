# Example 02 — every name it creates, all derived from NS.
#
# Read env.sh in example 01 first: the layering and the reasoning are the same,
# and what follows is only what this example adds.
#
# What it adds is the front half. Example 01's chunk topic is written by
# Docling from outside; this one's is written by a Flink statement that calls
# Docling. Everything downstream of the chunk topic is identical — literally
# the same files, from common/flink — which is the point of the pair.

EXAMPLE_TITLE="02 — Docling in Flink, from public URLs"

# --- what this example is made of -----------------------------------------

# The Python UDF artifact: the enrichment, plus dc_chunks, which turns a
# conversion into chunk records.
PY_UDF_DIR="common/udf"
ARTIFACT_NAME="${NS}-ex02-udf"

# The Java UDF artifact: the Docling call, and nothing else. Java because a
# Confluent Cloud Python UDF has no TLS and cannot be bound to a CONNECTION —
# see docs/05-confluent-cloud/LIMITS.md, and common/udf-java/.
JAVA_UDF_DIR="common/udf-java"
JAVA_ARTIFACT_NAME="${NS}-ex02-udf-java"

# --- connections ------------------------------------------------------------
#
# A CONNECTION is the only route out of Confluent Cloud a UDF has, and it is
# also where the Docling API key lives — the statement never sees it. Two of
# them, because Docling for IBM watsonx answers with a presigned download URL on
# Amazon S3 rather than with the document, so the artifact is on a second host
# and a UDF may only reach hosts it is bound to.
#
# CONNECTION_VARS is the list ./setup.sh info prints. What creates and drops
# them is flink/05_connection_*.sql, through ./setup.sh apply and destroy.

DOCLING_CONNECTION="${NS}_ex02_docling"
DOWNLOAD_CONNECTION="${NS}_ex02_artifacts"

# Where the converted artifact is fetched from: the S3 region the *Docling
# service* signs its URLs in — us-east-1 for Docling for IBM watsonx, whatever
# region your cluster is in (this once followed CC_REGION, which only worked
# because the two happened to match). A self-hosted docling-serve that answers
# in the body needs none of this; point it at the same host then.
DOWNLOAD_ENDPOINT="${DOWNLOAD_ENDPOINT:-https://s3.us-east-1.amazonaws.com}"

CONNECTION_VARS="DOCLING_CONNECTION DOWNLOAD_CONNECTION"

# --- topics ----------------------------------------------------------------

# The input: one row per document, `{"doc_id": "...", "url": "https://..."}`.
URLS_TOPIC="${NS}.ex02.urls"
# What the convert statement writes and the enrichment statement reads — the
# same contract as example 01's chunk topic, and the reason the second half of
# the pipeline needs no changes at all.
CHUNKS_TOPIC="${NS}.ex02.chunks"
# What Docling answered, one row per URL — failures included — and the only
# place the *paid* work survives. Typed (json-registry: doc_id, url, status,
# conversion), so `SELECT status, url FROM ...` works in the SQL workspace; and
# kept for good, whatever RETENTION says, because the convert statement
# rebuilds the chunk topic from it on every redeploy (flink/23_view_*).
CONVERSIONS_TOPIC="${NS}.ex02.conversions"
CONVERSIONS_RETENTION="0"   # Flink's spelling of infinite

ENRICHED_TOPIC="${NS}.ex02.chunks.enriched"
QUARANTINE_TOPIC="${NS}.ex02.pii.quarantine"
REJECTED_TOPIC="${NS}.ex02.quality.rejected"

TOPIC_VARS="URLS_TOPIC CONVERSIONS_TOPIC CHUNKS_TOPIC ENRICHED_TOPIC QUARANTINE_TOPIC REJECTED_TOPIC"

# Every one of them is created by its CREATE TABLE, in ./setup.sh apply. Never
# ahead of it: the conversions topic is typed, and only CREATE TABLE registers
# its schema — a topic created first would be inferred as `key BYTES, val
# BYTES`, and IF NOT EXISTS would keep that.

# --- Flink objects ----------------------------------------------------------

APP_NAME="${NS}-ex02"

VIEW_CONVERTED="${NS}_ex02_converted"          # new URLs, through dc_convert
VIEW_CONVERTED_ALL="${NS}_ex02_converted_all"  # ...plus the conversions of earlier deploys
VIEW_PREPARED="${NS}_ex02_prepared"
VIEW_GUARDED="${NS}_ex02_guarded"
VIEW_VARS="VIEW_CONVERTED VIEW_CONVERTED_ALL VIEW_PREPARED VIEW_GUARDED"

FN_IS_CHUNK="${NS}_ex02_is_chunk"
FN_PREPARE="${NS}_ex02_prepare"
FN_GUARD="${NS}_ex02_guard"
# The two this example adds.
FN_CONVERT="${NS}_ex02_convert"
FN_CHUNKS="${NS}_ex02_chunks"
FUNCTION_VARS="FN_IS_CHUNK FN_PREPARE FN_GUARD FN_CONVERT FN_CHUNKS"

# --- deploy -------------------------------------------------------------------
#
# `./pipeline.sh deploy` leaves an unchanged statement running (common/
# deploy_plan.py), and when it does replace the convert statement it sets these
# two to where the topics end at that moment:
#
#   URLS_FROM_OFFSETS          where the new statement starts reading URLs —
#                              from here on: a URL is converted, and billed,
#                              once, whatever you redeploy
#   CONVERSIONS_UNTIL_OFFSETS  how far it re-reads the conversions it already
#                              has, to rebuild the chunks from them for free
#
# The defaults only let the SQL render outside a deploy.
DEPLOY_OFFSET_VARS="URLS_FROM_OFFSETS:URLS_TOPIC CONVERSIONS_UNTIL_OFFSETS:CONVERSIONS_TOPIC"
URLS_FROM_OFFSETS="${URLS_FROM_OFFSETS:-partition:0,offset:0}"
CONVERSIONS_UNTIL_OFFSETS="${CONVERSIONS_UNTIL_OFFSETS:-partition:0,offset:0}"

# --- conversion knobs -------------------------------------------------------

# What Docling is asked for. Markdown, because the chunker in
# docling_udf.docling_response reads Markdown — 'json' would give you the whole
# DoclingDocument and nothing here parses it.
DOCLING_FORMAT="md"
