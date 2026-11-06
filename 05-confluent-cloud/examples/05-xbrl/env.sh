# Example 05 — every name it creates, all derived from NS.
#
# Sourced by ./setup.sh and ./pipeline.sh, after 05-confluent-cloud/.env (which is
# where NS comes from) and before examples/05-xbrl/.env.
#
# Read env.sh in example 02 first: this is the same shape — a topic of inputs and
# a Flink statement that calls Docling through a CONNECTION — with a different
# back half. Where example 02 turns a conversion into chunks and keeps the
# conversion, this one turns it into financial metrics and keeps only those: the
# DoclingDocument of a 10-Q is 14 MB and a row is 4 MB, so it is never stored.

EXAMPLE_TITLE="05 — SEC EDGAR XBRL Pipeline"

# The shared statements in common/flink are the chunk pipeline, which has no
# place here. lib.sh's sql_stage skips that directory when this is "no".
USE_COMMON_FLINK="no"

# --- what this example is made of -----------------------------------------

# The Python UDF artifact is required by the setup but this example has no
# Python function in it.
PY_UDF_DIR="common/udf"
ARTIFACT_NAME="${NS}-ex05-udf"

# The Java UDF artifact, this example's own (./udf): dc_metrics, which converts
# a filing with Docling, in-process, and reads the headline figures from the
# tables of the DoclingDocument. The common dc_convert is not used: its result
# would be a row.
JAVA_UDF_DIR="examples/05-xbrl/udf"
JAVA_ARTIFACT_NAME="${NS}-ex05-udf-java"

# --- connections ------------------------------------------------------------
#
# Two, as in example 02: the Docling service (which also holds its API key), and
# the S3 host Docling for IBM watsonx answers from, with a presigned URL.
#
# There is no connection to sec.gov, and that is deliberate. The *service*
# downloads the filing from the SEC, not Flink: the iXBRL report is up to 6 MB,
# a row is 4 MB. dc_metrics tells the service which User-Agent to fetch with
# (SEC_USER_AGENT below); the SEC answers 403 to a request without one.

DOCLING_CONNECTION="${NS}_ex05_docling"
DOWNLOAD_CONNECTION="${NS}_ex05_artifacts"
DOWNLOAD_ENDPOINT="${DOWNLOAD_ENDPOINT:-https://s3.us-east-1.amazonaws.com}"
CONNECTION_VARS="DOCLING_CONNECTION DOWNLOAD_CONNECTION"

# --- topics ----------------------------------------------------------------

# Sizing and retention
PARTITIONS="${PARTITIONS:-1}"
RETENTION="${RETENTION:-1 d}"

# The input: one record per filing, produced by scripts/sec_producer.py — CIK,
# accession number, and the links to the report.
FILINGS_TOPIC="${NS}.ex05.filings"

# The output: one row per metric per filing (total assets, net income, ...), and
# one row for a filing that gave none, with the reason. The converted document is
# not kept anywhere: this is all that is left of the paid conversion.
METRICS_TOPIC="${NS}.ex05.metrics"

# The events: what changed in a filing, one row per event (net income turning
# negative, cash falling sharply, ...), derived from the metrics — so changing
# a threshold below and redeploying costs no conversion.
EVENTS_TOPIC="${NS}.ex05.events"

TOPIC_VARS="FILINGS_TOPIC METRICS_TOPIC EVENTS_TOPIC"

# Every one of them is created by its CREATE TABLE, in ./setup.sh apply. Never
# ahead of it: they are typed, and only CREATE TABLE registers the schema.

# --- SEC EDGAR settings ------------------------------------------------------
#
# User-Agent complying with the SEC Fair Access Policy (Name <email>). Used by
# sec_producer.py, and handed to Docling by dc_metrics for its fetch.
SEC_USER_AGENT="${SEC_USER_AGENT:-DoclingConfluentCloudScraper/1.0 (researcher@example.com; academic research scraper)}"
SEC_FORM_TYPE="${SEC_FORM_TYPE:-10-K,10-Q}"

# --- Flink objects ----------------------------------------------------------

APP_NAME="${NS}-ex05"

VIEW_EXTRACTED="${NS}_ex05_extracted"          # new filings, through dc_metrics
VIEW_FINANCIALS="${NS}_ex05_financials"        # one row per filing, in dollars, with its ratios
VIEW_VARS="VIEW_EXTRACTED VIEW_FINANCIALS"

FN_METRICS="${NS}_ex05_metrics"
FUNCTION_VARS="FN_METRICS"

# --- events -----------------------------------------------------------------
#
# When a filing is an event (32_events.sql). All are fractions, as the ratios
# are: 0.02 is 2 per cent, or 2 percentage points for a margin. Each has a
# "high" severity above which the event is marked so. They are in the
# fingerprint of the events statement: change one and ./pipeline.sh deploy
# replaces that statement, and it re-derives every event from the metrics
# already read — no conversion is paid for again.
EVENT_MARGIN_DROP="${EVENT_MARGIN_DROP:-0.02}"            # gross margin fell by this many points
EVENT_MARGIN_DROP_HIGH="${EVENT_MARGIN_DROP_HIGH:-0.05}"
EVENT_COST_GAP="${EVENT_COST_GAP:-0.01}"                  # costs grew this many points faster than revenue
EVENT_COST_GAP_HIGH="${EVENT_COST_GAP_HIGH:-0.05}"
EVENT_CASH_DROP="${EVENT_CASH_DROP:-0.30}"                # cash fell by this share
EVENT_CASH_DROP_HIGH="${EVENT_CASH_DROP_HIGH:-0.50}"

# --- deploy -------------------------------------------------------------------
#
# `./pipeline.sh deploy` leaves an unchanged statement running (common/
# deploy_plan.py), and when it does replace the statement it sets this to where
# the filings topic ends at that moment:
#
#   FILINGS_FROM_OFFSETS   where the new statement starts reading filings — from
#                          here on: a filing is converted, and billed, once,
#                          whatever you redeploy
#
# The default only lets the SQL render outside a deploy.
DEPLOY_OFFSET_VARS="FILINGS_FROM_OFFSETS:FILINGS_TOPIC"
FILINGS_FROM_OFFSETS="${FILINGS_FROM_OFFSETS:-partition:0,offset:0}"
