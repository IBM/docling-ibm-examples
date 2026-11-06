# Example 04 — every name it creates, all derived from NS.
#
# Read example 01's env.sh first for the layering. What is different here is
# that this example does NOT run the shared chunk-enrichment half: there are no
# chunks. It extracts fields from invoices, converts them to EUR and keeps the
# aggregates a spend dashboard reads.

EXAMPLE_TITLE="04 — Invoice extraction, on a simulated clock"

# The shared statements in common/flink are the chunk pipeline, which has no
# place here. lib.sh's sql_stage skips that directory when this is "no".
USE_COMMON_FLINK="no"

# The pipeline is one long-running statement, flink/30_pipeline.sql: from
# `submissions` through the extraction (synthetic or real, per invoice) to
# `extractions`, `invoices`, `spend` and the three reports — the steps chained
# through views, so there is one commit between a release and the charts.
#
# The tables it writes are plain CREATE TABLEs (flink/00_table_*). This
# example's pipeline.sh deploys it its own way: unchanged, it is left running;
# changed, it is replaced, reading `submissions` from where the topic ends and
# `extractions` up to where that one ends — see pipeline.sh.

# --- what this example is made of -----------------------------------------

# Its own Python UDF artifact: the synthetic extraction and the invoice step. Not
# common/udf, because it carries this example's ground truth (see
# udf/compile_truth.py).
PY_UDF_DIR="examples/04-invoice-extraction/udf"
ARTIFACT_NAME="${NS}-ex04-udf"

# --- the extraction: synthetic or real, per invoice --------------------------
#
#   synthetic  invoice_udf.functions.synthetic_extract, in the Python artifact:
#              answers from the ground truth, in Docling's wire format. Free,
#              instant.
#   real       ai.docling.confluent.DoclingExtract, in the Java artifact, bound
#              to the Docling CONNECTION. Billed, seconds per page.
#
# Not a setting: each submission carries its `mode`, chosen when it is released
# — on the control app's page, beside the timeline — and the statement sends it
# to one function or the other (flink/21_view_extracted.sql).

# The real call is Java (Python UDFs cannot reach the network, docs/05-confluent-cloud/LIMITS.md):
# the repo's shared Java artifact, which holds DoclingConvert (example 02) and
# DoclingExtract (this one). Uploaded under this example's name.
JAVA_UDF_DIR="common/udf-java"
JAVA_ARTIFACT_NAME="${NS}-ex04-udf-java"

# Two connections, as in example 02. The service, with the key; and the host
# its answers live on — Docling for IBM watsonx returns a presigned S3 URL to
# the result, never the result, and a UDF reaches only hosts it is bound to.
# The PDFs are not on that list: Docling fetches them, not the UDF.
DOCLING_CONNECTION="${NS}_ex04_docling"
DOWNLOAD_CONNECTION="${NS}_ex04_artifacts"
DOWNLOAD_ENDPOINT="${DOWNLOAD_ENDPOINT:-https://s3.us-east-1.amazonaws.com}"
CONNECTION_VARS="DOCLING_CONNECTION DOWNLOAD_CONNECTION"

# What a fresh `./pipeline.sh deploy` starts with: the first SEED_DAYS days of
# the simulation (5–7 January: 21 invoices), released once the statements run,
# in synthetic mode, so every table and the dashboard have something in them and
# the rest of the half-year is left to release from the control app. Only when
# nothing has been released yet — a redeploy never ingests more. 0 seeds nothing.
SEED_DAYS="${SEED_DAYS:-3}"

# The PDFs, for Docling to fetch: a private Hugging Face Storage Bucket. The
# control app and scripts/extract.py mint a signed URL (valid one hour) per
# real invoice as it is released; see scripts/sources.py. Empty: real mode is
# unavailable, and every invoice carries the manifest's localhost URL, which
# only the synthetic extraction can use.
INVOICE_BUCKET="${INVOICE_BUCKET:-docling-project/docling-confluent-ex04}"

# --- topics ----------------------------------------------------------------
#
# One partition each, whatever .env says. The simulation's clock is the order
# of the submissions topic, and that order exists only within a partition.
PARTITIONS=1

# Infinite retention, whatever .env says — and this one is not optional. Kafka
# expires records by their *timestamp*, and every timestamp here is simulated:
# January to June 2026. With the repo's default of "1 d", the broker would
# delete each segment as soon as it rolled, because by the record's own clock it
# is months old. "0" is Flink's spelling of infinite.
RETENTION="0"

# Every topic carries a registered JSON Schema (json-registry) and a raw key:
# the dashboard decodes by the schema, the Real-Time Context Engine requires
# one, and the engine's upsert mode requires the raw key.

# In: typed tables the control app writes, on the simulated clock.
SUBMISSIONS_TOPIC="${NS}.ex04.submissions"   # doc_id, url, department, category, submitted_at, mode
FX_TOPIC="${NS}.ex04.fx.rates"               # currency, rate_date, rate, ecb_date — one per day

# Through: one row per invoice.
EXTRACTIONS_TOPIC="${NS}.ex04.extractions"   # the submission + what Docling answered (append)
INVOICES_TOPIC="${NS}.ex04.invoices"         # merged, checked, typed; status ok | review (upsert)
SPEND_TOPIC="${NS}.ex04.spend"               # an ok invoice, in EUR (upsert)

# Out: the reports the dashboard reads, all upsert.
DAILY_TOPIC="${NS}.ex04.spend.daily"         # per submission day and department
MONTHLY_TOPIC="${NS}.ex04.spend.monthly"     # per invoice month, department, category
VENDORS_TOPIC="${NS}.ex04.spend.vendors"     # per vendor

TOPIC_VARS="SUBMISSIONS_TOPIC FX_TOPIC EXTRACTIONS_TOPIC INVOICES_TOPIC SPEND_TOPIC DAILY_TOPIC MONTHLY_TOPIC VENDORS_TOPIC"

# Flink creates every topic here, by CREATE TABLE in ./setup.sh apply, which
# also registers its schema. A topic created ahead of Flink would be inferred as
# plain `key BYTES, val BYTES` with no schema, and CREATE TABLE IF NOT EXISTS
# would keep that.

# The topics the Real-Time Context Engine serves, for lightning queries:
# `./setup.sh context-engine on` after the first deploy. The per-invoice ones
# for lookups by doc_id or vendor; the two upsert reports for the latest value
# per key.
RTCE_TOPIC_VARS="EXTRACTIONS_TOPIC INVOICES_TOPIC SPEND_TOPIC MONTHLY_TOPIC VENDORS_TOPIC"

# --- Flink objects ----------------------------------------------------------

APP_NAME="${NS}-ex04"

# The views the statement is made of, in order. Named unlike any topic:
# Confluent treats `.` and `_` alike in names, so `${NS}_ex04_invoices` would be
# the topic.
VIEW_SUBMITTED="${NS}_ex04_submitted"          # new submissions + the extraction target
VIEW_EXTRACTED="${NS}_ex04_extracted"          # ...through extract_real() or extract_synthetic()
VIEW_EXTRACTED_ALL="${NS}_ex04_extracted_all"  # ...plus the extractions of earlier deploys
VIEW_INVOICED="${NS}_ex04_invoiced"            # ...through invoice(), unpacked
VIEW_CONVERTED="${NS}_ex04_converted"          # ok invoices joined with the FX rate, in EUR
VIEW_VARS="VIEW_SUBMITTED VIEW_EXTRACTED VIEW_EXTRACTED_ALL VIEW_INVOICED VIEW_CONVERTED"

# Where the statement starts reading `submissions`, and where it stops reading
# the earlier `extractions` — both the topic's end when it is deployed.
# ./pipeline.sh deploy sets them; these defaults only let the SQL render
# elsewhere (the dashboard's SQL view, scripts/diagram.py).
SUBMISSIONS_FROM_OFFSET="${SUBMISSIONS_FROM_OFFSET:-0}"
EXTRACTIONS_UNTIL_OFFSET="${EXTRACTIONS_UNTIL_OFFSET:-0}"

# Two extractions, one signature — extract(docling_conn, artifact_conn, url,
# target). The synthetic one takes the two connection names and ignores them.
FN_EXTRACT_SYNTHETIC="${NS}_ex04_extract_synthetic"  # Python, invoice_udf.functions.synthetic_extract
FN_EXTRACT_REAL="${NS}_ex04_extract_real"            # Java, ai.docling.confluent.DoclingExtract
FN_INVOICE="${NS}_ex04_invoice"
FUNCTION_VARS="FN_EXTRACT_SYNTHETIC FN_EXTRACT_REAL FN_INVOICE"

# --- knobs ------------------------------------------------------------------

# lib.sh hands this to every statement as `sql.state-ttl`. Here it bounds the
# FX join and the three report aggregates, which must outlive a demo paused
# over lunch. Six months of invoices is a few hundred rows of state.
DEDUP_TTL="30 d"

# The reporting currency. The ECB publishes rates against the euro, so this is
# the one choice that needs no cross rates.
REPORTING_CURRENCY="EUR"
