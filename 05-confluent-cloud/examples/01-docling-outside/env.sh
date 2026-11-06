# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# Example 01 — every name it creates, all derived from NS.
#
# Sourced by ./setup.sh and ./pipeline.sh, after 05-confluent-cloud/.env (which is
# where NS comes from) and before examples/01-docling-outside/.env (which is
# where this example's setup writes the artifact id back). Committed, unlike
# the other two, because none of it is a secret and all of it is worth reading
# before you run anything: these are the names that land in a namespace shared
# with everyone who can see your Confluent Cloud environment.
#
# `./setup.sh info` prints the resolved list. `./setup.sh destroy --dry-run`
# prints what would be removed.

EXAMPLE_TITLE="01 — Docling outside Flink"

# --- what this example is made of -----------------------------------------

# The Python UDF artifact. Shared with every other example, because every
# example runs the same enrichment — see common/udf/ and common/flink/.
PY_UDF_DIR="common/udf"
ARTIFACT_NAME="${NS}-ex01-udf"

# No Java artifact and no connection: nothing in this example calls out of
# Confluent Cloud. That is the whole difference between it and example 02.

# --- topics ----------------------------------------------------------------
#
# TOPIC_VARS is the list `./setup.sh info` prints and `destroy --topics` drops,
# in that order. Table name == topic name in Confluent Cloud Flink.

CHUNKS_TOPIC="${NS}.ex01.chunks"
ENRICHED_TOPIC="${NS}.ex01.chunks.enriched"
QUARANTINE_TOPIC="${NS}.ex01.pii.quarantine"
REJECTED_TOPIC="${NS}.ex01.quality.rejected"

TOPIC_VARS="CHUNKS_TOPIC ENRICHED_TOPIC QUARANTINE_TOPIC REJECTED_TOPIC"

# --- Flink objects ----------------------------------------------------------

# The prefix on every statement this example submits. Statement names are
# unique per *environment*, and `./pipeline.sh deploy` deletes the long-running
# ones by name before it submits new ones — so this being yours is what keeps
# that delete from reaching somebody else's job.
APP_NAME="${NS}-ex01"

# The two views the pipeline is assembled from. `deploy` DROPs these before
# recreating them, so a name you do not own is a view you destroy.
VIEW_PREPARED="${NS}_ex01_prepared"
VIEW_GUARDED="${NS}_ex01_guarded"

# The three UDFs. Registered per catalog+database — i.e. per environment and
# cluster — so these are shared names too.
FN_IS_CHUNK="${NS}_ex01_is_chunk"
FN_PREPARE="${NS}_ex01_prepare"
FN_GUARD="${NS}_ex01_guard"
