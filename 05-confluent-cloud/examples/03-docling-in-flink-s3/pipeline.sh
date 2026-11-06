#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# The pipeline itself.
#
#   ./pipeline.sh deploy [--force] objects -> presign -> convert -> chunks -> prepare -> guard -> dedup -> 3 topics
#   ./pipeline.sh status           the two statements, and the connector
#   ./pipeline.sh sql '<stmt>'     run one statement and print its rows
#   ./pipeline.sh delete           stop and remove the statements, and the views
#
# Exactly example 02's deploy: only what changed is replaced, and a replaced
# convert statement reads the objects topic from where it ends — no object is
# converted twice — and rebuilds the chunks from the conversions topic.
#
# `delete` leaves the connector running: it is setup, not pipeline, and
# notifications it collects meanwhile are on the topic for the next deploy to
# skip — `scripts/aws.sh backfill --since <then>` after the deploy resends them.
set -euo pipefail
EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$EXAMPLE_DIR/../../common/lib.sh"
. "$REPO_ROOT/common/example.sh"

pipeline_usage() { sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

pipeline_main "$@"
