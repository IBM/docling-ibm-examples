#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# The pipeline itself.
#
#   ./pipeline.sh deploy [--force]   filings -> Docling -> metrics -> events
#   ./pipeline.sh status             is it running, and what is it doing
#   ./pipeline.sh sql '<stmt>'       run one statement and print its rows
#   ./pipeline.sh delete             stop and remove it, and the views
#
# `deploy` replaces, it does not add — and it replaces only what changed. Each
# statement carries a fingerprint of its SQL, its views and its functions
# (common/deploy_plan.py); a running one that matches is left alone, one that
# does not is deleted before its successor starts. `deploy --force` replaces
# everything.
#
# Here that matters: the metrics statement calls Docling. When it is replaced it
# reads the filings topic from where it ends, so no filing is converted twice.
# The conversions are not kept (a 10-Q's DoclingDocument does not fit a 4 MB
# row), so only the metrics survive a redeploy. The events statement does not
# call Docling: it derives the events from the metrics, so a new threshold
# replaces it for free.
set -euo pipefail
EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$EXAMPLE_DIR/../../common/lib.sh"
. "$REPO_ROOT/common/example.sh"

pipeline_usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

pipeline_main "$@"
