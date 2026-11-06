#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# The pipeline itself.
#
#   ./pipeline.sh deploy [--force] chunks -> prepare -> guard -> dedup -> 3 topics
#   ./pipeline.sh status           is it running, and what is it doing
#   ./pipeline.sh sql '<stmt>'     run one statement and print its rows
#   ./pipeline.sh delete           stop and remove it, and the views
#
# `deploy` replaces, it does not add — and it replaces only what changed. Each
# statement carries a fingerprint of its SQL, its views and its functions
# (common/deploy_plan.py); a running one that matches is left alone, one that
# does not is deleted before its successor starts. Two statements reading the
# same topic with the same consumer group would split its partitions and each
# see half the data. `deploy --force` replaces everything.
set -euo pipefail
EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$EXAMPLE_DIR/../../common/lib.sh"
. "$REPO_ROOT/common/example.sh"

pipeline_usage() { sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

pipeline_main "$@"
