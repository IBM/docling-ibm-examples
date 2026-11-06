#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# Everything that happens once, before this example's pipeline can be deployed.
# The AWS side comes first and is not this script's: scripts/aws.sh, AWS.md.
# It is Terraform's — terraform/main.tf, beside this file, on
# ../../terraform/modules/example — and this script is how it is run:
#
#   ./setup.sh info        every name this example would touch
#   ./setup.sh plan        build the artifacts, and show what Terraform would do
#   ./setup.sh apply       build, apply (artifacts, connections, tables and their
#                          topics, functions), and write the artifact ids into .env
#   ./setup.sh tf <args>   any other terraform command: `tf state list`, `tf output`
#   ./setup.sh destroy     remove it all again (--dry-run first; --topics also
#                          drops the tables, and with them the topics' data)
#
# The account — credentials, ids, NS, keys — comes first and is not this
# script's business: see ../../account.sh. What this example names is env.sh.
set -euo pipefail
EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$EXAMPLE_DIR/../../common/lib.sh"
. "$REPO_ROOT/common/example.sh"

setup_usage() { sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

setup_main "$@"
