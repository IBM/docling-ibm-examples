#!/usr/bin/env bash
# What Terraform cannot say in HCL: a DROP. Called by the provisioners in
# terraform/modules/example — before a function or a connection is created, and
# when one is destroyed — never by hand.
#
#   tf-hook.sh <example-dir> run <file.sql>          one statement, e.g. a 91_drop_function_*.sql
#   tf-hook.sh <example-dir> drop-connection <name>  delete a connection, if it is there
#
# It loads the example's configuration itself, the way setup.sh does, so a
# provisioner needs nothing from the process that ran Terraform.
set -euo pipefail
# shellcheck disable=SC2034  # read by lib.sh
EXAMPLE_DIR="${1:?usage: tf-hook.sh <example-dir> run <file.sql> | drop-connection <name>}"
shift
. "$(cd "$(dirname "$0")" && pwd)/lib.sh"
need curl jq
load_env

case "${1:-}" in
  run)
    [ -f "${2:-}" ] || die "tf-hook.sh run: no such file ${2:-}"
    run_sql_file "$2" "$(sql_file_name "$2")" ;;
  drop-connection)
    [ -n "${2:-}" ] || die "tf-hook.sh drop-connection: which one?"
    delete_connection_if_present "$2"
    say "    $2: dropped, if it was there" ;;
  *) die "tf-hook.sh: run or drop-connection" ;;
esac
