#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# The pipeline itself: one long-running statement, flink/30_pipeline.sql.
#
#   ./pipeline.sh deploy           (re)start the statement if it changed, then the seed
#   ./pipeline.sh seed             the first SEED_DAYS days (3), if nothing is released yet
#   ./pipeline.sh status           is it running, and what is it doing
#   ./pipeline.sh sql '<stmt>'     run one statement and print its rows
#   ./pipeline.sh delete           stop the statement (tables and data kept)
#
# `deploy` is this example's own, not the shared one, because of what the
# statement costs to restart — it calls Docling. The shared deploy deletes and
# resubmits every statement; here
#
#   * unchanged — same SQL, views and functions, by a fingerprint it carries
#     (scripts/deploy_plan.py) — it is left running;
#   * changed, the old one is stopped, and the new one reads `submissions` from
#     where the topic ends then (nothing is extracted twice) and the earlier
#     `extractions` up to where that topic ends (every upsert table is rebuilt
#     from them, for free).
#
# Then it seeds: on a clock that has not moved, the first SEED_DAYS days, in
# synthetic mode. SEED_DAYS=0 skips it.
set -euo pipefail
EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$EXAMPLE_DIR/../../common/lib.sh"
. "$REPO_ROOT/common/example.sh"

pipeline_usage() { sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

# submit_long_running <name> <sql>
submit_long_running() {
  local name="$1" sql="$2" props payload out
  props=$(statement_properties)
  payload=$(jq -n --arg name "$name" --arg sql "$sql" --arg pool "$CC_COMPUTE_POOL_ID" --argjson props "$props" \
    '{name: $name, spec: {statement: $sql, compute_pool_id: $pool, properties: $props, stopped: false}}')
  out=$(flink_api POST /statements "$payload")
  flink_ok "$out" "submitting $name" >/dev/null
  await_statement "$name" RUNNING
}

deploy() {
  need curl jq uv
  load_env
  require_vars CC_COMPUTE_POOL_ID CC_CATALOG CC_DATABASE ARTIFACT_ID JAVA_ARTIFACT_ID

  local plan
  plan=$(uv run --quiet --project "$REPO_ROOT" python "$EXAMPLE_DIR/scripts/deploy_plan.py")

  step "Statements"
  local item name fp out status body replace="" keep_views="" keep_names=""
  while IFS= read -r item; do
    [ -n "$item" ] || continue
    name=$(printf '%s' "$item" | jq -r .name)
    fp=$(printf '%s' "$item" | jq -r .fingerprint)
    out=$(flink_api GET "/statements/$name"); status="${out##*$'\n'}"; body="${out%$'\n'*}"
    if [ "$status" -lt 400 ] \
        && [ "$(printf '%s' "$body" | jq -r .status.phase)" = "RUNNING" ] \
        && printf '%s' "$body" | jq -r .spec.statement | grep -q "fingerprint: $fp"; then
      say "    $name: unchanged, left running"
      keep_names="$keep_names $name"
      keep_views="$keep_views $(printf '%s' "$item" | jq -r '.views[]' | tr '\n' ' ')"
    else
      say "    $name: $([ "$status" -lt 400 ] && echo "changed or not running — replacing" || echo "new")"
      replace="$replace$item"$'\n'
    fi
  done <<EOF
$plan
EOF

  # A running statement of this example that the plan no longer has — the two
  # extraction statements of the previous version, say — is stopped: left
  # running, it would keep writing beside the new one.
  local planned stale
  planned=" $(printf '%s\n' "$plan" | jq -r .name | tr '\n' ' ') "
  for stale in $(list_statements \
      | jq -r --arg p "$APP_NAME-" 'select(.name|startswith($p)) | select(.status.phase=="RUNNING") | .name'); do
    case "$planned" in *" $stale "*) continue ;; esac
    say "    $stale: no longer part of the pipeline — stopping"
    delete_statement_if_present "$stale"
  done

  if [ -n "$replace" ]; then
    while IFS= read -r item; do
      [ -n "$item" ] || continue
      delete_statement_if_present "$(printf '%s' "$item" | jq -r .name)"
    done <<EOF
$replace
EOF

    # Where the topics end now that nothing writes `extractions`: the new
    # statement extracts `submissions` from here on, and rebuilds the reports
    # from `extractions` up to here. Then the plan again, rendered with them.
    local assign
    assign=$(uv run --quiet --project "$REPO_ROOT" python "$EXAMPLE_DIR/scripts/deploy_plan.py" offsets)
    say "    reading submissions from, and extractions up to: $assign"
    # shellcheck disable=SC2086
    export $assign
    plan=$(uv run --quiet --project "$REPO_ROOT" python "$EXAMPLE_DIR/scripts/deploy_plan.py")
    replace=""
    while IFS= read -r item; do
      [ -n "$item" ] || continue
      case " $keep_names " in *" $(printf '%s' "$item" | jq -r .name) "*) ;; *) replace="$replace$item"$'\n' ;; esac
    done <<EOF
$plan
EOF

    # The views the replaced statements read, minus those a running one still
    # holds (unchanged by construction: their fingerprint matched). Dropped
    # downstream first, created upstream first — the file order.
    local views v f
    views=$(printf '%s' "$replace" | jq -r '.views[]' | sort -u)
    for v in $keep_views; do views=$(printf '%s\n' "$views" | grep -vxF "$v" || true); done
    if [ -n "$views" ]; then
      step "Views"
      for v in $(printf '%s\n' "$views" | sort -r); do
        f=$(ls "$EXAMPLE_DIR"/flink/90_drop_view_*_"$(basename "$v" .sql | sed 's/^[0-9]*_view_//')".sql 2>/dev/null | head -1)
        if [ -n "$f" ]; then ( run_sql_file "$f" "$(sql_file_name "$f")" ) || true; fi
      done
      for v in $(printf '%s\n' "$views" | sort); do run_sql_file "$v" "$(sql_file_name "$v")"; done
    fi

    step "Starting"
    while IFS= read -r item; do
      [ -n "$item" ] || continue
      name=$(printf '%s' "$item" | jq -r .name)
      say "  $name"
      submit_long_running "$name" "$(printf '%s' "$item" | jq -r .sql)"
    done <<EOF
$replace
EOF
  fi

  say ""
  dim "  ./pipeline.sh status          what it is doing"
  dim "  ./pipeline.sh delete          stop it (tables and data kept)"
}

seed() {
  step "Seeding the simulation"
  uv run --quiet --project "$REPO_ROOT" "$EXAMPLE_DIR/scripts/control.py" seed
}

case "${1:-}" in
  deploy) shift; [ $# -eq 0 ] || die "deploy takes no arguments"; deploy; seed ;;
  seed)   seed ;;
  *)      pipeline_main "$@" ;;
esac
