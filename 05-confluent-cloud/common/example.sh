# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# The commands every example's setup.sh and pipeline.sh are built from.
# Sourced by them, after lib.sh.
#
# An example contributes three things and no code:
#
#   env.sh      the names — topics, views, functions, artifacts — all from NS
#   flink/      the statements that are its own; the rest come from common/flink
#   README.md   what it demonstrates
#
# Which means the two verbs below mean the same thing in every example, and
# `./setup.sh destroy` removes an example's objects without knowing which
# example it is.

# --- setup: the things that happen once ------------------------------------
#
# Terraform's, in examples/<n>/terraform on terraform/modules/example: the
# artifacts, the connections, the tables (and so the topics), the functions,
# and whatever an example adds — example 03's connector, example 04's Context
# Engine. What is left here is what Terraform cannot do for itself: build the
# artifacts it uploads, hand it the configuration, and write back what the
# pipeline needs.

tf_dir() { printf '%s/terraform' "$EXAMPLE_DIR"; }

# build_artifacts -> {"python": "<abs path>", "java": "<abs path>"} on stdout.
#
# Every plan builds, because Terraform decides whether to upload from the
# *sources* (terraform/modules/example), and the file it uploads has to be the
# one those sources make. Unchanged sources: built, and not uploaded.
build_artifacts() {
  need uv zip
  local py java=""
  py=$(_build "$PY_UDF_DIR")
  if [ -n "${JAVA_UDF_DIR:-}" ]; then
    need java
    java=$(_build "$JAVA_UDF_DIR")
  fi
  jq -n --arg py "$py" --arg java "$java" '{python: $py} + (if $java == "" then {} else {java: $java} end)'
}

# _build <dir> -> the absolute path its build.sh produced. Everything for a
# human goes to stderr: stdout is the JSON above.
_build() {
  local dir="$REPO_ROOT/$1" built
  step "Building $1" >&2
  # No `| head -1`: build.sh prints one line on stdout by contract, and piping
  # it into head closes the pipe and SIGPIPEs the build.
  built=$("$dir/build.sh")
  [ -f "$dir/$built" ] && built="$dir/$built"
  [ -f "$built" ] || die "$1/build.sh did not produce an artifact (got: ${built:-nothing})"
  say "    $built" >&2
  printf '%s' "$built"
}

# tf_example <terraform args...> — tf in this example's terraform/, with the
# artifacts as built (or, for a destroy, as nothing: there is nothing to upload).
tf_example() {
  local files="$1"; shift
  TF_VAR_artifact_files="$files" tf "$(tf_dir)" "$@"
}

cmd_plan() {
  need curl jq
  load_env
  local files; files=$(build_artifacts)
  step "Plan for $EXAMPLE_TITLE"
  tf_example "$files" plan "$@"
}

# apply [terraform flags] — and then the ids the pipeline needs, into the
# example's .env.
cmd_apply() {
  need curl jq
  load_env
  require_vars CC_ORG_ID FLINK_API_KEY CC_PRINCIPAL_ID
  local files; files=$(build_artifacts)
  step "Applying $EXAMPLE_TITLE"
  tf_example "$files" apply "$@"
  write_back
  say ""
  dim "  Next: ./pipeline.sh deploy"
}

# The module's `env` output — the artifact ids and versions — into the
# example's .env, where ./pipeline.sh reads them. --force because Terraform
# owns them now: a value there is only ever its own previous answer.
write_back() {
  step "Writing $(basename "$EXAMPLE_DIR")/.env"
  local pairs=()
  while IFS= read -r line; do
    [ -n "$line" ] && pairs+=("$line")
  done < <(TF_VAR_artifact_files='{}' tf "$(tf_dir)" output -json env | jq -r 'to_entries[] | "\(.key)=\(.value)"')
  [ ${#pairs[@]} -eq 0 ] || set_example_env --force "${pairs[@]}"
}

# The Real-Time Context Engine, on the topics in RTCE_TOPIC_VARS: a switch in
# the example's .env, which its terraform/main.tf reads. Opt-in, billed while
# on, and best after `./pipeline.sh deploy`, so the tables have rows to serve.
cmd_context_engine() {
  need curl jq
  load_env
  [ -n "${RTCE_TOPIC_VARS:-}" ] || die "this example lists no RTCE_TOPIC_VARS"
  case "${1:-status}" in
    on|off)
      set_example_env --force "CONTEXT_ENGINE=$1"
      shift
      cmd_apply "$@"
      [ "${CONTEXT_ENGINE:-}" != "on" ] || dim "  Provisioning takes several minutes; ./setup.sh context-engine status shows when each is ACTIVE." ;;
    status)
      rtce_list | jq -r '.data[]? | "    \(.spec.topic_name)  \(.status.phase)"' ;;
    *) die "context-engine: on, off or status" ;;
  esac
}

# tf <terraform args> — anything else: `./setup.sh tf state list`,
# `./setup.sh tf output`, an import. Nothing is built.
cmd_tf() {
  load_env
  tf_example '{}' "$@"
}

# The values behind a list of variable names, comma-separated, skipping unset
# ones: `names_of "VIEW_PREPARED VIEW_GUARDED"` -> "dol_ex01_prepared, ...".
names_of() {
  local var out=""
  for var in $1; do
    [ -n "${!var:-}" ] && out="${out:+$out, }${!var}"
  done
  printf '%s' "$out"
}

# What an example names, for info and destroy. VIEW_VARS / FUNCTION_VARS in its
# env.sh when it has them; otherwise the chunk pipeline's, which is every
# example that runs common/flink.
view_names()     { names_of "${VIEW_VARS:-VIEW_CONVERTED VIEW_PREPARED VIEW_GUARDED}"; }
function_names() { names_of "${FUNCTION_VARS:-FN_IS_CHUNK FN_PREPARE FN_GUARD FN_CONVERT FN_CHUNKS}"; }

cmd_info() {
  load_env
  cat <<INFO

  example       $EXAMPLE_TITLE
                ${EXAMPLE_DIR#"$REPO_ROOT"/}

  environment   ${CC_ENVIRONMENT_ID:-<unset>}   catalog  ${CC_CATALOG:-<unset>}
  cluster       ${CC_CLUSTER_ID:-<unset>}       database ${CC_DATABASE:-<unset>}
  compute pool  ${CC_COMPUTE_POOL_ID:-<unset>}  region   $CC_CLOUD/$CC_REGION

  artifacts     python  ${ARTIFACT_ID:-<unset>} / ${ARTIFACT_VERSION:-<unset>}   ($ARTIFACT_NAME-<hash>)
INFO
  [ -n "${JAVA_UDF_DIR:-}" ] && cat <<INFO
                java    ${JAVA_ARTIFACT_ID:-<unset>} / ${JAVA_ARTIFACT_VERSION:-<unset>}   ($JAVA_ARTIFACT_NAME-<hash>)
INFO
  cat <<INFO

  topics        $PARTITIONS partition(s), retention $RETENTION, replication 3 (fixed)
INFO
  local var
  for var in $TOPIC_VARS; do printf '                %-12s %s\n' "$(printf '%s' "${var%_TOPIC}" | tr 'A-Z_' 'a-z-')" "${!var}"; done
  for var in ${CONNECTION_VARS:-}; do printf '  connection    %s\n' "${!var}"; done
  for var in ${CONNECTOR_VARS:-}; do printf '  connector     %s   (billed while it runs)\n' "${!var}"; done
  cat <<INFO

  statements    $(long_running_names | tr '\n' ' ')
  views         $(view_names)
  functions     $(function_names)
  terraform     ${EXAMPLE_DIR#"$REPO_ROOT"/}/terraform   (./setup.sh tf state list)
INFO
  if [ "${USE_COMMON_FLINK:-yes}" != "no" ]; then
    cat <<INFO

  pipeline      min_chars=$MIN_CHARS drop_low_quality=$DROP_LOW_QUALITY
                pii=[$PII_DETECTORS] redact=$PII_REDACT blocked=[${BLOCKED_DOC_IDS}]
                dedup state ttl=$DEDUP_TTL
INFO
  else
    printf '\n  state ttl     %s\n' "$DEDUP_TTL"
  fi
  say ""
}

# --- destroy ---------------------------------------------------------------
#
# Everything this example creates, removed in dependency order. Two tiers,
# because one of them destroys data that does not come back:
#
#   ./setup.sh destroy                 the pipeline (statements, views), then
#                                      `terraform destroy`: the Context Engine,
#                                      the connector, the functions, the
#                                      connections, the artifacts. The topics
#                                      and their data stay — a table is never
#                                      Terraform's to drop. ./setup.sh apply
#                                      puts the rest back.
#   ./setup.sh destroy --topics        ...and DROP TABLE on every topic, which
#                                      in Confluent Cloud DELETES THE TOPICS AND
#                                      THEIR DATA.
#   ./setup.sh destroy --dry-run       print the plan and stop.
#
# Only ever names derived from NS, so it cannot reach anyone else's objects —
# and the leftover-statement sweep matches "$APP_NAME-*", never the `i-s-*` and
# `workspace-*` statements the Confluent Cloud SQL workspace leaves behind.
cmd_destroy() {
  need curl jq
  load_env
  local want_topics="no" dry="no" assume_yes="no"
  while [ $# -gt 0 ]; do
    case "$1" in
      --topics)     want_topics="yes" ;;
      # The tiers before Terraform; both now mean what plain destroy does.
      --artifact|--everything) [ "$1" = "--everything" ] && want_topics="yes" ;;
      --dry-run)    dry="yes" ;;
      --yes)        assume_yes="yes" ;;
      *) die "destroy: unknown flag $1 (--topics, --dry-run, --yes)" ;;
    esac
    shift
  done

  # --- work out what is actually there, before proposing to remove it -------
  local running="" name out status
  for name in $(long_running_names); do
    out=$(flink_api GET "/statements/$name"); status="${out##*$'\n'}"
    [ "$status" -lt 400 ] && running="$running $name"
  done
  # Context Engine topics of this example's that Terraform does not hold — left
  # on by the scripts before it. Its own it turns off in the destroy below.
  local rtce_left="" managed var
  if [ -n "${RTCE_TOPIC_VARS:-}" ]; then
    managed=" $(tf_example '{}' state list 2>/dev/null | grep -c rtce_topic || true) "
    local on
    on=" $(rtce_list 2>/dev/null | jq -r '.data[]?.spec.topic_name' | tr '\n' ' ') "
    [ "$managed" = " 0 " ] || on=""   # with CONTEXT_ENGINE=on they are all Terraform's
    for var in $RTCE_TOPIC_VARS; do
      case "$on" in *" ${!var} "*) rtce_left="$rtce_left ${!var}" ;; esac
    done
  fi

  # --- the plan -------------------------------------------------------------
  step "Plan for $EXAMPLE_TITLE"
  if [ -n "$running" ]; then
    for name in $running; do say "    stop + delete statement   $name"; done
  else
    say "    (no running statement)"
  fi
  say "    drop views                $(view_names)"
  say "    terraform destroy         every resource in ${EXAMPLE_DIR#"$REPO_ROOT"/}/terraform:"
  tf_example '{}' state list 2>/dev/null | sed 's/^/                                /' || true
  for name in $rtce_left; do say "    context engine off        $name (not Terraform's)"; done
  if [ "$want_topics" = "yes" ]; then
    printf '%s\n' "    ${C_RED}DROP TABLE${C_OFF}                (topic, and everything in it)"
    for var in $TOPIC_VARS; do say "                              ${!var}"; done
    warn "those are topics. Dropping them deletes the data in them, permanently."
  fi

  if [ "$dry" = "yes" ]; then
    say ""
    dim "  --dry-run: nothing was touched. ./setup.sh tf plan -destroy shows Terraform's half in full."
    return 0
  fi

  if [ "$assume_yes" != "yes" ]; then
    say ""
    local expected="destroy" reply
    [ "$want_topics" = "yes" ] && expected="delete the data"
    printf '    Type "%s" to continue: ' "$expected"
    read -r reply
    [ "$reply" = "$expected" ] || die "cancelled"
  fi

  # --- and the order matters ------------------------------------------------
  # A running statement holds the views; a view holds the functions; a function
  # holds the connection it was bound to. Backwards through that chain, or each
  # drop fails on a dependency that is still live — and Terraform knows only
  # the second half of it.
  if [ -n "$running" ]; then
    step "Statements"
    for name in $running; do
      delete_statement_if_present "$name"
      say "    deleted $name"
    done
  fi

  step "Views"
  drop_views

  step "Terraform"
  tf_example '{}' destroy -auto-approve
  set_example_env --force ARTIFACT_ID= ARTIFACT_VERSION= ${JAVA_UDF_DIR:+JAVA_ARTIFACT_ID= JAVA_ARTIFACT_VERSION=} >/dev/null

  if [ -n "$rtce_left" ]; then
    step "Context Engine"
    for name in $rtce_left; do rtce_off_if_present "$name"; say "    off  $name"; done
  fi

  if [ "$want_topics" = "yes" ]; then
    step "Tables (and their topics)"
    run_stage_best_effort 92_drop_table_
  fi

  # Last, because the drops above each add one more of these.
  step "Leftover DDL statements"
  local n=0
  for name in $(list_statements | jq -r --arg p "$APP_NAME-" 'select(.name|startswith($p)) | .name'); do
    delete_statement_if_present "$name"
    n=$((n + 1))
  done
  say "    deleted $n"

  say ""
  if [ "$want_topics" = "yes" ]; then
    dim "  Gone. ./setup.sh apply rebuilds everything from scratch."
  else
    dim "  The topics and their data are untouched. ./setup.sh apply && ./pipeline.sh deploy"
    dim "  puts the pipeline back."
  fi
}

# --- pipeline: the things you do over and over ------------------------------

# deploy [--force]
#
# Leaves what has not changed running. Each long-running statement carries, as
# its first line, a fingerprint of its SQL, the views it reads and the
# functions they call (common/deploy_plan.py); a RUNNING statement with the
# same fingerprint is left alone, and only what changed is replaced — with its
# views. Example 04 learned why: a statement that calls Docling pays for every
# record it reads again, so a restart that changes nothing is not free.
#
# Before a replaced statement starts, the offsets an example names in
# DEPLOY_OFFSET_VARS are set to where their topics end *then* — after the old
# statement has stopped writing — so its SQL can start reading an input "from
# here on" and bound a re-read of its own output "up to here".
#
# --force replaces every statement whatever its fingerprint.
cmd_deploy() {
  need curl jq uv
  load_env
  local force="no"
  case "${1:-}" in
    --force) force="yes" ;;
    "")      ;;
    *)       die "deploy: unknown flag $1 (only --force)" ;;
  esac
  require_vars CC_COMPUTE_POOL_ID CC_CATALOG CC_DATABASE ARTIFACT_ID

  local plan
  plan=$(deploy_plan) || die "could not render the plan"

  step "Statements"
  local item name fp out status body replace="" keep_names="" keep_views=""
  while IFS= read -r item; do
    [ -n "$item" ] || continue
    name=$(printf '%s' "$item" | jq -r .name)
    fp=$(printf '%s' "$item" | jq -r .fingerprint)
    out=$(flink_api GET "/statements/$name"); status="${out##*$'\n'}"; body="${out%$'\n'*}"
    if [ "$force" = "no" ] && [ "$status" -lt 400 ] \
        && [ "$(printf '%s' "$body" | jq -r .status.phase)" = "RUNNING" ] \
        && printf '%s' "$body" | jq -r .spec.statement | head -1 | grep -qx -- "-- fingerprint: $fp"; then
      say "    $name: unchanged, left running"
      keep_names="$keep_names $name "
      keep_views="$keep_views $(printf '%s' "$item" | jq -r '.views[].name' | tr '\n' ' ')"
    else
      if [ "$status" -ge 400 ]; then say "    $name: new"
      elif [ "$force" = "yes" ]; then say "    $name: --force — replacing"
      else say "    $name: changed or not running — replacing"; fi
      replace="$replace$name "
    fi
  done <<EOF
$plan
EOF

  # A running statement of this example that the plan no longer has — renamed
  # or removed — is stopped: left running, it would keep writing beside the new
  # one. Ad hoc `./pipeline.sh sql` statements are not this deploy's business.
  local planned stale
  planned=" $(printf '%s\n' "$plan" | jq -r .name | tr '\n' ' ') "
  for stale in $(list_statements | jq -r --arg p "$APP_NAME-" \
      'select(.name|startswith($p)) | select(.status.phase=="RUNNING") | .name'); do
    case "$planned" in *" $stale "*) continue ;; esac
    case "$stale" in "$APP_NAME"-adhoc-*) continue ;; esac
    say "    $stale: no longer part of the pipeline — stopping"
    delete_statement_if_present "$stale"
  done

  if [ -z "$replace" ]; then
    say ""
    dim "  Nothing changed. ./pipeline.sh deploy --force restarts it anyway."
    return 0
  fi

  for name in $replace; do delete_statement_if_present "$name"; done
  for name in $replace; do wait_statement_gone "$name"; done

  # Now that nothing of ours writes, where the topics end — and the plan again,
  # rendered with those offsets.
  if [ -n "${DEPLOY_OFFSET_VARS:-}" ]; then
    local line
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      export "${line?}"
      say "    ${line%%=*}  ${line#*=}"
    done <<EOF
$(uv run --quiet --project "$REPO_ROOT" python "$REPO_ROOT/common/deploy_plan.py" offsets)
EOF
    plan=$(deploy_plan) || die "could not render the plan"
  fi

  # The views the replaced statements read, minus those a kept one still holds
  # (unchanged by construction: its fingerprint matched). Dropped downstream
  # first, created upstream first — the file order. Dropped and recreated
  # rather than CREATE OR REPLACE: a view embeds the configured values as
  # literals, so a view that is not recreated is yesterday's policy.
  local views v file
  # Lines of "<file name>|<path>|<view>", so sorting orders by file number
  # whichever directory — the example's or common/ — a view came from.
  views=$(printf '%s\n' "$plan" | jq -r --arg r " $replace" \
      '. as $s | select($r | contains(" " + $s.name + " ")) | .views[] | "\(.file | split("/") | last)|\(.file)|\(.name)"' \
      | sort -u)
  for v in $keep_views; do views=$(printf '%s\n' "$views" | grep -v "|$v\$" || true); done
  if [ -n "$views" ]; then
    step "Views"
    for v in $(printf '%s\n' "$views" | sort -r | cut -d'|' -f3); do
      ( submit_statement "$(ddl_name drop-view)" "DROP VIEW IF EXISTS \`$v\`" ) >/dev/null || true
    done
    for file in $(printf '%s\n' "$views" | sort | cut -d'|' -f2); do
      run_sql_file "$file" "$(sql_file_name "$file")"
    done
  fi

  step "Starting"
  local sql
  while IFS= read -r item; do
    [ -n "$item" ] || continue
    name=$(printf '%s' "$item" | jq -r .name)
    case " $replace" in *" $name "*) ;; *) continue ;; esac
    sql=$(printf '%s' "$item" | jq -r .sql)
    say "  $(printf '%s' "$item" | jq -r .file | sed "s|^$REPO_ROOT/||") -> $name"
    submit_statement "$name" "$sql" RUNNING
  done <<EOF
$plan
EOF

  say ""
  dim "  ./pipeline.sh status          what they are doing"
  dim "  ./pipeline.sh sql '<stmt>'    one statement, with its rows"
}

# The plan, one JSON line per long-running statement — see common/deploy_plan.py.
deploy_plan() {
  uv run --quiet --project "$REPO_ROOT" python "$REPO_ROOT/common/deploy_plan.py"
}

cmd_status() {
  need curl jq
  load_env
  local name out status found=0
  for name in $(long_running_names); do
    out=$(flink_api GET "/statements/$name"); status="${out##*$'\n'}"
    if [ "$status" = "404" ]; then
      say "  $name: not deployed"
      continue
    fi
    found=1
    printf '%s' "${out%$'\n'*}" | jq -r '
      "\n  \(.name)
    phase    \(.status.phase)
    pool     \(.spec.compute_pool_id)
    version  \(.spec.statement | split("\n")[0] | ltrimstr("-- fingerprint: ") | if startswith("--") or (length > 16) then "-" else . end)
    detail   \(.status.detail // "-")"'
  done
  [ "$found" -eq 1 ] || printf '\n  nothing deployed (./pipeline.sh deploy)\n'
  local var
  for var in ${CONNECTOR_VARS:-}; do
    printf '\n  %s  (connector)\n    state    %s\n' "${!var}" "$(connector_state "${!var}")"
  done
  if [ -n "${RTCE_TOPIC_VARS:-}" ]; then
    say ""
    say "  Real-Time Context Engine"
    rtce_list 2>/dev/null | jq -r '.data[]? | "    \(.spec.topic_name)  \(.status.phase)"' || true
  fi
}

# One-shot statement, with its rows. This is the debugging front door.
cmd_sql() {
  need curl jq
  load_env
  local sql="${1:-}"; [ -n "$sql" ] || die "usage: ./pipeline.sh sql '<statement>'"
  local name; name=$(ddl_name adhoc)
  submit_statement "$name" "$sql" RUNNING
  # A SELECT keeps producing; a couple of pages is what a human wants here. The
  # first page of a streaming result is routinely empty, which is why this
  # follows `metadata.next` rather than reading once.
  local url rows=0 out n
  url="$(flink_base)/statements/$name/results"
  local _
  for _ in $(seq 1 "${SQL_POLLS:-12}"); do
    out=$(curl -sS -u "$FLINK_API_KEY:$FLINK_API_SECRET" "$url") || break
    n=$(printf '%s' "$out" | jq '.results.data | length // 0')
    if [ "${n:-0}" -gt 0 ]; then
      printf '%s' "$out" | jq -r '.results.data[]|.row|@tsv'
      rows=$((rows + n))
    fi
    [ "$rows" -ge "${SQL_ROWS:-20}" ] && break
    local nxt; nxt=$(printf '%s' "$out" | jq -r '.metadata.next // ""')
    [ -n "$nxt" ] && url="$nxt"
    sleep 2
  done
  delete_statement_if_present "$name"
}

cmd_delete() {
  need curl jq
  load_env
  step "Deleting the statements"
  local name
  for name in $(long_running_names); do
    delete_statement_if_present "$name"
    say "    $name"
  done
  step "Dropping views"
  drop_views
  say ""
  dim "  The functions, the tables, the connections and the artifacts — Terraform's —"
  dim "  are left alone —"
  dim "  this stops the job and nothing else, which is what you want between deploys."
  dim ""
  dim "  For the full teardown:  ./setup.sh destroy --dry-run"
}

# --- dispatch --------------------------------------------------------------

setup_main() {
  case "${1:-}" in
    info)        cmd_info ;;
    plan)        shift; cmd_plan "$@" ;;
    apply|all)   shift; cmd_apply "$@" ;;
    context-engine) shift; cmd_context_engine "$@" ;;
    tf)          shift; cmd_tf "$@" ;;
    destroy)     shift; cmd_destroy "$@" ;;
    topics|artifact|connections|connectors|tables|functions)
      die "./setup.sh $1 is Terraform's now: ./setup.sh plan, then ./setup.sh apply" ;;
    ""|-h|--help) setup_usage ;;
    *)           die "unknown command: $1 (try ./setup.sh --help)" ;;
  esac
}

pipeline_main() {
  case "${1:-}" in
    deploy) shift; cmd_deploy "$@" ;;
    status) cmd_status ;;
    sql)    shift; cmd_sql "${1:-}" ;;
    delete) cmd_delete ;;
    ""|-h|--help) pipeline_usage ;;
    *)      die "unknown command: $1 (try ./pipeline.sh --help)" ;;
  esac
}
