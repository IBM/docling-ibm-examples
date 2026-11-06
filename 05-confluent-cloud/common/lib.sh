# Everything the examples' setup.sh and pipeline.sh are made of. Sourced, never
# executed.
#
# An example's scripts are thin on purpose: they set EXAMPLE_DIR, source this,
# and dispatch. What differs between examples is the SQL and the names, not the
# mechanics, so the mechanics live here once.
#
# Three credentials, three jobs, and mixing them up is the first hour of anyone's
# first day with Confluent Cloud:
#
#   KAFKA_API_KEY          a *cluster* key. Reads and writes topics. Cannot see
#                          an environment, let alone a compute pool.
#   CONFLUENT_CLOUD_API_KEY  a *Cloud* key. The control plane: environments,
#                          clusters, compute pools, Flink artifacts, the other
#                          keys — it is what Terraform's provider runs as. Note that
#                          the `confluent` CLI does NOT accept it — the CLI logs
#                          in with an email and a password only — so everything
#                          here goes through the REST API instead.
#   FLINK_API_KEY          a *Flink region* key. Submits statements, and nothing
#                          else does.
#
# The Cloud key inherits its holder's RBAC. A key made under a service account
# with no role bindings authenticates perfectly and then lists zero
# environments, which looks exactly like an empty account. `./account.sh check`
# says so in as many words.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Set by an example's setup.sh / pipeline.sh before sourcing this. account.sh
# leaves it empty, and everything below that needs an example says so.
: "${EXAMPLE_DIR:=}"

# --- output ---------------------------------------------------------------

if [ -t 1 ]; then C_DIM=$'\033[2m'; C_RED=$'\033[31m'; C_GRN=$'\033[32m'; C_YEL=$'\033[33m'; C_OFF=$'\033[0m'
else C_DIM=""; C_RED=""; C_GRN=""; C_YEL=""; C_OFF=""; fi

say()  { printf '%s\n' "$*"; }
step() { printf '\n%s==>%s %s\n' "$C_GRN" "$C_OFF" "$*"; }
warn() { printf '%s!!%s  %s\n' "$C_YEL" "$C_OFF" "$*" >&2; }
die()  { printf '%sxx%s  %s\n' "$C_RED" "$C_OFF" "$*" >&2; exit 1; }
dim()  { printf '%s%s%s\n' "$C_DIM" "$*" "$C_OFF"; }

need() {
  # `local` on the loop variable, always. Without it this assigns to a global
  # named `cmd`, and a caller that happened to have one of its own loses it —
  # see require_vars below, where exactly that cost an afternoon.
  local cmd hint
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 && continue
    hint=""
    if [ "$cmd" = terraform ]; then
      hint=". On macOS: brew tap hashicorp/tap && brew install hashicorp/tap/terraform. Elsewhere: https://developer.hashicorp.com/terraform/install"
    fi
    die "$cmd is not installed$hint"
  done
}

# --- configuration --------------------------------------------------------

# The layering, and it is the same one common/ccenv.py implements for the
# Python scripts:
#
#   .env                the account — credentials, ids, NS, the shared knobs
#   <example>/env.sh    the example's names, derived from NS   (committed)
#   <example>/.env      what the example's setup wrote back    (generated)
#   .env.local          your private overrides, last so they win
load_env() {
  [ -f "$REPO_ROOT/.env" ] || die "no .env — run ./account.sh init, or copy .env.example to .env and fill it in"
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.env"
  if [ -n "$EXAMPLE_DIR" ]; then
    [ -f "$EXAMPLE_DIR/env.sh" ] || die "no $EXAMPLE_DIR/env.sh — that file is what names this example's objects"
    # shellcheck disable=SC1091
    . "$EXAMPLE_DIR/env.sh"
    # shellcheck disable=SC1091
    [ -f "$EXAMPLE_DIR/.env" ] && . "$EXAMPLE_DIR/.env"
  fi
  # shellcheck disable=SC1091
  [ -f "$REPO_ROOT/.env.local" ] && . "$REPO_ROOT/.env.local"
  set +a

  : "${CC_CLOUD:=aws}"
  : "${CC_REGION:=us-east-1}"
  : "${PARTITIONS:=1}"
  : "${RETENTION:=1 d}"
  : "${MIN_CHARS:=40}"
  : "${DROP_LOW_QUALITY:=FALSE}"
  : "${PII_DETECTORS:=email,credit_card,iban}"
  : "${PII_REDACT:=TRUE}"
  : "${BLOCKED_DOC_IDS:=}"
  : "${DEDUP_TTL:=24 h}"
  : "${CHUNKS_VALUE_COLUMN:=val}"

  # NS is not cosmetic. A Flink catalog is a Confluent *environment* and a Flink
  # database is a Kafka *cluster*, so every table, view and function this repo
  # creates lands in a namespace shared with everyone who can see that
  # environment — and `deploy` DROPs its own views and DELETEs its own
  # statements by name before it recreates them. A name you do not own is
  # somebody else's object you destroy, and NS is what prevents that.
  [ -n "${NS:-}" ] || die "NS is not set in .env — it is what keeps every name below yours. See .env.example section 3."

  # The artifact API wants the cloud in upper case and everything else wants it
  # lower. Computed here rather than with ${VAR^^}, which macOS's bash 3.2 —
  # still /bin/bash on every Mac — parses as a syntax error, not as a no-op.
  CC_CLOUD_UPPER=$(printf '%s' "$CC_CLOUD" | tr '[:lower:]' '[:upper:]')
  export CC_CLOUD_UPPER EXAMPLE_DIR REPO_ROOT

  # Every name the files above assign, and the defaults, for env_json — which
  # is how Terraform sees the same configuration the scripts do.
  local files=("$REPO_ROOT/.env")
  if [ -n "$EXAMPLE_DIR" ]; then
    files+=("$EXAMPLE_DIR/env.sh")
    [ -f "$EXAMPLE_DIR/.env" ] && files+=("$EXAMPLE_DIR/.env")
  fi
  [ -f "$REPO_ROOT/.env.local" ] && files+=("$REPO_ROOT/.env.local")
  CONFIG_VARS=$( {
    sed -nE 's/^[[:space:]]*(export[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*)=.*/\2/p' "${files[@]}"
    printf '%s\n' CC_CLOUD CC_REGION PARTITIONS RETENTION MIN_CHARS DROP_LOW_QUALITY PII_DETECTORS \
      PII_REDACT BLOCKED_DOC_IDS DEDUP_TTL CHUNKS_VALUE_COLUMN CC_CLOUD_UPPER EXAMPLE_DIR REPO_ROOT
  } | sort -u)
}

require_vars() {
  # `local name` is not tidiness. This function is called from deep inside
  # cc_get and cc_post, which are called from functions whose own working
  # variable is called `name` — and without `local` this loop assigns to
  # *theirs*. The symptom was an artifact uploaded under the display name
  # "CONFLUENT_CLOUD_API_SECRET", which is a very long way from the cause.
  local name
  local missing=()
  for name in "$@"; do
    [ -n "${!name:-}" ] || missing+=("$name")
  done
  [ ${#missing[@]} -eq 0 ] || die "unset in .env / env.sh: ${missing[*]}"
}

require_example() {
  [ -n "$EXAMPLE_DIR" ] || die "this command belongs to an example — run it from examples/<n>/setup.sh or pipeline.sh"
}

set_env()         { "$REPO_ROOT/common/set_env.py" "$@"; }
set_example_env() { "$REPO_ROOT/common/set_env.py" --file "$EXAMPLE_DIR/.env" "$@"; }

# --- the statement files --------------------------------------------------

# An example's statements come from two directories: its own `flink/`, and
# `common/flink/` — the enrichment half, which every example runs identically
# and which is therefore written once. They are merged by *file name*, so the
# numeric prefixes order the whole set regardless of which directory a file
# came from, and an example may replace a shared statement by giving a file the
# same name.
#
# An example with no chunks in it — 04 extracts invoice fields, 05 financial figures — sets
# USE_COMMON_FLINK=no in its env.sh, and then only its own directory counts.
#
#   sql_stage 00_table_    -> every table DDL, example's first
sql_stage() {
  local prefix="$1" f base seen=" " list="" common="$REPO_ROOT/common/flink"
  [ "${USE_COMMON_FLINK:-yes}" = "no" ] && common="$EXAMPLE_DIR/flink/.no-common"
  for f in "$EXAMPLE_DIR"/flink/"$prefix"*.sql "$common"/"$prefix"*.sql; do
    [ -e "$f" ] || continue
    base=$(basename "$f")
    case "$seen" in *" $base "*) continue ;; esac
    seen="$seen$base "
    list="$list$base|$f"$'\n'
  done
  [ -n "$list" ] || return 0
  printf '%s' "$list" | sort -t'|' -k1,1 | cut -d'|' -f2
}

# The statement name for one of those files: `<app>-<kind>-<name>-<stamp>`.
# A statement name is [a-z0-9-], max 100 chars, and cannot be reused — hence
# the timestamp on every one-shot DDL.
sql_file_name() {
  local file="$1" base
  base=$(basename "$file" .sql)
  # 00_table_enriched -> table-enriched ; 91_drop_function_guard -> drop-function-guard
  base=$(printf '%s' "$base" | sed 's/^[0-9]*_//' | tr '_' '-')
  ddl_name "$base"
}

# run_stage <prefix> <what>   — every statement in a stage, in order.
run_stage() {
  local prefix="$1" what="$2" f any=0
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    any=1
    run_sql_file "$f" "$(sql_file_name "$f")"
  done <<EOF
$(sql_stage "$prefix")
EOF
  [ "$any" -eq 1 ] || dim "    (no $what statements in this example)"
}

# As run_stage, but a failure is survivable. Teardown only: dropping something
# that is not there must never stop the rest of a teardown, and run_sql_file
# fails by calling die(), which exits — so each one gets its own subshell.
run_stage_best_effort() {
  local prefix="$1" f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    ( run_sql_file "$f" "$(sql_file_name "$f")" ) || true
  done <<EOF
$(sql_stage "$prefix")
EOF
}

# Dropped and recreated rather than CREATE OR REPLACE: a view's definition
# embeds the configured values as literals, so an edit to MIN_CHARS that does
# not reach the view is a pipeline running yesterday's policy.
drop_views() { run_stage_best_effort 90_drop_view_; }

# --- Confluent Cloud control plane ---------------------------------------

# Overridable so the discovery path can be exercised against a stub; nothing
# in normal use should set it.
: "${CC_API:=https://api.confluent.cloud}"

# cc_get <path-with-query> -> body on stdout, non-zero exit on a 4xx/5xx.
#
# Callers must handle that non-zero themselves — `if`, `||`, or cc_get_or_die.
# `set -o pipefail` is on, so `x=$(cc_get ... | jq ...)` fails the whole
# assignment when the request 4xxs, and `set -e` then exits the script *before*
# any message the caller meant to print. That is how a permissions error turns
# into a silent `exit 1`, which is worse than useless.
cc_get() {
  require_vars CONFLUENT_CLOUD_API_KEY CONFLUENT_CLOUD_API_SECRET
  local body status
  body=$(curl -sS -u "$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET" \
              -H 'Accept: application/json' \
              -w '\n%{http_code}' "$CC_API$1") || return 1
  status="${body##*$'\n'}"
  body="${body%$'\n'*}"
  CC_LAST_STATUS="$status"
  printf '%s' "$body"
  [ "$status" -lt 400 ] || return 1
}

# The reason depends on the status, and conflating them wastes an afternoon: a
# 401/403 is a permissions problem, anything else is the API saying the request
# was wrong. Answering a 400 with "check your role bindings" sends you off
# granting roles that were never it.
#
#   cc_die_by_status <what> <response-body>
#
# The status is read back out of the body rather than from a variable, because
# cc_get runs inside `$( )` in every caller — a subshell, so anything it
# assigns is gone by the time this runs. Confluent's error envelope carries it
# as .errors[0].status, and CC_LAST_STATUS is only a fallback for a caller in
# the same shell.
cc_die_by_status() {
  local what="$1" body="${2:-}" status
  status=$(printf '%s' "$body" | jq -r '.errors[0].status // empty' 2>/dev/null || true)
  [ -n "$status" ] || status="${CC_LAST_STATUS:-?}"
  case "$status" in
    401|403) die "$what was refused (HTTP $status). The Cloud API key authenticates but
      lacks the role binding for this — ./account.sh check explains what to grant." ;;
    *)       die "$what failed (HTTP $status). The reason is above; this is the API
      rejecting the request, not a permissions problem." ;;
  esac
}

# cc_get, but a failure is fatal and says why. Safe to use inside a pipeline or
# a command substitution: the message reaches stderr before the subshell exits.
cc_get_or_die() {
  local body
  if body=$(cc_get "$1"); then
    printf '%s' "$body"
    return 0
  fi
  # Everything from here goes to stderr. Callers pipe this function into jq —
  # a diagnostic on stdout would be fed to *their* jq as if it were the
  # response, and what you would see is a parse error instead of the reason.
  local pretty
  pretty=$(printf '%s' "$body" | jq -r '.errors[]?|"    \(.status) \(.code // ""): \(.detail // .title)"' 2>/dev/null || true)
  if [ -n "$pretty" ]; then printf '%s\n' "$pretty" >&2; else printf '    %s\n' "$body" >&2; fi
  cc_die_by_status "${2:-GET $1}" "$body"
}

# --- Flink SQL REST -------------------------------------------------------

flink_base() {
  require_vars CC_ORG_ID CC_ENVIRONMENT_ID
  printf 'https://flink.%s.%s.confluent.cloud/sql/v1/organizations/%s/environments/%s' \
    "$CC_REGION" "$CC_CLOUD" "$CC_ORG_ID" "$CC_ENVIRONMENT_ID"
}

# flink_api <METHOD> <path-under-environment> [json-body] -> body, status on last line
flink_api() {
  require_vars FLINK_API_KEY FLINK_API_SECRET
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" -u "$FLINK_API_KEY:$FLINK_API_SECRET"
              -H 'Accept: application/json' -w '\n%{http_code}')
  if [ -n "$body" ]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$body")
  fi
  curl "${args[@]}" "$(flink_base)$path"
}

flink_ok() {  # flink_api output -> body on stdout, die on HTTP error
  local out="$1" what="$2" status body
  status="${out##*$'\n'}"; body="${out%$'\n'*}"
  if [ "$status" -ge 400 ]; then
    # Same shape as cc_get_or_die: a body with no `.errors` makes jq succeed
    # and print nothing, so fall back on whether there was any output rather
    # than on jq's exit status.
    local pretty
    pretty=$(printf '%s' "$body" | jq -r '.errors[]?|"    \(.status) \(.code // ""): \(.detail // .title)"' 2>/dev/null || true)
    if [ -n "$pretty" ]; then printf '%s\n' "$pretty" >&2; else printf '    %s\n' "$body" >&2; fi
    die "$what failed (HTTP $status)"
  fi
  printf '%s' "$body"
}

# The properties every statement is submitted with. The two `sql.current-*` are
# what make a bare table name resolve: the catalog is the *environment's display
# name* and the database is the *cluster's display name* — display names, not
# ids, which is the detail that costs an afternoon.
statement_properties() {
  require_vars CC_CATALOG CC_DATABASE
  jq -n --arg cat "$CC_CATALOG" --arg db "$CC_DATABASE" --arg ttl "$DEDUP_TTL" \
    '{"sql.current-catalog": $cat, "sql.current-database": $db, "sql.state-ttl": $ttl}'
}

# submit_statement <name> <sql-text> [wait-for: COMPLETED|RUNNING]
submit_statement() {
  # Checked here, in the main shell, and not only inside statement_properties:
  # that function is called as `--argjson props "$(statement_properties)"`, and
  # a die() in a command substitution exits only the subshell. The message
  # would print and then jq would fail on an empty argument, burying it.
  require_vars CC_COMPUTE_POOL_ID CC_CATALOG CC_DATABASE
  local name="$1" sql="$2" want="${3:-COMPLETED}" payload out

  payload=$(jq -n \
    --arg name "$name" \
    --arg sql "$sql" \
    --arg pool "$CC_COMPUTE_POOL_ID" \
    --argjson props "$(statement_properties)" \
    '{name: $name, spec: {statement: $sql, compute_pool_id: $pool, properties: $props, stopped: false}}')

  out=$(flink_api POST /statements "$payload")
  flink_ok "$out" "submitting $name" >/dev/null
  await_statement "$name" "$want"
}

# await_statement <name> <phase> — poll until it reaches <phase>, or fails.
await_statement() {
  local name="$1" want="$2" phase detail
  for _ in $(seq 1 90); do
    phase=$(flink_ok "$(flink_api GET "/statements/$name")" "reading $name" | jq -r '.status.phase')
    case "$phase" in
      "$want")            say "    $name: $phase"; return 0 ;;
      COMPLETED|RUNNING)  say "    $name: $phase"; return 0 ;;
      FAILED|FAILING)
        detail=$(flink_ok "$(flink_api GET "/statements/$name")" "reading $name" | jq -r '.status.detail // "no detail"')
        # A one-shot DDL that failed is a dead resource: it holds no state, it
        # will never run, and leaving it behind means the environment fills up
        # with the history of every typo. The pipeline statement (want=RUNNING)
        # is kept instead, because a failed job is worth looking at in the UI.
        # Only ever our own name — nothing else is awaited here.
        if [ "$want" = "COMPLETED" ]; then
          delete_statement_if_present "$name"
        fi
        die "$name: $phase — $detail" ;;
      *) sleep 2 ;;
    esac
  done
  die "$name: still $phase after 3 minutes"
}

# run_sql_file <file.sql> <statement-name> [wait-for]
run_sql_file() {
  local file="$1" name="$2" want="${3:-COMPLETED}" sql
  sql=$("$REPO_ROOT/common/render.py" "$file")
  say "  ${file#"$REPO_ROOT"/} -> $name"
  submit_statement "$name" "$sql" "$want"
}

# A DDL statement gets a timestamp because a name cannot be reused. A
# long-running one gets a stable name on purpose, so `status` and `delete` have
# something to address — and so that redeploying replaces the running statement
# instead of adding a second one beside it.
#
# The long-running statements of an example are its `3*.sql` files: exactly one
# in example 01 (the enrichment pipeline), two in examples 02 and 03, where a
# second statement calls Docling and writes the chunk topic the first one
# reads, and two in example 05, where the second derives events from the
# metrics the first writes. Everything else finishes immediately.
ddl_name() { printf '%s-%s-%s' "$APP_NAME" "$1" "$(date +%Y%m%d%H%M%S)"; }

# long_running_name <file.sql>  ->  <app>-pipeline, <app>-convert, ...
long_running_name() {
  printf '%s-%s\n' "$APP_NAME" \
    "$(basename "$1" .sql | sed 's/^[0-9]*_//' | tr '_' '-')"
}

long_running_names() {
  local f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    long_running_name "$f"
  done <<EOF
$(sql_stage 3)
EOF
}

# list_statements -> every statement in the environment, one JSON object per
# line. The API answers at most 100 per page whatever page_size asks for, and an
# environment shared with other work easily has more — so this follows
# metadata.next to the end. Reading only the first page once missed two running
# statements that should have been stopped.
list_statements() {
  require_vars FLINK_API_KEY FLINK_API_SECRET
  local url body
  url="$(flink_base)/statements?page_size=100"
  while [ -n "$url" ]; do
    body=$(curl -sS -u "$FLINK_API_KEY:$FLINK_API_SECRET" -H 'Accept: application/json' "$url") \
      || die "listing statements failed"
    printf '%s' "$body" | jq -c '.data[]?'
    url=$(printf '%s' "$body" | jq -r '.metadata.next // empty')
  done
}

delete_statement_if_present() {
  local name="$1" out status
  out=$(flink_api DELETE "/statements/$name")
  status="${out##*$'\n'}"
  case "$status" in
    2*|404) return 0 ;;
    *) warn "could not delete $name (HTTP $status)"; return 0 ;;
  esac
}

# A DELETE is accepted at once and carried out later: the statement sits in
# DELETING for a while, and its name cannot be reused until it is gone.
wait_statement_gone() {
  local name="$1" tries=60 status
  while [ "$tries" -gt 0 ]; do
    status=$(flink_api GET "/statements/$name"); status="${status##*$'\n'}"
    [ "$status" = "404" ] && return 0
    sleep 2; tries=$((tries - 1))
  done
  warn "$name is still being deleted after two minutes"
}


# --- what is still read directly ---------------------------------------------
#
# Terraform creates and deletes these; the scripts only ask how they are doing,
# for ./pipeline.sh status — and drop a connection when a provisioner asks
# (common/tf-hook.sh).

# The Real-Time Context Engine, per topic — example 04's terraform/main.tf.
rtce_list() {
  cc_get "/rtce/v1/rtce-topics?environment=$CC_ENVIRONMENT_ID&spec.kafka_cluster=$CC_CLUSTER_ID"
}

# Terraform turns off what it turned on. This is for the rest — one turned on
# by the scripts before Terraform, which would otherwise outlive its topic and
# keep billing — and only ./setup.sh destroy calls it.
rtce_off_if_present() {
  local status
  status=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE \
    -u "$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET" \
    "$CC_API/rtce/v1/rtce-topics/$1?environment=$CC_ENVIRONMENT_ID&spec.kafka_cluster=$CC_CLUSTER_ID") || status=000
  case "$status" in
    2*|404) return 0 ;;
    *) warn "could not turn the Context Engine off for $1 (HTTP $status)"; return 0 ;;
  esac
}

delete_connection_if_present() {
  local name="$1" out status
  out=$(flink_api DELETE "/connections/$name")
  status="${out##*$'\n'}"
  case "$status" in
    2*|404) return 0 ;;
    *) warn "could not delete connection $name (HTTP $status)"; return 0 ;;
  esac
}

# A managed connector — example 03's SQS Source. Billed by the hour while it
# runs, which is why status shows it.
#   connector_state <name> -> "RUNNING · tasks RUNNING", or "not created"
connector_state() {
  local body
  require_vars CC_ENVIRONMENT_ID CC_CLUSTER_ID
  if ! body=$(cc_get "/connect/v1/environments/$CC_ENVIRONMENT_ID/clusters/$CC_CLUSTER_ID/connectors/$1/status" 2>/dev/null); then
    printf 'not created'
    return 0
  fi
  printf '%s' "$body" | jq -r '"\(.connector.state // "?")"
    + (if (.tasks // []) | length > 0 then " · tasks " + ([.tasks[].state] | join(",")) else "" end)
    + (if .connector.trace then " · " + (.connector.trace | split("\n")[0]) else "" end)'
}

# --- Terraform ---------------------------------------------------------------
#
# The setup is Terraform's: terraform/account for the account, and
# examples/<n>/terraform for each example, both on terraform/modules. The
# configuration stays where it was — .env and env.sh — and reaches Terraform as
# one variable, `env`, so the scripts and the plan can never disagree about a
# name. It is passed in the environment of the one terraform process and never
# written to a file; the state, which does hold the secrets, is gitignored.

# Every name the layered configuration sets, plus the defaults load_env fills
# in, as one JSON object of strings.
env_json() {
  local n
  for n in $CONFIG_VARS; do printf '%s\0%s\0' "$n" "${!n-}"; done \
    | jq -Rs 'split("\u0000") | .[:-1] | [range(0; length; 2) as $i | {(.[$i]): .[$i + 1]}] | add // {}'
}

# tf <dir> <terraform args...> — terraform in <dir>, with var.env set.
#
# `init` every time: it is a no-op when nothing changed, and when a module or a
# provider pin did change, a stale .terraform is an error about modules that
# are "not installed", a long way from the cause.
tf() {
  need terraform jq
  local dir="$1" out; shift
  if ! out=$(terraform -chdir="$dir" init -input=false -no-color 2>&1); then
    printf '%s\n' "$out" >&2
    die "terraform init failed in ${dir#"$REPO_ROOT"/}"
  fi
  # The provider also reads its settings from the environment — KAFKA_API_KEY,
  # FLINK_REST_ENDPOINT and the like — and load_env has exported names that
  # match. Everything reaches the configuration through TF_VAR_env instead, so
  # the provider sees only what its block says.
  local -a unset=()
  local v
  for v in $(compgen -e | grep -E '^(CONFLUENT_CLOUD|KAFKA|FLINK|SCHEMA_REGISTRY|TABLEFLOW|CATALOG)_'); do
    unset+=(-u "$v")
  done
  TF_VAR_env="$(env_json)" env "${unset[@]}" terraform -chdir="$dir" "$@"
}
