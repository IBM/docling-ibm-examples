#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# Your Confluent Cloud account, once — before any example can run.
#
#   ./account.sh init       fill in .env: asks for each value and says where to get it
#   ./account.sh check      what can this key see? (reads only — creates nothing)
#   ./account.sh plan       what terraform/account would do (creates nothing)
#   ./account.sh apply      make what .env does not name, mint the API keys —
#                           Flink, Kafka, Schema Registry, and a global key if
#                           GLOBAL_KEY="yes" — and write all of it into .env
#   ./account.sh destroy    delete the keys, and whatever apply made
#   ./account.sh tf <args>  any other terraform command, e.g. `tf state list`
#   ./account.sh info       every id an example will need, in the spelling it wants
#   ./account.sh topics     list the topics on the cluster
#
# You fill in three things by hand: CONFLUENT_CLOUD_API_KEY, its secret, and
# NS. `apply` uses the account's "default" environment and makes everything
# else in it — a cluster, a compute pool, a service account and its keys —
# unless .env names one you already have, and then it uses that. See .env.example.
#
# Nothing here belongs to an example. `check`, `plan`, `info` and `topics`
# create nothing in your account; `apply` shows you the plan and asks first.
set -euo pipefail
. "$(cd "$(dirname "$0")" && pwd)/common/lib.sh"

usage() { sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

# --- init -----------------------------------------------------------------

cmd_init() {
  need python3
  "$REPO_ROOT/common/init_env.py"
  local answer
  read -r -p "Check the Cloud API key now with ./account.sh check? [Y/n] " answer || answer="n"
  case "$answer" in
    [nN]*) ;;
    *) cmd_check ;;
  esac
  say ""
  dim "Next, run: ./account.sh plan"
  dim "It shows what would be created. Then ./account.sh apply creates it, and that is billed."
}

# --- check ----------------------------------------------------------------

cmd_check() {
  need curl jq uv
  load_env

  step "Cloud API key"
  local orgs envs
  if ! orgs=$(cc_get /org/v2/organizations); then
    die "the Cloud API key was rejected. Check CONFLUENT_CLOUD_API_KEY/SECRET in .env."
  fi
  local n_orgs; n_orgs=$(printf '%s' "$orgs" | jq '.data|length')
  if [ "$n_orgs" -eq 0 ]; then
    warn "authenticated, but this key can see no organizations."
    cat >&2 <<'MSG'

      That is an RBAC result, not a bad key: a Cloud API key inherits the
      permissions of whoever owns it, and a key created under a service account
      with no role bindings sees an empty account. Every list below will be
      empty too.

      Fix it one of two ways, in the Confluent Cloud console:
        * create the Cloud API key under your own user account (Accounts &
          access -> your user -> API keys), or
        * grant the service account a role — OrganizationAdmin is the blunt
          one; EnvironmentAdmin on the environment plus FlinkAdmin on the
          Flink region is the narrow one.

MSG
  else
    printf '%s' "$orgs" | jq -r '.data[]|"    \(.id)  \(.display_name)"'
  fi

  step "Environments"
  envs=$(cc_get /org/v2/environments) || die "could not list environments"
  printf '%s' "$envs" | jq -r '.data[]|"    \(.id)  \(.display_name)"'
  local env_ids; env_ids=$(printf '%s' "$envs" | jq -r '.data[].id')

  step "Kafka clusters"
  local e
  for e in $env_ids; do
    # `|| true` because pipefail turns a 403 on one environment into a failed
    # pipeline, and check is meant to survive every refusal and report it.
    cc_get "/cmk/v2/clusters?environment=$e" \
      | jq -r --arg e "$e" '.data[]|"    \($e)  \(.id)  \(.spec.display_name)  \(.spec.cloud)/\(.spec.region)  \(.spec.kafka_bootstrap_endpoint)"' || true
  done

  step "Flink compute pools"
  local found=0
  for e in $env_ids; do
    local pools; pools=$(cc_get "/fcpm/v2/compute-pools?environment=$e") || continue
    printf '%s' "$pools" | jq -r --arg e "$e" '.data[]|"    \($e)  \(.id)  \(.spec.display_name)  \(.spec.cloud)/\(.spec.region)  max_cfu=\(.spec.max_cfu)  \(.status.phase)"'
    [ "$(printf '%s' "$pools" | jq '.data|length')" -gt 0 ] && found=1
  done
  if [ "$found" -eq 0 ]; then
    dim "    none yet. With CC_COMPUTE_POOL_ID unset, ./account.sh apply makes one in"
    dim "    the cluster's region — Flink statements need one, and it is what is billed."
  fi

  step "Flink artifacts in $CC_CLOUD/$CC_REGION"
  if [ -n "${CC_ENVIRONMENT_ID:-}" ]; then
    cc_get "/artifact/v1/flink-artifacts?cloud=$CC_CLOUD_UPPER&region=$CC_REGION&environment=$CC_ENVIRONMENT_ID" \
      | jq -r '.data[]?|"    \(.id)  \(.display_name)  \(.runtime_language)"' 2>/dev/null || true
  else
    dim "    (after ./account.sh apply)"
  fi

  step "Flink API key"
  if [ -n "${FLINK_API_KEY:-}" ] && [ -n "${CC_ORG_ID:-}" ] && [ -n "${CC_ENVIRONMENT_ID:-}" ]; then
    local out status
    out=$(flink_api GET "/statements?page_size=1"); status="${out##*$'\n'}"
    if [ "$status" -lt 400 ]; then
      say "    ok — $(printf '%s' "${out%$'\n'*}" | jq -r '.metadata.total_size // 0') statement(s) in this environment"
    else
      warn "the Flink API key was rejected (HTTP $status). It must be a *Flink* key for $CC_CLOUD/$CC_REGION."
    fi
  else
    dim "    (none yet — ./account.sh apply mints it)"
  fi

  step "Kafka cluster"
  if [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ] && [ -n "${KAFKA_API_KEY:-}" ]; then
    uv run --quiet --project "$REPO_ROOT" "$REPO_ROOT/common/topics.py" list || warn "could not reach the cluster with the Kafka key"
  else
    dim "    (none yet — ./account.sh apply mints its key)"
  fi

  step "Docling for IBM watsonx (examples 02 to 05 only)"
  if [ -n "${DOCLING_SERVICE_URL:-}" ] && [ -n "${DOCLING_API_KEY:-}" ]; then
    local code
    code=$(curl -sS -o /dev/null -w '%{http_code}' -H "X-Api-Key: $DOCLING_API_KEY" "$DOCLING_SERVICE_URL/health" || echo 000)
    case "$code" in
      2*)  say "    ok — $DOCLING_SERVICE_URL answers /health" ;;
      401|403) warn "the Docling API key was rejected (HTTP $code) by $DOCLING_SERVICE_URL" ;;
      000) warn "could not reach $DOCLING_SERVICE_URL at all" ;;
      *)   warn "$DOCLING_SERVICE_URL/health answered HTTP $code" ;;
    esac
  else
    dim "    (unset — example 01 does not need it)"
  fi

  step "Namespace"
  dim "    NS=\"$NS\". Every object any example creates is named from it. Each"
  dim "    example's ./setup.sh info prints its own list; ./setup.sh destroy --dry-run"
  dim "    prints what that example would remove."

  step "Compute pool"
  dim "    A pool is shared: every statement in the environment that names it competes"
  dim "    for its CFUs, and a pool without running statements scales to zero. A"
  dim "    pipeline left deployed holds CFUs — and bills — until you delete it."
  dim "    An example's ./pipeline.sh delete is how you stop paying for it."

  say ""
  dim "Nothing above created anything. ./account.sh apply and an example's ./setup.sh apply are the steps that do."
}

# --- the account, in Terraform ------------------------------------------------
#
# terraform/account: it makes what .env does not name — cluster, compute pool,
# the service account that owns the keys — looks up what it does, and always
# makes the API keys. The environment it only ever looks up: CC_ENVIRONMENT_ID,
# or the account's "default". `apply` writes what it found and what it made
# into .env, which is where every script and every example reads them.

# The ids terraform/account made itself. They are in .env like every other id,
# for the scripts; Terraform gets them blank, because "set" means "look it up"
# to it, and a thing it made has to stay its own.
made_vars() {
  tf "$REPO_ROOT/terraform/account" output -json made 2>/dev/null | jq -r '.[]?' 2>/dev/null || true
}

account_tf() {
  need curl jq terraform
  load_env
  require_vars CONFLUENT_CLOUD_API_KEY CONFLUENT_CLOUD_API_SECRET NS
  local made v
  made=$(made_vars)
  (
    for v in $made; do printf -v "$v" '%s' ""; done
    tf "$REPO_ROOT/terraform/account" "$@"
  )
}

cmd_plan() { account_tf plan "$@"; }

cmd_apply() {
  account_tf apply "$@"
  step "Writing .env"
  local pairs=() line
  while IFS= read -r line; do
    [ -n "$line" ] && pairs+=("$line")
  done < <(tf "$REPO_ROOT/terraform/account" output -json env | jq -r 'to_entries[] | "\(.key)=\(.value)"')
  # --force: Terraform owns these keys now, and its answer replaces whatever
  # an earlier run wrote.
  set_env --force "${pairs[@]}"
  say ""
  dim "  A new key takes a few seconds to become usable. ./account.sh check verifies them."
}

# Deletes the keys, and whatever else this root made — a cluster it made goes
# with every topic in it; one it only looked up is left alone. Every example's ./setup.sh destroy first: they submit with these keys.
cmd_destroy() {
  local made v clear=()
  load_env
  made=$(made_vars)
  account_tf destroy "$@"
  for v in $made; do clear+=("$v="); done
  set_env --force FLINK_API_KEY= FLINK_API_SECRET= KAFKA_API_KEY= KAFKA_API_SECRET= \
    SCHEMA_REGISTRY_API_KEY= SCHEMA_REGISTRY_API_SECRET= RTCE_API_KEY= RTCE_API_SECRET= "${clear[@]}"
}

# --- info -----------------------------------------------------------------

cmd_info() {
  load_env
  cat <<INFO

  organization  ${CC_ORG_ID:-<unset>}
  environment   ${CC_ENVIRONMENT_ID:-<unset>}   catalog  ${CC_CATALOG:-<unset>}
  cluster       ${CC_CLUSTER_ID:-<unset>}       database ${CC_DATABASE:-<unset>}
  compute pool  ${CC_COMPUTE_POOL_ID:-<unset>}  region   $CC_CLOUD/$CC_REGION
  bootstrap     ${KAFKA_BOOTSTRAP_SERVERS:-<unset>}
  namespace     NS="$NS"

  docling saas  ${DOCLING_SERVICE_URL:-<unset>}   (examples 02 to 05)

  The catalog is the environment's *display name* and the database is the
  cluster's. Ids are accepted syntactically and then nothing resolves.

INFO
}

cmd_topics() {
  need uv
  load_env
  uv run --quiet --project "$REPO_ROOT" "$REPO_ROOT/common/topics.py" list
}

case "${1:-}" in
  init)      cmd_init ;;
  check)     cmd_check ;;
  plan)      shift; cmd_plan "$@" ;;
  apply)     shift; cmd_apply "$@" ;;
  destroy)   shift; cmd_destroy "$@" ;;
  tf)        shift; account_tf "$@" ;;
  discover|keys|schema-registry|global-key)
    die "./account.sh $1 is Terraform's now: ./account.sh plan, then ./account.sh apply" ;;
  info)      cmd_info ;;
  topics)    cmd_topics ;;
  ""|-h|--help) usage ;;
  *)         die "unknown command: $1 (try ./account.sh --help)" ;;
esac
