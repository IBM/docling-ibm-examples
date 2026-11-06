#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# The AWS side of example 03 — everything outside Confluent Cloud. AWS.md is
# the long version of what this does and why; this is the short way to do it.
#
#   scripts/aws.sh info                  what it would create, and under which names
#   scripts/aws.sh create [--bucket B] [--prefix P] [--region R]
#                                        bucket (unless you name yours), queue,
#                                        notification, reader IAM user and key
#   scripts/aws.sh check                 is all of it there, and does the key work
#   scripts/aws.sh samples               upload three public PDFs under the prefix
#   scripts/aws.sh backfill [args]       the objects already there -> the queue
#                                        (scripts/backfill.py; --dry-run first)
#   scripts/aws.sh destroy [--bucket]    remove it again; --bucket also empties and
#                                        deletes the bucket, if this script made it
#
# Runs as *you*: the aws CLI with your default credentials (or AWS_PROFILE).
# What it creates for Confluent is a separate IAM user that can read the
# prefix and the queue and nothing else, and its key goes into this example's
# .env — never into the repo's.
set -euo pipefail
EXAMPLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
. "$EXAMPLE_DIR/../../common/lib.sh"

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

export AWS_PAGER=""

# --- names ------------------------------------------------------------------

names() {
  load_env
  # Derived from NS like everything else, so two people sharing an AWS account
  # do not share a queue. Bucket names are global across all of AWS, hence the
  # account id in the default one.
  local ns; ns=$(printf '%s' "$NS" | tr '[:upper:]_' '[:lower:]-')
  QUEUE_NAME="${ns}-ex03-objects"
  IAM_USER="${ns}-ex03-confluent"
  NOTIFICATION_ID="${ns}-ex03"
  S3_REGION="${S3_REGION:-$CC_REGION}"
  S3_PREFIX="${S3_PREFIX-inbox/}"
  DEFAULT_BUCKET_STEM="${ns}-ex03-docling"
}

account_id() {
  aws sts get-caller-identity --query Account --output text 2>/dev/null \
    || die "the aws CLI has no working credentials. aws configure, or set AWS_PROFILE."
}

awsr() { aws --region "$S3_REGION" "$@"; }

# --- info -------------------------------------------------------------------

cmd_info() {
  need aws jq
  names
  local who; who=$(aws sts get-caller-identity --query Arn --output text 2>/dev/null || echo "<no credentials>")
  cat <<INFO

  running as    $who
  region        $S3_REGION

  bucket        ${S3_BUCKET:-$DEFAULT_BUCKET_STEM-<account-id>   (created; or --bucket yours)}
  prefix        ${S3_PREFIX:-<the whole bucket>}
  queue         $QUEUE_NAME   (SQS, standard)
  notification  $NOTIFICATION_ID   s3:ObjectCreated:* under the prefix -> the queue
  IAM user      $IAM_USER   s3:GetObject on the prefix, sqs:Receive/Delete on the queue
  access key    ${AWS_READER_KEY_ID:-<created by create>}

INFO
}

# --- create -----------------------------------------------------------------

cmd_create() {
  need aws jq
  names
  local bucket="${S3_BUCKET:-}" owned="${S3_BUCKET_OWNED:-}"
  while [ $# -gt 0 ]; do
    case "$1" in
      --bucket) bucket="$2"; owned="no"; shift 2 ;;
      --prefix) S3_PREFIX="$2"; shift 2 ;;
      --region) S3_REGION="$2"; shift 2 ;;
      *) die "create: unknown flag $1 (--bucket, --prefix, --region)" ;;
    esac
  done
  case "$S3_PREFIX" in ""|*/) ;; *) S3_PREFIX="$S3_PREFIX/" ;; esac

  local account; account=$(account_id)

  # --- the bucket -----------------------------------------------------------
  step "Bucket"
  if [ -z "$bucket" ]; then
    bucket="$DEFAULT_BUCKET_STEM-$account"
    owned="yes"
  fi
  if aws s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    # A queue must be in its bucket's region, so the bucket decides.
    local where
    where=$(aws s3api get-bucket-location --bucket "$bucket" --query LocationConstraint --output text)
    case "$where" in None|null|"") where="us-east-1" ;; esac
    if [ "$where" != "$S3_REGION" ]; then
      warn "the bucket is in $where, not $S3_REGION — using $where for the queue too"
      S3_REGION="$where"
    fi
    say "    = $bucket (exists, $S3_REGION)"
  else
    [ "$owned" = "yes" ] || die "bucket $bucket does not exist, or you cannot see it. Leave --bucket off and one is created."
    if [ "$S3_REGION" = "us-east-1" ]; then
      awsr s3api create-bucket --bucket "$bucket" >/dev/null
    else
      awsr s3api create-bucket --bucket "$bucket" \
        --create-bucket-configuration "LocationConstraint=$S3_REGION" >/dev/null
    fi
    # Private, explicitly. A presigned URL works through Block Public Access;
    # nothing in this example needs the bucket to be public, ever.
    awsr s3api put-public-access-block --bucket "$bucket" --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
    say "    + $bucket ($S3_REGION, private)"
  fi
  local bucket_arn="arn:aws:s3:::$bucket"

  # --- the queue ------------------------------------------------------------
  step "Queue"
  local queue_url queue_arn
  queue_url=$(awsr sqs create-queue --queue-name "$QUEUE_NAME" \
      --attributes ReceiveMessageWaitTimeSeconds=20 --query QueueUrl --output text)
  queue_arn=$(awsr sqs get-queue-attributes --queue-url "$queue_url" \
      --attribute-names QueueArn --query Attributes.QueueArn --output text)
  # Who may send to it: S3, for this bucket, in this account. The condition is
  # what stops any other bucket in AWS from filling your queue.
  local policy
  policy=$(jq -cn --arg q "$queue_arn" --arg b "$bucket_arn" --arg a "$account" '{
    Version: "2012-10-17",
    Statement: [{
      Sid: "s3-notifications", Effect: "Allow",
      Principal: {Service: "s3.amazonaws.com"},
      Action: "sqs:SendMessage", Resource: $q,
      Condition: {ArnEquals: {"aws:SourceArn": $b}, StringEquals: {"aws:SourceAccount": $a}}
    }]}')
  awsr sqs set-queue-attributes --queue-url "$queue_url" \
    --attributes "$(jq -cn --arg p "$policy" '{Policy: $p}')"
  say "    = $queue_url"

  # --- the notification -----------------------------------------------------
  # put-bucket-notification-configuration REPLACES the bucket's whole
  # configuration. So: read it, drop only our own entry, add ours back, write
  # it — anything else on the bucket survives.
  step "Notification"
  local current filter wanted
  current=$(awsr s3api get-bucket-notification-configuration --bucket "$bucket" 2>/dev/null || true)
  [ -n "$current" ] || current="{}"
  # From when the notification covers uploads: the backfill sends only what is
  # older, so the two roads into the queue do not overlap. Kept across re-runs
  # while the notification stays; a new one starts the clock again.
  local notified_at="${S3_NOTIFIED_AT:-}"
  if [ -z "$notified_at" ] || ! printf '%s' "$current" \
      | jq -e --arg id "$NOTIFICATION_ID" '.QueueConfigurations[]? | select(.Id == $id)' >/dev/null; then
    notified_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  fi
  filter="null"
  [ -z "$S3_PREFIX" ] || filter=$(jq -cn --arg p "$S3_PREFIX" '{Key: {FilterRules: [{Name: "prefix", Value: $p}]}}')
  wanted=$(printf '%s' "$current" | jq -c --arg id "$NOTIFICATION_ID" --arg q "$queue_arn" --argjson f "$filter" '
    .QueueConfigurations = ([(.QueueConfigurations // [])[] | select(.Id != $id)]
      + [{Id: $id, QueueArn: $q, Events: ["s3:ObjectCreated:*"]} + (if $f then {Filter: $f} else {} end)])')
  awsr s3api put-bucket-notification-configuration --bucket "$bucket" \
    --notification-configuration "$wanted" \
    || die "S3 refused the notification. If another rule on this bucket already covers ObjectCreated under an overlapping prefix, S3 will not add a second — use another --prefix."
  say "    = $NOTIFICATION_ID: s3:ObjectCreated:* on s3://$bucket/$S3_PREFIX -> $QUEUE_NAME (since $notified_at)"

  # --- the reader -----------------------------------------------------------
  # One IAM user for both readers on the Confluent side: the connector (the
  # queue) and dc_presign (GetObject — a presigned URL can do exactly what the
  # key that signed it can). Narrow on purpose; AWS.md has the IAM-role variant.
  step "IAM user"
  if aws iam get-user --user-name "$IAM_USER" >/dev/null 2>&1; then
    say "    = $IAM_USER"
  else
    aws iam create-user --user-name "$IAM_USER" --tags Key=purpose,Value=docling-confluent-ex03 >/dev/null
    say "    + $IAM_USER"
  fi
  local reader
  reader=$(jq -cn --arg b "$bucket_arn" --arg p "$S3_PREFIX" --arg q "$queue_arn" '{
    Version: "2012-10-17",
    Statement: [
      {Sid: "presign", Effect: "Allow", Action: "s3:GetObject", Resource: "\($b)/\($p)*"},
      {Sid: "connector", Effect: "Allow",
       Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
                "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
       Resource: $q}
    ]}')
  aws iam put-user-policy --user-name "$IAM_USER" --policy-name docling-ex03-reader --policy-document "$reader"
  say "    = policy docling-ex03-reader"

  local keys
  keys=$(aws iam list-access-keys --user-name "$IAM_USER" --query 'AccessKeyMetadata[].AccessKeyId' --output text)
  if [ -n "${AWS_READER_KEY_ID:-}" ] && [ -n "${AWS_READER_SECRET:-}" ] && printf '%s' "$keys" | grep -qw "$AWS_READER_KEY_ID"; then
    say "    = access key $AWS_READER_KEY_ID (in .env)"
  else
    # An IAM user has at most two keys; one this script cannot read the secret
    # of is no use to it, so say so rather than fail on the third.
    [ "$(printf '%s' "$keys" | wc -w | tr -d ' ')" -lt 2 ] \
      || die "$IAM_USER already has two access keys and neither is in .env. Delete one: aws iam delete-access-key --user-name $IAM_USER --access-key-id <id>"
    local created
    created=$(aws iam create-access-key --user-name "$IAM_USER" --output json)
    set_example_env --force \
      "AWS_READER_KEY_ID=$(printf '%s' "$created" | jq -r .AccessKey.AccessKeyId)" \
      "AWS_READER_SECRET=$(printf '%s' "$created" | jq -r .AccessKey.SecretAccessKey)"
    dim "    IAM keys take a few seconds to start working everywhere — scripts/aws.sh check, then setup."
  fi

  step "Writing $(basename "$EXAMPLE_DIR")/.env"
  set_example_env "S3_BUCKET=$bucket" "S3_BUCKET_OWNED=$owned" "S3_REGION=$S3_REGION" "S3_PREFIX=$S3_PREFIX" \
    "S3_NOTIFIED_AT=$notified_at" "SQS_QUEUE_URL=$queue_url" "SQS_QUEUE_ARN=$queue_arn"

  say ""
  dim "  Next: scripts/aws.sh check, then ./setup.sh apply"
}

# --- check ------------------------------------------------------------------

cmd_check() {
  need aws jq curl
  names
  require_vars S3_BUCKET SQS_QUEUE_URL AWS_READER_KEY_ID AWS_READER_SECRET
  local ok=1

  step "As you"
  if aws s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1; then say "    ok  bucket $S3_BUCKET"
  else warn "bucket $S3_BUCKET: not found"; ok=0; fi
  if awsr sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL" --attribute-names ApproximateNumberOfMessages \
      --query Attributes.ApproximateNumberOfMessages --output text >/dev/null 2>&1; then
    say "    ok  queue ($(awsr sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL" \
      --attribute-names ApproximateNumberOfMessages --query Attributes.ApproximateNumberOfMessages --output text) waiting)"
  else warn "queue $SQS_QUEUE_URL: not found"; ok=0; fi
  if awsr s3api get-bucket-notification-configuration --bucket "$S3_BUCKET" 2>/dev/null \
      | jq -e --arg id "$NOTIFICATION_ID" '.QueueConfigurations[]? | select(.Id == $id)' >/dev/null; then
    say "    ok  notification $NOTIFICATION_ID"
  else warn "notification $NOTIFICATION_ID: not on the bucket"; ok=0; fi

  # The rest as the reader — the key Confluent will hold — in a subshell, so
  # its credentials never leak into the rest of this script.
  step "As $IAM_USER (the key Confluent holds)"
  (
    export AWS_ACCESS_KEY_ID="$AWS_READER_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_READER_SECRET"
    unset AWS_PROFILE AWS_SESSION_TOKEN
    aws sts get-caller-identity --query Arn --output text >/dev/null 2>&1 \
      || { warn "the key does not authenticate (new keys can take ~10 s)"; exit 1; }
    say "    ok  key $AWS_READER_KEY_ID authenticates"
    awsr sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL" --attribute-names QueueArn >/dev/null 2>&1 \
      || { warn "it cannot read the queue"; exit 1; }
    say "    ok  it can read the queue"
  ) || ok=0

  # The one test that means something end to end: presign an object with the
  # reader's key — the same signature dc_presign computes — and fetch it.
  local key
  key=$(awsr s3api list-objects-v2 --bucket "$S3_BUCKET" --prefix "$S3_PREFIX" --max-items 1 \
        --query 'Contents[0].Key' --output text 2>/dev/null || true)
  if [ -n "$key" ] && [ "$key" != "None" ]; then
    local url code
    url=$(env -u AWS_PROFILE -u AWS_SESSION_TOKEN \
          AWS_ACCESS_KEY_ID="$AWS_READER_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_READER_SECRET" \
          aws --region "$S3_REGION" s3 presign "s3://$S3_BUCKET/$key" --expires-in 60)
    code=$(curl -s -o /dev/null -w '%{http_code}' -r 0-0 "$url")
    if [ "$code" = "206" ] || [ "$code" = "200" ]; then say "    ok  a URL it presigns fetches s3://$S3_BUCKET/$key"
    else warn "a URL it presigns answered HTTP $code for s3://$S3_BUCKET/$key"; ok=0; fi
  else
    dim "    (no object under the prefix yet — scripts/aws.sh samples, then check again to test a presigned fetch)"
  fi

  say ""
  [ "$ok" -eq 1 ] || die "not ready — see above, or AWS.md"
  dim "  Ready. Next: ./setup.sh apply"
}

# --- samples ----------------------------------------------------------------

# The same three as example 02: one tiny PDF, two real papers. Downloaded here
# and uploaded under the prefix as you, so the notification fires for each.
SAMPLES="dummy.pdf=https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf
2206.01062.pdf=https://arxiv.org/pdf/2206.01062
2408.09869.pdf=https://arxiv.org/pdf/2408.09869"

cmd_samples() {
  need aws curl
  names
  require_vars S3_BUCKET
  local line name url
  SAMPLES_TMP=$(mktemp -d)
  trap 'rm -rf "$SAMPLES_TMP"' EXIT
  step "Uploading to s3://$S3_BUCKET/$S3_PREFIX"
  while IFS= read -r line; do
    name="${line%%=*}"; url="${line#*=}"
    curl -fsSL -o "$SAMPLES_TMP/$name" "$url" || die "could not download $url"
    awsr s3 cp --only-show-errors --content-type application/pdf "$SAMPLES_TMP/$name" "s3://$S3_BUCKET/$S3_PREFIX$name"
    say "    + $S3_PREFIX$name"
  done <<EOF
$SAMPLES
EOF
  say ""
  dim "  Each upload is a notification, and each notification one billed Docling conversion"
  dim "  once ./pipeline.sh deploy is running. scripts/report.py follows them."
}

# --- backfill ---------------------------------------------------------------

cmd_backfill() {
  need uv
  exec uv run --quiet --project "$REPO_ROOT" --group aws "$EXAMPLE_DIR/scripts/backfill.py" "$@"
}

# --- destroy ----------------------------------------------------------------

cmd_destroy() {
  need aws jq
  names
  local want_bucket="no" assume_yes="no"
  while [ $# -gt 0 ]; do
    case "$1" in
      --bucket) want_bucket="yes" ;;
      --yes)    assume_yes="yes" ;;
      *) die "destroy: unknown flag $1 (--bucket, --yes)" ;;
    esac
    shift
  done

  step "Plan"
  [ -n "${S3_BUCKET:-}" ] && say "    remove notification   $NOTIFICATION_ID from s3://$S3_BUCKET (others are kept)"
  [ -n "${SQS_QUEUE_URL:-}" ] && say "    delete queue          $SQS_QUEUE_URL"
  say "    delete IAM user       $IAM_USER, its policy and its access keys"
  if [ "$want_bucket" = "yes" ]; then
    if [ "${S3_BUCKET_OWNED:-}" = "yes" ]; then
      printf '%s\n' "    ${C_RED}empty + delete bucket${C_OFF} s3://$S3_BUCKET — every object in it"
    else
      warn "--bucket ignored: ${S3_BUCKET:-the bucket} was yours before this script, and stays"
      want_bucket="no"
    fi
  fi
  dim "    Run ./setup.sh destroy first: the connector fails once its queue is gone."

  if [ "$assume_yes" != "yes" ]; then
    local expected="destroy" reply
    [ "$want_bucket" = "yes" ] && expected="delete the bucket"
    printf '\n    Type "%s" to continue: ' "$expected"
    read -r reply
    [ "$reply" = "$expected" ] || die "cancelled"
  fi

  if [ -n "${S3_BUCKET:-}" ] && aws s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1; then
    step "Notification"
    local current
    current=$(awsr s3api get-bucket-notification-configuration --bucket "$S3_BUCKET" 2>/dev/null || true)
    [ -n "$current" ] || current="{}"
    awsr s3api put-bucket-notification-configuration --bucket "$S3_BUCKET" --notification-configuration \
      "$(printf '%s' "$current" | jq -c --arg id "$NOTIFICATION_ID" \
          '.QueueConfigurations = [(.QueueConfigurations // [])[] | select(.Id != $id)]
           | if .QueueConfigurations == [] then del(.QueueConfigurations) else . end')"
    say "    - $NOTIFICATION_ID"
  fi

  if [ -n "${SQS_QUEUE_URL:-}" ]; then
    step "Queue"
    awsr sqs delete-queue --queue-url "$SQS_QUEUE_URL" 2>/dev/null && say "    - $SQS_QUEUE_URL" || dim "    (already gone)"
  fi

  step "IAM user"
  if aws iam get-user --user-name "$IAM_USER" >/dev/null 2>&1; then
    local id
    for id in $(aws iam list-access-keys --user-name "$IAM_USER" --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
      aws iam delete-access-key --user-name "$IAM_USER" --access-key-id "$id"
      say "    - access key $id"
    done
    aws iam delete-user-policy --user-name "$IAM_USER" --policy-name docling-ex03-reader 2>/dev/null || true
    aws iam delete-user --user-name "$IAM_USER"
    say "    - $IAM_USER"
  else
    dim "    (already gone)"
  fi

  if [ "$want_bucket" = "yes" ]; then
    step "Bucket"
    awsr s3 rb --force "s3://$S3_BUCKET" >/dev/null
    say "    - s3://$S3_BUCKET"
    set_example_env "S3_BUCKET=" "S3_BUCKET_OWNED="
  fi

  set_example_env --force "S3_NOTIFIED_AT=" "SQS_QUEUE_URL=" "SQS_QUEUE_ARN=" "AWS_READER_KEY_ID=" "AWS_READER_SECRET="
  say ""
  dim "  Done. scripts/aws.sh create sets it all up again."
}

case "${1:-}" in
  info)     cmd_info ;;
  create)   shift; cmd_create "$@" ;;
  check)    cmd_check ;;
  samples)  cmd_samples ;;
  backfill) shift; cmd_backfill "$@" ;;
  destroy)  shift; cmd_destroy "$@" ;;
  ""|-h|--help) usage ;;
  *) die "unknown command: $1 (try scripts/aws.sh --help)" ;;
esac
