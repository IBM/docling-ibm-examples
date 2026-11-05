#!/usr/bin/env python3
"""The objects already in the bucket, sent to the queue as if S3 had just announced them.

    scripts/aws.sh backfill --dry-run            what it would send
    scripts/aws.sh backfill                      every object under S3_PREFIX the notification missed
    scripts/aws.sh backfill --prefix inbox/2024/ --limit 10
    scripts/aws.sh backfill --since 2026-10-06T14:00:00Z   what changed since then, notified or not
    scripts/aws.sh backfill --all                every object, notified or not

S3 notifications only cover objects created *after* the notification exists.
This covers the rest: it lists the bucket and sends one message per object
**to the SQS queue**, shaped like the S3 event the notification would have
sent. Into the queue, not onto the Kafka topic — so the connector picks them
up by the same road as a live upload, and the records on the topic are the
same records by construction, whatever format the connector is set to write.

One message per object, one record per message, as S3 itself does it::

    {"Records": [{"eventVersion": "2.1", "eventSource": "aws:s3",
                  "eventName": "ObjectCreated:Backfill", "eventTime": "<LastModified>",
                  "s3": {"bucket": {"name": …, "arn": …},
                         "object": {"key": "inbox/my+report.pdf", "size": …, "eTag": …}}}]}

Three details are deliberate:

* only objects **older than the notification** are sent — ``S3_NOTIFIED_AT``,
  which ``scripts/aws.sh create`` records, plus five minutes' margin for S3 to
  start delivering. Newer ones were announced already, and Flink does not
  deduplicate (22_view_objects.sql says why), so sending them again would
  convert them again. ``--since`` sends what changed after a moment instead —
uploads announced while no convert statement was deployed — and ``--all``
sends everything;

* ``eventName`` is ``ObjectCreated:Backfill`` — not a value S3 sends, but it
  matches ``LIKE 'ObjectCreated:%'`` in 22_view_objects.sql, and it says where
  the record came from. (The connector's ``SenderId`` says it too: your IAM
  identity rather than S3's.)
* the key is form-encoded, ``+`` for a space, exactly as S3 encodes it in an
  event, so the Flink view decodes both the same way.

Run it **after** ``./pipeline.sh deploy``: the convert statement reads the
topic from where it ended at the deploy, so anything sent earlier is skipped.
**Every message it sends is one billed conversion** — run it twice and the
objects are converted twice. ``--dry-run`` first.

Runs as you (boto3's default credentials, or AWS_PROFILE): it needs
s3:ListBucket on the bucket and sqs:SendMessage on the queue, neither of which
the reader key Confluent holds has.
"""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import sys
from urllib.parse import quote_plus

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent.parent.parent / "common"))

import boto3  # noqa: E402

from ccenv import load_env, require  # noqa: E402

BATCH = 10  # SQS's ceiling for send_message_batch
# S3 can take a moment to start delivering a new notification. An object in
# that window is sent by both roads — one extra conversion — rather than by
# neither.
MARGIN = datetime.timedelta(minutes=5)


def event(bucket: str, region: str, obj: dict) -> str:
    """One object as the S3 event its upload would have produced."""
    return json.dumps({"Records": [{
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": region,
        "eventTime": obj["LastModified"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "eventName": "ObjectCreated:Backfill",
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "backfill",
            "bucket": {"name": bucket, "arn": f"arn:aws:s3:::{bucket}"},
            "object": {
                # S3's own encoding of a key in an event: form encoding, with
                # the slashes left alone.
                "key": quote_plus(obj["Key"], safe="/"),
                "size": obj["Size"],
                "eTag": obj["ETag"].strip('"'),
            },
        },
    }]})


def _utc(text: str) -> datetime.datetime:
    """``2026-10-06T14:00:00Z`` (or with an offset, or naive = UTC) -> an aware datetime."""
    moment = datetime.datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.timezone.utc)


def objects(s3, bucket: str, prefix: str):
    """Every real object under the prefix — no folder placeholders."""
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/") and obj["Size"] > 0:
                yield obj


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prefix", help="only keys under this prefix (default: S3_PREFIX)")
    parser.add_argument("--suffix", action="append", default=[],
                        help="only keys ending in this, e.g. .pdf (repeatable; default: every key)")
    parser.add_argument("--limit", type=int, help="at most this many objects")
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--since", help="only objects modified at or after this UTC time (ISO 8601), notified or not")
    window.add_argument("--all", action="store_true",
                        help="also objects newer than the notification (they were announced already)")
    parser.add_argument("--dry-run", action="store_true", help="list what would be sent, send nothing")
    args = parser.parse_args()

    env = load_env(_HERE.parent)
    require(env, "S3_BUCKET", "S3_REGION", "SQS_QUEUE_URL")
    bucket, region, queue = env["S3_BUCKET"], env["S3_REGION"], env["SQS_QUEUE_URL"]
    prefix = env.get("S3_PREFIX", "") if args.prefix is None else args.prefix
    suffixes = tuple(s.lower() for s in args.suffix)
    cutoff = since = None
    if args.since:
        since = _utc(args.since)
    elif not args.all:
        require(env, "S3_NOTIFIED_AT")
        cutoff = _utc(env["S3_NOTIFIED_AT"]) + MARGIN

    session = boto3.Session(region_name=region)
    s3, sqs = session.client("s3"), session.client("sqs")

    sent = failed = skipped = chosen = 0
    batch: list[dict] = []

    def flush() -> None:
        nonlocal sent, failed
        if not batch:
            return
        if not args.dry_run:
            answer = sqs.send_message_batch(QueueUrl=queue, Entries=batch)
            for failure in answer.get("Failed", []):
                print(f"    ! {failure['Id']}: {failure.get('Message', failure.get('Code'))}", file=sys.stderr)
            failed += len(answer.get("Failed", []))
            sent += len(answer.get("Successful", []))
        else:
            sent += len(batch)
        batch.clear()

    print(f"    s3://{bucket}/{prefix} -> {queue.rsplit('/', 1)[-1]}{'   (dry run)' if args.dry_run else ''}")
    if cutoff:
        print(f"    objects last modified before {cutoff:%Y-%m-%d %H:%M:%S} UTC (the notification covers the rest)")
    if since:
        print(f"    objects last modified at or after {since:%Y-%m-%d %H:%M:%S} UTC")
    for obj in objects(s3, bucket, prefix):
        if args.limit is not None and chosen >= args.limit:
            break
        if suffixes and not obj["Key"].lower().endswith(suffixes):
            continue
        if (cutoff and obj["LastModified"] >= cutoff) or (since and obj["LastModified"] < since):
            skipped += 1
            continue
        chosen += 1
        print(f"    {'·' if args.dry_run else '+'} {obj['Key']}  ({obj['Size']:,} bytes)")
        batch.append({"Id": str(len(batch)), "MessageBody": event(bucket, region, obj)})
        if len(batch) == BATCH:
            flush()
    flush()

    verb = "would send" if args.dry_run else "sent"
    print(f"\n    {verb} {sent} message(s){f', {failed} failed' if failed else ''}"
          f"{f'; skipped {skipped} outside the time window (--all sends them too)' if skipped else ''}")
    if not args.dry_run and sent:
        print("    Each one is a Docling conversion. scripts/report.py follows them.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
