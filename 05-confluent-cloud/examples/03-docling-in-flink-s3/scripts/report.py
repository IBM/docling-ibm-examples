#!/usr/bin/env python3
"""Where each object is: announced, converted, chunked, enriched — read from the topics.

    uv run --project ../.. scripts/report.py
    uv run --project ../.. scripts/report.py --watch      # again every 15 s, until ^C

One line per object, from the S3 event the connector delivered to the records
the pipeline wrote:

    key                      via       conversion             s  chunks  enriched  quarantine  rejected
    inbox/dummy.pdf          s3        success · good       2.1       1         1           0         0
    inbox/2206.01062.pdf     backfill  success · excellent  9.8      56        56           1         0

`via` is where the event came from: `s3` for a live upload, `backfill` for
scripts/backfill.py. An object announced twice is one line here, and two
conversions on the topic — the last one is shown. A line stuck at
`waiting` was announced before the convert statement's deploy (it reads from
there on) — run the backfill again — or the statement is not running:
`./pipeline.sh status`.

A plain Kafka reader, like `drain_topic.py`: no Flink statement, nothing billed.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
import uuid
from collections import defaultdict
from urllib.parse import unquote_plus

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent.parent.parent / "common"))

from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from ccenv import kafka_config, load_env  # noqa: E402


def _json(raw: bytes | None):
    if not raw:
        return None
    if len(raw) > 5 and raw[0] == 0:  # json-registry framing
        raw = raw[5:]
    try:
        return json.loads(raw)
    except ValueError:
        return None


def read_all(consumer: Consumer, topic: str):
    """Every record on a topic, from the start to where it ends now."""
    metadata = consumer.list_topics(topic, timeout=30).topics.get(topic)
    if metadata is None or metadata.error is not None:
        return []
    ends = {}
    assignment = []
    for partition in metadata.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
        if high > low:
            ends[partition] = high
            assignment.append(TopicPartition(topic, partition, low))
    if not assignment:
        return []
    consumer.assign(assignment)
    out = []
    while ends:
        message = consumer.poll(10)
        if message is None:
            break
        if message.error():
            continue
        out.append(message)
        if message.offset() + 1 >= ends.get(message.partition(), 0):
            ends.pop(message.partition(), None)
    consumer.unassign()
    return out


def announced(message) -> tuple[str, str, str] | None:
    """An objects-topic record -> (key, etag, via), or None for anything not an upload."""
    record = _json(message.value()) or {}
    body = record.get("Body") or record.get("body")
    event = _json(body.encode()) if isinstance(body, str) else None
    records = (event or {}).get("Records") or []
    if not records or not str(records[0].get("eventName", "")).startswith("ObjectCreated:"):
        return None
    obj = records[0].get("s3", {}).get("object", {})
    via = "backfill" if records[0]["eventName"] == "ObjectCreated:Backfill" else "s3"
    return unquote_plus(obj.get("key", "")), obj.get("eTag", ""), via


def report(env: dict[str, str]) -> str:
    consumer = Consumer(kafka_config(env) | {
        "group.id": f"report-{uuid.uuid4()}", "enable.auto.commit": False, "log_level": 0})
    try:
        events = [e for e in (announced(m) for m in read_all(consumer, env["OBJECTS_TOPIC"])) if e]
        conversions = [_json(m.value()) or {} for m in read_all(consumer, env["CONVERSIONS_TOPIC"])]
        by_stage = {}
        for stage in ("CHUNKS_TOPIC", "ENRICHED_TOPIC", "QUARANTINE_TOPIC", "REJECTED_TOPIC"):
            seen = defaultdict(set)
            for message in read_all(consumer, env[stage]):
                record = _json(message.value()) or {}
                seen[record.get("doc_id")].add(record.get("chunk_id"))
            by_stage[stage] = seen
    finally:
        consumer.close()

    # doc_id is the object key (23_view_converted.sql), so the two line up.
    order: list[str] = []
    via: dict[str, str] = {}
    versions: set[tuple[str, str]] = set()
    for key, etag, source in events:
        if key not in via:
            order.append(key)
            via[key] = source
        versions.add((key, etag))
    latest: dict[str, dict] = {}
    for row in conversions:
        doc = row.get("doc_id") or ""
        if doc not in via:
            order.append(doc)
            via[doc] = "-"
        latest[doc] = row

    head = (f"    {'key':<28} {'via':<9} {'conversion':<18} {'s':>5} {'chunks':>7} {'enriched':>9}"
            f" {'quarantine':>11} {'rejected':>9}")
    lines = [head]
    for doc in order:
        row = latest.get(doc)
        if row is None:
            lines.append(f"    {doc[-28:]:<28} {via[doc]:<9} {'waiting':<18} {'-':>5}")
            continue
        conv = _json((row.get("conversion") or "").encode()) or {}
        status = row.get("status") or conv.get("status") or "?"
        if conv.get("confidence"):
            status += f" · {conv['confidence']}"
        secs = f"{conv['elapsed_ms'] / 1000:.1f}" if conv.get("elapsed_ms") else "-"
        counts = [len(by_stage[s].get(doc, ())) for s in
                  ("CHUNKS_TOPIC", "ENRICHED_TOPIC", "QUARANTINE_TOPIC", "REJECTED_TOPIC")]
        line = (f"    {doc[-28:]:<28} {via[doc]:<9} {status:<18} {secs:>5} {counts[0]:>7} {counts[1]:>9}"
                f" {counts[2]:>11} {counts[3]:>9}")
        if conv.get("error"):
            line += f"   {conv['error'][:80]}"
        lines.append(line)
    lines.append(f"\n    {len(events)} event(s) for {len(versions)} object version(s), {len(conversions)} conversion(s)")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--watch", action="store_true", help="again every 15 s, until ^C")
    args = parser.parse_args()
    env = load_env(_HERE.parent)
    while True:
        print(f"\n  {time.strftime('%H:%M:%S')}\n{report(env)}", flush=True)
        if not args.watch:
            return
        try:
            time.sleep(15)
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    main()
