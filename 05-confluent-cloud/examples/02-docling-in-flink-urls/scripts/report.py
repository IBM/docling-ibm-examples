#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Where each document is: converted, chunked, enriched — read from the topics.

    uv run --project ../.. scripts/report.py
    uv run --project ../.. scripts/report.py --watch      # again every 15 s, until ^C

One line per document, from the URL you produced to the records the pipeline
wrote:

    doc_id        conversion             s  chunks  enriched  quarantine  rejected
    2206.01062    success · excellent  10.3      56        56           1         0
    2408.09869    success · excellent  10.5      41        40           0         0
    missing       failure              1.1       0         0           0         0   document status failure …

The stages are separate topics written by separate statements, each visible at
its own commit (about a minute apart), so a document fills in left to right.
`chunks` counts distinct chunk ids — a redeploy re-chunks the history, and the
records repeat, but the chunks do not. `enriched` likewise. A line that stays
short of the one before it is where to look: `./pipeline.sh status`.

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


def report(env: dict[str, str]) -> str:
    consumer = Consumer(kafka_config(env) | {
        "group.id": f"report-{uuid.uuid4()}", "enable.auto.commit": False, "log_level": 0})
    try:
        urls = [_json(m.value()) or {} for m in read_all(consumer, env["URLS_TOPIC"])]
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

    # The URL's own doc_id, or what the chunker would derive: its last path segment.
    order: list[str] = []
    url_of: dict[str, str] = {}
    for row in urls:
        url = row.get("url") or ""
        doc = row.get("doc_id") or url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1].removesuffix(".pdf")
        if doc not in url_of:
            order.append(doc)
        url_of[doc] = url
    latest: dict[str, dict] = {}
    for row in conversions:
        url = row.get("url") or ""
        doc = row.get("doc_id") or next((d for d, u in url_of.items() if u == url), url)
        if doc not in url_of:
            order.append(doc)
            url_of[doc] = url
        latest[doc] = row

    head = f"    {'doc_id':<22} {'conversion':<18} {'s':>5} {'chunks':>7} {'enriched':>9} {'quarantine':>11} {'rejected':>9}"
    lines = [head]
    for doc in order:
        row = latest.get(doc)
        if row is None:
            lines.append(f"    {doc[:22]:<22} {'waiting':<18} {'-':>5}")
            continue
        conv = _json((row.get("conversion") or "").encode()) or {}
        status = row.get("status") or conv.get("status") or "?"
        if conv.get("confidence"):
            status += f" · {conv['confidence']}"
        secs = f"{conv['elapsed_ms'] / 1000:.1f}" if conv.get("elapsed_ms") else "-"
        counts = [len(by_stage[s].get(doc, ())) for s in
                  ("CHUNKS_TOPIC", "ENRICHED_TOPIC", "QUARANTINE_TOPIC", "REJECTED_TOPIC")]
        line = f"    {doc[:22]:<22} {status:<18} {secs:>5} {counts[0]:>7} {counts[1]:>9} {counts[2]:>11} {counts[3]:>9}"
        if conv.get("error"):
            line += f"   {conv['error'][:80]}"
        lines.append(line)
    lines.append(f"\n    {len(urls)} URL record(s), {len(conversions)} conversion(s)")
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
