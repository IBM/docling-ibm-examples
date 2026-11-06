#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Read a topic back, one record per block, with the derived fields called out.

    uv run common/drain_topic.py --topic dol.ex01.chunks.enriched
    uv run common/drain_topic.py --topic dol.ex01.pii.quarantine --raw

The enriched record is a JSON object with the producer's fields untouched and
the derived ones added alongside, so diffing the chunk topic against the
enriched topic is the way to see what the pipeline did.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from confluent_kafka import Consumer

from ccenv import kafka_config, load_env

# The fields the pipeline adds, in the order they become interesting.
DERIVED = (
    "normalized", "fingerprint", "char_count", "word_count", "token_count",
    "heading_path", "page_start", "content_type", "script",
    "quality_flags", "keep", "pii_count", "pii_types", "pii_redacted",
    "rejected_reason", "ingested_at",
)


def render(record: dict, raw: bool) -> str:
    if raw:
        return json.dumps(record, indent=2, ensure_ascii=False)
    head = f"{record.get('doc_id')}#{record.get('chunk_index')}  {record.get('chunk_id', '')[:12]}"
    text = (record.get("text") or "").replace("\n", " ")
    lines = [head, f"    {text[:160]}{'…' if len(text) > 160 else ''}"]
    shown = {k: record[k] for k in DERIVED if k in record}
    lines.append("    " + "  ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in shown.items()))
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--max", type=int, default=50, help="stop after this many records")
    parser.add_argument("--timeout", type=float, default=10.0, help="seconds of silence before giving up")
    parser.add_argument("--raw", action="store_true", help="print the whole record")
    args = parser.parse_args()

    env = load_env()
    consumer = Consumer(
        kafka_config(env)
        | {
            # A throwaway group every run: this is a reader, and it must never
            # take partitions away from anything that is actually consuming.
            "group.id": f"drain-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    seen = 0
    try:
        while seen < args.max:
            message = consumer.poll(args.timeout)
            if message is None:
                break
            if message.error():
                print(f"! {message.error()}", file=sys.stderr)
                continue
            seen += 1
            value = message.value()
            # A typed topic (value.format = json-registry) frames each value:
            # a zero magic byte and a 4-byte schema id, then the JSON.
            if value and len(value) > 5 and value[0] == 0:
                value = value[5:]
            try:
                record = json.loads(value)
            except (TypeError, ValueError):
                print(f"{seen}. <not JSON> {message.value()!r}")
                continue
            print(f"\n{seen}. " + render(record, args.raw))
    finally:
        consumer.close()
    print(f"\n    {seen} record(s) from {args.topic}")


if __name__ == "__main__":
    main()
