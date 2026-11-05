#!/usr/bin/env python3
"""Where each filing is: waiting, or which figures were read — from the topics.

    uv run --project ../.. scripts/report.py
    uv run --project ../.. scripts/report.py --watch        # again every 15 s, until ^C
    uv run --project ../.. scripts/report.py --metrics      # also the figures themselves
    uv run --project ../.. scripts/report.py --events       # the events topic, instead
    uv run --project ../.. scripts/report.py --events --watch   # a live feed of them

One line per filing, from the record sec_producer.py wrote to what the pipeline
produced from it:

    filing                          form    result      metrics
    OPENLANE, Inc.                  10-Q    ok               11
    City Holding Co                 10-Q    ok               11
    Some Bank                       10-K    failed            0   GET /v1/result/… answered HTTP 403
    Some Trust                      10-K    no figures        0   no statement with a known row label

Nothing but the figures is kept (the converted document does not fit a 4 MB
row), so a filing that could not be converted is a row on the metrics topic
whose `metric` is `conversion_failed` and whose `label` says why. A filing
produced again after a failure has both: its figures win. A filing with no row
at all is still waiting, or the statement is not running: `./pipeline.sh status`.

A plain Kafka reader, like example 02's report.py: no Flink statement, nothing billed.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
import uuid

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent.parent.parent / "common"))

from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from ccenv import kafka_config, load_env  # noqa: E402


def _json(raw: bytes | str | None):
    if not raw:
        return None
    if isinstance(raw, bytes) and len(raw) > 5 and raw[0] == 0:  # json-registry framing
        raw = raw[5:]
    try:
        return json.loads(raw)
    except ValueError:
        return None


def read_all(consumer: Consumer, topic: str, keyed: bool = False) -> list[dict]:
    """Every record on a topic, from the start to where it ends now.

    With `keyed`, an upsert topic as the table it stands for: the last value of each
    key, and none for a key whose last record is a tombstone.
    """
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
    out: list[dict] = []
    latest: dict[bytes | None, dict | None] = {}
    while ends:
        message = consumer.poll(10)
        if message is None:
            break
        if message.error():
            continue
        if keyed:
            value = _json(message.value())
            latest.pop(message.key(), None)  # a key written again goes last
            if value is not None:
                latest[message.key()] = value
        else:
            out.append(_json(message.value()) or {})
        if message.offset() + 1 >= ends.get(message.partition(), 0):
            ends.pop(message.partition(), None)
    consumer.unassign()
    return list(latest.values()) if keyed else out


# A filing with none of the figures is one row with one of these as its metric.
PROBLEMS = {"conversion_failed": "failed", "no_figures_found": "no figures"}


def report(env: dict[str, str], show_metrics: bool) -> str:
    consumer = Consumer(kafka_config(env) | {
        "group.id": f"report-{uuid.uuid4()}", "enable.auto.commit": False, "log_level": 0})
    try:
        filings = read_all(consumer, env["FILINGS_TOPIC"])
        metrics = read_all(consumer, env["METRICS_TOPIC"])
    finally:
        consumer.close()

    order: list[str] = []
    name_of: dict[str, tuple[str, str]] = {}
    for row in filings:
        doc = row.get("doc_id") or row.get("accession_number") or "?"
        if doc not in name_of:
            order.append(doc)
        name_of[doc] = (row.get("company_name") or "", row.get("form") or "")
    # A filing produced again writes the same keys again: keep the last value of each.
    figures: dict[str, dict[str, dict]] = {}
    for row in metrics:
        doc = row.get("doc_id") or "?"
        if doc not in name_of:
            order.append(doc)
            name_of[doc] = (row.get("company_name") or "", row.get("form") or "")
        figures.setdefault(doc, {})[row.get("metric") or "?"] = row

    lines = [f"    {'filing':<32} {'form':<6} {'result':<11} {'metrics':>8}"]
    for doc in order:
        name, form = name_of[doc]
        rows = figures.get(doc)
        if not rows:
            lines.append(f"    {name[:32]:<32} {form:<6} {'waiting':<11}")
            continue
        # A failure row has its own key, so an earlier failed attempt stays on the topic beside the
        # figures of a later one that worked: figures win.
        real = {metric: m for metric, m in rows.items() if metric not in PROBLEMS}
        problem = next((p for p in PROBLEMS if p in rows), None)
        if not real and problem:
            lines.append(f"    {name[:32]:<32} {form:<6} {PROBLEMS[problem]:<11} {0:>8}   "
                         f"{str(rows[problem].get('label') or '')[:80]}")
            continue
        lines.append(f"    {name[:32]:<32} {form:<6} {'ok':<11} {len(real):>8}")
        if show_metrics:
            for metric, m in sorted(real.items()):
                scale = m.get("scale") or 1
                lines.append(f"        {metric:<26} {m.get('metric_value')!s:>14}  "
                             f"(prior {m.get('prior_value')!s:>14})  x{scale:<8} {m.get('label') or ''}")
    lines.append(f"\n    {len(filings)} filing(s), {len(metrics)} metric record(s)")
    return "\n".join(lines)


def events(env: dict[str, str]) -> str:
    """The events topic as it stands: newest filing first, high severity before medium."""
    consumer = Consumer(kafka_config(env) | {
        "group.id": f"report-{uuid.uuid4()}", "enable.auto.commit": False, "log_level": 0})
    try:
        rows = read_all(consumer, env["EVENTS_TOPIC"], keyed=True)
    finally:
        consumer.close()
    rows.sort(key=lambda r: (r.get("filing_date") or "", r.get("severity") == "high"), reverse=True)
    lines = [f"    {'severity':<8} {'filing':<28} {'form':<5} {'filed':<10}  event"]
    for row in rows:
        lines.append(f"    {row.get('severity') or '':<8} {(row.get('company_name') or '')[:28]:<28} "
                     f"{row.get('form') or '':<5} {row.get('filing_date') or '':<10}  {row.get('event_type')}")
        lines.append(f"        {row.get('description') or ''}")
    lines.append(f"\n    {len(rows)} event(s)")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--watch", action="store_true", help="again every 15 s, until ^C")
    parser.add_argument("--metrics", action="store_true", help="list each filing's figures")
    parser.add_argument("--events", action="store_true", help="the events topic instead of the filings")
    args = parser.parse_args()
    env = load_env(_HERE.parent)
    while True:
        body = events(env) if args.events else report(env, args.metrics)
        print(f"\n  {time.strftime('%H:%M:%S')}\n{body}", flush=True)
        if not args.watch:
            return
        try:
            time.sleep(15)
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    main()
