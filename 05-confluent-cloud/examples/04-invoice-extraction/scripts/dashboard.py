"""What the dashboard shows, read from the topics.

A background consumer reads every topic of the example from the beginning —
the two inputs as well as everything Flink writes — and keeps the latest value
per key: the upsert topics converge, the append ones accumulate. The page shows
what Flink computed, not a second computation in Python; the only arithmetic
here is summing the monthly rows into department and category totals.

Two clocks come out of it, and the gap between them is worth watching:
"submitted through" is the control app's; "processed through" is the newest
Kafka timestamp on the invoice and review topics, i.e. how far the extraction
statement has got through the simulated days. Per-topic record counts feed the
pipeline diagram; the input records and the per-invoice stages feed the
Inputs view, which follows one invoice from its message to its report row.

Reads are ``read_committed`` (librdkafka's default): Flink writes its topics
transactionally, and an uncommitted record is not a result yet. Values are
Schema Registry framed JSON (``sr.decode``); the review queue is the invoices
topic's ``status = 'review'`` rows — one table, both outcomes.
"""

from __future__ import annotations

import datetime as dt
import json
import threading
import uuid
from collections import OrderedDict, deque
from typing import Any

from confluent_kafka import Consumer, KafkaError
from sr import decode

UTC = dt.timezone.utc

TOPIC_VARS = ("SUBMISSIONS_TOPIC", "FX_TOPIC", "EXTRACTIONS_TOPIC", "INVOICES_TOPIC", "SPEND_TOPIC",
              "DAILY_TOPIC", "MONTHLY_TOPIC", "VENDORS_TOPIC")
#: Columns the invoices table carries as JSON strings, parsed for display.
JSON_COLUMNS = ("review_reasons", "line_items", "warnings")


def _parse_json_columns(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    for col in JSON_COLUMNS:
        if isinstance(out.get(col), str):
            try:
                out[col] = json.loads(out[col])
            except ValueError:
                pass
    return out


def _iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Dashboard:
    def __init__(self, env: dict[str, str]):
        from ccenv import kafka_config

        self.env = env
        self.by_topic = {env[var]: var for var in TOPIC_VARS if env.get(var)}
        self.consumer = Consumer({**kafka_config(env), "group.id": f"ex04-dashboard-{uuid.uuid4()}",
                                  "auto.offset.reset": "earliest", "enable.auto.commit": False,
                                  "topic.metadata.refresh.interval.ms": 5000})
        self.lock = threading.Lock()
        # Bumped on every change, so the page's event stream can wait for one
        # instead of the page asking every second.
        self.changed = threading.Condition(self.lock)
        self.version = 0
        self.count = {topic: 0 for topic in self.by_topic}
        self.last_ms = {topic: 0 for topic in self.by_topic}
        self.daily: dict[str, dict] = {}
        self.monthly: dict[str, dict] = {}
        self.vendors: dict[str, dict] = {}
        self.review: dict[str, dict] = {}
        self.submissions: OrderedDict[str, dict] = OrderedDict()
        self.fx: deque[dict] = deque(maxlen=16)
        self.extractions: dict[str, dict] = {}
        self.invoices: dict[str, dict] = {}
        self.spend: dict[str, dict] = {}
        self.feed: deque[dict] = deque(maxlen=12)
        self.processed_ms = 0
        self.spend_total = 0.0
        self.errors: deque[str] = deque(maxlen=5)
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="dashboard", daemon=True)

    def start(self) -> Dashboard:
        self.consumer.subscribe(list(self.by_topic))
        self.thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self.thread.join(5)
        self.consumer.close()

    def _run(self) -> None:
        while not self._stop.is_set():
            msg = self.consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                # A topic Flink has not created yet is not an error worth more
                # than a line on the page; the subscription picks it up later.
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    self.errors.append(str(msg.error()))
                continue
            self._apply(msg)

    def _apply(self, msg) -> None:
        topic = msg.topic()
        var = self.by_topic.get(topic)
        key = (msg.key() or b"").decode("utf-8", "replace")
        try:
            value = decode(msg.value())
        except ValueError:
            return
        ts = msg.timestamp()[1]
        with self.lock:
            self.count[topic] = self.count.get(topic, 0) + 1
            self.last_ms[topic] = max(self.last_ms.get(topic, 0), ts)
            if var in ("MONTHLY_TOPIC", "VENDORS_TOPIC", "DAILY_TOPIC"):
                table = {"MONTHLY_TOPIC": self.monthly, "VENDORS_TOPIC": self.vendors, "DAILY_TOPIC": self.daily}[var]
                if value is None:
                    table.pop(key, None)
                else:
                    table[key] = value
            elif var == "SUBMISSIONS_TOPIC":
                self.submissions[key] = {"key": key, "timestamp": _iso(ts), "value": value}
            elif var == "FX_TOPIC":
                self.fx.appendleft({"key": key, "timestamp": _iso(ts), "value": value})
            elif var == "EXTRACTIONS_TOPIC" and value:
                self.extractions[key] = value
            elif var == "INVOICES_TOPIC" and value:
                value = _parse_json_columns(value)
                if value.get("status") == "ok":
                    self.invoices[key] = value
                    self.review.pop(key, None)
                else:
                    self.review[key] = value
                    self.invoices.pop(key, None)
                self.processed_ms = max(self.processed_ms, ts)
            elif var == "SPEND_TOPIC" and value:
                if key not in self.spend:
                    self.spend_total += value.get("amount_eur") or 0.0
                self.spend[key] = value
                self.feed.appendleft(value)
            self.version += 1
            self.changed.notify_all()

    def bump(self) -> None:
        """Something outside the topics changed (the clock moved): tell the streams."""
        with self.lock:
            self.version += 1
            self.changed.notify_all()

    def wait_change(self, seen: int, timeout: float) -> int:
        """Block until the version differs from ``seen``, or ``timeout`` passes."""
        with self.lock:
            self.changed.wait_for(lambda: self.version != seen, timeout)
            return self.version

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            dept: dict[str, float] = {}
            cat: dict[str, float] = {}
            months: dict[str, dict[str, float]] = {}
            for row in self.monthly.values():
                amount = row.get("amount_eur") or 0.0
                dept[row["department"]] = dept.get(row["department"], 0.0) + amount
                cat[row["category"]] = cat.get(row["category"], 0.0) + amount
                m = months.setdefault(row["month"], {})
                m[row["department"]] = m.get(row["department"], 0.0) + amount
            days: dict[str, dict[str, float]] = {}
            for row in self.daily.values():
                days.setdefault(row["day"], {})[row["department"]] = row.get("amount_eur") or 0.0
            vendors = sorted(self.vendors.values(), key=lambda v: -(v.get("amount_eur") or 0.0))
            review = sorted(self.review.values(), key=lambda r: r.get("submitted_at") or "", reverse=True)
            processed = (dt.datetime.fromtimestamp(self.processed_ms / 1000, UTC).strftime("%Y-%m-%d %H:%M")
                         if self.processed_ms else None)
            return {
                "processed_through": processed,
                # One count per stage, so the page can say where an invoice is.
                # The report counts are exact: each monthly and vendor row
                # carries how many invoices it sums, so their totals say how
                # many converted invoices a report already includes.
                "counts": {"ok": len(self.invoices), "review": len(self.review), "converted": len(self.spend),
                           "extracted": len(self.extractions),
                           "in_monthly": sum(int(r.get("invoices") or 0) for r in self.monthly.values()),
                           "in_vendors": sum(int(r.get("invoices") or 0) for r in self.vendors.values())},
                "topics": {t: {"records": n, "last": _iso(self.last_ms[t]) if self.last_ms.get(t) else None}
                           for t, n in self.count.items()},
                "spend_total_eur": round(self.spend_total, 2),
                "by_department": dept, "by_category": cat,
                "monthly": dict(sorted(months.items())),
                "daily": dict(sorted(days.items())[-42:]),
                "vendors": vendors[:12],
                "review": [{k: r.get(k) for k in ("doc_id", "url", "vendor_name", "department", "submitted_at",
                                                   "review_reasons", "total_amount", "currency")}
                           for r in review[:15]],
                "feed": list(self.feed),
                "errors": list(self.errors),
            }

    def inputs(self, limit: int = 15) -> dict[str, Any]:
        """The newest records on the two input topics, as produced."""
        with self.lock:
            subs = list(self.submissions.values())[-limit:][::-1]
            for s in subs:
                s["stage"] = self._stage(s["key"])
            return {"submissions": subs, "fx": list(self.fx)}

    def _stage(self, doc_id: str) -> str:
        if doc_id in self.spend:
            return "converted"
        if doc_id in self.review:
            return "review"
        if doc_id in self.invoices:
            return "accepted"
        if doc_id in self.extractions:
            return "extracted"
        return "in flight"

    def trace(self, doc_id: str) -> dict[str, Any]:
        """One invoice through every stage, each as the record on its topic."""
        with self.lock:
            extraction = self.extractions.get(doc_id)
            return {"doc_id": doc_id, "source": "consumer", "submission": self.submissions.get(doc_id),
                    "extraction": extraction,
                    "invoice": self.invoices.get(doc_id) or self.review.get(doc_id),
                    "spend": self.spend.get(doc_id), "stage": self._stage(doc_id)}
