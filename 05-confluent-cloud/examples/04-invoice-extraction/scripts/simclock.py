"""The simulation's clock: what has been released, and releasing more of it.

Time in this example is not wall-clock time. It is a date, January 5 to June
30, 2026, and it only ever moves forward, a day at a time, when the control app
says so. "Moving to day D" means producing, in this order:

1. the FX rates for every calendar day up to D that has not been released yet
   (the first move also back-fills November and December 2025, because
   December's invoices are converted at December's rates);
2. every invoice whose ``submitted_at`` falls on such a day.

Both topics are typed — a registered JSON Schema per value, which Flink
registered when it created them — so each record goes out in Confluent's
wire format (``sr.py``). Each record's Kafka timestamp is its simulated time — the rate date at
midnight UTC, the submission instant — so ``$rowtime`` in Flink *is* this
clock. Rates for a day go out before that day's invoices, so a join on the
invoice date never waits and the spend topic stays in event-time order.

There is no local state file. Where the clock stands is read back from the
topics — on start-up and again before every release, so the control app and
``control.py advance`` can both move it: the last FX record says which day was
released, the last submission says how far into that day the invoices got. Restarting the app
continues where it stopped, and the only reset is recreating the topics
(``./setup.sh destroy --topics``), which is also the honest one: Kafka cannot
un-send a record.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import pathlib
import sys
import threading
from dataclasses import dataclass
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
EXAMPLE = HERE.parent
sys.path.insert(0, str(EXAMPLE.parent.parent / "common"))

from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402
from sources import Sources  # noqa: E402
from sr import SchemaRegistry, decode  # noqa: E402

from ccenv import kafka_config, load_env  # noqa: E402

UTC = dt.timezone.utc
SIM_START = dt.date(2026, 1, 5)
SIM_END = dt.date(2026, 6, 30)
CURRENCIES = ("USD", "GBP", "CHF")
#: How a released invoice is extracted — see flink/21_view_extracted.sql.
MODES = ("synthetic", "real")
#: Seconds between real invoices: four extractions run at once, and each has
#: under a minute (DoclingExtract) from the moment Flink hands it over.
REAL_PACE = 3.0


def _ms(when: dt.datetime) -> int:
    return int(when.timestamp() * 1000)


def _midnight(day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(0, 0), UTC)


def _parse_ts(text: str) -> dt.datetime:
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


@dataclass
class World:
    """The manifest and the ECB series — everything that *can* be released."""

    submissions: list[dict[str, Any]]
    rates: dict[dt.date, dict[str, float]]  # ECB publication days only
    first_rate_day: dt.date

    @classmethod
    def load(cls) -> World:
        manifest = EXAMPLE / "data" / "manifest.jsonl"
        if not manifest.exists():
            raise SystemExit(f"{manifest} not found — run scripts/generate.py first")
        subs = []
        for line in manifest.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            # Only what an employee submits. The ground truth stays behind.
            subs.append({"doc_id": rec["doc_id"], "url": rec["url"], "department": rec["department"],
                         "category": rec["category"], "submitted_at": rec["submitted_at"]})
        subs.sort(key=lambda r: r["submitted_at"])
        rates: dict[dt.date, dict[str, float]] = {}
        with open(EXAMPLE / "data" / "ecb_rates.csv", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                rates[dt.date.fromisoformat(row["date"])] = {c: float(row[c]) for c in CURRENCIES}
        return cls(subs, rates, min(rates))

    def fx_records(self, day: dt.date) -> list[dict[str, Any]]:
        """The rates applicable on ``day``: the last ECB publication on or before it."""
        ecb = max((d for d in self.rates if d <= day), default=None)
        if ecb is None:
            return []
        out = [{"currency": "EUR", "rate_date": day.isoformat(), "rate": 1.0, "ecb_date": ecb.isoformat()}]
        out += [{"currency": c, "rate_date": day.isoformat(), "rate": self.rates[ecb][c], "ecb_date": ecb.isoformat()}
                for c in CURRENCIES]
        return out

    def submissions_on(self, day: dt.date) -> list[dict[str, Any]]:
        key = day.isoformat()
        return [s for s in self.submissions if s["submitted_at"].startswith(key)]


class Clock:
    """Release the world, a day at a time, onto the two input topics."""

    def __init__(self, world: World, env: dict[str, str] | None = None, sources: Sources | None = None):
        self.world = world
        self.env = env or load_env(EXAMPLE)
        # Where each submission's URL points. Minted per record as it is
        # released, not when the world is loaded: a signed bucket URL lives an
        # hour, and a submission is extracted seconds after it goes out.
        self.sources = sources or Sources()
        # Set by interrupt(): a release in progress stops between invoices, so
        # Ctrl+C on the control app does not wait out a paced real release.
        self.stopping = threading.Event()
        self.sub_topic = self.env.get("SUBMISSIONS_TOPIC")
        self.fx_topic = self.env.get("FX_TOPIC")
        if not self.sub_topic or not self.fx_topic:
            raise SystemExit("SUBMISSIONS_TOPIC / FX_TOPIC are not set — is EXAMPLE_DIR the example's directory?")
        self.producer = Producer({**kafka_config(self.env), "linger.ms": 20, "enable.idempotence": True})
        self.registry = SchemaRegistry(self.env)
        self.released_through: dt.date | None = None
        self.last_submission: str | None = None
        self.recover()

    # --- where are we -------------------------------------------------------

    def _last_value(self, topic: str) -> dict[str, Any] | None:
        consumer = Consumer({**kafka_config(self.env), "group.id": "ex04-clock-probe", "enable.auto.commit": False})
        try:
            meta = consumer.list_topics(topic, timeout=15).topics.get(topic)
            if meta is None or meta.error is not None:
                raise SystemExit(f"topic {topic} does not exist — ./setup.sh apply first")
            best: tuple[int, dict[str, Any]] | None = None
            for p in meta.partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
                if high <= low:
                    continue
                consumer.assign([TopicPartition(topic, p, high - 1)])
                msg = consumer.poll(15)
                if msg is None or msg.error():
                    continue
                ts = msg.timestamp()[1]
                if best is None or ts > best[0]:
                    best = (ts, decode(msg.value()))
            return best[1] if best else None
        finally:
            consumer.close()

    def recover(self) -> None:
        fx = self._last_value(self.fx_topic)
        sub = self._last_value(self.sub_topic)
        self.released_through = dt.date.fromisoformat(fx["rate_date"]) if fx else None
        self.last_submission = sub["submitted_at"] if sub else None

    @property
    def submitted(self) -> int:
        if self.released_through is None:
            return 0
        limit = self.last_submission or ""
        through = self.released_through.isoformat()
        return sum(1 for s in self.world.submissions
                   if s["submitted_at"][:10] < through or s["submitted_at"] <= limit)

    @property
    def next_day(self) -> dt.date | None:
        if self.released_through is None:
            return SIM_START
        nxt = self.released_through + dt.timedelta(days=1)
        return nxt if nxt <= SIM_END else None

    def state(self) -> dict[str, Any]:
        return {"start": SIM_START.isoformat(), "end": SIM_END.isoformat(),
                "released_through": self.released_through.isoformat() if self.released_through else None,
                "next_day": self.next_day.isoformat() if self.next_day else None,
                "submitted": self.submitted, "total": len(self.world.submissions)}

    # --- moving it ----------------------------------------------------------

    def _produce(self, topic: str, key: str, value: dict[str, Any], when: dt.datetime) -> None:
        self.producer.produce(topic, key=key.encode(), value=self.registry.encode(topic, value), timestamp=_ms(when))
        self.producer.poll(0)

    def _release_day(self, day: dt.date, after: str | None = None, pace: float = 0.0,
                     mode: str = "synthetic") -> int:
        for rec in self.world.fx_records(day) if after is None else []:
            self._produce(self.fx_topic, rec["currency"], rec, _midnight(day))
        n = 0
        for sub in self.world.submissions_on(day):
            if self.stopping.is_set():
                break
            if after is not None and sub["submitted_at"] <= after:
                continue
            record = {**sub, "url": self.sources.url(sub["doc_id"], sub["url"], real=mode == "real"), "mode": mode}
            self._produce(self.sub_topic, sub["doc_id"], record, _parse_ts(sub["submitted_at"]))
            if pace > 0:
                # One invoice at a time: out to Kafka now, then a pause, so a
                # busy day reaches a slow extraction as a trickle, not a burst.
                self.producer.flush(30)
                self.stopping.wait(pace)
            n += 1
        return n

    def advance_to(self, target: dt.date, pace: float = 0.0, mode: str = "synthetic") -> dict[str, Any]:
        """Release everything up to and including ``target``. Never goes back.

        ``mode`` goes on every submission released: ``synthetic`` (answered
        from the ground truth) or ``real`` (Docling, on a signed bucket URL).
        ``pace`` seconds between invoices (0: a day at once). The real
        extraction wants a few: Confluent Cloud gives each call about a minute,
        and twelve invoices arriving together queue past it.
        """
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
        # Where the topics stand now, not where this process last left them:
        # the control app and `control.py advance` are two clocks over the same
        # topics, and one that trusted its memory would release a day the other
        # already had — the same invoices, twice.
        self.recover()
        target = min(target, SIM_END)
        if self.released_through is not None and target <= self.released_through:
            return {"released_days": 0, "released_invoices": 0, **self.state()}
        days = invoices = 0
        # Finish a day a previous run released only partly: its rates are out,
        # some of its submissions may not be.
        if self.released_through is not None:
            invoices += self._release_day(self.released_through, after=self.last_submission or "", pace=pace,
                                          mode=mode)
        day = self.released_through + dt.timedelta(days=1) if self.released_through else self.world.first_rate_day
        while day <= target:
            invoices += self._release_day(day, pace=pace, mode=mode)
            days += 1
            # Flushed per day, so that what recover() reads back is a day
            # boundary or the middle of one day — never a gap.
            if self.producer.flush(30) > 0:
                raise RuntimeError("Kafka did not acknowledge every record within 30 s")
            self.released_through = day
            day += dt.timedelta(days=1)
            if self.stopping.is_set():
                break  # what went out is on the topics; recover() picks up from there
        if self.released_through is None:
            return {"released_days": days, "released_invoices": invoices, **self.state()}
        subs = self.world.submissions_on(self.released_through)
        if subs:
            self.last_submission = max(s["submitted_at"] for s in subs)
        else:
            # Nothing was submitted on the last released day; the last submission
            # is whichever earlier day had one.
            earlier = [s["submitted_at"] for s in self.world.submissions
                       if s["submitted_at"][:10] <= self.released_through.isoformat()]
            self.last_submission = max(earlier) if earlier else None
        return {"released_days": days, "released_invoices": invoices, **self.state()}

    def interrupt(self) -> None:
        """Stop a release in progress, between two invoices."""
        self.stopping.set()

    def close(self) -> None:
        self.producer.flush(5)
