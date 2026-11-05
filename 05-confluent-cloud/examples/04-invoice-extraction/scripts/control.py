#!/usr/bin/env python3
"""The control layer: a simulated clock with a slider, and the live dashboard.

    uv run --project ../.. --group control scripts/control.py serve      # http://localhost:8004
    uv run --project ../.. scripts/control.py status
    uv run --project ../.. scripts/control.py seed                  # the first SEED_DAYS days, once
    uv run --project ../.. scripts/control.py advance --to 2026-02-15
    uv run --project ../.. scripts/control.py advance --days 7
    uv run --project ../.. scripts/control.py advance --days 1 --mode real   # through Docling

The clock lives in ``simclock.py``; this is the front end to it. ``serve`` adds
a web page with the slider, a play button, and the dashboard — whose numbers
come from the topics Flink writes (``dashboard.py``), not from anything
computed here — and serves the PDFs, so the invoice links on the page open.

Every release has a **mode**, chosen on the page beside the timeline (or
``--mode`` here): ``synthetic`` — answered from the ground truth, free and
instant — or ``real`` — Docling's extraction, billed, seconds per page. It goes
on each submission, and the pipeline sends it to one extraction or the other
(``flink/21_view_extracted.sql``). A real invoice carries a signed URL into the
private bucket ``INVOICE_BUCKET`` names, minted as it is released
(``sources.py``), and real invoices go out a few seconds apart; a synthetic one
carries this app's own /invoices URL. ``--base-url`` points real invoices at
any other public host instead.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import functools
import json
import os
import pathlib
import re
import sys
import threading

HERE = pathlib.Path(__file__).resolve().parent
EXAMPLE = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(EXAMPLE.parent.parent / "common"))
os.environ.setdefault("EXAMPLE_DIR", str(EXAMPLE))

from lightning import Lightning, LightningError, topic_status  # noqa: E402
from simclock import MODES, REAL_PACE, SIM_END, SIM_START, Clock, World  # noqa: E402
from sources import Sources  # noqa: E402

from ccenv import load_env  # noqa: E402

# The web front end only — `status` and `advance` need none of it, and run
# without `--group control`. At module level rather than inside serve() because
# FastAPI resolves a parameter's annotation by name in the module's globals.
try:
    from fastapi import FastAPI, HTTPException, Request
except ImportError:  # pragma: no cover
    FastAPI = HTTPException = Request = None


async def _json(request: Request) -> dict:
    """The request body as a dict; an empty body is ``{}``."""
    raw = await request.body()
    if not raw:
        return {}
    body = json.loads(raw)
    return body if isinstance(body, dict) else {}


DOC_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _invoice_pdf(pdfs: pathlib.Path, doc_id: str) -> pathlib.Path | None:
    """The invoice's PDF, or None when doc_id, which comes from a URL, is not an id or leaves ``pdfs``."""
    if not DOC_ID.fullmatch(doc_id):
        return None
    root = os.path.realpath(pdfs)
    path = os.path.realpath(os.path.join(root, f"{doc_id}.pdf"))
    if not path.startswith(root + os.sep):
        return None
    return pathlib.Path(path)


@functools.lru_cache(maxsize=256)
def _render_page(path: pathlib.Path, page: int) -> bytes | None:
    if not path.is_file() or page < 1:
        return None
    import io

    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(str(path))
    try:
        if page > len(pdf):
            return None
        buf = io.BytesIO()
        pdf[page - 1].render(scale=1.4).to_pil().save(buf, "PNG", optimize=True)
        return buf.getvalue()
    finally:
        pdf.close()


def _page_count(path: pathlib.Path) -> int:
    if not path.is_file():
        return 0
    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(str(path))
    try:
        return len(pdf)
    finally:
        pdf.close()


def company_summary() -> dict:
    """The whole simulated half-year, by department, from the files on disk.

    Everything in the dataset, not what has been released so far — this is
    the "about the data" view, and its numbers come from the manifest's
    ground truth converted at the ECB rate of each invoice date, so they stay
    right if the data is regenerated with another seed.
    """
    import csv
    from collections import Counter, defaultdict

    rates = {}
    with open(EXAMPLE / "data" / "ecb_rates.csv", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rates[row["date"]] = row
    dates = sorted(rates)

    def to_eur(truth: dict) -> float:
        if truth["currency"] == "EUR":
            return truth["total_amount"]
        day = max((d for d in dates if d <= truth["invoice_date"]), default=dates[0])
        return truth["total_amount"] / float(rates[day][truth["currency"]])

    records = [json.loads(line) for line in (EXAMPLE / "data" / "manifest.jsonl").read_text().splitlines() if line]
    depts: dict = defaultdict(lambda: {"invoices": 0, "eur": 0.0, "categories": Counter(), "vendors": Counter(),
                                       "vendor_n": Counter()})
    for r in records:
        d, eur = depts[r["department"]], to_eur(r["truth"])
        d["invoices"] += 1
        d["eur"] += eur
        d["categories"][r["category"]] += eur
        d["vendors"][r["truth"]["vendor_name"]] += eur
        d["vendor_n"][r["truth"]["vendor_name"]] += 1
    return {
        "invoices": len(records),
        "eur": round(sum(d["eur"] for d in depts.values()), 2),
        "vendors": len({r["truth"]["vendor_name"] for r in records}),
        "currencies": Counter(r["truth"]["currency"] for r in records),
        "layouts": len({r["layout"] for r in records}),
        "scanned": sum(r["scanned"] for r in records),
        "multipage": sum(r["pages"] > 1 for r in records),
        "first": min(r["submitted_at"] for r in records)[:10],
        "last": max(r["submitted_at"] for r in records)[:10],
        "departments": [
            {"name": name, "invoices": d["invoices"], "eur": round(d["eur"], 2),
             "categories": [{"name": c, "eur": round(v, 2)} for c, v in d["categories"].most_common()],
             "vendors": [{"name": v, "eur": round(x, 2), "invoices": d["vendor_n"][v]}
                         for v, x in d["vendors"].most_common(5)]}
            for name, d in sorted(depts.items(), key=lambda kv: -kv[1]["eur"])
        ],
    }


def cmd_status(clock: Clock) -> None:
    s = clock.state()
    print(f"    released through  {s['released_through'] or '(nothing yet)'}")
    print(f"    next day          {s['next_day'] or '(the end — June 30 is released)'}")
    print(f"    submitted         {s['submitted']} of {s['total']} invoices")


def cmd_seed(clock: Clock, env: dict[str, str], days: int | None, pace: float | None,
             mode: str = "synthetic") -> None:
    """The first few days, once — what a fresh deploy starts with.

    Enough for every table and the dashboard to have something in them, and
    most of the half-year left to release by hand. Only on a clock that has not
    moved: run again, after any release, it does nothing — so a redeploy never
    ingests more.
    """
    days = int(env.get("SEED_DAYS") or 3) if days is None else days
    if days <= 0:
        print("    SEED_DAYS=0: nothing seeded")
        return
    if clock.released_through is not None:
        print(f"    already released through {clock.released_through}: nothing seeded")
        return
    if pace is None:
        pace = REAL_PACE if mode == "real" else 0.0
    target = SIM_START + dt.timedelta(days=days - 1)
    out = clock.advance_to(target, pace=pace, mode=mode)
    print(f"    seeded {out['released_invoices']} {mode} invoices, {SIM_START} to {out['released_through']}"
          + (f", {pace:g} s apart" if pace else "") + " — release the rest from the control app")


def cmd_advance(clock: Clock, to: str | None, days: int | None, mode: str = "synthetic") -> None:
    if to:
        target = dt.date.fromisoformat(to)
    else:
        start = clock.next_day or SIM_END
        target = start + dt.timedelta(days=(days or 1) - 1)
    out = clock.advance_to(target, pace=REAL_PACE if mode == "real" else 0.0, mode=mode)
    print(f"    +{out['released_invoices']} {mode} invoices over {out['released_days']} day(s); "
          f"released through {out['released_through']}")


def serve(clock: Clock, env: dict[str, str], port: int) -> None:
    if FastAPI is None:
        raise SystemExit("serve needs the control dependencies: uv run --project ../.. --group control ...")
    import uvicorn
    from dashboard import Dashboard
    from fastapi.responses import FileResponse, Response, StreamingResponse
    from fastapi.staticfiles import StaticFiles
    from pipeline_info import Pipeline

    board = Dashboard(env).start()
    pipeline = Pipeline(env)
    app = FastAPI(title="Example 04 — control")
    lock = threading.Lock()
    # The mode is the server's, not the page's: every open page shows the same
    # one, and Play keeps releasing in the mode it was started with until it is
    # changed. Synthetic by default — free, and always available.
    play = {"task": None, "interval": 2.0, "mode": "synthetic"}
    # Set on the first Ctrl+C / SIGTERM (see Server below). Every long-lived
    # thing checks it: the event streams end, Play stops, a paced release stops
    # between invoices — so uvicorn has no open connection to wait for.
    stopping = threading.Event()

    pdfs = EXAMPLE / "data" / "invoices"
    if pdfs.is_dir():
        app.mount("/invoices", StaticFiles(directory=pdfs), name="invoices")

    def state() -> dict:
        return {"clock": clock.state(), "playing": play["task"] is not None, "interval": play["interval"],
                "mode": play["mode"], "real_unavailable": clock.sources.real_unavailable, "real_pace": REAL_PACE,
                "names": {k: env.get(k) for k in ("SUBMISSIONS_TOPIC", "SPEND_TOPIC", "NS")},
                **board.snapshot()}

    def set_mode(mode: str | None) -> None:
        if mode is None:
            return
        if mode not in MODES:
            raise HTTPException(400, f"mode must be one of {', '.join(MODES)}")
        if mode == "real" and clock.sources.real_unavailable:
            raise HTTPException(409, f"real mode is unavailable: {clock.sources.real_unavailable}")
        play["mode"] = mode

    def step(to: dt.date) -> dict:
        mode = play["mode"]
        with lock:
            out = clock.advance_to(to, pace=REAL_PACE if mode == "real" else 0.0, mode=mode)
        board.bump()  # the clock is part of what the page shows
        return out

    @app.get("/")
    def index():
        # no-cache: the page is edited while the app runs, and a browser that
        # keeps yesterday's copy shows controls the API no longer matches.
        return FileResponse(HERE / "static" / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/api/state")
    def api_state():
        return state()

    @app.get("/api/calendar")
    def api_calendar():
        """Submissions per simulated day, for the histogram over the slider."""
        counts: dict[str, int] = {}
        for sub in clock.world.submissions:
            counts[sub["submitted_at"][:10]] = counts.get(sub["submitted_at"][:10], 0) + 1
        day, out = SIM_START, []
        while day <= SIM_END:
            out.append({"day": day.isoformat(), "count": counts.get(day.isoformat(), 0)})
            day += dt.timedelta(days=1)
        return {"days": out}

    @app.get("/api/pipeline")
    async def api_pipeline():
        """The graph (from flink/), each statement's live state, the SQL."""
        out = await asyncio.to_thread(pipeline.snapshot)
        out["topics_live"] = board.snapshot()["topics"]
        out["rtce"] = await asyncio.to_thread(topic_status, env)
        out["names"] = {var: env.get(var) for var in (
            "SUBMISSIONS_TOPIC", "FX_TOPIC", "EXTRACTIONS_TOPIC", "INVOICES_TOPIC", "SPEND_TOPIC",
            "DAILY_TOPIC", "MONTHLY_TOPIC", "VENDORS_TOPIC", "FN_EXTRACT_SYNTHETIC", "FN_EXTRACT_REAL", "FN_INVOICE",
            "APP_NAME", "NS")}
        return out

    # --- lightning queries ------------------------------------------------------
    # The Real-Time Context Engine, through its MCP endpoint (lightning.py). The
    # page works without it — everything else reads the topics — and says what
    # is missing when it is not there: the engine turned on for the topics
    # (./setup.sh context-engine on) and a *global* API key (./account.sh
    # global-key) in .env.

    lightning = {"client": None, "error": None}

    def lq() -> Lightning | None:
        if lightning["client"] is None and lightning["error"] is None:
            try:
                lightning["client"] = Lightning(env)
            except LightningError as exc:
                lightning["error"] = str(exc)
        return lightning["client"]

    @app.get("/api/lightning")
    async def api_lightning():
        client = lq()
        status = await asyncio.to_thread(topic_status, env)
        rtce_topics = [env[v] for v in (env.get("RTCE_TOPIC_VARS") or "").split() if env.get(v)]
        return {"configured": client is not None, "global_key": bool(client and client.global_key),
                "endpoint": client.endpoint if client else None, "error": lightning["error"],
                "topics": {t: status.get(t, "OFF") for t in rtce_topics}}

    @app.post("/api/lightning/query")
    async def api_lightning_query(request: Request):  # {"topic": ..., "sql": ..., "max_rows": 50}
        body = await _json(request)
        client = lq()
        if client is None:
            raise HTTPException(503, lightning["error"] or "the Real-Time Context Engine is not configured")
        try:
            return await asyncio.to_thread(client.query, body.get("topic", ""), body.get("sql", ""),
                                           int(body.get("max_rows") or 50))
        except LightningError as exc:
            hint = ("" if client.global_key else
                    " — the engine's MCP endpoint takes a *global* API key: set GLOBAL_KEY=\"yes\" in .env and "
                    "run ./account.sh apply, or create one in the console and set RTCE_API_KEY / RTCE_API_SECRET in .env")
            raise HTTPException(502, f"{exc}{hint}") from exc

    def inputs() -> dict:
        nxt = clock.next_day
        preview = {"day": nxt.isoformat() if nxt else None,
                   "submissions": clock.world.submissions_on(nxt) if nxt else [],
                   "fx": clock.world.fx_records(nxt) if nxt else []}
        return {**board.inputs(), "next": preview}

    summary = company_summary()

    @app.get("/api/company")
    def api_company():
        """The dataset described: per department, what it spends on, with whom."""
        return summary

    @app.get("/api/inputs")
    def api_inputs():
        """The newest input records as produced, and what the next day will add."""
        return inputs()

    # --- the push channel --------------------------------------------------------
    # One Server-Sent Events stream per page, instead of the page asking every
    # second. The consumer bumps a version on every record it applies and the
    # clock on every day it releases; this waits on that and sends
    #
    #   event: state     the dashboard state and the Inputs tab's records
    #   event: pipeline  the statements' phase and the engine's status
    #
    # A burst — a released month is a hundred records — is coalesced: after a
    # send the stream waits a quarter of a second before looking again, so the
    # page redraws a few times per second at most, not once per record. The
    # pipeline status has no push of its own (the Flink and Cloud APIs are
    # request/response), so it is the one thing still checked on a timer — here,
    # once per stream every ten seconds against a cached snapshot, and sent only
    # when it differs.
    PIPELINE_EVERY = 10.0
    COALESCE = 0.25

    def pipeline_event() -> dict:
        snap = pipeline.snapshot()
        return {"objects": {k: {f: v.get(f) for f in ("phase", "version", "stopped", "detail")}
                            for k, v in snap["objects"].items() if v.get("phase")},
                "rtce": topic_status(env)}

    def sse(event: str, data: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"

    @app.get("/api/events")
    async def api_events(request: Request):
        async def stream():
            seen, last_pipeline, last_state, next_check = -1, None, None, 0.0
            loop = asyncio.get_running_loop()
            while not stopping.is_set() and not await request.is_disconnected():
                now = loop.time()
                if now >= next_check:
                    current = await asyncio.to_thread(pipeline_event)
                    if current != last_pipeline:
                        last_pipeline = current
                        yield sse("pipeline", current)
                    next_check = now + PIPELINE_EVERY
                version = await asyncio.to_thread(board.wait_change, seen, min(5.0, max(0.1, next_check - now)))
                if version != seen:
                    seen = version
                    # Most records change nothing the page shows (an FX rate,
                    # a report row rewritten with the same value): send only
                    # what differs from the last thing this stream sent.
                    payload = sse("state", {**state(), "inputs": inputs()})
                    if payload != last_state:
                        last_state = payload
                        yield payload
                        await asyncio.sleep(COALESCE)
                else:
                    yield ": keep-alive\n\n"  # a comment line: keeps proxies from closing an idle stream
        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/api/trace/{doc_id}")
    async def api_trace(doc_id: str, source: str = "auto"):
        """One invoice, stage by stage — from lightning queries when the engine
        answers, from the dashboard's own consumer otherwise."""
        pdf = _invoice_pdf(pdfs, doc_id)
        out = {**board.trace(doc_id), "pages": _page_count(pdf) if pdf else 0}
        client = lq()
        if source == "consumer" or client is None or not client.global_key:
            return out
        # The id goes into SQL, so it is held to the shape ids have here.
        if not DOC_ID.fullmatch(doc_id):
            raise HTTPException(400, "not a document id")
        lookups = {"extraction": env.get("EXTRACTIONS_TOPIC"), "invoice": env.get("INVOICES_TOPIC"),
                   "spend": env.get("SPEND_TOPIC")}
        # Stage by stage: one the engine cannot serve yet (a table still
        # materializing) keeps the consumer's copy, and the page marks only the
        # stages that came back as lightning queries.
        timings, errors = {}, {}
        for stage, topic in lookups.items():
            try:
                got = await asyncio.to_thread(client.query, topic,
                                              f"SELECT * FROM \"{topic}\" WHERE doc_id = '{doc_id}' LIMIT 1", 1)
            except LightningError as exc:
                errors[stage] = str(exc)[:300]
                continue
            if got["rows"]:
                out[stage] = got["rows"][0]
                timings[stage] = got["ms"]
        if timings:
            out["source"] = "lightning"
        out["timings"], out["lightning_errors"] = timings, errors
        return out

    # The invoice as images, one per page. Not the PDF in an iframe: whether a
    # browser shows PDFs inline is a setting (and some embedded ones cannot),
    # while a PNG shows everywhere. Rendered once per page and kept.
    @app.get("/preview/{doc_id}/{page}.png")
    def preview(doc_id: str, page: int):
        pdf = _invoice_pdf(pdfs, doc_id)
        png = _render_page(pdf, page) if pdf else None
        if png is None:
            raise HTTPException(404, "no such invoice page — run scripts/generate.py?")
        return Response(png, media_type="image/png", headers={"Cache-Control": "max-age=3600"})

    @app.post("/api/mode")
    async def api_mode(request: Request):  # {"mode": "synthetic" | "real"}
        set_mode((await _json(request)).get("mode"))
        board.bump()  # every open page shows the mode
        return state()

    @app.post("/api/advance")
    async def api_advance(request: Request):  # {"to": "YYYY-MM-DD"} or {"days": n}, optional "mode"
        body = await _json(request)
        set_mode(body.get("mode"))
        if body.get("to"):
            target = dt.date.fromisoformat(body["to"])
        else:
            nxt = clock.next_day
            if nxt is None:
                raise HTTPException(409, "the simulation is at its end")
            target = nxt + dt.timedelta(days=max(1, int(body.get("days") or 1)) - 1)
        await asyncio.to_thread(step, target)
        return state()

    async def player():
        try:
            while clock.next_day is not None and not stopping.is_set():
                await asyncio.to_thread(step, clock.next_day)
                await asyncio.sleep(play["interval"])
        finally:
            play["task"] = None
            board.bump()

    @app.post("/api/play")
    async def api_play(request: Request):  # {"interval": seconds per simulated day}, optional "mode"
        body = await _json(request)
        set_mode(body.get("mode"))
        play["interval"] = max(0.2, float(body.get("interval") or 2.0))
        if play["task"] is None and clock.next_day is not None:
            play["task"] = asyncio.create_task(player())
        board.bump()  # every open page shows the play state
        return state()

    @app.post("/api/pause")
    async def api_pause():
        task = play["task"]
        if task is not None:
            task.cancel()
            play["task"] = None
        board.bump()
        return state()

    class Server(uvicorn.Server):
        """uvicorn, with a shutdown that does not hang.

        uvicorn's graceful shutdown waits for open connections to close, and
        every open page holds one: the Server-Sent Events stream, which never
        ends by itself. So the first signal also tells everything long-lived
        to stop (and wakes the streams waiting for a change), and a second
        Ctrl+C exits on the spot, whatever is still running.
        """

        def handle_exit(self, sig: int, frame) -> None:
            if stopping.is_set():
                print("\n    stopping now", file=sys.stderr)
                os._exit(130)
            print("\n    shutting down (Ctrl+C again to force)", file=sys.stderr)
            stopping.set()
            clock.interrupt()
            board.bump()
            super().handle_exit(sig, frame)

    print(f"\n    control + dashboard on http://localhost:{port} — Ctrl+C to stop\n")
    server = Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning",
                                   timeout_graceful_shutdown=3))
    try:
        server.run()
    finally:
        board.stop()
        clock.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", help="where Docling fetches real invoices (default: signed INVOICE_BUCKET URLs)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_serve = sub.add_parser("serve", help="the web page: slider, play, dashboard")
    p_serve.add_argument("--port", type=int, default=8004)
    sub.add_parser("status", help="where the clock stands")
    p_seed = sub.add_parser("seed", help="release the first SEED_DAYS days, if nothing is released yet")
    p_seed.add_argument("--days", type=int, help="how many days (default: SEED_DAYS, 3)")
    p_seed.add_argument("--pace", type=float, help="seconds between invoices (default: 3 for real, 0 for synthetic)")
    p_seed.add_argument("--mode", choices=MODES, default="synthetic", help="how the seed is extracted (default: synthetic)")
    p_adv = sub.add_parser("advance", help="release more days")
    group = p_adv.add_mutually_exclusive_group()
    group.add_argument("--to", help="release through this date (YYYY-MM-DD)")
    group.add_argument("--days", type=int, help="release this many more days (default 1)")
    p_adv.add_argument("--mode", choices=MODES, default="synthetic",
                       help="synthetic (ground truth) or real (Docling); default synthetic")
    args = parser.parse_args()

    env = load_env(EXAMPLE)
    sources = Sources.from_env(env, args.base_url)
    print(f"    {sources.describe()}")
    clock = Clock(World.load(), env, sources)
    if args.cmd == "status":
        cmd_status(clock)
    elif args.cmd == "seed":
        cmd_seed(clock, env, args.days, args.pace, args.mode)
        clock.close()
    elif args.cmd == "advance":
        cmd_advance(clock, args.to, args.days, args.mode)
        clock.close()
    else:
        serve(clock, env, args.port)


if __name__ == "__main__":
    main()
