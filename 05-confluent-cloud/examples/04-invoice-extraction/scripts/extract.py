#!/usr/bin/env python3
"""Run the extraction from the laptop, and score it against the ground truth.

    uv run --project ../.. --group extract scripts/extract.py upload
    uv run --project ../.. --group extract scripts/extract.py run --sample 12
    uv run --project ../.. --group extract scripts/extract.py run --docs lumen-inv-0005,lumen-inv-0042
    uv run --project ../.. --group extract scripts/extract.py run --mode synthetic --all
    uv run --project ../.. --group extract scripts/extract.py score

The same call the pipeline makes — the target out of
``flink/31_mt_extractions.sql``, the URL a submission would carry — without
Kafka or Flink in between, so the extraction can be tried, and its target
tuned, on a handful of invoices before the pipeline pays for all 174.

``--mode real`` calls Docling's extraction API through the Docling Service
Client (``docling.service_client``, from docling-project/docling#4218 until it
is released; the ``extract`` dependency group pins that branch).
``--mode synthetic`` runs ``invoice_udf.synthetic`` — what the pipeline runs
for invoices released in synthetic mode — on the same documents, so the two
can be compared side by side.

Each result is kept as ``data/extractions/<mode>/<doc_id>.json``: the
``ExtractionDocumentResult`` exactly as Docling answered, plus timing. ``score``
re-reads them, runs the pipeline's own ``invoice()`` step on each, and compares
the record with the manifest's ``truth`` — per field, per layout, scans apart.

The PDFs must be reachable by Docling, i.e. public: ``upload`` puts them in the
private Hugging Face bucket ``INVOICE_BUCKET`` names, and every run mints a
signed URL per invoice (``scripts/sources.py``).
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import pathlib
import random
import re
import sys
import time
from collections import defaultdict
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
EXAMPLE = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(EXAMPLE / "udf" / "src"))
sys.path.insert(0, str(EXAMPLE.parent.parent / "common"))
os.environ.setdefault("EXAMPLE_DIR", str(EXAMPLE))

from sources import PREFIX, Sources, filename_of  # noqa: E402

from ccenv import load_env  # noqa: E402

OUT = EXAMPLE / "data" / "extractions"
MAX_PAGES = 100
FIELDS = ("vendor_name", "invoice_number", "invoice_date", "currency", "subtotal", "tax_amount", "total_amount",
          "line_item_count")


# --- inputs ------------------------------------------------------------------


def manifest() -> list[dict[str, Any]]:
    lines = (EXAMPLE / "data" / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def target_from_sql() -> dict[str, Any]:
    """The ``ExtractionTarget`` literal the pipeline sends — the deployed one."""
    sql = (EXAMPLE / "flink" / "20_view_submitted.sql").read_text(encoding="utf-8")
    match = re.search(r"'(\{.*?\})' AS `extraction_target`", sql, re.DOTALL)
    if not match:
        raise SystemExit("could not find the extraction target literal in flink/20_view_submitted.sql")
    return json.loads(match.group(1))


def target_hash(target: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(target, sort_keys=True).encode()).hexdigest()


def pick(records: list[dict[str, Any]], docs: str | None, sample: int | None, everything: bool) -> list[dict]:
    if docs:
        wanted = [d.strip() for d in docs.split(",") if d.strip()]
        by_id = {r["doc_id"]: r for r in records}
        missing = [d for d in wanted if d not in by_id]
        if missing:
            raise SystemExit(f"not in the manifest: {', '.join(missing)}")
        return [by_id[d] for d in wanted]
    if everything:
        return records
    # A spread, not the first N: every layout, the scans, the multi-page ones.
    # Seeded, so two runs of `--sample 12` pick the same invoices and compare.
    rng = random.Random(4)
    n = sample or 8
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        groups[(r["layout"], r["scanned"], r["pages"] > 1)].append(r)
    keys = sorted(groups)
    rng.shuffle(keys)
    out: list[dict] = []
    while len(out) < n and any(groups.values()):
        for key in keys:
            if groups[key] and len(out) < n:
                out.append(groups[key].pop(rng.randrange(len(groups[key]))))
    return sorted(out, key=lambda r: r["doc_id"])


# --- the two extractions -----------------------------------------------------


def failure(url: str, message: str) -> dict[str, Any]:
    """The failure envelope — the shape the UDF returns for anything that went wrong."""
    return {"source_index": 0, "source_uri": url, "filename": filename_of(url), "status": "failure",
            "errors": [{"component_type": "user_input", "module_name": "extract.py", "error_message": message}],
            "items": []}


def synthetic(records: list[dict], target: dict, sources: Sources) -> dict[str, Any]:
    from invoice_udf.synthetic import extract as synthetic_extract

    documents = {}
    for r in manifest():
        doc = dict(r["truth"])
        doc["pages"], doc["layout"] = r["pages"], r["layout"]
        doc["missing"] = (r.get("fake_extraction") or {}).get("missing", [])
        documents[r["doc_id"]] = doc

    def one(record: dict) -> dict[str, Any]:
        url = sources.url(record["doc_id"], record["url"])
        start = time.monotonic()
        envelope = synthetic_extract(url, json.dumps(target), documents)
        return {"envelope": envelope, "seconds": round(time.monotonic() - start, 3)}

    return {r["doc_id"]: one(r) for r in records}


def retryable(envelope: dict[str, Any]) -> bool:
    """Worth asking again: the task failed, or a page the model never answered.

    Docling for IBM watsonx's model server times out under load — a page comes
    back with an error and no ``inference_metadata`` while its neighbour
    extracted fine. Not the error's category: Docling files every item error,
    a schema violation included, as ``inference_failure``, and a page the model
    answered *wrongly* is quality, not availability. Same rule as
    ``DoclingExtract``.
    """
    if envelope.get("status") == "failure" and not envelope.get("items"):
        return True
    return any(i.get("errors") and not i.get("inference_metadata") for i in envelope.get("items") or [])


def real(records: list[dict], target: dict, sources: Sources, env: dict, preset: str | None,
            destination: str, concurrency: int, retries: int) -> dict[str, Any]:
    try:
        import httpx
        from docling.service_client import (
            DoclingServiceClient,
            ExtractDocumentsOptions,
            ExtractionTarget,
            PresignedUrlTarget,
        )
    except ImportError as exc:
        raise SystemExit(f"{exc}\n--mode real needs the extract group: uv run --project ../.. --group extract …")

    url_base, key = env.get("DOCLING_SERVICE_URL"), env.get("DOCLING_API_KEY")
    if not url_base:
        raise SystemExit("DOCLING_SERVICE_URL is not set in .env")
    extraction_target = ExtractionTarget.model_validate(target)
    # An explicit page range, because the client's default is (1, sys.maxsize),
    # and Docling for IBM watsonx fails that task outright ("Internal processing
    # error", retryable) while (1, 100) or no range at all extracts. The
    # invoices here have one or two pages.
    options = ExtractDocumentsOptions(extraction_preset=preset, page_range=(1, MAX_PAGES))
    out_target = PresignedUrlTarget() if destination == "presigned_url" else None  # None: in the body

    def attempt(url: str) -> tuple[dict[str, Any], float | None]:
        try:
            with DoclingServiceClient(url=url_base, api_key=key or "") as client:
                job = client.submit_extract(url, extraction_target=extraction_target, options=options,
                                            **({"target": out_target} if out_target else {}))
                result = job.result()
            wire = result.model_dump(mode="json")
            documents = wire.get("documents") or []
            if not documents:
                envelope = failure(url, "the extraction returned no documents")
            elif "items" in documents[0]:  # in the body: already an ExtractionDocumentResult
                envelope = documents[0]
            else:  # a presigned URL to the same thing, on the service's artifact store
                artifacts = documents[0].get("artifacts") or []
                if not artifacts:
                    envelope = failure(url, f"status {documents[0].get('status')} and no artifacts: "
                                            f"{documents[0].get('errors')}")
                else:
                    got = httpx.get(artifacts[0]["uri"], timeout=60)
                    got.raise_for_status()
                    envelope = got.json()
            return envelope, wire.get("processing_time")
        except Exception as exc:  # noqa: BLE001 — one bad invoice is a row, as in the pipeline
            detail = getattr(exc, "detail", None)
            return failure(url, f"{type(exc).__name__}: {exc}" + (f" ({detail})" if detail else "")), None

    def one(record: dict) -> dict[str, Any]:
        url = sources.url(record["doc_id"], record["url"])
        start = time.monotonic()
        for n in range(1, retries + 2):
            envelope, processing_time = attempt(url)
            if not retryable(envelope) or n > retries:
                break
            time.sleep(2 * n)
        return {"envelope": envelope, "seconds": round(time.monotonic() - start, 2),
                "processing_time": processing_time, "attempts": n}

    results: dict[str, Any] = {}
    with cf.ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = {pool.submit(one, r): r["doc_id"] for r in records}
        for future in cf.as_completed(futures):
            doc_id = futures[future]
            results[doc_id] = future.result()
            env_ = results[doc_id]["envelope"]
            print(f"    {doc_id}  {env_['status']:<15} {results[doc_id]['seconds']:>6.1f}s  "
                  f"x{results[doc_id]['attempts']}  "
                  f"{len(env_.get('items') or [])} item(s)"
                  + (f"  {env_['errors'][0].get('error_message', '')[:120]}" if env_.get("errors") else ""))
    return results


# --- scoring -------------------------------------------------------------------


def _norm(value: Any) -> Any:
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip().casefold()
    return value


def compare(field: str, got: Any, want: Any) -> bool:
    if field in ("subtotal", "tax_amount", "total_amount"):
        return got is not None and want is not None and abs(float(got) - float(want)) <= 0.011
    return _norm(got) == _norm(want)


def scored(label: str, only: set[str] | None = None) -> list[dict[str, Any]]:
    """Each kept result of a run, through ``invoice()``, against the truth."""
    from invoice_udf.invoice import build

    folder = OUT / label
    files = sorted(folder.glob("*.json")) if folder.is_dir() else []
    by_id = {r["doc_id"]: r for r in manifest()}
    rows = []
    for path in files:
        if only and path.stem not in only:
            continue
        saved = json.loads(path.read_text(encoding="utf-8"))
        record = by_id[path.stem]
        submission = {k: record[k] for k in ("doc_id", "url", "department", "category", "submitted_at")}
        invoice = build(saved["envelope"], submission)
        truth = dict(record["truth"], line_item_count=len(record["truth"].get("line_items") or []))
        hits = {f: compare(f, invoice.get(f), truth.get(f)) for f in FIELDS}
        rows.append({"doc_id": path.stem, "layout": record["layout"], "scanned": record["scanned"],
                     "pages": record["pages"], "status": invoice["status"], "hits": hits, "invoice": invoice,
                     "truth": truth, "seconds": saved.get("seconds"), "saved": saved})
    return rows


def score(label: str, only: set[str] | None = None) -> None:
    folder = OUT / label
    rows = scored(label, only)
    if not rows:
        print(f"    nothing in {folder} — run first")
        return

    print(f"\n  {label}: {len(rows)} invoice(s)\n")
    print("    " + f"{'doc':<15}{'layout':<12}{'pg':>3} {'scan':<5}{'status':<8}" +
          "".join(f"{f[:8]:>9}" for f in FIELDS) + "      s")
    for r in rows:
        print("    " + f"{r['doc_id']:<15}{r['layout']:<12}{r['pages']:>3} {'scan' if r['scanned'] else '':<5}"
              f"{r['status']:<8}" + "".join(f"{'✓' if r['hits'][f] else '✗':>9}" for f in FIELDS)
              + f"  {r['seconds'] or 0:>5.1f}")

    def line(label: str, group: list[dict]) -> str:
        cells = "".join(f"{sum(r['hits'][f] for r in group) / len(group):>9.0%}" for f in FIELDS)
        ok = sum(r["status"] == "ok" for r in group)
        return f"    {label:<30}{len(group):>3} {ok:>3} ok " + cells

    print("\n    " + f"{'':<30}{'n':>3} {'':>6}" + "".join(f"{f[:8]:>9}" for f in FIELDS))
    print(line("all", rows))
    for layout in sorted({r["layout"] for r in rows}):
        print(line(f"  {layout}", [r for r in rows if r["layout"] == layout]))
    for name, pred in (("scanned", lambda r: r["scanned"]), ("multi-page", lambda r: r["pages"] > 1)):
        group = [r for r in rows if pred(r)]
        if group:
            print(line(name, group))

    misses = [(r, f) for r in rows for f in FIELDS if not r["hits"][f]]
    if misses:
        print("\n    misses:")
        for r, f in misses[:60]:
            print(f"      {r['doc_id']}  {f:<16} got {r['invoice'].get(f)!r:<40.40} want {r['truth'].get(f)!r:.40}")
        if len(misses) > 60:
            print(f"      … and {len(misses) - 60} more")
    reviews = [r for r in rows if r["status"] == "review"]
    if reviews:
        print("\n    review:")
        for r in reviews:
            print(f"      {r['doc_id']}  {', '.join(r['invoice']['review_reasons'])[:140]}")


# --- commands ------------------------------------------------------------------


def cmd_upload(env: dict) -> None:
    bucket = env.get("INVOICE_BUCKET")
    if not bucket:
        raise SystemExit("set INVOICE_BUCKET=<owner>/<name> (env.sh or .env) first")
    try:
        from huggingface_hub import HfApi, sync_bucket
    except ImportError:
        raise SystemExit("upload needs the extract group: uv run --project ../.. --group extract …")
    api = HfApi()
    api.create_bucket(bucket, private=True, exist_ok=True)
    sync_bucket(str(EXAMPLE / "data" / "invoices"), f"hf://buckets/{bucket}/{PREFIX}")
    print(f"    synced data/invoices/ to hf://buckets/{bucket}/{PREFIX}/ (private)")


def cmd_run(args: argparse.Namespace, env: dict) -> None:
    target = json.loads(pathlib.Path(args.target).read_text()) if args.target else target_from_sql()
    # Both modes on the same signed URLs, as the pipeline would hand Docling.
    sources = Sources.from_env(env, args.base_url)
    records = pick(manifest(), args.docs, args.sample, args.all)
    print(f"    {args.mode}: {len(records)} invoice(s) from {sources.describe()}")
    if args.mode == "synthetic":
        results = synthetic(records, target, sources)
    else:
        results = real(records, target, sources, env, args.preset, args.destination, args.concurrency,
                          args.retries)
    folder = OUT / (args.label or args.mode)
    folder.mkdir(parents=True, exist_ok=True)
    for doc_id, saved in results.items():
        # The signed URL is an hour's read access to the PDF; keep only its path.
        envelope = saved["envelope"]
        envelope["source_uri"] = (envelope.get("source_uri") or "").split("?", 1)[0]
        (folder / f"{doc_id}.json").write_text(json.dumps({"doc_id": doc_id, "mode": args.mode,
                                                           "target_sha256": target_hash(target), **saved},
                                                          indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    score(args.label or args.mode, set(results))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upload", help="put data/invoices/ in the private bucket INVOICE_BUCKET names")
    p_run = sub.add_parser("run", help="extract some invoices and score them")
    p_run.add_argument("--mode", choices=("real", "synthetic"), default="real")
    group = p_run.add_mutually_exclusive_group()
    group.add_argument("--docs", help="comma-separated doc ids")
    group.add_argument("--sample", type=int, help="a spread of N invoices over layouts, scans, pages (default 8)")
    group.add_argument("--all", action="store_true", help="all 174")
    p_run.add_argument("--target", help="a JSON file with an ExtractionTarget to try instead of the SQL's")
    p_run.add_argument("--preset", help="extraction_preset (default: the service's)")
    p_run.add_argument("--destination", choices=("presigned_url", "inbody"), default="presigned_url",
                       help="where Docling puts the result: Docling for IBM watsonx only does presigned_url")
    p_run.add_argument("--concurrency", type=int, default=2)
    p_run.add_argument("--retries", type=int, default=2, help="re-asks on a failed task or a page's inference failure")
    p_run.add_argument("--label", help="keep the results under data/extractions/<label>/ (default: the mode)")
    p_run.add_argument("--base-url", help="fetch the PDFs from here instead of INVOICE_BUCKET")
    p_score = sub.add_parser("score", help="score what earlier runs kept")
    p_score.add_argument("label", nargs="?", default="real", help="real, synthetic, or a run's --label")
    args = parser.parse_args()

    env = load_env(EXAMPLE)
    if args.cmd == "upload":
        cmd_upload(env)
    elif args.cmd == "run":
        cmd_run(args, env)
    else:
        score(args.label)


if __name__ == "__main__":
    main()
