#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""The invoices as an evaluation dataset, in the Hugging Face bucket.

    uv run --project ../.. --group extract scripts/dataset.py build      # -> data/dataset/, to look at
    uv run --project ../.. --group extract scripts/dataset.py publish    # build, then sync to INVOICE_BUCKET

The bucket already holds the PDFs, because Docling fetches them from there
(``scripts/sources.py``). This adds what makes them a dataset someone else can
evaluate an extraction on:

    README.md                          the dataset card — generated here, from the data
    invoices/<doc_id>.pdf              the 174 invoices (uploaded by `extract.py upload`)
    ground_truth/manifest.jsonl        what is printed on each invoice, per field and line
    target/extraction_target.json      the ExtractionTarget the pipeline sends to Docling
    target/invoice.schema.json         its output_schema alone
    extractions/docling/<run>/
        run.json                       service, preset, target hash, date, settings
        results.jsonl                  Docling's ExtractionDocumentResult per invoice, verbatim
        scores.jsonl                   per invoice and field: predicted, expected, match
        SCORES.md                      the summary table

A run is a ``scripts/extract.py run --label <label>`` kept under
``data/extractions/<label>/``; ``--run`` picks which (repeat it for several),
and each is published under its own name. Scoring is ``extract.py``'s — the
pipeline's own ``invoice()`` step, then a field-by-field comparison — so the
numbers on the card are the numbers ``extract.py score`` prints.

Sync adds and updates; it never deletes, so runs published earlier stay.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import os
import pathlib
import shutil
import sys
from typing import Any

HERE = pathlib.Path(__file__).resolve().parent
EXAMPLE = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(EXAMPLE / "udf" / "src"))
sys.path.insert(0, str(EXAMPLE.parent.parent / "common"))
os.environ.setdefault("EXAMPLE_DIR", str(EXAMPLE))

import extract  # noqa: E402

from ccenv import load_env  # noqa: E402

BUILD = EXAMPLE / "data" / "dataset"

#: The default run: all 174 against Docling for IBM watsonx, 2026-10-05.
DEFAULT_RUNS = ("all-b=2026-10-05_granite-vision-v4",)

LAYOUTS = {
    "de_classic": "German DIN-style business letter: `Rechnung`, `1.234,56 €`, `zzgl. 19 % USt.`, dates `09.01.2026`",
    "de_modern": "German, modern single-column variant of the above",
    "us_saas": "US SaaS bill on Letter paper: `$1,234.56`, `Jan 5, 2026` (or `01/05/2026`), metered lines",
    "eu_saas": "EU SaaS invoice on A4, reverse-charge VAT note, unit column",
    "uk_legal": "UK solicitors' fee note: narrative time entries, hours × rate, `30 December 2025`",
    "ch_qr": "Swiss invoice with a QR payment part: `CHF 1'234.50`",
    "hotel_folio": "Portuguese hotel folio: per-night lines, three IVA rates, tourist tax",
}

FIELD_NOTES = {
    "vendor_name": "exact after case-folding and collapsing whitespace",
    "invoice_number": "exact, same normalisation",
    "invoice_date": "ISO date after parsing (local formats accepted; a slash date is month-first on USD invoices)",
    "currency": "exact ISO 4217 code",
    "subtotal": "within 0.01",
    "tax_amount": "within 0.01",
    "total_amount": "within 0.01",
    "line_item_count": "number of line items, all pages merged",
}


def _jsonl(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def _json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def ground_truth(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The manifest, minus what only the demo uses: the localhost URL, and the
    synthetic extraction's instructions (which fields *it* pretends to miss)."""
    out = []
    for r in records:
        out.append({"doc_id": r["doc_id"], "file": f"invoices/{r['doc_id']}.pdf", "layout": r["layout"],
                    "vendor_key": r["vendor_key"], "scanned": r["scanned"], "pages": r["pages"],
                    "department": r["department"], "category": r["category"], "submitted_at": r["submitted_at"],
                    "truth": r["truth"]})
    return out


def _pct(rows: list[dict], field: str) -> str:
    return f"{sum(r['hits'][field] for r in rows) / len(rows):.0%}"


def summary_table(rows: list[dict[str, Any]]) -> str:
    fields = extract.FIELDS
    head = "| | n | ok | " + " | ".join(f.replace("_", " ") for f in fields) + " |"
    lines = [head, "|---" * (len(fields) + 3) + "|"]

    def line(label: str, group: list[dict]) -> str:
        ok = sum(r["status"] == "ok" for r in group)
        return f"| {label} | {len(group)} | {ok} | " + " | ".join(_pct(group, f) for f in fields) + " |"

    lines.append(line("**all**", rows))
    for layout in sorted({r["layout"] for r in rows}):
        lines.append(line(f"`{layout}`", [r for r in rows if r["layout"] == layout]))
    for label, pred in (("scanned", lambda r: r["scanned"]), ("single-page", lambda r: r["pages"] == 1),
                        ("multi-page", lambda r: r["pages"] > 1)):
        group = [r for r in rows if pred(r)]
        if group:
            lines.append(line(label, group))
    return "\n".join(lines)


def build_run(label: str, name: str, target: dict[str, Any], out: pathlib.Path) -> dict[str, Any]:
    rows = extract.scored(label)
    if not rows:
        raise SystemExit(f"no results under data/extractions/{label}/ — run `extract.py run --label {label}` first")
    want = extract.target_hash(target)
    hashes = {r["saved"].get("target_sha256") for r in rows}
    if hashes - {None, want}:
        raise SystemExit(f"{label}: its results were extracted with another target than flink/31_mt_extractions.sql's")
    modes = {r["saved"].get("mode") for r in rows}
    # "docling" is what real runs were called before the modes were named.
    folder = out / "extractions" / ("docling" if modes <= {"docling", "real"} else "-".join(sorted(m or "?" for m in modes)))
    folder = folder / name
    results, scores = [], []
    for r in rows:
        saved = r["saved"]
        results.append({"doc_id": r["doc_id"], "extraction": saved["envelope"], "seconds": saved.get("seconds"),
                        "processing_time": saved.get("processing_time"), "attempts": saved.get("attempts")})
        scores.append({"doc_id": r["doc_id"], "layout": r["layout"], "scanned": r["scanned"], "pages": r["pages"],
                       "status": r["status"], "review_reasons": r["invoice"].get("review_reasons"),
                       "fields": {f: {"match": r["hits"][f], "predicted": r["invoice"].get(f),
                                      "expected": r["truth"].get(f)} for f in extract.FIELDS}})
    seconds = sorted(x["seconds"] for x in results if x["seconds"] is not None)
    run = {"name": name, "mode": sorted(modes), "invoices": len(rows),
           "service": "Docling for IBM watsonx (staging), POST /v1/extract/source/async",
           "extraction_preset": "granite_vision_v4_kserve (the service default, and the only one it allows)",
           "client": "docling-slim[service-client] from docling-project/docling#4218 "
                     "(branch cau/extraction-api-service-models), via scripts/extract.py",
           "options": {"page_range": [1, extract.MAX_PAGES], "target": "presigned_url"},
           "retries": "up to 2 more attempts when the task failed or a page got no model answer",
           "target_sha256": want,
           "extracted_on": dt.datetime.fromtimestamp(min(p.stat().st_mtime for p in (extract.OUT / label).glob("*.json")),
                                                     dt.timezone.utc).date().isoformat(),
           "seconds_per_invoice": {"median": seconds[len(seconds) // 2], "max": seconds[-1]} if seconds else None}
    _json(folder / "run.json", run)
    _jsonl(folder / "results.jsonl", results)
    _jsonl(folder / "scores.jsonl", scores)
    table = summary_table(rows)
    (folder / "SCORES.md").write_text(f"# {name}\n\n{table}\n", encoding="utf-8")
    return {"name": name, "path": folder.relative_to(out).as_posix(), "run": run, "table": table, "rows": rows}


def card(records: list[dict[str, Any]], target: dict[str, Any], runs: list[dict[str, Any]], bucket: str) -> str:
    layouts = collections.Counter(r["layout"] for r in records)
    currencies = collections.Counter(r["truth"]["currency"] for r in records)
    vendors = {r["truth"]["vendor_name"] for r in records}
    dates = [r["truth"]["invoice_date"] for r in records]
    lines_total = sum(len(r["truth"].get("line_items") or []) for r in records)
    schema = target["output_schema"]
    props = schema["properties"]
    required = set(schema.get("required", []))

    def kind(prop: dict) -> str:
        if "anyOf" in prop:
            return " or ".join(o.get("type", "?") for o in prop["anyOf"])
        if prop.get("type") == "array":
            return "array of LineItem"
        return prop.get("type", "?") + (f", `{prop['format']}`" if "format" in prop else "") \
            + (f", `{prop['pattern']}`" if "pattern" in prop else "")

    schema_rows = "\n".join(f"| `{n}` | {kind(p)} | {'yes' if n in required else ''} | {p.get('description', '')} |"
                            for n, p in props.items())
    line_item = schema["$defs"]["LineItem"]
    li_rows = "\n".join(f"| `{n}` | {kind(p)} | {'yes' if n in line_item.get('required', []) else ''} |"
                        for n, p in line_item["properties"].items())
    layout_rows = "\n".join(f"| `{k}` | {layouts[k]} | {v} |" for k, v in LAYOUTS.items())
    field_rows = "\n".join(f"| `{k}` | {v} |" for k, v in FIELD_NOTES.items())
    truth_fields = ", ".join(f"`{k}`" for k in records[0]["truth"] if k != "line_items")
    run_sections = "\n\n".join(
        f"### `{r['name']}`\n\n{r['run']['invoices']} invoices, {r['run']['service']}, preset "
        f"`granite_vision_v4_kserve`, extracted {r['run']['extracted_on']}; median "
        f"{r['run']['seconds_per_invoice']['median']} s per invoice. Files: `{r['path']}/`.\n\n{r['table']}"
        for r in runs)

    return f"""# Lumen Analytics invoices — an extraction evaluation set

> **Fictional.** Every company, person, address, VAT id and IBAN here is
> invented; every page says so along its edge. Generated for the Docling ×
> Confluent Cloud demo (`05-confluent-cloud/examples/04-invoice-extraction` in
> `IBM/docling-ibm-examples`), and usable on its own to evaluate
> structured extraction from invoices.

**{len(records)} supplier invoices** of a fictional Munich company, Lumen
Analytics GmbH, dated {min(dates)} to {max(dates)}: **{len(vendors)} vendors**,
**{len(layouts)} layouts**, {", ".join(f"{n} in {c}" for c, n in currencies.most_common())};
**{sum(r['scanned'] for r in records)} scans** (rasterized, tilted, noisy, no text layer) and
**{sum(r['pages'] > 1 for r in records)} two-page invoices**; {lines_total} line items in all. Each comes with
its ground truth — what is printed on it, field by field and line by line —
because the generator drew it from that.

## Files

| path | what |
|---|---|
| `invoices/<doc_id>.pdf` | the invoices |
| `ground_truth/manifest.jsonl` | one line per invoice: layout, scanned, pages, and `truth` |
| `target/extraction_target.json` | the extraction target the demo sends to Docling: JSON Schema + example + instructions |
| `target/invoice.schema.json` | its `output_schema` alone |
| `extractions/docling/<run>/results.jsonl` | Docling's answer per invoice, verbatim (`ExtractionDocumentResult`) |
| `extractions/docling/<run>/scores.jsonl` | per invoice and field: predicted, expected, match |
| `extractions/docling/<run>/run.json`, `SCORES.md` | how the run was made; its summary |

This is a private Hugging Face Storage Bucket, `{bucket}`:

```bash
hf buckets sync hf://buckets/{bucket} ./lumen-invoices
```

## The invoices

| layout | n | what it looks like |
|---|---|---|
{layout_rows}

The variety is the point: the same fields printed seven ways — German and
Swiss number formats, US and European dates, net/tax/total blocks with one,
two or three VAT rates, a two-page cloud bill whose metered lines spill over,
a hotel folio whose lines are nights. One template seventeen times would be
template matching, not extraction.

## Ground truth

`ground_truth/manifest.jsonl`, one JSON object per invoice:

```json
{{"doc_id": "lumen-inv-0001", "file": "invoices/lumen-inv-0001.pdf", "layout": "uk_legal",
 "vendor_key": "kestrel", "scanned": false, "pages": 1,
 "department": "Finance", "category": "Professional Services", "submitted_at": "2026-01-05T08:20:51Z",
 "truth": {{"vendor_name": "Kestrel & Finch LLP", "invoice_number": "KF/2025/0351",
           "invoice_date": "2025-12-30", "currency": "GBP", "total_amount": 7510.5, "…": "…",
           "line_items": [{{"description": "…", "quantity": 6.3, "unit_price": 395.0, "amount": 2488.5,
                           "page": 1}}]}}}}
```

`truth` holds {truth_fields} and `line_items` (`description`, `quantity`,
`unit_price`, `amount`, and the `page` it is printed on). Amounts are numbers
in the invoice's currency, dates ISO. `department`, `category` and
`submitted_at` are the demo's (who submitted the invoice, when); they are not
on the page.

## What is asked: the extraction target

`target/extraction_target.json` is a Docling `ExtractionTarget`
(docling-project/docling#4218): `output_schema` (the contract, validated per
page), `template` (an `example_json`: one made-up invoice shown as an example
answer) and `instructions`. It asks for a subset of the truth:

| field | type | required | description |
|---|---|---|---|
{schema_rows}

`LineItem`:

| field | type | required |
|---|---|---|
{li_rows}

## Evaluating

Per invoice, the pages' answers are merged — header fields from the first page
that has them, totals from the last, line items from all — and compared with
`truth`:

| field | matches when |
|---|---|
{field_rows}

`ok` is the demo pipeline's own verdict on the merged record: vendor, date,
currency and total present, and subtotal + tax = total. The reference
implementation is `scripts/extract.py score` with `invoice_udf.invoice.build`.
Line items are counted, not matched line by line — `scores.jsonl` and
`results.jsonl` have what is needed for a finer measure.

## Runs

{run_sections}

Things worth knowing about these runs:

- **Multi-page invoices**: page 1 of each carries 20+ line items, and the
  service's model server timed out on it (20 s) on every attempt — the page has
  an error and no data, so dates and line counts miss. A service limit at the
  time, not the model's answer.
- **Vendor names** miss where the model "corrects" a name (`Volterra` for
  `Voltara`, `Biurebedarf` for `Bürobedarf`) or drops a suffix (`, Inc.`).
- **Dates** come back as printed (`20.02.2026`, `30 December 2025`), despite
  the schema's `format: date` and the instructions; Docling does not validate
  `format`. They are scored after parsing.
- A page whose answer breaks the schema has `extracted_data: null` in Docling's
  result and the answer only in `raw_text`; the scoring reads it from there.

Generated by `scripts/dataset.py` on {dt.datetime.now(dt.timezone.utc).date().isoformat()}; edit that, not this file.
"""


def build(env: dict[str, str], run_specs: list[str], out: pathlib.Path) -> pathlib.Path:
    if out.exists():
        shutil.rmtree(out)
    records = extract.manifest()
    target = extract.target_from_sql()
    _jsonl(out / "ground_truth" / "manifest.jsonl", ground_truth(records))
    _json(out / "target" / "extraction_target.json", target)
    _json(out / "target" / "invoice.schema.json", target["output_schema"])
    runs = []
    for spec in run_specs:
        label, _, name = spec.partition("=")
        runs.append(build_run(label, name or label, target, out))
    bucket = env.get("INVOICE_BUCKET") or "<owner>/<bucket>"
    (out / "README.md").write_text(card(records, target, runs, bucket), encoding="utf-8")
    print(f"    built {out.relative_to(EXAMPLE)}/: {len(records)} invoices' truth, the target, "
          f"{len(runs)} run(s) — {', '.join(r['name'] for r in runs)}")
    return out


def publish(env: dict[str, str], out: pathlib.Path) -> None:
    bucket = env.get("INVOICE_BUCKET")
    if not bucket:
        raise SystemExit("set INVOICE_BUCKET=<owner>/<name> first")
    try:
        from huggingface_hub import sync_bucket
    except ImportError:
        raise SystemExit("publish needs the extract group: uv run --project ../.. --group extract …")
    # Everything but the PDFs, which `extract.py upload` puts there. No delete:
    # earlier runs stay.
    sync_bucket(str(out), f"hf://buckets/{bucket}")
    print(f"    synced to hf://buckets/{bucket}/ — https://huggingface.co/buckets/{bucket}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("cmd", choices=("build", "publish"))
    parser.add_argument("--run", action="append", metavar="LABEL[=NAME]",
                        help=f"a kept extract.py run, published as NAME (default: {DEFAULT_RUNS[0]})")
    parser.add_argument("--out", type=pathlib.Path, default=BUILD)
    args = parser.parse_args()
    env = load_env(EXAMPLE)
    out = build(env, args.run or list(DEFAULT_RUNS), args.out)
    if args.cmd == "publish":
        publish(env, out)


if __name__ == "__main__":
    main()
