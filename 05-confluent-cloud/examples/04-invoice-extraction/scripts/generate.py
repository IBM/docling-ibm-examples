#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Generate the demo's invoices: PDFs, and the manifest that describes them.

    uv run --project ../.. --group invoices scripts/generate.py

writes

    data/invoices/<doc_id>.pdf   ~170 invoices, 7 layouts, a few of them scans
    data/manifest.jsonl          one line per invoice — what gets submitted, and
                                 what is actually on the page

Deterministic, to the byte: the same seed gives the same invoices, numbers,
submission times and PDFs — the scans' noise and the PDFs' dates included.
The PDFs are committed anyway, beside the manifest: they are the fixture a real
extraction is scored against, and a fixture should not depend on which
version of a JPEG encoder regenerated it. Run this only when the generator
changes, and commit what it writes.

The manifest is the whole world of the simulation, and it has two halves that
must not be confused:

* ``department``, ``category``, ``submitted_at``, ``url`` — what an employee
  submits. This, and only this, goes on the submissions topic.
* ``truth`` — what is printed on the invoice. Nothing in the pipeline reads it.
  The fake extraction UDF is built from it (it has to answer from *something*
  until the real Docling call is wired in), and it is what you check the real
  extraction against later.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import random
import sys
import time
from collections import Counter, defaultdict
from decimal import Decimal
from zoneinfo import ZoneInfo

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from company import BUYER, SIM_END, SIM_START, Invoice, business_day, plan  # noqa: E402
from invoice_pdf import render  # noqa: E402

DATA = _HERE.parent / "data"
BERLIN = ZoneInfo("Europe/Berlin")

#: Where the PDFs will be served from. Only the last path segment matters to the
#: fake extraction, which looks documents up by file name; the real one (the
#: next iteration) needs a host Confluent Cloud can reach. Override with
#: --base-url, or let the control app rewrite it at publish time.
DEFAULT_BASE_URL = "http://localhost:8004/invoices"


def _f(x: Decimal) -> float:
    return float(x)


def submitted_at(inv: Invoice, rng: random.Random) -> dt.datetime | None:
    """When an employee submits this invoice for approval, in UTC.

    Business days and business hours, Munich time. December's invoices pile up
    over the holidays and come in during the first days of January — which is
    also why the simulation starts on a Monday with a backlog.
    """
    day = business_day(inv.date + dt.timedelta(days=inv.lag_days))
    if day < SIM_START:
        day = business_day(SIM_START + dt.timedelta(days=rng.randint(0, 3)))
    if day > SIM_END:
        return None
    local = dt.datetime.combine(day, dt.time(8, 0), BERLIN) + dt.timedelta(minutes=rng.randint(0, 10 * 60 + 30),
                                                                           seconds=rng.randint(0, 59))
    return local.astimezone(dt.timezone.utc)


def truth(inv: Invoice, line_pages: list[int]) -> dict:
    v = inv.vendor
    return {
        "vendor_name": v.name,
        "vendor_vat_id": v.vat_id,
        "vendor_address": ", ".join(v.address),
        "customer_name": BUYER["name"],
        "invoice_number": inv.number,
        "invoice_date": inv.date.isoformat(),
        "due_date": inv.due.isoformat(),
        "currency": v.currency,
        "subtotal": _f(inv.subtotal),
        "tax_amount": _f(inv.tax),
        "total_amount": _f(inv.total),
        "iban": v.iban,
        "line_items": [
            {"description": line.description, "quantity": _f(line.quantity), "unit_price": _f(line.unit_price),
             "amount": _f(line.amount), "page": page}
            for line, page in zip(inv.lines, line_pages)
        ],
    }


def scan(path: pathlib.Path, rng: random.Random) -> None:
    """Turn a born-digital PDF into what a phone or a copier makes of it.

    Rasterized at 150 dpi, grey, a degree or so off straight, a little noise
    and blur, JPEG-compressed. No text layer at all — so the real extraction
    has to read pixels, which is the case worth having in a demo.
    """
    import pypdfium2 as pdfium
    from PIL import Image, ImageChops, ImageFilter

    pdf = pdfium.PdfDocument(str(path))
    # Its own generator, seeded by the file: drawing the grain from the shared
    # one would shift every random choice after it, and the manifest with them.
    grain_rng = random.Random(path.name)
    pages = []
    for page in pdf:
        img = page.render(scale=150 / 72).to_pil().convert("L")
        img = img.rotate(rng.uniform(-1.4, 1.4), resample=Image.BICUBIC, expand=False, fillcolor=255)
        # Seeded speckle, not Image.effect_noise: that one draws from the C
        # library's generator, so every run made different bytes and the scans
        # were the only files a regeneration changed. About a third of the
        # pixels are dimmed a little — a copier's grain.
        grain = Image.frombytes("L", img.size, grain_rng.randbytes(img.size[0] * img.size[1]))
        noise = grain.point(lambda p: 255 if p > 85 else 230)
        img = ImageChops.multiply(img, noise).filter(ImageFilter.GaussianBlur(0.6))
        img = img.point(lambda p: min(255, int(p * 1.04 + 6)))  # a slightly washed-out copier
        pages.append(img)
    pdf.close()
    # Fixed dates: Pillow would stamp "now" into the PDF, the last thing that
    # made two runs differ. The ReportLab PDFs get the same with invariant=1.
    stamp = time.gmtime(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp())  # Pillow wants a struct_time
    pages[0].save(path, "PDF", resolution=150, save_all=True, append_images=pages[1:], quality=55,
                  creationDate=stamp, modDate=stamp, producer="Lumen demo invoice generator")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--manifest-only", action="store_true", help="skip the PDFs")
    args = parser.parse_args()

    rng = random.Random(args.seed + 1)
    invoices = plan(args.seed)
    rows = []
    for inv in invoices:
        when = submitted_at(inv, rng)
        if when is not None:
            rows.append((when, inv))
    rows.sort(key=lambda r: r[0])

    out_dir = DATA / "invoices"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    scanned_n = 0
    for i, (when, inv) in enumerate(rows, 1):
        doc_id = f"lumen-inv-{i:04d}"
        path = out_dir / f"{doc_id}.pdf"
        pages, line_pages = render(inv, str(path)) if not args.manifest_only else (1, [1] * len(inv.lines))
        scanned = rng.random() < inv.vendor.scanned
        record = {
            "doc_id": doc_id,
            "url": f"{args.base_url.rstrip('/')}/{doc_id}.pdf",
            "department": inv.department,
            "category": inv.category,
            "submitted_at": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "layout": inv.vendor.layout,
            "vendor_key": inv.vendor.key,
            "scanned": scanned,
            "pages": pages,
            "truth": truth(inv, line_pages),
        }
        if scanned:
            scanned_n += 1
            # Every third scan comes back from the fake extraction without its
            # total, the way a smudged last line does — so the review topic has
            # something in it. Documented in the README; nothing else is faked.
            if scanned_n % 3 == 0:
                record["fake_extraction"] = {"missing": ["total_amount"]}
            if not args.manifest_only:
                scan(path, rng)
        manifest.append(record)

    with open(DATA / "manifest.jsonl", "w", encoding="utf-8") as fh:
        for record in manifest:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    # A summary worth reading: this is the shape the dashboard should end up with.
    by = defaultdict(Counter)
    for r in manifest:
        t = r["truth"]
        by["currency"][t["currency"]] += 1
        by["department"][r["department"]] += 1
        by["category"][r["category"]] += 1
        by["layout"][r["layout"]] += 1
    print(f"    {len(manifest)} invoices, submitted {manifest[0]['submitted_at'][:10]} .. "
          f"{manifest[-1]['submitted_at'][:10]}, {scanned_n} scanned, "
          f"{sum(r['pages'] > 1 for r in manifest)} multi-page")
    for key, counter in by.items():
        print(f"    {key:<11} " + ", ".join(f"{k} {n}" for k, n in counter.most_common()))
    if not args.manifest_only:
        print(f"\n    PDFs     {out_dir.relative_to(_HERE.parent.parent.parent)}/")
    print(f"    manifest {(DATA / 'manifest.jsonl').relative_to(_HERE.parent.parent.parent)}")


if __name__ == "__main__":
    main()
