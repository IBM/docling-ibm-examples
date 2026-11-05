"""An extraction result, as one invoice record.

Docling answers page by page; the business wants one row per invoice. This is
the step between, and it is the same whether the extraction was real or
synthetic — which is why it is a UDF of its own.

Merging is by field, not by page:

* header fields (vendor, number, dates, currency) — **first** page that has one;
* totals (subtotal, tax, total) — **last** page that has one, because that is
  where an invoice prints them and a running "carried forward" on page 1 is not
  the total;
* line items — every page's, in page order.

Then the checks that decide where the record goes. A record is ``ok`` — and
goes on to currency conversion and the dashboard — when it has a vendor, a
date, a currency and a total, and the totals add up. Otherwise it is
``review``, with the reasons, and goes to a topic a human reads. Per-page
schema validation is reported but does not decide this: page 2 of a two-page
invoice fails it by construction (no vendor VAT id, no date), and that is not
a problem with the invoice.

Nothing here raises. Like every UDF in this repo, a bad input becomes a value
the SQL routes on.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any

FIRST = ("vendor_name", "vendor_vat_id", "vendor_address", "customer_name", "invoice_number", "invoice_date",
         "due_date", "currency", "iban")
LAST = ("subtotal", "tax_amount", "total_amount")
LIST = ("line_items",)
REQUIRED = ("vendor_name", "invoice_date", "currency", "total_amount")
#: Rounding slack, in the invoice's currency, before two amounts "do not add up".
TOLERANCE = 0.02


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(" ", "").replace(" ", "").replace("'", "")
        for symbol in ("€", "$", "£", "CHF", "EUR", "USD", "GBP"):
            text = text.replace(symbol, "")
        # 1.234,56 -> 1234.56 ; 1,234.56 -> 1234.56
        if "," in text and (text.rfind(",") > text.rfind(".")):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
        try:
            return float(text)
        except ValueError:
            return None
    return None


_MONTHS = {name: i + 1 for i, names in enumerate((
    ("january", "jan", "januar", "janvier", "gennaio", "enero"), ("february", "feb", "februar", "février", "febbraio"),
    ("march", "mar", "märz", "maerz", "mars", "marzo"), ("april", "apr", "avril", "aprile", "abril"),
    ("may", "mai", "maggio", "mayo"), ("june", "jun", "juni", "juin", "giugno", "junio"),
    ("july", "jul", "juli", "juillet", "luglio", "julio"), ("august", "aug", "août", "agosto"),
    ("september", "sep", "sept", "septembre", "settembre", "septiembre"), ("october", "oct", "oktober", "okt",
                                                                           "octobre", "ottobre", "octubre"),
    ("november", "nov", "novembre", "noviembre"), ("december", "dec", "dezember", "dez", "décembre", "dicembre",
                                                   "diciembre"),
)) for name in names}


def _date(value: Any, month_first: bool = False) -> str | None:
    """A date as the model wrote it -> ISO, or None.

    The schema says ``format: date`` and the instructions say YYYY-MM-DD; the
    model writes the date as the invoice prints it anyway, and Docling's
    validator does not check ``format`` — so ``20.02.2026``, ``30 December
    2025`` and ``01/02/2026`` all arrive "passed". The rules, from what Docling
    for IBM watsonx actually returned on this data set:

    * ``2026-02-20`` / ``2026-02-20T…``             ISO
    * ``20.02.2026``                                 day first (the dot is European)
    * ``30 December 2025`` / ``December 30, 2025``   a month name settles it
    * ``01/02/2026``, ``01-02-2026``                 ambiguous: day first, or month
      first when ``month_first`` — the caller passes the invoice's currency is
      USD. The model does not normalise these; it copies or reorders them as
      the vendor's country would write them, and on 174 invoices the currency
      told the two apart 16 times in 18. A first number over 12 settles it.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    try:
        return dt.date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        pass
    try:
        match = re.fullmatch(r"(\d{1,2})\.\s?(\d{1,2})\.\s?(\d{4})", text)
        if match:
            d, m, y = (int(g) for g in match.groups())
            return dt.date(y, m, d).isoformat()
        match = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", text)
        if match:
            a, b, y = (int(g) for g in match.groups())
            m, d = (a, b) if (month_first and a <= 12) or b > 12 else (b, a)
            return dt.date(y, m, d).isoformat()
        words = re.findall(r"[^\W\d_]+|\d+", text.casefold())
        months = [_MONTHS[w] for w in words if w in _MONTHS]
        numbers = [int(w) for w in words if w.isdigit()]
        years = [n for n in numbers if n >= 1000]
        days = [n for n in numbers if 1 <= n <= 31]
        if len(months) == 1 and len(years) == 1 and len(days) == 1:
            return dt.date(years[0], months[0], days[0]).isoformat()
    except ValueError:
        return None
    return None


def _items(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    items = [i for i in envelope.get("items") or [] if isinstance(i, dict)]
    return sorted(items, key=lambda i: (i.get("scope") or {}).get("page_no") or 0)


def page_data(item: dict[str, Any]) -> dict[str, Any]:
    """What the model said about one page.

    ``extracted_data`` when the answer passed validation. When it did not,
    Docling drops it there and keeps it only as ``raw_text`` — so page 1 of an
    invoice whose total the model could not read, which fails the schema for
    that one field, would otherwise lose its vendor, date and lines too. The
    answer is still the model's, and the checks below decide what it is worth.
    """
    data = item.get("extracted_data")
    if isinstance(data, dict):
        return data
    raw = item.get("raw_text")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def merge_pages(items: list[dict[str, Any]]) -> dict[str, Any]:
    pages = [page_data(i) for i in items]
    merged: dict[str, Any] = {}
    names: list[str] = []
    for data in pages:
        names.extend(k for k in data if k not in names)
    for name in names:
        values = [data.get(name) for data in pages if data.get(name) not in (None, "", [])]
        if name in LIST or any(isinstance(v, list) for v in values):
            merged[name] = [row for v in values if isinstance(v, list) for row in v if isinstance(row, dict)]
        elif not values:
            merged[name] = None
        elif name in LAST:
            merged[name] = values[-1]
        else:
            merged[name] = values[0]
    return merged


def build(envelope: Any, submission: Any) -> dict[str, Any]:
    """``ExtractionDocumentResult`` + the submission record -> an invoice record."""
    if isinstance(envelope, str):
        try:
            envelope = json.loads(envelope)
        except ValueError:
            envelope = None
    if isinstance(submission, str):
        try:
            submission = json.loads(submission)
        except ValueError:
            submission = None
    envelope = envelope if isinstance(envelope, dict) else {}
    submission = submission if isinstance(submission, dict) else {}

    record: dict[str, Any] = {
        "doc_id": submission.get("doc_id"),
        "url": submission.get("url") or envelope.get("source_uri"),
        "department": submission.get("department"),
        "category": submission.get("category"),
        "submitted_at": submission.get("submitted_at"),
    }
    items = _items(envelope)
    reasons: list[str] = []
    warnings: list[str] = []

    status = envelope.get("status")
    # Failed when no page said anything usable — not by the status alone, which
    # is "failure" also when every page broke the schema and the answers are
    # all in raw_text.
    if not any(page_data(i) for i in items):
        errors = [e.get("error_message", "") for e in envelope.get("errors") or [] if isinstance(e, dict)]
        errors += [e.get("error_message", "") for i in items for e in i.get("errors") or [] if isinstance(e, dict)]
        reasons.append("extraction_failed: " + ("; ".join(dict.fromkeys(errors)) or str(status or "no result")))
    unread = [str((i.get("scope") or {}).get("page_no")) for i in items if not page_data(i)]
    if unread and len(unread) < len(items):
        reasons.append("pages_not_extracted: " + ", ".join(unread))
    if any(i.get("extracted_data") is None and page_data(i) for i in items):
        warnings.append("schema_violation")

    merged = merge_pages(items)
    for name in ("subtotal", "tax_amount", "total_amount"):
        if name in merged:
            merged[name] = _number(merged[name])
    month_first = str(merged.get("currency") or "").strip().upper() == "USD"
    for name in ("invoice_date", "due_date"):
        if name in merged and merged[name] is not None:
            parsed = _date(merged[name], month_first)
            if parsed is None:
                warnings.append(f"unparseable_{name}")
            merged[name] = parsed
    if isinstance(merged.get("currency"), str):
        merged["currency"] = merged["currency"].strip().upper()
    lines = merged.get("line_items") or []
    for row in lines:
        for key in ("quantity", "unit_price", "amount"):
            if key in row:
                row[key] = _number(row[key])
    record.update(merged)

    if not reasons:
        reasons.extend(f"missing_{name}" for name in REQUIRED if record.get(name) in (None, ""))

    subtotal, tax, total = record.get("subtotal"), record.get("tax_amount"), record.get("total_amount")
    if subtotal is not None and total is not None and abs(subtotal + (tax or 0.0) - total) > TOLERANCE:
        reasons.append("totals_do_not_add_up")
    if lines:
        line_sum = sum(row.get("amount") or 0.0 for row in lines)
        base = subtotal if subtotal is not None else total
        if base is not None and abs(line_sum - base) > TOLERANCE:
            warnings.append("line_items_do_not_add_up")
        record["line_items_sum"] = round(line_sum, 2)
    record["line_item_count"] = len(lines)

    record["pages"] = len(items)
    record["validation"] = [i.get("validation_status") for i in items]
    record["extraction_status"] = status
    record["status"] = "review" if reasons else "ok"
    record["review_reasons"] = reasons
    record["warnings"] = warnings
    return record
