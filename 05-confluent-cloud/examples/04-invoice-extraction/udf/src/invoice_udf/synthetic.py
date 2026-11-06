"""A stand-in for Docling's structured extraction, with the real wire contract.

The real thing is `docling-project/docling#4218`: you send an
``ExtractionTarget`` — a JSON Schema (``output_schema``), a tagged template
(``nuextract`` or ``example_json``), optional ``instructions`` — and a source;
you get back an ``ExtractionDocumentResult``:

    {"source_index": 0, "source_uri": "https://…/lumen-inv-0042.pdf",
     "filename": "lumen-inv-0042.pdf", "status": "success", "errors": [],
     "items": [{"scope": {"kind": "page", "page_no": 1},
                "extracted_data": {"vendor_name": "…", "total_amount": 545.32, …},
                "raw_text": "{…}", "errors": [],
                "validation_status": "passed",
                "inference_metadata": {"generation_time": -1.0, "num_tokens": 2189,
                                       "usage": {…}, "stop_reason": "end_of_sequence"}}]}

That is the shape Docling for IBM watsonx answered with on this data set
(``scripts/extract.py``, 2026-10-05), and this was adjusted to it.

This module produces exactly that, from the ground truth the generator wrote,
so the Flink side can be built and run end to end before the real call exists —
and so that swapping the real call in later changes the function registration
and nothing downstream of it.

What it reproduces, because the pipeline has to cope with it either way:

* **One item per page.** A paginated source is extracted page by page. Header
  fields are on page 1, continuation pages repeat only the vendor and the
  invoice number, and the totals are on the last page.
* **A page that breaks the schema keeps no data.** Docling validates each
  page's answer against ``output_schema``; one that fails — page 1 has no
  total, page 2 no date, and both are required — comes back with
  ``extracted_data: null``, the answer only in ``raw_text``, and one error per
  violation (``Schema validation at $.total_amount: None is not of type
  'number'``). A document with no page left in ``extracted_data`` is a
  ``failure``. Recovering the answers is the pipeline's job; see
  :mod:`invoice_udf.invoice`.
* **Dates as printed.** The schema says ``format: date`` and the instructions
  say YYYY-MM-DD; the real model writes ``20.02.2026`` or ``30 December 2025``
  regardless, and Docling does not check ``format``, so those pass. This
  writes each layout's date the way the real one did.
* **The target decides the shape.** Only the properties the schema (or the
  template) asks for come back; a property this knows nothing about comes
  back ``null``, as a model that finds nothing would answer. Drop
  ``line_items`` from the target and they are gone from every record
  downstream.
* **Failures are values.** An unknown document or a malformed target is a
  ``"status": "failure"`` envelope with an ``errors`` entry, never an exception.
* **A few scans lose a field.** The manifest marks them (``fake_extraction``);
  it is how the review topic gets something to show.
* **The file name is the URL's.** Docling reports the last path segment of the
  source URL — for a signed bucket URL, a content hash. This finds the
  invoice by the name the URL asks the CDN to send back
  (``response-content-disposition``), and reports what Docling would.

What it does not do: look at the PDF. It cannot — a Confluent Cloud Python UDF
has no network access at all (``docs/05-confluent-cloud/LIMITS.md``) — and it does not need to.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from . import schema as js

#: Fields printed in the invoice header (page 1), and the subset a
#: continuation page repeats.
HEADER = ("vendor_name", "vendor_vat_id", "vendor_address", "customer_name", "invoice_number", "invoice_date",
          "due_date", "currency")
CONTINUATION = ("vendor_name", "invoice_number", "currency")
#: Fields that only appear with the totals, on the last page.
TOTALS = ("subtotal", "tax_amount", "total_amount", "iban")


#: How the real model wrote each layout's invoice dates — the majority, over
#: the 174 invoices of a scripts/extract.py run on 2026-10-05 — although the
#: instructions ask for YYYY-MM-DD. A layout not listed gets ISO.
DATE_STYLE: dict[str, str] = {
    "de_classic": "{d:02d}.{m:02d}.{y}",   # 94 of 95
    "de_modern": "{d:02d}.{m:02d}.{y}",    # 31 of 31
    "ch_qr": "{d:02d}.{m:02d}.{y}",        # 7 of 7
    "us_saas": "{m:02d}/{d:02d}/{y}",      # 8 of 14; else "Dec 2, 2025"
    "eu_saas": "{d:02d}/{m:02d}/{y}",      # 7 of 10; else "02 February 2026"
    "uk_legal": "{d:02d} {month} {y}",     # 7 of 7
    "hotel_folio": "{d:02d} {month} {y}",  # 1 of 2
}


def _error(message: str, module: str = "synthetic") -> dict[str, str]:
    """A document-level ``ErrorItem``, as Docling serializes it."""
    return {"component_type": "user_input", "module_name": module, "error_message": message}


def _item_error(message: str, page: int) -> dict[str, Any]:
    """An item-level one: Docling files every one of these, a schema violation
    included, as ``inference_failure`` from the pipeline, scoped to its page."""
    return {"component_type": "model", "module_name": "ExtractionVlmPipeline", "error_message": message,
            "category": "inference_failure", "page_no": page}


def _printed(value: Any, style: str | None) -> Any:
    if not isinstance(value, str) or not style:
        return value
    try:
        day = dt.date.fromisoformat(value)
    except ValueError:
        return value
    months = ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
              "November", "December")
    return style.format(d=day.day, m=day.month, y=day.year, month=months[day.month - 1], mon=months[day.month - 1][:3])


def source_name(url: str) -> str:
    """The invoice's file name: from a signed CDN URL's content disposition, else the path."""
    parsed = urlparse(url or "")
    disposition = parse_qs(parsed.query).get("response-content-disposition", [""])[0]
    for part in disposition.split(";"):
        key, _, value = part.strip().partition("=")
        if key in ("filename*", "filename") and value:
            return unquote(value.split("''", 1)[-1].strip('"'))
    return unquote(parsed.path.rsplit("/", 1)[-1]) or (url or "")


def _documents() -> dict[str, dict[str, Any]]:
    # Imported late: _truth is generated by build.sh, and the tests pass their
    # own documents rather than depending on a build having happened.
    from ._truth import DOCUMENTS

    return DOCUMENTS


def _guidance(target: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, list[str] | None]]:
    """The output schema, and ``{field: item-fields-or-None}`` to extract.

    From the schema if there is one, else from the template's keys — a
    NuExtract template is shape-identical to the output, so its keys are the
    fields and a list-of-object value is a list of records.
    """
    output_schema = target.get("output_schema")
    if isinstance(output_schema, dict):
        props = js.fields(output_schema)
        return output_schema, {name: js.item_fields(prop, output_schema) for name, prop in props.items()}
    template = target.get("template")
    value = template.get("value") if isinstance(template, dict) else None
    if isinstance(value, dict):
        shape: dict[str, list[str] | None] = {}
        for name, sub in value.items():
            shape[name] = list(sub[0].keys()) if isinstance(sub, list) and sub and isinstance(sub[0], dict) else None
        return None, shape
    return None, {}


def _page_data(doc: dict[str, Any], page: int, shape: dict[str, list[str] | None]) -> dict[str, Any]:
    pages = doc.get("pages", 1)
    missing = set(doc.get("missing", ()))
    visible = set(HEADER if page == 1 else CONTINUATION)
    if page == pages:
        visible |= set(TOTALS)
    data: dict[str, Any] = {}
    for name, item_keys in shape.items():
        if item_keys is not None:
            data[name] = [{k: item.get(k) for k in item_keys}
                          for item in doc.get(name) or [] if item.get("page", 1) == page]
        elif name in visible and name not in missing:
            data[name] = _printed(doc.get(name), DATE_STYLE.get(doc.get("layout", ""))) if name.endswith("_date") \
                else doc.get(name)
        else:
            data[name] = None
    return data


def _metadata(doc_id: str, page: int, raw_text: str) -> dict[str, Any]:
    """Telemetry as Docling for IBM watsonx reports it: no generation time
    (``-1.0``), token counts, stable per document and page."""
    h = int(hashlib.sha256(f"{doc_id}:{page}".encode()).hexdigest()[:8], 16)
    completion = max(1, len(raw_text) // 4)
    prompt = 1650 + h % 900
    return {"generation_time": -1.0, "num_tokens": prompt + completion,
            "usage": {"prompt_tokens": prompt, "total_tokens": prompt + completion,
                      "completion_tokens": completion, "prompt_tokens_details": None},
            "stop_reason": "end_of_sequence"}


def extract(url: str, target: Any, documents: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """``url`` + ``ExtractionTarget`` -> ``ExtractionDocumentResult`` (a dict)."""
    filename = unquote(urlparse(url or "").path.rsplit("/", 1)[-1]) or (url or "")  # what Docling reports
    result: dict[str, Any] = {"source_index": 0, "source_uri": url or "", "filename": filename,
                              "status": "failure", "errors": [], "items": []}

    if isinstance(target, str):
        try:
            target = json.loads(target)
        except ValueError as exc:
            result["errors"].append(_error(f"extraction_target is not valid JSON: {exc}"))
            return result
    if not isinstance(target, dict) or (target.get("output_schema") is None and target.get("template") is None):
        result["errors"].append(_error("An extraction target requires output_schema or template"))
        return result

    output_schema, shape = _guidance(target)
    if not shape:
        result["errors"].append(_error("the extraction target names no fields"))
        return result

    doc_id = source_name(url).rsplit(".", 1)[0]
    doc = (documents if documents is not None else _documents()).get(doc_id)
    if doc is None:
        result["errors"].append(_error(f"could not fetch source {url}: 404 Not Found", "source"))
        return result

    for page in range(1, int(doc.get("pages", 1)) + 1):
        data = _page_data(doc, page, shape)
        raw_text = json.dumps(data, ensure_ascii=False)
        item: dict[str, Any] = {"scope": {"kind": "page", "page_no": page}, "extracted_data": data,
                                "raw_text": raw_text, "errors": [], "validation_status": "not_requested",
                                "inference_metadata": _metadata(doc_id, page, raw_text)}
        if output_schema is not None:
            problems = js.errors(data, output_schema)
            item["validation_status"] = "failed" if problems else "passed"
            item["errors"] = [_item_error(f"Schema validation at {p}", page) for p in problems]
            if problems:
                item["extracted_data"] = None  # Docling keeps a failed answer in raw_text only
        result["items"].append(item)

    # Docling's rule: no page with data is a failure, any error a partial one.
    if not any(i["extracted_data"] is not None for i in result["items"]):
        result["status"] = "failure"
    elif any(i["errors"] for i in result["items"]):
        result["status"] = "partial_success"
    else:
        result["status"] = "success"
    return result
