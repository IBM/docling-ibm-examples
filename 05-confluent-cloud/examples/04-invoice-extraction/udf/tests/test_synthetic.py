"""The synthetic extraction speaks Docling's extraction contract — shape first, then content."""

from __future__ import annotations

import copy
import json

from invoice_udf.invoice import _date
from invoice_udf.schema import errors
from invoice_udf.synthetic import extract

ITEM_KEYS = {"scope", "extracted_data", "raw_text", "errors", "validation_status", "inference_metadata"}


def by_pages(manifest, n):
    return next(r for r in manifest if r["pages"] == n and not r.get("fake_extraction"))


def test_envelope_is_an_extraction_document_result(target, documents, manifest):
    rec = by_pages(manifest, 1)
    out = extract(rec["url"], target, documents)
    assert set(out) == {"source_index", "source_uri", "filename", "status", "errors", "items"}
    assert out["status"] == "success" and out["errors"] == []
    assert out["filename"] == f"{rec['doc_id']}.pdf"
    (item,) = out["items"]
    assert set(item) == ITEM_KEYS
    assert item["scope"] == {"kind": "page", "page_no": 1}
    assert item["validation_status"] == "passed"
    assert item["inference_metadata"]["stop_reason"] == "end_of_sequence"


def test_single_page_answers_match_the_truth(target, documents, manifest):
    rec = by_pages(manifest, 1)
    data = extract(rec["url"], target, documents)["items"][0]["extracted_data"]
    truth = rec["truth"]
    for key in ("vendor_name", "invoice_number", "currency", "total_amount"):
        assert data[key] == truth[key]
    assert data["invoice_date"] != truth["invoice_date"]  # written as the real model writes it...
    assert _date(data["invoice_date"], truth["currency"] == "USD") == truth["invoice_date"]  # ...and parsed back
    assert [li["amount"] for li in data["line_items"]] == [li["amount"] for li in truth["line_items"]]
    assert set(data["line_items"][0]) == {"description", "quantity", "unit_price", "amount"}  # no "page"


def test_every_document_validates_on_its_first_page(target, documents, manifest):
    for rec in manifest:
        if rec.get("fake_extraction") or rec["pages"] > 1:
            continue
        out = extract(rec["url"], target, documents)
        assert out["items"][0]["validation_status"] == "passed", (rec["doc_id"], out["items"][0]["errors"])


def test_multi_page_splits_header_and_totals(target, documents, manifest):
    rec = by_pages(manifest, 2)
    out = extract(rec["url"], target, documents)
    # Each page breaks the schema — page 1 has no total, page 2 no date — so,
    # as in Docling, neither keeps extracted_data, the answers are in raw_text,
    # and a document with no page left is a failure.
    assert [i["validation_status"] for i in out["items"]] == ["failed", "failed"]
    assert [i["extracted_data"] for i in out["items"]] == [None, None]
    assert out["status"] == "failure"
    first, last = (json.loads(i["raw_text"]) for i in out["items"])
    assert _date(first["invoice_date"], rec["truth"]["currency"] == "USD") == rec["truth"]["invoice_date"]
    assert first["total_amount"] is None
    assert last["invoice_date"] is None and last["total_amount"] == rec["truth"]["total_amount"]
    assert last["vendor_name"] == rec["truth"]["vendor_name"]  # repeated in the continuation header
    (error,) = out["items"][0]["errors"]
    assert error == {"component_type": "model", "module_name": "ExtractionVlmPipeline", "category": "inference_failure",
                     "page_no": 1, "error_message": "Schema validation at $.total_amount: None is not of type 'number'"}
    n = len(first["line_items"]) + len(last["line_items"])
    assert n == len(rec["truth"]["line_items"])


def test_a_signed_bucket_url_finds_the_invoice(target, documents, manifest):
    rec = by_pages(manifest, 1)
    url = (f"https://us.aws.cdn.hf.co/xet-bridge-us/6ac39b/b57b897f?response-content-disposition="
           f"inline%3B+filename*%3DUTF-8%27%27{rec['doc_id']}.pdf%3B&Expires=1791207725&Signature=x")
    out = extract(url, target, documents)
    assert out["status"] == "success"
    assert out["filename"] == "b57b897f"  # what Docling reports: the path, not the disposition


def test_the_target_decides_the_fields(target, documents, manifest):
    rec = by_pages(manifest, 1)
    narrowed = copy.deepcopy(target)
    del narrowed["output_schema"]["properties"]["line_items"]
    narrowed["output_schema"]["properties"]["po_number"] = {"anyOf": [{"type": "string"}, {"type": "null"}]}
    data = extract(rec["url"], narrowed, documents)["items"][0]["extracted_data"]
    assert "line_items" not in data
    assert data["po_number"] is None


def test_template_only_target_skips_validation(target, documents, manifest):
    rec = by_pages(manifest, 1)
    out = extract(rec["url"], {"template": target["template"]}, documents)
    assert out["items"][0]["validation_status"] == "not_requested"
    assert out["items"][0]["extracted_data"]["total_amount"] == rec["truth"]["total_amount"]


def test_failures_are_values(target, documents):
    unknown = extract("https://example.com/nope.pdf", target, documents)
    assert unknown["status"] == "failure" and "404" in unknown["errors"][0]["error_message"]
    assert extract("https://x/y.pdf", {"instructions": "only"}, documents)["status"] == "failure"
    assert extract("https://x/y.pdf", "{not json", documents)["status"] == "failure"


def test_scans_marked_in_the_manifest_lose_their_total(target, documents, manifest):
    rec = next(r for r in manifest if r.get("fake_extraction"))
    item = extract(rec["url"], target, documents)["items"][-1]
    assert item["extracted_data"] is None and json.loads(item["raw_text"])["total_amount"] is None
    assert item["validation_status"] == "failed"


def test_schema_subset():
    schema = {"type": "object", "required": ["a"], "properties": {"a": {"type": "string", "format": "date"},
                                                                   "b": {"$ref": "#/$defs/B"},
                                                                   "c": {"type": "string", "pattern": "^[A-Z]{3}$"}},
              "$defs": {"B": {"anyOf": [{"type": "number"}, {"type": "null"}]}}}
    assert errors({"a": "2026-01-05", "b": None}, schema) == []
    assert errors({"a": "05.01.2026"}, schema) == []  # format is not checked, as in Docling
    assert errors({"b": "x", "c": "eur"}, schema) == ["$: 'a' is a required property",
                                                      "$.b: 'x' is not valid under any of the given schemas",
                                                      "$.c: 'eur' does not match '^[A-Z]{3}$'"]
