# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Pages in, one invoice record out, with a status the SQL can route on."""

from __future__ import annotations

import json

from invoice_udf.invoice import build
from invoice_udf.synthetic import extract

from .conftest import submission


def test_every_invoice_in_the_data_set(target, documents, manifest):
    """The whole manifest through both steps: ok unless the data says otherwise."""
    for rec in manifest:
        out = build(json.dumps(extract(rec["url"], target, documents)), json.dumps(submission(rec)))
        truth = rec["truth"]
        if rec.get("fake_extraction"):
            assert out["status"] == "review" and out["review_reasons"] == ["missing_total_amount"]
            continue
        assert out["status"] == "ok", (rec["doc_id"], out["review_reasons"])
        # A multi-page invoice's pages each break the schema (see the synthetic extraction), and
        # are read back from raw_text — which is worth a warning, not a review.
        assert out["warnings"] == (["schema_violation"] if rec["pages"] > 1 else []), (rec["doc_id"], out["warnings"])
        assert out["invoice_date"] == truth["invoice_date"]
        assert out["total_amount"] == truth["total_amount"]
        assert out["vendor_name"] == truth["vendor_name"]
        assert out["department"] == rec["department"]
        assert out["line_item_count"] == len(truth["line_items"])


def test_merge_takes_header_first_and_totals_last():
    envelope = {"status": "partial_success", "items": [
        {"scope": {"kind": "page", "page_no": 2},
         "extracted_data": {"vendor_name": "V", "invoice_date": None, "total_amount": 110.0,
                            "subtotal": 100.0, "tax_amount": 10.0, "line_items": [{"amount": 40.0}]}},
        {"scope": {"kind": "page", "page_no": 1},
         "extracted_data": {"vendor_name": "V", "invoice_date": "2026-01-05", "currency": "eur",
                            "total_amount": 60.0, "line_items": [{"amount": 60.0}]}},
    ]}
    out = build(envelope, {"doc_id": "d"})
    assert out["invoice_date"] == "2026-01-05"
    assert out["total_amount"] == 110.0  # the last page's, not the carried-forward 60
    assert out["currency"] == "EUR"
    assert out["line_item_count"] == 2 and out["line_items_sum"] == 100.0
    assert out["status"] == "ok" and out["pages"] == 2


def test_review_reasons():
    page = {"scope": {"kind": "page", "page_no": 1}, "extracted_data": {
        "vendor_name": "V", "invoice_date": "2026-01-05", "currency": "EUR",
        "subtotal": 100.0, "tax_amount": 19.0, "total_amount": 120.0}}
    assert build({"status": "success", "items": [page]}, {})["review_reasons"] == ["totals_do_not_add_up"]
    failed = build({"status": "failure", "errors": [{"error_message": "404"}], "items": []}, {})
    assert failed["status"] == "review" and failed["review_reasons"] == ["extraction_failed: 404"]
    assert build("not json", None)["status"] == "review"


def test_amounts_as_printed_are_still_numbers():
    page = {"scope": {"kind": "page", "page_no": 1}, "extracted_data": {
        "vendor_name": "V", "invoice_date": "2026-01-05", "currency": "EUR", "total_amount": "1.234,56 €"}}
    assert build({"status": "success", "items": [page]}, {})["total_amount"] == 1234.56


def test_dates_as_the_model_writes_them():
    from invoice_udf.invoice import _date

    assert _date("2026-02-20") == _date("2026-02-20T00:00:00") == "2026-02-20"
    assert _date("20.02.2026") == _date("20. 02. 2026") == "2026-02-20"
    assert _date("30 December 2025") == _date("December 30, 2025") == _date("Dec 30, 2025") == "2025-12-30"
    assert _date("27. April 2026") == _date("1. März 2026".replace("1.", "27.").replace("März", "April")) == "2026-04-27"
    assert _date("01/02/2026") == _date("01-02-2026") == "2026-02-01"  # day first...
    assert _date("01/02/2026", month_first=True) == "2026-01-02"  # ...month first on a USD invoice
    assert _date("23/01/2026", month_first=True) == "2026-01-23"  # unless the first number cannot be a month
    assert _date("01/23/2026") == "2026-01-23"
    assert _date("31.02.2026") is None and _date("soon") is None and _date(None) is None


def test_a_page_that_broke_the_schema_is_read_from_raw_text():
    page = {"scope": {"kind": "page", "page_no": 1}, "extracted_data": None, "validation_status": "failed",
            "raw_text": json.dumps({"vendor_name": "V", "invoice_date": "05.01.2026", "currency": "EUR",
                                    "total_amount": None}),
            "errors": [{"error_message": "Schema validation at $.total_amount: None is not of type 'number'"}]}
    out = build({"status": "failure", "items": [page]}, {})
    assert out["vendor_name"] == "V" and out["invoice_date"] == "2026-01-05"
    assert out["review_reasons"] == ["missing_total_amount"]
    assert out["warnings"] == ["schema_violation"]


def test_a_page_the_model_never_answered_sends_the_invoice_to_review():
    answered = {"scope": {"kind": "page", "page_no": 2}, "extracted_data": {
        "vendor_name": "V", "invoice_date": "2026-01-05", "currency": "EUR", "total_amount": 10.0}}
    timed_out = {"scope": {"kind": "page", "page_no": 1}, "extracted_data": None, "raw_text": None,
                 "validation_status": "not_run", "inference_metadata": None,
                 "errors": [{"error_message": "Read timed out. (read timeout=20.0)", "category": "inference_failure"}]}
    out = build({"status": "partial_success", "items": [timed_out, answered]}, {})
    assert out["status"] == "review" and out["review_reasons"] == ["pages_not_extracted: 1"]
    nothing = build({"status": "failure", "items": [timed_out]}, {})
    assert nothing["review_reasons"] == ["extraction_failed: Read timed out. (read timeout=20.0)"]
