# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""What can be checked without a cluster: the three UDF bodies.

The `udf(...)`-wrapped names are Flink objects, so the tests call the bare
`_f_*` functions underneath them — the same split the Confluent examples use,
and the reason each function is written as a plain one first.
"""

from __future__ import annotations

import json

from docling_udf.functions import DROPPED, _f_guard, _f_is_chunk, _f_prepare


def chunk(**over):
    record = {
        "text": "I. INTRODUCTION\nOver the past decade the field has grown considerably.",
        "metadata": {"origin": {"mimetype": "application/pdf", "filename": "paper.pdf",
                                "binary_hash": 13823681836276983029}, "has_image": False},
        "doc_id": "paper",
        "chunk_index": 1,
        "chunk_id": "49eb" + "0" * 60,
        "headings": ["I. INTRODUCTION"],
        "page_numbers": [1],
    }
    record.update(over)
    return json.dumps(record)


# --- dc_is_chunk -----------------------------------------------------------

def test_is_chunk_accepts_a_chunk():
    assert _f_is_chunk(chunk()) is True


def test_is_chunk_rejects_control_and_junk():
    assert _f_is_chunk(json.dumps({"kind": "control", "doc_id": "x"})) is False
    assert _f_is_chunk(json.dumps({"text": "no doc_id"})) is False
    assert _f_is_chunk("not json at all") is False
    assert _f_is_chunk(None) is False          # a tombstone
    assert _f_is_chunk("[1, 2, 3]") is False   # JSON, but not an object


# --- dc_prepare ------------------------------------------------------------

def test_prepare_enriches_and_keeps_the_producers_fields():
    out = json.loads(_f_prepare(chunk(), 40, False))
    # Nothing the producer wrote is renamed or moved.
    assert out["doc_id"] == "paper"
    assert out["chunk_id"] == "49eb" + "0" * 60
    assert out["metadata"]["origin"]["filename"] == "paper.pdf"
    # ...and the derived fields are added alongside.
    assert len(out["fingerprint"]) == 32
    assert out["heading_path"] == "I. INTRODUCTION"
    assert out["page_start"] == 1
    assert out["content_type"] == "prose"
    assert out["keep"] is True
    assert out["ingested_at"].endswith("Z")


def test_prepare_drops_non_chunks_and_empties():
    assert _f_prepare(json.dumps({"kind": "control"}), 40, False) == DROPPED
    assert _f_prepare(chunk(text="   "), 40, False) == DROPPED
    assert _f_prepare("{oops", 40, False) == DROPPED


def test_prepare_normalizes_before_fingerprinting():
    # A soft hyphen splitting a word across a line break: the two records are
    # different bytes and must land on the same fingerprint.
    split = chunk(text="The inter­\nnational standard applies to every member state today.")
    joined = chunk(text="The international standard applies to every member state today.")
    a, b = json.loads(_f_prepare(split, 40, False)), json.loads(_f_prepare(joined, 40, False))
    assert a["fingerprint"] == b["fingerprint"]
    assert a["normalized"] is True and a["raw_text"] != a["text"]
    assert b["normalized"] is False and "raw_text" not in b


def test_prepare_drop_low_quality_is_the_flag_not_the_default():
    short = chunk(text="1234")
    assert json.loads(_f_prepare(short, 40, False))["keep"] is False
    assert _f_prepare(short, 40, True) == DROPPED


# --- dc_guard --------------------------------------------------------------

def prepared(**over):
    return _f_prepare(chunk(**over), 40, False)


def test_guard_redacts_and_reports():
    body = "Write to alice.smith@example.com about the renewal before the deadline."
    out = json.loads(_f_guard(prepared(text=body), None, True, 40, False, None))
    assert out["pii_count"] == 1
    assert out["pii_types"] == {"email": 1}
    assert out["pii_redacted"] is True
    assert "alice.smith@example.com" not in out["text"]
    assert "[REDACTED:EMAIL]" in out["text"]
    assert "rejected_reason" not in out


def test_guard_can_report_without_redacting():
    body = "Write to alice.smith@example.com about the renewal before the deadline."
    out = json.loads(_f_guard(prepared(text=body), "email", False, 40, False, None))
    assert out["pii_count"] == 1
    assert out["pii_redacted"] is False
    assert "alice.smith@example.com" in out["text"]   # quarantine still gets the original


def test_guard_detector_selection_is_the_sql_argument():
    body = "Card 4111 1111 1111 1111 on file, mail alice@example.com for the receipt."
    both = json.loads(_f_guard(prepared(text=body), "email,credit_card", True, 40, False, None))
    assert both["pii_types"] == {"email": 1, "credit_card": 1}
    only = json.loads(_f_guard(prepared(text=body), "email", True, 40, False, None))
    assert only["pii_types"] == {"email": 1}
    # An unknown name is ignored rather than failing a long-running statement.
    assert json.loads(_f_guard(prepared(text=body), "email,nonsense", True, 40, False, None))["pii_count"] == 1


def test_guard_defaults_when_the_detector_argument_is_null():
    body = "Card 4111 1111 1111 1111 on file, mail alice@example.com for the receipt."
    out = json.loads(_f_guard(prepared(text=body), None, True, 40, False, None))
    assert set(out["pii_types"]) == {"email", "credit_card"}   # pii.DEFAULT_ENABLED


def test_guard_blocks_doc_ids():
    out = json.loads(_f_guard(prepared(), None, True, 40, False, "other,paper"))
    assert out["rejected_reason"] == "blocked_doc_id"


def test_guard_rejects_low_quality_only_when_told_to():
    short = _f_prepare(chunk(text="12 34 56"), 40, False)
    assert "rejected_reason" not in json.loads(_f_guard(short, None, True, 40, False, None))
    assert json.loads(_f_guard(short, None, True, 40, True, None))["rejected_reason"] == "low_quality"
