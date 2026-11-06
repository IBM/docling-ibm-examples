# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Fixtures from the example's own files, so the tests check what is deployed.

The extraction target is read out of ``flink/20_view_submitted.sql`` rather
than copied here — a test against a copy passes while the statement drifts.
The documents are the manifest, compiled the way ``compile_truth.py`` does it.
"""

from __future__ import annotations

import json
import pathlib
import re

import pytest

EXAMPLE = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def target() -> dict:
    sql = (EXAMPLE / "flink" / "20_view_submitted.sql").read_text(encoding="utf-8")
    match = re.search(r"'(\{.*?\})' AS `extraction_target`", sql, re.DOTALL)
    assert match, "could not find the extraction target literal in 20_view_submitted.sql"
    return json.loads(match.group(1))


@pytest.fixture(scope="session")
def manifest() -> list[dict]:
    lines = (EXAMPLE / "data" / "manifest.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


@pytest.fixture(scope="session")
def documents(manifest) -> dict:
    out = {}
    for record in manifest:
        doc = dict(record["truth"])
        doc["pages"], doc["layout"] = record["pages"], record["layout"]
        doc["missing"] = (record.get("fake_extraction") or {}).get("missing", [])
        out[record["doc_id"]] = doc
    return out


def submission(record: dict) -> dict:
    return {k: record[k] for k in ("doc_id", "url", "department", "category", "submitted_at")}
