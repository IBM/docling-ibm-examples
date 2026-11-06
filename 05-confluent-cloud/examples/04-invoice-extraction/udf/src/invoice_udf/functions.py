# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""The entry points ``CREATE FUNCTION`` registers.

Two, and the split between them is the design:

    synthetic_extract(docling_conn, artifact_conn, url, target)
                                     -> ExtractionDocumentResult JSON
    invoice(extraction, submission)  -> one invoice record JSON

``synthetic_extract`` is the extraction for invoices released in *synthetic*
mode; *real* ones go to ``DoclingExtract`` (a Java UDF bound to a CONNECTION;
Python UDFs cannot reach the network, see ``docs/05-confluent-cloud/LIMITS.md``). It takes what
that takes — the two connection names, which it ignores, a source and an
``ExtractionTarget`` — and returns what Docling returns, so the two extraction
statements differ in the function they call and nothing else.
``invoice`` serves both: merging pages and deciding ok-or-review is the
pipeline's logic, not Docling's.

JSON strings in and out, as everywhere in this repo — no Schema Registry
subjects to keep in step, and ``JSON_VALUE`` in the SQL does the typing.
"""

from __future__ import annotations

import json

from pyflink.table import DataTypes
from pyflink.table.udf import udf

from .invoice import build as _build
from .synthetic import _error
from .synthetic import extract as _extract


def _f_synthetic_extract(docling_conn: str, artifact_conn: str, url: str, target: str) -> str:
    try:
        return json.dumps(_extract(url, target), ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001 — one bad row must not fail the statement
        return json.dumps({"source_index": 0, "source_uri": url or "", "filename": "", "status": "failure",
                           "errors": [_error(f"synthetic extraction crashed: {exc!r}")], "items": []})


synthetic_extract = udf(
    _f_synthetic_extract,
    input_types=[DataTypes.STRING()] * 4,
    result_type=DataTypes.STRING(),
)


def _f_invoice(extraction: str, submission: str) -> str:
    try:
        return json.dumps(_build(extraction, submission), ensure_ascii=False)
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"status": "review", "review_reasons": [f"invoice step crashed: {exc!r}"]})


invoice = udf(
    _f_invoice,
    input_types=[DataTypes.STRING(), DataTypes.STRING()],
    result_type=DataTypes.STRING(),
)
