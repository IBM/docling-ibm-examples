"""The UDF entry points Confluent Cloud registers with ``CREATE FUNCTION``.

Every one of them takes a JSON string and returns a JSON string, which is not
an accident: records accumulate fields as they move through the pipeline, and
this file is only the adapter between Flink SQL and the pure functions in
:mod:`docling_udf.enrichment` and :mod:`docling_udf.pii`.

Dedup has no UDF because it is SQL ``ROW_NUMBER()``, see
``flink/30_pipeline.sql``. Embedding has none because a Python UDF cannot make
a network call, see ``docs/05-confluent-cloud/LIMITS.md``.

Two consequences of being a UDF, and both are visible in the signatures below:

* **A UDF is a pure function of its arguments.** It has no ``open()`` to read
  configuration in, so ``min_chars``, the detector set and the rest arrive per
  call. The SQL passes constants, which is where they are configured.
* **A UDF returns one value, never zero and never a side output.** So dropping
  a record and sending it to a side output both become a value the SQL then
  routes on: :data:`DROPPED` for "emit nothing", and the ``pii_count`` /
  ``rejected_reason`` fields for the two side outputs. Routing is ``WHERE``
  clauses in a ``STATEMENT SET``.
"""

from __future__ import annotations

import json
import time
from typing import Any

from pyflink.table import DataTypes
from pyflink.table.types import DataType
from pyflink.table.udf import udf

from .chunk_record import is_chunk as _is_chunk
from .chunk_record import text as _text
from .docling_response import records as docling_records
from .enrichment import enrich, normalize_text
from .pii import DEFAULT_ENABLED, DETECTOR_NAMES, redact, summarize

# What "this record is gone" looks like coming out of a UDF. The empty string
# rather than NULL because the SQL filters on it, and a NULL comparison in a
# WHERE clause is UNKNOWN rather than false — which is the same as dropping the
# record, but only by accident, and it reads as a bug the first time you see it.
DROPPED = ""


def _loads(payload: str | None) -> dict[str, Any] | None:
    """Parse a record, or ``None`` if it is not a JSON object.

    A tombstone (NULL value) and a malformed record are both "not a chunk"
    here. The alternative — raising — fails the whole statement on one bad
    message, and Kafka topics acquire bad messages.
    """
    if not payload:
        return None
    try:
        record = json.loads(payload)
    except (TypeError, ValueError):
        return None
    return record if isinstance(record, dict) else None


def _dumps(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False)


def _name_set(csv: str | None, default: frozenset[str], known: tuple[str, ...]) -> frozenset[str]:
    """A comma-separated SQL argument as a validated name set.

    ``NULL`` and the empty string mean "the default", so the SQL can pass
    ``CAST(NULL AS STRING)`` to get the defaults. An unknown name is
    dropped rather than raised on: the argument is a literal in a long-running
    INSERT statement, and failing the statement at the first record is a worse
    answer than running with the detectors that do exist.
    """
    if not csv or not csv.strip():
        return default
    names = {n.strip() for n in csv.split(",") if n.strip()}
    return frozenset(names & set(known))


# --- 1. prepare ------------------------------------------------------------


def _f_prepare(payload: str, min_chars: int, drop_low_quality: bool) -> str:
    """Prepare one record for the pipeline.

    Drop control records and empty chunks, normalize the text *before* anything
    hashes it, then enrich. Returns :data:`DROPPED` for a dropped record.
    """
    record = _loads(payload)
    if record is None or not _is_chunk(record) or not _text(record).strip():
        return DROPPED

    original = _text(record)
    normalized = normalize_text(original)
    if normalized != original:
        record["raw_text"] = original
        record["text"] = normalized
    record["normalized"] = normalized != original

    # enrich() fingerprints what it is given, so it has to see the normalized
    # text — that is what makes dedup insensitive to formatting.
    enriched = enrich(record, min_chars=int(min_chars if min_chars is not None else 40))
    enriched["ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    if drop_low_quality and not enriched.get("keep", True):
        return DROPPED
    return _dumps(enriched)


_prepare_types: list[DataType] = [
    DataTypes.STRING(),
    DataTypes.INT(),
    DataTypes.BOOLEAN(),
]
dc_prepare = udf(
    _f_prepare,
    input_types=_prepare_types,
    result_type=DataTypes.STRING(),
)


# --- 2. guard --------------------------------------------------------------


def _f_guard(
    payload: str,
    detectors: str,
    redact_pii: bool,
    min_chars: int,
    drop_low_quality: bool,
    blocked_doc_ids: str,
) -> str:
    """Apply the PII and quality policy to one record.

    There are three outlets — main, quarantine, rejected. The policy is the
    argument list and the three outlets are fields the SQL routes on:

    * ``pii_count > 0``            — the record also belongs in quarantine, and
      what quarantine gets is the *input* payload (this function's argument),
      never the value it returns, because the returned one is redacted.
    * ``rejected_reason IS NOT NULL`` — the record belongs in rejected only.
    * otherwise                    — the main stream.

    See ``flink/30_pipeline.sql`` for the three ``WHERE`` clauses that
    say exactly that.
    """
    record = _loads(payload)
    if record is None:
        return DROPPED
    active = _name_set(detectors, DEFAULT_ENABLED, DETECTOR_NAMES)
    floor = int(min_chars if min_chars is not None else 40)

    blocked = {d.strip() for d in (blocked_doc_ids or "").split(",") if d.strip()}
    if (record.get("doc_id") or "") in blocked:
        record["rejected_reason"] = "blocked_doc_id"
        return _dumps(record)

    text = record.get("text") or ""
    redacted, findings = redact(text, active)
    if findings:
        record["pii_types"] = summarize(findings)
        record["pii_count"] = len(findings)
        # Always set, not only when it redacted. Downstream this field is read with JSON_VALUE, and a missing field
        # comes back NULL — which in a WHERE clause is neither true nor false.
        record["pii_redacted"] = bool(redact_pii)
        if redact_pii:
            record["text"] = redacted
    else:
        record["pii_count"] = 0
        record["pii_redacted"] = False

    # The quality gate, re-applied to the possibly-redacted text: redaction
    # replaces a long e-mail address with a short placeholder, so a chunk can
    # cross below the floor here that was above it in prepare.
    if len(record.get("text") or "") < floor:
        flags = list(record.get("quality_flags") or [])
        if "too_short" not in flags:
            flags.append("too_short")
        record["quality_flags"] = flags
        record["keep"] = False
    if drop_low_quality and not record.get("keep", True):
        record["rejected_reason"] = "low_quality"
    return _dumps(record)


_guard_types: list[DataType] = [
    DataTypes.STRING(),
    DataTypes.STRING(),
    DataTypes.BOOLEAN(),
    DataTypes.INT(),
    DataTypes.BOOLEAN(),
    DataTypes.STRING(),
]
dc_guard = udf(
    _f_guard,
    input_types=_guard_types,
    result_type=DataTypes.STRING(),
)


# --- 3. is_chunk -----------------------------------------------------------


def _f_is_chunk(payload: str) -> bool:
    """``chunk_record.is_chunk`` on its own.

    ``dc_prepare`` already applies it, so this exists for the cheap ``WHERE``
    that keeps non-chunks out of the more expensive call — and because the
    first thing anyone debugging a silent pipeline wants is
    ``SELECT dc_is_chunk(payload), payload FROM chunks LIMIT 20``.
    """
    record = _loads(payload)
    return bool(record is not None and _is_chunk(record))


dc_is_chunk = udf(
    _f_is_chunk,
    input_types=[DataTypes.STRING()],
    result_type=DataTypes.BOOLEAN(),
)


# --- 4. chunks -------------------------------------------------------------


def _f_chunks(envelope: str, url: str, doc_id: str) -> list[str]:
    """``dc_convert``'s JSON envelope, as an ARRAY of chunk records.

    Only examples 02 and 03 register this one. It is the piece that makes the
    two halves of those examples fit together: everything downstream of it is
    example 01's pipeline, unchanged, because what comes out of here is example
    01's record.

    An ARRAY rather than a table function because Confluent Cloud's Python UDFs
    are scalar-only — UDTFs are Java. The SQL then does the work a UDTF would
    have done, with ``CROSS JOIN UNNEST``, which is a plain SQL construct and
    arguably clearer about what is happening.

    Returns an empty array, never NULL, for a failed conversion: UNNEST of an
    empty array produces no rows, which is exactly "this document contributed
    nothing", while UNNEST of NULL is a different and less obvious thing.
    """
    try:
        return [_dumps(record) for record in docling_records(envelope, url or "", doc_id or "")]
    except Exception:  # noqa: BLE001 — one bad document must not fail the statement
        return []


dc_chunks = udf(
    _f_chunks,
    input_types=[DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()],
    result_type=DataTypes.ARRAY(DataTypes.STRING()),
)
