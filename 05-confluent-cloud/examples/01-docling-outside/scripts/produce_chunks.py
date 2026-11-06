#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Put chunk records on the chunk topic, so the pipeline has something to do.

    uv run scripts/produce_chunks.py --sample
    uv run scripts/produce_chunks.py --file records.jsonl

`--sample` is a crafted document: a normal paragraph, one with a soft
hyphen splitting a word across a line break, an exact duplicate of the first,
one carrying an e-mail address and a Luhn-valid card number, and one that is
too short to keep. Between them they make every branch of the full pipeline
fire once — which is the point, because a pipeline you cannot see working is a
pipeline you cannot debug.

    normal              -> enriched
    hyphenated          -> enriched, with raw_text kept and normalized=true
    exact duplicate     -> dropped by dedup (full pipeline only)
    contains PII        -> enriched (redacted) *and* quarantine (the original)
    too short           -> enriched with keep=false; rejected when
                           DROP_LOW_QUALITY=TRUE

This is what Docling's `kafka_chunks` target writes, byte for byte — the record
shape is `docling_udf/chunk_record.py`, and the ids are the same function the
converter uses — so nothing downstream can tell the difference.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent.parent
# The record shape belongs to common/udf/, not to a copy kept here: `build` and
# `document_hash` are the converter's, and a sample that drifted from them
# would exercise a record the pipeline never actually sees.
sys.path.insert(0, str(_REPO / "common"))
sys.path.insert(0, str(_REPO / "common" / "udf" / "src"))

from confluent_kafka import Producer  # noqa: E402
from docling_udf.chunk_record import build, document_hash  # noqa: E402

from ccenv import kafka_config, load_env  # noqa: E402

DOC = "sample-document"
FILE_BYTES = b"a stand-in for the PDF's bytes; only its hash travels in the record"

PARAGRAPHS = [
    (
        "Retrieval quality is decided long before the vector store. The chunk "
        "boundary, the normalization and the metadata carried alongside the text "
        "determine what can be found at all."
    ),
    # A soft hyphen (U+00AD) and a line break: two bytes different from the
    # same sentence written plainly, and the same chunk after normalization.
    (
        "The inter­\nnational standard applies to every member state, and the "
        "transition period ends at the close of the following calendar year."
    ),
    # Byte-identical to the first: what dedup collapses.
    (
        "Retrieval quality is decided long before the vector store. The chunk "
        "boundary, the normalization and the metadata carried alongside the text "
        "determine what can be found at all."
    ),
    # One e-mail and one card number that passes Luhn — the two detectors that
    # are on by default.
    (
        "For renewals write to alice.smith@example.com, quoting the card ending "
        "4111 1111 1111 1111 held on file since the previous billing period."
    ),
    # Below MIN_CHARS once normalized.
    "Table 4",
]


def sample_records() -> list[dict]:
    digest = document_hash(FILE_BYTES)
    return [
        build(
            doc_id=DOC,
            chunk_index=index,
            text=text,
            document_hash=digest,
            origin={
                "mimetype": "application/pdf",
                "filename": "sample-document.pdf",
                "binary_hash": int(hashlib.sha256(FILE_BYTES).hexdigest()[:16], 16),
                "uri": None,
            },
            headings=["Retrieval"] if index < 3 else ["Appendix"],
            page_numbers=[1 + index // 3],
        )
        for index, text in enumerate(PARAGRAPHS)
    ]


def read_jsonl(path: pathlib.Path) -> list[dict]:
    records = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            sys.exit(f"{path}:{number}: {exc}")
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sample", action="store_true", help="the crafted five-chunk document")
    group.add_argument("--file", type=pathlib.Path, help="one JSON record per line")
    parser.add_argument("--topic", help="override CHUNKS_TOPIC")
    args = parser.parse_args()

    env = load_env(_HERE.parent)
    topic = args.topic or env.get("CHUNKS_TOPIC")
    if not topic:
        sys.exit("CHUNKS_TOPIC is not set in env.sh")

    records = sample_records() if args.sample else read_jsonl(args.file)

    producer = Producer(kafka_config(env))
    for record in records:
        # The key is the chunk_id, as it is everywhere else in this repo: a
        # redelivery of the same chunk then lands on the same partition.
        producer.produce(
            topic,
            key=(record.get("chunk_id") or "").encode(),
            value=json.dumps(record, ensure_ascii=False).encode(),
        )
    producer.flush(30)
    print(f"    {len(records)} records -> {topic}")


if __name__ == "__main__":
    main()
