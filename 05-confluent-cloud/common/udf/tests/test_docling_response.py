"""The seam: a Docling conversion turned into example 01's chunk records.

What is worth testing here is not the chunk sizes — those are a judgement call
— but the two properties the rest of the pipeline depends on:

* what comes out is a record `chunk_record` recognises, with the ids the
  enrichment keys on, so example 02's chunks are example 01's chunks;
* a failed conversion produces nothing at all, rather than a record with empty
  text or an exception that stops the statement.
"""

from __future__ import annotations

import json

from docling_udf import chunk_record
from docling_udf.docling_response import MAX_CHARS, chunk_markdown, records

MARKDOWN = """# Docling

An introduction that is comfortably long enough to survive the minimum-length
merge, because a chunk shorter than eighty characters is folded into the next
one rather than emitted on its own.

## Layout

Docling reconstructs the reading order of a page before anything else happens,
which is what makes the chunk boundaries mean something.

| Column 1 | Column 2 |
|----------|----------|
| Data 1   | Data 2   |

## Tables

Table structure is recovered with TableFormer, and the result is emitted as a
Markdown table so that a text-only consumer still sees the cells.
"""


def envelope(**overrides) -> str:
    record = {
        "status": "success",
        "url": "https://arxiv.org/pdf/2408.09869",
        "filename": "2408.09869.pdf",
        "source_uri": "https://arxiv.org/pdf/2408.09869",
        "format": "markdown",
        "content": MARKDOWN,
        "processing_time": 2.2,
    }
    record.update(overrides)
    return json.dumps(record)


def test_headings_bound_the_chunks():
    chunks = chunk_markdown(MARKDOWN)
    assert chunks, "the sample document should produce chunks"
    # Every chunk belongs to exactly one section; a chunk never straddles two.
    for headings, text in chunks:
        assert text.strip()
        assert len(text) <= MAX_CHARS
    paths = [tuple(h) for h, _ in chunks]
    assert ("Docling",) in paths
    assert ("Docling", "Layout") in paths
    assert ("Docling", "Tables") in paths


def test_a_markdown_table_is_not_cut_in_half():
    chunks = chunk_markdown(MARKDOWN)
    table_rows = [text for _, text in chunks if "| Data 1" in text]
    assert len(table_rows) == 1
    assert "|----------|" in table_rows[0], "the header separator travels with the rows"


def test_long_paragraphs_are_split_at_sentence_ends():
    sentence = "This is a sentence of a perfectly ordinary length. "
    chunks = chunk_markdown("# T\n\n" + sentence * 80)
    assert len(chunks) > 1
    for _, text in chunks:
        assert len(text) <= MAX_CHARS
        assert text.endswith("."), "a chunk should not stop mid-sentence"


def test_records_are_chunk_records():
    out = records(envelope())
    assert out
    for index, record in enumerate(out):
        assert chunk_record.is_chunk(record)
        assert chunk_record.doc_id(record) == "2408.09869"
        assert chunk_record.chunk_index(record) == index
        assert chunk_record.text(record).strip()
        assert chunk_record.filename(record) == "2408.09869.pdf"
        assert chunk_record.origin(record)["mimetype"] == "application/pdf"
        assert chunk_record.page_numbers(record) == []
        # The id is the converter's function, so the dedup stage collapses a
        # re-conversion of the same document exactly as it would a redelivery.
        assert record["chunk_id"] == chunk_record.stable_chunk_id(
            chunk_record.document_hash(MARKDOWN.encode()), "2408.09869", index
        )


def test_converting_the_same_document_twice_gives_the_same_ids():
    first = [r["chunk_id"] for r in records(envelope())]
    second = [r["chunk_id"] for r in records(envelope(processing_time=9.9))]
    assert first == second


def test_doc_id_falls_back_to_the_url():
    out = records(envelope(filename="", url="https://example.com/docs/report.pdf"))
    assert out
    assert chunk_record.doc_id(out[0]) == "report"


def test_only_a_known_extension_is_stripped():
    # arXiv ids have a dot in them, and taking everything before the last dot
    # turns 2206.01062.pdf into 2206.
    out = records(envelope(filename="2206.01062.pdf"))
    assert chunk_record.doc_id(out[0]) == "2206.01062"
    out = records(envelope(filename="2206.01062"))
    assert chunk_record.doc_id(out[0]) == "2206.01062"


def test_an_explicit_doc_id_wins():
    out = records(envelope(filename="whatever.pdf"), doc_id_override="my-key")
    assert chunk_record.doc_id(out[0]) == "my-key"


def test_a_failed_conversion_produces_nothing():
    assert records(json.dumps({"status": "failure", "url": "x", "error": "404"})) == []
    assert records(envelope(content="")) == []
    assert records("not json") == []
    assert records(None) == []
    assert records("") == []
