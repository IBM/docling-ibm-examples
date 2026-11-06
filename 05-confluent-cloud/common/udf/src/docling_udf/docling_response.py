# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""A Docling conversion, as chunk records.

This is the seam between examples. Example 01 gets its chunk topic from Docling
itself — the converter's ``kafka_chunks`` target writes one message per chunk,
and the pipeline reads them. Examples 02 and 03 call Docling from inside a Flink
statement, and what comes back is a *document*, not chunks. Something has to
make the second look like the first, and this is it: the records this module
builds go through :func:`docling_udf.chunk_record.build`, so they are the same
records, with the same ``chunk_id`` function, and nothing downstream can tell
which example produced them.

**The chunking here is not Docling's.** Docling for IBM watsonx converts; it has
no chunk endpoint (``/v1/chunk/hybrid`` exists in docling-serve and is not among
the SaaS's supported routes), and Docling's own ``HybridChunker`` needs
``docling-core`` and a tokenizer — native code, which a Confluent Cloud UDF
runtime will not load. So chunking is done here, from the Markdown, with the
standard library only:

* split on headings, keeping the heading path, because a section boundary is the
  one boundary the document itself asserts;
* then pack paragraphs up to :data:`MAX_CHARS`, never splitting a paragraph
  unless it is longer than that on its own;
* then, for such a paragraph, split on sentence ends rather than mid-word.

That is a structural chunker, roughly what ``HierarchicalChunker`` does and
deliberately not what ``HybridChunker`` does — there is no token counting here,
because there is no tokenizer. If you need Docling's own chunks, run
docling-serve yourself and call ``/v1/chunk/hybrid``; the shape this module
produces is the same either way.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .chunk_record import build

#: Longest chunk this will emit, in characters. Around 250 tokens for English
#: prose — small enough to embed, large enough to carry a paragraph's context.
MAX_CHARS = 1200

#: A chunk shorter than this is merged into the next one rather than emitted.
#: A heading on its own is the usual case.
MIN_CHARS = 80

_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*$")
#: Markdown tables, code fences and lists are held together as one block, so a
#: table never gets cut in half. Everything else splits on a blank line.
_FENCE = re.compile(r"^\s*(```|~~~)")
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")


def _blocks(markdown: str):
    """Markdown as (heading-path, block-text) pairs, in reading order."""
    path: list[str] = []
    buffer: list[str] = []
    in_fence = False

    def flush():
        text = "\n".join(buffer).strip()
        buffer.clear()
        return text

    for line in markdown.splitlines():
        if _FENCE.match(line):
            in_fence = not in_fence
            buffer.append(line)
            continue
        if in_fence:
            buffer.append(line)
            continue

        heading = _HEADING.match(line)
        if heading:
            text = flush()
            if text:
                yield list(path), text
            level = len(heading.group(1))
            title = heading.group(2).strip()
            # A level-2 heading replaces everything from level 2 down.
            del path[level - 1:]
            while len(path) < level - 1:
                path.append("")
            path.append(title)
            continue

        if not line.strip():
            text = flush()
            if text:
                yield list(path), text
            continue

        buffer.append(line)

    text = flush()
    if text:
        yield list(path), text


def _split_long(text: str) -> list[str]:
    """One over-long block, cut at sentence ends."""
    if len(text) <= MAX_CHARS:
        return [text]
    pieces: list[str] = []
    current = ""
    for sentence in _SENTENCE_END.split(text):
        if current and len(current) + 1 + len(sentence) > MAX_CHARS:
            pieces.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}".strip()
    if current:
        pieces.append(current)
    # A single sentence longer than MAX_CHARS still has to go somewhere; a hard
    # cut is better than an unbounded chunk, and the 4 MB row limit is real.
    out: list[str] = []
    for piece in pieces:
        while len(piece) > MAX_CHARS:
            out.append(piece[:MAX_CHARS])
            piece = piece[MAX_CHARS:]
        if piece:
            out.append(piece)
    return out


def chunk_markdown(markdown: str) -> list[tuple[list[str], str]]:
    """Markdown as (heading-path, chunk-text), packed to :data:`MAX_CHARS`."""
    chunks: list[tuple[list[str], str]] = []
    path: list[str] = []
    current = ""

    def emit():
        nonlocal current
        if current.strip():
            chunks.append(([h for h in path if h], current.strip()))
        current = ""

    for block_path, block in _blocks(markdown or ""):
        if block_path != path:
            # A new section starts a new chunk: the heading path is part of the
            # chunk's meaning, so a chunk cannot straddle two of them.
            emit()
            path = block_path
        for piece in _split_long(block):
            if current and len(current) + 2 + len(piece) > MAX_CHARS:
                emit()
            current = f"{current}\n\n{piece}" if current else piece
    emit()

    # Merge anything too short into its neighbour, so a lone heading line or a
    # stray caption does not become a chunk of its own.
    merged: list[tuple[list[str], str]] = []
    for headings, text in chunks:
        if merged and len(text) < MIN_CHARS and merged[-1][0] == headings \
                and len(merged[-1][1]) + len(text) + 2 <= MAX_CHARS:
            merged[-1] = (headings, f"{merged[-1][1]}\n\n{text}")
        else:
            merged.append((headings, text))
    return merged


def _doc_id(url: str, filename: str) -> str:
    """A stable, readable document key.

    The filename when there is one — that is what example 01's records carry —
    and the URL's last path segment otherwise. Never the whole URL: a presigned
    or query-laden URL would make the key change every run, and the key is what
    every keyed stage partitions on.

    Only a *known* extension is stripped. Naively taking everything before the
    last dot turns "2206.01062.pdf" into "2206", because arXiv ids have a dot
    in them — the kind of bug that shows up as a topic keyed on the wrong thing
    and nothing else.
    """
    name = (filename or "").strip()
    if not name:
        name = (url or "").split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    stem, dot, suffix = name.rpartition(".")
    if dot and suffix.lower() in _MIMETYPES:
        name = stem
    return name or (url or "unknown")


def records(envelope: str | None, url_override: str = "",
            doc_id_override: str = "") -> list[dict[str, Any]]:
    """The JSON envelope ``dc_convert`` returns, as chunk records.

    ``doc_id_override`` is the ``doc_id`` from the URL row, and it wins when it
    is there: the person who listed the document knows its key better than any
    rule applied to a filename does. Without one the key is derived — see
    :func:`_doc_id`.

    A failed conversion produces no records rather than an exception: the URL
    table exists precisely because some of the URLs will be wrong, and one bad
    document must not stop the statement. What happened to it is visible in the
    envelope on the conversion topic.
    """
    if not envelope:
        return []
    try:
        response = json.loads(envelope)
    except (TypeError, ValueError):
        return []
    if not isinstance(response, dict) or response.get("status") != "success":
        return []

    content = response.get("content") or ""
    if not content.strip():
        return []

    url = url_override or str(response.get("url") or "")
    filename = str(response.get("filename") or "")
    document = (doc_id_override or "").strip() or _doc_id(url, filename)

    # The service does not tell us the source bytes' sha256, and `build` needs
    # one to reproduce the converter's chunk_id. Hashing the *converted* text
    # instead keeps the property that actually matters here: convert the same
    # document twice and the chunk ids match, so the dedup stage collapses the
    # re-run. It will not match the id the same file would get through example
    # 01, and that is the honest position — a different producer, a different
    # chunking, different chunks.
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()

    origin = {
        "mimetype": _mimetype(filename, url),
        "filename": filename or f"{document}",
        "binary_hash": int(digest[:16], 16),
        "uri": url or None,
    }

    out: list[dict[str, Any]] = []
    for index, (headings, text) in enumerate(chunk_markdown(content)):
        out.append(
            build(
                doc_id=document,
                chunk_index=index,
                text=text,
                document_hash=digest,
                origin=origin,
                headings=headings,
                # Markdown has no page numbers. An empty list rather than a
                # missing field, because `chunk_record.page_numbers` promises a
                # list and the enrichment reads it.
                page_numbers=[],
            )
        )
    return out


_MIMETYPES = {
    "pdf": "application/pdf",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "html": "text/html",
    "htm": "text/html",
    "md": "text/markdown",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "tiff": "image/tiff",
}


def _mimetype(filename: str, url: str) -> str:
    for candidate in (filename, (url or "").split("?", 1)[0]):
        suffix = candidate.rsplit(".", 1)[-1].lower() if "." in candidate else ""
        if suffix in _MIMETYPES:
            return _MIMETYPES[suffix]
    return "application/octet-stream"
