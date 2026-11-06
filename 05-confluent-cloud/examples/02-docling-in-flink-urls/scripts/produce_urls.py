#!/usr/bin/env python3
"""Put documents-to-convert on the URL topic, so the convert statement has work.

    uv run scripts/produce_urls.py --sample
    uv run scripts/produce_urls.py --url https://arxiv.org/pdf/2408.09869
    uv run scripts/produce_urls.py --file urls.txt

One record per document::

    {"doc_id": "2408.09869", "url": "https://arxiv.org/pdf/2408.09869"}

``doc_id`` is optional — the chunker falls back to the URL's last path segment —
and it is here because a URL is a poor key: it changes when a document moves,
and the key is what every keyed stage downstream partitions on.

**Every row costs one Docling conversion**, billed per page — once: a
redeploy reads this topic from where it ended, so nothing already produced is
converted again. ``--sample`` is three small public documents. Follow them
with ``scripts/report.py``.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent.parent
sys.path.insert(0, str(_REPO / "common"))

from confluent_kafka import Producer  # noqa: E402

from ccenv import kafka_config, load_env  # noqa: E402

# Small, public, and stable. The first is a one-page PDF that converts in under
# a second, which is what you want the first time you run this; the other two
# are real papers with headings and tables, so the chunker has something to do
# and the enriched topic is worth reading.
SAMPLE = [
    ("dummy", "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"),
    ("2206.01062", "https://arxiv.org/pdf/2206.01062"),
    ("2408.09869", "https://arxiv.org/pdf/2408.09869"),
]


def read_file(path: pathlib.Path) -> list[tuple[str, str]]:
    """One URL per line, or `doc_id<space>url`. Blank lines and # are skipped."""
    out: list[tuple[str, str]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) == 2 and not parts[0].startswith(("http://", "https://")):
            out.append((parts[0], parts[1].strip()))
        elif line.startswith(("http://", "https://")):
            out.append(("", line))
        else:
            sys.exit(f"{path}:{number}: not a URL or 'doc_id url' pair: {line!r}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sample", action="store_true", help="three small public documents")
    group.add_argument("--url", action="append", help="one URL (repeatable)")
    group.add_argument("--file", type=pathlib.Path, help="one URL per line")
    parser.add_argument("--doc-id", help="the doc_id for a single --url")
    parser.add_argument("--topic", help="override URLS_TOPIC")
    args = parser.parse_args()

    env = load_env(_HERE.parent)
    topic = args.topic or env.get("URLS_TOPIC")
    if not topic:
        sys.exit("URLS_TOPIC is not set in env.sh")

    if args.sample:
        rows = SAMPLE
    elif args.url:
        rows = [(args.doc_id or "", url) for url in args.url]
    else:
        rows = read_file(args.file)

    producer = Producer(kafka_config(env))
    for doc_id, url in rows:
        record = {"url": url}
        if doc_id:
            record["doc_id"] = doc_id
        # Keyed by URL, so re-submitting the same document lands on the same
        # partition as the conversion it will produce.
        producer.produce(topic, key=url.encode(), value=json.dumps(record).encode())
        print(f"    + {doc_id or '(derived)'}  {url}")
    producer.flush(30)
    print(f"\n    {len(rows)} document(s) -> {topic}")
    print("    Each one is a Docling conversion. scripts/report.py follows them through.")


if __name__ == "__main__":
    main()
