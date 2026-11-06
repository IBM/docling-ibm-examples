#!/usr/bin/env python3
"""Draw example 05's picture diagrams, in the style of docs/05-confluent-cloud/diagrams/draw.py.

    python3 docs/05-confluent-cloud/diagrams/draw_ex05.py

It imports the palette and the helpers from draw.py, and writes ex05.svg and
ex05-filing.svg with their .animated twins. When the example changes, change
these functions and run it again. (The detailed, hand-drawn pipeline.svg
beside the example's README is not drawn here, as in examples 02 and 03.)
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import draw  # noqa: E402
from draw import Svg  # noqa: E402

OUT = HERE


def statement(g: Svg, x, y, w, h, title, sub="one Flink statement") -> None:
    """A Flink statement: an orange card with a title, ready for chips."""
    g.add(f'  <rect class="card f-flink shadow" filter="url(#shadow)" x="{x}" y="{y}" width="{w}" height="{h}" rx="14"/>')
    g.icon("stream", x + 16, y + 14, 20, "flink")
    g.text(x + 44, y + 30, title, "h t-flink")
    g.text(x + 16, y + 52, sub, "s")


def ex05() -> Svg:
    g = Svg(1200, 556, "Example 05 — SEC filings, converted as they are published",
            "sec_producer.py polls SEC EDGAR's current-filings feed for new 10-K and 10-Q filings and writes one "
            "metadata record per filing to the filings topic in Confluent Cloud. A Flink statement calls dc_metrics "
            "for each filing: Docling for IBM watsonx fetches the inline XBRL report from SEC EDGAR itself and "
            "converts it, and the figures are read from the tables of the DoclingDocument into the metrics topic. "
            "A second Flink statement derives financial events from the metrics into the events topic. The "
            "converted document is never kept.")
    g.header("05 — SEC filings, converted as they are published",
             "New reports stream in from SEC EDGAR; Docling reads each one inside Flink, and the figures become events.")

    # SEC and the producer
    g.card(36, 110, 230, 72, "ext", "SEC EDGAR", "current filings feed", "archive",
           sub2="and each filing's folder")
    g.arrow("M151,182 V311", "Atom feed", (160, 250))
    g.card(36, 314, 230, 72, "ext", "sec_producer.py", "once · --watch · --replay", "refresh")
    g.text(40, 414, "--local writes data/queue/ on disk,", "s")
    g.text(40, 430, "to try it without Kafka", "s")

    # Docling, outside, above the statement that calls it
    g.card(470, 110, 280, 72, "docling", "Docling for IBM watsonx", "fetches the report itself", "spark")
    g.arrow("M266,146 H467")
    g.text(290, 134, "the iXBRL report, by its URL", "lbl")
    g.text(290, 168, "with our User-Agent", "lbl")

    g.region(292, 226, 876, 300, "Confluent Cloud", "flink", "cloud")
    g.arrow("M266,350 H313")
    g.pill(316, 325, 132, "filings", "one per filing")
    g.arrow("M448,350 H491")

    # the metrics statement
    statement(g, 494, 252, 232, 196, "metrics")
    g.chip(508, 318, 204, "docling", "dc_metrics · Java, async", "spark")
    g.chip(508, 358, 204, "flink", "figures from the tables", "chart")
    g.chip(508, 398, 204, "mute", "failures are rows", "check")
    g.arrow("M560,252 V185", "URL", (552, 212), anchor="end")
    g.arrow("M660,185 V249", "DoclingDocument", (668, 212))

    g.arrow("M726,350 H761")
    g.pill(764, 325, 132, "metrics", "one per figure")
    g.arrow("M896,350 H931")

    # the events statement
    statement(g, 934, 272, 216, 156, "events")
    g.chip(948, 338, 188, "flink", "financials · ratios", "chart")
    g.chip(948, 378, 188, "flink", "five detectors", "bolt")
    g.arrow("M1042,428 V449")
    g.pill(947, 452, 190, "events", "upsert · what changed", role="ok", icon="bolt")

    g.text(316, 398, "One billed conversion", "s")
    g.text(316, 414, "per filing; a redeploy", "s")
    g.text(316, 430, "never converts twice.", "s")
    g.text(494, 476, "Only the figures are kept: the DoclingDocument (14 MB for a 10-Q)", "s")
    g.text(494, 492, "is read and dropped inside dc_metrics, never a row.", "s")
    return g


FILES = [
    # name, bytes, what it is, recorded for Docling
    ("mmm-20251231.htm", 4113612, "the report", True),
    ("mmm-20251231_lab.xml", 1448730, "XBRL labels", False),
    ("mmm-20251231_pre.xml", 1093012, "XBRL presentation", False),
    ("mmm-20251231_def.xml", 703377, "XBRL definitions", False),
    ("ex1038-globalrsuagreement.htm", 341702, "exhibit", False),
    ("mmm-20251231_cal.xml", 165080, "XBRL calculations", False),
    ("mmm-20251231.xsd", 120432, "XBRL schema", False),
]


def size(n: int) -> str:
    return f"{n / 1e6:.1f} MB" if n >= 1e6 else f"{n / 1e3:.0f} KB"


def ex05_filing() -> Svg:
    g = Svg(1200, 520, "Example 05 — inside one filing: what Docling gets",
            "3M's 10-K for 2025, as an XBRL package of 17 files, 8.1 MB. sec_producer.py records the link to the "
            "report only, mmm-20251231.htm (4.1 MB, the report in inline XBRL), in the filings topic. The Flink "
            "statement hands that link to Docling for IBM watsonx, which fetches and converts it into a "
            "DoclingDocument; dc_metrics reads the figures from its tables into the metrics topic.")
    g.header("Inside one filing: what Docling gets",
             "A filing holds the report and much more. sec_producer.py records the link to the report; Docling fetches only that.")

    g.add('  <rect class="card f-ext shadow" filter="url(#shadow)" x="36" y="104" width="660" height="396" rx="14"/>')
    g.disc("archive", 70, 140, "ext")
    g.text(98, 136, "0000066740-26-000014-xbrl.zip", "mono h")
    g.text(98, 154, "3M Company · 10-K for 2025 · 17 files, 8.1 MB", "s")

    y0, step, bar_x, bar_max = 196, 36, 300, 230
    biggest = FILES[0][1]
    for i, (name, n, what, sent) in enumerate(FILES):
        y = y0 + i * step
        role = "docling" if sent else "mute"
        if sent:
            g.add(f'  <rect class="card f-docling" x="50" y="{y - 22}" width="632" height="34" rx="9"/>')
        g.text(62, y, name, f"mono {'t-docling' if sent else 's'}")
        w = max(4, bar_max * n / biggest)
        g.add(f'  <rect x="{bar_x}" y="{y - 11}" width="{w:.1f}" height="12" rx="3" class="d-{role}"'
              f'{"" if sent else " opacity=\".55\""}/>')
        g.text(bar_x + w + 8, y, size(n), "s")
        g.text(672, y, what, f"{'h t-docling' if sent else 's'}", "end")
    g.text(62, y0 + len(FILES) * step - 4, "+ 9 smaller exhibits and 1 image", "s")
    g.text(62, y0 + len(FILES) * step + 22, "sec_producer.py records the link to the report only:", "m")
    g.text(62, y0 + len(FILES) * step + 40, "not the XBRL linkbases and schema, images or exhibits.", "m")

    # what happens to the one that is recorded
    g.arrow(f"M682,{y0 - 5} C710,{y0 - 5} 712,129 737,129")
    g.pill(740, 104, 270, "filings", "ixbrl_url, the report's link")
    g.arrow("M875,154 V187")
    g.card(740, 190, 270, 72, "docling", "Docling for IBM watsonx", "fetches and converts it", "spark")
    g.arrow("M875,262 V293")
    g.card(740, 296, 420, 86, "ok", "DoclingDocument", "headings, paragraphs and tables, as JSON", "check",
           sub2="14 MB for a 10-Q: read inside dc_metrics, never a row")
    g.arrow("M950,382 V413")
    g.pill(740, 416, 270, "metrics", "net income, total assets, …")
    return g


DIAGRAMS = {"ex05.svg": ex05, "ex05-filing.svg": ex05_filing}

if __name__ == "__main__":
    for animated in (False, True):
        draw.ANIMATED = animated
        for name, fn in DIAGRAMS.items():
            if animated:
                name = name.replace(".svg", ".animated.svg")
            (OUT / name).write_text(fn().render(), encoding="utf-8")
            print(f"wrote docs/05-confluent-cloud/diagrams/{name}")
