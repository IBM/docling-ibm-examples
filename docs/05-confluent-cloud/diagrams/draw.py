#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Draw the picture diagrams in docs/05-confluent-cloud/diagrams/ — one script, one palette.

    python3 docs/05-confluent-cloud/diagrams/draw.py

Each diagram is written twice: <name>.svg, still, and <name>.animated.svg,
with the flows moving.

Standard library only. Each diagram is a function below that returns its
SVG; edit the function, run the script, commit the SVGs. They are the "what
it does" pictures; the detailed, hand-drawn pipeline.svg of examples 02, 03 and 05
and the generated mermaid of example 04 stay where they are.
"""

from __future__ import annotations

from html import escape
from pathlib import Path

OUT = Path(__file__).parent
ANIMATED = False  # set per pass in __main__; every diagram comes out twice

# The animated pass: dots run along each solid arrow, dashed ones march, and
# Docling's sparkle twinkles. All of it CSS, so it plays in a README's <img>
# and stops for anyone who asked for reduced motion.
MOTION = """
    .flow { fill: none; stroke: #2a78d6; stroke-width: 6; stroke-linecap: round;
            stroke-dasharray: 0 30; animation: flow .9s linear infinite; }
    .march { animation: march 1.1s linear infinite; }
    .tw { animation: tw 2.6s ease-in-out infinite; }
    @keyframes flow { to { stroke-dashoffset: -30; } }
    @keyframes march { to { stroke-dashoffset: -11; } }
    @keyframes tw { 50% { opacity: .4; } }
    @media (prefers-color-scheme: dark) { .flow { stroke: #7fb4f0; } }
    @media (prefers-reduced-motion: reduce) {
      .flow { display: none; }
      .march, .tw { animation: none; }
    }"""

# role: light (fill, stroke, ink), dark (fill, stroke, ink)
PALETTE = {
    "topic":   (("#e8f1fc", "#2a78d6", "#0b3d75"), ("#13243a", "#5a9be6", "#cfe3fb")),
    "flink":   (("#fff3e8", "#eb6834", "#a8401a"), ("#2a1e14", "#eb6834", "#f5a274")),
    "docling": (("#f1ecfe", "#7a5af8", "#4a2fb8"), ("#211a38", "#9b85fb", "#d9cffe")),
    "ok":      (("#e8f6ee", "#1f9d55", "#11653a"), ("#12261b", "#3fbf77", "#bfead2")),
    "warn":    (("#fff5e0", "#d69211", "#7a4e00"), ("#2b2312", "#e0a83a", "#f6dca4")),
    "bad":     (("#fdecea", "#d1453b", "#8f1f17"), ("#2e1716", "#e8736a", "#f6c4bf")),
    "aws":     (("#fdf6ec", "#b07a2a", "#6b4510"), ("#2a2116", "#d19a4a", "#f0d3a6")),
    "ext":     (("#ffffff", "#9a9892", "#2f2e2b"), ("#232325", "#8f8d88", "#ecebe8")),
    "mute":    (("#f3f2ef", "#c4c2bc", "#6b6964"), ("#29292b", "#55544f", "#a3a19c")),
}

BG = ("#fbfaf8", "#e6e4df", "#1f1e1c", "#6b6964")       # fill, border, text, muted
BG_DARK = ("#18181a", "#333336", "#ecebe8", "#a3a19c")
EDGE, EDGE_DARK = "#6b6964", "#a3a19c"

FONT = '-apple-system, "Segoe UI", Helvetica, Arial, sans-serif'
MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace'

# 24×24 line icons, stroked in currentColor
ICONS = {
    "doc": '<path d="M6 3h8l4 4v14H6z"/><path d="M14 3v4h4"/><path d="M9 12h6M9 15h6M9 18h4"/>',
    "docs": '<path d="M8 3h7l4 4v11H8z"/><path d="M15 3v4h4"/><path d="M5 7v14h11"/>',
    "laptop": '<rect x="4" y="5" width="16" height="11" rx="1.5"/><path d="M2 19.5h20"/>',
    "link": '<path d="M10 14a4 4 0 0 0 5.7 0l3-3a4 4 0 0 0-5.7-5.7l-1 1"/>'
            '<path d="M14 10a4 4 0 0 0-5.7 0l-3 3a4 4 0 0 0 5.7 5.7l1-1"/>',
    "bucket": '<ellipse cx="12" cy="6" rx="8" ry="3"/>'
              '<path d="M4 6l2 13c.3 1.5 2.9 2.5 6 2.5s5.7-1 6-2.5L20 6"/>',
    "queue": '<rect x="3" y="4" width="18" height="4.5" rx="1"/><rect x="3" y="10" width="18" height="4.5" rx="1"/>'
             '<rect x="3" y="16" width="18" height="4.5" rx="1"/>',
    "plug": '<path d="M9 2v5M15 2v5"/><path d="M6 7h12v4a6 6 0 0 1-12 0z"/><path d="M12 17v5"/>',
    "stream": '<path d="M3 7c3-3 6 3 9 0s6 3 9 0"/><path d="M3 12c3-3 6 3 9 0s6 3 9 0"/>'
              '<path d="M3 17c3-3 6 3 9 0s6 3 9 0"/>',
    "spark": '<path d="M11 3l1.9 5.6L18.5 10.5l-5.6 1.9L11 18l-1.9-5.6L3.5 10.5l5.6-1.9z"/>'
             '<path d="M19 15l.8 2.2L22 18l-2.2.8L19 21l-.8-2.2L16 18l2.2-.8z"/>',
    "check": '<circle cx="12" cy="12" r="9"/><path d="M8 12.5l2.7 2.7L16 9.5"/>',
    "lock": '<rect x="5" y="11" width="14" height="10" rx="2"/><path d="M8 11V8a4 4 0 0 1 8 0v3"/>',
    "xcircle": '<circle cx="12" cy="12" r="9"/><path d="M9 9l6 6M15 9l-6 6"/>',
    "clock": '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    "receipt": '<path d="M6 3h12v18l-2-1.5-2 1.5-2-1.5-2 1.5-2-1.5L6 21z"/><path d="M9 8h6M9 12h6M9 16h3"/>',
    "chart": '<path d="M4 20h16"/><path d="M7 17v-5M12 17V7M17 17v-8"/>',
    "bolt": '<path d="M13 2L4 14h7l-1 8 9-12h-7z"/>',
    "key": '<circle cx="8" cy="15" r="4"/><path d="M11 12l9-9M17 6l3 3M14.5 8.5l2 2"/>',
    "euro": '<path d="M17.5 6.5a7 7 0 1 0 0 11"/><path d="M4 10h9M4 14h9"/>',
    "cloud": '<path d="M7 18.5a5 5 0 0 1-.7-9.9A6.5 6.5 0 0 1 18.4 9.6 4.5 4.5 0 0 1 17.5 18.5z"/>',
    "upload": '<path d="M12 15V4M7 9l5-5 5 5"/><path d="M4 15v5h16v-5"/>',
    "shield": '<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M9 12l2 2 4-4"/>',
    "copy": '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V5a1 1 0 0 0-1-1H5a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h3"/>',
    "sliders": '<path d="M4 7h9M17 7h3M4 17h3M11 17h9"/><circle cx="15" cy="7" r="2"/><circle cx="9" cy="17" r="2"/>',
    "gauge": '<path d="M4 17a8 8 0 0 1 16 0"/><path d="M12 17l4-5"/>',
    "archive": '<rect x="3" y="4" width="18" height="5" rx="1"/><path d="M5 9v11h14V9"/><path d="M10 13h4"/>',
    "scissors": '<circle cx="6" cy="7" r="3"/><circle cx="6" cy="17" r="3"/><path d="M8.5 8.5L20 19M8.5 15.5L20 5"/>',
    "merge": '<path d="M6 4v4a6 6 0 0 0 6 6h0a6 6 0 0 1 6 6"/><path d="M18 4v4a6 6 0 0 1-6 6"/>',
    "dashboard": '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 9h18"/><path d="M7 17v-4M11 17v-6M15 17v-3"/>',
    "trash": '<path d="M4 7h16M10 11v6M14 11v6"/><path d="M6 7l1 13h10l1-13"/><path d="M9 7V4h6v3"/>',
    "refresh": '<path d="M20 11a8 8 0 0 0-14.6-4.5M4 13a8 8 0 0 0 14.6 4.5"/><path d="M5 3v4h4M19 21v-4h-4"/>',
}


def css() -> str:
    light, dark = [], []
    for role, (lt, dk) in PALETTE.items():
        for rules, (fill, stroke, ink) in ((light, lt), (dark, dk)):
            rules.append(
                f".f-{role}{{fill:{fill};stroke:{stroke}}} .s-{role}{{stroke:{stroke}}} "
                f".c-{role}{{color:{stroke}}} .t-{role}{{fill:{ink}}} .d-{role}{{fill:{stroke}}} "
                f".l-{role}{{fill:{fill}}}"
            )
    return f"""
    text {{ font-family: {FONT}; fill: {BG[2]}; font-size: 13px; }}
    .bg {{ fill: {BG[0]}; stroke: {BG[1]}; }}
    .title {{ font-size: 21px; font-weight: 700; letter-spacing: -0.2px; }}
    .lede {{ font-size: 14px; fill: {BG[3]}; }}
    .h {{ font-size: 15px; font-weight: 650; }}
    .m {{ font-size: 12.5px; fill: {BG[3]}; }}
    .s {{ font-size: 11.5px; fill: {BG[3]}; }}
    .mono {{ font-family: {MONO}; font-size: 12px; }}
    .num {{ font-size: 12px; font-weight: 700; fill: #fff; }}
    .lbl {{ font-size: 11.5px; fill: {BG[3]}; font-style: italic;
            paint-order: stroke; stroke: {BG[0]}; stroke-width: 5px; stroke-linejoin: round; }}
    .region-t {{ font-size: 12px; font-weight: 700; letter-spacing: 1.2px; }}
    .edge {{ fill: none; stroke: {EDGE}; stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round; }}
    .dash {{ stroke-dasharray: 6 5; }}
    .ah {{ fill: {EDGE}; }}
    .card {{ stroke-width: 1.4; }}
    .region {{ stroke-width: 1.4; stroke-dasharray: 7 5; }}
    .ico {{ fill: none; stroke: currentColor; stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round; }}
    .hl {{ fill-opacity: .75; }}
    {' '.join(light)}
    @media (prefers-color-scheme: dark) {{
      text {{ fill: {BG_DARK[2]}; }}
      .bg {{ fill: {BG_DARK[0]}; stroke: {BG_DARK[1]}; }}
      .lede, .m, .s {{ fill: {BG_DARK[3]}; }}
      .lbl {{ fill: {BG_DARK[3]}; stroke: {BG_DARK[0]}; }}
      .edge {{ stroke: {EDGE_DARK}; }}
      .ah {{ fill: {EDGE_DARK}; }}
      .shadow {{ filter: none; }}
      {' '.join(dark)}
    }}"""


class Svg:
    def __init__(self, w: int, h: int, title: str, desc: str):
        self.w, self.h, self.title, self.desc = w, h, title, desc
        self.parts: list[str] = []
        self.animated = ANIMATED
        self.flows: list[str] = []  # under everything, so cards hide the dots

    def add(self, s: str) -> None:
        self.parts.append(s)

    def render(self) -> str:
        symbols = "\n".join(
            f'    <symbol id="i-{k}" viewBox="0 0 24 24"><g class="ico">{v}</g></symbol>'
            for k, v in ICONS.items()
        )
        return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" height="{self.h}" role="img" aria-labelledby="t d">
  <title id="t">{escape(self.title)}</title>
  <desc id="d">{escape(self.desc)}</desc>
  <!-- drawn by docs/05-confluent-cloud/diagrams/draw.py — edit that, not this -->
  <defs>
    <style>{css()}{MOTION if self.animated else ""}
    </style>
    <filter id="shadow" x="-10%" y="-10%" width="120%" height="140%">
      <feDropShadow dx="0" dy="1.5" stdDeviation="2.2" flood-color="#000" flood-opacity=".10"/>
    </filter>
    <marker id="arrow" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="7.5" markerHeight="7.5" orient="auto-start-reverse">
      <path class="ah" d="M0,0.5 L10,5 L0,9.5 z"/>
    </marker>
{symbols}
  </defs>
  <rect class="bg" x="0.5" y="0.5" width="{self.w - 1}" height="{self.h - 1}" rx="18"/>
{chr(10).join(self.flows + self.parts)}
</svg>
"""

    # ---- primitives ------------------------------------------------------

    def text(self, x, y, s, cls="", anchor="start", **attrs) -> None:
        extra = "".join(f' {k.replace("_", "-")}="{v}"' for k, v in attrs.items())
        a = f' text-anchor="{anchor}"' if anchor != "start" else ""
        self.add(f'  <text x="{x}" y="{y}" class="{cls}"{a}{extra}>{escape(s)}</text>')

    def rich(self, x, y, runs, cls="", anchor="start") -> None:
        """One line from (text, tspan class) runs."""
        a = f' text-anchor="{anchor}"' if anchor != "start" else ""
        body = "".join(
            f'<tspan class="{c}">{escape(t)}</tspan>' if c else escape(t) for t, c in runs
        )
        self.add(f'  <text x="{x}" y="{y}" class="{cls}"{a} xml:space="preserve">{body}</text>')

    def icon(self, name, x, y, size, role) -> None:
        tw = " tw" if self.animated and name == "spark" else ""
        self.add(f'  <use href="#i-{name}" x="{x}" y="{y}" width="{size}" height="{size}" class="c-{role}{tw}"/>')

    def disc(self, name, cx, cy, role, r=18) -> None:
        self.add(f'  <circle cx="{cx}" cy="{cy}" r="{r}" class="l-{role}"/>')
        s = r * 1.15
        self.icon(name, cx - s / 2, cy - s / 2, s, role)

    def badge(self, cx, cy, s, role, r=13) -> None:
        self.add(f'  <circle cx="{cx}" cy="{cy}" r="{r}" class="d-{role}"/>')
        self.text(cx, cy + 4.3, s, "num", "middle")

    def header(self, title, lede) -> None:
        self.text(36, 48, title, "title")
        self.text(36, 74, lede, "lede")

    def region(self, x, y, w, h, label, role, icon=None) -> None:
        self.add(f'  <rect class="region f-{role}" x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill-opacity=".45"/>')
        tx = x + 18
        if icon:
            self.icon(icon, x + 16, y + 11, 18, role)
            tx = x + 40
        self.text(tx, y + 25, label.upper(), f"region-t t-{role}")

    def card(self, x, y, w, h, role, title, sub=None, icon=None, mono=False, sub2=None,
             dashed=False) -> None:
        d = ' stroke-dasharray="6 4"' if dashed else ""
        self.add(f'  <rect class="card f-{role} shadow" filter="url(#shadow)" x="{x}" y="{y}" '
                 f'width="{w}" height="{h}" rx="12"{d}/>')
        tx = x + 16
        if icon:
            self.disc(icon, x + 34, y + h / 2, role)
            tx = x + 62
        lines = 1 + (sub is not None) + (sub2 is not None)
        ty = y + h / 2 - (lines - 1) * 9.5 + 5
        self.text(tx, ty, title, f"h t-{role}")
        if sub is not None:
            self.text(tx, ty + 19, sub, "mono" if mono else "m")
        if sub2 is not None:
            self.text(tx, ty + 37, sub2, "m")

    def pill(self, x, y, w, title, sub=None, role="topic", icon="stream", h=50, dashed=False) -> None:
        d = ' stroke-dasharray="6 4"' if dashed else ""
        self.add(f'  <rect class="card f-{role} shadow" filter="url(#shadow)" x="{x}" y="{y}" '
                 f'width="{w}" height="{h}" rx="{h / 2}"{d}/>')
        self.icon(icon, x + 14, y + h / 2 - 10, 20, role)
        tx = x + 42
        if sub is None:
            self.text(tx, y + h / 2 + 5, title, f"h t-{role}")
        else:
            self.text(tx, y + h / 2 - 3, title, f"h t-{role}")
            self.text(tx, y + h / 2 + 14, sub, "s")

    def chip(self, x, y, w, role, label, icon=None, h=30) -> None:
        self.add(f'  <rect class="card f-{role}" x="{x}" y="{y}" width="{w}" height="{h}" rx="{h / 2}"/>')
        tx = x + 12
        if icon:
            self.icon(icon, x + 9, y + h / 2 - 8, 16, role)
            tx = x + 31
        self.text(tx, y + h / 2 + 4.5, label, f"m t-{role}")

    def arrow(self, d, label=None, at=None, dashed=False, anchor="start", head=True) -> None:
        cls = "edge dash" if dashed else "edge"
        if dashed and self.animated:
            cls += " march"
        m = ' marker-end="url(#arrow)"' if head else ""
        self.add(f'  <path class="{cls}" d="{d}"{m}/>')
        if self.animated and not dashed:
            self.flows.append(f'  <path class="flow" d="{d}"/>')
        if label:
            self.text(at[0], at[1], label, "lbl", anchor)

    def legend(self, x, y, items) -> None:
        for role, label, rx in items:
            self.add(f'  <rect class="card f-{role}" x="{x}" y="{y}" width="26" height="14" rx="{rx}"/>')
            self.text(x + 33, y + 11.5, label, "s")
            x += 33 + len(label) * 6.4 + 22


def outputs(g: Svg, x, y, w=210, gap=64) -> list[float]:
    """The three topics every chunk example ends in; returns their mid-y."""
    rows = [
        ("chunks.enriched", "redacted · deduplicated", "ok", "check"),
        ("pii.quarantine", "the unredacted originals", "warn", "lock"),
        ("quality.rejected", "dropped, but visible", "bad", "xcircle"),
    ]
    mids = []
    for i, (t, s, role, ic) in enumerate(rows):
        g.pill(x, y + i * gap, w, t, s, role=role, icon=ic)
        mids.append(y + i * gap + 25)
    return mids


def enrichment(g: Svg, x, y, w, h, title="Shared enrichment", sub="common/flink · one statement") -> None:
    """The common/flink half, as a Flink card with its four steps."""
    g.add(f'  <rect class="card f-flink shadow" filter="url(#shadow)" x="{x}" y="{y}" width="{w}" height="{h}" rx="14"/>')
    g.icon("stream", x + 16, y + 16, 20, "flink")
    g.text(x + 44, y + 32, title, "h t-flink")
    g.text(x + 16, y + 54, sub, "s")
    steps = [("sliders", "normalize"), ("shield", "PII guard"), ("copy", "dedup"), ("gauge", "quality gate")]
    sy = y + 70
    step = (h - 84) / len(steps)
    for ic, label in steps:
        g.chip(x + 14, sy, w - 28, "flink", label, ic)
        sy += step


def fan(g: Svg, x0, y0, x1, ys) -> None:
    """One source at (x0, y0) to several targets at x1."""
    for y in ys:
        if abs(y - y0) < 1:
            g.arrow(f"M{x0},{y0} H{x1 - 3}")
        else:
            mx = (x0 + x1) / 2
            g.arrow(f"M{x0},{y0} C{mx},{y0} {mx},{y} {x1 - 3},{y}")


# ---- the diagrams ----------------------------------------------------------


def overview() -> Svg:
    g = Svg(1200, 740, "Docling and Confluent Cloud — five ways into a stream",
            "Five examples. 01: Docling for IBM watsonx, outside Confluent Cloud, streams chunks in. 02: a topic of URLs, "
            "converted by Docling called from Flink. 03: uploads to a private S3 bucket, converted the same "
            "way. All three feed the same shared enrichment (normalize, PII guard, dedup, quality gate) into "
            "enriched, quarantine and rejected topics. 04: invoices on a simulated clock, fields extracted by "
            "Docling, converted to EUR and aggregated into a live dashboard with lightning queries. "
            "05: SEC filings as they are published, each report converted by Docling called from Flink, "
            "the financial figures extracted and turned into events.")
    g.header("Five ways to get documents into a stream",
             "Each example is one front door. Docling converts; Confluent Cloud for Apache Flink does the rest.")

    g.region(300, 100, 870, 614, "Confluent Cloud", "flink", "cloud")

    # sources
    src = [
        ("01", "spark", "Docling for IBM watsonx", "converts, streams the chunks", "docling"),
        ("02", "link", "A list of URLs", "public PDFs, one per row", "ext"),
        ("03", "bucket", "An S3 bucket", "upload a file, it's converted", "aws"),
        ("04", "receipt", "Supplier invoices", "six months, on a sim clock", "ext"),
    ]
    rows = [170, 270, 370, 520]
    for (n, ic, t, s, role), cy in zip(src, rows):
        g.card(36, cy - 34, 250, 68, role, t, s, ic)
        g.badge(36, cy - 34, n, "topic", r=12)

    # Docling column
    g.text(318, 160, "chunks, by Docling's kafka_chunks target", "lbl")
    for cy in rows[1:]:
        g.arrow(f"M286,{cy} H331")
        g.card(334, cy - 27, 168, 54, "docling", "Docling", "called from Flink" if cy != 520 else "extracts fields", "spark")

    # 01–03 converge on the chunk topic
    g.pill(523, 245, 136, "chunks", "the seam")
    g.arrow("M502,270 H520")
    g.arrow("M502,370 C512,370 512,282 522,282")
    g.arrow("M286,170 H470 C540,170 560,200 572,243")

    enrichment(g, 700, 130, 210, 280)
    g.arrow("M659,270 H697")
    mids = outputs(g, 940, 150, 210, 86)
    fan(g, 910, 270, 940, mids)

    # 04 — a lane of its own
    g.add('  <path class="edge dash" d="M318,446 H1152" opacity=".5"/>')
    g.text(320, 466, "04 is the other shape: fields, not chunks", "lbl")
    g.arrow("M502,520 H557")
    g.card(560, 486, 190, 68, "flink", "Invoice → EUR", "merge pages · FX join", "euro")
    g.arrow("M750,520 H787")
    g.card(790, 486, 140, 68, "flink", "Reports", "GROUP BY", "chart")
    g.arrow("M930,520 H937")
    g.card(940, 476, 210, 88, "ok", "Live dashboard", "spend by month, vendor", "dashboard",
           sub2="⚡ lightning queries")

    # 05 — a lane of its own
    g.add('  <path class="edge dash" d="M318,592 H1152" opacity=".5"/>')
    g.text(320, 612, "05 is a live feed: SEC filings, converted as they are published", "lbl")
    g.card(36, 632, 250, 68, "ext", "SEC filings", "10-K and 10-Q, as filed", "archive")
    g.badge(36, 632, "05", "topic", r=12)
    g.arrow("M286,666 H331")
    g.pill(334, 641, 168, "filings", "one per filing")
    g.arrow("M502,666 H557")
    g.card(560, 639, 170, 54, "docling", "Docling", "called from Flink", "spark")
    g.arrow("M730,666 H787")
    g.card(790, 632, 170, 68, "flink", "Metrics", "figures per filing", "chart")
    g.arrow("M960,666 H977")
    g.card(980, 632, 170, 68, "ok", "Events", "what changed", "bolt")
    return g


def chunk_journey() -> Svg:
    g = Svg(1200, 660, "What the shared enrichment does to five chunks",
            "Example 01's sample document, chunk by chunk. 0: ordinary prose passes every step into "
            "enriched. 1: a soft hyphen across a line break is normalized; enriched with normalized=true. "
            "2: byte-identical to chunk 0, dropped by dedup. 3: an e-mail and a card number are redacted in "
            "enriched, and the unredacted original goes to quarantine. 4: 'Table 4' is too short; flagged "
            "keep=false, and rejected when DROP_LOW_QUALITY is TRUE.")
    g.header("Five chunks in, and where each one lands",
             "Example 01's sample document. Every example from the chunk topic on runs these same steps.")

    g.text(40, 122, "CHUNK IN", "region-t t-topic")
    g.text(522, 122, "STEP THAT DECIDES", "region-t t-flink")
    g.text(780, 122, "WHERE IT LANDS", "region-t t-ok")

    y0, step = 138, 100
    rows = [
        # (prose runs line 1, line 2, note)
        ([("Retrieval quality is decided long before the", "")],
         [("vector store. The chunk boundary, the …", "")], None),
        ([("The inter", ""), ("‐⏎", "t-warn"), ("national standard applies to", "")],
         [("every member state, and the transition …", "")], "soft hyphen + line break"),
        ([("Retrieval quality is decided long before the", "")],
         [("vector store. The chunk boundary, the …", "")], "byte-identical to 0"),
        ([("For renewals write to ", ""), ("alice.smith@example.com", "t-bad")],
         [("quoting the card ending ", ""), ("4111 1111 1111 1111", "t-bad"), (" …", "")], None),
        ([("Table 4", "")], [], "too short to be useful"),
    ]
    for i, (l1, l2, note) in enumerate(rows):
        y = y0 + i * step
        g.add(f'  <rect class="card f-ext shadow" filter="url(#shadow)" x="40" y="{y}" width="420" height="78" rx="12"/>')
        g.badge(64, y + 39, str(i), "topic", r=13)
        g.rich(88, y + 33 if l2 else y + 44, l1, "mono")
        if l2:
            g.rich(88, y + 52, l2, "mono")
        if note:
            g.text(448, y + 70, note, "s", "end")

    def stage(i, icon, label, role="flink"):
        y = y0 + i * step + 24
        g.arrow(f"M460,{y + 15} H517")
        g.chip(520, y, 200, role, label, icon)

    stage(0, "check", "passes every step")
    stage(1, "sliders", "normalize")
    stage(2, "copy", "dedup")
    stage(3, "shield", "PII guard")
    stage(4, "gauge", "quality gate")

    def land(i, role, icon, title, sub, mono=False, dy=0, h=78):
        y = y0 + i * step + dy
        g.card(780, y, 380, h, role, title, sub, icon, mono=mono)

    for i in (0, 1, 2, 4):
        cy = y0 + i * step + 39
        g.arrow(f"M720,{cy} H777")
    land(0, "ok", "check", "chunks.enriched", "as it came, with derived fields")
    land(1, "ok", "check", "chunks.enriched", "“international” · normalized=true, raw_text kept")
    land(2, "mute", "trash", "dropped", "same fingerprint as chunk 0")
    # chunk 3 forks
    cy = y0 + 3 * step + 39
    g.arrow(f"M720,{cy} C750,{cy} 750,{cy - 21} 777,{cy - 21}")
    g.arrow(f"M720,{cy} C750,{cy} 750,{cy + 21} 777,{cy + 21}")
    y3 = y0 + 3 * step
    g.add(f'  <rect class="card f-ok shadow" filter="url(#shadow)" x="780" y="{y3 - 6}" width="380" height="38" rx="10"/>')
    g.icon("check", 792, y3 + 3, 20, "ok")
    g.rich(820, y3 + 18, [("enriched  ", "h t-ok"), ("write to ", "s"), ("[REDACTED:EMAIL]", "mono t-ok")])
    g.add(f'  <rect class="card f-warn shadow" filter="url(#shadow)" x="780" y="{y3 + 44}" width="380" height="38" rx="10"/>')
    g.icon("lock", 792, y3 + 53, 20, "warn")
    g.rich(820, y3 + 68, [("pii.quarantine  ", "h t-warn"), ("the original, unredacted", "s")])
    land(4, "bad", "xcircle", "quality.rejected", "keep=false · dropped when DROP_LOW_QUALITY")
    return g


def ex01() -> Svg:
    g = Svg(1200, 400, "Example 01 — Docling outside Flink",
            "Your PDFs are converted and chunked by Docling for IBM watsonx, outside Confluent Cloud, which writes one Kafka "
            "message per chunk with its kafka_chunks target. In Confluent Cloud a Flink statement "
            "normalizes, PII-guards, deduplicates and quality-gates the chunks into enriched, quarantine "
            "and rejected topics.")
    g.header("01 — Docling outside, Flink inside",
             "Docling for IBM watsonx converts; the chunks stream in. The simplest shape, and the one to read first.")
    g.card(36, 178, 150, 70, "ext", "Your PDFs", "any documents", "docs")
    g.arrow("M186,213 H223")
    g.region(206, 112, 276, 258, "Outside Confluent Cloud", "docling")
    g.card(220, 160, 248, 106, "docling", "Docling for IBM watsonx", "convert + chunk", "spark",
           sub2="kafka_chunks target")
    g.text(344, 300, "one Kafka message", "s", "middle")
    g.text(344, 316, "per chunk", "s", "middle")

    g.region(500, 112, 664, 258, "Confluent Cloud", "flink", "cloud")
    g.arrow("M468,213 H535")
    g.pill(538, 188, 132, "chunks", "the input topic")
    g.arrow("M670,213 H711")
    enrichment(g, 714, 138, 190, 216, "Enrichment", "one Flink statement")
    mids = outputs(g, 934, 158, 214, 66)
    fan(g, 904, 246, 934, mids)
    return g


def ex02() -> Svg:
    g = Svg(1200, 470, "Example 02 — Docling in Flink, from public URLs",
            "You write URLs to a topic. A Flink statement calls Docling for IBM watsonx for each one, keeps "
            "every answer on a conversions topic for good, and chunks it onto the chunk topic. The shared "
            "enrichment takes it from there into enriched, quarantine and rejected topics.")
    g.header("02 — Docling in Flink, from a list of URLs",
             "Write a URL, get chunks. Flink calls Docling for each one, and keeps every answer.")

    g.card(36, 270, 184, 90, "ext", "You", "one row per URL:", "link", sub2="doc_id, https://…pdf")

    # Docling, outside, above
    g.card(436, 96, 250, 66, "docling", "Docling for IBM watsonx", "the conversion service", "spark")

    g.region(244, 196, 920, 254, "Confluent Cloud", "flink", "cloud")
    g.arrow("M220,315 H261")
    g.pill(264, 290, 112, "urls")
    g.arrow("M376,315 H413")

    # convert statement
    g.add('  <rect class="card f-flink shadow" filter="url(#shadow)" x="416" y="236" width="290" height="190" rx="14"/>')
    g.icon("stream", 432, 250, 20, "flink")
    g.text(460, 266, "convert", "h t-flink")
    g.text(432, 288, "one Flink statement", "s")
    g.chip(432, 302, 258, "docling", "dc_convert · Java UDF, async", "spark")
    g.chip(432, 342, 258, "flink", "dc_chunks · headings + paragraphs", "scissors")
    g.chip(432, 382, 258, "mute", "failures are rows, not crashes", "check")
    g.arrow("M600,236 V165", "URL", (592, 190), anchor="end")
    g.arrow("M660,165 V233", "Markdown", (668, 190))

    g.arrow("M706,317 C730,317 730,262 753,262")
    g.pill(756, 238, 188, "conversions", "kept for good · paid once", role="topic", icon="archive")
    g.arrow("M706,357 C730,357 730,372 753,372")
    g.pill(756, 348, 120, "chunks", "the seam")
    g.arrow("M842,288 C842,315 822,325 812,345", "re-chunk on redeploy", (850, 322), dashed=True)

    g.arrow("M876,373 H905")
    g.card(908, 330, 246, 86, "flink", "Enrichment", "the same steps as example 01", "stream",
           sub2="→ the 3 output topics")
    g.text(1144, 126, "Each URL is one billed conversion;", "s", "end")
    g.text(1144, 142, "a redeploy never converts twice.", "s", "end")
    return g


def ex03() -> Svg:
    g = Svg(1200, 520, "Example 03 — Docling in Flink, from a private S3 bucket",
            "Upload a file to a private S3 bucket. Its notification goes to an SQS queue, which Confluent's "
            "managed SQS Source connector turns into records on an objects topic. A Flink statement presigns "
            "each object and calls Docling for IBM watsonx, which fetches the file itself by the presigned "
            "URL. Conversions are kept, chunks go through the shared enrichment.")
    g.header("03 — Docling in Flink, from a private S3 bucket",
             "Drop a file in the bucket and it is converted. Nobody types anything, and the bucket stays private.")

    g.region(28, 104, 236, 396, "Your AWS account", "aws")
    g.card(46, 140, 200, 62, "aws", "Upload", "aws s3 cp, console, …", "upload")
    g.arrow("M146,202 V227")
    g.card(46, 230, 200, 62, "aws", "S3 bucket", "private", "bucket")
    g.arrow("M146,292 V317", "ObjectCreated", (156, 310))
    g.card(46, 320, 200, 62, "aws", "SQS queue", "one message per file", "queue")
    g.card(46, 416, 200, 62, "ext", "backfill", "files already there", "refresh", dashed=True)
    g.arrow("M146,416 V385")

    g.card(476, 100, 250, 66, "docling", "Docling for IBM watsonx", "fetches the file itself", "spark")
    g.arrow("M246,250 C330,250 360,133 473,133", dashed=True)
    g.text(262, 122, "Docling GETs the file", "lbl")
    g.text(262, 138, "by its presigned URL", "lbl")

    g.region(288, 200, 880, 300, "Confluent Cloud", "flink", "cloud")
    g.arrow("M246,351 H299")
    g.card(302, 322, 164, 58, "flink", "SQS Source", "connector", "plug")
    g.arrow("M384,380 V405")
    g.pill(318, 408, 126, "objects", "S3 events")
    g.arrow("M444,433 H473")

    g.add('  <rect class="card f-flink shadow" filter="url(#shadow)" x="476" y="240" width="300" height="240" rx="14"/>')
    g.icon("stream", 492, 254, 20, "flink")
    g.text(520, 270, "convert", "h t-flink")
    g.text(492, 292, "one Flink statement", "s")
    g.chip(492, 306, 268, "flink", "decode the event · skip folders", "doc")
    g.chip(492, 346, 268, "aws", "dc_presign · a URL valid 10 min", "key")
    g.chip(492, 386, 268, "docling", "dc_convert · Java UDF, async", "spark")
    g.chip(492, 426, 268, "flink", "dc_chunks", "scissors")
    g.arrow("M600,240 V169", "presigned URL", (592, 190), anchor="end")
    g.arrow("M680,169 V237", "Markdown", (688, 190))

    g.arrow("M776,401 C800,401 800,300 823,300")
    g.pill(826, 276, 188, "conversions", "kept for good · paid once", icon="archive")
    g.arrow("M776,441 H823")
    g.pill(826, 416, 120, "chunks", "the seam")
    g.arrow("M946,441 H975")
    g.card(978, 356, 172, 120, "flink", "Enrichment", "as in example 01", "stream", sub2="→ 3 topics")
    g.text(1010, 236, "From the presigned URL on,", "s")
    g.text(1010, 252, "it is example 02.", "s")
    return g


def ex04() -> Svg:
    g = Svg(1200, 560, "Example 04 — invoice extraction on a simulated clock",
            "A control app releases simulated days of supplier invoices and ECB FX rates. One Flink statement "
            "extracts vendor, total and line items from each invoice — with Docling for real, or synthetically "
            "from the ground truth, chosen per release — merges the pages, converts to EUR at the day's rate, "
            "and keeps daily, monthly and per-vendor spend reports. A live dashboard reads them; the Real-Time "
            "Context Engine serves lightning queries.")
    g.header("04 — Invoices to a live spend dashboard",
             "Release a day of invoices; seconds later the charts move. Fields, not chunks: vendor, total, line items.")

    # control app
    g.add('  <rect class="card f-ext shadow" filter="url(#shadow)" x="36" y="130" width="210" height="190" rx="14"/>')
    g.disc("clock", 70, 166, "ext")
    g.text(98, 162, "Control app", "h")
    g.text(98, 180, "a simulated clock", "s")
    # a little slider
    g.add('  <rect x="56" y="206" width="170" height="6" rx="3" class="l-topic"/>')
    g.add('  <rect x="56" y="206" width="96" height="6" rx="3" class="d-topic"/>')
    g.add('  <circle cx="152" cy="209" r="8" class="f-topic card"/>')
    g.text(56, 236, "Jan", "s")
    g.text(226, 236, "Jun", "s", "end")
    g.chip(56, 252, 82, "mute", "Synthetic")
    g.chip(142, 252, 84, "docling", "Real", "spark")
    g.text(56, 304, "+1 day · +1 week · Play", "s")

    g.region(270, 104, 650, 432, "Confluent Cloud", "flink", "cloud")
    g.arrow("M246,190 C266,190 266,170 285,170")
    g.pill(288, 146, 150, "submissions", "invoice PDFs")
    g.arrow("M246,270 C266,270 266,440 285,440")
    g.pill(288, 416, 150, "fx.rates", "ECB, per day")

    # the one statement
    g.add('  <rect class="card f-flink shadow" filter="url(#shadow)" x="464" y="130" width="236" height="386" rx="14"/>')
    g.icon("stream", 480, 144, 20, "flink")
    g.text(508, 160, "pipeline", "h t-flink")
    g.text(480, 180, "one Flink statement, five views", "s")
    g.arrow("M438,171 H461")
    steps = [
        ("docling", "spark", "extract", "Docling or synthetic"),
        ("flink", "merge", "invoice()", "merge pages · ok/review"),
        ("flink", "euro", "× FX rate", "→ EUR, that day"),
        ("flink", "chart", "GROUP BY", "day · month · vendor"),
    ]
    for i, (role, ic, t, s) in enumerate(steps):
        y = 196 + i * 78
        g.card(480, y, 204, 62, role, t, s, ic)
        if i:
            g.arrow(f"M582,{y - 16} V{y - 3}")
    g.arrow("M438,441 H452 V352 H477")

    # tables
    tables = [("extractions", "verbatim, append"), ("invoices", "ok or review"),
              ("spend", "per invoice, EUR"), ("spend.daily …", "monthly · vendors")]
    for i, (t, s) in enumerate(tables):
        y = 202 + i * 78
        g.arrow(f"M684,{y + 25} H723")
        g.pill(726, y, 172, t, s)

    # the outside world
    g.add('  <rect class="card f-ok shadow" filter="url(#shadow)" x="944" y="130" width="220" height="230" rx="14"/>')
    g.disc("dashboard", 978, 166, "ok")
    g.text(1006, 162, "Live dashboard", "h t-ok")
    g.text(1006, 180, "localhost:8004", "s")
    bars = [38, 52, 44, 70, 61, 96, 82]
    for i, b in enumerate(bars):
        g.add(f'  <rect x="{966 + i * 26}" y="{330 - b}" width="18" height="{b}" rx="3" class="d-ok" opacity="{.45 + i * .08:.2f}"/>')
    g.add('  <path class="edge" d="M960,332 H1146" opacity=".5"/>')
    g.text(966, 350, "spend by month, in EUR", "s")

    g.card(944, 384, 220, 90, "warn", "⚡ Lightning", "Real-Time Context Engine", "bolt",
           sub2="lookups in milliseconds")
    g.arrow("M898,227 C920,227 920,245 941,245")
    g.arrow("M898,461 C920,461 920,430 941,430", dashed=True)
    return g


DIAGRAMS = {
    "overview.svg": overview,
    "chunk-journey.svg": chunk_journey,
    "ex01.svg": ex01,
    "ex02.svg": ex02,
    "ex03.svg": ex03,
    "ex04.svg": ex04,
}

if __name__ == "__main__":
    for ANIMATED in (False, True):
        for name, draw in DIAGRAMS.items():
            if ANIMATED:
                name = name.replace(".svg", ".animated.svg")
            (OUT / name).write_text(draw().render(), encoding="utf-8")
            print(f"wrote docs/05-confluent-cloud/diagrams/{name}")
