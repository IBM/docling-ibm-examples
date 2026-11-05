"""Seven invoice layouts, drawn with ReportLab.

Not seven colour schemes over one template: the layouts differ where real
invoices differ, because that is what an extraction model actually has to cope
with —

    de_classic   German, DIN-5008-ish letter: sender line over the address
                 window, info block on the right, Netto / USt. / Brutto,
                 three-column footer with bank and register. 1.234,56 €
    de_modern    German, accent sidebar, totals in a coloured box
    us_saas      US SaaS: Letter paper, big INVOICE, "Amount due", $1,234.56,
                 US dates (Jan 5, 2026 — or 01/05/2026, which is the point)
    eu_saas      English from an EU supplier, reverse-charge note, €1,234.56
    uk_legal     a law firm's fee note: serif, narrative time entries with
                 fee-earner and hours, £, "5 January 2026"
    ch_qr        Swiss: CHF 1'234.50 and a payment part with a QR code at the
                 bottom of the last page
    hotel_folio  Portuguese hotel folio: per-night rows, three IVA rates, a
                 tourist tax, 1 234,56 € and ISO dates

Long invoices paginate, and continuation pages repeat the vendor and invoice
number but not the totals — which is exactly how page-by-page extraction sees
them (see ``udf/src/invoice_udf/fake_docling.py``).

``render()`` returns the page each line item landed on, so the ground truth in
the manifest knows it too.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import math
import random
from decimal import Decimal
from typing import ClassVar

from company import BUYER, Invoice, Line, Vendor
from reportlab.lib.colors import Color, HexColor, white
from reportlab.lib.pagesizes import A4, LETTER
from reportlab.lib.units import mm
from reportlab.lib.utils import simpleSplit
from reportlab.pdfgen.canvas import Canvas

GREY = HexColor("#6b7280")
LIGHT = HexColor("#e5e7eb")
INK = HexColor("#111827")

# --- formatting ---------------------------------------------------------------


def num(x: Decimal, style: str, places: int = 2) -> str:
    s = f"{x:,.{places}f}"
    if style == "de":
        return s.replace(",", "§").replace(".", ",").replace("§", ".")
    if style == "ch":
        return s.replace(",", "'")
    if style == "pt":
        return s.replace(",", " ").replace(".", ",")
    return s


def qty(x: Decimal, style: str) -> str:
    s = f"{x.normalize():f}" if x == x.to_integral() else f"{x:.1f}"
    s = s.split(".")[0] if "." in s and set(s.split(".")[1]) <= {"0"} else s
    return s.replace(".", ",") if style in ("de", "pt") else s


def price_places(x: Decimal) -> int:
    """Metered prices keep their four decimals, as a cloud bill prints them."""
    return 2 if x == x.quantize(Decimal("0.01")) else 4


def cash(x: Decimal, currency: str, style: str, places: int = 2) -> str:
    sign = "-" if x < 0 else ""
    body = num(abs(x), style, places)
    if style == "de":
        return f"{sign}{body} €"
    if style == "pt":
        return f"{sign}{body} €"
    if style == "ch":
        return f"{sign}{body}"
    symbol = {"USD": "$", "EUR": "€", "GBP": "£"}.get(currency, currency + " ")
    return f"{sign}{symbol}{body}"


DATE_STYLES = {
    "de": lambda d: d.strftime("%d.%m.%Y"),
    "us_short": lambda d: d.strftime("%b ") + str(d.day) + d.strftime(", %Y"),
    "us_long": lambda d: d.strftime("%B ") + str(d.day) + d.strftime(", %Y"),
    "us_numeric": lambda d: d.strftime("%m/%d/%Y"),
    "eu_slash": lambda d: d.strftime("%d/%m/%Y"),
    "uk": lambda d: f"{d.day} {d:%B %Y}",
    "iso": lambda d: d.isoformat(),
}

VENDOR_DATES = {"nimbus": "us_short", "quillboard": "us_numeric", "reachly": "us_long", "stackwise": "eu_slash",
                "brightline": "uk", "kestrel": "uk", "arcadia": "iso", "learnwell": "de"}


def date_fmt(vendor: Vendor):
    return DATE_STYLES[VENDOR_DATES.get(vendor.key, "de")]


# --- logos --------------------------------------------------------------------


def logo(c: Canvas, kind: str, x: float, y: float, s: float, color) -> None:
    """A small geometric mark, ``s`` points square, bottom-left at ``(x, y)``."""
    c.saveState()
    c.setFillColor(color)
    c.setStrokeColor(color)
    if kind == "cloud":
        for dx, dy, r in ((0.3, 0.35, 0.22), (0.55, 0.5, 0.28), (0.78, 0.35, 0.2)):
            c.circle(x + dx * s, y + dy * s, r * s, stroke=0, fill=1)
        c.rect(x + 0.1 * s, y + 0.14 * s, 0.8 * s, 0.22 * s, stroke=0, fill=1)
    elif kind == "stack":
        for i in range(3):
            c.roundRect(x + i * 0.08 * s, y + i * 0.3 * s, 0.75 * s, 0.22 * s, 0.05 * s, stroke=0, fill=1)
    elif kind == "quill":
        p = c.beginPath()
        p.moveTo(x + 0.15 * s, y)
        p.curveTo(x + 0.2 * s, y + 0.7 * s, x + 0.6 * s, y + 0.95 * s, x + s, y + s)
        p.curveTo(x + 0.7 * s, y + 0.6 * s, x + 0.5 * s, y + 0.2 * s, x + 0.15 * s, y)
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "dots":
        for i in range(3):
            for j in range(3):
                if (i + j) % 2 == 0:
                    c.circle(x + (0.17 + i * 0.33) * s, y + (0.17 + j * 0.33) * s, 0.14 * s, stroke=0, fill=1)
    elif kind == "square":
        c.rect(x, y, s, s, stroke=0, fill=1)
        c.setFillColor(white)
        c.rect(x + 0.25 * s, y + 0.25 * s, 0.5 * s, 0.5 * s, stroke=0, fill=1)
    elif kind == "leaf":
        p = c.beginPath()
        p.moveTo(x, y)
        p.curveTo(x, y + 0.8 * s, x + 0.5 * s, y + s, x + s, y + s)
        p.curveTo(x + s, y + 0.3 * s, x + 0.4 * s, y, x, y)
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "wave":
        c.setLineWidth(0.12 * s)
        for k in range(3):
            p = c.beginPath()
            yy = y + (0.2 + 0.3 * k) * s
            p.moveTo(x, yy)
            p.curveTo(x + 0.3 * s, yy + 0.2 * s, x + 0.6 * s, yy - 0.2 * s, x + s, yy)
            c.drawPath(p, stroke=1, fill=0)
    elif kind == "bolt":
        p = c.beginPath()
        for px, py in ((0.6, 1), (0.15, 0.45), (0.45, 0.45), (0.35, 0), (0.85, 0.58), (0.55, 0.58)):
            (p.lineTo if px != 0.6 or py != 1 else p.moveTo)(x + px * s, y + py * s)
        p.close()
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "compass":
        c.circle(x + s / 2, y + s / 2, s / 2, stroke=0, fill=1)
        c.setFillColor(white)
        p = c.beginPath()
        p.moveTo(x + s / 2, y + 0.9 * s)
        p.lineTo(x + 0.6 * s, y + s / 2)
        p.lineTo(x + s / 2, y + 0.1 * s)
        p.lineTo(x + 0.4 * s, y + s / 2)
        p.close()
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "arch":
        c.rect(x, y, s, 0.12 * s, stroke=0, fill=1)
        c.setLineWidth(0.12 * s)
        c.arc(x + 0.12 * s, y + 0.05 * s, x + 0.88 * s, y + 0.95 * s, 0, 180)
        c.line(x + 0.18 * s, y + 0.1 * s, x + 0.18 * s, y + 0.5 * s)
        c.line(x + 0.82 * s, y + 0.1 * s, x + 0.82 * s, y + 0.5 * s)
    elif kind == "spark":
        p = c.beginPath()
        cx, cy = x + s / 2, y + s / 2
        for k in range(8):
            r = 0.5 * s if k % 2 == 0 else 0.16 * s
            a = math.pi / 2 + k * math.pi / 4
            (p.moveTo if k == 0 else p.lineTo)(cx + r * math.cos(a), cy + r * math.sin(a))
        p.close()
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "pixel":
        for i, j in ((0, 0), (1, 0), (2, 0), (0, 1), (2, 1), (0, 2), (1, 2), (2, 2), (1, 1)):
            if (i, j) != (1, 1):
                c.rect(x + i * s / 3 + 0.02 * s, y + j * s / 3 + 0.02 * s, s / 3 - 0.04 * s, s / 3 - 0.04 * s,
                       stroke=0, fill=1)
    elif kind == "kestrel":
        p = c.beginPath()
        p.moveTo(x, y + 0.6 * s)
        p.lineTo(x + 0.5 * s, y + 0.2 * s)
        p.lineTo(x + s, y + 0.75 * s)
        p.lineTo(x + 0.5 * s, y + 0.45 * s)
        p.close()
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "columns":
        c.rect(x, y, s, 0.1 * s, stroke=0, fill=1)
        for k in range(4):
            c.rect(x + (0.08 + 0.24 * k) * s, y + 0.14 * s, 0.12 * s, 0.6 * s, stroke=0, fill=1)
        p = c.beginPath()
        p.moveTo(x, y + 0.78 * s)
        p.lineTo(x + s / 2, y + s)
        p.lineTo(x + s, y + 0.78 * s)
        p.close()
        c.drawPath(p, stroke=0, fill=1)
    elif kind == "bridge":
        c.setLineWidth(0.1 * s)
        c.arc(x, y + 0.1 * s, x + s, y + 1.1 * s, 180, 180)
        c.line(x, y + 0.6 * s, x + s, y + 0.6 * s)
        for k in range(5):
            c.line(x + (0.1 + 0.2 * k) * s, y + 0.6 * s, x + (0.1 + 0.2 * k) * s, y + 0.3 * s)
    elif kind == "book":
        c.rect(x, y + 0.1 * s, 0.47 * s, 0.8 * s, stroke=0, fill=1)
        c.rect(x + 0.53 * s, y + 0.1 * s, 0.47 * s, 0.8 * s, stroke=0, fill=1)
    elif kind == "fork":
        c.setLineWidth(0.08 * s)
        for k in range(3):
            c.line(x + (0.3 + 0.2 * k) * s, y + 0.6 * s, x + (0.3 + 0.2 * k) * s, y + s)
        c.line(x + 0.3 * s, y + 0.6 * s, x + 0.7 * s, y + 0.6 * s)
        c.line(x + 0.5 * s, y, x + 0.5 * s, y + 0.6 * s)
    c.restoreState()


# --- the generic engine -------------------------------------------------------


class Layout:
    """Paginated invoice: header, a table of lines, totals, notes, footer.

    Subclasses set the column set and the page furniture; pagination is here
    once. Heights are computed before anything is drawn, so every page knows
    "page x of n" and the table never runs into the footer.
    """

    pagesize = A4
    font, bold = "Helvetica", "Helvetica-Bold"
    size = 8.5
    style = "de"  # number style
    margin_l = 20 * mm
    margin_r = 20 * mm
    first_table_top = 170 * mm
    cont_table_top = 245 * mm
    bottom = 32 * mm
    #: Extra space the last page keeps free below the notes (a payment slip).
    last_reserve = 0.0
    row_pad = 2.6
    labels: ClassVar[dict[str, str]] = {}
    # (key, label key, width in mm or None for the flexible column, align)
    columns: tuple[tuple[str, str, float | None, str], ...] = ()

    def __init__(self, inv: Invoice):
        self.inv = inv
        self.v = inv.vendor
        self.accent = HexColor(self.v.accent)
        self.d = date_fmt(self.v)
        self.width, self.height = self.pagesize
        self.inner = self.width - self.margin_l - self.margin_r
        flex = self.inner - sum(w * mm for _, _, w, _ in self.columns if w)
        self.cols = [(k, lab, (w * mm if w else flex), a) for k, lab, w, a in self.columns]

    # sizes
    def desc_width(self) -> float:
        return next(w for k, _, w, _ in self.cols if k == "desc") - 4

    def row_height(self, line: Line) -> float:
        n = len(simpleSplit(line.description, self.font, self.size, self.desc_width()))
        if line.detail and self.v.layout != "hotel_folio":
            n += len(simpleSplit(line.detail, self.font, self.size - 1.5, self.desc_width()))
        return n * (self.size + 2) + self.row_pad * 2

    def totals_height(self) -> float:
        return (len(self.totals_rows()) + 1) * (self.size + 6) + 6 * mm

    def notes_height(self) -> float:
        return len(self.notes()) * (self.size + 3.5) + 4 * mm

    # pagination
    def paginate(self) -> list[list[Line]]:
        header = self.size + 10
        pages: list[list[Line]] = [[]]
        y = self.first_table_top - header
        for line in self.inv.lines:
            h = self.row_height(line)
            if y - h < self.bottom:
                pages.append([])
                y = self.cont_table_top - header
            pages[-1].append(line)
            y -= h
        if y - self.totals_height() - self.notes_height() - self.last_reserve < self.bottom:
            pages.append([])
        return pages

    # drawing
    def render(self, path: str) -> tuple[int, list[int]]:
        pages = self.paginate()
        c = Canvas(path, pagesize=self.pagesize, invariant=1)
        c.setTitle(f"{self.labels['invoice']} {self.inv.number}")
        c.setAuthor(self.v.name)
        c.setCreator("Lumen demo invoice generator")
        n = len(pages)
        line_pages: list[int] = []
        pos = 0
        for i, rows in enumerate(pages, 1):
            if i == 1:
                self.first_header(c)
                y = self.first_table_top
            else:
                self.cont_header(c, i, n)
                y = self.cont_table_top
            if rows or i == 1:
                y = self.table_header(c, y)
            for line in rows:
                pos += 1
                y = self.row(c, y, pos, line)
                line_pages.append(i)
            if i == n:
                y = self.totals(c, y - 4 * mm)
                self.draw_notes(c, y - 4 * mm)
                self.last_page(c)
            self.footer(c, i, n)
            self.demo_mark(c)
            c.showPage()
        c.save()
        return n, line_pages

    def demo_mark(self, c):
        """Every page says what it is, along the left edge, outside the content.

        These invoices are realistic on purpose and come from companies that do
        not exist; a page that leaves the demo should not be mistakable for a
        real one. Small, grey and outside the margins, so it does not change
        what an extraction model is asked to read.
        """
        c.saveState()
        c.translate(5 * mm, 20 * mm)
        c.rotate(90)
        self.text(c, 0, 0, "DEMO DOCUMENT — fictional company, generated for a software demo — not a real invoice",
                  "Helvetica", 6, GREY)
        c.restoreState()

    def text(self, c, x, y, s, font=None, size=None, color=INK, align="l"):
        c.setFont(font or self.font, size or self.size)
        c.setFillColor(color)
        {"l": c.drawString, "r": c.drawRightString, "c": c.drawCentredString}[align](x, y, s)

    def table_header(self, c, y):
        x = self.margin_l
        c.setFillColor(LIGHT)
        c.rect(x, y - self.size - 6, self.inner, self.size + 8, stroke=0, fill=1)
        for key, lab, w, a in self.cols:
            tx = x + 2 if a == "l" else x + w - 2
            self.text(c, tx, y - self.size - 2, self.labels[lab], self.bold, align=a)
            x += w
        return y - self.size - 10

    def cell(self, key: str, pos: int, line: Line) -> str:
        cur = self.inv.vendor.currency
        return {
            "pos": str(pos),
            "qty": qty(line.quantity, self.style),
            "unit": line.unit,
            "price": num(line.unit_price, self.style, price_places(line.unit_price)) if self.style != "en"
            else cash(line.unit_price, cur, "en", price_places(line.unit_price)),
            "amount": num(line.amount, self.style) if self.style != "en" else cash(line.amount, cur, "en"),
            "tax": f"{line.tax_rate:g} %" if self.style != "en" else f"{line.tax_rate:g}%",
            "date": line.detail if self.v.layout == "hotel_folio" else "",
        }.get(key, "")

    def row(self, c, y, pos, line):
        x = self.margin_l
        top = y - self.row_pad - self.size
        for key, _, w, a in self.cols:
            if key == "desc":
                yy = top
                for part in simpleSplit(line.description, self.font, self.size, self.desc_width()):
                    self.text(c, x + 2, yy, part)
                    yy -= self.size + 2
                if line.detail and self.v.layout != "hotel_folio":
                    for part in simpleSplit(line.detail, self.font, self.size - 1.5, self.desc_width()):
                        self.text(c, x + 2, yy, part, size=self.size - 1.5, color=GREY)
                        yy -= self.size + 2
            else:
                s = self.cell(key, pos, line)
                self.text(c, x + 2 if a == "l" else x + w - 2, top, s, align=a)
            x += w
        y -= self.row_height(line)
        c.setStrokeColor(LIGHT)
        c.setLineWidth(0.4)
        c.line(self.margin_l, y, self.margin_l + self.inner, y)
        return y

    def totals_rows(self) -> list[tuple[str, str]]:
        raise NotImplementedError

    def total_row(self) -> tuple[str, str]:
        raise NotImplementedError

    def totals(self, c, y):
        xl = self.margin_l + self.inner * 0.5
        xr = self.margin_l + self.inner - 2
        for label, value in self.totals_rows():
            y -= self.size + 6
            self.text(c, xl, y, label)
            self.text(c, xr, y, value, align="r")
        y -= 4
        c.setStrokeColor(INK)
        c.setLineWidth(0.8)
        c.line(xl, y, xr, y)
        y -= self.size + 6
        label, value = self.total_row()
        self.text(c, xl, y, label, self.bold, self.size + 1.5)
        self.text(c, xr, y, value, self.bold, self.size + 1.5, align="r")
        return y - 2 * mm

    def notes(self) -> list[str]:
        return []

    def draw_notes(self, c, y):
        for note in self.notes():
            y -= self.size + 3.5
            self.text(c, self.margin_l, y, note, size=self.size - 0.5, color=GREY if note.startswith(" ") else INK)

    def last_page(self, c):
        pass

    def first_header(self, c):
        raise NotImplementedError

    def cont_header(self, c, page, pages):
        logo(c, self.v.logo, self.margin_l, self.height - 22 * mm, 9 * mm, self.accent)
        self.text(c, self.margin_l + 12 * mm, self.height - 17 * mm, self.v.name, self.bold, 11)
        self.text(c, self.width - self.margin_r, self.height - 15 * mm,
                  f"{self.labels['invoice']} {self.inv.number}", self.bold, 10, align="r")
        self.text(c, self.width - self.margin_r, self.height - 20 * mm,
                  self.labels["page"].format(page, pages), color=GREY, align="r")

    def footer(self, c, page, pages):
        raise NotImplementedError


# --- German -----------------------------------------------------------------

DE = {
    "invoice": "Rechnung", "number": "Rechnungsnummer", "date": "Rechnungsdatum", "customer": "Kundennummer",
    "period": "Leistungszeitraum", "due": "Zahlbar bis", "ref": "Ihre Referenz", "pos": "Pos.", "qty": "Menge",
    "unit": "Einheit", "desc": "Bezeichnung", "price": "Einzelpreis €", "amount": "Gesamt €", "tax": "USt.",
    "page": "Seite {} von {}", "vat": "USt-IdNr.",
}


class DeClassic(Layout):
    labels = DE
    columns = (("pos", "pos", 10, "l"), ("desc", "desc", None, "l"), ("qty", "qty", 14, "r"),
               ("unit", "unit", 16, "l"), ("tax", "tax", 13, "r"), ("price", "price", 24, "r"),
               ("amount", "amount", 24, "r"))
    first_table_top = 174 * mm
    bottom = 34 * mm

    def first_header(self, c):
        v, inv = self.v, self.inv
        top = self.height - 18 * mm
        logo(c, v.logo, self.width - self.margin_r - 14 * mm, top - 10 * mm, 14 * mm, self.accent)
        self.text(c, self.width - self.margin_r - 17 * mm, top - 3 * mm, v.name, self.bold, 12, align="r")
        y = top - 8 * mm
        for part in v.address:
            self.text(c, self.width - self.margin_r - 17 * mm, y, part, align="r", color=GREY)
            y -= 4 * mm
        # The sender line above the address window.
        self.text(c, self.margin_l, self.height - 50 * mm, f"{v.name} · {v.address[0]} · {v.address[1]}",
                  size=6.5, color=GREY)
        c.setStrokeColor(GREY)
        c.setLineWidth(0.3)
        c.line(self.margin_l, self.height - 51 * mm, self.margin_l + 85 * mm, self.height - 51 * mm)
        y = self.height - 57 * mm
        for part in (BUYER["name"], BUYER["attn"]) + BUYER["address"]:
            self.text(c, self.margin_l, y, part, size=10)
            y -= 4.6 * mm
        # The info block.
        rows = [(DE["number"], inv.number), (DE["date"], self.d(inv.date))]
        if v.customer_no:
            rows.append((DE["customer"], v.customer_no))
        if inv.period:
            rows.append((DE["period"], f"{self.d(inv.period[0])} – {self.d(inv.period[1])}"))
        if inv.reference:
            rows.append((DE["ref"], inv.reference))
        rows.append((DE["vat"] + " Kunde", BUYER["vat_id"]))
        y = self.height - 57 * mm
        for label, value in rows:
            self.text(c, self.width - self.margin_r - 75 * mm, y, label, color=GREY)
            self.text(c, self.width - self.margin_r, y, value, align="r")
            y -= 4.6 * mm
        self.text(c, self.margin_l, self.height - 100 * mm, f"{DE['invoice']} {inv.number}", self.bold, 15)
        intro = "Sehr geehrte Damen und Herren,"
        self.text(c, self.margin_l, self.height - 110 * mm, intro, size=9.5)
        self.text(c, self.margin_l, self.height - 115 * mm,
                  "wir erlauben uns, Ihnen folgende Lieferungen und Leistungen in Rechnung zu stellen:", size=9.5)

    def totals_rows(self):
        inv = self.inv
        rows = [("Nettobetrag", cash(inv.subtotal, "EUR", "de"))]
        for rate, net, tax in inv.tax_groups():
            if rate:
                rows.append((f"zzgl. {rate:g} % USt. auf {num(net, 'de')} €", cash(tax, "EUR", "de")))
            elif len(inv.tax_groups()) > 1:
                rows.append((f"steuerfrei / nicht steuerbar: {num(net, 'de')} €", cash(Decimal(0), "EUR", "de")))
        return rows

    def total_row(self):
        return "Rechnungsbetrag", cash(self.inv.total, "EUR", "de")

    def notes(self):
        inv = self.inv
        due = "sofort nach Erhalt" if self.v.terms_days == 0 else f"bis zum {self.d(inv.due)}"
        out = [f"Bitte überweisen Sie den Rechnungsbetrag {due} ohne Abzug unter Angabe der Rechnungsnummer.",
               "Leistungsdatum entspricht dem Rechnungsdatum, sofern nicht anders angegeben."]
        if self.v.tax_note:
            out.append(self.v.tax_note)
        out.append("Mit freundlichen Grüßen")
        out.append(f" {self.v.name}")
        return out

    def footer(self, c, page, pages):
        v = self.v
        c.setStrokeColor(LIGHT)
        c.line(self.margin_l, 26 * mm, self.width - self.margin_r, 26 * mm)
        cols = [
            [v.name, *v.address[:2], f"Tel. {v.phone}" if v.phone else ""],
            [v.bank, f"IBAN {v.iban}", f"BIC {v.bic}"],
            [f"{DE['vat']} {v.vat_id}", v.email, v.web],
        ]
        w = self.inner / 3
        for i, col in enumerate(cols):
            y = 22 * mm
            for part in col:
                for chunk in simpleSplit(part, self.font, 6.5, w - 4):
                    self.text(c, self.margin_l + i * w, y, chunk, size=6.5, color=GREY)
                    y -= 3.2 * mm
        if v.register:
            self.text(c, self.width / 2, 8 * mm, v.register, size=6, color=GREY, align="c")
        self.text(c, self.width - self.margin_r, 12 * mm - 6, DE["page"].format(page, pages), size=6.5,
                  color=GREY, align="r")


class DeModern(DeClassic):
    font, bold = "Helvetica", "Helvetica-Bold"
    margin_l = 28 * mm
    first_table_top = 205 * mm

    def first_header(self, c):
        v, inv = self.v, self.inv
        c.setFillColor(self.accent)
        c.rect(0, 0, 10 * mm, self.height, stroke=0, fill=1)
        logo(c, v.logo, self.margin_l, self.height - 30 * mm, 12 * mm, self.accent)
        self.text(c, self.margin_l + 15 * mm, self.height - 23 * mm, v.name, self.bold, 14)
        self.text(c, self.margin_l + 15 * mm, self.height - 28 * mm, " · ".join(v.address), size=7.5, color=GREY)
        self.text(c, self.width - self.margin_r, self.height - 23 * mm, DE["invoice"].upper(), self.bold, 20,
                  color=self.accent, align="r")
        y = self.height - 48 * mm
        self.text(c, self.margin_l, y + 6 * mm, "Rechnungsempfänger", self.bold, 7.5, color=GREY)
        for part in (BUYER["name"], BUYER["attn"]) + BUYER["address"]:
            self.text(c, self.margin_l, y, part, size=9.5)
            y -= 4.5 * mm
        rows = [("Nr.", inv.number), ("Datum", self.d(inv.date)), ("Fällig", self.d(inv.due))]
        if v.customer_no:
            rows.append(("Kunde", v.customer_no))
        if inv.reference:
            rows.append(("Referenz", inv.reference))
        if inv.period:
            rows.append(("Zeitraum", f"{self.d(inv.period[0])} – {self.d(inv.period[1])}"))
        y = self.height - 48 * mm
        for label, value in rows:
            self.text(c, self.width - self.margin_r - 62 * mm, y, label, self.bold, color=GREY)
            self.text(c, self.width - self.margin_r, y, value, align="r")
            y -= 4.5 * mm

    def table_header(self, c, y):
        x = self.margin_l
        for key, lab, w, a in self.cols:
            tx = x + 2 if a == "l" else x + w - 2
            self.text(c, tx, y - self.size - 2, self.labels[lab], self.bold, color=self.accent, align=a)
            x += w
        c.setStrokeColor(self.accent)
        c.setLineWidth(1.2)
        c.line(self.margin_l, y - self.size - 6, self.margin_l + self.inner, y - self.size - 6)
        return y - self.size - 10

    def totals(self, c, y):
        xl = self.margin_l + self.inner * 0.48
        box_h = (len(self.totals_rows()) + 1) * (self.size + 6) + 6
        c.setFillColor(Color(0.97, 0.97, 0.98))
        c.rect(xl - 3 * mm, y - box_h - 2, self.inner * 0.52 + 3 * mm, box_h, stroke=0, fill=1)
        return super().totals(c, y)

    def total_row(self):
        return "Gesamtbetrag", cash(self.inv.total, "EUR", "de")

    def notes(self):
        return [f"Zahlungsziel: {self.v.terms_days} Tage netto, fällig am {self.d(self.inv.due)}.",
                f"Bankverbindung: {self.v.bank} · IBAN {self.v.iban} · BIC {self.v.bic}",
                "Vielen Dank für die Zusammenarbeit!"]


# --- English ------------------------------------------------------------------

EN = {
    "invoice": "Invoice", "number": "Invoice number", "date": "Invoice date", "customer": "Customer",
    "period": "Billing period", "due": "Due date", "ref": "Reference", "pos": "#", "qty": "Qty", "unit": "Unit",
    "desc": "Description", "price": "Unit price", "amount": "Amount", "tax": "VAT", "page": "Page {} of {}",
    "vat": "VAT ID",
}


class UsSaas(Layout):
    pagesize = LETTER
    style = "en"
    labels = EN
    columns = (("desc", "desc", None, "l"), ("qty", "qty", 20, "r"), ("price", "price", 26, "r"),
               ("amount", "amount", 28, "r"))
    margin_l = margin_r = 18 * mm
    first_table_top = 168 * mm
    cont_table_top = 235 * mm
    bottom = 26 * mm

    def first_header(self, c):
        v, inv = self.v, self.inv
        c.setFillColor(self.accent)
        c.rect(0, self.height - 6 * mm, self.width, 6 * mm, stroke=0, fill=1)
        logo(c, v.logo, self.margin_l, self.height - 27 * mm, 12 * mm, self.accent)
        self.text(c, self.margin_l + 15 * mm, self.height - 20 * mm, v.name.replace(", Inc.", ""), self.bold, 15)
        y = self.height - 26 * mm
        for part in v.address[:2]:
            self.text(c, self.margin_l + 15 * mm, y, part, size=7.5, color=GREY)
            y -= 3.5 * mm
        self.text(c, self.width - self.margin_r, self.height - 22 * mm, "INVOICE", self.bold, 24, align="r")
        # bill to / details
        y0 = self.height - 52 * mm
        self.text(c, self.margin_l, y0, "BILL TO", self.bold, 7.5, color=GREY)
        y = y0 - 5 * mm
        for part in (BUYER["name"], *BUYER["address_en"], f"VAT ID {BUYER['vat_id']}"):
            self.text(c, self.margin_l, y, part, size=9)
            y -= 4.2 * mm
        rows = [("Invoice number", inv.number), ("Invoice date", self.d(inv.date)), ("Due date", self.d(inv.due))]
        if v.customer_no:
            rows.append(("Account", v.customer_no))
        if inv.period:
            rows.append(("Service period", f"{self.d(inv.period[0])} – {self.d(inv.period[1])}"))
        if inv.reference:
            rows.append(("Reference", inv.reference))
        y = y0
        for label, value in rows:
            self.text(c, self.width - self.margin_r - 70 * mm, y, label, color=GREY)
            self.text(c, self.width - self.margin_r, y, value, self.bold, align="r")
            y -= 4.6 * mm
        # amount due banner
        y = self.height - 92 * mm
        c.setFillColor(Color(0.96, 0.97, 0.98))
        c.roundRect(self.margin_l, y - 8 * mm, self.inner, 14 * mm, 2 * mm, stroke=0, fill=1)
        self.text(c, self.margin_l + 5 * mm, y - 1 * mm, f"{cash(inv.total, v.currency, 'en')} {v.currency} due "
                  f"{self.d(inv.due)}", self.bold, 13)
        self.text(c, self.margin_l + 5 * mm, y - 6 * mm, f"Pay online at pay.{v.web} or by wire transfer.", size=8,
                  color=GREY)

    def totals_rows(self):
        inv, cur = self.inv, self.v.currency
        return [("Subtotal", cash(inv.subtotal, cur, "en")), ("Tax (0%)", cash(inv.tax, cur, "en"))]

    def total_row(self):
        return f"Amount due ({self.v.currency})", cash(self.inv.total, self.v.currency, "en")

    def notes(self):
        v = self.v
        return [v.tax_note, f"Wire transfer: {v.bank} · Account/IBAN {v.iban} · SWIFT {v.bic}",
                f"Questions? Contact {v.email}"]

    def footer(self, c, page, pages):
        v = self.v
        self.text(c, self.margin_l, 12 * mm, f"{v.name} · {', '.join(v.address)} · {v.vat_id}", size=6.5,
                  color=GREY)
        self.text(c, self.width - self.margin_r, 12 * mm, f"{self.inv.number} · Page {page} of {pages}", size=6.5,
                  color=GREY, align="r")


class EuSaas(UsSaas):
    pagesize = A4
    columns = (("desc", "desc", None, "l"), ("qty", "qty", 18, "r"), ("unit", "unit", 16, "l"),
               ("price", "price", 24, "r"), ("amount", "amount", 26, "r"))
    first_table_top = 192 * mm
    cont_table_top = 252 * mm

    def first_header(self, c):
        v, inv = self.v, self.inv
        logo(c, v.logo, self.margin_l, self.height - 30 * mm, 13 * mm, self.accent)
        self.text(c, self.margin_l + 16 * mm, self.height - 21 * mm, v.name, self.bold, 14)
        self.text(c, self.margin_l + 16 * mm, self.height - 26.5 * mm, ", ".join(v.address), size=7.5, color=GREY)
        self.text(c, self.margin_l + 16 * mm, self.height - 30.5 * mm, f"VAT {v.vat_id} · {v.register or v.web}",
                  size=7.5, color=GREY)
        self.text(c, self.margin_l, self.height - 50 * mm, f"Invoice {inv.number}", self.bold, 18, color=self.accent)
        y0 = self.height - 62 * mm
        self.text(c, self.margin_l, y0, "Billed to", self.bold, 8, color=GREY)
        y = y0 - 5 * mm
        for part in (BUYER["name"], BUYER["attn"], *BUYER["address_en"], f"VAT {BUYER['vat_id']}"):
            self.text(c, self.margin_l, y, part, size=9)
            y -= 4.2 * mm
        rows = [("Date of issue", self.d(inv.date)), ("Date due", self.d(inv.due))]
        if v.customer_no:
            rows.append(("Customer ref.", v.customer_no))
        if inv.period:
            rows.append(("Period", f"{self.d(inv.period[0])} – {self.d(inv.period[1])}"))
        if inv.reference:
            rows.append(("Event", inv.reference))
        y = y0
        for label, value in rows:
            self.text(c, self.width - self.margin_r - 75 * mm, y, label, color=GREY)
            self.text(c, self.width - self.margin_r, y, value, align="r")
            y -= 4.6 * mm

    def totals_rows(self):
        inv = self.inv
        return [("Subtotal (excl. VAT)", cash(inv.subtotal, "EUR", "en")),
                ("VAT 0% – reverse charge", cash(inv.tax, "EUR", "en"))]

    def total_row(self):
        return "Total due (EUR)", cash(self.inv.total, "EUR", "en")

    def notes(self):
        v = self.v
        return [v.tax_note, f"Please pay within {v.terms_days} days to {v.bank}, IBAN {v.iban}, BIC {v.bic}.",
                f"Reference the invoice number on your payment. Questions: {v.email}"]


class UkLegal(Layout):
    font, bold = "Times-Roman", "Times-Bold"
    size = 9.5
    style = "en"
    labels: ClassVar[dict[str, str]] = {**EN, "desc": "Work done", "qty": "Hours", "price": "Rate", "amount": "Fee"}
    columns = (("desc", "desc", None, "l"), ("qty", "qty", 16, "r"), ("price", "price", 22, "r"),
               ("amount", "amount", 26, "r"))
    first_table_top = 182 * mm
    bottom = 30 * mm

    def first_header(self, c):
        v, inv = self.v, self.inv
        self.text(c, self.width / 2, self.height - 22 * mm, v.name.upper(), self.bold, 16, align="c")
        logo(c, v.logo, self.width / 2 - 5 * mm, self.height - 36 * mm, 10 * mm, self.accent)
        self.text(c, self.width / 2, self.height - 41 * mm, " · ".join(v.address), size=8, color=GREY, align="c")
        c.setStrokeColor(INK)
        c.setLineWidth(0.6)
        c.line(self.margin_l, self.height - 45 * mm, self.width - self.margin_r, self.height - 45 * mm)
        y = self.height - 56 * mm
        for part in ("Private & Confidential", BUYER["name"], "For the attention of the Finance Director",
                     *BUYER["address_en"]):
            self.text(c, self.margin_l, y, part, self.bold if part.startswith("Private") else self.font)
            y -= 4.6 * mm
        rows = [("Invoice No.", inv.number), ("Date", self.d(inv.date)), ("Our ref.", v.customer_no),
                ("Matter", inv.reference), ("VAT reg. no.", v.vat_id)]
        y = self.height - 56 * mm
        for label, value in rows:
            self.text(c, self.width - self.margin_r - 70 * mm, y, label, self.bold)
            self.text(c, self.width - self.margin_r, y, value, align="r")
            y -= 4.6 * mm
        self.text(c, self.margin_l, self.height - 98 * mm, "INVOICE FOR PROFESSIONAL SERVICES", self.bold, 12)
        self.text(c, self.margin_l, self.height - 105 * mm,
                  f"To our professional charges in connection with the above matter for the period ending "
                  f"{self.d(inv.date)}, as follows:")

    def table_header(self, c, y):
        x = self.margin_l
        for key, lab, w, a in self.cols:
            self.text(c, x + 2 if a == "l" else x + w - 2, y - self.size - 2, self.labels[lab], self.bold, align=a)
            x += w
        c.setStrokeColor(INK)
        c.setLineWidth(0.6)
        c.line(self.margin_l, y - self.size - 5, self.margin_l + self.inner, y - self.size - 5)
        return y - self.size - 9

    def totals_rows(self):
        inv = self.inv
        return [("Total professional fees and disbursements", cash(inv.subtotal, "GBP", "en")),
                ("VAT (outside the scope)", cash(inv.tax, "GBP", "en"))]

    def total_row(self):
        return "Total due", cash(self.inv.total, "GBP", "en")

    def notes(self):
        v = self.v
        return [v.tax_note, f"Payment is due within {v.terms_days} days of the date of this invoice.",
                f"Bank: {v.bank} · IBAN {v.iban} · SWIFT/BIC {v.bic} · Please quote {self.inv.number}."]

    def footer(self, c, page, pages):
        for i, part in enumerate(simpleSplit(self.v.register, self.font, 6.5, self.inner)):
            self.text(c, self.width / 2, 14 * mm - i * 3 * mm, part, size=6.5, color=GREY, align="c")
        self.text(c, self.width / 2, 7 * mm, f"Page {page} of {pages}", size=6.5, color=GREY, align="c")


# --- Swiss ------------------------------------------------------------------


class ChQr(DeClassic):
    style = "ch"
    labels: ClassVar[dict[str, str]] = {**DE, "price": "Preis CHF", "amount": "Betrag CHF"}
    columns = (("pos", "pos", 10, "l"), ("desc", "desc", None, "l"), ("qty", "qty", 14, "r"),
               ("unit", "unit", 12, "l"), ("price", "price", 24, "r"), ("amount", "amount", 26, "r"))
    last_reserve = 105 * mm
    bottom = 18 * mm
    first_table_top = 196 * mm

    def first_header(self, c):
        """DeClassic's letterhead, without the salutation — the payment part
        needs the bottom third of the last page."""
        v, inv = self.v, self.inv
        logo(c, v.logo, self.margin_l, self.height - 28 * mm, 12 * mm, self.accent)
        self.text(c, self.margin_l + 15 * mm, self.height - 20 * mm, v.name, self.bold, 13)
        self.text(c, self.margin_l + 15 * mm, self.height - 25 * mm, " · ".join(v.address), size=7.5, color=GREY)
        self.text(c, self.margin_l + 15 * mm, self.height - 29 * mm, f"{v.vat_id} · {v.email}", size=7.5,
                  color=GREY)
        y = self.height - 45 * mm
        for part in (BUYER["name"], BUYER["attn"]) + BUYER["address"]:
            self.text(c, self.margin_l, y, part, size=9.5)
            y -= 4.4 * mm
        rows = [("Rechnungsnummer", inv.number), ("Datum", self.d(inv.date)), ("Kundennummer", v.customer_no),
                ("Projekt", inv.reference)]
        y = self.height - 45 * mm
        for label, value in rows:
            self.text(c, self.width - self.margin_r - 70 * mm, y, label, color=GREY)
            self.text(c, self.width - self.margin_r, y, value, align="r")
            y -= 4.4 * mm
        self.text(c, self.margin_l, self.height - 78 * mm, f"Rechnung {inv.number}", self.bold, 14)

    def totals_rows(self):
        return [("Total exkl. MWST", f"CHF {num(self.inv.subtotal, 'ch')}"),
                ("MWST 0 %", f"CHF {num(self.inv.tax, 'ch')}")]

    def total_row(self):
        return "Rechnungsbetrag", f"CHF {num(self.inv.total, 'ch')}"

    def notes(self):
        due = f"Zahlbar innert {self.v.terms_days} Tagen ({self.d(self.inv.due)})"
        return [self.v.tax_note, f"{due} mit dem untenstehenden QR-Zahlteil oder per Überweisung.",
                "Besten Dank für Ihr Vertrauen."]

    def footer(self, c, page, pages):
        if page == 1:  # continuation pages carry it in their header
            self.text(c, self.width - self.margin_r, 108 * mm if pages == 1 else 8 * mm,
                      DE["page"].format(page, pages), size=6.5, color=GREY, align="r")

    def last_page(self, c):
        """The payment part of a Swiss QR-bill, drawn rather than encoded."""
        v, inv = self.v, self.inv
        top = 105 * mm
        c.setDash(2, 2)
        c.setStrokeColor(GREY)
        c.line(0, top, self.width, top)
        c.line(62 * mm, 0, 62 * mm, top)
        c.setDash()
        self.text(c, 5 * mm, top - 10 * mm, "Empfangsschein", self.bold, 11)
        self.text(c, 67 * mm, top - 10 * mm, "Zahlteil", self.bold, 11)
        for x0, w in ((5 * mm, 52 * mm), (118 * mm, 85 * mm)):
            y = top - 17 * mm
            for label, value in (("Konto / Zahlbar an", f"{v.iban}\n{v.name}\n{v.address[0]}\n{v.address[1]}"),
                                 ("Referenz", f"RF18 {inv.number.replace('-', '')}"),
                                 ("Zahlbar durch", "\n".join((BUYER["name"], BUYER["address"][0],
                                                              f"DE-{BUYER['address'][1]}")))):
                self.text(c, x0, y, label, self.bold, 6)
                y -= 3.4 * mm
                for part in value.split("\n"):
                    self.text(c, x0, y, part, size=8)
                    y -= 3.4 * mm
                y -= 2 * mm
        self.text(c, 5 * mm, 20 * mm, "Währung   Betrag", self.bold, 6)
        self.text(c, 5 * mm, 16 * mm, f"CHF          {num(inv.total, 'ch')}", size=8)
        self.text(c, 67 * mm, 20 * mm, "Währung   Betrag", self.bold, 6)
        self.text(c, 67 * mm, 16 * mm, f"CHF          {num(inv.total, 'ch')}", size=10)
        self.qr(c, 67 * mm, top - 63 * mm, 46 * mm)

    def qr(self, c, x, y, s):
        n = 29
        cell = s / n
        rng = random.Random(hashlib.sha256(self.inv.number.encode()).hexdigest())
        c.setFillColor(INK)
        for i in range(n):
            for j in range(n):
                finder = any(i - a in range(7) and j - b in range(7) for a, b in ((0, 0), (0, n - 7), (n - 7, 0)))
                if finder:
                    di, dj = next((i - a, j - b) for a, b in ((0, 0), (0, n - 7), (n - 7, 0))
                                  if i - a in range(7) and j - b in range(7))
                    on = di in (0, 6) or dj in (0, 6) or (2 <= di <= 4 and 2 <= dj <= 4)
                else:
                    on = rng.random() < 0.5
                if on:
                    c.rect(x + i * cell, y + j * cell, cell, cell, stroke=0, fill=1)
        # the Swiss cross
        c.setFillColor(white)
        c.rect(x + s / 2 - 4 * mm, y + s / 2 - 4 * mm, 8 * mm, 8 * mm, stroke=0, fill=1)
        c.setFillColor(INK)
        c.rect(x + s / 2 - 3.5 * mm, y + s / 2 - 3.5 * mm, 7 * mm, 7 * mm, stroke=0, fill=1)
        c.setFillColor(white)
        c.rect(x + s / 2 - 0.6 * mm, y + s / 2 - 2.3 * mm, 1.2 * mm, 4.6 * mm, stroke=0, fill=1)
        c.rect(x + s / 2 - 2.3 * mm, y + s / 2 - 0.6 * mm, 4.6 * mm, 1.2 * mm, stroke=0, fill=1)


# --- Portuguese hotel folio ---------------------------------------------------


class HotelFolio(Layout):
    style = "pt"
    labels: ClassVar[dict[str, str]] = {"invoice": "Fatura", "date": "Data", "desc": "Descrição / Description", "qty": "Qtd.",
              "price": "Preço €", "amount": "Valor €", "tax": "IVA", "page": "Página {} de {}", "pos": "#",
              "unit": "Un."}
    columns = (("date", "date", 22, "l"), ("desc", "desc", None, "l"), ("qty", "qty", 12, "r"),
               ("tax", "tax", 12, "r"), ("price", "price", 20, "r"), ("amount", "amount", 22, "r"))
    first_table_top = 192 * mm

    def first_header(self, c):
        v, inv = self.v, self.inv
        logo(c, v.logo, self.width / 2 - 8 * mm, self.height - 32 * mm, 16 * mm, self.accent)
        self.text(c, self.width / 2, self.height - 38 * mm, "ARCADIA LISBOA", self.bold, 14, color=self.accent,
                  align="c")
        self.text(c, self.width / 2, self.height - 43 * mm, f"{v.name} · {', '.join(v.address)}", size=7.5,
                  color=GREY, align="c")
        self.text(c, self.width / 2, self.height - 47 * mm, f"NIF {v.vat_id} · {v.phone} · {v.email}", size=7.5,
                  color=GREY, align="c")
        self.text(c, self.margin_l, self.height - 62 * mm, f"FATURA / INVOICE  {inv.number}", self.bold, 12)
        rows = [("Data / Date", self.d(inv.date)), ("Cliente / Guest", BUYER["name"]),
                ("NIF Cliente / VAT", BUYER["vat_id"]), ("Morada / Address", ", ".join(BUYER["address_en"]))]
        if inv.reference:
            rows.append(("Grupo / Group", inv.reference))
        y = self.height - 72 * mm
        for label, value in rows:
            self.text(c, self.margin_l, y, label, self.bold, color=GREY)
            self.text(c, self.margin_l + 38 * mm, y, value)
            y -= 4.6 * mm
        self.text(c, self.width - self.margin_r, self.height - 62 * mm, "ORIGINAL", self.bold, 9, color=GREY,
                  align="r")

    def totals_rows(self):
        inv = self.inv
        rows = [("Total ilíquido / Net", cash(inv.subtotal, "EUR", "pt"))]
        for rate, net, tax in inv.tax_groups():
            rows.append((f"IVA {rate:g}% s/ {num(net, 'pt')}", cash(tax, "EUR", "pt")))
        return rows

    def total_row(self):
        return "Total a pagar / Total", cash(self.inv.total, "EUR", "pt")

    def notes(self):
        return ["Pago / Paid: transferência bancária – " + self.v.iban,
                "Taxa turística municipal não sujeita a IVA (Regulamento Municipal de Lisboa).",
                "Processado por programa certificado n.º 0000/AT · Obrigado pela sua visita!"]

    def footer(self, c, page, pages):
        self.text(c, self.width / 2, 12 * mm, f"{self.v.register} · {self.v.web}", size=6.5, color=GREY, align="c")
        self.text(c, self.width - self.margin_r, 12 * mm, self.labels["page"].format(page, pages), size=6.5,
                  color=GREY, align="r")


LAYOUTS = {"de_classic": DeClassic, "de_modern": DeModern, "us_saas": UsSaas, "eu_saas": EuSaas,
           "uk_legal": UkLegal, "ch_qr": ChQr, "hotel_folio": HotelFolio}


def render(inv: Invoice, path: str) -> tuple[int, list[int]]:
    """Draw ``inv`` to ``path``. Returns (pages, page of each line item)."""
    return LAYOUTS[inv.vendor.layout](inv).render(path)


def date_on_invoice(inv: Invoice, date: dt.date) -> str:
    return date_fmt(inv.vendor)(date)
