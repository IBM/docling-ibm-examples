# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""The fictional world the invoices come from.

One buyer — Lumen Analytics GmbH, a ~40-person data company in Munich that
grows to ~55 over the first half of 2026 — and the seventeen suppliers it pays.
Every name, address, VAT id and IBAN here is invented; none of them is meant to
be, or to look like, a real company.

The point of modelling it rather than drawing random numbers is the dashboard.
Random vendors and amounts make a flat, unreadable chart. A company with a
cloud bill that grows month over month, a sales kickoff in January, a trade fair
in March, a hiring wave in spring and a financing round in May makes a chart
that tells you something — and lets you check, by eye, that the pipeline got it
right.

Nothing here touches a PDF. ``plan()`` returns invoices as data; the layouts in
``invoice_pdf.py`` draw them, and ``generate.py`` ties the two together.
"""

from __future__ import annotations

import calendar
import datetime as dt
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

D = Decimal
CENT = D("0.01")
ZERO = D(0)


def money(x: Decimal | float | str) -> Decimal:
    return D(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)


# --- the buyer ----------------------------------------------------------------

BUYER = {
    "name": "Lumen Analytics GmbH",
    "address": ("Leopoldstraße 112", "80802 München", "Deutschland"),
    "address_en": ("Leopoldstrasse 112", "80802 Munich", "Germany"),
    "vat_id": "DE318204771",
    "attn": "Accounts Payable",
    "email": "invoices@lumen-analytics.example",
}

DEPARTMENTS = ("Engineering", "Marketing", "Sales", "Operations", "People", "Finance")

CATEGORIES = (
    "Cloud & Software",
    "Hardware",
    "Travel",
    "Marketing & Events",
    "Office & Facilities",
    "Professional Services",
    "People & Training",
)

#: Headcount by month. Seats, payroll fees and onboarding hardware follow it.
HEADCOUNT = {11: 37, 12: 38, 1: 39, 2: 42, 3: 46, 4: 50, 5: 52, 6: 54}

#: Employees who travel, as they appear on a ticket.
TRAVELLERS = {
    "Sales": ("J. Weber", "A. Okafor", "L. Moreau", "D. Costa", "R. Haddad"),
    "Marketing": ("S. Lindqvist", "M. Rossi"),
    "Engineering": ("T. Nguyen", "K. Schmidt", "P. Kowalski"),
    "People": ("E. Fischer",),
    "Finance": ("H. Yilmaz",),
    "Operations": ("C. Brandl",),
}

# --- lines and invoices -------------------------------------------------------


@dataclass
class Line:
    description: str
    quantity: Decimal
    unit_price: Decimal
    unit: str = ""
    tax_rate: Decimal = ZERO
    #: Free-text second line under the description (a period, a reference).
    detail: str = ""

    @property
    def amount(self) -> Decimal:
        return money(self.quantity * self.unit_price)


@dataclass
class Invoice:
    vendor: Vendor
    number: str
    date: dt.date
    department: str
    category: str
    lines: list[Line]
    period: tuple[dt.date, dt.date] | None = None
    reference: str = ""  # a PO, a project, an event
    #: Hours, days after the invoice date that it is submitted for approval.
    lag_days: int = 3

    @property
    def due(self) -> dt.date:
        return self.date + dt.timedelta(days=self.vendor.terms_days)

    @property
    def subtotal(self) -> Decimal:
        return money(sum((line.amount for line in self.lines), D(0)))

    def tax_groups(self) -> list[tuple[Decimal, Decimal, Decimal]]:
        """``(rate, net, tax)`` per VAT rate, the way an invoice prints them."""
        groups: dict[Decimal, Decimal] = {}
        for line in self.lines:
            groups[line.tax_rate] = groups.get(line.tax_rate, D(0)) + line.amount
        return [(rate, money(net), money(net * rate / 100)) for rate, net in sorted(groups.items())]

    @property
    def tax(self) -> Decimal:
        return money(sum((t for _, _, t in self.tax_groups()), D(0)))

    @property
    def total(self) -> Decimal:
        return self.subtotal + self.tax


# --- vendors ------------------------------------------------------------------

Allocation = tuple[tuple[str, str, float], ...]
LineMaker = Callable[[random.Random, dt.date, "Vendor"], tuple[list[Line], str]]


@dataclass
class Vendor:
    key: str
    name: str
    address: tuple[str, ...]
    country: str
    vat_id: str | None
    currency: str
    layout: str
    accent: str
    logo: str
    bank: str
    iban: str
    bic: str
    terms_days: int
    number: Callable[[int, dt.date], str]
    seq_start: int
    allocation: Allocation
    lines: LineMaker
    tax_note: str = ""
    phone: str = ""
    email: str = ""
    web: str = ""
    register: str = ""
    customer_no: str = ""
    #: Share of this vendor's invoices that arrive as a scan rather than a PDF.
    scanned: float = 0.0
    lag: tuple[int, int] = (1, 8)
    seq_step: tuple[int, int] = (3, 40)
    _seq: int = field(default=0, init=False, repr=False)

    def next_number(self, rng: random.Random, date: dt.date) -> str:
        # Gaps between consecutive numbers: a vendor invoices other customers too.
        self._seq = (self._seq or self.seq_start) + rng.randint(*self.seq_step)
        return self.number(self._seq, date)


def _pick(rng: random.Random, allocation: Allocation) -> tuple[str, str]:
    dept, cat, _ = rng.choices(allocation, weights=[w for *_, w in allocation])[0]
    return dept, cat


def _month_span(date: dt.date) -> tuple[dt.date, dt.date]:
    first = date.replace(day=1)
    return first, first.replace(day=calendar.monthrange(first.year, first.month)[1])


def _prev_month(date: dt.date) -> tuple[dt.date, dt.date]:
    return _month_span(date.replace(day=1) - dt.timedelta(days=1))


def _months_since_dec(date: dt.date) -> int:
    return (date.year - 2025) * 12 + date.month - 12


def _de_month(date: dt.date) -> str:
    names = ("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September",
             "Oktober", "November", "Dezember")
    return f"{names[date.month - 1]} {date.year}"


def _q(x: float, places: str = "1") -> Decimal:
    return D(str(x)).quantize(D(places))


# Line makers. Each one returns the lines for one invoice and an optional
# reference string, given the invoice date. They are where the realism lives:
# the cloud bill is many small metered lines and spills onto a second page, the
# law firm bills narrative time entries, the travel agency mixes a VAT-free
# ticket with a taxed service fee.


def _nimbus(rng, date, v):
    growth = 1 + 0.045 * _months_since_dec(date)
    regions = (("eu-central-1", 1.0), ("eu-west-1", 0.35), ("us-east-1", 0.18))
    lines: list[Line] = []
    for region, share in regions:
        for kind, rate, base in (
            ("Compute – nc6.xlarge (on-demand)", "0.1920", 2900),
            ("Compute – nc6.2xlarge (reserved, 1yr)", "0.2410", 2160),
            ("Compute – nm6.4xlarge (memory optimized)", "0.9070", 610),
            ("Managed PostgreSQL – db.r6.large", "0.2600", 1440),
            ("Managed Kafka – broker hours", "0.2100", 2160),
            ("Object storage – standard tier (GB-month)", "0.0230", 41000),
            ("Block storage – gp3 (GB-month)", "0.0800", 9800),
            ("Data transfer out to internet (GB)", "0.0900", 5200),
            ("Load balancer – application (LB-hours)", "0.0225", 2190),
            ("NAT gateway – data processed (GB)", "0.0450", 3100),
        ):
            if region != "eu-central-1" and rng.random() < 0.45:
                continue
            qty = _q(base * share * growth * rng.uniform(0.85, 1.15))
            if qty <= 0:
                continue
            lines.append(Line(f"{kind}, {region}", qty, D(rate), "unit"))
    usage = sum((line.amount for line in lines), D(0))
    lines.append(Line("Business Support plan (10% of usage, min. $100)", D(1), money(max(usage * D("0.10"), D(100)))))
    lines.append(Line("Credits – Startup Program (applied)", D(1), money(-min(usage * D("0.04"), D(400)))))
    return lines, ""


def _stackwise(rng, date, v):
    seats = HEADCOUNT[date.month] - 14 + rng.randint(0, 2)
    lines = [
        Line("Stackwise CI – Team plan", D(seats), D("19.00"), "seat"),
        Line("Stackwise Observe – infrastructure hosts", D(14 + _months_since_dec(date)), D("23.00"), "host"),
        Line("Build minutes overage (per 1,000 min)", D(rng.randint(8, 30)), D("8.00"), "1k min"),
    ]
    if rng.random() < 0.3:
        lines.append(Line("Log retention add-on – 30 days", D(1), D("149.00")))
    return lines, ""


def _quillboard(rng, date, v):
    seats = HEADCOUNT[date.month] + rng.randint(1, 3)
    lines = [Line("Quillboard Business – annual commitment, billed monthly", D(seats), D("12.00"), "seat")]
    if date.month in (2, 4):
        lines.append(Line("Quillboard AI add-on (prorated)", D(seats // 2), D("6.50"), "seat"))
    return lines, ""


def _reachly(rng, date, v):
    boost = {1: 0.8, 2: 1.1, 3: 1.9, 4: 1.2, 5: 1.0, 6: 1.3, 12: 0.6}[date.month]
    lines = [
        Line("Search ads – DACH, data platform keywords", D(1), money(rng.uniform(2200, 3000) * boost)),
        Line("Sponsored content – DataWorld Expo campaign" if date.month in (2, 3) else
             "Sponsored content – thought leadership series", D(1), money(rng.uniform(1100, 2400) * boost)),
        Line("Retargeting – display, CPM", _q(rng.uniform(180, 420) * boost), D("4.80"), "1k impr."),
    ]
    return lines, "Campaign period " + _prev_month(date)[0].strftime("%B %Y")


_KONTOR_ITEMS = (
    ("Kopierpapier A4, 80 g/m², 5 × 500 Blatt", "24.90", "Karton", (2, 8)),
    ("Kaffeebohnen Crema, 1 kg", "17.50", "Stk.", (4, 12)),
    ("Haftnotizen 76 × 76 mm, 12er-Pack", "8.40", "Pack", (1, 5)),
    ("Ordner A4, 80 mm, blau", "2.95", "Stk.", (5, 30)),
    ("Whiteboard-Marker, 4 Farben", "6.80", "Set", (2, 10)),
    ("Toner schwarz, Laserdrucker LX-400", "89.00", "Stk.", (1, 2)),
    ("Hafermilch Barista, 1 l", "2.19", "Stk.", (12, 36)),
    ("Spülmaschinentabs, 100 Stk.", "14.90", "Pack", (1, 3)),
    ("Notizbücher A5, dotted", "7.90", "Stk.", (5, 20)),
    ("USB-C Ladekabel, 2 m", "12.90", "Stk.", (2, 8)),
    ("Desinfektionsmittel 500 ml", "5.60", "Stk.", (2, 6)),
)


def _kontor(rng, date, v):
    picks = rng.sample(_KONTOR_ITEMS, rng.randint(3, 7))
    lines = [Line(d, D(rng.randint(*r)), D(p), u, D(19)) for d, p, u, r in picks]
    if rng.random() < 0.6:
        lines.append(Line("Versandkostenpauschale", D(1), D("6.90"), "", D(19)))
    return lines, ""


def _brandt(rng, date, v):
    lines = [Line("Unterhaltsreinigung Büroflächen, 4. OG, 620 m²", D(1), D("1480.00"), "Monat", D(19),
                  _de_month(date))]
    if date.month in (3, 6):
        lines.append(Line("Glas- und Rahmenreinigung, innen/außen", D(1), D("690.00"), "pausch.", D(19)))
    if rng.random() < 0.25:
        lines.append(Line("Sonderreinigung nach Veranstaltung", D(rng.randint(3, 6)), D("42.00"), "Std.", D(19)))
    return lines, ""


def _spreeworks(rng, date, v):
    desks = 4 + (1 if date.month >= 3 else 0) + (1 if date.month >= 5 else 0)
    lines = [
        Line("Fixed Desk, Coworking Kreuzberg", D(desks), D("390.00"), "Platz", D(19), _de_month(date)),
        Line("Meetingraum „Spree“, stundenweise", D(rng.randint(4, 18)), D("35.00"), "Std.", D(19)),
    ]
    if rng.random() < 0.3:
        lines.append(Line("Postservice & Geschäftsadresse", D(1), D("49.00"), "Monat", D(19)))
    return lines, "Standort Berlin"


def _voltara(rng, date, v):
    hires = max(1, HEADCOUNT[date.month] - HEADCOUNT.get(date.month - 1, 38) + rng.randint(-1, 1))
    lines = [
        Line("Notebook 14\", 12-Kern, 32 GB RAM, 1 TB SSD", D(hires), D("2190.00"), "Stk.", D(19)),
        Line("Monitor 27\", 4K, USB-C, höhenverstellbar", D(hires + rng.randint(0, 2)), D("389.00"), "Stk.", D(19)),
        Line("Dockingstation USB-C, 100 W", D(hires), D("229.00"), "Stk.", D(19)),
    ]
    if rng.random() < 0.6:
        lines.append(Line("Headset, kabellos, ANC", D(hires), D("149.00"), "Stk.", D(19)))
    lines.append(Line("Vor-Ort-Service, 3 Jahre (Notebook)", D(hires), D("249.00"), "Stk.", D(19)))
    lines.append(Line("Einrichtung & Imaging, je Gerät", D(hires), D("45.00"), "Stk.", D(19)))
    return lines, f"Bestellung PO-{date:%y%m}-{rng.randint(10, 99)}"


_ROUTES = (("MUC", "LHR"), ("MUC", "AMS"), ("MUC", "CDG"), ("MUC", "BER"), ("MUC", "VIE"), ("MUC", "ZRH"),
           ("MUC", "LIS"), ("MUC", "CPH"), ("MUC", "MAD"))


def _meridian(rng, date, v, dept="Sales", route=None, travellers=1):
    lines: list[Line] = []
    names = rng.sample(TRAVELLERS[dept] * 3, travellers)
    route = route or rng.choice(_ROUTES)
    out = date + dt.timedelta(days=rng.randint(5, 20))
    back = out + dt.timedelta(days=rng.randint(1, 3))
    for name in names:
        lines.append(Line(f"Flug {route[0]}–{route[1]}–{route[0]}, Economy", D(1), money(rng.uniform(190, 640)),
                          "Ticket", D(0), f"{out:%d.%m.}–{back:%d.%m.%Y}, Passagier: {name}"))
        if rng.random() < 0.5:
            nights = (back - out).days
            lines.append(Line(f"Hotelvermittlung {route[1]}, {nights} Nacht/Nächte", D(nights),
                              money(rng.uniform(140, 260)), "Nacht", D(0), f"Gast: {name}"))
    lines.append(Line("Servicegebühr Buchung", D(len(names)), D("29.00"), "Vorgang", D(19)))
    return lines, ""


def _brightline(rng, date, v, phase="deposit"):
    stand = [
        Line("Booth space, hall 3, stand C14 (36 m²)", D(36), D("495.00"), "m²"),
        Line("Booth construction – modular, incl. graphics", D(1), D("11800.00")),
        Line("Lead retrieval licences", D(4), D("185.00"), "licence"),
        Line("Electricity & internet connection", D(1), D("1240.00")),
        Line("Booth catering package, 3 days", D(1), D("2150.00")),
    ]
    if phase == "deposit":
        total = sum((x.amount for x in stand), D(0))
        return [Line("Deposit 50% – DataWorld Expo Amsterdam 2026, stand C14", D(1), money(total / 2))], \
            "DataWorld Expo Amsterdam, 17–19 March 2026"
    if phase == "final":
        total = sum((x.amount for x in stand), D(0))
        return stand + [Line("Less deposit invoiced 15 January 2026", D(1), money(-total / 2))], \
            "DataWorld Expo Amsterdam, 17–19 March 2026"
    return [Line("Meetup sponsorship – Amsterdam Data Engineering, May 2026", D(1), D("3500.00")),
            Line("Venue & AV contribution", D(1), D("850.00"))], "Community sponsorship"


def _pixelhaus(rng, date, v):
    work = (
        ("Konzeption & Wireframes Landingpage", (8, 24)),
        ("Design Messestand DataWorld Expo, Grafiken", (12, 40)),
        ("Social-Media-Assets, Kampagne Q2", (6, 18)),
        ("Präsentationsvorlage, Corporate Design", (6, 14)),
        ("Illustrationen Whitepaper", (8, 20)),
        ("Projektmanagement & Abstimmung", (3, 8)),
    )
    picks = rng.sample(work[:5], rng.randint(1, 3)) + [work[5]]
    if date.month in (1, 2) and work[1] not in picks:
        picks.insert(0, work[1])
    return [Line(d, D(rng.randint(*r)), D("115.00"), "Std.", D(19)) for d, r in picks], ""


_KESTREL_TASKS = (
    "Review of customer master services agreement and data processing addendum; mark-up and call with client",
    "Drafting of employee share option plan rules and board resolutions",
    "Advice on UK subsidiary employment contracts; review of template offer letters",
    "Correspondence with investors' counsel regarding term sheet; issues list",
    "Due diligence questionnaire responses; coordination of data room",
    "Drafting of subscription and shareholders' agreement; internal review",
    "Attendance at completion call; preparation of closing deliverables",
    "Review of reseller agreement (Nordics); comments on liability caps",
    "Trade mark clearance search report for product name; advice",
)


def _kestrel(rng, date, v):
    heavy = date.month in (4, 5)
    n = rng.randint(5, 9) if heavy else rng.randint(2, 4)
    tasks = rng.sample(_KESTREL_TASKS[3:7] if heavy else _KESTREL_TASKS, min(n, 4 if heavy else 9))
    if heavy:
        tasks += rng.sample(_KESTREL_TASKS[:3] + _KESTREL_TASKS[7:], n - len(tasks))
    lines = []
    for task in tasks:
        partner = rng.random() < 0.4
        lines.append(Line(task, _q(rng.uniform(0.8, 7.5) * (1.6 if heavy else 1), "0.1"),
                          D("620.00") if partner else D("395.00"), "hrs", D(0),
                          "Partner (E. Finch)" if partner else "Senior Associate (R. Kestrel)"))
    if rng.random() < 0.5:
        lines.append(Line("Disbursement: Companies House filing fees", D(1), D("50.00"), "", D(0)))
    return lines, "Series B financing" if heavy else "General corporate matters"


def _hartmann(rng, date, v):
    hc = HEADCOUNT[_prev_month(date)[0].month]
    lines = [
        Line("Finanzbuchhaltung, laufende Buchungen", D(1), D("1150.00"), "pausch.", D(19),
             _de_month(_prev_month(date)[0])),
        Line("Lohn- und Gehaltsabrechnung", D(hc), D("28.00"), "MA", D(19)),
        Line("Umsatzsteuer-Voranmeldung", D(1), D("180.00"), "pausch.", D(19)),
    ]
    if date.month in (1, 4):
        lines.append(Line("Zusammenfassende Meldung, Quartal", D(1), D("120.00"), "pausch.", D(19)))
    if date.month == 3:
        lines.append(Line("Vorbereitung Jahresabschluss 2025, Abschlagszahlung", D(1), D("4800.00"), "", D(19)))
    return lines, ""


_ROLES = ("Senior Data Engineer", "Backend Engineer (Kotlin)", "Product Marketing Manager", "Account Executive DACH",
          "Site Reliability Engineer", "Data Scientist")


def _talentbridge(rng, date, v):
    role = rng.choice(_ROLES)
    salary = rng.choice((78000, 84000, 90000, 96000, 105000))
    start = (date + dt.timedelta(days=rng.randint(25, 60))).replace(day=1)
    return [Line(f"Vermittlungshonorar {role} (m/w/d)", D(1), money(salary * D("0.22")), "", D(19),
                 f"22 % des Jahreszielgehalts ({salary:,} EUR), Eintritt {start:%d.%m.%Y}".replace(",", "."))], \
        f"Suchauftrag {role}"


def _learnwell(rng, date, v):
    course, price, days = rng.choice((
        ("Kubernetes Security Workshop", "1950.00", 3),
        ("Leadership Essentials for New Managers", "2400.00", 2),
        ("Streaming Data Architectures", "1650.00", 2),
        ("Negotiation Skills for Sales Teams", "1450.00", 2),
    ))
    people = rng.randint(1, 4)
    lines = [Line(f"{course}, {days} Tage, Zürich", D(people), D(price), "TN", D(0))]
    if rng.random() < 0.4:
        lines.append(Line("Zertifizierungsprüfung", D(people), D("290.00"), "TN", D(0)))
    return lines, "Weiterbildungsbudget 2026"


def _gusto(rng, date, v, big=False):
    guests = rng.randint(80, 110) if big else rng.randint(12, 45)
    menu = rng.choice(("Mediterranes Fingerfood", "Bowl-Buffet vegetarisch", "Bayerisches Brotzeitbuffet",
                       "Pasta-Station mit Antipasti"))
    lines = [
        Line(f"{menu}, je Person", D(guests), money(rng.uniform(18, 29) if not big else 42), "Pers.", D(7)),
        Line("Getränkepauschale alkoholfrei", D(guests), D("6.50"), "Pers.", D(19)),
        Line("Lieferung & Abholung", D(1), D("45.00"), "", D(19)),
    ]
    if big or rng.random() < 0.3:
        lines.append(Line("Servicepersonal", D(rng.randint(2, 6) * (3 if big else 1)), D("38.00"), "Std.", D(19)))
    if big:
        lines.append(Line("Mietgeschirr & Stehtische", D(1), D("380.00"), "pausch.", D(19)))
    return lines, "Sommerfest 2026" if big else ""


def _arcadia(rng, date, v, group=False):
    lines: list[Line] = []
    nights = 3 if group else rng.randint(1, 3)
    rooms = 14 if group else 1
    arrive = date - dt.timedelta(days=nights)
    for i in range(nights):
        day = arrive + dt.timedelta(days=i)
        lines.append(Line(f"Accommodation – superior double, {rooms} room(s)", D(rooms), D("142.00"), "room-night",
                          D(6), f"{day:%Y-%m-%d}"))
        lines.append(Line("Breakfast buffet", D(rooms), D("18.00"), "pax", D(13), f"{day:%Y-%m-%d}"))
    lines.append(Line("Taxa turística municipal", D(rooms * nights), D("4.00"), "pax-night", D(0)))
    if group:
        lines.append(Line("Meeting room 'Tejo', full day incl. AV", D(2), D("1450.00"), "day", D(23)))
        lines.append(Line("Coffee breaks", D(28), D("14.50"), "pax", D(13)))
        lines.append(Line("Group dinner, menu 3 courses", D(15), D("58.00"), "pax", D(13)))
    return lines, "Sales Kickoff 2026, 20–22 Jan" if group else ""


# The vendors. Accent colours and logo shapes only make the PDFs look like they
# come from seventeen different places — which matters, because one layout
# seventeen times is a template-matching demo, not an extraction demo.

VENDORS: dict[str, Vendor] = {v.key: v for v in (
    Vendor("nimbus", "Nimbus Cloud Services, Inc.",
           ("1200 Westlake Ave N, Suite 800", "Seattle, WA 98109", "United States"), "US",
           "EIN 84-3729105", "USD", "us_saas", "#ff8a00", "cloud",
           "Cascade National Bank", "US64 CNBA 0000 4417 2093 51", "CNBAUS6S", 30,
           lambda s, d: f"NCS-{d.year}-{s:06d}", 118200,
           (("Engineering", "Cloud & Software", 1.0),), _nimbus,
           tax_note="Reverse charge: VAT to be accounted for by the recipient.",
           email="billing@nimbus-cloud.example", web="nimbus-cloud.example", customer_no="ACCT 3348-1173-9902",
           lag=(1, 4), seq_step=(800, 2400)),
    Vendor("stackwise", "Stackwise Software Ltd",
           ("Unit 4, Grand Canal Quay", "Dublin 2, D02 X576", "Ireland"), "IE",
           "IE3456789TH", "EUR", "eu_saas", "#3b5bdb", "stack",
           "Bank of Liffey", "IE29 LIFF 9311 5212 3456 78", "LIFFIE2D", 14,
           lambda s, d: f"SW-{s:05d}", 40210,
           (("Engineering", "Cloud & Software", 1.0),), _stackwise,
           tax_note="VAT reverse charge – Article 196 Council Directive 2006/112/EC.",
           email="accounts@stackwise.example", web="stackwise.example", customer_no="CUS-88412", lag=(1, 6),
           seq_step=(90, 300)),
    Vendor("quillboard", "Quillboard, Inc.",
           ("548 Market St, PMB 72115", "San Francisco, CA 94104", "United States"), "US",
           "EIN 47-1182930", "USD", "us_saas", "#12b886", "quill",
           "Golden Gate Commerce Bank", "US12 GGCB 0000 9182 7733 10", "GGCBUS6L", 30,
           lambda s, d: f"QB-{d:%Y%m}-{s:04d}", 2100,
           (("Operations", "Cloud & Software", 1.0),), _quillboard,
           tax_note="Reverse charge: VAT to be accounted for by the recipient.",
           email="ar@quillboard.example", web="quillboard.example", customer_no="Workspace lumen-analytics",
           lag=(2, 9)),
    Vendor("reachly", "Reachly Media, Inc.",
           ("75 Varick Street, 9th Floor", "New York, NY 10013", "United States"), "US",
           "EIN 82-4410937", "USD", "us_saas", "#e64980", "dots",
           "Hudson Trust Company", "US88 HUDT 0000 1204 5560 07", "HUDTUS33", 15,
           lambda s, d: f"RM-{s:06d}", 310400,
           (("Marketing", "Marketing & Events", 1.0),), _reachly,
           tax_note="Reverse charge: VAT to be accounted for by the recipient.",
           email="billing@reachly.example", web="reachly.example", customer_no="Advertiser 220417", lag=(2, 10),
           seq_step=(400, 1500)),
    Vendor("kontor", "Kontor Bürobedarf GmbH",
           ("Landsberger Straße 291", "80687 München"), "DE",
           "DE287144532", "EUR", "de_classic", "#1c3d6e", "square",
           "Stadtsparkasse Isartal", "DE44 7015 0000 0912 4471 85", "SSKIDEMMXXX", 14,
           lambda s, d: f"RE{d.year}-{s:05d}", 10340,
           (("Operations", "Office & Facilities", 1.0),), _kontor,
           phone="+49 89 5480 220", email="buchhaltung@kontor-buero.example", web="kontor-buero.example",
           register="Amtsgericht München HRB 182044 · Geschäftsführer: Peter Kranz", customer_no="K-20417",
           scanned=0.35, lag=(1, 12)),
    Vendor("brandt", "Brandt Gebäudeservice GmbH",
           ("Truderinger Straße 40", "81677 München"), "DE",
           "DE301874422", "EUR", "de_classic", "#2f9e44", "leaf",
           "Isar Volksbank eG", "DE81 7019 0000 0004 4182 63", "GENODEF1M01", 14,
           lambda s, d: f"{d.year}/{s:04d}", 210,
           (("Operations", "Office & Facilities", 1.0),), _brandt,
           phone="+49 89 4316 007", email="rechnung@brandt-service.example", web="brandt-service.example",
           register="Amtsgericht München HRB 201557 · Geschäftsführerin: Anja Brandt", customer_no="100-3318",
           lag=(2, 9), seq_step=(20, 60)),
    Vendor("spreeworks", "Spreeworks Coworking GmbH",
           ("Schlesische Straße 26", "10997 Berlin"), "DE",
           "DE329018456", "EUR", "de_modern", "#7048e8", "wave",
           "Nordstadt Bank AG", "DE12 1007 0000 0388 4410 02", "NORDDEBBXXX", 7,
           lambda s, d: f"SPW-{d.year}-{s:04d}", 610,
           (("Sales", "Office & Facilities", 1.0),), _spreeworks,
           phone="+49 30 2201 4410", email="billing@spreeworks.example", web="spreeworks.example",
           register="Amtsgericht Charlottenburg HRB 214433 B", customer_no="M-0581", lag=(1, 7)),
    Vendor("voltara", "Voltara Systemhaus GmbH",
           ("Hanauer Landstraße 182", "60314 Frankfurt am Main"), "DE",
           "DE266109384", "EUR", "de_modern", "#f03e3e", "bolt",
           "Mainufer Bank AG", "DE07 5004 0000 0661 2903 44", "MAINDEFFXXX", 30,
           lambda s, d: f"VR-{s:07d}", 2271900,
           (("Engineering", "Hardware", 0.6), ("Operations", "Hardware", 0.4)), _voltara,
           phone="+49 69 9433 870", email="faktura@voltara.example", web="voltara.example",
           register="Amtsgericht Frankfurt am Main HRB 99812", customer_no="KD 774120", lag=(2, 10),
           seq_step=(300, 1200)),
    Vendor("meridian", "Meridian Travel Management GmbH",
           ("Arnulfstraße 59", "80636 München"), "DE",
           "DE279104455", "EUR", "de_classic", "#0b7285", "compass",
           "Stadtsparkasse Isartal", "DE65 7015 0000 0901 7720 31", "SSKIDEMMXXX", 10,
           lambda s, d: f"MTM-{d.year}-{s:05d}", 30100,
           (("Sales", "Travel", 0.55), ("Marketing", "Travel", 0.15), ("Engineering", "Travel", 0.15),
            ("People", "Travel", 0.05), ("Finance", "Travel", 0.05), ("Operations", "Travel", 0.05)),
           _meridian, phone="+49 89 1890 4400", email="invoice@meridian-travel.example",
           web="meridian-travel.example", register="Amtsgericht München HRB 166921", customer_no="FK-3390",
           scanned=0.12, lag=(3, 21), seq_step=(40, 160)),
    Vendor("arcadia", "Arcadia Hotels Lisboa, S.A.",
           ("Avenida da Liberdade, 214", "1250-147 Lisboa", "Portugal"), "PT",
           "PT509284117", "EUR", "hotel_folio", "#9c6644", "arch",
           "Banco Atlântico Central", "PT50 0035 0412 0001 7731 2905 4", "BACTPTPL", 0,
           lambda s, d: f"FT A{d.year}/{s:05d}", 8800,
           (("Sales", "Travel", 1.0),), _arcadia,
           phone="+351 21 340 8800", email="frontoffice@arcadia-lisboa.example", web="arcadia-lisboa.example",
           register="Capital social 2.500.000 € · C.R.C. Lisboa", lag=(4, 14)),
    Vendor("brightline", "Brightline Events B.V.",
           ("Danzigerkade 23C", "1013 AP Amsterdam", "The Netherlands"), "NL",
           "NL859273641B01", "EUR", "eu_saas", "#fab005", "spark",
           "Grachten Bank N.V.", "NL91 GRAC 0417 1643 00", "GRACNL2A", 21,
           lambda s, d: f"BL{d.year}{s:04d}", 20,
           (("Marketing", "Marketing & Events", 1.0),), _brightline,
           tax_note="VAT reverse charge – Article 196 Council Directive 2006/112/EC.",
           email="finance@brightline-events.example", web="brightline-events.example",
           register="KvK 81234567", customer_no="LUM-2026", lag=(2, 8), seq_step=(5, 25)),
    Vendor("pixelhaus", "Pixelhaus Kreativagentur GmbH",
           ("Schanzenstraße 75", "20357 Hamburg"), "DE",
           "DE314776102", "EUR", "de_modern", "#d6336c", "pixel",
           "Hanseatische Privatbank", "DE89 2003 0000 0012 4419 77", "HANSDEHHXXX", 14,
           lambda s, d: f"PH-{d.year}-{s:03d}", 15,
           (("Marketing", "Professional Services", 1.0),), _pixelhaus,
           phone="+49 40 3290 1180", email="hallo@pixelhaus.example", web="pixelhaus.example",
           register="Amtsgericht Hamburg HRB 155092", customer_no="LUMEN", lag=(2, 12), seq_step=(2, 9)),
    Vendor("kestrel", "Kestrel & Finch LLP",
           ("14 Bream's Buildings", "London EC4A 1HP", "United Kingdom"), "GB",
           "GB 274 1934 82", "GBP", "uk_legal", "#343a40", "kestrel",
           "Thameside Bank plc", "GB33 THMS 2004 1531 2208 77", "THMSGB2L", 30,
           lambda s, d: f"KF/{d.year}/{s:04d}", 300,
           (("Finance", "Professional Services", 1.0),), _kestrel,
           tax_note="Outside the scope of UK VAT – place of supply is the customer's country (B2B).",
           phone="+44 20 7946 0331", email="accounts@kestrelfinch.example", web="kestrelfinch.example",
           register="A limited liability partnership registered in England and Wales, no. OC412233. "
                    "Authorised and regulated by the Solicitors Regulation Authority.",
           customer_no="Client LUM001", lag=(3, 14), seq_step=(30, 90)),
    Vendor("hartmann", "Hartmann & Partner Steuerberatungsgesellschaft mbB",
           ("Prinzregentenstraße 61", "81675 München"), "DE",
           "DE811203987", "EUR", "de_classic", "#495057", "columns",
           "Isar Volksbank eG", "DE29 7019 0000 0001 2290 44", "GENODEF1M01", 14,
           lambda s, d: f"{d.year}-{s:04d}", 90,
           (("Finance", "Professional Services", 1.0),), _hartmann,
           phone="+49 89 4111 920", email="kanzlei@hartmann-partner.example", web="hartmann-partner.example",
           register="Partnerschaftsregister AG München PR 2210", customer_no="Mandant 10417", lag=(2, 10),
           seq_step=(40, 110)),
    Vendor("talentbridge", "Talentbridge Recruiting GmbH",
           ("Torstraße 140", "10119 Berlin"), "DE",
           "DE326550190", "EUR", "de_modern", "#1098ad", "bridge",
           "Nordstadt Bank AG", "DE58 1007 0000 0412 6630 18", "NORDDEBBXXX", 14,
           lambda s, d: f"TB-{d.year}-{s:03d}", 40,
           (("People", "People & Training", 1.0),), _talentbridge,
           phone="+49 30 4081 7720", email="rechnung@talentbridge.example", web="talentbridge.example",
           register="Amtsgericht Charlottenburg HRB 229810 B", customer_no="Kunde 2031", lag=(2, 9),
           seq_step=(4, 15)),
    Vendor("learnwell", "Learnwell Academy AG",
           ("Hardturmstrasse 161", "8005 Zürich", "Schweiz"), "CH",
           "CHE-412.889.301 MWST", "CHF", "ch_qr", "#5f3dc4", "book",
           "Zürcher Gewerbebank", "CH56 0483 5012 3456 7800 9", "ZGBKCHZZ", 30,
           lambda s, d: f"LW-{d.year}-{s:05d}", 1200,
           (("Engineering", "People & Training", 0.5), ("People", "People & Training", 0.3),
            ("Sales", "People & Training", 0.2)), _learnwell,
           tax_note="Leistung im Ausland – nicht der schweizerischen MWST unterstellt (Art. 8 MWSTG).",
           phone="+41 44 512 33 00", email="admin@learnwell.example", web="learnwell.example",
           register="CHE-412.889.301 · Handelsregister des Kantons Zürich", customer_no="Kunde 7781",
           lag=(2, 12), seq_step=(20, 80)),
    Vendor("gusto", "Gusto Catering München GmbH",
           ("Tumblingerstraße 32", "80337 München"), "DE",
           "DE299410233", "EUR", "de_classic", "#e8590c", "fork",
           "Stadtsparkasse Isartal", "DE30 7015 0000 0907 3318 20", "SSKIDEMMXXX", 10,
           lambda s, d: f"G-{s:05d}", 7700,
           (("People", "People & Training", 0.7), ("Marketing", "Marketing & Events", 0.2),
            ("Sales", "Marketing & Events", 0.1)), _gusto,
           phone="+49 89 7677 4100", email="events@gusto-catering.example", web="gusto-catering.example",
           register="Amtsgericht München HRB 190334", customer_no="", scanned=0.35, lag=(1, 10),
           seq_step=(10, 60)),
)}

# --- the plan ---------------------------------------------------------------

SIM_START = dt.date(2026, 1, 5)
SIM_END = dt.date(2026, 6, 30)

#: First business day of each month the recurring vendors bill.
MONTHS = [dt.date(2025, 12, 1)] + [dt.date(2026, m, 1) for m in range(1, 7)]


def business_day(date: dt.date, forward: bool = True) -> dt.date:
    step = dt.timedelta(days=1 if forward else -1)
    while date.weekday() >= 5:
        date += step
    return date


def _random_day(rng: random.Random, month: dt.date) -> dt.date:
    first, last = _month_span(month)
    return business_day(first + dt.timedelta(days=rng.randint(0, (last - first).days)), forward=False)


def plan(seed: int = 2026) -> list[Invoice]:
    """Every invoice Lumen receives between December 2025 and June 2026."""
    rng = random.Random(seed)
    for vendor in VENDORS.values():
        vendor._seq = 0
    out: list[Invoice] = []

    def add(vendor_key: str, date: dt.date, lines_ref=None, dept_cat=None, **extra) -> None:
        vendor = VENDORS[vendor_key]
        date = business_day(date)
        lines, ref = lines_ref if lines_ref is not None else vendor.lines(rng, date, vendor)
        dept, cat = dept_cat or _pick(rng, vendor.allocation)
        out.append(Invoice(vendor, vendor.next_number(rng, date), date, dept, cat, lines,
                           reference=ref, lag_days=rng.randint(*vendor.lag), **extra))

    # Recurring, monthly.
    for month in MONTHS:
        add("nimbus", month + dt.timedelta(days=1), period=_prev_month(month))
        add("stackwise", month, period=_month_span(month))
        add("quillboard", month + dt.timedelta(days=4), period=_month_span(month))
        add("reachly", month + dt.timedelta(days=2), period=_prev_month(month))
        add("spreeworks", month, period=_month_span(month))
        add("brandt", _month_span(month)[1] - dt.timedelta(days=1), period=_month_span(month))
        add("hartmann", month + dt.timedelta(days=rng.randint(6, 10)), period=_prev_month(month))

    # Ad hoc, with a monthly rate and a seasonal multiplier.
    adhoc = {
        "kontor": (3, {}),
        "meridian": (6, {1: 1.3, 3: 1.4, 5: 1.1}),
        "voltara": (1.2, {2: 2.0, 3: 2.5, 4: 2.0, 12: 0.5}),
        "pixelhaus": (1, {2: 2.0, 1: 1.5}),
        "gusto": (1.5, {12: 2.0}),
        "kestrel": (0.6, {4: 2.5, 5: 3.0}),
        "talentbridge": (0.5, {2: 2.0, 3: 2.5, 4: 2.0}),
        "learnwell": (0.8, {3: 1.5, 5: 1.5}),
    }
    for vendor_key, (rate, season) in adhoc.items():
        for month in MONTHS:
            expected = rate * season.get(month.month, 1.0)
            n = int(expected) + (1 if rng.random() < expected - int(expected) else 0)
            for _ in range(n):
                add(vendor_key, _random_day(rng, month))

    # The story. Each of these is what makes a bar on the dashboard stand out.
    v = VENDORS
    # Sales kickoff in Lisbon, 20–22 January: flights for the whole sales team
    # and some of everyone else, then one large hotel group invoice.
    for dept, n in (("Sales", 5), ("Marketing", 1), ("Engineering", 2), ("People", 1), ("Finance", 1)):
        add("meridian", dt.date(2026, 1, rng.randint(5, 12)),
            _meridian(rng, dt.date(2026, 1, 5), v["meridian"], dept, ("MUC", "LIS"), min(n, len(TRAVELLERS[dept]))),
            (dept, "Travel"))
    add("arcadia", dt.date(2026, 1, 22), _arcadia(rng, dt.date(2026, 1, 23), v["arcadia"], group=True),
        ("Sales", "Travel"))
    add("arcadia", dt.date(2026, 1, 23), dept_cat=("Sales", "Travel"))
    # DataWorld Expo Amsterdam, 17–19 March: deposit, final invoice, travel.
    add("brightline", dt.date(2026, 1, 15), _brightline(rng, dt.date(2026, 1, 15), v["brightline"], "deposit"))
    add("brightline", dt.date(2026, 3, 23), _brightline(rng, dt.date(2026, 3, 23), v["brightline"], "final"))
    add("brightline", dt.date(2026, 5, 4), _brightline(rng, dt.date(2026, 5, 4), v["brightline"], "meetup"))
    for dept in ("Marketing", "Sales", "Sales"):
        add("meridian", dt.date(2026, 3, rng.randint(2, 6)),
            _meridian(rng, dt.date(2026, 3, 2), v["meridian"], dept, ("MUC", "AMS"), 2), (dept, "Travel"))
    # Summer party in June.
    add("gusto", dt.date(2026, 6, 19), _gusto(rng, dt.date(2026, 6, 19), v["gusto"], big=True),
        ("People", "People & Training"))

    out.sort(key=lambda inv: (inv.date, inv.vendor.key, inv.number))
    return out
