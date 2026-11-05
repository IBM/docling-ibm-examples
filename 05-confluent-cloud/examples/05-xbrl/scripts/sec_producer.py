#!/usr/bin/env python3
# /// script
# dependencies = [
#     "requests",
#     "confluent-kafka",
# ]
# ///
"""
SEC EDGAR XBRL Kafka Producer.

This script:
1. Listens/polls the SEC's current filings Atom feed to discover newly available reports (Form 10-K or 10-Q).
2. Queries the filing directory index to resolve the inline XBRL (iXBRL) report `.htm` URL (the document Docling
   for watsonx converts, as HTML), the XBRL instance XML file URL and the custom taxonomy files URLs
   (.xsd schema, _cal.xml calculation, _def.xml definition, _lab.xml label, _pre.xml presentation linkbases).
3. Gathers all filing metadata and resource links.
4. Produces the metadata record onto a Confluent Cloud Kafka topic or writes to a local queue folder.

Modes:
- one-shot (default): produce the most recent `--limit` filings, then exit.
- `--replay [DIR]`: no SEC access; produce the saved records in `data/queue/` (or DIR), in a fixed order.
- `--watch`: poll the SEC feed every `--interval` seconds and produce each new filing once, until Ctrl-C.

This producer does not download files or run Docling conversions.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

# Setup import paths to resolve common modules from repo root
_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent.parent
sys.path.insert(0, str(_REPO / "common"))
sys.path.insert(0, str(_HERE))
# Also fallback to examples/04-invoice-extraction/scripts if needed
sys.path.insert(0, str(_REPO / "examples" / "04-invoice-extraction" / "scripts"))

try:
    from confluent_kafka import Producer
    from sr import SchemaRegistry

    from ccenv import kafka_config, load_env
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

# SEC Rate limits and compliance
SEC_SLEEP_INTERVAL = 0.15
DEFAULT_USER_AGENT = "DoclingConfluentCloudScraper/1.0 (researcher@example.com; academic research scraper)"
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
# Exhibits are named like "ex21.htm", "a2025exhibit211.htm", "abbv-20251231xex311.htm".
EXHIBIT_NAME = re.compile(r"exhibit|ex[-_]?\d", re.IGNORECASE)


def find_ixbrl_document(
    items: list[dict[str, Any]], instance_file: str | None
) -> tuple[str | None, int | None]:
    """
    Locate the inline XBRL (iXBRL) report: the human-readable .htm document that carries the XBRL tags.

    SEC names the XBRL instance extracted from it `<name>_htm.xml`, so the report is `<name>.htm`.
    When that name does not match, fall back to the largest .htm that is not an exhibit.

    Returns:
        (ixbrl_file_name, ixbrl_size)
    """
    htm_items = [it for it in items if it.get("name", "").lower().endswith((".htm", ".html"))]

    def size_of(it: dict[str, Any]) -> int | None:
        return int(it["size"]) if it.get("size") and str(it["size"]).isdigit() else None

    if instance_file and instance_file.lower().endswith("_htm.xml"):
        wanted = (instance_file[: -len("_htm.xml")] + ".htm").lower()
        for it in htm_items:
            if it["name"].lower() == wanted:
                return it["name"], size_of(it)

    candidates = [it for it in htm_items if not EXHIBIT_NAME.search(it["name"])]
    if not candidates:
        return None, None
    best = max(candidates, key=lambda it: size_of(it) or 0)
    return best["name"], size_of(best)


def resolve_filing_xbrl_resources(
    cik_short: str, acc_nodash: str, user_agent: str
) -> tuple[
    str | None, str | None, int | None, str | None, str | None, int | None, list[dict[str, Any]], list[str]
]:
    """
    Query index.json of the SEC EDGAR filing directory to locate:
    1. The inline XBRL (iXBRL) report (.htm): the document Docling for watsonx converts, as HTML.
    2. The primary XBRL instance file (*_htm.xml or primary .xml).
    3. The custom taxonomy files (.xsd, _cal.xml, _def.xml, _lab.xml, _pre.xml, _ref.xml).

    Returns:
        (instance_url, instance_file_name, instance_size, ixbrl_url, ixbrl_file_name, ixbrl_size,
         taxonomy_files, taxonomy_urls)
    """
    idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/index.json"
    headers = {"User-Agent": user_agent}
    no_resources = (None, None, None, None, None, None, [], [])

    try:
        resp = requests.get(idx_url, headers=headers, timeout=30)
        time.sleep(SEC_SLEEP_INTERVAL)
        if resp.status_code != 200:
            return no_resources
        data = resp.json()
        items = data.get("directory", {}).get("item", [])
    except Exception as e:  # noqa: BLE001
        print(f"  [!] Warning: Error querying filing index.json: {e}", file=sys.stderr)
        return no_resources

    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}"
    instance_file: str | None = None
    instance_size: int | None = None
    taxonomy_files: list[dict[str, Any]] = []

    # 1. Identify XBRL instance XML file
    # Top priority: *_htm.xml (the standard SEC instance XML name)
    htm_xml_candidates = [it for it in items if it.get("name", "").lower().endswith("_htm.xml")]
    if htm_xml_candidates:
        inst_item = htm_xml_candidates[0]
        instance_file = inst_item.get("name")
        if inst_item.get("size") and str(inst_item["size"]).isdigit():
            instance_size = int(inst_item["size"])
    else:
        # Fallback: any .xml file that is NOT a taxonomy linkbase, filing summary, or metalinks
        excluded_suffixes = (
            "_cal.xml", "_def.xml", "_lab.xml", "_pre.xml", "_ref.xml",
            "filingsummary.xml", "metalinks.xml"
        )
        xml_candidates = [
            it for it in items
            if it.get("name", "").lower().endswith(".xml")
            and not any(it.get("name", "").lower().endswith(s) for s in excluded_suffixes)
        ]
        if xml_candidates:
            inst_item = xml_candidates[0]
            instance_file = inst_item.get("name")
            if inst_item.get("size") and str(inst_item["size"]).isdigit():
                instance_size = int(inst_item["size"])

    instance_url = f"{base_url}/{instance_file}" if instance_file else None

    ixbrl_file, ixbrl_size = find_ixbrl_document(items, instance_file)
    ixbrl_url = f"{base_url}/{ixbrl_file}" if ixbrl_file else None

    # 2. Identify taxonomy files
    for it in items:
        name = it.get("name", "")
        name_lower = name.lower()
        tax_type: str | None = None

        if name_lower.endswith(".xsd"):
            tax_type = "schema"
        elif name_lower.endswith("_cal.xml"):
            tax_type = "calculation"
        elif name_lower.endswith("_def.xml"):
            tax_type = "definition"
        elif name_lower.endswith("_lab.xml"):
            tax_type = "label"
        elif name_lower.endswith("_pre.xml"):
            tax_type = "presentation"
        elif name_lower.endswith("_ref.xml"):
            tax_type = "reference"

        if tax_type:
            size_val = int(it["size"]) if it.get("size") and str(it["size"]).isdigit() else None
            taxonomy_files.append({
                "name": name,
                "type": tax_type,
                "url": f"{base_url}/{name}",
                "size": size_val,
            })

    # Sort taxonomy files consistently: schema first, then calculation, definition, label, presentation, reference
    type_order = {
        "schema": 0,
        "calculation": 1,
        "definition": 2,
        "label": 3,
        "presentation": 4,
        "reference": 5,
    }
    taxonomy_files.sort(key=lambda x: (type_order.get(x["type"], 99), x["name"]))
    taxonomy_urls = [tf["url"] for tf in taxonomy_files]

    return (
        instance_url, instance_file, instance_size, ixbrl_url, ixbrl_file, ixbrl_size,
        taxonomy_files, taxonomy_urls,
    )


def parse_raw_feed_entries(form_type: str, count: int, user_agent: str) -> list[dict[str, Any]]:
    """
    Query the SEC Atom feed for a specific form type and parse feed-level metadata.
    """
    type_param = "" if form_type.lower() == "all" else form_type.upper()
    fetch_count = max(count, 10) if count > 0 else 100
    url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcurrent&type={type_param}&count={fetch_count}&output=atom"
    )
    headers = {"User-Agent": user_agent}

    print(f"[*] Querying SEC Atom Feed for recent {form_type} reports...")
    resp = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            time.sleep(SEC_SLEEP_INTERVAL)
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503):
                time.sleep(0.5 * attempt)
                continue
            resp.raise_for_status()
        except requests.RequestException as e:
            if attempt == 3:
                print(f"[!] Error querying SEC Atom Feed: {e}", file=sys.stderr)
                return []
            time.sleep(0.5 * attempt)

    if resp is None or resp.status_code != 200:
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        print(f"[!] Error parsing XML feed: {e}", file=sys.stderr)
        return []

    entries: list[dict[str, Any]] = []

    for entry in root.findall("atom:entry", ATOM_NS):
        title_elem = entry.find("atom:title", ATOM_NS)
        title_text = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

        updated_elem = entry.find("atom:updated", ATOM_NS)
        updated_text = updated_elem.text.strip() if updated_elem is not None and updated_elem.text else ""

        link_elem = entry.find("atom:link", ATOM_NS)
        link_url = link_elem.attrib.get("href", "").strip() if link_elem is not None else ""

        summary_elem = entry.find("atom:summary", ATOM_NS)
        summary_raw = summary_elem.text.strip() if summary_elem is not None and summary_elem.text else ""
        summary_clean = re.sub(r"<[^>]+>", " ", summary_raw)

        cat_elem = entry.find("atom:category", ATOM_NS)
        category_term = cat_elem.attrib.get("term", "").strip() if cat_elem is not None else ""

        # Extract CIK (10 digits)
        cik_match = re.search(r"\((\d{10})\)", title_text)
        cik = cik_match.group(1) if cik_match else ""
        if not cik and "/data/" in link_url:
            parts = link_url.split("/data/")
            if len(parts) > 1:
                raw_cik = parts[1].split("/")[0]
                if raw_cik.isdigit():
                    cik = f"{int(raw_cik):010d}"

        if not cik:
            continue

        cik_short = str(int(cik))

        # Extract Accession Number
        acc_match = re.search(r"AccNo:\s*([0-9\-]+)", summary_clean)
        acc_no = acc_match.group(1) if acc_match else ""
        if not acc_no and "/data/" in link_url:
            parts = link_url.split("/data/")
            if len(parts) > 1:
                sub_parts = parts[1].split("/")
                if len(sub_parts) > 1:
                    raw_acc = sub_parts[1]
                    if len(raw_acc) == 18 and "-" not in raw_acc:
                        acc_no = f"{raw_acc[:10]}-{raw_acc[10:12]}-{raw_acc[12:]}"
                    else:
                        acc_no = raw_acc

        if not acc_no:
            continue

        acc_nodash = acc_no.replace("-", "")

        # Extract Company Name
        if " - " in title_text:
            company_part = title_text.split(" - ", 1)[1]
        else:
            company_part = title_text
        company_name = re.sub(r"\s*\(\d{10}\).*", "", company_part).strip()

        # Extract Filing Date
        date_match = re.search(r"Filed:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", summary_clean)
        filing_date = date_match.group(1) if date_match else (updated_text[:10] if updated_text else "")

        # Extract File Size
        size_match = re.search(r"Size:\s*(\S+\s+\S+)", summary_clean)
        file_size = size_match.group(1).strip() if size_match else ""

        # Form type (e.g. 10-K, 10-Q, 10-K/A, 10-Q/A)
        form = category_term or (title_text.split(" - ")[0].strip() if " - " in title_text else form_type)
        doc_id = f"{cik}_{acc_nodash}"

        filing_details_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/"
        filing_index_url = link_url or f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/{acc_no}-index.htm"
        filing_index_json_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/index.json"
        xbrl_zip_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/{acc_no}-xbrl.zip"

        entries.append({
            "doc_id": doc_id,
            "company_name": company_name,
            "cik": cik,
            "cik_short": cik_short,
            "form": form,
            "accession_number": acc_no,
            "accession_number_raw": acc_nodash,
            "filing_date": filing_date,
            "updated": updated_text,
            "size": file_size,
            "filing_details_url": filing_details_url,
            "filing_index_url": filing_index_url,
            "filing_index_json_url": filing_index_json_url,
            "xbrl_zip_url": xbrl_zip_url,
        })

    return entries


def parse_sec_atom_feed(
    form_type: str, count: int, user_agent: str, seen_ids: set[str] | None = None
) -> list[dict[str, Any]]:
    """
    Query the SEC Atom feed for specified form type(s), merge & sort, take top `count`,
    and resolve XBRL instance and taxonomy file links for each.

    If `seen_ids` is given, filings whose doc_id is already in it are dropped before the (costly)
    resolution step, and every filing discovered by this call is added to it, so that a repeated
    call returns only filings that appeared since the previous one.
    """
    forms = [f.strip() for f in form_type.split(",") if f.strip()]
    raw_entries: list[dict[str, Any]] = []
    merged_ids: set[str] = set()

    for f in forms:
        f_entries = parse_raw_feed_entries(f, count, user_agent)
        for entry in f_entries:
            if entry["doc_id"] not in merged_ids and (seen_ids is None or entry["doc_id"] not in seen_ids):
                merged_ids.add(entry["doc_id"])
                raw_entries.append(entry)

    # Sort merged entries by updated timestamp descending
    raw_entries.sort(key=lambda x: x.get("updated", ""), reverse=True)
    selected_entries = raw_entries[:count] if count > 0 else raw_entries
    if seen_ids is not None:
        seen_ids.update(e["doc_id"] for e in selected_entries)

    # Resolve individual XBRL resources for selected entries
    resolved_records: list[dict[str, Any]] = []
    print(f"[*] Resolving XBRL instance and taxonomy file links for {len(selected_entries)} filing(s)...")

    for entry in selected_entries:
        cik_short = entry["cik_short"]
        acc_nodash = entry["accession_number_raw"]

        (
            xbrl_instance_url,
            xbrl_instance_file,
            xbrl_instance_size,
            ixbrl_url,
            ixbrl_file,
            ixbrl_size,
            taxonomy_files,
            taxonomy_urls,
        ) = resolve_filing_xbrl_resources(cik_short, acc_nodash, user_agent)

        record = dict(entry)
        record.update({
            "ixbrl_file": ixbrl_file,
            "ixbrl_url": ixbrl_url,
            "ixbrl_size": ixbrl_size,
            "xbrl_instance_file": xbrl_instance_file,
            "xbrl_instance_url": xbrl_instance_url,
            "xbrl_instance_size": xbrl_instance_size,
            "taxonomy_files": taxonomy_files,
            "taxonomy_urls": taxonomy_urls,
        })
        resolved_records.append(record)

    return resolved_records


def load_replay_records(replay_dir: pathlib.Path, forms: set[str] | None) -> list[dict[str, Any]]:
    """
    Load previously saved filing records (one JSON file each) from `replay_dir`.

    Files are read in file-name order so every replay produces the same records in the same order.
    When `forms` is given, only records whose `form` is in that set are kept.
    """
    records: list[dict[str, Any]] = []
    for path in sorted(replay_dir.glob("*.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            print(f"[!] Skipping unreadable record {path.name}: {e}", file=sys.stderr)
            continue
        if "doc_id" not in record:
            print(f"[!] Skipping {path.name}: no doc_id field.", file=sys.stderr)
            continue
        if forms and record.get("form", "").upper() not in forms:
            continue
        records.append(record)
    return records


def main() -> None:
    env = load_env(_HERE.parent) if HAS_KAFKA else {}
    default_topic = env.get("FILINGS_TOPIC", "ex05_filings")
    default_ua = env.get("SEC_USER_AGENT", DEFAULT_USER_AGENT)
    default_form = env.get("SEC_FORM_TYPE", "10-K,10-Q")
    default_queue_dir = _HERE.parent / "data" / "queue"

    parser = argparse.ArgumentParser(
        description="Poll SEC EDGAR Atom feed and produce filing metadata records with XBRL instance and taxonomy links."
    )
    parser.add_argument(
        "--form", "-f",
        type=str,
        default=None,
        help=f"Form type(s) to filter (e.g. '10-K', '10-Q', or '10-K,10-Q'). (Default: {default_form}; "
             "with --replay, records are only filtered when this is given)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Max number of filings to fetch and produce (0 for all). (Default: 5 for a one-shot run, "
             "0 for --replay and --watch; with --watch it caps each poll)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run in local mode saving messages to disk instead of Kafka."
    )
    parser.add_argument(
        "--replay",
        nargs="?",
        const=str(default_queue_dir),
        default=None,
        metavar="DIR",
        help="Do not contact the SEC: produce the filing records saved as JSON files in DIR "
             f"(default: {default_queue_dir}), in file-name order. Always yields the same data."
    )
    parser.add_argument(
        "--watch", "-w",
        action="store_true",
        help="Keep polling the SEC feed and produce each filing once, as it appears, until interrupted."
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between polls with --watch. (Default: 60)"
    )
    parser.add_argument(
        "--topic", "-t",
        type=str,
        default=None,
        help=f"Kafka topic override (defaults to FILINGS_TOPIC in env: '{default_topic}')."
    )
    parser.add_argument(
        "--user-agent", "-u",
        type=str,
        default=default_ua,
        help="User-Agent header required by the SEC."
    )

    args = parser.parse_args()
    if args.replay is not None and args.watch:
        parser.error("--replay and --watch cannot be combined.")
    if args.replay is not None and args.local:
        parser.error("--replay reads data/queue; --local writes to it. Do not combine them.")

    explicit_form = args.form
    form_filter = explicit_form or default_form
    one_shot_default = 5 if args.replay is None and not args.watch else 0
    limit = args.limit if args.limit is not None else one_shot_default

    # Determine mode: local or Kafka
    use_local = args.local or not HAS_KAFKA
    producer = None
    registry = None
    topic_name = args.topic or default_topic

    if not use_local:
        try:
            topic_name = args.topic or env.get("FILINGS_TOPIC", "ex05_filings")
            config = kafka_config(env)
            producer = Producer({**config, "linger.ms": 20, "enable.idempotence": True})
            try:
                registry = SchemaRegistry(env)
                print(f"[+] Connected to Schema Registry at {registry.url}")
            except Exception as e:  # noqa: BLE001
                print(f"[!] Schema Registry not configured or failed to connect: {e}. Writing raw JSON.")
            print(f"[+] Initialized Kafka Producer pointing to topic '{topic_name}'")
        except Exception as e:  # noqa: BLE001
            print(f"[!] Failed to initialize Kafka: {e}. Falling back to local disk-only mode.")
            use_local = True

    if args.replay is not None and use_local:
        print("[-] --replay needs Kafka, but it is not available or not configured.", file=sys.stderr)
        sys.exit(1)

    queue_dir = default_queue_dir
    if use_local:
        os.makedirs(queue_dir, exist_ok=True)
        print(f"[+] Operating in LOCAL mode. Output records will be saved to: {queue_dir.resolve()}")

    def emit(record: dict[str, Any], idx: int, total: int) -> bool:
        doc_id = record["doc_id"]
        tax_files = record.get("taxonomy_files") or []

        print("\n" + "=" * 80)
        print(f"[{idx}/{total}] {record.get('company_name')} ({record.get('form')})")
        print(f"CIK: {record.get('cik')} | Accession: {record.get('accession_number')} | "
              f"Filed: {record.get('filing_date')}")
        print(f"iXBRL report      : {record.get('ixbrl_file') or 'Not found'} ({record.get('ixbrl_url') or 'N/A'})")
        print(f"XBRL Instance XML : {record.get('xbrl_instance_file') or 'Not found'} "
              f"({record.get('xbrl_instance_url') or 'N/A'})")
        print(f"Taxonomy Files ({len(tax_files)}): {[tf['name'] for tf in tax_files]}")
        print("=" * 80)

        if use_local:
            local_file_path = queue_dir / f"{doc_id}.json"
            with open(local_file_path, "w", encoding="utf-8") as lf:
                json.dump(record, lf, indent=2, ensure_ascii=False)
            print(f"  [+] Saved record to {local_file_path}")
            return True

        print(f"  [*] Producing message onto Kafka topic '{topic_name}'...")
        key = doc_id.encode()
        val = registry.encode(topic_name, record) if registry else json.dumps(record).encode()
        producer.produce(topic_name, key=key, value=val)
        producer.poll(0)
        print("  [+] Successfully produced record to Kafka!")
        return True

    def emit_all(records: list[dict[str, Any]]) -> int:
        count = 0
        for idx, record in enumerate(records, 1):
            if emit(record, idx, len(records)):
                count += 1
        if producer:
            print("\n[*] Flushing Kafka producer...")
            producer.flush(30)
        return count

    # 1a. Replay mode: produce the saved records, no SEC access.
    if args.replay is not None:
        replay_dir = pathlib.Path(args.replay).expanduser().resolve()
        wanted = {f.strip().upper() for f in explicit_form.split(",") if f.strip()} if explicit_form else None
        records = load_replay_records(replay_dir, wanted)
        if limit > 0:
            records = records[:limit]
        if not records:
            print(f"[-] No filing records found in {replay_dir}.")
            return
        print(f"[+] Replaying {len(records)} saved filing record(s) from {replay_dir}.")
        success_count = emit_all(records)
        print("\n" + "=" * 80)
        print(f"[+] Complete. Successfully produced {success_count}/{len(records)} filing records.")
        print("=" * 80)
        return

    # 1b. Watch mode: poll forever, produce each filing once.
    if args.watch:
        seen: set[str] = set()
        total = 0
        print(f"[+] Watching SEC feed for '{form_filter}' every {args.interval}s. Press Ctrl-C to stop.")
        try:
            while True:
                new_entries = parse_sec_atom_feed(form_filter, limit, args.user_agent, seen_ids=seen)
                if new_entries:
                    print(f"[+] {len(new_entries)} new filing(s).")
                    total += emit_all(new_entries)
                else:
                    print(f"[-] No new filings. {time.strftime('%H:%M:%S')}")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            if producer:
                producer.flush(30)
            print(f"\n[+] Stopped. Produced {total} filing record(s).")
        return

    # 1c. One-shot: fetch the most recent filings from the Atom feed and resolve XBRL links
    feed_entries = parse_sec_atom_feed(form_filter, limit, args.user_agent)
    if not feed_entries:
        print("[-] No filings discovered.")
        return

    print(f"[+] Discovered {len(feed_entries)} filings matching '{form_filter}'.")
    success_count = emit_all(feed_entries)

    print("\n" + "=" * 80)
    print(f"[+] Complete. Successfully produced/saved {success_count}/{len(feed_entries)} filing records.")
    print("=" * 80)


if __name__ == "__main__":
    main()
