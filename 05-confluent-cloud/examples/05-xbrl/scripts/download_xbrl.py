#!/usr/bin/env python3
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# /// script
# dependencies = [
#     "requests",
#     "beautifulsoup4",
# ]
# ///
"""
SEC EDGAR XBRL Downloader for S&P 500 Companies.

This script:
1. Scrapes the S&P 500 companies list from Wikipedia to get tickers, names, and CIKs.
2. Queries the official SEC EDGAR submissions API to find the latest 10-K (Annual Report) for those companies.
3. Resolves the folder directory and displays the direct links to the individual XBRL files.
4. Downloads the official bulk XBRL ZIP file to disk.

Complies strictly with the SEC's Developer API requirements (User-Agent header and rate limiting).
"""

import argparse
import os
import sys
import time

import requests
from bs4 import BeautifulSoup

# SEC EDGAR rate limit is 10 requests per second. We maintain a conservative sleep interval.
SEC_SLEEP_INTERVAL = 0.15

# Default User-Agent to satisfy the SEC fair access policy
DEFAULT_USER_AGENT = "DoclingConfluentCloudScraper/1.0 (researcher@example.com; academic research scraper)"


def get_sp500_companies(user_agent: str) -> list[dict[str, str]]:
    """
    Scrape S&P 500 constituents from Wikipedia.
    
    Returns a list of dictionaries with 'ticker', 'name', and 'cik'.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": user_agent}
    
    print("[*] Fetching S&P 500 companies from Wikipedia...")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[!] Error fetching S&P 500 list from Wikipedia: {e}", file=sys.stderr)
        sys.exit(1)
        
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    if not table:
        print("[!] Error: Could not find S&P 500 constituents table on Wikipedia.", file=sys.stderr)
        sys.exit(1)
        
    headers_text = [th.text.strip() for th in table.find_all("th")]
    
    # Identify indices of key columns dynamically
    try:
        symbol_idx = headers_text.index("Symbol")
        security_idx = headers_text.index("Security")
        cik_idx = headers_text.index("CIK")
    except ValueError as e:
        print(f"[!] Error parsing table columns: {e}. Falling back to default indices.", file=sys.stderr)
        symbol_idx, security_idx, cik_idx = 0, 1, 6
        
    companies = []
    rows = table.find_all("tr")[1:]  # skip header row
    for row in rows:
        cols = row.find_all("td")
        if len(cols) > max(symbol_idx, security_idx, cik_idx):
            ticker = cols[symbol_idx].text.strip()
            # Clean up ticker formats for SEC EDGAR (e.g. BF.B -> BF-B or BRK.B -> BRK-B)
            ticker_clean = ticker.replace(".", "-")
            name = cols[security_idx].text.strip()
            cik = cols[cik_idx].text.strip()
            companies.append({
                "ticker_wiki": ticker,
                "ticker": ticker_clean,
                "name": name,
                "cik": cik
            })
            
    print(f"[+] Successfully loaded {len(companies)} S&P 500 companies from Wikipedia.")
    return companies


def get_latest_10k_metadata(cik: str, ticker: str, user_agent: str) -> dict | None:
    """
    Fetch submission history for a given CIK from SEC EDGAR and locate the latest 10-K filing.
    """
    # CIK must be padded to 10 digits
    padded_cik = f"{int(cik):010d}"
    url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
    headers = {"User-Agent": user_agent}
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        time.sleep(SEC_SLEEP_INTERVAL)  # Politeness delay to satisfy rate limit
        
        if resp.status_code == 404:
            print(f"[!] CIK {padded_cik} ({ticker}) not found on SEC API.")
            return None
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[!] Error fetching submissions for {ticker} (CIK {padded_cik}): {e}")
        return None
        
    recent = data.get("filings", {}).get("recent", {})
    if not recent:
        print(f"[!] No recent filings found for {ticker} (CIK {padded_cik}).")
        return None
        
    forms = recent.get("form", [])
    for i, form in enumerate(forms):
        if form == "10-K":
            is_xbrl = recent.get("isXBRL", [])[i] if "isXBRL" in recent else 1
            return {
                "accession_number": recent["accessionNumber"][i],
                "filing_date": recent["filingDate"][i],
                "primary_document": recent["primaryDocument"][i],
                "is_xbrl": is_xbrl,
                "cik_short": str(int(cik))
            }
            
    print(f"[-] No 10-K filing found in recent history for {ticker} (CIK {padded_cik}).")
    return None


def get_individual_xbrl_links(cik_short: str, acc_nodash: str, user_agent: str) -> list[dict[str, str]]:
    """
    Query index.json of the filing folder to locate individual XBRL and supporting files.
    """
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/index.json"
    headers = {"User-Agent": user_agent}
    
    files = []
    try:
        resp = requests.get(index_url, headers=headers, timeout=30)
        time.sleep(SEC_SLEEP_INTERVAL)
        if resp.status_code == 200:
            index_data = resp.json()
            for item in index_data.get("directory", {}).get("item", []):
                name = item.get("name", "")
                name_lower = name.lower()
                # Filter for XBRL instance, schema, linkbase files or the bulk zip file
                if name_lower.endswith((".xsd", ".xml", ".zip")):
                    file_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/{name}"
                    files.append({
                        "name": name,
                        "url": file_url,
                        "size": item.get("size", "unknown")
                    })
    except requests.RequestException as e:
        print(f"[!] Warning: Could not retrieve individual file list from index.json: {e}")
        
    return files


def download_file(url: str, dest_path: str, user_agent: str) -> bool:
    """
    Download a file from a URL to a local destination path.
    """
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        print(f"    [+] File already exists on disk: {dest_path}. Skipping download.")
        return True

    headers = {"User-Agent": user_agent}
    try:
        print(f"    -> Downloading from {url}...")
        resp = requests.get(url, headers=headers, stream=True, timeout=60)
        time.sleep(SEC_SLEEP_INTERVAL)
        resp.raise_for_status()
        
        # Ensure target directory exists
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"    [+] Saved to {dest_path}")
        return True
    except (requests.RequestException, OSError) as e:
        print(f"    [!] Error downloading file {url}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download S&P 500 latest 10-K annual report XBRL files from SEC EDGAR."
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=5,
        help="Number of S&P 500 companies to process. Set to 0 to process all 500+ companies. (Default: 5)"
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str,
        default="examples/05-xbrl/data",
        help="Directory where downloaded files will be saved. (Default: examples/05-xbrl/data)"
    )
    parser.add_argument(
        "--user-agent", "-u",
        type=str,
        default=DEFAULT_USER_AGENT,
        help="User-Agent header required by the SEC. Format: 'Name [email protected]'"
    )
    
    args = parser.parse_args()
    
    # 1. Fetch S&P 500 list
    companies = get_sp500_companies(args.user_agent)
    
    limit = len(companies) if args.limit <= 0 else args.limit
    companies_to_process = companies[:limit]
    
    print(f"\n[*] Processing top {limit} companies to locate latest 10-K XBRL files...\n")
    
    success_count = 0
    for idx, comp in enumerate(companies_to_process, 1):
        ticker = comp["ticker"]
        cik = comp["cik"]
        name = comp["name"]
        
        print("=" * 80)
        print(f"[{idx}/{limit}] {name} ({ticker}) | CIK: {cik}")
        print("=" * 80)
        
        # 2. Get latest 10-K filing metadata
        meta = get_latest_10k_metadata(cik, ticker, args.user_agent)
        if not meta:
            continue
            
        acc_dashed = meta["accession_number"]
        acc_nodash = acc_dashed.replace("-", "")
        cik_short = meta["cik_short"]
        filing_date = meta["filing_date"]
        
        print(f"  [+] Latest 10-K found! Filed on: {filing_date}")
        print(f"  [+] Accession Number: {acc_dashed}")
        
        # 3. Locate individual XBRL links
        print("  [*] Querying filing file index...")
        individual_files = get_individual_xbrl_links(cik_short, acc_nodash, args.user_agent)
        
        zip_file_info = None
        
        if individual_files:
            print("  [+] Discovered XBRL & Supporting files:")
            for f in individual_files:
                print(f"    - {f['name']} (Size: {f['size']} bytes)")
                print(f"      URL: {f['url']}")
                if f["name"].endswith("-xbrl.zip"):
                    zip_file_info = f
        else:
            print("  [!] Failed to retrieve filing file index.")
            
        # 4. Construct zip URL if not found in index but isXBRL is true
        if not zip_file_info and meta["is_xbrl"]:
            fallback_zip_name = f"{acc_dashed}-xbrl.zip"
            fallback_zip_url = f"https://www.sec.gov/Archives/edgar/data/{cik_short}/{acc_nodash}/{fallback_zip_name}"
            zip_file_info = {
                "name": fallback_zip_name,
                "url": fallback_zip_url,
                "size": "unknown"
            }
            print("  [*] constructed fallback ZIP URL:")
            print(f"    - {fallback_zip_name}")
            print(f"      URL: {fallback_zip_url}")
            
        if zip_file_info:
            # 5. Download the bulk XBRL ZIP file
            comp_dir = os.path.join(args.output_dir, ticker)
            local_zip_path = os.path.join(comp_dir, zip_file_info["name"])
            
            print("  [*] Downloading bulk XBRL ZIP file...")
            downloaded = download_file(zip_file_info["url"], local_zip_path, args.user_agent)
            
            if downloaded:
                success_count += 1
        else:
            print(f"  [!] No XBRL archive available for this filing (isXBRL={meta['is_xbrl']}).")
            
        print()
        
    print("=" * 80)
    print(f"[+] Process complete. Successfully downloaded {success_count}/{limit} filing packages.")
    print(f"[+] All files saved under: {os.path.abspath(args.output_dir)}")
    print("=" * 80)


if __name__ == "__main__":
    main()
