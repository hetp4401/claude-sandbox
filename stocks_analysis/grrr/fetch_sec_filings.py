#!/usr/bin/env python3
"""
GRRR SEC Filings Monitor.

Tracks all SEC filings (8-K, 10-Q, 10-K, S-1, Form 4, etc.) for
Gorilla Technology Group from EDGAR.

Sources:
  - SEC EDGAR full-text search API
  - SEC EDGAR company submissions API
  - RSS/Atom feed from EDGAR

Outputs:
  - grrr_sec_filings.csv:     All filings with dates, types, links
  - grrr_sec_report.txt:      Human-readable report with filing analysis
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

import requests
import pandas as pd

TICKER = "GRRR"
COMPANY = "Gorilla Technology"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
SEC_FILE = os.path.join(DATA_DIR, "grrr_sec_filings.csv")
SEC_REPORT = os.path.join(DATA_DIR, "grrr_sec_report.txt")
LOOKBACK_DAYS = 60

EDGAR_HEADERS = {
    "User-Agent": "StockResearch research@example.com",
    "Accept": "application/json",
}

# Filing type significance for traders
FILING_SIGNIFICANCE = {
    "8-K": ("HIGH", "Material event - earnings, contracts, leadership changes, acquisitions"),
    "8-K/A": ("HIGH", "Amended material event"),
    "10-Q": ("HIGH", "Quarterly earnings report"),
    "10-K": ("HIGH", "Annual report"),
    "10-K/A": ("HIGH", "Amended annual report"),
    "S-1": ("HIGH", "IPO/offering registration"),
    "S-3": ("MEDIUM", "Shelf registration (potential dilution)"),
    "F-1": ("HIGH", "Foreign IPO registration"),
    "F-3": ("MEDIUM", "Foreign shelf registration"),
    "F-4": ("MEDIUM", "Foreign business combination"),
    "4": ("MEDIUM", "Insider transaction"),
    "4/A": ("MEDIUM", "Amended insider transaction"),
    "SC 13D": ("HIGH", "Activist investor >5% stake"),
    "SC 13D/A": ("HIGH", "Amended activist filing"),
    "SC 13G": ("MEDIUM", "Passive investor >5% stake"),
    "SC 13G/A": ("MEDIUM", "Amended passive investor filing"),
    "DEF 14A": ("MEDIUM", "Proxy statement"),
    "424B": ("MEDIUM", "Prospectus - offering details"),
    "6-K": ("LOW", "Foreign private issuer current report"),
    "20-F": ("HIGH", "Foreign annual report"),
    "3": ("LOW", "Initial insider holdings"),
    "5": ("LOW", "Annual insider holdings"),
    "EFFECT": ("MEDIUM", "Registration effective (offering live)"),
}


def resolve_cik(ticker):
    """Resolve ticker to CIK."""
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for key, entry in data.items():
                if entry.get("ticker", "").upper() == ticker.upper():
                    return str(entry["cik_str"]).zfill(10)
    except Exception as e:
        print(f"  CIK error: {e}")
    return None


def fetch_company_filings(cik):
    """Fetch all recent filings from EDGAR submissions API."""
    filings = []
    if not cik:
        return filings

    cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"  EDGAR returned {resp.status_code}")
            return filings

        data = resp.json()
        company_name = data.get("name", COMPANY)
        recent = data.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        descriptions = recent.get("primaryDocDescription", [])
        accessions = recent.get("accessionNumber", [])
        doc_names = recent.get("primaryDocument", [])

        cik_clean = cik.lstrip("0")

        for i in range(len(forms)):
            if dates[i] < cutoff:
                continue

            acc_clean = accessions[i].replace("-", "")
            doc = doc_names[i] if i < len(doc_names) else ""
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_clean}/{doc}" if doc else ""
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_clean}/{accessions[i]}-index.htm"

            sig_info = FILING_SIGNIFICANCE.get(forms[i], ("LOW", "Standard filing"))

            filings.append({
                "filing_date": dates[i],
                "form_type": forms[i],
                "description": descriptions[i] if i < len(descriptions) else "",
                "significance": sig_info[0],
                "significance_note": sig_info[1],
                "accession": accessions[i],
                "filing_url": filing_url,
                "index_url": index_url,
                "company": company_name,
            })

    except Exception as e:
        print(f"  EDGAR parse error: {e}")

    return filings


def fetch_efts_search():
    """Search EDGAR full-text search for GRRR-related filings."""
    filings = []
    cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    try:
        url = (
            f"https://efts.sec.gov/LATEST/search-index?"
            f"q=%22{TICKER}%22&dateRange=custom&startdt={cutoff}&"
            f"forms=8-K,10-Q,10-K,S-3,SC+13D,SC+13G"
        )
        # Use the EFTS search endpoint
        url = (
            f"https://efts.sec.gov/LATEST/search-index?"
            f"q=%22Gorilla+Technology%22&dateRange=custom&startdt={cutoff}"
        )
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            for hit in hits:
                src = hit.get("_source", {})
                filings.append({
                    "filing_date": src.get("file_date", ""),
                    "form_type": src.get("form_type", ""),
                    "description": src.get("display_names", [""])[0] if src.get("display_names") else "",
                    "entity": src.get("entity_name", ""),
                })
    except Exception:
        pass

    return filings


def fetch_sec_filings():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Fetching SEC filings for {TICKER}...")
    print(f"  Lookback: {LOOKBACK_DAYS} days\n")

    # Resolve CIK
    print("  Resolving CIK...")
    cik = resolve_cik(TICKER)
    if cik:
        print(f"  CIK: {cik}")
    else:
        print("  Could not resolve CIK")

    # Fetch from EDGAR
    print("  Fetching EDGAR submissions...")
    filings = fetch_company_filings(cik)
    print(f"  Found {len(filings)} filings")

    if not filings:
        print("  No SEC filings found.")
        pd.DataFrame().to_csv(SEC_FILE, index=False)
        _write_report([], cik)
        return True

    # Sort by date
    filings.sort(key=lambda x: x["filing_date"], reverse=True)

    # Save
    df = pd.DataFrame(filings)
    df.to_csv(SEC_FILE, index=False)
    print(f"  Saved to {SEC_FILE}")

    # Report
    _write_report(filings, cik)
    return True


def _write_report(filings, cik):
    r = []
    r.append("=" * 70)
    r.append("  GRRR SEC FILINGS REPORT")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  CIK: {cik or 'Unknown'}")
    r.append(f"  Lookback: {LOOKBACK_DAYS} days")
    r.append("=" * 70)
    r.append("")

    if not filings:
        r.append("  No filings found in the lookback period.")
        r.append("")
    else:
        # Summary by type
        form_counts = {}
        for f in filings:
            ft = f["form_type"]
            form_counts[ft] = form_counts.get(ft, 0) + 1

        r.append(f"FILING SUMMARY: {len(filings)} total filings")
        r.append("-" * 70)
        for ft, count in sorted(form_counts.items(), key=lambda x: -x[1]):
            sig = FILING_SIGNIFICANCE.get(ft, ("LOW", ""))
            r.append(f"  {ft:<12} {count:>3} filings  [{sig[0]:<6}] {sig[1]}")
        r.append("")

        # High significance filings
        high = [f for f in filings if f.get("significance") == "HIGH"]
        if high:
            r.append("HIGH SIGNIFICANCE FILINGS (catalysts):")
            r.append("-" * 70)
            for f in high:
                r.append(f"  {f['filing_date']} | {f['form_type']:<8} | {f['description'][:55]}")
                r.append(f"    Why: {f.get('significance_note', '')}")
                r.append(f"    URL: {f.get('index_url', '')}")
                r.append("")

        # Medium significance
        medium = [f for f in filings if f.get("significance") == "MEDIUM"]
        if medium:
            r.append("MEDIUM SIGNIFICANCE FILINGS:")
            r.append("-" * 70)
            for f in medium:
                r.append(f"  {f['filing_date']} | {f['form_type']:<8} | {f['description'][:55]}")
            r.append("")

        # All filings chronological
        r.append("ALL FILINGS (chronological):")
        r.append("-" * 70)
        r.append(f"{'Date':<12} {'Form':<10} {'Sig':<7} {'Description'}")
        r.append("-" * 70)
        for f in filings:
            r.append(
                f"{f['filing_date']:<12} {f['form_type']:<10} "
                f"[{f.get('significance', 'LOW'):<5}] {f['description'][:50]}"
            )
        r.append("")

        # Filing frequency analysis
        if len(filings) >= 2:
            dates = [datetime.strptime(f["filing_date"], "%Y-%m-%d") for f in filings]
            date_range = (dates[0] - dates[-1]).days
            freq = len(filings) / max(date_range, 1) * 7  # per week
            r.append(f"FILING FREQUENCY:")
            r.append(f"  Filings per week: {freq:.1f}")
            if freq > 3:
                r.append("  >>> HIGH FILING ACTIVITY - watch for catalysts <<<")
            r.append("")

    r.append("=" * 70)
    r.append("Source: SEC EDGAR")
    r.append("=" * 70)

    report_text = "\n".join(r)
    with open(SEC_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {SEC_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = fetch_sec_filings()
    sys.exit(0 if success else 1)
