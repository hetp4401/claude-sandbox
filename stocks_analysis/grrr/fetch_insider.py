#!/usr/bin/env python3
"""
GRRR Insider Transactions Tracker (SEC Form 4).

Sources:
  - SEC EDGAR API (free, no auth for basic access)
  - yfinance insider transactions
  - Google News RSS for insider filing news

Tracks: buys, sells, option exercises, RSU vesting, gift transactions.

Outputs:
  - grrr_insider_transactions.csv:  All insider trades
  - grrr_insider_summary.csv:       Monthly aggregated buy/sell
  - grrr_insider_report.txt:        Human-readable report
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

import requests
import pandas as pd
import yfinance as yf

TICKER = "GRRR"
CIK = None  # Will be resolved dynamically
COMPANY = "Gorilla Technology"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
INSIDER_FILE = os.path.join(DATA_DIR, "grrr_insider_transactions.csv")
INSIDER_SUMMARY = os.path.join(DATA_DIR, "grrr_insider_summary.csv")
INSIDER_REPORT = os.path.join(DATA_DIR, "grrr_insider_report.txt")
LOOKBACK_DAYS = 60

EDGAR_HEADERS = {
    "User-Agent": "StockResearch research@example.com",
    "Accept": "application/json",
}


def resolve_cik(ticker):
    """Resolve ticker to CIK number via SEC EDGAR."""
    try:
        url = "https://efts.sec.gov/LATEST/search-index?q=%22GRRR%22&dateRange=custom&startdt=2024-01-01&forms=4"
        # Try the ticker mapping endpoint
        url = f"https://efts.sec.gov/LATEST/search-index?q={ticker}&forms=4"
        # Actually use the company tickers JSON
        url = "https://www.sec.gov/files/company_tickers.json"
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for key, entry in data.items():
                if entry.get("ticker", "").upper() == ticker.upper():
                    cik = str(entry["cik_str"]).zfill(10)
                    print(f"  Resolved {ticker} -> CIK {cik} ({entry.get('title', '')})")
                    return cik
    except Exception as e:
        print(f"  CIK resolution error: {e}")
    return None


def fetch_edgar_filings(cik):
    """Fetch Form 4 filings from SEC EDGAR."""
    filings = []
    if not cik:
        return filings

    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        if resp.status_code != 200:
            print(f"  EDGAR API returned {resp.status_code}")
            return filings

        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        descriptions = recent.get("primaryDocDescription", [])
        accessions = recent.get("accessionNumber", [])

        cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        for i in range(len(forms)):
            if forms[i] in ("3", "3/A", "4", "4/A", "5", "144"):
                if dates[i] >= cutoff:
                    acc = accessions[i].replace("-", "")
                    filings.append({
                        "form": forms[i],
                        "filing_date": dates[i],
                        "description": descriptions[i] if i < len(descriptions) else "",
                        "accession": accessions[i],
                        "url": f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{acc}/{accessions[i]}-index.htm",
                    })

    except Exception as e:
        print(f"  EDGAR filings error: {e}")

    return filings


def fetch_yfinance_insiders(ticker):
    """Get insider transactions and holder data from yfinance."""
    transactions = []
    try:
        t = yf.Ticker(ticker)

        # Insider transactions
        try:
            it = t.insider_transactions
            if it is not None and not it.empty:
                for _, row in it.iterrows():
                    tx_date = row.get("startDate") or row.get("Start Date")
                    if tx_date is not None:
                        tx_date_str = tx_date.strftime("%Y-%m-%d") if hasattr(tx_date, "strftime") else str(tx_date)[:10]
                    else:
                        tx_date_str = ""
                    transactions.append({
                        "date": tx_date_str,
                        "insider": str(row.get("insider", row.get("Insider Trading", ""))),
                        "relationship": str(row.get("relation", row.get("Relationship", ""))),
                        "transaction_type": str(row.get("text", row.get("Transaction", ""))),
                        "shares": row.get("shares", row.get("Shares", 0)),
                        "value": row.get("value", row.get("Value", 0)),
                        "source": "yfinance_transactions",
                    })
        except Exception as e:
            print(f"    insider_transactions: {e}")

        # Insider purchases summary
        try:
            ip = t.insider_purchases
            if ip is not None and not ip.empty:
                for _, row in ip.iterrows():
                    label = str(row.iloc[0]) if len(row) > 0 else ""
                    shares = row.get("Shares", 0)
                    trans = row.get("Trans", 0)
                    if label and str(shares) != "<NA>":
                        transactions.append({
                            "date": datetime.now().strftime("%Y-%m-%d"),
                            "insider": "AGGREGATE",
                            "relationship": "Summary (6 months)",
                            "transaction_type": label,
                            "shares": shares if not pd.isna(shares) else 0,
                            "value": 0,
                            "source": "yfinance_purchases",
                        })
        except Exception as e:
            print(f"    insider_purchases: {e}")

        # Insider roster
        try:
            ir = t.insider_roster_holders
            if ir is not None and not ir.empty:
                for _, row in ir.iterrows():
                    transactions.append({
                        "date": str(row.get("latestTransDate", row.get("Date Reported", "")))[:10],
                        "insider": str(row.get("name", row.get("Name", ""))),
                        "relationship": str(row.get("position", row.get("Relation", ""))),
                        "transaction_type": str(row.get("transactionDescription", "")),
                        "shares": row.get("positionDirect", row.get("Shares", 0)),
                        "value": 0,
                        "source": "yfinance_roster",
                    })
        except Exception as e:
            print(f"    insider_roster: {e}")

        # Institutional holders (top holders info is very useful)
        try:
            ih = t.institutional_holders
            if ih is not None and not ih.empty:
                for _, row in ih.iterrows():
                    date_reported = row.get("Date Reported", "")
                    date_str = date_reported.strftime("%Y-%m-%d") if hasattr(date_reported, "strftime") else str(date_reported)[:10]
                    pct_change = row.get("pctChange", 0)
                    shares = row.get("Shares", 0)
                    value = row.get("Value", 0)
                    holder = str(row.get("Holder", ""))

                    action = "Increased" if pct_change and pct_change > 0 else "Decreased" if pct_change and pct_change < 0 else "Held"
                    transactions.append({
                        "date": date_str,
                        "insider": holder,
                        "relationship": "Institutional Holder",
                        "transaction_type": f"{action} position ({pct_change*100:+.1f}%)" if pct_change else "Held",
                        "shares": shares if not pd.isna(shares) else 0,
                        "value": value if not pd.isna(value) else 0,
                        "source": "yfinance_institutional",
                    })
        except Exception as e:
            print(f"    institutional_holders: {e}")

        # Major holders summary
        try:
            mh = t.major_holders
            if mh is not None and not mh.empty:
                for _, row in mh.iterrows():
                    transactions.append({
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "insider": "SUMMARY",
                        "relationship": str(row.get("Breakdown", row.name if hasattr(row, "name") else "")),
                        "transaction_type": "major_holders",
                        "shares": 0,
                        "value": float(row.get("Value", 0)) if not pd.isna(row.get("Value", 0)) else 0,
                        "source": "yfinance_major",
                    })
        except Exception as e:
            print(f"    major_holders: {e}")

    except Exception as e:
        print(f"  yfinance insider error: {e}")

    return transactions


def classify_transaction(tx_type):
    """Classify a transaction as buy, sell, option exercise, RSU, or other."""
    t = str(tx_type).lower()
    if any(w in t for w in ["purchase", "buy", "bought", "acquisition"]):
        return "BUY"
    elif any(w in t for w in ["sale", "sell", "sold", "disposition"]):
        return "SELL"
    elif any(w in t for w in ["option", "exercise"]):
        return "OPTION_EXERCISE"
    elif any(w in t for w in ["rsu", "restricted", "award", "grant", "vest"]):
        return "RSU_GRANT"
    elif any(w in t for w in ["gift"]):
        return "GIFT"
    return "OTHER"


def fetch_insider():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Fetching insider transactions for {TICKER}...")
    print(f"  Lookback: {LOOKBACK_DAYS} days\n")

    all_transactions = []

    # 1. yfinance insiders
    print("  [1/2] yfinance insider data...")
    yf_tx = fetch_yfinance_insiders(TICKER)
    print(f"         {len(yf_tx)} transactions")
    all_transactions.extend(yf_tx)

    # 2. SEC EDGAR Form 4
    print("  [2/2] SEC EDGAR Form 4 filings...")
    cik = resolve_cik(TICKER)
    edgar_filings = []
    if cik:
        edgar_filings = fetch_edgar_filings(cik)
        print(f"         {len(edgar_filings)} Form 4 filings")
        for f in edgar_filings:
            all_transactions.append({
                "date": f["filing_date"],
                "insider": f["description"],
                "relationship": "",
                "transaction_type": f["form"],
                "shares": 0,
                "value": 0,
                "source": "SEC_EDGAR",
                "url": f["url"],
            })
    else:
        print("         Could not resolve CIK, skipping EDGAR")

    print(f"\n  Total transactions: {len(all_transactions)}")

    if not all_transactions:
        print("  No insider transactions found.")
        pd.DataFrame().to_csv(INSIDER_FILE, index=False)
        _write_empty_report()
        return True

    # Classify transactions
    for tx in all_transactions:
        tx["classification"] = classify_transaction(tx["transaction_type"])

    # Save raw
    df = pd.DataFrame(all_transactions)
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    df.to_csv(INSIDER_FILE, index=False)
    print(f"  Saved to {INSIDER_FILE}")

    # Monthly summary
    df_dated = df[df["date"].str.len() >= 7].copy()
    if not df_dated.empty:
        df_dated["month"] = df_dated["date"].str[:7]
        summary = df_dated.groupby("month").agg(
            total_transactions=("date", "count"),
            buys=("classification", lambda x: (x == "BUY").sum()),
            sells=("classification", lambda x: (x == "SELL").sum()),
            options=("classification", lambda x: (x == "OPTION_EXERCISE").sum()),
            rsus=("classification", lambda x: (x == "RSU_GRANT").sum()),
            other=("classification", lambda x: (x.isin(["GIFT", "OTHER"])).sum()),
        )
        summary = summary.sort_index(ascending=False)
        summary.to_csv(INSIDER_SUMMARY)
        print(f"  Saved summary to {INSIDER_SUMMARY}")
    else:
        summary = pd.DataFrame()

    # === Report ===
    _write_report(df, summary, edgar_filings)
    return True


def _write_empty_report():
    lines = [
        "=" * 65,
        "  GRRR INSIDER TRANSACTIONS REPORT",
        f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 65,
        "",
        "  No insider transactions found in the last 60 days.",
        "=" * 65,
    ]
    with open(INSIDER_REPORT, "w") as f:
        f.write("\n".join(lines))


def _write_report(df, summary, edgar_filings):
    r = []
    r.append("=" * 65)
    r.append("  GRRR INSIDER TRANSACTIONS REPORT")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Lookback:  {LOOKBACK_DAYS} days")
    r.append("=" * 65)
    r.append("")

    total = len(df)
    buys = (df["classification"] == "BUY").sum()
    sells = (df["classification"] == "SELL").sum()
    options = (df["classification"] == "OPTION_EXERCISE").sum()
    rsus = (df["classification"] == "RSU_GRANT").sum()

    r.append("TRANSACTION OVERVIEW:")
    r.append(f"  Total transactions:  {total}")
    r.append(f"  Open market BUYS:    {buys}")
    r.append(f"  SELLS:               {sells}")
    r.append(f"  Option exercises:    {options}")
    r.append(f"  RSU grants/vesting:  {rsus}")
    r.append("")

    # Buy/sell signal
    if buys > sells:
        signal = "BULLISH (more buying than selling)"
    elif sells > buys:
        signal = "BEARISH (more selling than buying)"
    elif rsus > 0 and sells == 0 and buys == 0:
        signal = "NEUTRAL (RSU activity only, no open market trades)"
    else:
        signal = "NEUTRAL"
    r.append(f"  >>> INSIDER SIGNAL: {signal} <<<")
    r.append("")

    # List unique insiders
    insiders = df[df["insider"].str.len() > 0]["insider"].unique()
    if len(insiders) > 0:
        r.append("ACTIVE INSIDERS:")
        r.append("-" * 65)
        for name in insiders[:15]:
            ins_df = df[df["insider"] == name]
            r.append(f"  {name}")
            for _, row in ins_df.iterrows():
                shares_str = f" | {row['shares']:,.0f} shares" if row["shares"] else ""
                val_str = f" | ${row['value']:,.0f}" if row.get("value") and row["value"] else ""
                r.append(f"    {row['date']} | {row['classification']}{shares_str}{val_str}")
            r.append("")

    # SEC filings
    if edgar_filings:
        r.append("SEC FORM 4 FILINGS (last 60 days):")
        r.append("-" * 65)
        for f in edgar_filings[:20]:
            r.append(f"  {f['filing_date']} | {f['description'][:60]}")
            r.append(f"    {f['url']}")
        r.append("")

    # Monthly summary
    if not summary.empty:
        r.append("MONTHLY SUMMARY:")
        r.append("-" * 65)
        r.append(f"{'Month':<10} {'Total':>6} {'Buys':>5} {'Sells':>6} {'Options':>8} {'RSUs':>5}")
        r.append("-" * 65)
        for month, row in summary.iterrows():
            r.append(
                f"{month:<10} {int(row['total_transactions']):>6} {int(row['buys']):>5} "
                f"{int(row['sells']):>6} {int(row['options']):>8} {int(row['rsus']):>5}"
            )
        r.append("")

    r.append("=" * 65)
    r.append("Sources: yfinance, SEC EDGAR")
    r.append("=" * 65)

    report_text = "\n".join(r)
    with open(INSIDER_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {INSIDER_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = fetch_insider()
    sys.exit(0 if success else 1)
