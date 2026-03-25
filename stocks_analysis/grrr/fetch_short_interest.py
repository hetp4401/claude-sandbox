#!/usr/bin/env python3
"""
GRRR Short Interest & Borrow Rate Tracker.

Sources:
  - yfinance: short interest, short ratio, short % of float
  - SEC/FINRA short volume via public data
  - Google News RSS for short squeeze chatter

Outputs:
  - grrr_short_interest.csv:  Historical short data
  - grrr_short_report.txt:    Human-readable report
"""

import os
import sys
import json
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import requests
import feedparser
import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
SHORT_FILE = os.path.join(DATA_DIR, "grrr_short_interest.csv")
SHORT_REPORT = os.path.join(DATA_DIR, "grrr_short_report.txt")
LOOKBACK_DAYS = 60

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
}


def fetch_short_interest():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Fetching short interest data for {TICKER}...")

    ticker = yf.Ticker(TICKER)
    info = {}
    try:
        info = ticker.info or {}
    except Exception as e:
        print(f"  Warning: could not fetch ticker info: {e}")

    # === Core short interest metrics from yfinance ===
    short_data = {
        "ticker": TICKER,
        "fetch_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "short_interest": info.get("sharesShort", "N/A"),
        "short_interest_prior": info.get("sharesShortPriorMonth", "N/A"),
        "short_ratio": info.get("shortRatio", "N/A"),
        "short_pct_float": info.get("shortPercentOfFloat", "N/A"),
        "short_pct_shares_out": info.get("sharesPercentSharesOut", "N/A"),
        "shares_outstanding": info.get("sharesOutstanding", "N/A"),
        "float_shares": info.get("floatShares", "N/A"),
        "held_pct_insiders": info.get("heldPercentInsiders", "N/A"),
        "held_pct_institutions": info.get("heldPercentInstitutions", "N/A"),
        "short_interest_date": "",
    }

    # Try to get the short interest date
    si_date = info.get("dateShortInterest")
    if si_date:
        short_data["short_interest_date"] = datetime.fromtimestamp(si_date).strftime("%Y-%m-%d")

    # === Get daily volume for days-to-cover calculation ===
    hist = ticker.history(period="3mo", interval="1d")
    if not hist.empty:
        avg_vol_20 = hist["Volume"].tail(20).mean()
        avg_vol_10 = hist["Volume"].tail(10).mean()
        short_data["avg_volume_20d"] = round(avg_vol_20)
        short_data["avg_volume_10d"] = round(avg_vol_10)

        si = short_data["short_interest"]
        if isinstance(si, (int, float)) and si > 0:
            short_data["days_to_cover_20d"] = round(si / avg_vol_20, 2) if avg_vol_20 > 0 else "N/A"
            short_data["days_to_cover_10d"] = round(si / avg_vol_10, 2) if avg_vol_10 > 0 else "N/A"
        else:
            short_data["days_to_cover_20d"] = "N/A"
            short_data["days_to_cover_10d"] = "N/A"

        # Calculate volume trend (are shorts covering? volume spike?)
        recent_vol = hist["Volume"].tail(5).mean()
        prior_vol = hist["Volume"].tail(20).head(15).mean()
        if prior_vol > 0:
            short_data["volume_change_pct"] = round((recent_vol - prior_vol) / prior_vol * 100, 2)
        else:
            short_data["volume_change_pct"] = "N/A"

        # Price trend during short interest period
        if len(hist) >= 20:
            short_data["price_20d_ago"] = round(hist["Close"].iloc[-20], 4)
            short_data["price_now"] = round(hist["Close"].iloc[-1], 4)
            short_data["price_change_20d_pct"] = round(
                (hist["Close"].iloc[-1] - hist["Close"].iloc[-20]) / hist["Close"].iloc[-20] * 100, 2
            )

    # === Build historical short volume proxy from daily data ===
    # Use daily volume + price action to estimate short pressure
    if not hist.empty:
        df_hist = hist.tail(LOOKBACK_DAYS).copy()
        df_hist.index = df_hist.index.strftime("%Y-%m-%d")
        df_hist.index.name = "Date"

        # Short pressure indicators
        df_hist["Down_Day"] = (df_hist["Close"] < df_hist["Close"].shift(1)).astype(int)
        df_hist["Down_Volume"] = df_hist["Volume"] * df_hist["Down_Day"]
        df_hist["Up_Volume"] = df_hist["Volume"] * (1 - df_hist["Down_Day"])
        df_hist["Down_Vol_Ratio"] = (
            df_hist["Down_Volume"].rolling(5).sum() /
            df_hist["Volume"].rolling(5).sum()
        ).round(4)
        df_hist["Vol_Spike"] = (df_hist["Volume"] / df_hist["Volume"].rolling(20).mean()).round(4)

        # Squeeze indicator: price compressed + volume dropping = coiled spring
        df_hist["BB_Width"] = (
            (df_hist["Close"].rolling(20).std() * 2) / df_hist["Close"].rolling(20).mean() * 100
        ).round(4)

        cols = ["Open", "High", "Low", "Close", "Volume",
                "Down_Day", "Down_Volume", "Up_Volume", "Down_Vol_Ratio",
                "Vol_Spike", "BB_Width"]
        df_hist[cols].to_csv(SHORT_FILE)
        print(f"  Saved {len(df_hist)} days of short pressure data to {SHORT_FILE}")

    # === Search for short squeeze chatter ===
    squeeze_mentions = 0
    try:
        url = f"https://news.google.com/rss/search?q={quote_plus('GRRR short squeeze')}+when:60d&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        squeeze_mentions = len(feed.entries)
    except Exception:
        pass

    short_data["squeeze_news_mentions"] = squeeze_mentions

    # === Generate report ===
    r = []
    r.append("=" * 65)
    r.append("  GRRR SHORT INTEREST REPORT")
    r.append(f"  Generated: {short_data['fetch_date']}")
    r.append("=" * 65)
    r.append("")

    r.append("SHORT INTEREST SNAPSHOT:")
    r.append(f"  Shares Short:            {_fmt(short_data['short_interest'])}")
    r.append(f"  Prior Month Short:       {_fmt(short_data['short_interest_prior'])}")

    si = short_data.get("short_interest", 0)
    si_prior = short_data.get("short_interest_prior", 0)
    if isinstance(si, (int, float)) and isinstance(si_prior, (int, float)) and si_prior > 0:
        si_change = (si - si_prior) / si_prior * 100
        direction = "INCREASING" if si_change > 5 else "DECREASING" if si_change < -5 else "STABLE"
        r.append(f"  Short Interest Change:   {si_change:+.1f}% ({direction})")
    r.append(f"  Short Interest Date:     {short_data.get('short_interest_date', 'N/A')}")
    r.append(f"  Short Ratio (DTC):       {short_data.get('short_ratio', 'N/A')}")
    r.append(f"  Short % of Float:        {_pct(short_data.get('short_pct_float'))}")
    r.append(f"  Short % of Shares Out:   {_pct(short_data.get('short_pct_shares_out'))}")
    r.append("")

    r.append("SHARE STRUCTURE:")
    r.append(f"  Shares Outstanding:      {_fmt(short_data.get('shares_outstanding'))}")
    r.append(f"  Float Shares:            {_fmt(short_data.get('float_shares'))}")
    r.append(f"  Insider Ownership:       {_pct(short_data.get('held_pct_insiders'))}")
    r.append(f"  Institutional Ownership: {_pct(short_data.get('held_pct_institutions'))}")
    r.append("")

    r.append("VOLUME ANALYSIS:")
    r.append(f"  20-Day Avg Volume:       {_fmt(short_data.get('avg_volume_20d'))}")
    r.append(f"  10-Day Avg Volume:       {_fmt(short_data.get('avg_volume_10d'))}")
    r.append(f"  Days to Cover (20d vol): {short_data.get('days_to_cover_20d', 'N/A')}")
    r.append(f"  Days to Cover (10d vol): {short_data.get('days_to_cover_10d', 'N/A')}")
    r.append(f"  Volume Trend (5d vs 15d):{short_data.get('volume_change_pct', 'N/A')}%")
    r.append("")

    r.append("PRICE CONTEXT:")
    r.append(f"  Price 20 days ago:       ${short_data.get('price_20d_ago', 'N/A')}")
    r.append(f"  Price now:               ${short_data.get('price_now', 'N/A')}")
    r.append(f"  20-day price change:     {short_data.get('price_change_20d_pct', 'N/A')}%")
    r.append("")

    r.append(f"SQUEEZE CHATTER:")
    r.append(f"  News mentions (60d):     {squeeze_mentions}")
    r.append("")

    # Squeeze potential assessment
    spf = short_data.get("short_pct_float")
    dtc = short_data.get("days_to_cover_20d")
    r.append("SQUEEZE POTENTIAL ASSESSMENT:")
    signals = []
    if isinstance(spf, (int, float)) and spf > 0.15:
        signals.append(f"  [!] High short % of float: {spf*100:.1f}%")
    if isinstance(dtc, (int, float)) and dtc > 3:
        signals.append(f"  [!] High days to cover: {dtc}")
    if isinstance(si, (int, float)) and isinstance(si_prior, (int, float)) and si_prior > 0:
        if si > si_prior * 1.1:
            signals.append(f"  [!] Short interest increasing MoM")
    if squeeze_mentions > 5:
        signals.append(f"  [!] Active squeeze chatter in news ({squeeze_mentions} mentions)")
    vol_change = short_data.get("volume_change_pct")
    if isinstance(vol_change, (int, float)) and vol_change > 30:
        signals.append(f"  [!] Volume surging: +{vol_change}%")

    if len(signals) >= 3:
        r.append("  >>> SQUEEZE POTENTIAL: HIGH <<<")
    elif len(signals) >= 1:
        r.append("  >>> SQUEEZE POTENTIAL: MODERATE <<<")
    else:
        r.append("  >>> SQUEEZE POTENTIAL: LOW <<<")
    for s in signals:
        r.append(s)
    r.append("")

    r.append("=" * 65)

    report_text = "\n".join(r)
    with open(SHORT_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {SHORT_REPORT}")
    print(f"\n{report_text}")
    return True


def _fmt(val):
    if isinstance(val, (int, float)):
        return f"{val:,.0f}"
    return str(val)

def _pct(val):
    if isinstance(val, (int, float)):
        return f"{val*100:.2f}%"
    return str(val)


if __name__ == "__main__":
    success = fetch_short_interest()
    sys.exit(0 if success else 1)
