#!/usr/bin/env python3
"""
Fetch GRRR (Gorilla Technologies) daily price data for the last 30 days.
Saves to CSV with: Date, Open, High, Low, Close, Adj Close, Volume.
"""

import os
import sys
import yfinance as yf
import pandas as pd
from datetime import datetime

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
DAILY_FILE = os.path.join(DATA_DIR, "grrr_daily_30d.csv")

def fetch_daily():
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"[{datetime.now()}] Fetching 30-day daily data for {TICKER}...")
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period="1mo", interval="1d")

    if df.empty:
        print(f"WARNING: No daily data returned for {TICKER}. Market may be closed or ticker invalid.")
        return False

    df.index = df.index.strftime("%Y-%m-%d")
    df.index.name = "Date"
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    df = df.round(4)

    df.to_csv(DAILY_FILE)
    print(f"Saved {len(df)} days to {DAILY_FILE}")
    print(df.tail(5).to_string())
    return True

if __name__ == "__main__":
    success = fetch_daily()
    sys.exit(0 if success else 1)
