#!/usr/bin/env python3
"""
Fetch GRRR (Gorilla Technologies) intraday 5-minute interval data.
Covers market hours (9:30 AM - 4:00 PM ET) for the most recent trading day(s).

yfinance allows up to 60 days of 5-min data. We fetch the last 5 trading days
to ensure we always capture the latest full session.
"""

import os
import sys
import yfinance as yf
import pandas as pd
from datetime import datetime

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
INTRADAY_FILE = os.path.join(DATA_DIR, "grrr_intraday_5min.csv")
INTRADAY_LATEST = os.path.join(DATA_DIR, "grrr_intraday_latest_day.csv")

def fetch_intraday():
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"[{datetime.now()}] Fetching 5-min intraday data for {TICKER}...")
    ticker = yf.Ticker(TICKER)

    # Fetch last 5 trading days at 5-min intervals
    df = ticker.history(period="5d", interval="5m")

    if df.empty:
        print(f"WARNING: No intraday data returned for {TICKER}.")
        return False

    df.index.name = "Datetime"
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    df = df.round(4)

    # Save full multi-day 5-min data
    df.to_csv(INTRADAY_FILE)
    print(f"Saved {len(df)} rows (5-min bars) to {INTRADAY_FILE}")

    # Also extract and save just the latest trading day
    dates = df.index.date
    latest_date = dates[-1]
    latest_df = df[dates == latest_date]
    latest_df.to_csv(INTRADAY_LATEST)
    print(f"Latest day ({latest_date}): {len(latest_df)} bars saved to {INTRADAY_LATEST}")

    print(f"\n--- Latest day summary ({latest_date}) ---")
    print(f"  Open:   {latest_df['Open'].iloc[0]:.4f}")
    print(f"  High:   {latest_df['High'].max():.4f}")
    print(f"  Low:    {latest_df['Low'].min():.4f}")
    print(f"  Close:  {latest_df['Close'].iloc[-1]:.4f}")
    print(f"  Volume: {latest_df['Volume'].sum():,.0f}")
    print(f"  Bars:   {len(latest_df)}")

    return True

if __name__ == "__main__":
    success = fetch_intraday()
    sys.exit(0 if success else 1)
