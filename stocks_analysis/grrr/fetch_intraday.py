#!/usr/bin/env python3
"""
Fetch GRRR (Gorilla Technologies) intraday 5-minute interval data.
Covers market hours (9:30 AM - 4:00 PM ET).

Includes key intraday trading indicators per bar:
  - VWAP (cumulative, resets each day)
  - Cumulative volume & relative volume
  - Price change & % change from prior bar
  - EMA 9, 21 (on 5-min bars)
  - RSI 14 (on 5-min bars)
  - Bollinger Bands (20-bar)
  - MACD (12, 26, 9)
  - Bar range & spread
  - Volume delta (buy vs sell pressure estimate)
  - Candle body size & upper/lower wick ratios
"""

import os
import sys
import numpy as np
import yfinance as yf
import pandas as pd
from datetime import datetime

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
INTRADAY_FILE = os.path.join(DATA_DIR, "grrr_intraday_5min.csv")
INTRADAY_LATEST = os.path.join(DATA_DIR, "grrr_intraday_latest_day.csv")


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def add_indicators(df):
    """Add trading indicators to a 5-min OHLCV dataframe."""

    # === Price change per bar ===
    df["Change"] = df["Close"] - df["Close"].shift(1)
    df["Change_Pct"] = (df["Change"] / df["Close"].shift(1) * 100)

    # === Bar range & candle anatomy ===
    df["Bar_Range"] = df["High"] - df["Low"]
    df["Body"] = (df["Close"] - df["Open"]).abs()
    bar_range_safe = df["Bar_Range"].replace(0, np.nan)
    df["Upper_Wick_Pct"] = ((df["High"] - df[["Open", "Close"]].max(axis=1)) / bar_range_safe * 100)
    df["Lower_Wick_Pct"] = ((df[["Open", "Close"]].min(axis=1) - df["Low"]) / bar_range_safe * 100)

    # === VWAP (cumulative per day) ===
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    dates = df.index.date
    df["VWAP"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        tp_vol = (typical_price[mask] * df["Volume"][mask]).cumsum()
        cum_vol = df["Volume"][mask].cumsum()
        df.loc[mask, "VWAP"] = tp_vol / cum_vol

    # === Cumulative volume per day ===
    df["Cum_Volume"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        df.loc[mask, "Cum_Volume"] = df["Volume"][mask].cumsum()

    # === Volume delta estimate (positive bar = buy pressure, negative = sell) ===
    df["Vol_Delta"] = np.where(df["Close"] >= df["Open"], df["Volume"], -df["Volume"])
    df["Cum_Vol_Delta"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        df.loc[mask, "Cum_Vol_Delta"] = df["Vol_Delta"][mask].cumsum()

    # === EMAs ===
    df["EMA_9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["EMA_21"] = df["Close"].ewm(span=21, adjust=False).mean()

    # === RSI 14 ===
    df["RSI_14"] = compute_rsi(df["Close"], 14)

    # === MACD ===
    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # === Bollinger Bands (20-bar) ===
    sma20 = df["Close"].rolling(window=20).mean()
    std20 = df["Close"].rolling(window=20).std()
    df["BB_Upper"] = sma20 + 2 * std20
    df["BB_Mid"] = sma20
    df["BB_Lower"] = sma20 - 2 * std20

    # === Volume SMA for relative volume ===
    df["Vol_SMA_20"] = df["Volume"].rolling(window=20).mean()
    df["Rel_Volume"] = df["Volume"] / df["Vol_SMA_20"]

    return df


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

    # Add all indicators
    df = add_indicators(df)
    df = df.round(4)

    # Save full multi-day 5-min data
    df.to_csv(INTRADAY_FILE)
    print(f"Saved {len(df)} rows (5-min bars) to {INTRADAY_FILE}")

    # Extract and save just the latest trading day
    dates = df.index.date
    latest_date = dates[-1]
    latest_df = df[dates == latest_date]
    latest_df.to_csv(INTRADAY_LATEST)
    print(f"Latest day ({latest_date}): {len(latest_df)} bars saved to {INTRADAY_LATEST}")

    print(f"\n--- Latest day summary ({latest_date}) ---")
    print(f"  Open:       {latest_df['Open'].iloc[0]:.4f}")
    print(f"  High:       {latest_df['High'].max():.4f}")
    print(f"  Low:        {latest_df['Low'].min():.4f}")
    print(f"  Close:      {latest_df['Close'].iloc[-1]:.4f}")
    print(f"  Volume:     {latest_df['Volume'].sum():,.0f}")
    print(f"  VWAP:       {latest_df['VWAP'].iloc[-1]:.4f}")
    print(f"  RSI:        {latest_df['RSI_14'].iloc[-1]:.1f}")
    print(f"  Bars:       {len(latest_df)}")
    print(f"\nColumns: {list(df.columns)}")

    return True


if __name__ == "__main__":
    success = fetch_intraday()
    sys.exit(0 if success else 1)
