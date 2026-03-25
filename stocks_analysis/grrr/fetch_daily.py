#!/usr/bin/env python3
"""
Fetch GRRR (Gorilla Technologies) daily price data for the last 30 days.
Includes derived day-trading indicators:
  - VWAP (Volume Weighted Average Price)
  - Price change / % change from prior close
  - Daily range & range % (volatility)
  - Moving averages: SMA 5, 10, 20
  - EMA 9, 21
  - RSI (14-period)
  - MACD (12, 26, 9)
  - Bollinger Bands (20, 2)
  - Average volume (20-day) & relative volume
  - ATR (14-period Average True Range)
  - On-Balance Volume (OBV)
  - Accumulation/Distribution line
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
DAILY_FILE = os.path.join(DATA_DIR, "grrr_daily_30d.csv")


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_atr(df, period=14):
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - df["Close"].shift()).abs()
    low_close = (df["Low"] - df["Close"].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return true_range.rolling(window=period).mean()


def fetch_daily():
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"[{datetime.now()}] Fetching 30-day daily data for {TICKER}...")
    ticker = yf.Ticker(TICKER)
    # Fetch extra history so moving averages have enough lookback
    df = ticker.history(period="3mo", interval="1d")

    if df.empty:
        print(f"WARNING: No daily data returned for {TICKER}.")
        return False

    df.index.name = "Date"

    # === Core price columns (already present) ===
    # Open, High, Low, Close, Volume

    # === Price Change ===
    df["Prev_Close"] = df["Close"].shift(1)
    df["Change"] = df["Close"] - df["Prev_Close"]
    df["Change_Pct"] = (df["Change"] / df["Prev_Close"] * 100)

    # === Daily Range (volatility measure) ===
    df["Range"] = df["High"] - df["Low"]
    df["Range_Pct"] = (df["Range"] / df["Low"] * 100)

    # === VWAP (daily approximation using typical price * volume) ===
    df["VWAP"] = ((df["High"] + df["Low"] + df["Close"]) / 3 * df["Volume"]).cumsum() / df["Volume"].cumsum()

    # === Simple Moving Averages ===
    df["SMA_5"] = df["Close"].rolling(window=5).mean()
    df["SMA_10"] = df["Close"].rolling(window=10).mean()
    df["SMA_20"] = df["Close"].rolling(window=20).mean()

    # === Exponential Moving Averages ===
    df["EMA_9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["EMA_21"] = df["Close"].ewm(span=21, adjust=False).mean()

    # === RSI (14-period) ===
    df["RSI_14"] = compute_rsi(df["Close"], 14)

    # === MACD ===
    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # === Bollinger Bands (20-period, 2 std) ===
    df["BB_Mid"] = df["SMA_20"]
    bb_std = df["Close"].rolling(window=20).std()
    df["BB_Upper"] = df["BB_Mid"] + 2 * bb_std
    df["BB_Lower"] = df["BB_Mid"] - 2 * bb_std
    df["BB_Width"] = (df["BB_Upper"] - df["BB_Lower"]) / df["BB_Mid"] * 100

    # === Volume indicators ===
    df["Vol_SMA_20"] = df["Volume"].rolling(window=20).mean()
    df["Rel_Volume"] = df["Volume"] / df["Vol_SMA_20"]

    # === ATR (14-period) ===
    df["ATR_14"] = compute_atr(df, 14)

    # === On-Balance Volume (OBV) ===
    obv = [0]
    for i in range(1, len(df)):
        if df["Close"].iloc[i] > df["Close"].iloc[i - 1]:
            obv.append(obv[-1] + df["Volume"].iloc[i])
        elif df["Close"].iloc[i] < df["Close"].iloc[i - 1]:
            obv.append(obv[-1] - df["Volume"].iloc[i])
        else:
            obv.append(obv[-1])
    df["OBV"] = obv

    # === Accumulation/Distribution ===
    mfm = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / (df["High"] - df["Low"])
    mfm = mfm.fillna(0)
    df["AD_Line"] = (mfm * df["Volume"]).cumsum()

    # === Trim to last ~30 trading days and clean up ===
    df = df.tail(30).copy()
    df.index = df.index.strftime("%Y-%m-%d")

    # Drop helper columns
    cols_to_keep = [
        "Open", "High", "Low", "Close", "Volume",
        "Prev_Close", "Change", "Change_Pct",
        "Range", "Range_Pct", "VWAP",
        "SMA_5", "SMA_10", "SMA_20",
        "EMA_9", "EMA_21",
        "RSI_14",
        "MACD", "MACD_Signal", "MACD_Hist",
        "BB_Upper", "BB_Mid", "BB_Lower", "BB_Width",
        "Vol_SMA_20", "Rel_Volume",
        "ATR_14", "OBV", "AD_Line",
    ]
    df = df[cols_to_keep]
    df = df.round(4)

    df.to_csv(DAILY_FILE)
    print(f"Saved {len(df)} days to {DAILY_FILE}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nLast 3 days:")
    print(df.tail(3).T.to_string())
    return True


if __name__ == "__main__":
    success = fetch_daily()
    sys.exit(0 if success else 1)
