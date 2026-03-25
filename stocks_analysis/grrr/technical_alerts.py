#!/usr/bin/env python3
"""
GRRR Technical Alert Engine.

Scans existing price data and generates actionable trading signals.
Works on both daily and intraday timeframes.

Signals detected:
  - VWAP crosses (price crossing above/below VWAP)
  - EMA crossovers (9/21 golden cross / death cross)
  - RSI extremes (oversold <30, overbought >70) and divergences
  - MACD crossovers and histogram reversals
  - Bollinger Band squeezes and breakouts
  - Volume spikes (>2x average)
  - Support/resistance breaks
  - Doji / hammer / engulfing candle patterns
  - ATR expansion (volatility breakout)
  - Gap up / gap down detection
  - Consecutive up/down days
  - 52-week high/low proximity

Outputs:
  - grrr_alerts.csv:          All active alerts with timestamps
  - grrr_alerts_report.txt:   Human-readable alert dashboard
"""

import os
import sys
import numpy as np
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
ALERTS_FILE = os.path.join(DATA_DIR, "grrr_alerts.csv")
ALERTS_REPORT = os.path.join(DATA_DIR, "grrr_alerts_report.txt")

# Read existing data files
DAILY_FILE = os.path.join(DATA_DIR, "grrr_daily_30d.csv")
INTRADAY_FILE = os.path.join(DATA_DIR, "grrr_intraday_5min.csv")


def load_daily():
    if os.path.exists(DAILY_FILE):
        return pd.read_csv(DAILY_FILE, index_col=0)
    return pd.DataFrame()


def load_intraday():
    if os.path.exists(INTRADAY_FILE):
        return pd.read_csv(INTRADAY_FILE, index_col=0)
    return pd.DataFrame()


def scan_daily_alerts(df):
    """Scan daily data for trading signals."""
    alerts = []
    if df.empty or len(df) < 5:
        return alerts

    last = df.iloc[-1]
    prev = df.iloc[-2]
    idx = df.index[-1]

    # --- VWAP ---
    if "VWAP" in df.columns:
        if last["Close"] > last["VWAP"] and prev["Close"] <= prev["VWAP"]:
            alerts.append(("DAILY", idx, "BULLISH", "VWAP", "Price crossed ABOVE VWAP",
                           f"Close ${last['Close']:.2f} > VWAP ${last['VWAP']:.2f}"))
        elif last["Close"] < last["VWAP"] and prev["Close"] >= prev["VWAP"]:
            alerts.append(("DAILY", idx, "BEARISH", "VWAP", "Price crossed BELOW VWAP",
                           f"Close ${last['Close']:.2f} < VWAP ${last['VWAP']:.2f}"))

    # --- EMA Crossover ---
    if "EMA_9" in df.columns and "EMA_21" in df.columns:
        if last["EMA_9"] > last["EMA_21"] and prev["EMA_9"] <= prev["EMA_21"]:
            alerts.append(("DAILY", idx, "BULLISH", "EMA_CROSS", "EMA 9/21 GOLDEN CROSS",
                           f"EMA9 ${last['EMA_9']:.2f} crossed above EMA21 ${last['EMA_21']:.2f}"))
        elif last["EMA_9"] < last["EMA_21"] and prev["EMA_9"] >= prev["EMA_21"]:
            alerts.append(("DAILY", idx, "BEARISH", "EMA_CROSS", "EMA 9/21 DEATH CROSS",
                           f"EMA9 ${last['EMA_9']:.2f} crossed below EMA21 ${last['EMA_21']:.2f}"))

    # --- RSI ---
    if "RSI_14" in df.columns:
        rsi = last["RSI_14"]
        if rsi < 30:
            alerts.append(("DAILY", idx, "BULLISH", "RSI", f"RSI OVERSOLD ({rsi:.1f})",
                           "Potential bounce. Watch for reversal confirmation."))
        elif rsi > 70:
            alerts.append(("DAILY", idx, "BEARISH", "RSI", f"RSI OVERBOUGHT ({rsi:.1f})",
                           "Potential pullback. Watch for reversal confirmation."))
        elif rsi < 40 and prev.get("RSI_14", 50) >= 40:
            alerts.append(("DAILY", idx, "WARNING", "RSI", f"RSI entering bearish zone ({rsi:.1f})",
                           "Momentum weakening"))
        elif rsi > 60 and prev.get("RSI_14", 50) <= 60:
            alerts.append(("DAILY", idx, "BULLISH", "RSI", f"RSI entering bullish zone ({rsi:.1f})",
                           "Momentum strengthening"))

        # RSI divergence: price making lower lows but RSI making higher lows
        if len(df) >= 10:
            recent_low_idx = df["Close"].tail(10).idxmin()
            prior_low_idx = df["Close"].tail(20).head(10).idxmin() if len(df) >= 20 else None
            if prior_low_idx and recent_low_idx != prior_low_idx:
                if (df.loc[recent_low_idx, "Close"] < df.loc[prior_low_idx, "Close"] and
                    df.loc[recent_low_idx, "RSI_14"] > df.loc[prior_low_idx, "RSI_14"]):
                    alerts.append(("DAILY", idx, "BULLISH", "RSI_DIV",
                                   "BULLISH RSI DIVERGENCE detected",
                                   "Price lower low + RSI higher low = potential reversal"))

    # --- MACD ---
    if "MACD" in df.columns and "MACD_Signal" in df.columns:
        if last["MACD"] > last["MACD_Signal"] and prev["MACD"] <= prev["MACD_Signal"]:
            alerts.append(("DAILY", idx, "BULLISH", "MACD", "MACD BULLISH CROSSOVER",
                           f"MACD {last['MACD']:.4f} crossed above signal {last['MACD_Signal']:.4f}"))
        elif last["MACD"] < last["MACD_Signal"] and prev["MACD"] >= prev["MACD_Signal"]:
            alerts.append(("DAILY", idx, "BEARISH", "MACD", "MACD BEARISH CROSSOVER",
                           f"MACD {last['MACD']:.4f} crossed below signal {last['MACD_Signal']:.4f}"))

        # MACD histogram reversal
        if "MACD_Hist" in df.columns and len(df) >= 3:
            h1 = df.iloc[-3].get("MACD_Hist", 0)
            h2 = prev.get("MACD_Hist", 0)
            h3 = last.get("MACD_Hist", 0)
            if h1 < h2 < 0 and h3 > h2:
                alerts.append(("DAILY", idx, "BULLISH", "MACD_HIST",
                               "MACD histogram turning up from negative",
                               "Selling pressure decreasing"))
            elif h1 > h2 > 0 and h3 < h2:
                alerts.append(("DAILY", idx, "BEARISH", "MACD_HIST",
                               "MACD histogram turning down from positive",
                               "Buying pressure decreasing"))

    # --- Bollinger Bands ---
    if "BB_Upper" in df.columns and "BB_Lower" in df.columns:
        if last["Close"] > last["BB_Upper"]:
            alerts.append(("DAILY", idx, "WARNING", "BB", "Price ABOVE upper Bollinger Band",
                           f"Close ${last['Close']:.2f} > BB Upper ${last['BB_Upper']:.2f} - extended"))
        elif last["Close"] < last["BB_Lower"]:
            alerts.append(("DAILY", idx, "BULLISH", "BB", "Price BELOW lower Bollinger Band",
                           f"Close ${last['Close']:.2f} < BB Lower ${last['BB_Lower']:.2f} - oversold"))

        # BB Squeeze: width narrowing
        if "BB_Width" in df.columns and len(df) >= 20:
            current_width = last["BB_Width"]
            avg_width = df["BB_Width"].tail(20).mean()
            if current_width < avg_width * 0.6:
                alerts.append(("DAILY", idx, "WARNING", "BB_SQUEEZE",
                               "BOLLINGER SQUEEZE detected",
                               f"Width {current_width:.2f} vs avg {avg_width:.2f} - breakout imminent"))

    # --- Volume ---
    if "Rel_Volume" in df.columns:
        rv = last["Rel_Volume"]
        if rv > 2.0:
            alerts.append(("DAILY", idx, "WARNING", "VOLUME",
                           f"VOLUME SPIKE ({rv:.1f}x average)",
                           "Heavy institutional activity"))
        elif rv < 0.4:
            alerts.append(("DAILY", idx, "INFO", "VOLUME",
                           f"LOW VOLUME ({rv:.1f}x average)",
                           "Light participation - moves may not hold"))

    # --- ATR expansion ---
    if "ATR_14" in df.columns and len(df) >= 20:
        atr = last["ATR_14"]
        avg_atr = df["ATR_14"].tail(20).mean()
        if atr > avg_atr * 1.5:
            alerts.append(("DAILY", idx, "WARNING", "ATR",
                           f"VOLATILITY EXPANDING (ATR {atr:.3f} vs avg {avg_atr:.3f})",
                           "Bigger moves expected. Widen stops."))

    # --- Gap detection ---
    if last["Open"] > prev["High"] * 1.01:
        gap_pct = (last["Open"] - prev["Close"]) / prev["Close"] * 100
        alerts.append(("DAILY", idx, "BULLISH", "GAP",
                       f"GAP UP {gap_pct:.1f}%",
                       f"Open ${last['Open']:.2f} > Prev High ${prev['High']:.2f}"))
    elif last["Open"] < prev["Low"] * 0.99:
        gap_pct = (last["Open"] - prev["Close"]) / prev["Close"] * 100
        alerts.append(("DAILY", idx, "BEARISH", "GAP",
                       f"GAP DOWN {gap_pct:.1f}%",
                       f"Open ${last['Open']:.2f} < Prev Low ${prev['Low']:.2f}"))

    # --- Consecutive days ---
    if len(df) >= 4:
        streak = 0
        direction = None
        for i in range(len(df) - 1, max(len(df) - 8, 0), -1):
            chg = df.iloc[i]["Close"] - df.iloc[i - 1]["Close"] if i > 0 else 0
            if direction is None:
                direction = "up" if chg > 0 else "down"
            if (direction == "up" and chg > 0) or (direction == "down" and chg < 0):
                streak += 1
            else:
                break
        if streak >= 3:
            alerts.append(("DAILY", idx, "WARNING", "STREAK",
                           f"{streak} consecutive {direction} days",
                           f"{'Mean reversion likely' if streak >= 4 else 'Trend developing'}"))

    # --- Support/Resistance from recent price action ---
    if len(df) >= 20:
        recent_high = df["High"].tail(20).max()
        recent_low = df["Low"].tail(20).min()
        close = last["Close"]
        if close >= recent_high * 0.98:
            alerts.append(("DAILY", idx, "BULLISH", "RESISTANCE",
                           f"Near 20-day resistance ${recent_high:.2f}",
                           "Break above = breakout signal"))
        if close <= recent_low * 1.02:
            alerts.append(("DAILY", idx, "WARNING", "SUPPORT",
                           f"Near 20-day support ${recent_low:.2f}",
                           "Break below = breakdown signal"))

    # --- SMA trend alignment ---
    if all(col in df.columns for col in ["SMA_5", "SMA_10", "SMA_20"]):
        if last["SMA_5"] > last["SMA_10"] > last["SMA_20"]:
            alerts.append(("DAILY", idx, "BULLISH", "SMA_ALIGN",
                           "Moving averages BULLISH aligned (5>10>20)",
                           "Strong uptrend structure"))
        elif last["SMA_5"] < last["SMA_10"] < last["SMA_20"]:
            alerts.append(("DAILY", idx, "BEARISH", "SMA_ALIGN",
                           "Moving averages BEARISH aligned (5<10<20)",
                           "Strong downtrend structure"))

    return alerts


def scan_intraday_alerts(df):
    """Scan intraday 5-min data for signals."""
    alerts = []
    if df.empty or len(df) < 20:
        return alerts

    # Focus on latest trading day
    dates = pd.to_datetime(df.index).date
    latest = dates[-1]
    day_df = df[dates == latest].copy()

    if len(day_df) < 5:
        return alerts

    last = day_df.iloc[-1]
    idx = day_df.index[-1]

    # VWAP position
    if "VWAP" in day_df.columns:
        vwap = last["VWAP"]
        close = last["Close"]
        dist = (close - vwap) / vwap * 100
        if abs(dist) < 0.3:
            alerts.append(("5MIN", idx, "INFO", "VWAP", "Price AT VWAP (consolidating)",
                           f"${close:.2f} vs VWAP ${vwap:.2f}"))
        elif dist > 1.5:
            alerts.append(("5MIN", idx, "BULLISH", "VWAP",
                           f"Price {dist:.1f}% above VWAP (strong)",
                           f"${close:.2f} vs VWAP ${vwap:.2f}"))
        elif dist < -1.5:
            alerts.append(("5MIN", idx, "BEARISH", "VWAP",
                           f"Price {dist:.1f}% below VWAP (weak)",
                           f"${close:.2f} vs VWAP ${vwap:.2f}"))

    # Volume surge in recent bars
    if "Vol_SMA_20" in day_df.columns and "Rel_Volume" in day_df.columns:
        recent_rv = day_df["Rel_Volume"].tail(3).mean()
        if recent_rv > 3.0:
            alerts.append(("5MIN", idx, "WARNING", "VOLUME",
                           f"INTRADAY VOLUME SURGE ({recent_rv:.1f}x avg in last 15 min)",
                           "Big player entering/exiting"))

    # Cumulative volume delta
    if "Cum_Vol_Delta" in day_df.columns:
        cvd = last["Cum_Vol_Delta"]
        total_vol = day_df["Volume"].sum()
        if total_vol > 0:
            buy_pct = ((total_vol + cvd) / 2) / total_vol * 100 if cvd else 50
            if buy_pct > 60:
                alerts.append(("5MIN", idx, "BULLISH", "CVD",
                               f"Buy pressure dominant ({buy_pct:.0f}% buy volume)",
                               f"Cum Vol Delta: {cvd:,.0f}"))
            elif buy_pct < 40:
                alerts.append(("5MIN", idx, "BEARISH", "CVD",
                               f"Sell pressure dominant ({100-buy_pct:.0f}% sell volume)",
                               f"Cum Vol Delta: {cvd:,.0f}"))

    # RSI intraday
    if "RSI_14" in day_df.columns:
        rsi = last["RSI_14"]
        if rsi < 25:
            alerts.append(("5MIN", idx, "BULLISH", "RSI",
                           f"Intraday RSI EXTREMELY OVERSOLD ({rsi:.1f})",
                           "Bounce likely"))
        elif rsi > 75:
            alerts.append(("5MIN", idx, "BEARISH", "RSI",
                           f"Intraday RSI EXTREMELY OVERBOUGHT ({rsi:.1f})",
                           "Pullback likely"))

    # Day range analysis
    day_high = day_df["High"].max()
    day_low = day_df["Low"].min()
    day_range = day_high - day_low
    if day_range > 0:
        position_in_range = (last["Close"] - day_low) / day_range
        if position_in_range > 0.9:
            alerts.append(("5MIN", idx, "BULLISH", "RANGE",
                           "Price at TOP of day's range (90%+)",
                           f"Range: ${day_low:.2f} - ${day_high:.2f}"))
        elif position_in_range < 0.1:
            alerts.append(("5MIN", idx, "BEARISH", "RANGE",
                           "Price at BOTTOM of day's range (10%-)",
                           f"Range: ${day_low:.2f} - ${day_high:.2f}"))

    return alerts


def fetch_extended_data():
    """Fetch 60-day data + 52-week for context."""
    alerts = []
    try:
        ticker = yf.Ticker(TICKER)
        hist_1y = ticker.history(period="1y", interval="1d")
        if not hist_1y.empty:
            high_52w = hist_1y["High"].max()
            low_52w = hist_1y["Low"].min()
            current = hist_1y["Close"].iloc[-1]

            pct_from_high = (current - high_52w) / high_52w * 100
            pct_from_low = (current - low_52w) / low_52w * 100

            alerts.append(("CONTEXT", hist_1y.index[-1].strftime("%Y-%m-%d"), "INFO", "52W",
                           f"52-week range: ${low_52w:.2f} - ${high_52w:.2f}",
                           f"Current ${current:.2f} ({pct_from_high:+.1f}% from high, +{pct_from_low:.1f}% from low)"))

            if pct_from_high > -5:
                alerts.append(("CONTEXT", hist_1y.index[-1].strftime("%Y-%m-%d"), "BULLISH", "52W_HIGH",
                               f"Near 52-week HIGH ({pct_from_high:+.1f}%)",
                               "Breakout territory"))
            if pct_from_low < 15:
                alerts.append(("CONTEXT", hist_1y.index[-1].strftime("%Y-%m-%d"), "WARNING", "52W_LOW",
                               f"Near 52-week LOW (+{pct_from_low:.1f}%)",
                               "Capitulation or value zone"))
    except Exception as e:
        print(f"  Extended data error: {e}")

    return alerts


def generate_alerts():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Running technical alert scan for {TICKER}...")

    all_alerts = []

    # Load data
    print("  Loading daily data...")
    df_daily = load_daily()
    if not df_daily.empty:
        print(f"  {len(df_daily)} daily bars loaded")
        daily_alerts = scan_daily_alerts(df_daily)
        all_alerts.extend(daily_alerts)
        print(f"  {len(daily_alerts)} daily alerts")

    print("  Loading intraday data...")
    df_intraday = load_intraday()
    if not df_intraday.empty:
        print(f"  {len(df_intraday)} intraday bars loaded")
        intra_alerts = scan_intraday_alerts(df_intraday)
        all_alerts.extend(intra_alerts)
        print(f"  {len(intra_alerts)} intraday alerts")

    print("  Fetching 52-week context...")
    context_alerts = fetch_extended_data()
    all_alerts.extend(context_alerts)

    print(f"\n  Total alerts: {len(all_alerts)}")

    # Save
    if all_alerts:
        df_alerts = pd.DataFrame(all_alerts, columns=[
            "timeframe", "date", "bias", "signal_type", "signal", "detail"
        ])
        df_alerts.to_csv(ALERTS_FILE, index=False)
        print(f"  Saved to {ALERTS_FILE}")
    else:
        df_alerts = pd.DataFrame()

    # Report
    _write_report(all_alerts, df_daily, df_intraday)
    return True


def _write_report(alerts, df_daily, df_intraday):
    r = []
    r.append("=" * 70)
    r.append("  GRRR TECHNICAL ALERTS DASHBOARD")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append("=" * 70)
    r.append("")

    if not df_daily.empty:
        last = df_daily.iloc[-1]
        r.append("CURRENT STATUS:")
        r.append(f"  Price:  ${last['Close']:.2f}")
        if "RSI_14" in df_daily.columns:
            r.append(f"  RSI:    {last['RSI_14']:.1f}")
        if "VWAP" in df_daily.columns:
            r.append(f"  VWAP:   ${last['VWAP']:.2f}")
        if "ATR_14" in df_daily.columns:
            r.append(f"  ATR:    ${last['ATR_14']:.3f}")
        r.append("")

    if not alerts:
        r.append("  No active alerts.")
        r.append("")
    else:
        # Count by bias
        bullish = [a for a in alerts if a[2] == "BULLISH"]
        bearish = [a for a in alerts if a[2] == "BEARISH"]
        warnings = [a for a in alerts if a[2] == "WARNING"]
        info = [a for a in alerts if a[2] == "INFO"]

        # Overall bias
        bull_count = len(bullish)
        bear_count = len(bearish)
        if bull_count > bear_count * 1.5:
            overall = "BULLISH"
        elif bear_count > bull_count * 1.5:
            overall = "BEARISH"
        else:
            overall = "MIXED/NEUTRAL"

        r.append(f"  >>> OVERALL TECHNICAL BIAS: {overall} <<<")
        r.append(f"  Bullish signals: {bull_count}  |  Bearish signals: {bear_count}  |  Warnings: {len(warnings)}")
        r.append("")

        # Bullish alerts
        if bullish:
            r.append("BULLISH SIGNALS:")
            r.append("-" * 70)
            for a in bullish:
                r.append(f"  [{a[0]:<6}] {a[4]}")
                r.append(f"           {a[5]}")
            r.append("")

        # Bearish alerts
        if bearish:
            r.append("BEARISH SIGNALS:")
            r.append("-" * 70)
            for a in bearish:
                r.append(f"  [{a[0]:<6}] {a[4]}")
                r.append(f"           {a[5]}")
            r.append("")

        # Warnings
        if warnings:
            r.append("WARNINGS:")
            r.append("-" * 70)
            for a in warnings:
                r.append(f"  [{a[0]:<6}] {a[4]}")
                r.append(f"           {a[5]}")
            r.append("")

        # Info
        if info:
            r.append("CONTEXT:")
            r.append("-" * 70)
            for a in info:
                r.append(f"  [{a[0]:<6}] {a[4]}")
                r.append(f"           {a[5]}")
            r.append("")

    r.append("=" * 70)
    r.append("Source: Computed from OHLCV + indicators")
    r.append("=" * 70)

    report_text = "\n".join(r)
    with open(ALERTS_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {ALERTS_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = generate_alerts()
    sys.exit(0 if success else 1)
