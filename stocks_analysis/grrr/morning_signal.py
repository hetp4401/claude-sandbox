#!/usr/bin/env python3
"""
GRRR Pre-Market Signal Generator.

Run this before market open each day to get a trading signal based on:
  1. Previous day direction (alternation pattern)
  2. Day-of-week tendencies
  3. Technical indicators (RSI, MACD, BB, EMAs)
  4. Volume trends
  5. Sentiment (news + Reddit)
  6. Options positioning
  7. Short interest context

Outputs:
  - grrr_morning_signal.txt:  Today's actionable signal + confidence
  - grrr_signal_log.csv:      Historical signal log for backtesting
"""

import os
import sys
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
SIGNAL_FILE = os.path.join(DATA_DIR, "grrr_morning_signal.txt")
SIGNAL_LOG = os.path.join(DATA_DIR, "grrr_signal_log.csv")

DOW_NAMES = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday"}

# Historical day-of-week stats (from 60-day analysis)
DOW_STATS = {
    0: {"name": "Monday",    "up_pct": 63.6, "avg_ret": 1.816, "first_2h_spike": 3.14, "rest_ret": 1.47},
    1: {"name": "Tuesday",   "up_pct": 46.2, "avg_ret": -0.313, "first_2h_spike": 3.00, "rest_ret": -0.58},
    2: {"name": "Wednesday", "up_pct": 38.5, "avg_ret": -0.865, "first_2h_spike": 2.18, "rest_ret": -0.78},
    3: {"name": "Thursday",  "up_pct": 72.7, "avg_ret": 2.264, "first_2h_spike": 4.44, "rest_ret": 0.58},
    4: {"name": "Friday",    "up_pct": 58.3, "avg_ret": 0.158, "first_2h_spike": 2.81, "rest_ret": -0.15},
}

# Alternation stats
ALTERNATION_STATS = {
    "overall_rate": 56.1,
    "up_after_down_pct": 64.0,
    "down_after_up_pct": 50.0,
    "avg_return_after_down": 1.512,
    "avg_return_after_up": 0.296,
    "first_2h_spike_after_down": 3.45,
    "first_2h_spike_after_up": 2.71,
}


def load_latest_daily():
    """Load latest daily data."""
    path = os.path.join(DATA_DIR, "grrr_daily_30d.csv")
    if os.path.exists(path):
        return pd.read_csv(path, index_col=0)
    return pd.DataFrame()


def load_sentiment_scores():
    """Load latest news + reddit sentiment."""
    scores = {"news_polarity": 0, "reddit_polarity": 0, "news_gauge": "N/A", "reddit_gauge": "N/A"}

    news_path = os.path.join(DATA_DIR, "grrr_news_raw.csv")
    if os.path.exists(news_path):
        try:
            df = pd.read_csv(news_path)
            if not df.empty and "polarity" in df.columns:
                scores["news_polarity"] = df["polarity"].mean()
                pol = scores["news_polarity"]
                if pol > 0.1: scores["news_gauge"] = "BULLISH"
                elif pol > 0.03: scores["news_gauge"] = "MILD BULL"
                elif pol > -0.03: scores["news_gauge"] = "NEUTRAL"
                elif pol > -0.1: scores["news_gauge"] = "MILD BEAR"
                else: scores["news_gauge"] = "BEARISH"
        except Exception:
            pass

    reddit_path = os.path.join(DATA_DIR, "grrr_reddit_posts.csv")
    if os.path.exists(reddit_path):
        try:
            df = pd.read_csv(reddit_path)
            if not df.empty and "polarity" in df.columns:
                scores["reddit_polarity"] = df["polarity"].mean()
                pol = scores["reddit_polarity"]
                if pol > 0.1: scores["reddit_gauge"] = "BULLISH"
                elif pol > 0.03: scores["reddit_gauge"] = "MILD BULL"
                elif pol > -0.03: scores["reddit_gauge"] = "NEUTRAL"
                elif pol > -0.1: scores["reddit_gauge"] = "MILD BEAR"
                else: scores["reddit_gauge"] = "BEARISH"
        except Exception:
            pass

    return scores


def load_options_signal():
    """Load options flow signal."""
    path = os.path.join(DATA_DIR, "grrr_options_flow.csv")
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if not df.empty:
                total_call_vol = df["call_volume"].sum()
                total_put_vol = df["put_volume"].sum()
                pc_ratio = total_put_vol / total_call_vol if total_call_vol > 0 else 1
                return {"pc_ratio": round(pc_ratio, 3), "call_vol": total_call_vol, "put_vol": total_put_vol}
        except Exception:
            pass
    return {"pc_ratio": 1.0, "call_vol": 0, "put_vol": 0}


def load_short_data():
    """Load short interest context."""
    path = os.path.join(DATA_DIR, "grrr_short_report.txt")
    if os.path.exists(path):
        try:
            with open(path) as f:
                text = f.read()
            # Extract key numbers
            import re
            si_match = re.search(r"Short % of Float:\s+([\d.]+)%", text)
            dtc_match = re.search(r"Days to Cover \(20d vol\):\s+([\d.]+)", text)
            return {
                "short_pct_float": float(si_match.group(1)) if si_match else 0,
                "days_to_cover": float(dtc_match.group(1)) if dtc_match else 0,
            }
        except Exception:
            pass
    return {"short_pct_float": 0, "days_to_cover": 0}


def generate_signal():
    os.makedirs(DATA_DIR, exist_ok=True)

    now = datetime.now()
    today_dow = now.weekday()

    # Skip weekends
    if today_dow >= 5:
        print(f"  It's {DOW_NAMES.get(today_dow, 'weekend')} -- market closed.")
        return True

    print(f"[{now}] Generating morning signal for {TICKER}...")
    print(f"  Today: {DOW_NAMES[today_dow]}")

    # === Load all data ===
    df_daily = load_latest_daily()
    sentiment = load_sentiment_scores()
    options = load_options_signal()
    short = load_short_data()

    signals = []  # (name, direction, weight, detail)
    # direction: +1 = bullish, -1 = bearish, 0 = neutral

    if df_daily.empty or len(df_daily) < 3:
        print("  ERROR: Not enough daily data. Run fetch_daily.py first.")
        return False

    last = df_daily.iloc[-1]
    prev = df_daily.iloc[-2]
    last_date = df_daily.index[-1]
    last_close = last["Close"]
    prev_close = prev["Close"]
    last_return = (last_close - last["Open"]) / last["Open"] * 100
    last_direction = "UP" if last_return > 0 else "DOWN"

    print(f"  Last trading day: {last_date}")
    print(f"  Last close: ${last_close:.2f} ({last_direction} {last_return:+.2f}%)")

    # ========================================
    # SIGNAL 1: ALTERNATION PATTERN (weight 2)
    # ========================================
    if last_direction == "DOWN":
        alt_signal = +1
        alt_prob = ALTERNATION_STATS["up_after_down_pct"]
        alt_detail = f"Previous day was DOWN -> UP probability {alt_prob:.0f}% (avg return +{ALTERNATION_STATS['avg_return_after_down']:.2f}%)"
    else:
        alt_signal = -1
        alt_prob = ALTERNATION_STATS["down_after_up_pct"]
        alt_detail = f"Previous day was UP -> DOWN probability {alt_prob:.0f}% (avg return +{ALTERNATION_STATS['avg_return_after_up']:.2f}%)"

    signals.append(("Alternation Pattern", alt_signal, 2, alt_detail))

    # ========================================
    # SIGNAL 2: DAY-OF-WEEK (weight 2)
    # ========================================
    dow = DOW_STATS.get(today_dow, {})
    if dow:
        up_pct = dow["up_pct"]
        if up_pct > 60:
            dow_signal = +1
        elif up_pct < 45:
            dow_signal = -1
        else:
            dow_signal = 0
        dow_detail = (
            f"{dow['name']}: historically UP {up_pct:.0f}%, avg return {dow['avg_ret']:+.2f}%, "
            f"first-2h spike +{dow['first_2h_spike']:.1f}%, rest-of-day {dow['rest_ret']:+.2f}%"
        )
        signals.append(("Day-of-Week", dow_signal, 2, dow_detail))

    # ========================================
    # SIGNAL 3: RSI (weight 1.5)
    # ========================================
    if "RSI_14" in df_daily.columns:
        rsi = last["RSI_14"]
        if rsi < 30:
            rsi_signal = +1
            rsi_detail = f"RSI {rsi:.1f} - OVERSOLD (strong bounce signal)"
        elif rsi < 40:
            rsi_signal = +0.5
            rsi_detail = f"RSI {rsi:.1f} - approaching oversold (lean bullish)"
        elif rsi > 70:
            rsi_signal = -1
            rsi_detail = f"RSI {rsi:.1f} - OVERBOUGHT (pullback signal)"
        elif rsi > 60:
            rsi_signal = -0.5
            rsi_detail = f"RSI {rsi:.1f} - approaching overbought (lean bearish)"
        else:
            rsi_signal = 0
            rsi_detail = f"RSI {rsi:.1f} - neutral zone"
        signals.append(("RSI", rsi_signal, 1.5, rsi_detail))

    # ========================================
    # SIGNAL 4: MACD (weight 1)
    # ========================================
    if "MACD" in df_daily.columns and "MACD_Signal" in df_daily.columns:
        macd = last["MACD"]
        macd_sig = last["MACD_Signal"]
        macd_hist = last.get("MACD_Hist", macd - macd_sig)
        prev_hist = prev.get("MACD_Hist", prev.get("MACD", 0) - prev.get("MACD_Signal", 0))

        if macd > macd_sig:
            macd_signal = +1
            macd_detail = f"MACD above signal (bullish crossover)"
        elif macd_hist > prev_hist:
            macd_signal = +0.5
            macd_detail = f"MACD histogram improving ({macd_hist:.4f} vs {prev_hist:.4f})"
        else:
            macd_signal = -0.5
            macd_detail = f"MACD below signal, histogram {macd_hist:.4f}"
        signals.append(("MACD", macd_signal, 1, macd_detail))

    # ========================================
    # SIGNAL 5: BOLLINGER BANDS (weight 1)
    # ========================================
    if "BB_Lower" in df_daily.columns and "BB_Upper" in df_daily.columns:
        bb_lower = last["BB_Lower"]
        bb_upper = last["BB_Upper"]
        bb_mid = last.get("BB_Mid", (bb_upper + bb_lower) / 2)

        if last_close <= bb_lower * 1.01:
            bb_signal = +1
            bb_detail = f"Price at lower BB (${bb_lower:.2f}) - oversold bounce zone"
        elif last_close >= bb_upper * 0.99:
            bb_signal = -1
            bb_detail = f"Price at upper BB (${bb_upper:.2f}) - extended"
        elif last_close < bb_mid:
            bb_signal = -0.3
            bb_detail = f"Below BB midline (${bb_mid:.2f})"
        else:
            bb_signal = +0.3
            bb_detail = f"Above BB midline (${bb_mid:.2f})"
        signals.append(("Bollinger Bands", bb_signal, 1, bb_detail))

    # ========================================
    # SIGNAL 6: EMA TREND (weight 1)
    # ========================================
    if "EMA_9" in df_daily.columns and "EMA_21" in df_daily.columns:
        ema9 = last["EMA_9"]
        ema21 = last["EMA_21"]
        if ema9 > ema21:
            ema_signal = +1
            ema_detail = f"EMA9 (${ema9:.2f}) > EMA21 (${ema21:.2f}) - bullish trend"
        else:
            ema_signal = -0.5
            ema_detail = f"EMA9 (${ema9:.2f}) < EMA21 (${ema21:.2f}) - bearish trend"
        signals.append(("EMA Trend", ema_signal, 1, ema_detail))

    # ========================================
    # SIGNAL 7: VOLUME (weight 0.5)
    # ========================================
    if "Rel_Volume" in df_daily.columns:
        rv = last["Rel_Volume"]
        if rv < 0.5 and last_direction == "DOWN":
            vol_signal = +0.5
            vol_detail = f"Low volume selloff ({rv:.2f}x avg) - weak selling, potential reversal"
        elif rv > 1.5 and last_direction == "UP":
            vol_signal = +0.5
            vol_detail = f"High volume rally ({rv:.2f}x avg) - strong buying conviction"
        elif rv > 1.5 and last_direction == "DOWN":
            vol_signal = -1
            vol_detail = f"High volume selloff ({rv:.2f}x avg) - strong selling"
        else:
            vol_signal = 0
            vol_detail = f"Volume {rv:.2f}x average - neutral"
        signals.append(("Volume", vol_signal, 0.5, vol_detail))

    # ========================================
    # SIGNAL 8: NEWS SENTIMENT (weight 0.5)
    # ========================================
    news_pol = sentiment["news_polarity"]
    if news_pol > 0.05:
        news_signal = +0.5
    elif news_pol < -0.05:
        news_signal = -0.5
    else:
        news_signal = 0
    signals.append(("News Sentiment", news_signal, 0.5,
                     f"News polarity: {news_pol:.4f} ({sentiment['news_gauge']})"))

    # ========================================
    # SIGNAL 9: REDDIT SENTIMENT (weight 0.5)
    # ========================================
    reddit_pol = sentiment["reddit_polarity"]
    if reddit_pol > 0.05:
        reddit_signal = +0.5
    elif reddit_pol < -0.05:
        reddit_signal = -0.5
    else:
        reddit_signal = 0
    signals.append(("Reddit Sentiment", reddit_signal, 0.5,
                     f"Reddit polarity: {reddit_pol:.4f} ({sentiment['reddit_gauge']})"))

    # ========================================
    # SIGNAL 10: OPTIONS FLOW (weight 1)
    # ========================================
    pc = options["pc_ratio"]
    if pc < 0.7:
        opt_signal = +1
        opt_detail = f"Put/Call ratio {pc:.3f} - heavy call buying (BULLISH)"
    elif pc > 1.3:
        opt_signal = -0.5
        opt_detail = f"Put/Call ratio {pc:.3f} - heavy put buying (bearish, but can be contrarian)"
    else:
        opt_signal = 0
        opt_detail = f"Put/Call ratio {pc:.3f} - neutral"
    signals.append(("Options Flow", opt_signal, 1, opt_detail))

    # ========================================
    # SIGNAL 11: SHORT INTEREST (weight 0.5)
    # ========================================
    si_pct = short["short_pct_float"]
    dtc = short["days_to_cover"]
    if si_pct > 15 and dtc > 4:
        si_signal = +0.5
        si_detail = f"Short {si_pct:.1f}% of float, {dtc:.1f} DTC - squeeze potential"
    elif si_pct > 10:
        si_signal = +0.3
        si_detail = f"Short {si_pct:.1f}% of float, {dtc:.1f} DTC - elevated shorts"
    else:
        si_signal = 0
        si_detail = f"Short {si_pct:.1f}% of float, {dtc:.1f} DTC"
    signals.append(("Short Interest", si_signal, 0.5, si_detail))

    # ========================================
    # CALCULATE COMPOSITE SIGNAL
    # ========================================
    weighted_sum = sum(s[1] * s[2] for s in signals)
    total_weight = sum(s[2] for s in signals)
    composite = weighted_sum / total_weight if total_weight > 0 else 0

    # Determine direction and confidence
    if composite > 0.3:
        direction = "BULLISH"
        action = "BUY / LONG"
    elif composite > 0.1:
        direction = "LEAN BULLISH"
        action = "CAUTIOUS LONG"
    elif composite > -0.1:
        direction = "NEUTRAL"
        action = "WAIT / NO TRADE"
    elif composite > -0.3:
        direction = "LEAN BEARISH"
        action = "CAUTIOUS SHORT"
    else:
        direction = "BEARISH"
        action = "SHORT / AVOID LONGS"

    confidence = min(abs(composite) / 0.6 * 100, 100)

    # Intraday strategy
    if dow:
        if dow["first_2h_spike"] > 3 and dow["rest_ret"] < 0:
            intraday_plan = (
                f"FADE THE SPIKE: Expect +{dow['first_2h_spike']:.1f}% in first 2h, "
                f"then fade {dow['rest_ret']:+.1f}% rest of day. "
                f"Consider selling/shorting at 11:30 AM ET."
            )
        elif dow["first_2h_spike"] > 3 and dow["rest_ret"] > 0:
            intraday_plan = (
                f"RIDE THE MOVE: Expect +{dow['first_2h_spike']:.1f}% in first 2h, "
                f"momentum holds ({dow['rest_ret']:+.1f}% rest of day). "
                f"Hold through close."
            )
        else:
            intraday_plan = (
                f"NORMAL DAY: First-2h spike ~{dow['first_2h_spike']:.1f}%, "
                f"rest of day {dow['rest_ret']:+.1f}%."
            )
    else:
        intraday_plan = "No day-of-week data available."

    # After DOWN day specific advice
    if last_direction == "DOWN":
        entry_advice = (
            f"AFTER DOWN DAY: Historical first-2h spike is +{ALTERNATION_STATS['first_2h_spike_after_down']:.1f}%. "
            f"Consider entering at open, taking profits at +3-4% within first 2 hours."
        )
    else:
        entry_advice = (
            f"AFTER UP DAY: First-2h spike is smaller (+{ALTERNATION_STATS['first_2h_spike_after_up']:.1f}%). "
            f"Less edge today. Be more selective."
        )

    # ========================================
    # BUILD REPORT
    # ========================================
    r = []
    r.append("=" * 65)
    r.append(f"  GRRR MORNING TRADING SIGNAL")
    r.append(f"  Date: {now.strftime('%Y-%m-%d')} ({DOW_NAMES[today_dow]})")
    r.append(f"  Generated: {now.strftime('%H:%M:%S')}")
    r.append("=" * 65)
    r.append("")
    r.append(f"  Last Close: ${last_close:.2f} ({last_direction} {last_return:+.2f}%)")
    r.append("")
    r.append(f"  ╔═══════════════════════════════════════════╗")
    r.append(f"  ║  SIGNAL:  {direction:<15}                  ║")
    r.append(f"  ║  ACTION:  {action:<15}                  ║")
    r.append(f"  ║  CONFIDENCE: {confidence:>5.1f}%                       ║")
    r.append(f"  ║  COMPOSITE SCORE: {composite:>+6.3f}                  ║")
    r.append(f"  ╚═══════════════════════════════════════════╝")
    r.append("")

    # Intraday game plan
    r.append("INTRADAY GAME PLAN:")
    r.append("-" * 65)
    r.append(f"  {intraday_plan}")
    r.append(f"  {entry_advice}")
    r.append("")

    # Expected ranges
    if "ATR_14" in df_daily.columns:
        atr = last["ATR_14"]
        r.append("EXPECTED RANGES:")
        r.append(f"  Based on ATR (${atr:.3f}):")
        r.append(f"    Conservative: ${last_close - atr:.2f} - ${last_close + atr:.2f}")
        r.append(f"    Extended:     ${last_close - atr*1.5:.2f} - ${last_close + atr*1.5:.2f}")
    if dow:
        pct_move = dow["first_2h_spike"] / 100
        r.append(f"  Based on first-2h pattern:")
        r.append(f"    Spike target: ${last_close * (1 + pct_move):.2f} (+{dow['first_2h_spike']:.1f}%)")
    r.append("")

    # Signal breakdown
    r.append("SIGNAL BREAKDOWN:")
    r.append("-" * 65)
    r.append(f"  {'Signal':<22} {'Dir':>5} {'Wt':>4} {'Score':>7}  Detail")
    r.append("-" * 65)
    for name, direction_val, weight, detail in signals:
        dir_str = "BULL" if direction_val > 0 else "BEAR" if direction_val < 0 else "NEUT"
        score = direction_val * weight
        r.append(f"  {name:<22} {dir_str:>5} {weight:>4.1f} {score:>+7.3f}  {detail[:50]}")
    r.append("-" * 65)
    r.append(f"  {'COMPOSITE':<22} {'':>5} {total_weight:>4.1f} {weighted_sum:>+7.3f}")
    r.append("")

    # Bullish vs bearish count
    bull_signals = [s for s in signals if s[1] > 0]
    bear_signals = [s for s in signals if s[1] < 0]
    neut_signals = [s for s in signals if s[1] == 0]
    r.append(f"  Bullish signals: {len(bull_signals)}  |  Bearish: {len(bear_signals)}  |  Neutral: {len(neut_signals)}")
    r.append("")

    # Risk warnings
    r.append("RISK FACTORS:")
    r.append("-" * 65)
    if si_pct > 10:
        r.append(f"  [!] High short interest ({si_pct:.1f}%) - volatile, can squeeze or dump")
    if "ATR_14" in df_daily.columns and last["ATR_14"] > 0.8:
        r.append(f"  [!] High ATR (${last['ATR_14']:.3f}) - wide swings expected")
    if pc > 1.3:
        r.append(f"  [!] Heavy put buying (P/C {pc:.2f}) - hedging or bearish bets")
    r.append(f"  [!] This is a volatile small-cap. Position size accordingly.")
    r.append(f"  [!] Pattern confidence is based on 60 days of data. Patterns can break.")
    r.append("")

    r.append("=" * 65)
    r.append(f"  This is NOT financial advice. For educational/research purposes only.")
    r.append("=" * 65)

    report_text = "\n".join(r)

    with open(SIGNAL_FILE, "w") as f:
        f.write(report_text)
    print(f"  Saved signal to {SIGNAL_FILE}")

    # Log the signal for backtesting
    log_entry = {
        "date": now.strftime("%Y-%m-%d"),
        "day_of_week": DOW_NAMES[today_dow],
        "prev_close": last_close,
        "prev_direction": last_direction,
        "prev_return": round(last_return, 4),
        "signal": direction,
        "action": action,
        "confidence": round(confidence, 2),
        "composite": round(composite, 4),
        "rsi": round(last.get("RSI_14", 0), 2) if "RSI_14" in df_daily.columns else "",
    }

    if os.path.exists(SIGNAL_LOG):
        log_df = pd.read_csv(SIGNAL_LOG)
        # Don't duplicate today's entry
        log_df = log_df[log_df["date"] != log_entry["date"]]
        log_df = pd.concat([log_df, pd.DataFrame([log_entry])], ignore_index=True)
    else:
        log_df = pd.DataFrame([log_entry])
    log_df.to_csv(SIGNAL_LOG, index=False)

    print(f"\n{report_text}")
    return True


if __name__ == "__main__":
    success = generate_signal()
    sys.exit(0 if success else 1)
