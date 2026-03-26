#!/usr/bin/env python3
"""
GRRR Intraday Association Rule Miner (5-minute bars, 60 days).

Discretizes every 5-min bar's features into categorical bins,
then mines rules predicting what happens in the NEXT bar(s):
  - Next 1 bar (5 min)
  - Next 6 bars (30 min)
  - Next 12 bars (1 hour)
  - Next 24 bars (2 hours)

~4,500 bars = much larger sample than daily analysis.

Outputs:
  - grrr_intraday_rules.csv:        All rules above thresholds
  - grrr_intraday_rules_report.txt:  Human-readable report
"""

import os
import sys
import itertools
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
RULES_FILE = os.path.join(DATA_DIR, "grrr_intraday_rules.csv")
RULES_REPORT = os.path.join(DATA_DIR, "grrr_intraday_rules_report.txt")

MIN_SUPPORT_COUNT = 15
MIN_CONFIDENCE = 0.58
MIN_LIFT = 1.10
MAX_ANTECEDENT_SIZE = 3


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_and_prepare():
    """Fetch 60 days of 5-min data, add indicators."""
    print("  Fetching 60-day 5-min data...")
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period="60d", interval="5m")

    if df.empty:
        print("  ERROR: No 5-min data returned.")
        return pd.DataFrame()

    print(f"  Got {len(df)} bars across {len(df.groupby(df.index.date))} days")

    df.index.name = "Datetime"
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()

    # === Derived features ===

    # Bar return
    df["Bar_Return"] = (df["Close"] - df["Open"]) / df["Open"] * 100

    # Bar range
    df["Bar_Range"] = (df["High"] - df["Low"]) / df["Low"] * 100

    # Volume moving averages
    df["Vol_SMA_20"] = df["Volume"].rolling(20).mean()
    df["Rel_Volume"] = df["Volume"] / df["Vol_SMA_20"]

    # EMAs
    df["EMA_9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["EMA_21"] = df["Close"].ewm(span=21, adjust=False).mean()

    # RSI
    df["RSI_14"] = compute_rsi(df["Close"], 14)

    # MACD
    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # Bollinger Bands
    sma20 = df["Close"].rolling(20).mean()
    std20 = df["Close"].rolling(20).std()
    df["BB_Upper"] = sma20 + 2 * std20
    df["BB_Lower"] = sma20 - 2 * std20
    df["BB_Position"] = (df["Close"] - df["BB_Lower"]) / (df["BB_Upper"] - df["BB_Lower"])

    # VWAP (per day)
    df["Typical"] = (df["High"] + df["Low"] + df["Close"]) / 3
    dates = df.index.date
    df["VWAP"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        tp_vol = (df.loc[mask, "Typical"] * df.loc[mask, "Volume"]).cumsum()
        cum_vol = df.loc[mask, "Volume"].cumsum()
        df.loc[mask, "VWAP"] = tp_vol / cum_vol
    df["VWAP_Dist"] = (df["Close"] - df["VWAP"]) / df["VWAP"] * 100

    # Cumulative volume per day
    df["Cum_Volume"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        df.loc[mask, "Cum_Volume"] = df.loc[mask, "Volume"].cumsum()

    # Time of day (bar number within day, 0-77)
    df["Bar_Num"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        n = mask.sum()
        df.loc[mask, "Bar_Num"] = range(n)

    # Consecutive bar direction
    df["Bar_Dir"] = np.where(df["Close"] > df["Open"], 1, -1)
    streak = []
    current = 0
    prev_dir = 0
    for d in df["Bar_Dir"]:
        if d == prev_dir:
            current += d
        else:
            current = d
        streak.append(current)
        prev_dir = d
    df["Streak"] = streak

    # Previous bar return
    df["Prev_Return"] = df["Bar_Return"].shift(1)

    # 3-bar momentum
    df["Mom_3"] = df["Close"].pct_change(3) * 100

    # Price distance from day open
    df["Day_Open"] = np.nan
    for date in pd.unique(dates):
        mask = dates == date
        df.loc[mask, "Day_Open"] = df.loc[mask, "Open"].iloc[0]
    df["From_Day_Open"] = (df["Close"] - df["Day_Open"]) / df["Day_Open"] * 100

    return df


def discretize_bars(df):
    """Discretize all 5-min bar features into categories."""
    cat = pd.DataFrame(index=df.index)

    # Time of day
    cat["Time_Block"] = pd.cut(df["Bar_Num"], bins=[-1, 6, 12, 24, 48, 78],
                                labels=["open_30min", "first_hour", "morning", "midday", "afternoon"])

    # Bar direction/magnitude
    cat["Bar_Dir"] = pd.cut(df["Bar_Return"], bins=[-999, -1, -0.3, 0, 0.3, 1, 999],
                            labels=["sharp_down", "down", "slight_down",
                                    "slight_up", "up", "sharp_up"])

    # Bar range (volatility of this specific bar)
    cat["Bar_Range"] = pd.cut(df["Bar_Range"], bins=[0, 0.3, 0.7, 1.5, 999],
                              labels=["tight", "normal", "wide", "explosive"])

    # Relative volume
    cat["Rel_Vol"] = pd.cut(df["Rel_Volume"], bins=[0, 0.3, 0.7, 1.3, 2.5, 999],
                            labels=["dead", "low", "normal", "high", "surge"])

    # RSI
    cat["RSI"] = pd.cut(df["RSI_14"], bins=[0, 25, 35, 45, 55, 65, 75, 100],
                         labels=["extreme_oversold", "oversold", "weak",
                                 "neutral", "strong", "overbought", "extreme_overbought"])

    # MACD histogram
    macd_std = df["MACD_Hist"].std()
    if macd_std > 0:
        cat["MACD_H"] = pd.cut(df["MACD_Hist"],
                                bins=[-999, -macd_std, -macd_std/3, macd_std/3, macd_std, 999],
                                labels=["strong_bear", "bear", "neutral", "bull", "strong_bull"])

    # MACD cross
    macd_above = (df["MACD"] > df["MACD_Signal"]).astype(bool)
    prev_macd_above = macd_above.shift(1).fillna(False).astype(bool)
    cat["MACD_X"] = "hold"
    cat.loc[macd_above & ~prev_macd_above, "MACD_X"] = "bull_cross"
    cat.loc[~macd_above & prev_macd_above, "MACD_X"] = "bear_cross"
    cat.loc[macd_above & prev_macd_above, "MACD_X"] = "above"
    cat.loc[~macd_above & ~prev_macd_above, "MACD_X"] = "below"

    # Bollinger position
    cat["BB_Pos"] = pd.cut(df["BB_Position"], bins=[-999, 0, 0.2, 0.4, 0.6, 0.8, 1, 999],
                           labels=["below_lower", "low", "low_mid",
                                   "mid", "high_mid", "high", "above_upper"])

    # VWAP position
    cat["VWAP"] = pd.cut(df["VWAP_Dist"], bins=[-999, -1.5, -0.5, 0, 0.5, 1.5, 999],
                          labels=["far_below", "below", "slight_below",
                                  "slight_above", "above", "far_above"])

    # EMA trend
    ema_diff = (df["EMA_9"] - df["EMA_21"]) / df["EMA_21"] * 100
    cat["EMA"] = pd.cut(ema_diff, bins=[-999, -0.5, -0.15, 0, 0.15, 0.5, 999],
                         labels=["strong_bear", "bear", "slight_bear",
                                 "slight_bull", "bull", "strong_bull"])

    # Streak
    cat["Streak"] = pd.cut(df["Streak"], bins=[-999, -3, -2, -1, 0, 1, 2, 3, 999],
                           labels=["long_down", "3_down", "2_down", "1_down",
                                   "1_up", "2_up", "3_up", "long_up"])

    # Previous bar
    cat["Prev_Bar"] = pd.cut(df["Prev_Return"], bins=[-999, -0.5, -0.1, 0.1, 0.5, 999],
                              labels=["prev_sharp_down", "prev_down", "prev_flat",
                                      "prev_up", "prev_sharp_up"])

    # 3-bar momentum
    cat["Mom_3bar"] = pd.cut(df["Mom_3"], bins=[-999, -1, -0.3, 0.3, 1, 999],
                              labels=["falling_fast", "falling", "flat", "rising", "rising_fast"])

    # Position from day open
    cat["Day_Pos"] = pd.cut(df["From_Day_Open"], bins=[-999, -2, -0.5, 0.5, 2, 999],
                             labels=["deep_red", "red", "flat", "green", "deep_green"])

    return cat


def build_outcomes(df):
    """Build NEXT-bar outcome features at multiple timeframes."""
    outcomes = pd.DataFrame(index=df.index)

    # Next 1 bar (5 min)
    next_1 = df["Close"].shift(-1) / df["Close"] * 100 - 100
    outcomes["NEXT_5m"] = pd.cut(next_1, bins=[-999, -0.5, -0.15, 0, 0.15, 0.5, 999],
                                  labels=["5m_sharp_down", "5m_down", "5m_slight_down",
                                          "5m_slight_up", "5m_up", "5m_sharp_up"])
    outcomes["NEXT_5m_dir"] = np.where(next_1 > 0, "5m_UP", "5m_DOWN")
    outcomes.loc[next_1.isna(), "NEXT_5m_dir"] = np.nan

    # Next 6 bars (30 min)
    next_6 = df["Close"].shift(-6) / df["Close"] * 100 - 100
    outcomes["NEXT_30m"] = pd.cut(next_6, bins=[-999, -1, -0.3, 0, 0.3, 1, 999],
                                   labels=["30m_dump", "30m_down", "30m_slight_down",
                                           "30m_slight_up", "30m_up", "30m_rip"])
    outcomes["NEXT_30m_dir"] = np.where(next_6 > 0, "30m_UP", "30m_DOWN")
    outcomes.loc[next_6.isna(), "NEXT_30m_dir"] = np.nan

    # Next 12 bars (1 hour)
    next_12 = df["Close"].shift(-12) / df["Close"] * 100 - 100
    outcomes["NEXT_1h"] = pd.cut(next_12, bins=[-999, -2, -0.5, 0, 0.5, 2, 999],
                                  labels=["1h_crash", "1h_down", "1h_slight_down",
                                          "1h_slight_up", "1h_up", "1h_surge"])
    outcomes["NEXT_1h_dir"] = np.where(next_12 > 0, "1h_UP", "1h_DOWN")
    outcomes.loc[next_12.isna(), "NEXT_1h_dir"] = np.nan

    # Next 24 bars (2 hours)
    next_24 = df["Close"].shift(-24) / df["Close"] * 100 - 100
    outcomes["NEXT_2h"] = pd.cut(next_24, bins=[-999, -3, -1, 0, 1, 3, 999],
                                  labels=["2h_crash", "2h_down", "2h_slight_down",
                                          "2h_slight_up", "2h_up", "2h_rip"])
    outcomes["NEXT_2h_dir"] = np.where(next_24 > 0, "2h_UP", "2h_DOWN")
    outcomes.loc[next_24.isna(), "NEXT_2h_dir"] = np.nan

    # Max gain in next 12 bars (best exit in next hour)
    max_gains = []
    for i in range(len(df)):
        future = df["High"].iloc[i+1:i+13]
        if len(future) > 0:
            max_g = (future.max() - df["Close"].iloc[i]) / df["Close"].iloc[i] * 100
        else:
            max_g = np.nan
        max_gains.append(max_g)
    outcomes["NEXT_1h_MaxGain"] = pd.cut(max_gains,
                                          bins=[-999, 0.3, 0.7, 1.5, 3, 999],
                                          labels=["1h_no_gain", "1h_small_gain",
                                                  "1h_med_gain", "1h_big_gain", "1h_huge_gain"])

    # Max drawdown in next 12 bars
    max_dds = []
    for i in range(len(df)):
        future = df["Low"].iloc[i+1:i+13]
        if len(future) > 0:
            max_dd = (future.min() - df["Close"].iloc[i]) / df["Close"].iloc[i] * 100
        else:
            max_dd = np.nan
        max_dds.append(max_dd)
    outcomes["NEXT_1h_MaxDD"] = pd.cut(max_dds,
                                        bins=[-999, -3, -1.5, -0.7, -0.3, 999],
                                        labels=["1h_big_drop", "1h_med_drop",
                                                "1h_small_drop", "1h_dip", "1h_holds"])

    return outcomes


def mine_rules(ant_df, con_df, total_n):
    """Mine association rules exhaustively."""
    rules = []
    ant_cols = list(ant_df.columns)
    con_cols = list(con_df.columns)

    # Precompute consequent masks
    con_masks = {}
    for col in con_cols:
        for val in con_df[col].dropna().unique():
            mask = con_df[col] == val
            count = mask.sum()
            if count >= MIN_SUPPORT_COUNT:
                con_masks[(col, str(val))] = mask

    total_checked = 0
    print(f"  Antecedent columns: {len(ant_cols)}")
    print(f"  Consequent values: {len(con_masks)}")

    for size in range(1, min(MAX_ANTECEDENT_SIZE + 1, len(ant_cols) + 1)):
        combos = list(itertools.combinations(ant_cols, size))
        print(f"  Testing {len(combos)} combos of size {size}...", end=" ", flush=True)
        found = 0

        for combo in combos:
            sub = ant_df[list(combo)].dropna()
            if len(sub) < MIN_SUPPORT_COUNT:
                continue

            groups = sub.groupby(list(combo))

            for group_key, group_idx in groups:
                if not isinstance(group_key, tuple):
                    group_key = (group_key,)

                ant_mask = ant_df.index.isin(group_idx.index)
                ant_count = ant_mask.sum()

                if ant_count < MIN_SUPPORT_COUNT:
                    continue

                ant_desc = " & ".join(f"{c}={v}" for c, v in zip(combo, group_key))

                for (con_col, con_val), con_mask in con_masks.items():
                    total_checked += 1
                    both = ant_mask & con_mask
                    both_count = both.sum()

                    if both_count < MIN_SUPPORT_COUNT:
                        continue

                    confidence = both_count / ant_count
                    expected = con_mask.sum() / total_n
                    lift = confidence / expected if expected > 0 else 0

                    if confidence >= MIN_CONFIDENCE and lift >= MIN_LIFT:
                        rules.append({
                            "antecedent": ant_desc,
                            "consequent": f"{con_col}={con_val}",
                            "support": round(both_count / total_n, 4),
                            "confidence": round(confidence, 4),
                            "lift": round(lift, 4),
                            "count": both_count,
                            "antecedent_count": ant_count,
                            "consequent_count": int(con_mask.sum()),
                            "total_n": total_n,
                            "size": size,
                        })
                        found += 1

        print(f"{found} rules")

    print(f"\n  Total checked: {total_checked:,}")
    return rules


def run_intraday_apriori():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Intraday Association Rule Mining for {TICKER}...")

    # Fetch and prepare data
    df = fetch_and_prepare()
    if df.empty:
        return False

    # Save raw data
    df.to_csv(os.path.join(DATA_DIR, "grrr_5min_60d.csv"))

    # Discretize
    print("\n  Discretizing features...")
    antecedents = discretize_bars(df)
    print(f"  {len(antecedents.columns)} antecedent features")
    for col in antecedents.columns:
        dist = antecedents[col].value_counts()
        print(f"    {col}: {len(dist)} values, top: {dist.head(3).to_dict()}")

    # Build outcomes
    print("\n  Building outcomes...")
    outcomes = build_outcomes(df)
    print(f"  {len(outcomes.columns)} outcome features")
    for col in outcomes.columns:
        dist = outcomes[col].value_counts()
        print(f"    {col}: {len(dist)} values, top: {dist.head(3).to_dict()}")

    # Align
    valid = outcomes.notna().any(axis=1)
    antecedents = antecedents[valid]
    outcomes = outcomes[valid]
    total_n = len(antecedents)
    print(f"\n  Valid samples: {total_n}")

    # Mine
    print(f"\n  Mining rules (min support={MIN_SUPPORT_COUNT}, min conf={MIN_CONFIDENCE}, min lift={MIN_LIFT})...")
    rules = mine_rules(antecedents, outcomes, total_n)

    print(f"\n  Total rules: {len(rules)}")

    if not rules:
        print("  No rules found.")
        return True

    rules.sort(key=lambda r: r["confidence"] * r["lift"] * r["count"], reverse=True)

    df_rules = pd.DataFrame(rules)
    df_rules.to_csv(RULES_FILE, index=False)
    print(f"  Saved to {RULES_FILE}")

    _write_report(rules, total_n, antecedents.columns, outcomes.columns)
    return True


def _write_report(rules, total_n, ant_cols, out_cols):
    r = []
    r.append("=" * 80)
    r.append("  GRRR INTRADAY ASSOCIATION RULES (5-MIN BARS)")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Sample: {total_n} five-minute bars (~60 trading days)")
    r.append("=" * 80)
    r.append("")
    r.append(f"  Features: {len(ant_cols)} | Outcomes: {len(out_cols)}")
    r.append(f"  Min support: {MIN_SUPPORT_COUNT} | Min confidence: {MIN_CONFIDENCE*100:.0f}% | Min lift: {MIN_LIFT}")
    r.append(f"  Total rules: {len(rules)}")
    r.append("")

    df_r = pd.DataFrame(rules)

    # Top overall
    r.append("TOP 25 RULES (ranked by confidence × lift × count):")
    r.append("-" * 80)
    r.append(f"{'Conf':>5} {'Lift':>5} {'Hits':>8} {'Sup':>5}  Rule")
    r.append("-" * 80)
    for i, rule in enumerate(rules[:25]):
        r.append(f"{rule['confidence']*100:>4.0f}% {rule['lift']:>5.2f} {rule['count']:>4}/{rule['antecedent_count']:<4} {rule['support']*100:>4.1f}%  IF {rule['antecedent'][:50]}")
        r.append(f"{'':>36}→ {rule['consequent']}")
    r.append("")

    # By timeframe
    for tf, label in [("5m_dir", "NEXT 5 MIN DIRECTION"),
                       ("30m_dir", "NEXT 30 MIN DIRECTION"),
                       ("1h_dir", "NEXT 1 HOUR DIRECTION"),
                       ("2h_dir", "NEXT 2 HOUR DIRECTION"),
                       ("1h_MaxGain", "MAX GAIN IN NEXT HOUR"),
                       ("1h_MaxDD", "MAX DRAWDOWN IN NEXT HOUR")]:
        subset = [x for x in rules if tf in x["consequent"]]
        if not subset:
            continue
        subset.sort(key=lambda x: x["count"] * x["confidence"], reverse=True)
        r.append(f"\n{'=' * 80}")
        r.append(f"  {label}")
        r.append("=" * 80)

        # Simplest rules first (1 feature)
        simple = [x for x in subset if x["size"] == 1]
        if simple:
            simple.sort(key=lambda x: x["count"], reverse=True)
            r.append(f"\n  Simple rules (1 condition):")
            r.append(f"  {'Conf':>5} {'Lift':>5} {'Hits':>10}  Rule")
            r.append(f"  {'-'*70}")
            for rule in simple[:15]:
                r.append(f"  {rule['confidence']*100:>4.0f}% {rule['lift']:>5.2f} {rule['count']:>5}/{rule['antecedent_count']:<5}  IF {rule['antecedent']}")
                r.append(f"  {'':>30}→ {rule['consequent']}")

        # High count rules
        high_count = sorted(subset, key=lambda x: x["count"], reverse=True)[:10]
        r.append(f"\n  Highest sample count:")
        r.append(f"  {'Conf':>5} {'Lift':>5} {'Hits':>10}  Rule")
        r.append(f"  {'-'*70}")
        for rule in high_count:
            r.append(f"  {rule['confidence']*100:>4.0f}% {rule['lift']:>5.2f} {rule['count']:>5}/{rule['antecedent_count']:<5}  IF {rule['antecedent'][:50]}")
            r.append(f"  {'':>30}→ {rule['consequent']}")

    r.append(f"\n{'=' * 80}")
    r.append("5-min Apriori analysis across 60 trading days")
    r.append("This is NOT financial advice.")
    r.append("=" * 80)

    report_text = "\n".join(r)
    with open(RULES_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {RULES_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = run_intraday_apriori()
    sys.exit(0 if success else 1)
