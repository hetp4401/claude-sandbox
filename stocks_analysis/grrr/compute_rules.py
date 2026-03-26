#!/usr/bin/env python3
"""
GRRR Master Association Rule Miner.

Single-command pipeline that:
  1. Fetches latest 60-day price data (daily, hourly, 5-min)
  2. Scrapes latest news + Reddit sentiment
  3. Runs Apriori association rule mining at 3 granularities:
     a) Daily price patterns → next day outcomes
     b) Intraday 5-min patterns → next 5m/30m/1h/2h outcomes
     c) Sentiment (news + Reddit) → next session price outcomes
  4. Combines all rules, ranks by confidence × sample count
  5. Outputs a master report with the highest-confidence tradeable rules

Usage:
  python3 compute_rules.py              # Full run (all 3 levels)
  python3 compute_rules.py --daily      # Daily rules only
  python3 compute_rules.py --intraday   # Intraday 5-min rules only
  python3 compute_rules.py --sentiment  # Sentiment rules only
  python3 compute_rules.py --report     # Just regenerate master report from existing rules
"""

import os
import sys
import argparse
import time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
sys.path.insert(0, SCRIPT_DIR)

MASTER_REPORT = os.path.join(DATA_DIR, "grrr_master_rules_report.txt")


def step_fetch_data():
    """Refresh all price + sentiment data."""
    print("\n" + "=" * 70)
    print("STEP 1: REFRESHING DATA")
    print("=" * 70)

    from fetch_daily import fetch_daily
    from fetch_intraday import fetch_intraday

    fetch_daily()
    fetch_intraday()

    # Sentiment sources (may fail in restricted networks, that's ok)
    try:
        from fetch_news import fetch_news
        fetch_news()
    except Exception as e:
        print(f"  News fetch skipped: {e}")

    try:
        from fetch_reddit import fetch_reddit
        fetch_reddit()
    except Exception as e:
        print(f"  Reddit fetch skipped: {e}")


def step_daily_rules():
    """Run daily-level association rule mining."""
    print("\n" + "=" * 70)
    print("STEP 2: DAILY PRICE ASSOCIATION RULES")
    print("=" * 70)

    import numpy as np
    import pandas as pd
    import yfinance as yf
    import itertools

    TICKER = "GRRR"
    MIN_SUPPORT = 3
    MIN_CONFIDENCE = 0.60
    MIN_LIFT = 1.10
    MAX_SIZE = 4
    RULES_FILE = os.path.join(DATA_DIR, "grrr_association_rules.csv")

    print("  Fetching 6-month daily data for deep lookback...")
    ticker = yf.Ticker(TICKER)
    df = ticker.history(period="6mo", interval="1d")
    if df.empty:
        print("  ERROR: No daily data.")
        return

    print(f"  {len(df)} daily bars")

    # === Add indicators ===
    df["Daily_Return"] = (df["Close"] - df["Open"]) / df["Open"] * 100
    df["Prev_Return"] = df["Daily_Return"].shift(1)
    df["Gap"] = (df["Open"] - df["Close"].shift(1)) / df["Close"].shift(1) * 100
    df["Range_Pct"] = (df["High"] - df["Low"]) / df["Low"] * 100

    # Volume
    df["Vol_SMA_20"] = df["Volume"].rolling(20).mean()
    df["Rel_Vol"] = df["Volume"] / df["Vol_SMA_20"]

    # RSI
    delta = df["Close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(com=13, min_periods=14).mean()
    avg_loss = loss.ewm(com=13, min_periods=14).mean()
    df["RSI"] = 100 - (100 / (1 + avg_gain / avg_loss))

    # MACD
    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # EMAs
    df["EMA_9"] = df["Close"].ewm(span=9, adjust=False).mean()
    df["EMA_21"] = df["Close"].ewm(span=21, adjust=False).mean()

    # SMAs
    df["SMA_5"] = df["Close"].rolling(5).mean()
    df["SMA_10"] = df["Close"].rolling(10).mean()
    df["SMA_20"] = df["Close"].rolling(20).mean()

    # Bollinger
    std20 = df["Close"].rolling(20).std()
    df["BB_Upper"] = df["SMA_20"] + 2 * std20
    df["BB_Lower"] = df["SMA_20"] - 2 * std20
    df["BB_Width"] = (df["BB_Upper"] - df["BB_Lower"]) / df["SMA_20"] * 100
    df["BB_Pos"] = (df["Close"] - df["BB_Lower"]) / (df["BB_Upper"] - df["BB_Lower"])

    # ATR
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"] - df["Close"].shift()).abs()
    ], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    df["ATR_Pct"] = df["ATR"] / df["Close"] * 100

    # OBV trend
    obv = [0]
    for i in range(1, len(df)):
        if df["Close"].iloc[i] > df["Close"].iloc[i-1]:
            obv.append(obv[-1] + df["Volume"].iloc[i])
        elif df["Close"].iloc[i] < df["Close"].iloc[i-1]:
            obv.append(obv[-1] - df["Volume"].iloc[i])
        else:
            obv.append(obv[-1])
    df["OBV"] = obv
    df["OBV_Trend"] = np.where(
        pd.Series(obv).rolling(5).mean() > pd.Series(obv).rolling(10).mean(),
        1, 0
    )

    # VWAP
    df["VWAP"] = ((df["High"] + df["Low"] + df["Close"]) / 3 * df["Volume"]).cumsum() / df["Volume"].cumsum()
    df["VWAP_Dist"] = (df["Close"] - df["VWAP"]) / df["VWAP"] * 100

    # Streak
    streak = []
    current = 0
    for i in range(len(df)):
        if df["Daily_Return"].iloc[i] > 0:
            current = current + 1 if current > 0 else 1
        else:
            current = current - 1 if current < 0 else -1
        streak.append(current)
    df["Streak"] = streak

    # === Discretize ===
    cat = pd.DataFrame(index=df.index)
    cat["DOW"] = df.index.dayofweek.map({0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"})
    cat["Day_Dir"] = pd.cut(df["Daily_Return"], bins=[-999,-3,-1,0,1,3,999],
                            labels=["big_down","mod_down","slight_down","slight_up","mod_up","big_up"])
    cat["Gap"] = pd.cut(df["Gap"], bins=[-999,-2,-0.5,0.5,2,999],
                        labels=["gap_down_big","gap_down","flat","gap_up","gap_up_big"])
    cat["Range"] = pd.cut(df["Range_Pct"], bins=[0,3,5,8,999],
                          labels=["tight","normal","wide","very_wide"])
    cat["Volume"] = pd.cut(df["Rel_Vol"], bins=[0,0.5,0.8,1.2,2,999],
                           labels=["very_low","low","normal","high","very_high"])
    cat["RSI"] = pd.cut(df["RSI"], bins=[0,30,40,50,60,70,100],
                        labels=["oversold","weak","neutral_low","neutral_high","strong","overbought"])
    macd_std = df["MACD_Hist"].std()
    cat["MACD_Hist"] = pd.cut(df["MACD_Hist"], bins=[-999,-macd_std,-macd_std/3,macd_std/3,macd_std,999],
                              labels=["strong_bear","bear","neutral","bull","strong_bull"])
    cat["MACD_Cross"] = np.where(df["MACD"] > df["MACD_Signal"], "above", "below")
    ema_diff = (df["EMA_9"] - df["EMA_21"]) / df["EMA_21"] * 100
    cat["EMA_Trend"] = pd.cut(ema_diff, bins=[-999,-3,-1,0,1,3,999],
                              labels=["strong_bear","bear","slight_bear","slight_bull","bull","strong_bull"])
    cat["BB_Position"] = pd.cut(df["BB_Pos"], bins=[-999,0,0.25,0.5,0.75,1,999],
                                labels=["below_lower","lower_quarter","lower_mid","upper_mid","upper_quarter","above_upper"])
    cat["BB_Squeeze"] = pd.cut(df["BB_Width"], bins=[0,15,25,35,999],
                               labels=["tight_squeeze","normal","wide","very_wide"])
    cat["SMA_Align"] = np.where(
        (df["SMA_5"] > df["SMA_10"]) & (df["SMA_10"] > df["SMA_20"]), "bullish",
        np.where((df["SMA_5"] < df["SMA_10"]) & (df["SMA_10"] < df["SMA_20"]), "bearish", "mixed"))
    cat["OBV_Trend"] = np.where(df["OBV_Trend"] == 1, "accumulating", "distributing")
    cat["VWAP_Pos"] = pd.cut(df["VWAP_Dist"], bins=[-999,-5,-1,1,5,999],
                             labels=["far_below","below","near","above","far_above"])
    cat["ATR_Regime"] = pd.cut(df["ATR_Pct"], bins=[0,4,6,8,999],
                               labels=["low_vol","normal_vol","high_vol","extreme_vol"])
    cat["Streak"] = pd.cut(df["Streak"], bins=[-999,-3,-2,-1,0,1,2,3,999],
                           labels=["long_down","3_down","2_down","1_down","1_up","2_up","3_up","long_up"])
    cat["Prev_Change"] = pd.cut(df["Prev_Return"], bins=[-999,-4,-2,-0.5,0.5,2,4,999],
                                labels=["crash","big_drop","drop","flat","gain","big_gain","surge"])

    # === Outcomes ===
    outcomes = pd.DataFrame(index=df.index)
    next_ret = df["Daily_Return"].shift(-1)
    outcomes["NEXT_UpDown"] = np.where(next_ret > 0, "NEXT_UP", "NEXT_DOWN")
    outcomes.loc[next_ret.isna(), "NEXT_UpDown"] = np.nan
    outcomes["NEXT_Dir"] = pd.cut(next_ret, bins=[-999,-3,-1,0,1,3,999],
                                  labels=["next_big_down","next_mod_down","next_slight_down",
                                          "next_slight_up","next_mod_up","next_big_up"])

    # Trim to last 60 days for relevance
    cat = cat.tail(60)
    outcomes = outcomes.tail(60)

    # === Mine ===
    print("  Mining daily rules...")
    rules = _mine(cat, outcomes, MIN_SUPPORT, MIN_CONFIDENCE, MIN_LIFT, MAX_SIZE)
    print(f"  {len(rules)} rules found")

    if rules:
        pd.DataFrame(rules).to_csv(RULES_FILE, index=False)
        print(f"  Saved to {RULES_FILE}")


def step_intraday_rules():
    """Run intraday 5-min association rule mining."""
    print("\n" + "=" * 70)
    print("STEP 3: INTRADAY 5-MIN ASSOCIATION RULES")
    print("=" * 70)

    # Delegate to existing script
    from association_rules_intraday import run_intraday_apriori
    run_intraday_apriori()


def step_sentiment_rules():
    """Run sentiment-to-price association rule mining."""
    print("\n" + "=" * 70)
    print("STEP 4: SENTIMENT → PRICE ASSOCIATION RULES")
    print("=" * 70)

    from association_rules_sentiment import run
    run()


def step_master_report():
    """Combine all rules and generate the master ranked report."""
    print("\n" + "=" * 70)
    print("STEP 5: GENERATING MASTER RULES REPORT")
    print("=" * 70)

    import pandas as pd

    sources = {
        "DAILY": "grrr_association_rules.csv",
        "INTRADAY_5MIN": "grrr_intraday_rules.csv",
        "NEWS": "grrr_sentiment_price_rules_news.csv",
        "REDDIT": "grrr_sentiment_price_rules_reddit.csv",
    }

    all_rules = []
    for name, filename in sources.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df["source"] = name
            all_rules.append(df)
            print(f"  {name}: {len(df)} rules")

    if not all_rules:
        print("  No rules found. Run compute first.")
        return

    combined = pd.concat(all_rules, ignore_index=True)
    combined.to_csv(os.path.join(DATA_DIR, "grrr_all_rules_combined.csv"), index=False)
    print(f"\n  TOTAL: {len(combined):,} rules")

    # === Build report ===
    r = []
    r.append("=" * 85)
    r.append("  GRRR MASTER ASSOCIATION RULES REPORT")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Total rules: {len(combined):,}")
    r.append("=" * 85)

    # --- TIER S: 100% confidence, count >= 10 ---
    tier_s = combined[(combined["confidence"] >= 0.99) & (combined["count"] >= 10)]
    tier_s = tier_s.sort_values("count", ascending=False).drop_duplicates(
        subset=["consequent", "count"], keep="first")
    r.append(f"\n{'='*85}")
    r.append(f"  TIER S: PERFECT RULES (100% confidence, 10+ samples) [{len(tier_s)}]")
    r.append("=" * 85)
    _append_rules(r, tier_s.head(20))

    # --- TIER A: >= 80%, count >= 7 ---
    tier_a = combined[(combined["confidence"] >= 0.80) & (combined["count"] >= 7) & (combined["confidence"] < 0.99)]
    tier_a = tier_a.sort_values(["count", "confidence"], ascending=[False, False])
    tier_a = tier_a.drop_duplicates(subset=["consequent", "count"], keep="first")
    r.append(f"\n{'='*85}")
    r.append(f"  TIER A: NEAR-PERFECT (80-99% confidence, 7+ samples) [{len(tier_a)}]")
    r.append("=" * 85)
    _append_rules(r, tier_a.head(25))

    # --- TIER B: >= 65%, count >= 20 (high volume rules) ---
    tier_b = combined[(combined["confidence"] >= 0.65) & (combined["count"] >= 20)]
    tier_b = tier_b.sort_values(["count", "confidence"], ascending=[False, False])
    tier_b = tier_b.drop_duplicates(subset=["consequent", "count"], keep="first")
    r.append(f"\n{'='*85}")
    r.append(f"  TIER B: HIGH-VOLUME RULES (65%+ confidence, 20+ samples) [{len(tier_b)}]")
    r.append("=" * 85)
    _append_rules(r, tier_b.head(25))

    # --- SIMPLEST RULES: 1 condition only ---
    if "size" in combined.columns:
        simple = combined[combined["size"] == 1]
    elif "antecedent_size" in combined.columns:
        simple = combined[combined["antecedent_size"] == 1]
    else:
        simple = combined[~combined["antecedent"].str.contains(" & ", na=False)]

    simple = simple[(simple["count"] >= 5) & (simple["confidence"] >= 0.58)]
    simple = simple.sort_values(["count", "confidence"], ascending=[False, False])
    r.append(f"\n{'='*85}")
    r.append(f"  SIMPLEST RULES (1 condition, 5+ samples, 58%+ conf) [{len(simple)}]")
    r.append("=" * 85)
    _append_rules(r, simple.head(30))

    # --- DIRECTION RULES: UP/DOWN only ---
    dir_mask = combined["consequent"].str.contains("UP|DOWN|up|down", na=False)
    dir_rules = combined[dir_mask & (combined["count"] >= 5) & (combined["confidence"] >= 0.65)]
    dir_rules = dir_rules.sort_values(["count", "confidence"], ascending=[False, False])

    for direction, label in [("UP", "BUY"), ("DOWN", "SELL")]:
        subset = dir_rules[dir_rules["consequent"].str.contains(direction)]
        subset = subset.drop_duplicates(subset=["consequent", "count"], keep="first")
        r.append(f"\n{'='*85}")
        r.append(f"  BEST {label} SIGNALS ({direction}, 65%+ conf, 5+ samples) [{len(subset)}]")
        r.append("=" * 85)
        _append_rules(r, subset.head(20))

    r.append(f"\n{'='*85}")
    r.append(f"  Last computed: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Run: python3 compute_rules.py")
    r.append("=" * 85)

    report = "\n".join(r)
    with open(MASTER_REPORT, "w") as f:
        f.write(report)
    print(f"\n  Saved master report to {MASTER_REPORT}")
    print(f"\n{report}")


def _append_rules(lines, df, max_show=30):
    lines.append(f"\n  {'Src':<12} {'Conf':>5} {'Lift':>5} {'Hits':>9}  Rule")
    lines.append(f"  {'-'*75}")
    for i, (_, r) in enumerate(df.iterrows()):
        if i >= max_show:
            break
        ant = str(r['antecedent'])[:48]
        lines.append(
            f"  {r['source']:<12} {r['confidence']*100:>4.0f}% {r['lift']:>5.2f} "
            f"{r['count']:>4}/{int(r['antecedent_count']):<4}  IF {ant}"
        )
        lines.append(f"  {'':>36}→ {r['consequent']}")


def _mine(ant_df, con_df, min_sup, min_conf, min_lift, max_size):
    """Generic Apriori miner."""
    import itertools
    rules = []
    ant_cols = list(ant_df.columns)
    con_cols = list(con_df.columns)
    total_n = len(ant_df)

    con_masks = {}
    for col in con_cols:
        for val in con_df[col].dropna().unique():
            mask = con_df[col] == val
            count = mask.sum()
            if count >= min_sup:
                con_masks[(col, str(val))] = mask

    for size in range(1, min(max_size + 1, len(ant_cols) + 1)):
        combos = list(itertools.combinations(ant_cols, size))
        for combo in combos:
            sub = ant_df[list(combo)].dropna()
            if len(sub) < min_sup:
                continue
            groups = sub.groupby(list(combo))
            for group_key, group_idx in groups:
                if not isinstance(group_key, tuple):
                    group_key = (group_key,)
                ant_mask = ant_df.index.isin(group_idx.index)
                ant_count = ant_mask.sum()
                if ant_count < min_sup:
                    continue
                ant_desc = " & ".join(f"{c}={v}" for c, v in zip(combo, group_key))
                for (con_col, con_val), con_mask in con_masks.items():
                    both_count = (ant_mask & con_mask).sum()
                    if both_count < min_sup:
                        continue
                    confidence = both_count / ant_count
                    expected = con_mask.sum() / total_n
                    lift = confidence / expected if expected > 0 else 0
                    if confidence >= min_conf and lift >= min_lift:
                        rules.append({
                            "antecedent": ant_desc,
                            "consequent": f"{con_col}={con_val}",
                            "support": round(both_count / total_n, 4),
                            "confidence": round(confidence, 4),
                            "lift": round(lift, 4),
                            "count": both_count,
                            "antecedent_count": ant_count,
                            "antecedent_size": size,
                        })
    return rules


def main():
    parser = argparse.ArgumentParser(description="GRRR Master Association Rule Miner")
    parser.add_argument("--daily", action="store_true", help="Run daily rules only")
    parser.add_argument("--intraday", action="store_true", help="Run intraday 5-min rules only")
    parser.add_argument("--sentiment", action="store_true", help="Run sentiment rules only")
    parser.add_argument("--report", action="store_true", help="Just regenerate the master report")
    parser.add_argument("--no-fetch", action="store_true", help="Skip data refresh")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    start = time.time()

    print("=" * 70)
    print(f"  GRRR ASSOCIATION RULE MINER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    run_all = not (args.daily or args.intraday or args.sentiment or args.report)

    if not args.no_fetch and not args.report:
        step_fetch_data()

    if run_all or args.daily:
        step_daily_rules()

    if run_all or args.intraday:
        step_intraday_rules()

    if run_all or args.sentiment:
        step_sentiment_rules()

    # Always generate master report
    step_master_report()

    elapsed = time.time() - start
    print(f"\n  Done in {elapsed/60:.1f} minutes.")


if __name__ == "__main__":
    main()
