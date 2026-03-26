#!/usr/bin/env python3
"""
GRRR Association Rule Miner (Apriori-style).

Discretizes all continuous trading features into meaningful categorical bins,
then mines association rules to find patterns like:
  {prev_day=DOWN, DOW=Thursday, RSI=oversold} → {next_day=UP, spike>3%}
  with confidence 78%, support 12%, lift 1.6

Exhaustively tests all combinations of antecedent features against
outcome features (next-day direction, magnitude, first-2h behavior).

Outputs:
  - grrr_association_rules.csv:     All rules above min thresholds
  - grrr_rules_report.txt:          Human-readable top rules
"""

import os
import sys
import itertools
from datetime import datetime
from collections import Counter

import numpy as np
import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
RULES_FILE = os.path.join(DATA_DIR, "grrr_association_rules.csv")
RULES_REPORT = os.path.join(DATA_DIR, "grrr_rules_report.txt")

# Minimum thresholds
MIN_SUPPORT_COUNT = 3      # At least 3 occurrences
MIN_CONFIDENCE = 0.55      # At least 55% confidence
MIN_LIFT = 1.05            # Lift > 1 means better than random
MAX_ANTECEDENT_SIZE = 4    # Max features in the "IF" side


def discretize_daily(df):
    """
    Convert all continuous daily features into discrete categorical bins.
    Returns a DataFrame with all categorical columns.
    """
    cat = pd.DataFrame(index=df.index)

    # === Day of week ===
    cat["DOW"] = pd.to_datetime(df.index).dayofweek.map({
        0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"
    })

    # === Current day direction (open to close) ===
    daily_ret = (df["Close"] - df["Open"]) / df["Open"] * 100
    cat["Day_Dir"] = pd.cut(daily_ret, bins=[-999, -3, -1, 0, 1, 3, 999],
                            labels=["big_down", "mod_down", "slight_down",
                                    "slight_up", "mod_up", "big_up"])

    # === Gap from previous close ===
    if "Prev_Close" in df.columns:
        gap = (df["Open"] - df["Prev_Close"]) / df["Prev_Close"] * 100
        cat["Gap"] = pd.cut(gap, bins=[-999, -2, -0.5, 0.5, 2, 999],
                            labels=["gap_down_big", "gap_down", "flat", "gap_up", "gap_up_big"])

    # === Daily range (volatility) ===
    if "Range_Pct" in df.columns:
        cat["Range"] = pd.cut(df["Range_Pct"], bins=[0, 3, 5, 8, 999],
                              labels=["tight", "normal", "wide", "very_wide"])

    # === Volume relative ===
    if "Rel_Volume" in df.columns:
        cat["Volume"] = pd.cut(df["Rel_Volume"], bins=[0, 0.5, 0.8, 1.2, 2, 999],
                               labels=["very_low", "low", "normal", "high", "very_high"])

    # === RSI ===
    if "RSI_14" in df.columns:
        cat["RSI"] = pd.cut(df["RSI_14"], bins=[0, 30, 40, 50, 60, 70, 100],
                            labels=["oversold", "weak", "neutral_low", "neutral_high", "strong", "overbought"])

    # === MACD ===
    if "MACD_Hist" in df.columns:
        cat["MACD_Hist"] = pd.cut(df["MACD_Hist"], bins=[-999, -0.1, -0.02, 0.02, 0.1, 999],
                                  labels=["strong_bear", "bear", "neutral", "bull", "strong_bull"])

    # === MACD crossover ===
    if "MACD" in df.columns and "MACD_Signal" in df.columns:
        macd_above = (df["MACD"] > df["MACD_Signal"]).astype(bool)
        prev_macd_above = macd_above.shift(1).fillna(False).astype(bool)
        cat["MACD_Cross"] = "none"
        cat.loc[macd_above & ~prev_macd_above, "MACD_Cross"] = "bull_cross"
        cat.loc[~macd_above & prev_macd_above, "MACD_Cross"] = "bear_cross"
        cat.loc[macd_above & prev_macd_above, "MACD_Cross"] = "above"
        cat.loc[~macd_above & ~prev_macd_above, "MACD_Cross"] = "below"

    # === Bollinger Band position ===
    if "BB_Upper" in df.columns and "BB_Lower" in df.columns:
        bb_pos = (df["Close"] - df["BB_Lower"]) / (df["BB_Upper"] - df["BB_Lower"])
        cat["BB_Position"] = pd.cut(bb_pos, bins=[-999, 0, 0.25, 0.5, 0.75, 1, 999],
                                    labels=["below_lower", "lower_quarter", "lower_mid",
                                            "upper_mid", "upper_quarter", "above_upper"])

    # === BB Width (squeeze) ===
    if "BB_Width" in df.columns:
        cat["BB_Squeeze"] = pd.cut(df["BB_Width"], bins=[0, 15, 25, 35, 999],
                                   labels=["tight_squeeze", "normal", "wide", "very_wide"])

    # === EMA trend ===
    if "EMA_9" in df.columns and "EMA_21" in df.columns:
        ema_diff_pct = (df["EMA_9"] - df["EMA_21"]) / df["EMA_21"] * 100
        cat["EMA_Trend"] = pd.cut(ema_diff_pct, bins=[-999, -3, -1, 0, 1, 3, 999],
                                  labels=["strong_bear", "bear", "slight_bear",
                                          "slight_bull", "bull", "strong_bull"])

    # === SMA alignment ===
    if all(c in df.columns for c in ["SMA_5", "SMA_10", "SMA_20"]):
        cat["SMA_Align"] = "mixed"
        bull_align = (df["SMA_5"] > df["SMA_10"]) & (df["SMA_10"] > df["SMA_20"])
        bear_align = (df["SMA_5"] < df["SMA_10"]) & (df["SMA_10"] < df["SMA_20"])
        cat.loc[bull_align, "SMA_Align"] = "bullish"
        cat.loc[bear_align, "SMA_Align"] = "bearish"

    # === Price vs VWAP ===
    if "VWAP" in df.columns:
        vwap_dist = (df["Close"] - df["VWAP"]) / df["VWAP"] * 100
        cat["VWAP_Pos"] = pd.cut(vwap_dist, bins=[-999, -5, -2, 0, 2, 5, 999],
                                 labels=["far_below", "below", "slight_below",
                                         "slight_above", "above", "far_above"])

    # === ATR (volatility regime) ===
    if "ATR_14" in df.columns:
        atr_pct = df["ATR_14"] / df["Close"] * 100
        cat["ATR_Regime"] = pd.cut(atr_pct, bins=[0, 4, 6, 8, 999],
                                   labels=["low_vol", "normal_vol", "high_vol", "extreme_vol"])

    # === OBV trend (5-day) ===
    if "OBV" in df.columns:
        obv_change = df["OBV"].diff(5)
        cat["OBV_Trend"] = "flat"
        cat.loc[obv_change > 0, "OBV_Trend"] = "accumulating"
        cat.loc[obv_change < 0, "OBV_Trend"] = "distributing"

    # === Consecutive days ===
    directions = (df["Close"] > df["Open"]).astype(int)
    streaks = []
    current_streak = 0
    current_dir = None
    for d in directions:
        if d == current_dir:
            current_streak += 1
        else:
            current_streak = 1
            current_dir = d
        streaks.append(current_streak * (1 if d == 1 else -1))
    cat["Streak"] = pd.cut(streaks, bins=[-999, -3, -2, -1, 0, 1, 2, 3, 999],
                           labels=["long_down_streak", "3_down", "2_down", "1_down",
                                   "1_up", "2_up", "3_up", "long_up_streak"])

    # === Price change magnitude (close to close) ===
    if "Change_Pct" in df.columns:
        cat["Prev_Change"] = pd.cut(df["Change_Pct"], bins=[-999, -4, -2, -0.5, 0.5, 2, 4, 999],
                                    labels=["crash", "big_drop", "drop", "flat",
                                            "gain", "big_gain", "surge"])

    return cat


def build_outcome_features(df):
    """
    Build NEXT-DAY outcome features (what we want to predict).
    These go on the RIGHT side of the rule (consequent).
    """
    outcomes = pd.DataFrame(index=df.index)

    daily_ret = (df["Close"] - df["Open"]) / df["Open"] * 100

    # Next day direction
    next_ret = daily_ret.shift(-1)
    outcomes["NEXT_Dir"] = pd.cut(next_ret, bins=[-999, -3, -1, 0, 1, 3, 999],
                                  labels=["next_big_down", "next_mod_down", "next_slight_down",
                                          "next_slight_up", "next_mod_up", "next_big_up"])

    # Next day simple UP/DOWN
    outcomes["NEXT_UpDown"] = np.where(next_ret > 0, "NEXT_UP", "NEXT_DOWN")
    outcomes.loc[next_ret.isna(), "NEXT_UpDown"] = np.nan

    # Next day close-to-close return
    next_cc = df["Close"].pct_change().shift(-1) * 100
    outcomes["NEXT_CC"] = pd.cut(next_cc, bins=[-999, -3, -1, 0, 1, 3, 999],
                                 labels=["next_cc_crash", "next_cc_drop", "next_cc_slight_drop",
                                         "next_cc_slight_gain", "next_cc_gain", "next_cc_surge"])

    # Next day range (volatile or tight)
    if "Range_Pct" in df.columns:
        next_range = df["Range_Pct"].shift(-1)
        outcomes["NEXT_Range"] = pd.cut(next_range, bins=[0, 3, 5, 8, 999],
                                        labels=["next_tight", "next_normal", "next_wide", "next_very_wide"])

    # Next day volume
    if "Rel_Volume" in df.columns:
        next_vol = df["Rel_Volume"].shift(-1)
        outcomes["NEXT_Volume"] = pd.cut(next_vol, bins=[0, 0.6, 1, 1.5, 999],
                                         labels=["next_low_vol", "next_normal_vol",
                                                 "next_high_vol", "next_very_high_vol"])

    return outcomes


def build_hourly_outcomes(daily_df):
    """
    Build first-2-hour outcome features from hourly data if available.
    """
    hourly_path = os.path.join(DATA_DIR, "grrr_hourly_60d.csv")
    if not os.path.exists(hourly_path):
        return pd.DataFrame(index=daily_df.index)

    df_1h = pd.read_csv(hourly_path, index_col=0)
    df_1h.index = pd.to_datetime(df_1h.index, utc=True)
    days = df_1h.groupby(df_1h.index.date)

    intraday_data = {}
    for date, day_df in days:
        if len(day_df) < 5:
            continue
        day_open = day_df["Open"].iloc[0]
        first_2h = day_df.iloc[:2]
        first_2h_high = first_2h["High"].max()
        first_2h_close = first_2h["Close"].iloc[-1]
        day_close = day_df["Close"].iloc[-1]

        first_2h_spike = (first_2h_high - day_open) / day_open * 100
        first_2h_ret = (first_2h_close - day_open) / day_open * 100
        rest_ret = (day_close - first_2h_close) / first_2h_close * 100 if first_2h_close > 0 else 0

        # High of day timing
        all_highs = list(day_df["High"])
        hod_bar = all_highs.index(max(all_highs))

        date_str = str(date)
        intraday_data[date_str] = {
            "first_2h_spike": first_2h_spike,
            "first_2h_ret": first_2h_ret,
            "rest_ret": rest_ret,
            "hod_bar": hod_bar,
        }

    outcomes = pd.DataFrame(index=daily_df.index)

    # Map daily index to next day's intraday data
    dates = list(daily_df.index)
    for i in range(len(dates) - 1):
        next_date = dates[i + 1]
        if next_date in intraday_data:
            data = intraday_data[next_date]
            outcomes.loc[dates[i], "NEXT_Spike_Raw"] = data["first_2h_spike"]
            outcomes.loc[dates[i], "NEXT_Rest_Raw"] = data["rest_ret"]
            outcomes.loc[dates[i], "NEXT_HOD_Bar"] = data["hod_bar"]

    # Discretize
    if "NEXT_Spike_Raw" in outcomes.columns:
        outcomes["NEXT_1st2h_Spike"] = pd.cut(
            outcomes["NEXT_Spike_Raw"],
            bins=[-999, 1, 2, 3, 5, 999],
            labels=["next_no_spike", "next_small_spike", "next_med_spike",
                    "next_big_spike", "next_huge_spike"]
        )
        outcomes["NEXT_Fade"] = "next_no_fade"
        outcomes.loc[outcomes["NEXT_Rest_Raw"] < -1, "NEXT_Fade"] = "next_faded"
        outcomes.loc[outcomes["NEXT_Rest_Raw"] < -2, "NEXT_Fade"] = "next_big_fade"

        outcomes["NEXT_HOD_Timing"] = pd.cut(
            outcomes["NEXT_HOD_Bar"],
            bins=[-1, 1, 3, 5, 999],
            labels=["next_hod_first_hour", "next_hod_second_hour",
                    "next_hod_midday", "next_hod_afternoon"]
        )

        outcomes = outcomes.drop(columns=["NEXT_Spike_Raw", "NEXT_Rest_Raw", "NEXT_HOD_Bar"], errors="ignore")

    return outcomes


def mine_rules(antecedent_df, consequent_df, total_n):
    """
    Mine all association rules: antecedent → consequent.
    Tests all combinations of antecedent columns (1 to MAX_ANTECEDENT_SIZE).
    """
    rules = []
    ant_cols = list(antecedent_df.columns)
    con_cols = list(consequent_df.columns)

    # Precompute: for each consequent column+value, which rows match
    con_masks = {}
    for col in con_cols:
        for val in consequent_df[col].dropna().unique():
            mask = consequent_df[col] == val
            count = mask.sum()
            if count >= MIN_SUPPORT_COUNT:
                con_masks[(col, str(val))] = mask

    total_rules_checked = 0
    print(f"  Antecedent columns: {len(ant_cols)}")
    print(f"  Consequent values: {len(con_masks)}")

    for size in range(1, min(MAX_ANTECEDENT_SIZE + 1, len(ant_cols) + 1)):
        combos = list(itertools.combinations(ant_cols, size))
        print(f"  Testing {len(combos)} combinations of size {size}...", end=" ", flush=True)
        rules_found = 0

        for combo in combos:
            # Get unique value combinations for this set of columns
            sub = antecedent_df[list(combo)].copy()
            sub = sub.dropna()

            if len(sub) < MIN_SUPPORT_COUNT:
                continue

            # Group by all values in the combo
            groups = sub.groupby(list(combo))

            for group_key, group_idx in groups:
                if not isinstance(group_key, tuple):
                    group_key = (group_key,)

                ant_mask = antecedent_df.index.isin(group_idx.index)
                ant_count = ant_mask.sum()

                if ant_count < MIN_SUPPORT_COUNT:
                    continue

                # Build antecedent description
                ant_desc = " & ".join(f"{c}={v}" for c, v in zip(combo, group_key))

                # Test against each consequent
                for (con_col, con_val), con_mask in con_masks.items():
                    total_rules_checked += 1

                    # Both antecedent AND consequent true
                    both = ant_mask & con_mask
                    both_count = both.sum()

                    if both_count < MIN_SUPPORT_COUNT:
                        continue

                    support = both_count / total_n
                    confidence = both_count / ant_count
                    expected = con_mask.sum() / total_n
                    lift = confidence / expected if expected > 0 else 0

                    if confidence >= MIN_CONFIDENCE and lift >= MIN_LIFT:
                        rules.append({
                            "antecedent": ant_desc,
                            "consequent": f"{con_col}={con_val}",
                            "support": round(support, 4),
                            "confidence": round(confidence, 4),
                            "lift": round(lift, 4),
                            "count": both_count,
                            "antecedent_count": ant_count,
                            "consequent_count": int(con_mask.sum()),
                            "total_n": total_n,
                            "antecedent_size": size,
                        })
                        rules_found += 1

        print(f"{rules_found} rules found")

    print(f"\n  Total combinations checked: {total_rules_checked:,}")
    return rules


def run_apriori():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Running Association Rule Mining for {TICKER}...")

    # Load daily data
    daily_path = os.path.join(DATA_DIR, "grrr_daily_30d.csv")
    if not os.path.exists(daily_path):
        print("  ERROR: No daily data found. Run fetch_daily.py first.")
        return False

    df = pd.read_csv(daily_path, index_col=0)
    print(f"  Loaded {len(df)} trading days")

    # === Step 1: Discretize all features ===
    print("\n  Step 1: Discretizing features...")
    antecedents = discretize_daily(df)
    print(f"  Created {len(antecedents.columns)} antecedent features: {list(antecedents.columns)}")

    # Show bin distributions
    for col in antecedents.columns:
        dist = antecedents[col].value_counts()
        print(f"    {col}: {dict(dist)}")

    # === Step 2: Build outcome features ===
    print("\n  Step 2: Building outcome features...")
    outcomes_daily = build_outcome_features(df)
    outcomes_intraday = build_hourly_outcomes(df)

    # Merge outcomes
    outcomes = pd.concat([outcomes_daily, outcomes_intraday], axis=1)
    # Drop rows where ALL outcomes are NaN (last row has no next day)
    valid_mask = outcomes.notna().any(axis=1)
    outcomes = outcomes[valid_mask]
    antecedents = antecedents.loc[outcomes.index]

    print(f"  Created {len(outcomes.columns)} outcome features: {list(outcomes.columns)}")
    for col in outcomes.columns:
        dist = outcomes[col].value_counts()
        print(f"    {col}: {dict(dist)}")

    total_n = len(antecedents)
    print(f"\n  Valid samples: {total_n}")

    # === Step 3: Mine rules ===
    print(f"\n  Step 3: Mining association rules...")
    print(f"  Min support count: {MIN_SUPPORT_COUNT}")
    print(f"  Min confidence: {MIN_CONFIDENCE}")
    print(f"  Min lift: {MIN_LIFT}")
    print(f"  Max antecedent size: {MAX_ANTECEDENT_SIZE}")
    print()

    rules = mine_rules(antecedents, outcomes, total_n)
    print(f"\n  Total rules found: {len(rules)}")

    if not rules:
        print("  No rules met the minimum thresholds.")
        pd.DataFrame().to_csv(RULES_FILE, index=False)
        _write_report([], total_n, antecedents.columns, outcomes.columns)
        return True

    # Sort by confidence * lift (best rules first)
    rules.sort(key=lambda r: r["confidence"] * r["lift"], reverse=True)

    # Save all rules
    df_rules = pd.DataFrame(rules)
    df_rules.to_csv(RULES_FILE, index=False)
    print(f"  Saved {len(rules)} rules to {RULES_FILE}")

    # Generate report
    _write_report(rules, total_n, antecedents.columns, outcomes.columns)
    return True


def _write_report(rules, total_n, ant_cols, out_cols):
    r = []
    r.append("=" * 75)
    r.append("  GRRR ASSOCIATION RULE MINING REPORT")
    r.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    r.append(f"  Sample size: {total_n} trading days")
    r.append("=" * 75)
    r.append("")
    r.append("METHODOLOGY:")
    r.append(f"  Antecedent features: {len(ant_cols)}")
    r.append(f"  Outcome features:    {len(out_cols)}")
    r.append(f"  Min support count:   {MIN_SUPPORT_COUNT}")
    r.append(f"  Min confidence:      {MIN_CONFIDENCE*100:.0f}%")
    r.append(f"  Min lift:            {MIN_LIFT}")
    r.append(f"  Max antecedent size: {MAX_ANTECEDENT_SIZE}")
    r.append(f"  Total rules found:   {len(rules)}")
    r.append("")

    if not rules:
        r.append("  No significant rules found with current thresholds.")
        r.append("")
    else:
        # === Top rules by confidence ===
        r.append("TOP 30 RULES BY CONFIDENCE x LIFT:")
        r.append("-" * 75)
        r.append(f"{'#':>3} {'Conf':>6} {'Lift':>5} {'Cnt':>4} {'Sup':>5}  Rule")
        r.append("-" * 75)
        for i, rule in enumerate(rules[:30]):
            r.append(
                f"{i+1:>3} {rule['confidence']*100:>5.1f}% {rule['lift']:>5.2f} "
                f"{rule['count']:>4} {rule['support']*100:>4.1f}%  "
                f"IF {rule['antecedent']} → {rule['consequent']}"
            )
        r.append("")

        # === Rules grouped by outcome ===
        outcome_groups = {}
        for rule in rules:
            con = rule["consequent"].split("=")[0]
            if con not in outcome_groups:
                outcome_groups[con] = []
            outcome_groups[con].append(rule)

        # Focus on NEXT_UpDown (most actionable)
        for outcome_name in ["NEXT_UpDown", "NEXT_Dir", "NEXT_1st2h_Spike", "NEXT_Fade"]:
            if outcome_name in outcome_groups:
                r.append(f"\nBEST RULES FOR {outcome_name}:")
                r.append("-" * 75)
                group = sorted(outcome_groups[outcome_name],
                              key=lambda x: x["confidence"] * x["lift"], reverse=True)
                for i, rule in enumerate(group[:15]):
                    r.append(
                        f"  [{rule['confidence']*100:.0f}% conf, {rule['lift']:.2f}x lift, "
                        f"n={rule['count']}/{rule['antecedent_count']}]"
                    )
                    r.append(f"    IF {rule['antecedent']}")
                    r.append(f"    → {rule['consequent']}")
                    r.append("")

        # === Actionable trading rules (NEXT_UP with high confidence) ===
        buy_rules = [r for r in rules if "NEXT_UP" in r["consequent"]
                     and r["confidence"] >= 0.65 and r["count"] >= 3]
        sell_rules = [r for r in rules if "NEXT_DOWN" in r["consequent"]
                      and r["confidence"] >= 0.65 and r["count"] >= 3]

        if buy_rules:
            r.append("\n" + "=" * 75)
            r.append("ACTIONABLE BUY SIGNALS (next day UP, conf >= 65%):")
            r.append("=" * 75)
            for i, rule in enumerate(sorted(buy_rules, key=lambda x: -x["confidence"])[:20]):
                r.append(
                    f"\n  RULE #{i+1}: {rule['confidence']*100:.0f}% confident "
                    f"(hit {rule['count']}/{rule['antecedent_count']} times, "
                    f"lift {rule['lift']:.2f}x)"
                )
                r.append(f"    WHEN: {rule['antecedent']}")
                r.append(f"    THEN: Next day goes UP")

        if sell_rules:
            r.append("\n" + "=" * 75)
            r.append("ACTIONABLE SELL SIGNALS (next day DOWN, conf >= 65%):")
            r.append("=" * 75)
            for i, rule in enumerate(sorted(sell_rules, key=lambda x: -x["confidence"])[:20]):
                r.append(
                    f"\n  RULE #{i+1}: {rule['confidence']*100:.0f}% confident "
                    f"(hit {rule['count']}/{rule['antecedent_count']} times, "
                    f"lift {rule['lift']:.2f}x)"
                )
                r.append(f"    WHEN: {rule['antecedent']}")
                r.append(f"    THEN: Next day goes DOWN")

        # === Spike rules ===
        spike_rules = [r for r in rules if "spike" in r["consequent"].lower()
                       and r["confidence"] >= 0.6]
        if spike_rules:
            r.append("\n" + "=" * 75)
            r.append("FIRST-2-HOUR SPIKE RULES (conf >= 60%):")
            r.append("=" * 75)
            for i, rule in enumerate(sorted(spike_rules, key=lambda x: -x["confidence"])[:15]):
                r.append(
                    f"\n  RULE #{i+1}: {rule['confidence']*100:.0f}% confident "
                    f"(n={rule['count']}, lift {rule['lift']:.2f}x)"
                )
                r.append(f"    WHEN: {rule['antecedent']}")
                r.append(f"    THEN: {rule['consequent']}")

    r.append("\n" + "=" * 75)
    r.append("Methodology: Apriori-style association rule mining")
    r.append("Discretization: Continuous features binned into trading-relevant categories")
    r.append("This is NOT financial advice.")
    r.append("=" * 75)

    report_text = "\n".join(r)
    with open(RULES_REPORT, "w") as f:
        f.write(report_text)
    print(f"  Saved report to {RULES_REPORT}")
    print(f"\n{report_text}")


if __name__ == "__main__":
    success = run_apriori()
    sys.exit(0 if success else 1)
