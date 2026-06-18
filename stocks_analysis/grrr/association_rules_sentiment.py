#!/usr/bin/env python3
"""
GRRR Sentiment-Price Association Rule Miner.

Correlates timestamped news + Reddit sentiment with SUBSEQUENT stock price action.
Handles after-hours/weekend sentiment by mapping to next market session.

Two separate datasets:
  1. News sentiment → price outcomes
  2. Reddit sentiment → price outcomes

Features (antecedents):
  - Polarity bucket (strong_neg, neg, neutral, pos, strong_pos)
  - Subjectivity bucket (factual, mixed, opinionated)
  - Time posted (pre_market, market_hours, after_hours, overnight, weekend)
  - Day of week posted
  - Buzz level (how many articles in last 24h)
  - Sentiment trend (improving, stable, deteriorating over last 48h)
  - Source type (for news: which outlet category)
  - Current price context (RSI, trend, VWAP position at that time)

Outcomes (consequents):
  - Next session gap (gap_up, flat, gap_down)
  - First 30 min move
  - First 1 hour move
  - First 2 hour move
  - Full day move
  - Next 2 day move

Outputs:
  - grrr_sentiment_price_rules_news.csv
  - grrr_sentiment_price_rules_reddit.csv
  - grrr_sentiment_price_report.txt
"""

import os
import sys
import itertools
from datetime import datetime, timedelta, time as dtime

import numpy as np
import pandas as pd
import yfinance as yf

TICKER = "GRRR"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

NEWS_FILE = os.path.join(DATA_DIR, "grrr_news_raw.csv")
REDDIT_FILE = os.path.join(DATA_DIR, "grrr_reddit_posts.csv")

RULES_NEWS = os.path.join(DATA_DIR, "grrr_sentiment_price_rules_news.csv")
RULES_REDDIT = os.path.join(DATA_DIR, "grrr_sentiment_price_rules_reddit.csv")
REPORT_FILE = os.path.join(DATA_DIR, "grrr_sentiment_price_report.txt")

MIN_SUPPORT = 5
MIN_CONFIDENCE = 0.55
MIN_LIFT = 1.10
MAX_ANT_SIZE = 3


def load_price_data():
    """Load hourly + daily price data for outcome matching."""
    print("  Loading price data...")
    ticker = yf.Ticker(TICKER)

    # Daily for gap/full-day outcomes
    daily = ticker.history(period="3mo", interval="1d")
    daily.index = daily.index.tz_localize(None) if daily.index.tz else daily.index
    print(f"    Daily: {len(daily)} bars")

    # Hourly for intraday outcomes
    hourly = ticker.history(period="60d", interval="1h")
    hourly.index = hourly.index.tz_convert("US/Eastern").tz_localize(None) if hourly.index.tz else hourly.index
    print(f"    Hourly: {len(hourly)} bars")

    # 5-min for fine-grained outcomes
    fivemin = ticker.history(period="60d", interval="5m")
    fivemin.index = fivemin.index.tz_convert("US/Eastern").tz_localize(None) if fivemin.index.tz else fivemin.index
    print(f"    5-min: {len(fivemin)} bars")

    return daily, hourly, fivemin


def classify_post_time(dt):
    """Classify when sentiment was posted relative to market hours."""
    if dt is None:
        return "unknown"
    dow = dt.weekday()
    hour = dt.hour

    if dow >= 5:  # Saturday/Sunday
        return "weekend"
    if 4 <= hour < 9:
        return "pre_market"
    elif 9 <= hour < 10:
        return "market_open"
    elif 10 <= hour < 15:
        return "market_hours"
    elif 15 <= hour < 16:
        return "market_close"
    elif 16 <= hour < 20:
        return "after_hours"
    else:
        return "overnight"


def find_next_market_open(dt, daily):
    """Find the next market trading day open after a given datetime."""
    if dt is None:
        return None, None

    # If posted during market hours, "next" is today's remaining session
    # If posted after hours, "next" is tomorrow's open
    check_date = dt.date()
    dow = dt.weekday()
    hour = dt.hour

    # If during market hours (9:30-16:00), use today
    if dow < 5 and 9 <= hour < 16:
        target_date = check_date
    else:
        # Find next trading day
        target_date = check_date + timedelta(days=1)
        while target_date.weekday() >= 5:
            target_date += timedelta(days=1)

    # Find this date in daily data
    daily_dates = daily.index.date
    matches = daily[daily_dates == target_date]
    if matches.empty:
        # Try next few days
        for offset in range(1, 5):
            next_try = target_date + timedelta(days=offset)
            matches = daily[daily_dates == next_try]
            if not matches.empty:
                break

    if matches.empty:
        return None, None

    return matches.index[0], matches.iloc[0]


def compute_outcomes(dt, daily, hourly, fivemin):
    """Compute price outcomes after a sentiment event."""
    next_open_dt, next_day = find_next_market_open(dt, daily)
    if next_open_dt is None:
        return {}

    outcomes = {}
    next_date = next_open_dt.date() if hasattr(next_open_dt, 'date') else next_open_dt

    # Get previous close for gap calculation
    daily_dates = daily.index.date
    prev_days = daily[daily_dates < next_date]
    if not prev_days.empty:
        prev_close = prev_days["Close"].iloc[-1]
        gap_pct = (next_day["Open"] - prev_close) / prev_close * 100
        outcomes["gap"] = gap_pct
    else:
        outcomes["gap"] = 0

    # Full day return
    outcomes["full_day"] = (next_day["Close"] - next_day["Open"]) / next_day["Open"] * 100

    # Next 2 day return
    idx = list(daily_dates).index(next_date) if next_date in daily_dates else -1
    if idx >= 0 and idx + 1 < len(daily):
        day2_close = daily.iloc[idx + 1]["Close"]
        outcomes["two_day"] = (day2_close - next_day["Open"]) / next_day["Open"] * 100
    else:
        outcomes["two_day"] = np.nan

    # Intraday outcomes from hourly data
    hourly_dates = hourly.index.date
    day_hourly = hourly[hourly_dates == next_date]
    if not day_hourly.empty:
        open_price = day_hourly["Open"].iloc[0]

        # 30 min ~ first bar
        if len(day_hourly) >= 1:
            outcomes["first_30m"] = (day_hourly["Close"].iloc[0] - open_price) / open_price * 100

        # 1 hour ~ first 1 bar
        if len(day_hourly) >= 1:
            outcomes["first_1h"] = (day_hourly["High"].iloc[0] - open_price) / open_price * 100

        # 2 hours ~ first 2 bars
        if len(day_hourly) >= 2:
            max_2h = day_hourly["High"].iloc[:2].max()
            outcomes["first_2h"] = (max_2h - open_price) / open_price * 100
    else:
        # Try 5-min data
        fivemin_dates = fivemin.index.date
        day_5m = fivemin[fivemin_dates == next_date]
        if not day_5m.empty:
            open_price = day_5m["Open"].iloc[0]
            # 30 min = 6 bars
            if len(day_5m) >= 6:
                outcomes["first_30m"] = (day_5m["Close"].iloc[5] - open_price) / open_price * 100
            if len(day_5m) >= 12:
                outcomes["first_1h"] = (day_5m["High"].iloc[:12].max() - open_price) / open_price * 100
            if len(day_5m) >= 24:
                outcomes["first_2h"] = (day_5m["High"].iloc[:24].max() - open_price) / open_price * 100

    return outcomes


def get_market_context(dt, daily):
    """Get the stock's technical state at the time sentiment was posted."""
    if dt is None:
        return {}

    # Find the most recent trading day at or before this timestamp
    check_date = dt.date()
    daily_dates = daily.index.date
    prior = daily[daily_dates <= check_date]
    if prior.empty:
        return {}

    last = prior.iloc[-1]
    context = {}

    # Trend: use EMA5 (fast) instead of SMA10 (was lagging during crashes)
    if len(prior) >= 5:
        ema5 = prior["Close"].ewm(span=5, adjust=False).mean().iloc[-1]
        sma5 = prior["Close"].tail(5).mean()
        context["trend"] = "uptrend" if last["Close"] > ema5 else "downtrend"
    else:
        context["trend"] = "unknown"

    # Trend breaking: if today dropped >3% while near the EMA crossover, trend is broken
    if len(prior) >= 5:
        today_ret = (last["Close"] - last["Open"]) / last["Open"] * 100 if last["Open"] > 0 else 0
        ema5_dist = abs(last["Close"] - ema5) / ema5 * 100
        if today_ret < -3 and ema5_dist < 2:
            context["trend"] = "trend_breaking"
        elif today_ret < -5:
            context["trend"] = "trend_breaking"

    # Trend strength: how far from EMA5
    if len(prior) >= 5:
        ema5_pct = (last["Close"] - ema5) / ema5 * 100
        if ema5_pct > 3:
            context["trend_strength"] = "strong_uptrend"
        elif ema5_pct > 0:
            context["trend_strength"] = "weak_uptrend"
        elif ema5_pct > -3:
            context["trend_strength"] = "weak_downtrend"
        else:
            context["trend_strength"] = "strong_downtrend"

    # Mean reversion signal: big drop = bounce likely (GRRR-specific)
    if len(prior) >= 2:
        last_ret = (last["Close"] - last["Open"]) / last["Open"] * 100 if last["Open"] > 0 else 0
        if last_ret < -3:
            context["reversion"] = "bounce_setup"
        elif last_ret < -1:
            context["reversion"] = "mild_dip"
        elif last_ret > 3:
            context["reversion"] = "fade_setup"
        elif last_ret > 1:
            context["reversion"] = "mild_rally"
        else:
            context["reversion"] = "neutral"

    # Recent momentum: last 5 days
    if len(prior) >= 5:
        mom5 = (last["Close"] - prior["Close"].iloc[-5]) / prior["Close"].iloc[-5] * 100
        if mom5 > 3:
            context["momentum"] = "strong_up"
        elif mom5 > 0:
            context["momentum"] = "up"
        elif mom5 > -3:
            context["momentum"] = "down"
        else:
            context["momentum"] = "strong_down"

    # Volatility regime
    if len(prior) >= 14:
        atr = (prior["High"] - prior["Low"]).tail(14).mean()
        atr_pct = atr / last["Close"] * 100
        context["volatility"] = "high" if atr_pct > 6 else "normal" if atr_pct > 3 else "low"

    # Recent volume
    if len(prior) >= 20:
        avg_vol = prior["Volume"].tail(20).mean()
        rel_vol = last["Volume"] / avg_vol if avg_vol > 0 else 1
        context["volume_regime"] = "high_vol" if rel_vol > 1.5 else "normal_vol" if rel_vol > 0.5 else "low_vol"

    return context


def process_sentiment_source(source_name, df_sentiment, daily, hourly, fivemin):
    """Process a sentiment source (news or reddit) into discretized features + outcomes."""
    print(f"\n  Processing {source_name} ({len(df_sentiment)} items)...")

    records = []
    skipped = 0

    # Compute rolling sentiment for trend detection
    df_sentiment = df_sentiment.sort_values("published").copy()

    for idx, row in df_sentiment.iterrows():
        # Parse timestamp
        pub = row.get("published", "")
        if not pub or str(pub) == "nan" or len(str(pub)) < 10:
            skipped += 1
            continue

        try:
            dt = pd.to_datetime(pub)
            if hasattr(dt, 'tz') and dt.tz:
                dt = dt.tz_localize(None)
        except Exception:
            skipped += 1
            continue

        polarity = row.get("polarity", 0)
        subjectivity = row.get("subjectivity", 0)

        # === ANTECEDENT FEATURES ===
        features = {}

        # Polarity bucket
        if polarity > 0.3:
            features["Polarity"] = "strong_pos"
        elif polarity > 0.1:
            features["Polarity"] = "pos"
        elif polarity > -0.1:
            features["Polarity"] = "neutral"
        elif polarity > -0.3:
            features["Polarity"] = "neg"
        else:
            features["Polarity"] = "strong_neg"

        # Subjectivity
        if subjectivity > 0.6:
            features["Subjectivity"] = "opinionated"
        elif subjectivity > 0.3:
            features["Subjectivity"] = "mixed"
        else:
            features["Subjectivity"] = "factual"

        # Time posted
        features["Post_Time"] = classify_post_time(dt)

        # Day of week posted
        dow_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
        features["Post_DOW"] = dow_names.get(dt.weekday(), "unknown")

        # Buzz: count articles in last 24 hours
        cutoff_24h = dt - timedelta(hours=24)
        recent_mask = df_sentiment["published"].apply(
            lambda x: pd.to_datetime(x, errors="coerce")
        ).between(cutoff_24h, dt)
        buzz_count = recent_mask.sum()
        if buzz_count >= 8:
            features["Buzz"] = "high_buzz"
        elif buzz_count >= 4:
            features["Buzz"] = "moderate_buzz"
        elif buzz_count >= 2:
            features["Buzz"] = "low_buzz"
        else:
            features["Buzz"] = "quiet"

        # Sentiment trend: avg polarity in last 48h vs prior 48h
        cutoff_48h = dt - timedelta(hours=48)
        cutoff_96h = dt - timedelta(hours=96)
        recent_items = df_sentiment[
            df_sentiment["published"].apply(lambda x: pd.to_datetime(x, errors="coerce")).between(cutoff_48h, dt)
        ]
        older_items = df_sentiment[
            df_sentiment["published"].apply(lambda x: pd.to_datetime(x, errors="coerce")).between(cutoff_96h, cutoff_48h)
        ]
        if len(recent_items) >= 2 and len(older_items) >= 2:
            recent_avg = recent_items["polarity"].mean()
            older_avg = older_items["polarity"].mean()
            diff = recent_avg - older_avg
            if diff > 0.1:
                features["Sent_Trend"] = "improving"
            elif diff < -0.1:
                features["Sent_Trend"] = "deteriorating"
            else:
                features["Sent_Trend"] = "stable"
        else:
            features["Sent_Trend"] = "unknown"

        # Market context at time of sentiment
        context = get_market_context(dt, daily)
        for k, v in context.items():
            features[f"Mkt_{k}"] = v

        # Combined sentiment label
        features["Sentiment"] = row.get("sentiment", "neutral")

        # === OUTCOME FEATURES ===
        outcomes_raw = compute_outcomes(dt, daily, hourly, fivemin)
        if not outcomes_raw:
            skipped += 1
            continue

        outcomes = {}

        # Gap
        gap = outcomes_raw.get("gap", 0)
        if gap > 1.5:
            outcomes["NEXT_Gap"] = "big_gap_up"
        elif gap > 0.3:
            outcomes["NEXT_Gap"] = "gap_up"
        elif gap > -0.3:
            outcomes["NEXT_Gap"] = "flat_open"
        elif gap > -1.5:
            outcomes["NEXT_Gap"] = "gap_down"
        else:
            outcomes["NEXT_Gap"] = "big_gap_down"

        # First 30m
        f30 = outcomes_raw.get("first_30m")
        if f30 is not None:
            if f30 > 2:
                outcomes["NEXT_30m"] = "30m_surge"
            elif f30 > 0.5:
                outcomes["NEXT_30m"] = "30m_up"
            elif f30 > -0.5:
                outcomes["NEXT_30m"] = "30m_flat"
            elif f30 > -2:
                outcomes["NEXT_30m"] = "30m_down"
            else:
                outcomes["NEXT_30m"] = "30m_dump"

        # First 1h
        f1h = outcomes_raw.get("first_1h")
        if f1h is not None:
            if f1h > 3:
                outcomes["NEXT_1h_Spike"] = "1h_big_spike"
            elif f1h > 1:
                outcomes["NEXT_1h_Spike"] = "1h_spike"
            elif f1h > 0:
                outcomes["NEXT_1h_Spike"] = "1h_small_up"
            else:
                outcomes["NEXT_1h_Spike"] = "1h_no_spike"

        # First 2h
        f2h = outcomes_raw.get("first_2h")
        if f2h is not None:
            if f2h > 4:
                outcomes["NEXT_2h_Max"] = "2h_rip"
            elif f2h > 2:
                outcomes["NEXT_2h_Max"] = "2h_strong"
            elif f2h > 0.5:
                outcomes["NEXT_2h_Max"] = "2h_up"
            else:
                outcomes["NEXT_2h_Max"] = "2h_flat"

        # Full day
        fd = outcomes_raw.get("full_day", 0)
        if fd > 3:
            outcomes["NEXT_FullDay"] = "day_big_up"
        elif fd > 1:
            outcomes["NEXT_FullDay"] = "day_up"
        elif fd > -1:
            outcomes["NEXT_FullDay"] = "day_flat"
        elif fd > -3:
            outcomes["NEXT_FullDay"] = "day_down"
        else:
            outcomes["NEXT_FullDay"] = "day_big_down"

        # Full day direction
        outcomes["NEXT_DayDir"] = "day_UP" if fd > 0 else "day_DOWN"

        # Two day
        td = outcomes_raw.get("two_day")
        if td is not None and not np.isnan(td):
            if td > 3:
                outcomes["NEXT_2Day"] = "2day_big_up"
            elif td > 1:
                outcomes["NEXT_2Day"] = "2day_up"
            elif td > -1:
                outcomes["NEXT_2Day"] = "2day_flat"
            elif td > -3:
                outcomes["NEXT_2Day"] = "2day_down"
            else:
                outcomes["NEXT_2Day"] = "2day_big_down"

        record = {**features, **outcomes, "_timestamp": str(dt)}
        records.append(record)

    print(f"    Processed: {len(records)}, Skipped: {skipped}")
    return pd.DataFrame(records)


def mine_rules_from_df(df, name):
    """Run Apriori-style mining on a processed dataframe."""
    # Split into antecedent and consequent columns
    ant_cols = [c for c in df.columns if not c.startswith("NEXT_") and not c.startswith("_")]
    con_cols = [c for c in df.columns if c.startswith("NEXT_")]

    print(f"\n  Mining {name} rules...")
    print(f"    Antecedent features: {len(ant_cols)}: {ant_cols}")
    print(f"    Outcome features: {len(con_cols)}: {con_cols}")
    print(f"    Samples: {len(df)}")

    total_n = len(df)
    rules = []

    # Precompute consequent masks
    con_masks = {}
    for col in con_cols:
        for val in df[col].dropna().unique():
            mask = df[col] == val
            count = mask.sum()
            if count >= MIN_SUPPORT:
                con_masks[(col, str(val))] = mask

    print(f"    Consequent values: {len(con_masks)}")

    for size in range(1, min(MAX_ANT_SIZE + 1, len(ant_cols) + 1)):
        combos = list(itertools.combinations(ant_cols, size))
        print(f"    Size {size}: {len(combos)} combos...", end=" ", flush=True)
        found = 0

        for combo in combos:
            sub = df[list(combo)].dropna()
            if len(sub) < MIN_SUPPORT:
                continue

            groups = sub.groupby(list(combo))

            for group_key, group_idx in groups:
                if not isinstance(group_key, tuple):
                    group_key = (group_key,)

                ant_mask = df.index.isin(group_idx.index)
                ant_count = ant_mask.sum()

                if ant_count < MIN_SUPPORT:
                    continue

                ant_desc = " & ".join(f"{c}={v}" for c, v in zip(combo, group_key))

                for (con_col, con_val), con_mask in con_masks.items():
                    both = ant_mask & con_mask
                    both_count = both.sum()

                    if both_count < MIN_SUPPORT:
                        continue

                    confidence = both_count / ant_count
                    expected = con_mask.sum() / total_n
                    lift = confidence / expected if expected > 0 else 0

                    if confidence >= MIN_CONFIDENCE and lift >= MIN_LIFT:
                        rules.append({
                            "antecedent": ant_desc,
                            "consequent": f"{con_col}={con_val}",
                            "confidence": round(confidence, 4),
                            "lift": round(lift, 4),
                            "count": both_count,
                            "antecedent_count": ant_count,
                            "support": round(both_count / total_n, 4),
                            "size": size,
                        })
                        found += 1

        print(f"{found} rules")

    rules.sort(key=lambda r: r["count"] * r["confidence"] * r["lift"], reverse=True)
    print(f"    Total {name} rules: {len(rules)}")
    return rules


def run():
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{datetime.now()}] Sentiment-Price Association Rule Mining for {TICKER}")

    # Load price data
    daily, hourly, fivemin = load_price_data()

    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("  GRRR SENTIMENT → PRICE ASSOCIATION RULES")
    report_lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report_lines.append("=" * 80)

    # === NEWS ===
    if os.path.exists(NEWS_FILE):
        df_news = pd.read_csv(NEWS_FILE)
        print(f"\n  News articles loaded: {len(df_news)}")

        news_processed = process_sentiment_source("News", df_news, daily, hourly, fivemin)
        news_processed.to_csv(os.path.join(DATA_DIR, "grrr_news_price_matched.csv"), index=False)

        if len(news_processed) >= MIN_SUPPORT:
            news_rules = mine_rules_from_df(news_processed, "News")
            if news_rules:
                pd.DataFrame(news_rules).to_csv(RULES_NEWS, index=False)
                report_lines.extend(_format_rules("NEWS SENTIMENT", news_rules, len(news_processed)))
        else:
            print("    Not enough matched news records for mining.")
            report_lines.append("\n  NEWS: Not enough matched records.")
    else:
        print("  No news data found.")

    # === REDDIT ===
    if os.path.exists(REDDIT_FILE):
        df_reddit = pd.read_csv(REDDIT_FILE)
        print(f"\n  Reddit posts loaded: {len(df_reddit)}")

        reddit_processed = process_sentiment_source("Reddit", df_reddit, daily, hourly, fivemin)
        reddit_processed.to_csv(os.path.join(DATA_DIR, "grrr_reddit_price_matched.csv"), index=False)

        if len(reddit_processed) >= MIN_SUPPORT:
            reddit_rules = mine_rules_from_df(reddit_processed, "Reddit")
            if reddit_rules:
                pd.DataFrame(reddit_rules).to_csv(RULES_REDDIT, index=False)
                report_lines.extend(_format_rules("REDDIT SENTIMENT", reddit_rules, len(reddit_processed)))
        else:
            print("    Not enough matched Reddit records for mining.")
            report_lines.append("\n  REDDIT: Not enough matched records.")
    else:
        print("  No Reddit data found.")

    report_lines.append("\n" + "=" * 80)
    report_lines.append("This is NOT financial advice.")
    report_lines.append("=" * 80)

    report = "\n".join(report_lines)
    with open(REPORT_FILE, "w") as f:
        f.write(report)
    print(f"\n  Saved report to {REPORT_FILE}")
    print(f"\n{report}")
    return True


def _format_rules(title, rules, n_samples):
    lines = []
    lines.append(f"\n{'=' * 80}")
    lines.append(f"  {title} → PRICE RULES ({len(rules)} rules, {n_samples} samples)")
    lines.append("=" * 80)

    df_r = pd.DataFrame(rules)

    # Simple rules (1 feature) with highest count
    simple = [r for r in rules if r["size"] == 1]
    if simple:
        simple.sort(key=lambda x: x["count"], reverse=True)
        lines.append(f"\n  SIMPLE RULES (1 condition, sorted by sample count):")
        lines.append(f"  {'Conf':>5} {'Lift':>5} {'Hits':>8}  Rule")
        lines.append(f"  {'-' * 70}")
        for r in simple[:20]:
            lines.append(f"  {r['confidence']*100:>4.0f}% {r['lift']:>5.2f} {r['count']:>4}/{r['antecedent_count']:<4}  IF {r['antecedent']}")
            lines.append(f"  {'':>30}→ {r['consequent']}")

    # By outcome type
    for outcome, label in [
        ("NEXT_DayDir", "NEXT DAY DIRECTION"),
        ("NEXT_FullDay", "NEXT DAY MAGNITUDE"),
        ("NEXT_Gap", "NEXT SESSION GAP"),
        ("NEXT_1h_Spike", "FIRST HOUR SPIKE"),
        ("NEXT_2h_Max", "FIRST 2 HOURS"),
        ("NEXT_2Day", "2-DAY MOVE"),
    ]:
        subset = [r for r in rules if outcome in r["consequent"]]
        if not subset:
            continue
        subset.sort(key=lambda x: x["count"] * x["confidence"], reverse=True)
        lines.append(f"\n  --- {label} ---")
        lines.append(f"  {'Conf':>5} {'Lift':>5} {'Hits':>8}  Rule")
        lines.append(f"  {'-' * 70}")
        for r in subset[:12]:
            lines.append(f"  {r['confidence']*100:>4.0f}% {r['lift']:>5.2f} {r['count']:>4}/{r['antecedent_count']:<4}  IF {r['antecedent'][:50]}")
            lines.append(f"  {'':>30}→ {r['consequent']}")

    return lines


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
